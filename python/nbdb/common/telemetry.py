"""
Telemetry Module
"""
from __future__ import annotations

import functools
import logging
import os
import pickle
import socket
import struct
import time
from typing import List, Optional

from flask_httpauth import HTTPBasicAuth
import pyformance.reporters.carbon_reporter
import requests
from nbdb.config.settings import Settings
from pyformance import MetricsRegistry, set_global_registry
from pyformance.registry import get_qualname
from pyformance.reporters.influx import InfluxReporter

logger = logging.getLogger()
_global_registry = None


def meter_failures(fn):
    """
    Decorator to the rate at which a function fails.
    :param fn: the function to be decorated
    :type fn: C{func}
    :return: the decorated function
    :rtype: C{func}
    """
    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            _global_registry.meter("%s_failures" % fn.__name__).mark()
            raise e
    return wrapper


def user_meter_calls_with_flask_auth(flask_auth: HTTPBasicAuth):
    """
    Same as pyformance.meter_calls but adds user as a tag

    flask_auth object must be available at compile time
    """
    def meter_calls(fn):
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            qualname = get_qualname(fn)
            user = flask_auth.current_user()
            _global_registry.meter(
                "%s_calls" % qualname, tag_key_values=[f"User={user}"]
            ).mark()
            return fn(*args, **kwargs)
        return wrapper

    return meter_calls


def user_time_calls_with_flask_auth(flask_auth: HTTPBasicAuth):
    """
    Same as pyformance.time_calls but adds user as a tag.

    flask_auth object must be available at compile time
    """
    def time_calls(fn):
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            qualname = get_qualname(fn)
            user = flask_auth.current_user()
            _timer = _global_registry.timer(
                "%s_calls" % qualname, tag_key_values=[f"User={user}"])
            with _timer.time(fn=qualname):
                return fn(*args, **kwargs)
        return wrapper

    return time_calls


def user_time_calls(fn):
    """
    Same as pyformance.time_calls but adds user as a tag

    Meant to be used for functions where it is not possible to have flask_auth
    available at compile time.

    These functions must have a keyword argument 'user'
    """
    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        qualname = get_qualname(fn)
        # If flask_auth is not provided, one of the keyword arguments
        # to the original function must be "user"
        user = kwargs.get("user")
        assert user is not None, \
            ("Non-null keyword argument value for 'user' not found "
             "in func: %s" % qualname)
        _timer = _global_registry.timer(
            "%s_calls" % qualname, tag_key_values=[f"User={user}"])
        with _timer.time(fn=get_qualname(fn)):
            return fn(*args, **kwargs)
    return wrapper


class InvalidOperationException(Exception):
    """
    raised when an invalid operation is performed
    """


class MultiRegistry(MetricsRegistry):
    """
    Same as MetricsRegistry but records metrics in Influx format
    and capable of dumping in both Influx / graphite format
    """
    def __init__(self, extra_tags=None, clock=time):
        self.extra_tags = extra_tags
        super(MultiRegistry, self).__init__(clock)

    @staticmethod
    def generate_key(measurement: str,
                     tag_key_values: Optional[List[str]]) -> str:
        """
        Generate Influx format key based on provided arguments
        """
        if tag_key_values is None:
            tag_key_values = []
        return ",".join([measurement] + tag_key_values)

    # We are deliberately overriding with different arguments to allow for a
    # common way to generate both Influx / graphite metrics
    # pylint: disable-msg=W0221  # Arguments Differ
    def counter(self, measurement: str,
                tag_key_values: Optional[List[str]] = None):
        """
        Store counter with key in Influx format.
        :param measurement: Measurement name
        :param tag_key_values: List of tag_key=tag_value pairs
        """
        return super(MultiRegistry, self).counter(
            MultiRegistry.generate_key(measurement, tag_key_values))

    # We are deliberately overriding with different arguments to allow for a
    # common way to generate both Influx / graphite metrics
    # pylint: disable-msg=W0221  # Arguments Differ
    def gauge(self, measurement: str,
              tag_key_values: Optional[List[str]] = None):
        """
        Store gauge with key in Influx format.
        :param measurement: Measurement name
        :param tag_key_values: List of tag_key=tag_value pairs
        """
        return super(MultiRegistry, self).gauge(
            MultiRegistry.generate_key(measurement, tag_key_values))

    # We are deliberately overriding with different arguments to allow for a
    # common way to generate both Influx / graphite metrics
    # pylint: disable-msg=W0221  # Arguments Differ
    def histogram(self, measurement: str,
                  tag_key_values: Optional[List[str]] = None):
        """
        Store histogram with key in Influx format.
        :param measurement: Measurement name
        :param tag_key_values: List of tag_key=tag_value pairs
        """
        return super(MultiRegistry, self).histogram(
            MultiRegistry.generate_key(measurement, tag_key_values))

    # We are deliberately overriding with different arguments to allow for a
    # common way to generate both Influx / graphite metrics
    # pylint: disable-msg=W0221  # Arguments Differ
    def meter(self, measurement: str,
              tag_key_values: Optional[List[str]] = None):
        """
        Store meter with key in Influx format.
        :param measurement: Measurement name
        :param tag_key_values: List of tag_key=tag_value pairs
        """
        return super(MultiRegistry, self).meter(
            MultiRegistry.generate_key(measurement, tag_key_values))

    # We are deliberately overriding with different arguments to allow for a
    # common way to generate both Influx / graphite metrics
    # pylint: disable-msg=W0221  # Arguments Differ
    def timer(self, measurement: str,
              tag_key_values: Optional[List[str]] = None):
        """
        Store timer with key in Influx format.
        :param measurement: Measurement name
        :param tag_key_values: List of tag_key=tag_value pairs
        """
        return super(MultiRegistry, self).timer(
            MultiRegistry.generate_key(measurement, tag_key_values))

    @staticmethod
    def convert_key_to_graphite(influx_key):
        """
        Converts key stored in Influx format to flattened Graphite format

        Influx Syntax
        <measurement>[,tg_k1=tg_v1[,tg_k2=tg_v2]]
        Graphite syntax
        measurement.tg_v1.tg_v2
        """
        parts = influx_key.split(",")
        measurement = parts[0]
        tag_values = [tag_part.split('=')[1] for tag_part in parts[1:]]
        return ".".join([measurement] + tag_values)

    def add_extra_tags(self, key, protocol):
        """
        Adds extra tags to an Influx metric.

        If protocol is graphite, nothing is done. If no extra tags were
        specified during initialization, nothing is done. Otherwise, we will
        add the provided extra tags

        Eg. The following influx metric
        MetricConsumer.time_taken,Task=on_assign

        will be converted to
        MetricConsumer.time_taken,Identifier=XYZ,Task=on_assign
        """
        if protocol != 'influx' or not self.extra_tags:
            # Nothing to be done if protocol isn't influx or if no extra tags
            # were provided
            return key

        parts = key.split(",")
        measurement = parts[0]
        return ",".join([measurement] + self.extra_tags + parts[1:])

    def dump_metrics(self, protocol='influx'):
        """
        Formats all of the metrics and returns them as a dict.
        :return: C{list} of C{dict} of metrics
        """
        metrics = {}
        for metric_type in (
                self._counters,
                self._histograms,
                self._meters,
                self._timers,
                self._gauges,
        ):
            for key in metric_type:
                # We can potentially dump the key in a format different to how
                # it is stored. For example, we store the key in Influx format.
                # But if graphite protocol is requested, we will convert it
                written_key = key
                if protocol == 'graphite':
                    # Convert key to flattened graphite format
                    written_key = MultiRegistry.convert_key_to_graphite(key)
                elif protocol == 'influx':
                    # Nothing to do
                    pass
                else:
                    assert False, "Unsupported protocol %s" % protocol

                # Add extra tags while dumping metrics
                written_key = self.add_extra_tags(written_key, protocol)
                metrics[written_key] = self.get_metrics(key)

        return metrics


# pylint: disable-msg=R0903  # Too Few Public Methods
class CarbonReporter(pyformance.reporters.carbon_reporter.CarbonReporter):
    """
    Same as CarbonReporter but can convert Influx metrics to Graphite
    """
    # Copied shamelessly from the following:
    # https://github.com/omergertel/pyformance/blob/master/pyformance/reporters/carbon_reporter.py
    #
    # Ideally the base class would have been written in a way as to avoid
    # needing to override the whole _collect_metrics() method, but there's no
    # way to do this right now.
    def _collect_metrics(self, registry, timestamp=None):
        """
        Collect metrics.

        Copied from parent class except we call
        self.dump_metrics(protocol='graphite')
        instead of
        self.dump_metrics()
        """
        timestamp = timestamp or int(round(self.clock.time()))
        metrics = registry.dump_metrics(protocol='graphite')
        if self.pickle_protocol:
            payload = pickle.dumps(
                [
                    (
                        "%s%s.%s" % (self.prefix, metric_name, metric_key),
                        (timestamp, metric_value),
                    )
                    for metric_name, metric in metrics.items()
                    for metric_key, metric_value in metric.items()
                ],
                protocol=2,
            )
            header = struct.pack("!L", len(payload))
            return header + payload
        else:
            metrics_data = []
            for metric_name, metric in metrics.items():
                for metric_key, metric_value in metric.items():
                    metric_line = "%s%s.%s %s %s\n" % (
                        self.prefix,
                        metric_name,
                        metric_key,
                        metric_value,
                        timestamp,
                    )
                    metrics_data.append(metric_line)
            result = "".join(metrics_data)
            return result.encode()


# This class should be turned into a data class
# pylint: disable-msg=R0903 # Too Few Public Methods
class Telemetry:
    """
    Simple wrapper around the PyFormance module
    """

    inst: Telemetry = None

    def __init__(self):
        """
        Initialize the registry and create the reporter
        """
        # Default metrics identifier is the hostname
        identifier = socket.gethostname()

        if os.environ.get("ECS_CONTAINER_METADATA_URI"):
            # We are running within AWS ECS. Determine the ECS task ID and use
            # that as our metrics identifier
            url = "%s/task" % os.environ["ECS_CONTAINER_METADATA_URI"]
            try:
                resp = requests.get(url, timeout=10)
                resp.raise_for_status()
            except Exception as exc:
                logger.exception("Unable to determine ECS task ID "
                                 "due to error: %s", exc)
            else:
                task_arn = resp.json()["TaskARN"]
                # Task ARN looks like the following
                # arn:aws:<id>:task/<service_name>/<task_id>
                identifier = task_arn.split("/")[-1]

        # Initialize MultiRegistry which can generate stats in both Influx and
        # Graphite format
        self.registry = MultiRegistry(
            extra_tags=[
                f"Identifier={identifier}",
                f"Environment={Settings.inst.telemetry.environment}"])
        # Set global registry so that we use MultiRegistry for all decorators
        # like time_calls() etc.
        set_global_registry(self.registry)
        # pylint: disable-msg=W0603  # Global Statement
        global _global_registry
        _global_registry = self.registry

        protocol = Settings.inst.telemetry.protocol
        if protocol == 'graphite':
            # Start Carbon reporter
            carbon_prefix = '{}.{}.'.format(
                Settings.inst.telemetry.graphite.prefix, identifier)
            self.reporter = CarbonReporter(
                registry=self.registry,
                reporting_interval=Settings.inst.telemetry.\
                graphite.reporting_interval,
                prefix=carbon_prefix,
                server=Settings.inst.telemetry.graphite.server,
                port=Settings.inst.telemetry.graphite.port)

        elif protocol == 'influx':
            # Start Influx reporter
            self.reporter = InfluxReporter(
                registry=self.registry,
                reporting_interval=Settings.inst.telemetry.\
                influx.reporting_interval,
                prefix=Settings.inst.telemetry.influx.prefix,
                database=Settings.inst.telemetry.influx.database,
                server=Settings.inst.telemetry.influx.server,
                port=Settings.inst.telemetry.influx.port)
        else:
            assert False, f"Unsupported protocol: {protocol}"

    @staticmethod
    def initialize(start_reporter: bool = True):
        """
        Initialize the telemetry and starts the reporter
        :return:
        """
        if Telemetry.inst is not None:
            raise InvalidOperationException('Telemetry is already initialized')

        Telemetry.inst = Telemetry()
        if start_reporter:
            Telemetry.inst.reporter.start()
