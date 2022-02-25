"""Metrics library."""
# pylint: skip-file

import argparse
import logging
import os
import string
import sys
import time

from datetime import datetime, timedelta
import requests

SRC_ROOT = os.path.join(os.path.dirname(__file__), '..', '..')
sys.path.insert(0, os.path.join(SRC_ROOT))

log = logging.getLogger(os.path.basename(__file__))

GRAPHITE_ABS_TIME_FORMAT = "%H:%M_%Y%m%d"


class MetricServer:
    """Represents a graphite-web endpoint that can be queried for Metrics.

    A typical graphite deploy has a graphite-web instance running that exposes
    a URL API for querying metrics. The URL API allows querying of metrics
    stored in the local whisper database as well as other graphite-web servers
    in the cluster
    """

    def __init__(self,
                 host,
                 port=8081,
                 use_ssl=True,
                 api_key=None,
                 url_prefix=None,
                 use_tunnel=False):
        """Initialize."""
        self.host = host
        self.port = port
        self.use_ssl = use_ssl
        self.use_tunnel = use_tunnel
        self.url_prefix = url_prefix
        self.session = requests.Session()
        if api_key is not None:
            self.session.auth = ('api_key', api_key)

    def __fetch_json_from_url(self, host, port, url_path):
        url = self.__make_url_prefix(host, port) + url_path
        log.debug('Fetching metric from %s' % url)
        # Rubrik clusters use self-signed certs.
        response = self.session.get(url, verify=False)

        # pylint: disable-msg=E1101
        # Pylint wrongly thinks 'ok' is not a member of requests.codes
        if response.status_code == requests.codes.ok:
            return response.json()
        else:
            log.error('%s got invalid response %s %s' % (
                url, response.status_code, response.text))
            return None

    def __fetch_json(self, url_path):
        if self.use_tunnel:
            assert False, "Port forwarding not supported"
        else:
            return self.__fetch_json_from_url(self.host, self.port, url_path)

    def __make_url_prefix(self, host, port):
        if self.use_ssl:
            protocol = 'https'
        else:
            protocol = 'http'

        url = protocol + '://' + host + ':' + str(port)
        if self.url_prefix:
            url += self.url_prefix

        return url

    def __make_url_common(self,
                          target,
                          from_expr,
                          interval_seconds,
                          max_datapoints):
        return '/render?target=%s&format=json%s&until=-%ds%s' % (
            target,
            '&from=%s' % from_expr if from_expr else '',
            interval_seconds,
            '&maxDataPoints=%d' % max_datapoints
            if max_datapoints is not None else '')

    def __make_rel_url(self,
                       target,
                       from_delta=None,
                       interval_seconds=10,
                       max_datapoints=None):
        from_expr = None
        if from_delta:
            from_expr = '%ds' % from_delta.total_seconds()
        return self.__make_url_common(target,
                                      from_expr,
                                      interval_seconds,
                                      max_datapoints)

    def __make_abs_url(self,
                       target,
                       timestamp,
                       interval_seconds=10,
                       max_datapoints=None):
        return self.__make_url_common(target,
                                      str(timestamp),
                                      interval_seconds,
                                      max_datapoints)

    def __make_full_abs_url(self,
                            target,
                            start_time,
                            end_time,
                            max_datapoints=None):
        """Return the url with absolute time stamps."""
        return '/render?target=%s&format=json&from=%s&until=%s%s' % (
            target,
            start_time,
            end_time,
            '&maxDataPoints=%d' % max_datapoints
            if max_datapoints is not None else '')

    def find(self, query, from_epoch_ts=None):
        url = '/metrics/find?query=%s' % query
        # Find command does not support relative time frames. Must provide
        # absolute linux epoch timestamp to filter out series.
        if from_epoch_ts:
            url += "&from=%s" % from_epoch_ts
        return self.__fetch_json(url)

    def get_leaf_node_report(self, prefix, from_epoch_ts):
        report = []
        metrics = self.find(query=prefix + ".*", from_epoch_ts=from_epoch_ts)
        for metric in metrics:
            leaf_nodes = set()
            self.get_leaf_nodes(metric['id'], from_epoch_ts, leaf_nodes)
            report.append((metric['id'], len(leaf_nodes)))
        return report

    def get_leaf_nodes(self, prefix, from_epoch_ts, leaf_nodes):
        metrics = self.find(query=prefix + ".*", from_epoch_ts=from_epoch_ts)
        for metric in metrics:
            if metric['leaf']:
                leaf_nodes.add(metric['id'])
            else:
                self.get_leaf_nodes(metric['id'], from_epoch_ts, leaf_nodes)
        return leaf_nodes

    def get_nodes(self):
        return self.expand('*')

    def expand(self, query):
        url = '/metrics/expand?query=%s' % query
        result = self.__fetch_json(url)
        if not result:
            return None
        return result['results']

    def get_metrics(self, query, from_delta=timedelta(days=1)):
        url = ('/render?target=%s&format=json&from=-%ds&maxDataPoints=1' %
               (query, abs(from_delta.total_seconds())))
        return [x['target'] for x in self.__fetch_json(url)]

    def get_current(self, m, wait=0, from_delta=timedelta(seconds=-120)):
        url = self.__make_rel_url(target=m.metric,
                                  from_delta=from_delta,
                                  interval_seconds=10,
                                  max_datapoints=1)
        while True:
            result = self.__fetch_json(url)
            log.debug("Query result: %s" % result)
            if result:
                # There are some timing issues where we get some data
                # but no datapoints. Return the latest non-NULL datapoint in
                # that case
                datapoints = list(filter(lambda d: d[0] is not None,
                                         result[0]['datapoints']))
                if len(datapoints) >= 1:
                    return datapoints[-1][0]

            if wait <= 0:
                break

            wait -= 1
            time.sleep(1)

        return None

    def get_last(self, m, from_delta=None):
        url = self.__make_rel_url(m.metric, from_delta=from_delta)

        result = self.__fetch_json(url)
        if not result:
            return None

        datapoints = result[0]['datapoints']

        # There are some timing issues where we get some data
        # but no datapoints - Assume none in that case
        return None if len(datapoints) < 1 else datapoints[-1][0]

    def __fetch_datapoints(self, url):
        result = self.__fetch_json(url)
        if not result:
            return None
        return result[0]['datapoints']

    def get_rel_series(self, m, from_delta):
        """Return the datapoints for a single time-relative series."""
        url = self.__make_rel_url(m.metric, from_delta)
        return self.__fetch_datapoints(url)

    def get_abs_series(self, m, timestamp):
        """Return the datapoints for a single time-absolute series."""
        url = self.__make_abs_url(m.metric, timestamp)
        return self.__fetch_datapoints(url)

    def get_all_abs_series(self,
                           m,
                           start_datetime,
                           end_datetime,
                           max_datapoints=None):
        url = self.__make_full_abs_url(
            m.metric,
            start_datetime.strftime(GRAPHITE_ABS_TIME_FORMAT),
            end_datetime.strftime(GRAPHITE_ABS_TIME_FORMAT),
            max_datapoints)
        return self.__fetch_json(url)

    def get_all_rel_series(self,
                           m,
                           from_delta,
                           max_datapoints=None,
                           interval_seconds=10):
        """Return the datapoints for one or more time-relative series.

        The values are returned as a list of rows where each row is a
        dictionary with the time series name and a list of datapoints.
        Each datapoint is a tuple of value and timestamp.
        """
        url = self.__make_rel_url(
            m.metric,
            from_delta,
            interval_seconds=interval_seconds,
            max_datapoints=max_datapoints)
        return self.__fetch_json(url)


class Metric(object):
    """A time-series metric retrieved from a graphite server.

    Each metric is represented as a time series. When retrieving a metric from
    a graphite server, we can use the get_current() or get_last() methods to
    retrieve a discrete value.
    """

    def __init__(self, metric):
        """Initialize."""
        self.metric = metric

    def __str__(self):
        """Return string representation."""
        return self.metric

    def apply(self, function):
        self.metric = '%s(%s)' % (function, self.metric)
        return self

    def absolute(self):
        self.metric = 'absolute(%s)' % self.metric
        return self

    def changed(self):
        self.metric = 'changed(%s)' % self.metric
        return self

    def counter_delta(self):
        """Total increase of a counter."""
        assert self.metric.endswith('.count'), \
            'Use only with counters %s' % self.metric
        return self.non_negative_derivative().transform_null().integral()

    def count_series(self):
        self.metric = 'countSeries(%s)' % self.metric
        return self

    def derivative(self):
        self.metric = 'derivative(%s)' % self.metric
        return self

    def remove_below_value(self, val):
        self.metric = 'removeBelowValue(%s, %d)' % (self.metric, val)
        return self

    def equal_to(self, value):
        """Return series that have at least one value equal to the given.

        In those series, data point values that are not equal to the given are
        set to None
        """
        self.metric = 'removeAboveValue(removeBelowValue(%s,%d),%d)' % \
                      (self.metric, value, value)
        self.metric = 'removeEmptySeries(%s)' % self.metric
        return self

    def exclude(self, pattern):
        """Return data points that exclude the provided pattern."""
        self.metric = "exclude(%s,'%s')" % (self.metric, pattern)
        return self

    def greater(self, value):
        """Return series that have at least one value greater than the given.

        In those series, data point values that are not greater than the given
        are set to None
        """
        self.metric = 'removeBelowValue(%s,%d)' % (self.metric, value + 1)
        self.metric = 'removeEmptySeries(%s)' % self.metric
        return self

    def group_by_node(self, node, op='sum'):
        self.metric = "groupByNode(%s, %d, '%s')" % (self.metric, node, op)
        return self

    def group(self, other_metric):
        self.metric = 'group(%s, %s)' % (self.metric, other_metric)
        return self

    def group_by_nodes(self, op, *nodes):
        s = 'groupByNodes(%s, "%s"' % (self.metric, op)
        for node in nodes:
            s += ', %s' % node
        s += ')'
        self.metric = s
        return self

    def integral(self):
        self.metric = 'integral(%s)' % self.metric
        return self

    def is_non_null(self):
        self.metric = 'isNonNull(%s)' % self.metric
        return self

    def keep_last(self, datapoints=None):
        if datapoints is None:
            self.metric = 'keepLastValue(%s)' % self.metric
        else:
            self.metric = 'keepLastValue(%s, %d)' % (self.metric, datapoints)
        return self

    def name_nodes(self, *node_positions):
        self.metric = 'aliasByNode(%s,%s)' % (
            self.metric, ','.join(map(str, node_positions)))
        return self

    def non_negative_derivative(self):
        self.metric = 'nonNegativeDerivative(%s)' % self.metric
        return self

    def not_equal(self, value):
        """Return series that have at least one value not equal to the given.

        In those series, data point values that are equal to the given are
        set to None
        """
        self.metric = 'removeBelowValue(absolute(offset(%s,%d)),1)' % (
            self.metric, -value
        )
        self.metric = 'removeEmptySeries(%s)' % self.metric
        return self

    def offset(self, value):
        self.metric = 'offset(%s,%d)' % (self.metric, value)
        return self

    def scale(self, factor):
        self.metric = 'scale(%s, %s)' % (self.metric, factor)
        return self

    def scale_to_seconds(self, seconds):
        """Return value per seconds for the given number of seconds."""
        self.metric = 'scaleToSeconds(%s, %d)' % (self.metric, seconds)
        return self

    def sum_series(self):
        self.metric = 'sumSeries(%s)' % self.metric
        return self

    def max_series(self):
        self.metric = 'maxSeries(%s)' % self.metric
        return self

    def avg_series(self):
        self.metric = 'averageSeries(%s)' % self.metric
        return self

    def divide_series(self, divisor_metric):
        self.metric = 'divideSeries(%s, %s)' % (self.metric,
                                                divisor_metric.metric)
        return self

    def diff_series(self, other):
        self.metric = 'diffSeries(%s, %s)' % (self.metric,
                                              other.metric)
        return self

    def moving_average(self, interval):
        self.metric = 'movingAverage(%s, "%s")' % (self.metric, interval)
        return self

    def percentile(self, num):
        self.metric = 'nPercentile(%s, %s)' % (self.metric, num)
        return self

    def summarize(self, interval, func='sum'):
        self.metric = 'summarize(%s,"%s","%s")' % (self.metric, interval, func)
        return self

    def transform_null(self, value=0):
        self.metric = 'transformNull(%s,%s)' % (self.metric, str(value))
        return self

    def average_with_wildcards(self, idx):
        self.metric = 'averageSeriesWithWildcards(%s,%s)' % (self.metric, idx)
        return self

    def sum_with_wildcards(self, *nodes):
        self.metric = 'sumSeriesWithWildcards(%s,%s)' % (
            self.metric, ','.join(map(str, nodes)))
        return self

    def template_substitute(self, params):
        return Metric(string.Template(self.metric).safe_substitute(params))


class NodeMetric(Metric):
    """Convenience metric class for nodes.

    Metrics in are prefixed with the hostname
    """

    def __init__(self, node, metric):
        """Initialize."""
        super(NodeMetric, self).__init__(node + '.' + metric)


class ClusterMetric(Metric):
    """Convenience metric class for cluster-wide metrics.

    Metrics in are prefixed with the hostname, thus cluster-wide metrics are
    prefixed with '*'
    """

    def __init__(self, metric):
        """Initialize."""
        super(ClusterMetric, self).__init__('*.' + metric)


class GroupMetric(Metric):
    """Convenience metric class for grouping metrics."""

    def __init__(self, *metrics):
        """Initialize."""
        metric_strs = ','.join([metric.metric for metric in metrics])
        super(GroupMetric, self).__init__('group(%s)' % metric_strs)


if __name__ == "__main__":
    argparser = argparse.ArgumentParser(description='Graphite')
    argparser.add_argument('--ip_address', type=str)
    argparser.add_argument('--metric', type=str)
    argparser.add_argument('--function', type=str)
    argparser.add_argument('--use_tunnel', action='store_true', default=False)
    args = argparser.parse_args()

    server = MetricServer(args.ip_address, use_tunnel=args.use_tunnel)

    if args.metric:
        m = Metric(args.metric)
        if args.function:
            now = datetime.now()
            print(server.get_all_abs_series(m.apply(args.function),
                                            now - timedelta(hours=1),
                                            now))

        else:
            print(server.get_current(m))
