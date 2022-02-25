"""
BatchJsonParser
"""
import fnmatch
import json
import logging
import os
from dataclasses import dataclass
from typing import Callable, Dict, List, Optional, Tuple
import gzip

from nbdb.common.data_point import DATAPOINT_VERSION, DataPoint
from nbdb.common.metric_parsers import MetricParsers
from nbdb.common.metric_parsers import FLAT_SERIES_FIELD_NAME
from nbdb.common.metric_parsers import FLAT_SERIES_SEPARATOR
import yaml

logger = logging.getLogger()

TIMESTAMP_KEY = "timestamp"
FIELD_KEY = "field"
VALUE_KEY = "value"
TAGS_KEY = "tags"
NAME_KEY = "name"

GZ_SUFFIX = ".gz"


@dataclass
class LastSeriesCache:
    """Cached properties of the last seen flat series"""
    series_id : str
    datasource: str
    tags: Dict[str, str]
    field_name: str

# We don't create a dataclass here to minimize copies
class JsonDatapoint: # pylint: disable=missing-function-docstring
    """
    Parsed json object.
    """
    def __init__(self, raw_dict: str):
        self.raw_dict = raw_dict

    @property
    def name(self) -> str:
        return self.raw_dict[NAME_KEY]

    @property
    def field(self) -> str:
        return self.raw_dict[FIELD_KEY]

    @property
    def value(self) -> float:
        return float(self.raw_dict[VALUE_KEY])

    @property
    def tags(self) -> dict:
        return self.raw_dict[TAGS_KEY]

    @property
    def timestamp_ns(self) -> int:
        return int(self.raw_dict[TIMESTAMP_KEY])


class BatchJsonParser:
    """
    Parser for the anomalyDB batch ingest file format.

    Usage:
    ```parser = JsonParser(...)
    with parser:
        for dp in parser:
            ...
    ```
    Note that `with` is needed so that we clean up the file descriptor
    after it is used.

    The file must contain newline separated serialized JSON objects
    with the following fields:
    ```
        {
            "name": "<string>",
            "timestamp": <nanoseconds since epoch>,
            "field": "<field name>",
            "value": <double value>,
            "tags": [{"key": "<string>", "value": "<string>"}]
        }
    ```
    """
    # pylint: disable=too-many-instance-attributes
    def __init__(
            self,
            datasource_getter_fn: Callable[[str, Dict[str, str], str], str],
            min_storage_interval: int,
            cluster_id: str,
            filepath: str,
            batch_filter_file: str,
            bundle_creation_time: int):
        """
        :param filepath: Absolute path of the batch ingest file.
        :param batch_filter_file: Absolute path of the batch filter rules file.
        :param bundle_creation_time: Seconds since epoch when bundle was
        created.
        :param datasource_getter_fn:
               Function which takes in (field, tags, protocol) and returns
               a datasource name.
        """
        if not os.path.isfile(filepath):
            raise OSError(f"File {filepath} doesn't exist")
        if not os.path.isfile(batch_filter_file):
            raise OSError(f"File {batch_filter_file} doesn't exist")
        self.filepath = filepath
        self.batch_filter_file = batch_filter_file
        self.use_gzip = filepath.endswith(GZ_SUFFIX)
        self.datasource_getter_fn = datasource_getter_fn
        self.min_storage_interval = min_storage_interval
        self.cluster_id = cluster_id
        self.bundle_creation_time = bundle_creation_time
        self.file = None
        # Total number of datapoints that could not be parsed in the file
        self.num_skipped = 0
        # Number of flat series parsed in the file
        self.num_flat = 0
        # Number of non-flat series parsed in the file
        self.num_tagged = 0
        # Tuple of current flat series name to series cache
        self.last_flat_series_cache: Tuple[str, LastSeriesCache] = (None, None)

        # Filter rules indexed by protocol
        self.filter_rules: Dict[str, List] = {}
        # Cached results of filter rule match operations
        self.filter_rules_result_cache: Dict[str, bool] = {}

    def __enter__(self):
        if self.use_gzip:
            self.file = gzip.open(self.filepath, 'rt')
        else:
            self.file = open(self.filepath, 'r')
        self.num_skipped = 0
        self.num_flat = 0
        self.num_tagged = 0
        self._parse_batch_filter_rules()

    def __exit__(self, typ, value, traceback):
        if self.file:
            self.file.close()
            self.file = None

    def __iter__(self):
        return self

    def __next__(self) -> DataPoint:
        """
        Fetch the next DataPoint from the backing JSON file.

        For now, only graphite style flate metrics are supported and
        native influx style metrics are skipped.
        """
        if not self.file:
            raise Exception("Backing file not initialized.")
        while True:
            line = self.file.readline()
            if not line:
                raise StopIteration
            line_dict = json.loads(line)
            json_dp = JsonDatapoint(line_dict)
            data_point = self._extract_datapoint(json_dp)
            if not data_point:
                logger.debug("Could not parse data point %s", json_dp)
                self.num_skipped += 1
                continue
            return data_point

    def _parse_batch_filter_rules(self):
        """
        Parse the batch filter rules
        """
        with open(self.batch_filter_file, "r") as rules_file:
            self.filter_rules = yaml.safe_load(rules_file)

    def _match_filter_rules(self, metric_name: str, protocol: str) -> bool:
        """
        Check if the given metric is allowed by blacklist / whitelist rules.

        :param metric_name: Metric name. For Graphite, this is the metric name
        without the 'clusters.<CID>.<NID>' prefix. Skipping the prefix makes
        the matching run faster
        :param protocol: graphite or influx.
        :return: Boolean indicating whether the blacklist & whitelist rules
        allow the metric
        """
        # Try checking in cache first
        if metric_name in self.filter_rules_result_cache:
            return self.filter_rules_result_cache[metric_name]

        rules = self.filter_rules[protocol]

        # First try going over blacklist rules
        for rule in rules['blacklist_rules']:
            if fnmatch.fnmatch(metric_name, rule):
                self.filter_rules_result_cache[metric_name] = False
                return False

        # If none of the blacklist rules matched, try matching the whitelist
        # rules
        result = False
        for rule in rules['whitelist_rules']:
            if fnmatch.fnmatch(metric_name, rule):
                result = True
                break

        self.filter_rules_result_cache[metric_name] = result
        return result

    def _extract_datapoint(self, json_dp: JsonDatapoint) -> \
        Optional[DataPoint]:
        """
        Create a DataPoint object.
        """
        # pylint: disable=no-else-return
        if BatchJsonParser._is_flat_series(json_dp):
            self.num_flat += 1
            return self._extract_flat_datapoint(json_dp)
        else:
            self.num_tagged += 1
            return None

    def _extract_flat_datapoint(self, json_dp: JsonDatapoint) -> DataPoint:
        """
        Create a DataPoint object from a flat graphite style raw datapoint.
        """
        # We store in seconds
        epoch = json_dp.timestamp_ns / 1000000000

        # Converting flat graphite series name to tagged schema in expensive,
        # and so in creating the series id in DataPoint.__init__, so we reuse
        # these values if the flat series name remains the same
        if self.last_flat_series_cache[0] != json_dp.name:
            # We expect series of the form `clusters.$clusterId.$nodeId.*`, but
            # series names extracted from influx backups are of the form
            # `$nodeId.*`, so we append `clusters.$clusterId`
            series_name = f"clusters.{self.cluster_id}.{json_dp.name}"
            field_name, tags, tokens = \
                MetricParsers.parse_graphite_series_name(series_name)
            # series_name is of the form clusters.$clusterId.$node_id.A.B.count,
            # so we remove `clusters.$clusterId.$nodeId` before we pass it to
            # datasource_getter_fn
            series_name_wo_node_id = '.'.join(tokens[3:])

            # Check if the series needs to be filtered
            if not self._match_filter_rules(series_name_wo_node_id,
                                            protocol="graphite"):
                return None

            datasource = self.datasource_getter_fn(series_name_wo_node_id,
                                                   tags, protocol='graphite')
            # Add the bundle creation timestamp as an extra tag. This will be
            # used for versioning and invalidating tombstones later on the read
            # side
            tags[DATAPOINT_VERSION] = self.bundle_creation_time

            series_id = DataPoint.generate_series_id(datasource,
                                                     tags,
                                                     field_name)

            self.last_flat_series_cache = (
                json_dp.name,
                LastSeriesCache(
                    datasource=datasource,
                    field_name=field_name,
                    series_id=series_id,
                    tags=tags
                )
            )

        series_cache = self.last_flat_series_cache[1]
        data_point = DataPoint(
            datasource=series_cache.datasource,
            field=series_cache.field_name,
            tags=series_cache.tags,
            epoch=epoch,
            # Since this is historical data, we don't have a notion of
            # server_rx_epoch. Instead, we use the datapoint time itself
            # as the server_rx_epoch.
            server_rx_epoch=epoch,
            value=json_dp.value,
            series_id=series_cache.series_id
        )
        data_point.adjust_epoch(self.min_storage_interval)
        return data_point

    @staticmethod
    def _is_flat_series(json_dp: JsonDatapoint) -> bool:
        """
        Whether a series is a flat graphite style series.

        Flat series are of the form:
        {
            "name":"vm-machine-opp4oy-nyne5zw.AgentServerMain.Timer.1m.count",
            "timestamp":1598348245000000000,
            "field":"value",
            "value":0,
            "tags":[]
            }
        """
        return len(json_dp.tags) == 0 and \
                FLAT_SERIES_SEPARATOR in json_dp.name and \
                json_dp.field == FLAT_SERIES_FIELD_NAME
