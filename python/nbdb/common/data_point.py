"""
DataPoint
"""
from __future__ import  annotations

import json
import time
from math import ceil
from typing import Optional

# tag key constants commonly used
CLUSTER_TAG_KEY = 'Cluster'
NODE_TAG_KEY = 'Node'
TOKEN_TAG_PREFIX = 'tkn'
TOKEN_COUNT = 'tkns'
MEASUREMENT = 'measurement'
FIELD = 'field'
VALUE = 'value'
DATAPOINT_VERSION = '_datapoint_version'

CLUSTERED_FLAT_METRIC_PREFIX = 'clusters'
INTERNAL_FLAT_METRIC_PREFIX = 'internal'

BAND_LOW = 'band_low'
BAND_HIGH = 'band_high'
BAND_PROB = 'band_prob'

# Special value reserved for missing datapoints
MISSING_POINT_VALUE = -1
# Special value reserved for series marked dead
TOMBSTONE_VALUE = -3

# Translated value to use if raw value is -1. We have reserved -1 for missing
# point markers. We are assuming here that a raw value of pi (3.14159) is an
# extremely rare possibility
TRANSLATED_MINUS_ONE_VALUE = -3.14159
# Translated value to use if raw value is -3. We have reserved -3 for tombstone
# markers. We are assuming here that a raw value of 3*pi (3*3.14159) is an
# extremely rare possibility.
TRANSLATED_MINUS_THREE_VALUE = -9.42477

# Integer enum values indicating the sparse algo decision
DATA_DROP = 0
DATA_WRITE = 1
DATA_FORCE_WRITE = 2

# String enum constants indicating consumer mode
# Realtime: Only generates raw data
# Rollup: Only generates rollup data
# Both: Generates both
MODE_REALTIME = "realtime"
MODE_ROLLUP = "rollup"
MODE_BOTH = "both"

MODE_RECOVERY = "recovery"

class DataPoint:
    """
    Represents the metadata associated with a metric
    A metric is represented using Influx schema of measurement, field and tags
    There are two distinct forms supported
    1) Human readable text based. Here metadata is of string type
    2) Machine optimized compressed. Here metadata is of integer type
    The same object can support both and provides generator methods
    to convert human friendly to machine optimized
    """

    # pylint: disable=too-many-arguments
    def __init__(self,
                 datasource: str,
                 field: str,
                 tags: dict,
                 epoch: int,
                 server_rx_epoch: int,
                 value: float,
                 is_special_value: bool = False,
                 series_id: Optional[str] = None
                 ) -> None:
        """
        Initialize the Metric Key object using the metadata provided
        :param datasource: <String or integer>
        :param field: <String or integer>
        :param tags: Dictionary where key,values can be string or integer
        :param epoch: epoch in seconds
        :param value: float value
        :param server_rx_epoch: seconds since epoch when the point was received
        :param is_special_value: Indicate if special reserved values are being
        used. Eg. We reserve -1 and -3 for missing point & tombstone markers
        :param series_id: Precomputed series_id. Optional
        """
        self.datasource = datasource
        self.field = field
        self.tags = tags
        self.epoch = epoch
        if not is_special_value:
            # Need to normalize in the usual case to ensure we don't end up
            # using any special reserved values.
            self.value = DataPoint.normalize_value(value)
        else:
            # Special values need to be stored as is, without normalization
            self.value = value
        # generated fields
        # only valid for data_points that are transformed
        self.pre_transform_value = None
        self.server_rx_epoch = server_rx_epoch
        if series_id:
            self.series_id = series_id
        else:
            self.series_id = DataPoint.generate_series_id(
                datasource, tags, field)

    def adjust_epoch(self, min_storage_interval: int = 1):
        """
        :param min_storage_interval: min granularity at which data points are
        stored
        """
        self.epoch = (int(ceil(self.epoch/min_storage_interval)) *
                      min_storage_interval)

    @staticmethod
    def generate_series_id(datasource: str, tags: dict, field: str) -> str:
        """Generate series id for the datasource."""
        series_id = datasource
        for tag_key in sorted(tags.keys()):
            if tag_key == DATAPOINT_VERSION:
                # DATAPOINT_VERSION is a tag we never want to include in the
                # series ID. This is because on the read side, we aggregate
                # datapoints per unique series ID. By excluding
                # DATAPOINT_VERSION from the series ID, we ensure that
                # different bundle datapoints for the same series are all
                # processed together.
                continue
            series_id = series_id + \
                             '|' + tag_key + '=' + str(tags[tag_key])
        series_id += '|' + field
        return series_id

    def flat_field(self) -> str:
        """
        Flattened field name from read api perspective
        :return:
        """
        return self.datasource + '.' + self.field

    def to_druid_json_str(self) -> str:
        """
        Creates a json string representation, this is cheaper than calling
        json.dumps
        :return:
        """
        druid_json_str = '{"time":' + str(self.epoch)
        if DATAPOINT_VERSION in self.tags:
            # If DATAPOINT_VERSION is available, print it right after epoch
            druid_json_str += ',"' + DATAPOINT_VERSION + '":' + \
                str(self.tags[DATAPOINT_VERSION])
            # Filter it out so that we don't generate bundle_version a second
            # time
            tags_items = [(k, v) for (k, v) in self.tags.items()
                          if k != DATAPOINT_VERSION]
        else:
            tags_items = self.tags.items()

        druid_json_str += (',"' + VALUE + '":' + str(self.value) +
                           ',"' + FIELD + '":"' + self.field + '"')

        if len(self.tags) > 0:
            druid_json_str += ','
            druid_json_str += ','.join(['"' + i[0] + '":"' + i[1] + '"' for i
                                        in tags_items]) + '}'
        else:
            # It's possible there are no tags. Eg. Graphite flat metrics
            druid_json_str += '}'

        return druid_json_str

    def __str__(self) -> str:
        """
        Returns a string representation of the stats object
        :return:
        """
        # Denormalize & then return
        dictionary = self.__dict__.copy()
        dictionary[VALUE] = DataPoint.denormalize_value(self.value)
        return json.dumps(dictionary)

    def __repr__(self):
        return str(self)

    @staticmethod
    def from_series_id(series_id: str, epoch: int, value: float,
                       is_special_value: bool = False,
                       datapoint_version: int = None) -> DataPoint:
        """
        Create a datapoint from metadata encoded in series_id
        :param series_id:
        :param epoch:
        :param value:
        :param is_special_value:
        :param datapoint_version:
        :return: DataPoint
        """
        parts = series_id.split('|')
        if len(parts) < 2:
            raise ValueError('Unable to create Datapoint from malformed '
                             'series_id {}'.format(series_id))
        datasource = parts[0]
        field = parts[-1]
        tags = dict()
        for i in range(1, len(parts)-1):
            # maxsplit=1 is needed because some metrics can contain "="
            # character in their names.
            # Eg. "clusters.<CID>.<NID>.SprayServer.Performance.Authorization.
            # HierarchyProvider.MVID=ManagedVolume.HierarchyContaining.
            # LoadTimeMs.p95"
            # has the token "MVID=ManagedVolume"
            tag_parts = parts[i].split('=', maxsplit=1)
            if len(tag_parts) != 2:
                raise ValueError('Unable to create Datapoint from malformed '
                                 'series_id {} tag_part {}'.format(series_id,
                                                                   parts[i]))
            tags[tag_parts[0]] = tag_parts[1]

        if datapoint_version is not None:
            # DATAPOINT_VERSION is not included in the series ID but included in
            # the tags. If the caller has specified DATAPOINT_VERSION, explicitly
            # add it.
            tags[DATAPOINT_VERSION] = datapoint_version

        return DataPoint(datasource,
                         field,
                         tags,
                         epoch,
                         int(time.time()),
                         value,
                         is_special_value=is_special_value,
                         series_id=series_id)

    @staticmethod
    def datasource_from_series_id(series_id: str) -> str:
        """
        Extract the datasource name from the series_id
        :param series_id: series id created by datapoint
        :return: datasource
        """
        parts = series_id.split('|')
        if len(parts) < 2:
            raise ValueError('Unable to get datasource from malformed '
                             'series_id {}'.format(series_id))
        datasource = parts[0]
        return datasource

    def get_cluster_id(self) -> str:
        """
        Checks if the cluster id is specified in the tags
        :return: cluster id or None
        """
        # identify the cluster id
        if CLUSTER_TAG_KEY in self.tags:
            return self.tags[CLUSTER_TAG_KEY]
        # check if its a flat metric
        TOKEN_0_KEY = TOKEN_TAG_PREFIX + str(0)
        if TOKEN_0_KEY in self.tags and \
            self.tags[TOKEN_0_KEY] == CLUSTERED_FLAT_METRIC_PREFIX:
            TOKEN_1_KEY = TOKEN_TAG_PREFIX + str(1)
            return self.tags[TOKEN_1_KEY]
        # No cluster id in the datapoint
        return None

    @staticmethod
    def normalize_value(value: float) -> float:
        """
        Convert value to a different value incase any special reserved values
        are being used. That way it can be safely written to the
        backend, without causing confusion.

        For example, we reserve -1 & -3 to indicate missing point & tombstone
        markers. If the raw value is -1 or -3, we transform it to something
        else.

        :param value: original value that should be normalized
        :return: normalied value that can be safely written to druid
        """
        # XXX: Commenting out translation of zero values. Not sure if it is
        # needed anymore.
        #
        # NOTE: druid doesn't know how to distinguish null from 0
        # since sparse metrics generate lot of nulls, its important
        # we map valid 0 values to another number so that we have
        # an unambiguous representation of 0=null in druid
        # if value == 0:
        #   return ZERO_VALUE

        if value == -1:
            # -1 is a special value reserved for missing point markers. Convert
            # to another value so that we don't get confused
            return TRANSLATED_MINUS_ONE_VALUE
        if value == -3:
            # -3 is a special value reserved for tombstone markers. Convert to
            # another value so that we don't get confused
            return TRANSLATED_MINUS_THREE_VALUE
        return value

    @staticmethod
    def denormalize_value(value: float) -> float:
        """
        See the comments in normalize, this does the reverse mapping
        :param value:
        :return:
        """
        if value == TRANSLATED_MINUS_ONE_VALUE:
            return -1
        if value == TRANSLATED_MINUS_THREE_VALUE:
            return -3
        return value
