"""
MetricParsers for standard metric serialization formats
"""
import logging
import time
from typing import List, Dict

from nbdb.common.context import Context
from nbdb.common.telemetry import Telemetry
from nbdb.common.tracing_config import TracingConfig
from nbdb.config.settings import Settings
from nbdb.common.data_point import DataPoint, CLUSTERED_FLAT_METRIC_PREFIX, \
    TOKEN_COUNT, TOKEN_TAG_PREFIX, CLUSTER_TAG_KEY, FIELD, MEASUREMENT, \
    INTERNAL_FLAT_METRIC_PREFIX

logger = logging.getLogger()

FLAT_SERIES_FIELD_NAME = "value"
FLAT_SERIES_SEPARATOR = "."


class MetricParsers:

    def __init__(self, context: Context, protocol: str, \
            past_message_lag: int=None, future_message_lag: int=None):
        self.context = context
        self.duplicated_data_points = 0
        if protocol == 'influx':
            self.parser = self.parse_influx_metric_values
        elif protocol == 'graphite':
            self.parser = self.parse_graphite_metric_values
        elif protocol == 'auto':
            self.parser = self.parse_with_auto_protocol_detection
        else:
            assert False, "Unsupported protocol: %s" % protocol
        self.past_message_lag = past_message_lag
        self.future_message_lag = future_message_lag

    def parse(self, msg: str) -> List[DataPoint]:
        """
        Parse the message based on the defined protocol
        :return:
        """
        return self.parser(msg)

    def parse_with_auto_protocol_detection(self, msg) -> List[DataPoint]:
        """
        Detects the protocol and parses accordingly
        :param msg:
        :return: datapoints
        """
        if msg.startswith('clusters'):
            return self.parse_graphite_metric_values(msg)
        return self.parse_influx_metric_values(msg)

    def parse_influx_metric_values(self, influx_metric_line_protocol)\
            -> List[DataPoint]:
        """
        Parses the metrics serialized in influx line protocol
        see https://v2.docs.influxdata.com/v2.0/reference/syntax/line-protocol/
        Syntax
        <measurement>[,tg_k=tg_v[,tg_k=tg_v]] f_k=f_v[,f_k=f_v] [<timestamp>]
        Example
        myMeasurement,tag1=value1,tag2=value2 fieldKey="fieldValue" 15568135610
        :param influx_metric_line_protocol:
        :return: list of data points one for each field
        """
        TracingConfig.TRACE_ACTIVE = False
        parts = influx_metric_line_protocol.split(' ')
        if len(parts) != 3:
            raise SyntaxError('Invalid line protocol syntax: {}'
                              ' expected 3 parts delimited by space found {}'
                              .format(influx_metric_line_protocol, len(parts)))
        m_tag_parts = parts[0].split(',')
        measurement = m_tag_parts[0]
        tags: Dict[str, str] = self._parse_key_value_pairs(m_tag_parts[1:])
        field_values = self._parse_key_value_pairs(
            parts[1].split(','))
        # Influx epoch timestamp is always in ns, we store in seconds
        timestamp = int(int(parts[2])/1000000000)

        current_time = int(time.time())
        if timestamp <= 0:
            # No timestamp defined, replace it with server_rx_time
            timestamp = current_time

        data_points: List[DataPoint] = list()
        for field_name in field_values:
            field_value_str = field_values[field_name]
            field_value = float(field_value_str.strip('i'))
            # Treat measurement as a special tag when writing to Druid
            tags[MEASUREMENT] = measurement
            datasource = self.context.schema.get_datasource(
                measurement, tags, protocol='influx')

            data_point = DataPoint(datasource,
                                   field_name,
                                   tags,
                                   timestamp,
                                   current_time,
                                   field_value)
            data_points.append(data_point)
            field_and_tags = tags.copy()
            field_and_tags.update({FIELD: data_point.field})
            TracingConfig.TRACE_ACTIVE = TracingConfig.inst.match(
                field_and_tags)

            self.detect_and_report_clock_skew(current_time,
                                              timestamp,
                                              'parse_influx_metric_values',
                                              datasource,
                                              influx_metric_line_protocol,
                                              self.past_message_lag,
                                              self.future_message_lag)

            cluster_id = tags.get(CLUSTER_TAG_KEY)
            self.create_dashboard_datapoint(data_point,
                                            field_name,
                                            tags,
                                            cluster_id,
                                            data_points)

        return data_points

    def parse_graphite_metric_values(self,
                                     metric_line: str) -> List[DataPoint]:
        """
        Parses the metrics serialized in Graphite protocol.
        Syntax: <measurement> <value> <timestamp>
        Example: clusters.ABC.RVM123.Diamond.process.InfluxDB.uptime 20 15568135610

        Additionally we try to extract the cluster UUID and node ID as tags if the
        CDM metrics are provided as input.

        :param metric_line: String metric line received
        :return: list of DataPoint objects
        """
        TracingConfig.TRACE_ACTIVE = False

        if not metric_line:
            # Not a real message
            return
        parts = metric_line.split(' ')
        if len(parts) != 3:
            raise SyntaxError('Invalid Graphite protocol syntax: line=\'{}\''
                              ' expected 3 parts delimited by space found {}'
                              .format(metric_line, len(parts)))

        try:
            value = float(parts[1])
            timestamp = int(parts[2])
        except ValueError as exc:
            raise SyntaxError('Invalid Graphite protocol syntax: {}. '
                              'Unable to parse value and timestamp: {}'
                              .format(metric_line, exc))

        current_time = int(time.time())
        if timestamp <= 0:
            # No timestamp defined, replace it with server_rx_time
            timestamp = current_time

        field_name, tags, tokens = \
            MetricParsers.parse_graphite_series_name(parts[0])
        field = parts[0]
        cluster_id = None
        # we know how to identify cluster_id for metrics with clusters. prefix
        if parts[0].startswith(CLUSTERED_FLAT_METRIC_PREFIX):
            # First three dots contains clusters.cid.nid, skip these
            # and use the remaining dots to match against data source
            field = FLAT_SERIES_SEPARATOR.join(tokens[3:])
            cluster_id = tokens[1]
        elif parts[0].startswith(INTERNAL_FLAT_METRIC_PREFIX):
            # First three dots contains clusters.cid.nid, skip these
            # and use the remaining dots to match against data source
            field = parts[0]
            cluster_id = None

        # for all other metrics, we cannot support shard by cluster unless
        # we know how to identify the cluster_id
        datasource_name = self.context.schema.get_datasource(
            field, tags, protocol='graphite')
        data_point = DataPoint(datasource_name,
                               field_name,
                               tags,
                               timestamp,
                               current_time,
                               value)
        field_and_tags = tags.copy()
        field_and_tags.update({FIELD: data_point.field})
        TracingConfig.TRACE_ACTIVE = TracingConfig.inst.match(field_and_tags)

        self.detect_and_report_clock_skew(current_time,
                                          timestamp,
                                          'parse_graphite_metric_values',
                                          datasource_name,
                                          metric_line,
                                          self.past_message_lag,
                                          self.future_message_lag)

        data_points = [data_point]
        self.create_dashboard_datapoint(data_point,
                                        field_name,
                                        tags,
                                        cluster_id,
                                        data_points)
        return data_points

    @staticmethod
    def parse_graphite_series_name(name: str) -> \
        (str, Dict[str, str], List[str]):
        """
        Decompose graphite flat format to influx style field name
        and tags.

        Returns:
        Tuple(field_name: str, tags: dict, tokens: List[str])
        """
        # convert all the dots to tags
        tokens = name.split(FLAT_SERIES_SEPARATOR)

        tags: Dict[str, str] = dict()
        # last dot we store in field a required parameter
        for i in range(len(tokens) - 1):
            tags[TOKEN_TAG_PREFIX + str(i)] = tokens[i]
        tags[TOKEN_COUNT] = str(len(tokens))
        field_name = tokens[-1]
        return (field_name, tags, tokens)

    @staticmethod
    def detect_and_report_clock_skew(current_time: int,
                                     timestamp: int,
                                     parser_type: str,
                                     datasource_name: str,
                                     line: str,
                                     past_message_lag: int=None,
                                     future_message_lag: int=None) -> None:
        """
        Checks if the clock skew exceeds the expectation and reports
        to log and telemetry
        :param current_time:
        :param timestamp:
        :param parser_type:
        :param datasource_name:
        :param line:
        """
        datasource_name_encoded = datasource_name.replace('.', '_')
        # Don't include datasource name in the metric
        del datasource_name_encoded
        delta: int = abs(timestamp - current_time)
        if future_message_lag is not None and timestamp - current_time > future_message_lag:
            # Log the too old messages for investigation
            logger.debug('%s: FUTURE MESSAGE ALERT: %s', parser_type, line)
            Telemetry.inst.registry.meter(
                'MetricConsumer.clock_skew.future').mark()
            Telemetry.inst.registry.histogram(
                'MetricConsumer.clock_skew.future').add(delta)

        if past_message_lag is not None and current_time - timestamp > past_message_lag:
            # Log the too old messages for investigation
            logger.debug('%s: OLD MESSAGE ALERT: %s', parser_type, line)
            Telemetry.inst.registry.meter(
                'MetricConsumer.clock_skew.past').mark()
            Telemetry.inst.registry.histogram(
                'MetricConsumer.clock_skew.past').add(delta)

    @staticmethod
    def _parse_key_value_pairs(kv_pairs: List[str]) -> dict:
        """
        Parses serialized key=value pairs from the provided list
        :param kv_pairs: list of strings of form key=value
        :return: Dictionary of key=value
        """
        dictionary = dict()
        for part in kv_pairs:
            kv_parts = part.split('=')
            if len(kv_parts) != 2:
                raise SyntaxError('Expected key,value {} pair to be delimited '
                                  'by =, found {} parts expected 2'.
                                  format(part, len(kv_parts)))
            dictionary[kv_parts[0]] = kv_parts[1]
        return dictionary

    def create_dashboard_datapoint(self,
                                   data_point: DataPoint,
                                   field: str,
                                   tags: Dict[str, str],
                                   cluster_id: str,
                                   data_points: List[DataPoint]) -> None:
        """
        Checs if the datapoint should be collocated, if so then
        duplicates the query
        :param data_point:
        :param field:
        :param tags:
        :param cluster_id:
        :param data_points:
        :return:
        """
        if cluster_id is None:
            # When cluster ID is NULL, e.g. for internal metrics,
            # such datapoints is not supported in dashboard
            return
        # check if this is also collocated
        field_and_tags = dict(tags)
        field_and_tags[FIELD] = field
        dashboard_datasource = self.context.schema.get_dashboard_datasource(
            field_and_tags, cluster_id)
        if dashboard_datasource is not None:
            dashboard_datapoint = DataPoint(dashboard_datasource,
                                            data_point.field,
                                            data_point.tags,
                                            data_point.epoch,
                                            data_point.server_rx_epoch,
                                            data_point.value)
            if TracingConfig.TRACE_ACTIVE:
                logger.info('TRACE: metric_parsers.create_dashboard_datapoint '
                            'data_point %s duplicated to dashboard_queries',
                            str(data_point))
            data_points.append(dashboard_datapoint)
            self.duplicated_data_points += 1
            if self.duplicated_data_points > 1000:
                Telemetry.inst.registry.meter(
                    'MetricConsumer.duplicated_data_points'
                ).mark(self.duplicated_data_points)
                self.duplicated_data_points = 0
        else:
            if TracingConfig.TRACE_ACTIVE:
                logger.info('TRACE: metric_parsers.create_dashboard_datapoint '
                            'data_point %s is not dashboard_queries',
                            str(data_point))
