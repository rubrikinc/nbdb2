"""
Unit tests for MetricConsumer
"""
import os
import unittest
from unittest import mock

from nbdb.common.context import Context
from nbdb.common.data_point import CLUSTER_TAG_KEY, NODE_TAG_KEY
from nbdb.common.data_point import MODE_BOTH, MODE_REALTIME, MODE_ROLLUP
from nbdb.common.telemetry import Telemetry
from nbdb.common.tracing_config import TracingConfig
from nbdb.schema.schema import Schema

from nbdb.common.thread_pools import ThreadPools
from nbdb.config.settings import Settings
from nbdb.metric_consumer.metric_consumer import MetricConsumer
from nbdb.metric_consumer.recovery_consumer import RecoveryConsumer
from nbdb.string_table.identity_string_table import IdentityStringTable

from nbdb.store.sparse_series_stats import SparseSeriesStats
from nbdb.store.recovery_series_stats import RecoverySeriesStats

# pylint: disable-msg=E0611  # No Name In Module
from lru import LRU


class TestMetricConsumer(unittest.TestCase):
    """
    Basic Single Threaded test for MetricConsumer
    This does not test for concurrency or persistence

    """

    def setUp(self):
        """
        Initialize the MetricConsumer
        :return:
        """
        schema = Schema.load_from_file(
            os.path.dirname(__file__) + '/test_schema.yaml')
        self.context = Context(schema=schema)
        Settings.load_yaml_settings(os.path.dirname(__file__) +
                                    '/test_settings.yaml')
        if TracingConfig.inst is None:
            TracingConfig.initialize(Settings.inst.tracing_config)
        if Telemetry.inst is None:
            Telemetry.initialize()

        ThreadPools.instantiate()
        self.mock_sp_writer = mock.MagicMock()
        self.string_table = IdentityStringTable()
        self.consumer = None
        self.offset_count = {}

    def tearDown(self) -> None:
        """
        Start graceful shutdown of all threadpools
        :return:
        """
        ThreadPools.inst.stop()

    def start_consumer(self, mode=MODE_BOTH):
        """
        Start MetricConsumer
        """
        self.consumer = MetricConsumer(self.context,
                                       Settings.inst.metric_consumer,
                                       self.mock_sp_writer,
                                       mode)
        # Store some dummy committed offsets: partition A with committed offset
        # of zero, meaning 0 messages were processed by the old consumer
        self.consumer.last_consumer_committed_offsets['A'] = 0
        self.offset_count['A'] = 0

    def create_kafka_msg(self, str_msg, partition='A', offset=None):
        """
        Create Kakfa-like message object with partition & offset
        """
        if offset is None:
            if partition not in self.offset_count:
                self.offset_count[partition] = 0
            self.offset_count[partition] += 1
            offset = self.offset_count[partition]

        return (str_msg, partition, offset)

    def test_process_influx_metric(self):
        """
        Checks that process_item() processes Influx metrics correctly
        :return:
        """
        Settings.inst.metric_consumer.metric_protocol = 'influx'
        self.start_consumer()

        # TEST1: Influx metric with no whitelist
        msg = 'diamond.process,{}=ABC,{}=RVM123 cpu_perc=12 100000000000'.\
            format(CLUSTER_TAG_KEY, NODE_TAG_KEY)
        self.consumer.process_item(*self.create_kafka_msg(msg))
        self.assertEqual(self.mock_sp_writer.append_metrics.call_count, 1)
        self.mock_sp_writer.reset_mock()

        # TEST2: Influx metric with whitelist rule and matching entry
        Settings.inst.metric_consumer.whitelist_cluster_uuid = 'ABC'
        self.consumer.process_item(*self.create_kafka_msg(msg))
        self.assertEqual(self.mock_sp_writer.append_metrics.call_count, 1)
        self.mock_sp_writer.reset_mock()

        # TEST3: Influx metric with whitelist rule and non-matching entry
        Settings.inst.metric_consumer.whitelist_cluster_uuid = 'gandalf'
        self.consumer.process_item(*self.create_kafka_msg(msg))
        self.assertEqual(self.mock_sp_writer.append_metrics.call_count, 0)
        self.mock_sp_writer.reset_mock()

        # Reset
        Settings.inst.metric_consumer.whitelist_cluster_uuid = None

    def test_process_graphite_metric(self):
        """
        Checks that process_item() processes Graphite metrics correctly
        :return:
        """
        Settings.inst.metric_consumer.metric_protocol = 'graphite'
        Settings.inst.metric_consumer.whitelist_cluster_uuid = None
        self.start_consumer()

        # TEST1: Graphite metric with no whitelist
        msg = 'clusters.ABC.RVM123.diamond.process.cpu_perc 12 100000000000'
        self.consumer.process_item(*self.create_kafka_msg(msg))
        self.assertEqual(self.mock_sp_writer.append_metrics.call_count, 1)
        self.mock_sp_writer.reset_mock()

        # TEST2: Graphite metric with whitelist rule and matching entry
        Settings.inst.metric_consumer.whitelist_cluster_uuid = 'ABC'
        self.consumer.process_item(*self.create_kafka_msg(msg))
        self.assertEqual(self.mock_sp_writer.append_metrics.call_count, 1)
        self.mock_sp_writer.reset_mock()

        # TEST3: Graphite metric with whitelist rule and non-matching entry
        Settings.inst.metric_consumer.whitelist_cluster_uuid = 'gandalf'
        self.consumer.process_item(*self.create_kafka_msg(msg))
        self.assertEqual(self.mock_sp_writer.append_metrics.call_count, 0)
        self.mock_sp_writer.reset_mock()

        # Cleanup
        Settings.inst.metric_consumer.whitelist_cluster_uuid = None

    def test_replay_mode(self):
        """
        Checks that process_item() correctly detects whether messages are being
        reprocessed or if they are new
        :return:
        """
        # Choose any protocol
        Settings.inst.metric_consumer.metric_protocol = 'graphite'
        self.start_consumer()

        # Mimic committed offset for partition A and B to be 1 and 3. This
        # means the first message on partition A and first 3 messages on
        # partition B should be treated as if they are being replayed.
        self.consumer.last_consumer_committed_offsets['A'] = 1
        self.consumer.last_consumer_committed_offsets['B'] = 3

        # TEST1: Send 1 message on partition A and 3 messages on partition B.
        # They should be treated as replayed messages
        msg = 'clusters.ABC.%s.diamond.process.cpu_perc 12 100000000000'
        self.consumer.process_item(
            *self.create_kafka_msg(msg % '1', partition='A'))
        self.consumer.process_item(
            *self.create_kafka_msg(msg % '2', partition='B'))
        self.consumer.process_item(
            *self.create_kafka_msg(msg % '3', partition='B'))
        self.consumer.process_item(
            *self.create_kafka_msg(msg % '4', partition='B'))
        self.assertEqual(self.consumer.non_replay_items_received, 0)
        replay_mode_msgs = [call for call in
                            self.mock_sp_writer.append_metrics.mock_calls
                            if call[1][1]]
        self.assertEqual(len(replay_mode_msgs), 4)
        self.mock_sp_writer.reset_mock()

        # TEST2: Send one more message each for partition A and B. These should
        # be treated as new messages
        self.consumer.process_item(
            *self.create_kafka_msg(msg % '5', partition='A'))
        self.consumer.process_item(
            *self.create_kafka_msg(msg % '6', partition='B'))
        self.assertEqual(self.consumer.non_replay_items_received, 2)
        replay_mode_msgs = [call for call in
                            self.mock_sp_writer.append_metrics.mock_calls
                            if call[1][1]]
        self.assertEqual(len(replay_mode_msgs), 0)

    def test_realtime_and_rollup_replay_mode(self):
        """
        Checks that the realtime consumer doesn't support replay mode
        processing and the rollup consumer does.
        :return:
        """
        # Choose any protocol
        Settings.inst.metric_consumer.metric_protocol = 'graphite'
        for mode in (MODE_REALTIME, MODE_ROLLUP):
            self.start_consumer(mode=mode)

            # Mimic committed offset for partition A and B to be 1 and 3. These
            # should be ignored for the realtime consumer
            self.offset_count.clear()
            self.consumer.last_consumer_committed_offsets['A'] = 1
            self.consumer.last_consumer_committed_offsets['B'] = 3

            # TEST1: Send 1 message on partition A and 3 messages on partition B.
            # They should be treated as replayed messages for rollup consumer
            # and regular messages for realtime consumer
            msg = 'clusters.ABC.%s.diamond.process.cpu_perc 12 100000000000'
            self.consumer.process_item(
                *self.create_kafka_msg(msg % '1', partition='A'))
            self.consumer.process_item(
                *self.create_kafka_msg(msg % '2', partition='B'))
            self.consumer.process_item(
                *self.create_kafka_msg(msg % '3', partition='B'))
            self.consumer.process_item(
                *self.create_kafka_msg(msg % '4', partition='B'))
            replay_mode_msgs = [call for call in
                                self.mock_sp_writer.append_metrics.mock_calls
                                if call[1][1]]
            if mode == MODE_REALTIME:
                # For realtime consumer, all messages should be considered
                # non-replay messages
                self.assertEqual(self.consumer.non_replay_items_received, 4)
                self.assertEqual(len(replay_mode_msgs), 0)
            else:
                # For rollup consumer, all messages should be considered as
                # replay messages
                self.assertEqual(self.consumer.non_replay_items_received, 0)
                self.assertEqual(len(replay_mode_msgs), 4)

            self.mock_sp_writer.reset_mock()

    def test_merge_stats_dict(self):
        """
        Check if merge_stats_dict generates markers or updates
        the realtime_stats_cache
        """
        Settings.inst.metric_consumer.metric_protocol = 'influx'
        self.start_consumer()

        realtime_stats_cache = LRU(10)
        recovery_stats_cache = LRU(10)

        # expect a missing point value at epoch of 130
        stats_key = 'datasource|key=val|first'
        realtime_stats = SparseSeriesStats()
        realtime_stats.set_first_epoch(150)
        realtime_stats.set_refresh_epoch(160)
        realtime_stats.set_server_rx_time(160)
        realtime_stats.set_crosscluster_shard('RT')
        realtime_stats_cache[stats_key] = realtime_stats
        recovery_stats = RecoverySeriesStats()
        recovery_stats.set_refresh_epoch(10)
        recovery_stats.set_server_rx_time(10)
        recovery_stats.set_crosscluster_shard('RC')
        recovery_stats_cache[stats_key] = recovery_stats

        # internal is not large enough, thus no missing point
        stats_key = 'datasource|key=val|second'
        realtime_stats = SparseSeriesStats()
        realtime_stats.set_first_epoch(100)
        realtime_stats.set_refresh_epoch(130)
        realtime_stats.set_server_rx_time(130)
        realtime_stats.set_crosscluster_shard('RT')
        realtime_stats_cache[stats_key] = realtime_stats
        recovery_stats = RecoverySeriesStats()
        recovery_stats.set_refresh_epoch(20)
        recovery_stats.set_server_rx_time(20)
        recovery_stats.set_crosscluster_shard('RC')
        recovery_stats_cache[stats_key] = recovery_stats

        # realtime_stats exists, recovery_stats does not exist
        stats_key = 'datasource|key=val|third'
        realtime_stats = SparseSeriesStats()
        realtime_stats.set_first_epoch(110)
        realtime_stats.set_refresh_epoch(140)
        realtime_stats.set_server_rx_time(140)
        realtime_stats.set_crosscluster_shard('RT')
        realtime_stats_cache[stats_key] = realtime_stats

        # recovery_stats exists, realtime_stats does not exist
        stats_key = 'datasource|key=val|fourth'
        recovery_stats = RecoverySeriesStats()
        recovery_stats.set_refresh_epoch(30)
        recovery_stats.set_server_rx_time(30)
        recovery_stats.set_crosscluster_shard('RC')
        recovery_stats_cache[stats_key] = recovery_stats

        marker_list = self.consumer.merge_stats_dict(realtime_stats_cache, \
                                                    recovery_stats_cache)

        self.assertEqual(len(marker_list), 1)

        self.assertEqual(marker_list[0].field, 'first')
        self.assertEqual(marker_list[0].value, -1)
        self.assertEqual(marker_list[0].epoch, 130)

        self.assertEqual(
            realtime_stats_cache['datasource|key=val|first']\
                                .get_refresh_epoch(), 160)
        self.assertEqual(
            realtime_stats_cache['datasource|key=val|first']\
                                .get_server_rx_time(), 160)
        self.assertEqual(
            realtime_stats_cache['datasource|key=val|first']\
                                .get_crosscluster_shard(), 'RT')

        self.assertEqual(
            realtime_stats_cache['datasource|key=val|second']\
                                .get_refresh_epoch(), 130)
        self.assertEqual(
            realtime_stats_cache['datasource|key=val|second']\
                                .get_server_rx_time(), 130)
        self.assertEqual(
            realtime_stats_cache['datasource|key=val|second']\
                                .get_crosscluster_shard(), 'RT')

        self.assertEqual(
            realtime_stats_cache['datasource|key=val|third']\
                                .get_refresh_epoch(), 140)
        self.assertEqual(
            realtime_stats_cache['datasource|key=val|third']\
                                .get_server_rx_time(), 140)
        self.assertEqual(
            realtime_stats_cache['datasource|key=val|third']\
                                .get_crosscluster_shard(), 'RT')

        self.assertEqual(
            realtime_stats_cache['datasource|key=val|fourth']\
                                .get_refresh_epoch(), 30)
        self.assertEqual(
            realtime_stats_cache['datasource|key=val|fourth']\
                                .get_server_rx_time(), 30)
        # inherit shard RC from the recovery stats
        self.assertEqual(
            realtime_stats_cache['datasource|key=val|fourth']\
                                .get_crosscluster_shard(), 'RC')

    def test_is_partition_processable(self):
        """
        Checks that process_item() correctly detects whether the recovery
        consumer is terminated.
        :return:
        """
        consumer = RecoveryConsumer(self.context,
                                       Settings.inst.metric_consumer,
                                       self.mock_sp_writer)
        first_partition = 'first'
        second_partition = 'second'
        consumer.partition_recovery_in_progress = {}
        consumer.partition_recovery_in_progress[first_partition] = True
        consumer.partition_recovery_in_progress[second_partition] = True
        consumer.active_partition_count = 2

        consumer.partition_end_offsets[first_partition] = 3
        consumer.partition_end_offsets[second_partition] = 5

        self.assertEqual( \
                consumer.is_partition_processable(first_partition, 1), \
                True)
        self.assertEqual( \
                consumer.partition_recovery_in_progress[first_partition], \
                True)
        self.assertEqual( \
                consumer.partition_recovery_in_progress[second_partition], \
                True)
        self.assertEqual(consumer.active_partition_count, 2)
        self.assertEqual(consumer.terminate, False)

        self.assertEqual( \
                consumer.is_partition_processable(first_partition, 2), \
                False)
        self.assertEqual( \
                consumer.partition_recovery_in_progress[first_partition], \
                False)
        self.assertEqual( \
                consumer.partition_recovery_in_progress[second_partition], \
                True)
        self.assertEqual(consumer.active_partition_count, 1)
        self.assertEqual(consumer.terminate, False)


        self.assertEqual( \
                consumer.is_partition_processable(second_partition, 3), \
                True)
        self.assertEqual( \
                consumer.partition_recovery_in_progress[first_partition], \
                False)
        self.assertEqual( \
                consumer.partition_recovery_in_progress[second_partition], \
                True)
        self.assertEqual(consumer.active_partition_count, 1)
        self.assertEqual(consumer.terminate, False)

        self.assertEqual( \
                consumer.is_partition_processable(second_partition, 4), \
                False)
        self.assertEqual( \
                consumer.partition_recovery_in_progress[first_partition], \
                False)
        self.assertEqual( \
                consumer.partition_recovery_in_progress[second_partition], \
                False)
        self.assertEqual(consumer.active_partition_count, 0)
        self.assertEqual(consumer.terminate, True)

