"""
Unittest for SparseSeriesData class
"""

import os
import time
from typing import List
from unittest import TestCase
from unittest.mock import Mock

from nbdb.common.context import Context
from nbdb.common.data_point import DataPoint
from nbdb.common.data_point import MISSING_POINT_VALUE, TOMBSTONE_VALUE
from nbdb.common.telemetry import Telemetry
from nbdb.common.tracing_config import TracingConfig
from nbdb.config.settings import Settings
from nbdb.schema.schema import Schema
from nbdb.store.recovery_series_writer import RecoverySeriesWriter


class TestRecoverySeriesWriter(TestCase):
    """
    Test the write path by writing data and verifying that only sparse
    points that conform to appropriate smoothing are written to the db
    """

    def setUp(self) -> None:
        """
        Setup a partitioned mocked sparse-time-series for testing
        :return:
        """
        Settings.load_yaml_settings(os.path.dirname(__file__) +
                                    '/test_settings.yaml')
        schema = Schema.load_from_file(
            os.path.dirname(__file__) + '/test_schema.yaml')
        self.context = Context(schema=schema)
        Telemetry.inst = Mock()
        TracingConfig.inst = Mock()
        self.recovery_series_writer = None
        self.create_recovery_series_writer()
        self._unit_test_real_time_clock = 0
        self.exploration_ds = schema.exploration_datasources.get(
            Settings.inst.realtime_metric_consumer.metric_protocol)

    def create_recovery_series_writer(self):
        """
        Create RecoverySeriesWriter.
        """
        self.recovery_series_writer = RecoverySeriesWriter(
            sparse_store=Mock(),
            sparse_store_settings=Settings.inst.sparse_store,
            tracing_config_settings=Settings.inst.tracing_config,
            context=self.context,
            sparse_telemetry_source_id="test"
        )

    def test_append_metric_cc(self) -> None:
        """
        Test writing a metric to recovery_series_writer, the data points should also
        be sharded by metric name
        """
        dps = [
            DataPoint('m', 'f', {'tkn3' : 'service_1', 'tkn4' :
                                 'Fileset'}, 10, int(time.time()), 10000),
            DataPoint('m', 'f', {'tkn3' : 'service_1', 'tkn4' :
                                 'Fileset'}, 6020, int(time.time()), 10200),
            DataPoint('m', 'f', {'tkn3' : 'service_1', 'tkn4' :
                                 'Fileset'}, 6030, int(time.time()), 10201),
            DataPoint('m', 'f', {'tkn3' : 'service_1', 'tkn4' :
                                 'Fileset'}, 18040, int(time.time()), 10401)
            ]

        for dp in dps:
            self.recovery_series_writer.append(dp, False)

        # Two markers will be written for the original series. 2 additional
        # marker should be written because it matches a cross-cluster pattern.
        self.assertEqual(
            self.recovery_series_writer.sparse_store.write.call_count, 4)

    def test_sparseness_disabled_metric(self) -> None:
        """
        Test logic for metrics matching one of the sparseness disabled
        patterns.
        """
        # We generate 2 datapoints for 2 unique series, one of which also
        # matches one of the cross-cluster patterns
        dps = [
            # Series 1 which matches both a sparseness disabled pattern and a
            # cross-cluster pattern
            DataPoint('m', '01',
                      {'tkn3' : 'Diamond', 'tkn4' : 'loadavg'},
                      10, 10, 10000),
            DataPoint('m', '01',
                      {'tkn3' : 'Diamond', 'tkn4' : 'loadavg'},
                      20, 20, 10000),
            # Series 2 which matches only a sparseness disabled pattern
            DataPoint('m', 'seconds',
                      {'tkn3' : 'Diamond', 'tkn4' : 'uptime'},
                      30, 30, 20000),
            DataPoint('m', 'seconds',
                      {'tkn3' : 'Diamond', 'tkn4' : 'uptime'},
                      40, 40, 20000),
        ]

        for dp in dps:
            self.recovery_series_writer.append(dp, False)

        # Make sure we correctly determined that sparseness was disabled for
        # both series
        for stat in self.recovery_series_writer.stats_cache.values():
            self.assertTrue(stat.is_sparseness_disabled())

        # We should have zero point written for both series
        self.assertEqual(
            self.recovery_series_writer.sparse_store.write.call_count, 0)

        # Finally we simulate a heartbeat scan. Both series should be removed
        # from the stats cache but no tombstones should be generated
        self.recovery_series_writer.heartbeat_scan()
        self.assertEqual(len(self.recovery_series_writer.stats_cache), 0)
        # Write count should remain the same
        self.assertEqual(
            self.recovery_series_writer.sparse_store.write.call_count, 0)

    def test_append_metric(self) -> None:
        """
        Test writing a metric to recovery_series_writer
        """
        dps = [
            DataPoint('m', 'f', {}, 10, int(time.time()), 10000),
            DataPoint('m', 'f', {}, 6020, int(time.time()), 10200),
            DataPoint('m', 'f', {}, 6030, int(time.time()), 10201),
            DataPoint('m', 'f', {}, 18040, int(time.time()), 10401)
            ]

        # recovery series writer does not write the raw data point
        self.recovery_series_writer.append(dps[0], False)
        expected_size = 0
        self.assertEqual(
            self.recovery_series_writer.sparse_store.write.call_count, \
            expected_size)

        # the epoch gap is large, so a missing_value marker is written
        self.recovery_series_writer.append(dps[1], False)
        expected_size = 1
        self.assertEqual(
            self.recovery_series_writer.sparse_store.write.call_count, \
            expected_size)
        self.assertEqual(
            self.recovery_series_writer.sparse_store.mock_calls\
                [expected_size - 1][1][0].value,
            MISSING_POINT_VALUE)

        # recovery series writer does not write the raw data point
        self.recovery_series_writer.append(dps[2], False)
        expected_size = 1
        self.assertEqual(
            self.recovery_series_writer.sparse_store.write.call_count, \
            expected_size)

        # the epoch gap is large, so a tombstone marker is written
        self.recovery_series_writer.append(dps[3], False)
        expected_size = 2
        self.assertEqual(
            self.recovery_series_writer.sparse_store.write.call_count, \
            expected_size)
        self.assertEqual(
            self.recovery_series_writer.sparse_store.mock_calls\
                [expected_size - 1][1][0].value,
            MISSING_POINT_VALUE)

    def test_append_metric_replay(self) -> None:
        """
        Test writing a metric to recovery_series_writer
        """
        dps = [
            DataPoint('m', 'f', {}, 10, int(time.time()), 10000),
            DataPoint('m', 'f', {}, 6020, int(time.time()), 10200),
            DataPoint('m', 'f', {}, 6030, int(time.time()), 10201),
            DataPoint('m', 'f', {}, 18040, int(time.time()), 10401)
            ]

        # recovery series writer does not write the raw data point
        self.recovery_series_writer.append(dps[0], True)
        expected_size = 0
        self.assertEqual(
            self.recovery_series_writer.sparse_store.write.call_count, \
            expected_size)

        # the epoch gap is large, so a missing_value marker is written
        self.recovery_series_writer.append(dps[1], True)
        expected_size = 0
        self.assertEqual(
            self.recovery_series_writer.sparse_store.write.call_count, \
            expected_size)

        # recovery series writer does not write the raw data point
        self.recovery_series_writer.append(dps[2], True)
        expected_size = 0
        self.assertEqual(
            self.recovery_series_writer.sparse_store.write.call_count, \
            expected_size)

        # the epoch gap is large, so a tombstone marker is written
        self.recovery_series_writer.append(dps[3], True)
        expected_size = 0
        self.assertEqual(
            self.recovery_series_writer.sparse_store.write.call_count, \
            expected_size)

    # pylint: disable-msg=R0913  # Too Many Arguments
    def generate_datapoints(self,
                            nodes: int,
                            fields: int,
                            epoch: int,
                            skip_node_fields: List = None,
                            ephemeral_tag_value: str = None,
                            replay_mode: bool = False) -> None:
        """
        :param nodes:
        :param fields:
        :param epoch:
        :param skip_node_fields
        :param ephemeral_tag_value
        """

        for n in range(nodes):
            for f in range(fields):
                if skip_node_fields is not None and (n, f) in skip_node_fields:
                    continue
                tags = {'node': 'n_' + str(n)}
                if ephemeral_tag_value is not None:
                    tags['ephemeral'] = ephemeral_tag_value
                data_points = [DataPoint('m',
                                         'f_' + str(f),
                                         tags,
                                         epoch,
                                         int(time.time()),
                                         epoch*10)]
                self.recovery_series_writer.append_metrics(data_points,
                                                         replay_mode)

    def test_inline_heartbeat(self) -> None:
        """
        Inline heartbeat can only create missing point markers if the series
        generates a new datapoint. If a series permanently dies then inline
        heartbeat cannot create a marker
        """
        # pylint: disable-msg=R0914  # Too Many Locals
        time.time = self.unit_test_time
        self.recovery_series_writer.data_gap_detection_interval = 10
        nodes = 1
        fields = 1
        server_clock_skew = 1
        epochs = [10,
                  20,
                  # 30, Dropped
                  40,
                  50,
                  # 60, Dropped
                  # 70, Dropped
                  80,
                  90]

        # Prepare for testcase
        self.recovery_series_writer.reinitialize()
        self.recovery_series_writer.sparse_store.reset_mock()

        for epoch in epochs:
            self._unit_test_real_time_clock = epoch + server_clock_skew
            self.generate_datapoints(nodes, fields, epoch,
                                        replay_mode=False)

        missing_30 = DataPoint('m', 'f_0', {'node': 'n_0'}, 30,
                                int(time.time()), MISSING_POINT_VALUE,
                                is_special_value=True)
        missing_60 = DataPoint('m', 'f_0', {'node': 'n_0'}, 60,
                                int(time.time()), MISSING_POINT_VALUE,
                                is_special_value=True)
        expected_missing_markers = [missing_30.to_druid_json_str(),
                                    missing_60.to_druid_json_str()]
        all_missing_markers = \
            [call_item[1][0].to_druid_json_str() for call_item in
                self.recovery_series_writer.sparse_store.mock_calls
                if call_item[1][0].value == MISSING_POINT_VALUE]

        self.assertEqual(expected_missing_markers, all_missing_markers)
        last_epoch: int = 90
        stats = self.recovery_series_writer.get_stats(missing_30)

        self.assertEqual(stats.get_refresh_epoch(), last_epoch)

    def test_inline_heartbeat_replay(self) -> None:
        """
        Inline heartbeat does not create missing point markers when
        in replay mode
        """
        # pylint: disable-msg=R0914  # Too Many Locals
        time.time = self.unit_test_time
        self.recovery_series_writer.data_gap_detection_interval = 10
        nodes = 1
        fields = 1
        server_clock_skew = 1
        epochs = [10,
                  20,
                  # 30, Dropped
                  40,
                  50,
                  # 60, Dropped
                  # 70, Dropped
                  80,
                  90]

        # Prepare for testcase
        self.recovery_series_writer.reinitialize()
        self.recovery_series_writer.sparse_store.reset_mock()

        for epoch in epochs:
            self._unit_test_real_time_clock = epoch + server_clock_skew
            self.generate_datapoints(nodes, fields, epoch,
                                        replay_mode=True)

        expected_missing_markers = []
        all_missing_markers = \
            [call_item[1][0].to_druid_json_str() for call_item in
                self.recovery_series_writer.sparse_store.mock_calls
                if call_item[1][0].value == MISSING_POINT_VALUE]

        self.assertEqual(expected_missing_markers, all_missing_markers)

    def unit_test_time(self) -> int:
        """
        Test method to replace time.time() with a fake clock
        that follows along with the generated epochs
        :return: generated time in epoch
        """
        return self._unit_test_real_time_clock

    def test_offline_heartbeat(self) -> None:
        """
        Tests that a series dies permanently is tombstoned in the end
        by an offline heartbeat
        """
        self.recovery_series_writer.reinitialize()
        self.recovery_series_writer.sparse_store.reset_mock()
        # override the time.time() method so we can control the real time
        # clock behavior
        time.time = self.unit_test_time
        dg_interval = \
            self.recovery_series_writer.data_gap_detection_interval
        term_interval = \
            self.recovery_series_writer.termination_detection_interval
        nodes = 2
        fields = 2
        start_epoch = 10
        server_clock_skew = 1
        total_points = 10
        series_death_point = total_points - ((term_interval / dg_interval) + 1)

        for d in range(total_points):
            epoch = start_epoch + d * dg_interval
            self._unit_test_real_time_clock = epoch + server_clock_skew

            # skip a field for 3 consecutive points twice
            # this should create two tombstones
            if d >= series_death_point:
                self.generate_datapoints(nodes, fields, epoch, [(0, 0)])
            else:
                self.generate_datapoints(nodes, fields, epoch)

        # The configured periodic heartbeat scan will not trigger in
        # unittest environment because it follows a real-time clock
        # we will explicitly trigger it in unittest
        self.recovery_series_writer.heartbeat_scan(
            self._unit_test_real_time_clock)

        tombstone_1 = DataPoint(
            'm', 'f_0', {'node': 'n_0'},
            int(start_epoch + series_death_point * dg_interval),
            int(time.time()), TOMBSTONE_VALUE, is_special_value=True)
        expected_tombstones = [tombstone_1.to_druid_json_str()]
        all_tombstones = \
            [call[1][0].to_druid_json_str()
             for call in
             self.recovery_series_writer.sparse_store.write.mock_calls
             if call[1][0].value == TOMBSTONE_VALUE and
             not call[1][0].datasource.startswith('rollup')]

        self.assertEqual(expected_tombstones, all_tombstones)
        self.assertTrue(tombstone_1.series_id not in
                        self.recovery_series_writer.stats_cache)

    def test_ephemeral_series_containment(self) -> None:
        """
        Tests with heartbeat stats size is contained as
        ephemeral series are cleaned up
        """
        self.recovery_series_writer.stats_cache.clear()
        # override the time.time() method so we can control the real time
        # clock behavior
        time.time = self.unit_test_time
        dg_interval = self.recovery_series_writer.data_gap_detection_interval
        dp_interval = dg_interval/2
        nodes = 2
        fields = 2
        ephemeral_series = 4
        start_epoch = 10
        server_clock_skew = 1
        total_points = 50

        # confirm with non-ephmeral series stats cache grows
        for d in range(total_points):
            epoch = start_epoch + d * dp_interval
            self._unit_test_real_time_clock = epoch + server_clock_skew
            for active_id in range(ephemeral_series):
                active_tag_value = 'act_' + str(active_id)
                self.generate_datapoints(nodes,
                                         fields,
                                         epoch,
                                         None,
                                         active_tag_value)
            if d % 10 == 0:
                # Run heart beat scan after every 10 datapoints
                self.recovery_series_writer.heartbeat_scan(
                    self._unit_test_real_time_clock)

        # In this case we repeated every ephemeral series for every epoch
        # so no series was ephemeral, hence every series must remain
        self.assertEqual(nodes*fields*ephemeral_series,
                         len(self.recovery_series_writer.stats_cache.items()))

        # clear the cache
        self.recovery_series_writer.stats_cache.clear()
        total_points = 100
        points_in_ephemeral = 10
        # now we are going to generate an ephemeral series for short periods
        for d in range(total_points):
            epoch = start_epoch + d * dp_interval
            self._unit_test_real_time_clock = epoch + server_clock_skew
            ephemeral_tag_value = 'eph_' + str(int(d/points_in_ephemeral))
            self.generate_datapoints(nodes,
                                     fields,
                                     epoch,
                                     None,
                                     ephemeral_tag_value)
            if d % 10 == 0:
                # Run heart beat scan after every 10 datapoints
                self.recovery_series_writer.heartbeat_scan(
                    self._unit_test_real_time_clock)

        # Run heart beat scan after every 10 datapoints
        self.recovery_series_writer.heartbeat_scan(
            self._unit_test_real_time_clock)
        # In this case we created an ephemeral series that only reported
        # data for 10 points, and then the series changed
        self.assertEqual(nodes*fields,
                         len(self.recovery_series_writer.stats_cache.items()))
