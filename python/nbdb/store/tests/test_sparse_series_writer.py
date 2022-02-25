"""
Unittest for SparseSeriesData class
"""

import copy
import json
import os
import random
import shutil
import time
import tempfile
from typing import List, Tuple, Dict
from unittest import TestCase
from unittest.mock import Mock

from nbdb.common.context import Context
from nbdb.common.data_point import DataPoint, BAND_LOW, BAND_HIGH
from nbdb.common.data_point import MISSING_POINT_VALUE, TOMBSTONE_VALUE
from nbdb.common.data_point import MODE_BOTH, MODE_REALTIME, MODE_ROLLUP
from nbdb.common.telemetry import Telemetry
from nbdb.common.tracing_config import TracingConfig
from nbdb.config.settings import Settings
from nbdb.readapi.time_series_response import TimeRange
from nbdb.schema.schema import Schema, ROLLUP_MEAN
from nbdb.store.dist_hist import DistHist
from nbdb.store.sparse_algo_lossless import SparseAlgoLossLess
from nbdb.store.sparse_algo_percentile_window_delta import \
    SparseAlgoPercentileWindowDelta
from nbdb.store.file_backed_sparse_store import FileBackedSparseStore
from nbdb.store.sparse_algo_selector import SparseAlgoSelector
from nbdb.store.sparse_series_stats import SparseSeriesStats
from nbdb.store.sparse_series_writer import SparseSeriesWriter
from nbdb.store.batch_sparse_series_writer import BatchSparseSeriesWriter


class TestSparseSeriesWriter(TestCase):
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
        self.sparse_series_writer = None
        self.create_sparse_series_writer(MODE_BOTH)
        self._unit_test_real_time_clock = 0
        self.exploration_ds = schema.exploration_datasources.get(
            Settings.inst.realtime_metric_consumer.metric_protocol)

    def create_sparse_series_writer(self, consumer_mode):
        """
        Create SparseSeriesWriter.
        """
        self.sparse_series_writer = SparseSeriesWriter(
            sparse_store=Mock(),
            sparse_store_settings=Settings.inst.sparse_store,
            tracing_config_settings=Settings.inst.tracing_config,
            context=self.context,
            protocol=Settings.inst.realtime_metric_consumer.metric_protocol,
            sparse_telemetry_source_id="test",
            past_message_lag=86400,
            future_message_lag=86400,
            consumer_mode=consumer_mode
        )

    def test_prod_sparse_algo_regex_rules(self) -> None:
        """
        Test the regex rules used in the prod schema to select sparse algos
        :return:
        """
        # Instantiate selector
        schema = Schema.load_from_file(
            os.path.dirname(__file__) + '/../../config/schema_prod.yaml')
        context = Context(schema=schema)
        selector = SparseAlgoSelector(
            sparse_telemetry_settings=Mock(),
            sparse_telemetry_source_id="test",
            context=context,
            sparse_store=None,
            consumer_mode=MODE_BOTH)
        # Use hardcoded cluster ID and node IDs
        cluster = 'ABC'
        node_id = 'RVM123'

        # *.count should get lossless
        tags = {'tkn0': 'clusters',
                'tkn1': cluster,
                'tkn2': node_id,
                'tkn3': 'Diamond',
                'tkn4': 'service',
                'tkn5': 'Diamond',
                'tkn6': 'Run',
                'tkns': '7'}
        datapoint = DataPoint('all_metrics_t_0', 'count', tags, 0, 0, 0)
        algo = selector.match_algo(datapoint)
        self.assertEqual(algo.__class__, SparseAlgoLossLess)

        # *.uptime.* should get lossless
        tags = {'tkn0': 'clusters',
                'tkn1': cluster,
                'tkn2': node_id,
                'tkn3': 'Diamond',
                'tkn4': 'uptime',
                'tkns': '5'}
        datapoint = DataPoint('all_metrics_t_0', 'seconds', tags, 0, 0, 0)
        algo = selector.match_algo(datapoint)
        self.assertEqual(algo.__class__, SparseAlgoLossLess)

        # *.uptime should get lossless
        tags = {'tkn0': 'clusters',
                'tkn1': cluster,
                'tkn2': node_id,
                'tkn3': 'Diamond',
                'tkn4': 'service',
                'tkn5': 'Diamond',
                'tkns': '6'}
        datapoint = DataPoint('all_metrics_t_0', 'uptime', tags, 0, 0, 0)
        algo = selector.match_algo(datapoint)
        self.assertEqual(algo.__class__, SparseAlgoLossLess)

        # Percentiles should get PercentileWindowDelta
        tags = {'tkn0': 'clusters',
                'tkn1': cluster,
                'tkn2': node_id,
                'tkn3': 'Diamond',
                'tkn4': 'service',
                'tkn5': 'Diamond',
                'tkn6': 'queue_latency',
                'tkns': '7'}
        datapoint = DataPoint('all_metrics_t_0', 'p999', tags, 0, 0, 0)
        algo = selector.match_algo(datapoint)
        self.assertEqual(algo.__class__, SparseAlgoPercentileWindowDelta)

        # Everything else should not match anything
        tags = {'tkn0': 'clusters',
                'tkn1': cluster,
                'tkn2': node_id,
                'tkn3': 'Diamond',
                'tkn4': 'service',
                'tkn5': 'Diamond',
                'tkns': '6'}
        datapoint = DataPoint('all_metrics_t_0', 'cpu_percent', tags, 0, 0, 0)
        algo = selector.match_algo(datapoint)
        self.assertEqual(algo, None)

    def test_append_metric_cc(self) -> None:
        """
        Test writing a metric to sparse_series_writer, the data points should also
        be sharded by metric name
        """
        dps = [
            DataPoint('m', 'f', {'tkn3' : 'service_1', 'tkn4' :
                                 'Fileset'}, 10, int(time.time()), 10000),
            DataPoint('m', 'f', {'tkn3' : 'service_1', 'tkn4' :
                                 'Fileset'}, 20, int(time.time()), 10200),
            DataPoint('m', 'f', {'tkn3' : 'service_1', 'tkn4' :
                                 'Fileset'}, 30, int(time.time()), 10201),
            DataPoint('m', 'f', {'tkn3' : 'service_1', 'tkn4' :
                                 'Fileset'}, 40, int(time.time()), 10401)
            ]

        # First point is always written. Since replay mode is false, an
        # exploration point should also be generated
        for dp in dps:
            self.sparse_series_writer.append(dp, False)

        # Three points will be written for the original series. 3 additional
        # points should be written because it matches a cross-cluster pattern.
        # Adding the exploration datapoint at the beginning, we should see a
        # total of 7 writes
        self.assertEqual(
            self.sparse_series_writer.sparse_store.write.call_count, 7)

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
            self.sparse_series_writer.append(dp, False)

        # Make sure we correctly determined that sparseness was disabled for
        # both series
        for stat in self.sparse_series_writer.stats_cache.values():
            self.assertTrue(stat.is_sparseness_disabled())

        # We should have 4 points written for series 1: 2 for the original
        # series and an additional 2 for matching a cross-cluster pattern
        # For series 2, we will have 2 points written.
        # Additionally we will have 2 exploration datapoints written, resulting
        # in a grand total of 4 + 2 + 2 = 8 points written
        self.assertEqual(
            self.sparse_series_writer.sparse_store.write.call_count, 8)

        # Finally we simulate a heartbeat scan. Both series should be removed
        # from the stats cache but no tombstones should be generated
        self.sparse_series_writer.heartbeat_scan()
        self.assertEqual(len(self.sparse_series_writer.stats_cache), 0)
        # Write count should remain the same
        self.assertEqual(
            self.sparse_series_writer.sparse_store.write.call_count, 8)

    def test_append_metric(self) -> None:
        """
        Test writing a metric to sparse_series_writer
        """
        dps = [
            DataPoint('m', 'f', {}, 10, int(time.time()), 10000),
            DataPoint('m', 'f', {}, 20, int(time.time()), 10200),
            DataPoint('m', 'f', {}, 30, int(time.time()), 10201),
            DataPoint('m', 'f', {}, 40, int(time.time()), 10401)
            ]

        # First point is always written. Since replay mode is false, an
        # exploration point should also be generated
        self.sparse_series_writer.append(dps[0], False)
        # Two points will be written: the original point and exploration point
        self.assertEqual(
            self.sparse_series_writer.sparse_store.write.call_count, 2)

        exploration_dp = copy.deepcopy(dps[0])
        exploration_dp.datasource = self.exploration_ds
        mock_calls = self.sparse_series_writer.sparse_store.write.mock_calls
        dp_written_1 = mock_calls[0][1][0]
        self.assertEqual(exploration_dp.to_druid_json_str(),
                         dp_written_1.to_druid_json_str())

        self.sparse_series_writer.sparse_store.write.assert_called_with(
            dps[0])

        # the delta is greater than min_delta so should be written
        self.sparse_series_writer.append(dps[1], False)
        self.assertEqual(
            self.sparse_series_writer.sparse_store.write.call_count, 3)
        self.sparse_series_writer.sparse_store.write.assert_called_with(
            dps[1])

        # the delta is less than min_delta so should be skipped
        self.sparse_series_writer.append(dps[2], False)
        self.assertEqual(
            self.sparse_series_writer.sparse_store.write.call_count, 3)

        # the delta is greater than min_delta so should be written
        self.sparse_series_writer.append(dps[3], False)
        self.assertEqual(
            self.sparse_series_writer.sparse_store.write.call_count, 4)
        self.sparse_series_writer.sparse_store.write.assert_called_with(
            dps[3])

    def test_append_metric_in_replay_mode(self) -> None:
        """
        Test writing a metric to sparse_series_writer when some messages are
        being replayed
        """
        # First four messages will be with replay_mode=True. The last two will
        # be non replay messages
        dps = [
            DataPoint('m', 'f', {}, 10, int(time.time()), 10000),
            DataPoint('m', 'f', {}, 20, int(time.time()), 10200),
            DataPoint('m', 'f', {}, 30, int(time.time()), 10201),
            DataPoint('m', 'f', {}, 40, int(time.time()), 10401),
            DataPoint('m', 'f', {}, 50, int(time.time()), 10401),
            DataPoint('m', 'f', {}, 60, int(time.time()), 10402),
            DataPoint('m', 'f', {}, 70, int(time.time()), 10602),
            ]

        # First 4 points are appended. None of them should be written since
        # they are replay messages
        for idx in range(4):
            self.sparse_series_writer.append(dps[idx], True)
        self.assertEqual(
            self.sparse_series_writer.sparse_store.write.call_count, 0)

        # Fifth message should be force written even though delta is zero. We
        # write the first datapoint after transitioning from replay mode to
        # non-replay mode to maintain our 12h forced_write_interval invariant.
        self.sparse_series_writer.append(dps[4], False)
        # We will actually generate two writes: the original datapoint and the
        # exploration datapoint
        exploration_dp = copy.deepcopy(dps[4])
        exploration_dp.datasource = self.exploration_ds
        self.assertEqual(
            self.sparse_series_writer.sparse_store.write.call_count, 2)
        mock_calls = self.sparse_series_writer.sparse_store.write.mock_calls
        dp_written_1 = mock_calls[0][1][0]
        self.assertEqual(exploration_dp.to_druid_json_str(),
                         dp_written_1.to_druid_json_str())
        self.sparse_series_writer.sparse_store.write.assert_called_with(
            dps[4])
        self.sparse_series_writer.sparse_store.reset_mock()

        # Sixth message should be dropped since delta < min_delta
        self.sparse_series_writer.append(dps[5], False)
        self.assertEqual(
            self.sparse_series_writer.sparse_store.write.call_count, 0)

        # For the seventh, the delta is greater than min_delta so should be
        # written
        self.sparse_series_writer.append(dps[6], False)
        self.assertEqual(
            self.sparse_series_writer.sparse_store.write.call_count, 1)
        self.sparse_series_writer.sparse_store.write.assert_called_with(
            dps[6])

    def test_append_metric_in_realtime_consumer(self) -> None:
        """
        Test writing a metric to sparse_series_writer for realtime consumer
        """
        self.create_sparse_series_writer(MODE_REALTIME)
        dps = [
            DataPoint('m', 'f', {}, 10, int(time.time()), 10000),
            DataPoint('m', 'f', {}, 20, int(time.time()), 10200),
            DataPoint('m', 'f', {}, 30, int(time.time()), 10201),
            DataPoint('m', 'f', {}, 40, int(time.time()), 10401),
            DataPoint('m', 'f', {}, 50, int(time.time()), 10401),
            DataPoint('m', 'f', {}, 60, int(time.time()), 10402),
            DataPoint('m', 'f', {}, 70, int(time.time()), TOMBSTONE_VALUE,
                      is_special_value=True),
            ]
        for dp in dps:
            self.sparse_series_writer.append(dp, False)

        # First, second, fourth and seventh datapoints should be written. We
        # should also generate an extra exploration datapoint
        mock_calls = self.sparse_series_writer.sparse_store.write.mock_calls
        dps_written = [call[1][0] for call in mock_calls]
        self.assertEqual(dps_written[1:], [dps[0], dps[1], dps[3], dps[6]])

        self.assertEqual(
            self.sparse_series_writer.sparse_store.write.call_count, 5)

        # Reset SparseSeriesWriter at end of testcase because other testcases
        # need MODE_BOTH
        self.create_sparse_series_writer(MODE_BOTH)

    def test_append_metric_in_rollup_consumer(self) -> None:
        """
        Test writing a metric to sparse_series_writer for rollup consumer
        """
        self.create_sparse_series_writer(MODE_ROLLUP)
        dps = [
            DataPoint('m', 'f', {}, 10, int(time.time()), 10000),
            DataPoint('m', 'f', {}, 20, int(time.time()), 10200),
            DataPoint('m', 'f', {}, 30, int(time.time()), 10201),
            DataPoint('m', 'f', {}, 40, int(time.time()), 10401),
            DataPoint('m', 'f', {}, 50, int(time.time()), 10401),
            DataPoint('m', 'f', {}, 60, int(time.time()), 10402),
            # Last datapoint is 2 hrs after prev datapoint to coerce writing of
            # the 3600 rollup datapoint
            DataPoint('m', 'f', {}, 7260, int(time.time()), 10602),
        ]
        for dp in dps:
            self.sparse_series_writer.append(dp, False)

        # Four datapoints should be generated: one exploration datapoint, 2
        # datapoints for the 3600 window & 1 datapoint for the 7200 window
        self.assertEqual(
            self.sparse_series_writer.sparse_store.write.call_count, 4)

        mock_calls = self.sparse_series_writer.sparse_store.write.mock_calls
        dps_written = [call[1][0] for call in mock_calls]
        self.assertEqual(dps_written[0].datasource, self.exploration_ds)
        self.assertTrue(dps_written[1].datasource.startswith('rollup'))

        # Reset SparseSeriesWriter at end of testcase because other testcases
        # need MODE_BOTH
        self.create_sparse_series_writer(MODE_BOTH)

    def test_outlier_metric(self) -> None:
        """
        Test writing a high cardinality metric to sparse_series_writer
        where the cardinality is preserved but data in high density
        bands is dropped resulting in significant reduction in DPM
        Requires setting : report_aggregated_stats to false
        """

        # multi-modal distribution
        modes: List[Tuple[float, float, float]] = [(20, 2, 70),  # 70% prob
                                                   (60, 1, 90),  # 20% prob
                                                   (90, 1, 98)]   # 8% prob]
        data_range = (0, 1000)
        data_source = 'm'
        field = 'outlier'
        tags = {'node': 'n1'}
        dps = DistHist.generate_multi_modal_series(
            TimeRange(0, 4400, 1),
            data_source,
            field,
            tags,
            modes,
            data_range
        )

        for dp in dps:
            self.sparse_series_writer.append(dp, False)

        # By now distribution is generated
        # write an outlier and confirm its an outlier written
        # different tag set
        tags = {'node': 'n2'}
        outlier_data_point = DataPoint(data_source,
                                       field,
                                       tags,
                                       2000,
                                       int(time.time()),
                                       2)
        self.sparse_series_writer.append(outlier_data_point, False)
        self.sparse_series_writer.sparse_store.write.assert_called_with(
            outlier_data_point)

        # write within a band and confirm its a band written
        within_band_data_point = DataPoint(data_source,
                                           field,
                                           tags,
                                           2000,
                                           int(time.time()),
                                           20)

        # the delta is greater than min_delta so should be written
        self.sparse_series_writer.append(within_band_data_point, False)
        ldp = self.sparse_series_writer.sparse_store.write.call_args[0][0]
        self.assertEqual(2000, ldp.epoch)
        self.assertTrue(float(ldp.tags[BAND_LOW]) <= 20
                        <= float(ldp.tags[BAND_HIGH]))

    def test_outlier_stats_metric(self) -> None:
        """
        Test writing a high cardinality metric to sparse_series_writer
        using outlier sparse algo, in this case the high cardinality data is
        mapped to a low cardinality stats + ephemeral outlier series in orginal
        dimensions. This results in significant reduction in data points and
        also the overall cardinality of the series
        Requires setting : report_aggregated_stats to true
        """

        data_source = 'm'
        field = 'high_dimensional_stats'

        # generate a high cardinality data (1000 series X 10 epochs) = 10000 dp
        total_epochs = 10
        total_nodes = 10
        total_jobs = 100
        for epoch in range(1, total_epochs, 1):
            for node_id in range(total_nodes):
                for job_id in range(total_jobs):
                    value = random.gauss(100, 2)
                    if random.uniform(0, 100) >= 99:
                        # draw from the outlier distribution
                        value = random.gauss(500, 50)
                    tags = {
                        'job_type': 'j1',
                        'node': str(node_id),
                        'job_id': str(job_id)
                    }
                    dp = DataPoint(data_source,
                                   field,
                                   tags,
                                   epoch,
                                   int(time.time()),
                                   value)
                    self.sparse_series_writer.append(dp, False)

        mock_store = self.sparse_series_writer.sparse_store

        points_received = total_epochs*total_nodes*total_jobs
        # Exclude exploration datapoints generated from the points written
        # count. There will be one exploration datapoint for each unique series
        points_written = mock_store.write.call_count - total_nodes*total_jobs

        drop_ratio = 1-points_written/points_received
        print('points_received: {} points_written: {} drop_ratio: {}'.format(
            points_received, points_written, drop_ratio))
        self.assertLess(0.98, drop_ratio, 'expected a drop ratio of 95%')

        band_series: Dict[str, int] = dict()
        outlier_series: Dict[str, int] = dict()
        for call in mock_store.write.call_args_list:
            dp = call[0][0]
            if dp.datasource == self.exploration_ds:
                # Exclude exploration datasources from our analysis
                continue
            if BAND_LOW in dp.tags:
                if dp.series_id not in band_series:
                    band_series[dp.series_id] = 0
                band_series[dp.series_id] = band_series[dp.series_id] + 1
            else:
                if dp.series_id not in outlier_series:
                    outlier_series[dp.series_id] = 0
                outlier_series[dp.series_id] = outlier_series[dp.series_id] + 1

        total_series = total_nodes*total_jobs
        series_dropped = 1-len(outlier_series)/total_series
        band_series_added = len(band_series)/total_series
        print('TotalSeries: {} series_dropped: {} band_series:{}'.format(
            total_series, series_dropped, band_series_added
        ))
        self.assertLess(0.9, series_dropped,
                        'expected a series drop rate of 90% or more')
        self.assertGreater(0.01, band_series_added,
                           'expected band series to be a small fraction '
                           'of total series')

    def test_stream_transform(self) -> None:
        """
        Tests the stream transformation of a counter to a derivative
        """

        dps = [
            DataPoint('m', 'transform.count', {}, 10, int(time.time()), 10),
            DataPoint('m', 'transform.count', {}, 20, int(time.time()), 11),
            DataPoint('m', 'transform.count', {}, 30, int(time.time()), 12),
            DataPoint('m', 'transform.count', {}, 40, int(time.time()), 20)
        ]
        dps_dv = [
            DataPoint('m', 'transform.count.t_meter', {}, 10,
                      int(time.time()), MISSING_POINT_VALUE,
                      is_special_value=True),
            DataPoint('m', 'transform.count.t_meter', {}, 20,
                      int(time.time()), (11-10)/(20-10)),
            DataPoint('m', 'transform.count.t_meter', {}, 40,
                      int(time.time()), (20-11)/(40-20))
        ]
        exploration_dp = copy.deepcopy(dps[0])
        exploration_dp.datasource = self.exploration_ds

        # first point is always written, but we are computing derivative
        # so first point is always 0
        self.sparse_series_writer.append(dps[0], False)
        mock_calls = self.sparse_series_writer.sparse_store.mock_calls
        # Since this is the first datapoint of a new series, we will also see
        # an exploration datapoint
        dp_written_1 = mock_calls[0][1][0]
        self.assertEqual(exploration_dp.to_druid_json_str(),
                         dp_written_1.to_druid_json_str())

        dp_written_2 = mock_calls[1][1][0]
        self.assertEqual(dps_dv[0].to_druid_json_str(),
                         dp_written_2.to_druid_json_str())

        # second point is when we compute the derivative
        self.sparse_series_writer.append(dps[1], False)
        dp_written = mock_calls[2][1][0]
        self.assertEqual(dps_dv[1].to_druid_json_str(),
                         dp_written.to_druid_json_str())

        # third point on actual value is higher than min_delta
        # but derivative is same so shouldn't be written
        self.sparse_series_writer.append(dps[2], False)
        self.assertEqual(3, len(mock_calls))

        # fourth point has much higher derivative so should be written
        self.sparse_series_writer.append(dps[3], False)
        dp_written = mock_calls[3][1][0]
        self.assertEqual(dps_dv[2].to_druid_json_str(),
                         dp_written.to_druid_json_str())

    def test_append_metric_with_percentile_window(self) -> None:
        """
        Test writing a metric to sparse_series_writer
        """

        # lets fill up the window with consecutive values that are very large
        for epoch in range(10):
            # first point is always written
            value = 1000 - epoch * 100
            self.sparse_series_writer.append(
                DataPoint('m', 'f.p99', {}, epoch, int(time.time()), value),
                False)

        # now append a small value, this will be written
        # because delta change is pretty high
        # abs(2-900)/990 = 99.7%
        self.sparse_series_writer.append(
            DataPoint('m', 'f.p99', {}, 11, int(time.time()), 2), False)

        # now append another small value but with a large delta
        # compared to last value (4-2)/2 = 100% delta
        self.sparse_series_writer.append(
            DataPoint('m', 'f.p99', {}, 12, int(time.time()), 4), False)

        # The percentile_window algorithm will prevent this value from
        # being written because delta is computed against
        # the 90th percentile of last 10 recorded values
        # i.e. (4-2)/980 = 0.2 %

        # write a value that is significant compared to percentile value
        # (100-2)/980 = 10 %
        self.sparse_series_writer.append(
            DataPoint('m', 'f.p99', {}, 13, int(time.time()), 100), False)
        dp = DataPoint('m', 'f.p99', {}, 0, int(time.time()), 0)

        sparse_algo = self.sparse_series_writer.sparse_algo_selector.\
            get_algo(SparseAlgoPercentileWindowDelta.__name__)
        stats = self.sparse_series_writer.get_stats(dp)
        self.assertEqual(
            str(SparseSeriesStats().
                set_crosscluster_shard(None).
                set_first_epoch(0).
                set_refresh_epoch(13).
                set_server_rx_time(stats.get_server_rx_time()).
                set_window_epoch(0, 13).
                set_window_value(0, 100).
                set_check_point(3600, 0).
                set_check_point(7200, 0).
                set_rollup_intermediate_delta_epoch(3600, 12).
                set_rollup_intermediate_delta_epoch(7200, 12).
                set_rollup_intermediate_value(3600, 4606).
                set_rollup_intermediate_value(7200, 4606).
                set_algo(sparse_algo).
                set_replay_mode(False).
                set_rollup_function(ROLLUP_MEAN).
                set_moving_window_value(0,
                                        [700, 600, 500, 400, 300,
                                         200, 100, 2, 4, 100])),
            str(stats))

    def test_append_counter(self) -> None:
        """
        Test writing a metric to sparse_series_writer
        """

        # this should be lossless, so only equal values are ignored
        self.sparse_series_writer.append(
            DataPoint('m', 'f.count', {}, 10, int(time.time()), 50), False)

        # equal value ignored
        self.sparse_series_writer.append(
            DataPoint('m', 'f.count', {}, 12, int(time.time()), 50), False)

        # unequal value but very small, not ignored
        self.sparse_series_writer.append(
            DataPoint('m', 'f.count', {}, 13, int(time.time()), 50.001), False)

        dp = DataPoint('m', 'f.count', {}, 0, int(time.time()), 0)
        sparse_algo = self.sparse_series_writer.sparse_algo_selector.get_algo(
            SparseAlgoLossLess.__name__)
        stats = self.sparse_series_writer.get_stats(dp)
        expected_stats = SparseSeriesStats()\
            .set_crosscluster_shard(None)\
            .set_window_epoch(0, 13)\
            .set_refresh_epoch(13)\
            .set_window_value(0, 50.001)\
            .set_server_rx_time(stats.get_server_rx_time())\
            .set_algo(sparse_algo)\
            .set_replay_mode(False)\
            .set_rollup_function(ROLLUP_MEAN)\
            .set_check_point(3600, 0)\
            .set_check_point(7200, 0)\
            .set_rollup_intermediate_delta_epoch(3600, 3)\
            .set_rollup_intermediate_delta_epoch(7200, 3)\
            .set_rollup_intermediate_value(3600, 150)\
            .set_rollup_intermediate_value(7200, 150)\
            .set_first_epoch(10)
        self.assertEqual(str(expected_stats), str(stats))

    def test_exploration_dp_writes(self) -> None:
        """
        Verify that exploration datapoints are generated correctly
        """
        # Forced write interval is 12h. Simulate data gap interval of 4h to
        # reduce testcase complexity
        self.sparse_series_writer.data_gap_detection_interval = 14400
        # Disable rollup writes
        self.sparse_series_writer.write_rollup_datapoints = False
        se = 10
        # We simulate a case where 4 datapoints are received with the same
        # value. The fifth value is beyond the forced write interval and has a
        # different value
        dps = [
            DataPoint('m', 'f', {}, se, int(time.time()), 10000),
            DataPoint('m', 'f', {}, se + 14400, int(time.time()), 10000),
            DataPoint('m', 'f', {}, se + 2*14400, int(time.time()), 10000),
            DataPoint('m', 'f', {}, se + 3*14400, int(time.time()), 10000),
            DataPoint('m', 'f', {}, se + 4*14400, int(time.time()), 12000),
        ]

        # First point is always written. Since replay mode is false, an
        # exploration point should also be generated
        self.sparse_series_writer.append(dps[0], False)
        # Two points will be written: the original point and exploration point
        self.assertEqual(
            self.sparse_series_writer.sparse_store.write.call_count, 2)

        exploration_dp = copy.deepcopy(dps[0])
        exploration_dp.datasource = self.exploration_ds
        mock_calls = self.sparse_series_writer.sparse_store.write.mock_calls
        dp_written_1 = mock_calls[0][1][0]
        self.assertEqual(exploration_dp.to_druid_json_str(),
                         dp_written_1.to_druid_json_str())

        self.sparse_series_writer.sparse_store.write.assert_called_with(
            dps[0])

        # No writes should be generated for the second to fourth datapoints
        # because the values are the same
        for idx in range(1, 4):
            self.sparse_series_writer.append(dps[idx], False)
            self.assertEqual(
                self.sparse_series_writer.sparse_store.write.call_count, 2)

        # We should generate two writes for the fifth datapoint. Even though
        # the value hasn't changed, it is past the forced write interval
        self.sparse_series_writer.append(dps[4], False)
        self.assertEqual(
            self.sparse_series_writer.sparse_store.write.call_count, 4)
        self.sparse_series_writer.sparse_store.write.assert_called_with(
            dps[4])

        exploration_dp = copy.deepcopy(dps[4])
        exploration_dp.datasource = self.exploration_ds
        dp_written_4 = mock_calls[3][1][0]
        self.assertEqual(exploration_dp.to_druid_json_str(),
                         dp_written_4.to_druid_json_str())

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
                self.sparse_series_writer.append_metrics(data_points,
                                                         replay_mode)

    def test_inline_heartbeat(self) -> None:
        """
        Inline heartbeat can only create missing point markers if the series
        generates a new datapoint. If a series permanently dies then inline
        heartbeat cannot create a marker
        """
        # pylint: disable-msg=R0914  # Too Many Locals
        time.time = self.unit_test_time
        self.sparse_series_writer.data_gap_detection_interval = 10
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

        # Missing point markers should be written only during non-replay mode
        # messages
        for replay_mode in (False, True):
            # Prepare for testcase
            self.sparse_series_writer.reinitialize()
            self.sparse_series_writer.sparse_store.reset_mock()

            for epoch in epochs:
                self._unit_test_real_time_clock = epoch + server_clock_skew
                self.generate_datapoints(nodes, fields, epoch,
                                         replay_mode=replay_mode)

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
                 self.sparse_series_writer.sparse_store.mock_calls
                 if call_item[1][0].value == MISSING_POINT_VALUE]

            if replay_mode:
                self.assertEqual(len(all_missing_markers), 0)
            else:
                self.assertEqual(expected_missing_markers, all_missing_markers)
            last_epoch: int = 90
            stats = self.sparse_series_writer.get_stats(missing_30)

            expected_stats = SparseSeriesStats()\
                .set_crosscluster_shard(None)\
                .set_first_epoch(10)\
                .set_refresh_epoch(last_epoch)\
                .set_server_rx_time(stats.get_server_rx_time())\
                .set_window_epoch(0, last_epoch)\
                .set_window_value(0, last_epoch*10)\
                .set_check_point(3600, 0)\
                .set_check_point(7200, 0)\
                .set_rollup_intermediate_delta_epoch(3600, 50)\
                .set_rollup_intermediate_delta_epoch(7200, 50)\
                .set_rollup_intermediate_value(3600, 20000)\
                .set_rollup_intermediate_value(7200, 20000)\
                .set_rollup_function(ROLLUP_MEAN)\
                .set_replay_mode(replay_mode)
            self.maxDiff = None
            self.assertEqual(str(expected_stats), str(stats))

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
        self.sparse_series_writer.reinitialize()
        self.sparse_series_writer.sparse_store.reset_mock()
        # override the time.time() method so we can control the real time
        # clock behavior
        time.time = self.unit_test_time
        dg_interval = \
            self.sparse_series_writer.data_gap_detection_interval
        term_interval = \
            self.sparse_series_writer.termination_detection_interval
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
        self.sparse_series_writer.heartbeat_scan(
            self._unit_test_real_time_clock)

        tombstone_1 = DataPoint(
            'm', 'f_0', {'node': 'n_0'},
            int(start_epoch + series_death_point * dg_interval),
            int(time.time()), TOMBSTONE_VALUE, is_special_value=True)
        expected_tombstones = [tombstone_1.to_druid_json_str()]
        all_tombstones = \
            [call[1][0].to_druid_json_str()
             for call in
             self.sparse_series_writer.sparse_store.write.mock_calls
             if call[1][0].value == TOMBSTONE_VALUE and
             not call[1][0].datasource.startswith('rollup')]

        self.assertEqual(expected_tombstones, all_tombstones)
        self.assertTrue(tombstone_1.series_id not in
                        self.sparse_series_writer.stats_cache)

    def test_ephemeral_series_containment(self) -> None:
        """
        Tests with heartbeat stats size is contained as
        ephemeral series are cleaned up
        """
        self.sparse_series_writer.stats_cache.clear()
        # override the time.time() method so we can control the real time
        # clock behavior
        time.time = self.unit_test_time
        dg_interval = self.sparse_series_writer.data_gap_detection_interval
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
                self.sparse_series_writer.heartbeat_scan(
                    self._unit_test_real_time_clock)

        # In this case we repeated every ephemeral series for every epoch
        # so no series was ephemeral, hence every series must remain
        self.assertEqual(nodes*fields*ephemeral_series,
                         len(self.sparse_series_writer.stats_cache.items()))

        # clear the cache
        self.sparse_series_writer.stats_cache.clear()
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
                self.sparse_series_writer.heartbeat_scan(
                    self._unit_test_real_time_clock)

        # Run heart beat scan after every 10 datapoints
        self.sparse_series_writer.heartbeat_scan(
            self._unit_test_real_time_clock)
        # In this case we created an ephemeral series that only reported
        # data for 10 points, and then the series changed
        self.assertEqual(nodes*fields,
                         len(self.sparse_series_writer.stats_cache.items()))

    def test_stats_scan_perf(self) -> None:
        """
        Tests with heartbeat stats size is contained as
        ephemeral series are cleaned up
        """
        # override the time.time() method so we can control the real time
        # clock behavior
        nodes = 20
        fields = 2000

        # generate lot of series , but just artificially populate stats
        epoch = 10
        for n in range(nodes):
            for f in range(fields):
                datapoint = DataPoint(str(n)+str(f), "", dict(), 0, 0, 0)
                stats = self.sparse_series_writer.get_stats(datapoint)
                stats.set_window_epoch(0, epoch).\
                    set_window_value(0, 10).\
                    set_server_rx_time(epoch).\
                    set_algo(SparseAlgoLossLess.__name__)

        series_count = len(self.sparse_series_writer.stats_cache.items())
        scan_time = time.time()
        self.sparse_series_writer.heartbeat_scan(
            self._unit_test_real_time_clock)
        scan_time = int((time.time() - scan_time)*1000)
        cache_memory_usage_time = time.time()
        size = self.sparse_series_writer.report_stats_cache_telemetry()
        memory_time = int((time.time() - cache_memory_usage_time)*1000)
        if scan_time > 0:
            print('heartbeat_scan: {} ms series:{} throughput: {} /s'.format(
                scan_time, series_count, int(series_count*1000/scan_time)))
        if memory_time > 0:
            print('memory_scan: {} ms series:{} throughput: {} /s '.format(
                memory_time, series_count, int(series_count*1000/memory_time)))
        print('memory_size: {} MB per_series:{} B'.format(
            int(size/(1024*1024)), int(size/series_count)))

class TestFileBackedSparseSeriesWriter(TestCase):
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
            os.path.dirname(__file__) + '/test_schema.yaml', batch_mode=True)
        self.context = Context(schema=schema)
        Telemetry.inst = Mock()
        TracingConfig.inst = Mock()

        self.test_dir = os.path.join(tempfile.mkdtemp(), "filestore")
        self.sparse_series_writer = BatchSparseSeriesWriter(
            context=self.context,
            sparse_store=FileBackedSparseStore(self.test_dir, compress=False),
            sparse_store_settings=Settings.inst.sparse_store,
            tracing_config_settings=Settings.inst.tracing_config,
            protocol=Settings.inst.realtime_metric_consumer.metric_protocol,
            sparse_telemetry_source_id="test",
            past_message_lag=86400,
            future_message_lag=86400,
            consumer_mode=MODE_BOTH
        )
        self._unit_test_real_time_clock = 0
        self.exploration_ds = schema.exploration_datasources.get(
            Settings.inst.realtime_metric_consumer.metric_protocol)

    def tearDown(self) -> None:
        shutil.rmtree(self.test_dir)

    def test_append_metric_with_file_sparse_store(self):
        """
        Test writing data points to file backed sparse_series_writer
        """

        ds1_dps = [
            DataPoint('batch_m', 'f', {}, 10, int(time.time()), 10000),
            DataPoint('batch_m', 'f', {}, 20, int(time.time()), 10200),
            DataPoint('batch_m', 'f', {}, 30, int(time.time()), 10201),
            DataPoint('batch_m', 'f', {}, 40, int(time.time()), 10401)
            ]

        ds2_dps = [
            DataPoint('batch_n', 'f', {}, 10, int(time.time()), 10000),
            DataPoint('batch_n', 'f', {}, 20, int(time.time()), 10200),
            DataPoint('batch_n', 'f', {}, 30, int(time.time()), 10201),
            DataPoint('batch_n', 'f', {}, 40, int(time.time()), 10401)
            ]

        def get_points_written(datasource: str) -> int:
            sparse_store = self.sparse_series_writer.sparse_store
            return sparse_store.points_written_for_datasource(datasource)

        for idx, dps in enumerate([ds1_dps, ds2_dps]):
            datasource = dps[0].datasource

            # First point is always written
            self.sparse_series_writer.append(dps[0], False)
            self.assertEqual(get_points_written(datasource), 1)
            # We should have also generated an exploration datapoint since this
            # is the first datapoint of a new series
            exp_exploration_points = 1 if idx == 0 else 2
            self.assertEqual(get_points_written(self.exploration_ds),
                             exp_exploration_points)

            # the delta is greater than min_delta so should be written
            self.sparse_series_writer.append(dps[1], False)
            self.assertEqual(get_points_written(datasource), 2)

            # the delta is less than min_delta so should be skipped
            self.sparse_series_writer.append(dps[2], False)
            self.assertEqual(get_points_written(datasource), 2)

            # the delta is greater than min_delta so should be written
            self.sparse_series_writer.append(dps[3], False)
            self.assertEqual(get_points_written(datasource), 3)
            self.sparse_series_writer.sparse_store.flush()

            manifest_dict = \
                FileBackedSparseStore.parse_datasources_from_manifest(
                    self.test_dir)
            self.assertEqual(manifest_dict[datasource].points_written, 3)
            json_filename = manifest_dict[datasource].filename
            self._verify_sparse_store_json(json_filename, 3)

        # We have 3 datapoints for both series + 2 exploration points
        self.assertEqual(
            self.sparse_series_writer.sparse_store.points_written, 8)
        self.sparse_series_writer.sparse_store.close()

    def _verify_sparse_store_json(self, filename: str, num_dps: int) -> None:
        filepath = os.path.join(self.test_dir, filename)
        with open(filepath, 'r') as file:
            lines = file.readlines()
            parsed = [json.loads(line) for line in lines]
            self.assertEqual(len(parsed), num_dps)
