"""
Unittest for SparseSeriesData class
"""

import os
import time
from unittest import TestCase
from unittest.mock import Mock

from nbdb.common.context import Context
from nbdb.common.data_point import DataPoint, MODE_ROLLUP
from nbdb.common.data_point import MISSING_POINT_VALUE, TOMBSTONE_VALUE
from nbdb.common.metric_parsers import CLUSTER_TAG_KEY
from nbdb.common.telemetry import Telemetry
from nbdb.config.settings import Settings
from nbdb.schema.schema import Schema, ROLLUP_LAST, ROLLUP_MAX, ROLLUP_MEAN
from nbdb.schema.schema import ROLLUP_SUM
from nbdb.store.rollups import Rollups
from nbdb.store.sparse_algo_selector import SparseAlgoSelector
from nbdb.store.sparse_series_stats import SparseSeriesStats


class TestRollups(TestCase):
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
        sparse_algo_selector = SparseAlgoSelector(
            self.context,
            Settings.inst.sparse_store.sparse_telemetry,
            Settings.inst.realtime_metric_consumer,
            Mock(),
            MODE_ROLLUP
            )
        self.mock_sparse_store = Mock()
        self.rollups = Rollups(rollup_settings=Settings.inst.sparse_store.rollups,
                               schema=schema,
                               sparse_store=self.mock_sparse_store,
                               sparse_algo_selector=sparse_algo_selector)

        self._unit_test_real_time_clock = 0

    def test_append_metric_with_mean(self) -> None:
        """
        Test writing a metric to sparse_series_writer
        """
        # create the stat object
        stat = SparseSeriesStats()
        self.mock_sparse_store.reset_mock()
        se = 3600
        # 1st dp
        dp1 = DataPoint('m', 'f', {CLUSTER_TAG_KEY: '0'},
                        se, int(time.time()), 100)
        self.rollups.add(dp1, stat, replay_mode=False)
        self.assertEqual(0, stat.get_rollup_intermediate_value(3600))
        self.assertEqual(0, stat.get_rollup_intermediate_value(7200))
        self.assertEqual(se, stat.get_check_point(3600, 0))
        self.assertEqual(0, stat.get_check_point(7200, 0))
        self.assertEqual(0, self.mock_sparse_store.write.call_count)

        stat.set_window_value(0, dp1.value)
        stat.set_window_epoch(0, dp1.epoch)

        dp2 = DataPoint('m', 'f', {CLUSTER_TAG_KEY: '0'},
                        se + 200, int(time.time()), 200)
        self.rollups.add(dp2, stat, replay_mode=False)
        self.assertEqual(100*200,
                         stat.get_rollup_intermediate_value(3600))
        self.assertEqual(100*200,
                         stat.get_rollup_intermediate_value(7200))
        self.assertEqual(se, stat.get_check_point(3600))
        self.assertEqual(0, stat.get_check_point(7200))
        self.assertEqual(0, self.mock_sparse_store.write.call_count)

        stat.set_window_value(0, dp2.value)
        stat.set_window_epoch(0, dp2.epoch)

        dp3 = DataPoint('m', 'f', {CLUSTER_TAG_KEY: '0'},
                        3700 + se, int(time.time()), 300)
        self.rollups.add(dp3, stat, replay_mode=False)
        self.assertEqual(200*(3700-3600),
                         stat.get_rollup_intermediate_value(3600))
        self.assertEqual(200*(3700 + se - 7200),
                         stat.get_rollup_intermediate_value(7200))
        self.assertEqual((100*200 + 200*(3600-200))/3600,
                         stat.get_window_value(3600))
        self.assertEqual((100*200 + 200*(7200-se-200))/(7200-se),
                         stat.get_window_value(7200))
        self.assertEqual(se + 3600, stat.get_check_point(3600))
        self.assertEqual(7200, stat.get_check_point(7200))
        # 3600 & 7200 window values must be written by now
        self.assertEqual(2, self.mock_sparse_store.write.call_count)

        stat.set_window_value(0, dp3.value)
        stat.set_window_epoch(0, dp3.epoch)

        # dp4 valid for 1 additional hour
        dp4 = DataPoint('m', 'f', {CLUSTER_TAG_KEY: '0'},
                        7200 + se, int(time.time()), 400)
        self.rollups.add(dp4, stat, replay_mode=False)
        self.assertEqual(0, stat.get_rollup_intermediate_value(3600))
        self.assertEqual(200*(3700 + se - 7200) + 3500*300,
                         stat.get_rollup_intermediate_value(7200))
        self.assertEqual((100*200 + 3500*300)/3600,
                         stat.get_window_value(3600))
        self.assertEqual((100*200 + 200*(7200-se-200))/(7200-se),
                         stat.get_window_value(7200))
        self.assertEqual(se + 7200, stat.get_check_point(3600))
        self.assertEqual(7200, stat.get_check_point(7200))
        # One additional value will be written for 3600 window
        self.assertEqual(3, self.mock_sparse_store.write.call_count)

        stat.set_window_value(0, dp4.value)
        stat.set_window_epoch(0, dp4.epoch)

        # dp5 comes in 1 hour after dp4
        dp5 = DataPoint('m', 'f', {CLUSTER_TAG_KEY: '0'},
                        10800 + se, int(time.time()), 500)
        self.rollups.add(dp5, stat, replay_mode=False)
        self.assertEqual(0, stat.get_rollup_intermediate_value(3600))
        self.assertEqual(0, stat.get_rollup_intermediate_value(7200))
        self.assertEqual(400, stat.get_window_value(3600))
        self.assertEqual((100*200 + 3500*300 + 3600*400)/7200,
                         stat.get_window_value(7200))
        self.assertEqual(14400, stat.get_check_point(3600))
        self.assertEqual(14400, stat.get_check_point(7200))
        # Values for both 3600 & 7200 get written
        self.assertEqual(5, self.mock_sparse_store.write.call_count)

        stat.set_window_value(0, dp5.value)
        stat.set_window_epoch(0, dp5.epoch)

        # We get a momentary blip ie. points go missing after dp5. And about 50
        # mins after dp5 value was received, dp6 appears. SparseSeriesWriter
        # will insert a MISSING_POINT_VALUE 10 mins after dp5
        dp_missing = DataPoint('m', 'f', {CLUSTER_TAG_KEY: '0'},
                               10800 + 600 + se,
                               int(time.time()), MISSING_POINT_VALUE,
                               is_special_value=True)
        self.rollups.add(dp_missing, stat, replay_mode=False)
        self.assertEqual(500*600, stat.get_rollup_intermediate_value(3600))
        self.assertEqual(500*600, stat.get_rollup_intermediate_value(7200))
        self.assertEqual(400, stat.get_window_value(3600))
        self.assertEqual((100*200 + 3500*300 + 3600*400)/7200,
                         stat.get_window_value(7200))
        self.assertEqual(14400, stat.get_check_point(3600))
        self.assertEqual(14400, stat.get_check_point(7200))
        self.assertEqual(5, self.mock_sparse_store.write.call_count)

        stat.set_window_value(0, dp_missing.value)
        stat.set_window_epoch(0, dp_missing.epoch)

        dp6 = DataPoint('m', 'f', {CLUSTER_TAG_KEY: '0'},
                        10800 + 3000 + se, int(time.time()), 100)
        self.rollups.add(dp6, stat, replay_mode=False)
        self.assertEqual(500*600, stat.get_rollup_intermediate_value(3600))
        self.assertEqual(500*600, stat.get_rollup_intermediate_value(7200))
        self.assertEqual(400, stat.get_window_value(3600))
        self.assertEqual((100*200 + 3500*300 + 3600*400)/7200,
                         stat.get_window_value(7200))
        self.assertEqual(14400, stat.get_check_point(3600))
        self.assertEqual(14400, stat.get_check_point(7200))
        self.assertEqual(5, self.mock_sparse_store.write.call_count)

        stat.set_window_value(0, dp6.value)
        stat.set_window_epoch(0, dp6.epoch)

        # Add another datapoint to complete previous window
        dp7 = DataPoint('m', 'f', {CLUSTER_TAG_KEY: '0'},
                        14400 + se, int(time.time()), 600)
        self.rollups.add(dp7, stat, replay_mode=False)
        self.assertEqual(0, stat.get_rollup_intermediate_value(3600))
        # dp5 was active for 10m and dp6 was active for 10m till dp7 arrived
        self.assertEqual(500 * 600 + 100 * 600,
                         stat.get_rollup_intermediate_value(7200))
        # For the 3600 window, we only had 2 non-NULL datapoints dp5 & dp6
        # active for 10m each. So the avg should be 300
        self.assertEqual((500+100)/2, stat.get_window_value(3600))
        self.assertEqual((100*200 + 3500*300 + 3600*400)/7200,
                         stat.get_window_value(7200))
        self.assertEqual(18000, stat.get_check_point(3600))
        self.assertEqual(14400, stat.get_check_point(7200))
        # One additional 3600 window value should be written
        self.assertEqual(6, self.mock_sparse_store.write.call_count)

        stat.set_window_value(0, dp7.value)
        stat.set_window_epoch(0, dp7.epoch)

        # Add a tombstone value indicating the series is dead. We should
        # generate extra datapoints for windows with intermediate computations,
        # and generate tombstones for each window
        dp_tombstone = DataPoint('m', 'f', {CLUSTER_TAG_KEY: '0'},
                                 14400 + 600 + se,
                                 int(time.time()), TOMBSTONE_VALUE,
                                 is_special_value=True)
        self.rollups.add(dp_tombstone, stat, replay_mode=False)
        # We should be storing TOMBSTONE_VALUE for next checkpoint + window
        self.assertEqual(TOMBSTONE_VALUE, stat.get_window_value(3600))
        self.assertEqual(18000 + 3600 + 3600, stat.get_window_epoch(3600))
        self.assertEqual(TOMBSTONE_VALUE, stat.get_window_value(7200))
        self.assertEqual(14400 + 7200 + 7200, stat.get_window_epoch(7200))
        # Four more writes should be generated: 2 writes for the intermediate
        # state at next_checkpoint & 2 writes for tombstone values at
        # next_checkpoint + 10m
        self.assertEqual(10, self.mock_sparse_store.write.call_count)
        # 3600 intermediate datapoint
        self.assertEqual(18000 + 3600,
                         self.mock_sparse_store.mock_calls[-4][1][0].epoch)
        self.assertEqual((600*600)/600,
                         self.mock_sparse_store.mock_calls[-4][1][0].value)
        self.assertTrue(
            self.mock_sparse_store.mock_calls[-4][1][0].datasource.endswith(
                '3600'))
        # 3600 tombstone
        self.assertEqual(18000 + 3600 + 3600,
                         self.mock_sparse_store.mock_calls[-3][1][0].epoch)
        self.assertEqual(TOMBSTONE_VALUE,
                         self.mock_sparse_store.mock_calls[-3][1][0].value)
        self.assertTrue(
            self.mock_sparse_store.mock_calls[-3][1][0].datasource.endswith(
                '3600'))
        # 7200 intermediate datapoint
        self.assertEqual(14400 + 7200,
                         self.mock_sparse_store.mock_calls[-2][1][0].epoch)
        self.assertEqual((500*600 + 100*600 + 600*600)/1800,
                         self.mock_sparse_store.mock_calls[-2][1][0].value)
        self.assertTrue(
            self.mock_sparse_store.mock_calls[-2][1][0].datasource.endswith(
                '7200'))
        # 7200 tombstone
        self.assertEqual(14400 + 7200 + 7200,
                         self.mock_sparse_store.mock_calls[-1][1][0].epoch)
        self.assertEqual(TOMBSTONE_VALUE,
                         self.mock_sparse_store.mock_calls[-1][1][0].value)
        self.assertTrue(
            self.mock_sparse_store.mock_calls[-1][1][0].datasource.endswith(
                '7200'))

        stat.set_window_value(0, dp_tombstone.value)
        stat.set_window_epoch(0, dp_tombstone.epoch)

    def test_missing_points_spanning_mul_windows(self) -> None:
        """
        Test handling of missing points spanning multiple windows
        """
        for rollup_function in [ROLLUP_MEAN, ROLLUP_MAX, ROLLUP_LAST,
                                ROLLUP_SUM]:
            self.context.schema.get_rollup_function = lambda x: rollup_function
            # create the stat object
            stat = SparseSeriesStats()
            se = 3600
            # 1st dp
            dp1 = DataPoint('m', 'f', {CLUSTER_TAG_KEY: '0'},
                            se, int(time.time()), 100)
            self.rollups.add(dp1, stat, replay_mode=False)
            # first time stats is not updated
            self.assertEqual(0, stat.get_rollup_intermediate_value(3600))
            self.assertEqual(0, stat.get_rollup_intermediate_value(7200))
            self.assertEqual(3600, stat.get_check_point(3600, 0))
            self.assertEqual(0, stat.get_check_point(7200, 0))

            stat.set_window_value(0, dp1.value)
            stat.set_window_epoch(0, dp1.epoch)

            dp2 = DataPoint('m', 'f', {CLUSTER_TAG_KEY: '0'},
                            se + 200, int(time.time()), 200)
            self.rollups.add(dp2, stat, replay_mode=False)
            if rollup_function == ROLLUP_MEAN:
                intermediate = 100*200
            elif rollup_function == ROLLUP_MAX:
                intermediate = 100
            elif rollup_function == ROLLUP_LAST:
                intermediate = 100
            else:
                # Rollup sum
                intermediate = 100*200
            self.assertEqual(intermediate,
                             stat.get_rollup_intermediate_value(3600))
            self.assertEqual(intermediate,
                             stat.get_rollup_intermediate_value(7200))
            self.assertEqual(se, stat.get_check_point(3600))
            self.assertEqual(0, stat.get_check_point(7200))

            stat.set_window_value(0, dp2.value)
            stat.set_window_epoch(0, dp2.epoch)

            # Simulate a blip which spans multiple windows. Insert a missing
            # point marker 10m after dp2 and another datapoint dp3 multiple
            # windows later
            dp_missing = DataPoint('m', 'f', {CLUSTER_TAG_KEY: '0'},
                                   se + 200 + 600, int(time.time()),
                                   MISSING_POINT_VALUE, is_special_value=True)
            self.rollups.add(dp_missing, stat, replay_mode=False)
            stat.set_window_value(0, dp_missing.value)
            stat.set_window_epoch(0, dp_missing.epoch)

            # dp3 is 4 windows away from dp3 for the 3600 rollup and 2 windows
            # away for the 7200 rollup
            dp3 = DataPoint('m', 'f', {CLUSTER_TAG_KEY: '0'},
                            se + 14400 + 200, int(time.time()), 300)
            self.rollups.add(dp3, stat, replay_mode=False)
            # Verify that the last window values for both windows were the
            # missing point marker
            self.assertEqual(18000, stat.get_check_point(3600))
            self.assertEqual(MISSING_POINT_VALUE, stat.get_window_value(3600))
            self.assertEqual(14400, stat.get_check_point(7200))
            self.assertEqual(MISSING_POINT_VALUE, stat.get_window_value(7200))

    def test_append_metric_with_last(self) -> None:
        """
        Test writing a metric to sparse_series_writer
        """
        self.context.schema.get_rollup_function = lambda x: ROLLUP_LAST
        # create the stat object
        stat = SparseSeriesStats()
        se = 3600
        # 1st dp
        dp1 = DataPoint('m', 'f', {CLUSTER_TAG_KEY: '0'},
                        se, int(time.time()), 100)
        self.rollups.add(dp1, stat, replay_mode=False)
        # first time stats is not updated
        self.assertEqual(0, stat.get_rollup_intermediate_value(3600))
        self.assertEqual(0, stat.get_rollup_intermediate_value(7200))
        self.assertEqual(3600, stat.get_check_point(3600, 0))
        self.assertEqual(0, stat.get_check_point(7200, 0))

        stat.set_window_value(0, dp1.value)
        stat.set_window_epoch(0, dp1.epoch)

        dp2 = DataPoint('m', 'f', {CLUSTER_TAG_KEY: '0'},
                        se + 200, int(time.time()), 200)
        self.rollups.add(dp2, stat, replay_mode=False)
        self.assertEqual(100, stat.get_rollup_intermediate_value(3600))
        self.assertEqual(100, stat.get_rollup_intermediate_value(7200))
        self.assertEqual(3600, stat.get_check_point(3600))
        self.assertEqual(0, stat.get_check_point(7200))

        stat.set_window_value(0, dp2.value)
        stat.set_window_epoch(0, dp2.epoch)

        dp3 = DataPoint('m', 'f', {CLUSTER_TAG_KEY: '0'},
                        3700 + se, int(time.time()), 300)
        self.rollups.add(dp3, stat, replay_mode=False)
        self.assertEqual(200, stat.get_rollup_intermediate_value(3600))
        self.assertEqual(200, stat.get_rollup_intermediate_value(7200))
        self.assertEqual(200, stat.get_window_value(3600))
        self.assertEqual(200, stat.get_window_value(7200))
        self.assertEqual(se + 3600, stat.get_check_point(3600))
        self.assertEqual(7200, stat.get_check_point(7200))

        stat.set_window_value(0, dp3.value)
        stat.set_window_epoch(0, dp3.epoch)

        # dp4 valid for 1 additional hour
        dp4 = DataPoint('m', 'f', {CLUSTER_TAG_KEY: '0'},
                        7200 + se, int(time.time()), 400)
        self.rollups.add(dp4, stat, replay_mode=False)
        self.assertEqual(300, stat.get_rollup_intermediate_value(3600))
        self.assertEqual(300, stat.get_rollup_intermediate_value(7200))
        self.assertEqual(300, stat.get_window_value(3600))
        self.assertEqual(200, stat.get_window_value(7200))
        self.assertEqual(se + 7200, stat.get_check_point(3600))
        self.assertEqual(7200, stat.get_check_point(7200))

        stat.set_window_value(0, dp4.value)
        stat.set_window_epoch(0, dp4.epoch)

        # dp5 valid for 1 additional hour
        dp5 = DataPoint('m', 'f', {CLUSTER_TAG_KEY: '0'},
                        10800 + se, int(time.time()), 500)
        self.rollups.add(dp5, stat, replay_mode=False)
        self.assertEqual(400, stat.get_rollup_intermediate_value(3600))
        self.assertEqual(400, stat.get_rollup_intermediate_value(7200))
        self.assertEqual(400, stat.get_window_value(3600))
        self.assertEqual(400, stat.get_window_value(7200))
        self.assertEqual(14400, stat.get_check_point(3600))
        self.assertEqual(14400, stat.get_check_point(7200))

        stat.set_window_value(0, dp5.value)
        stat.set_window_epoch(0, dp5.epoch)

    def test_append_metric_with_max(self) -> None:
        """
        Test writing a metric to sparse_series_writer
        """
        self.context.schema.get_rollup_function = lambda x: ROLLUP_MAX
        # create the stat object
        stat = SparseSeriesStats()
        se = 3600
        # 1st dp
        dp1 = DataPoint('m', 'f', {CLUSTER_TAG_KEY: '0'},
                        se, int(time.time()), 200)
        self.rollups.add(dp1, stat, replay_mode=False)
        # first time stats is not updated
        self.assertEqual(0, stat.get_rollup_intermediate_value(3600))
        self.assertEqual(0, stat.get_rollup_intermediate_value(7200))
        self.assertEqual(3600, stat.get_check_point(3600, 0))
        self.assertEqual(0, stat.get_check_point(7200, 0))

        stat.set_window_value(0, dp1.value)
        stat.set_window_epoch(0, dp1.epoch)

        dp2 = DataPoint('m', 'f', {CLUSTER_TAG_KEY: '0'},
                        se + 200, int(time.time()), 100)
        self.rollups.add(dp2, stat, replay_mode=False)
        self.assertEqual(200, stat.get_rollup_intermediate_value(3600))
        self.assertEqual(200, stat.get_rollup_intermediate_value(7200))
        self.assertEqual(se, stat.get_check_point(3600))
        self.assertEqual(0, stat.get_check_point(7200))

        stat.set_window_value(0, dp2.value)
        stat.set_window_epoch(0, dp2.epoch)

        dp3 = DataPoint('m', 'f', {CLUSTER_TAG_KEY: '0'},
                        3700 + se, int(time.time()), 300)
        self.rollups.add(dp3, stat, replay_mode=False)
        self.assertEqual(100, stat.get_rollup_intermediate_value(3600))
        self.assertEqual(100, stat.get_rollup_intermediate_value(7200))
        self.assertEqual(200, stat.get_window_value(3600))
        self.assertEqual(200, stat.get_window_value(7200))
        self.assertEqual(se + 3600, stat.get_check_point(3600))
        self.assertEqual(7200, stat.get_check_point(7200))

        stat.set_window_value(0, dp3.value)
        stat.set_window_epoch(0, dp3.epoch)

        # dp4 valid for 1 additional hour
        dp4 = DataPoint('m', 'f', {CLUSTER_TAG_KEY: '0'},
                        7200 + se, int(time.time()), 400)
        self.rollups.add(dp4, stat, replay_mode=False)
        self.assertEqual(300, stat.get_rollup_intermediate_value(3600))
        self.assertEqual(300, stat.get_rollup_intermediate_value(7200))
        self.assertEqual(300, stat.get_window_value(3600))
        self.assertEqual(200, stat.get_window_value(7200))
        self.assertEqual(se + 7200, stat.get_check_point(3600))
        self.assertEqual(7200, stat.get_check_point(7200))

        stat.set_window_value(0, dp4.value)
        stat.set_window_epoch(0, dp4.epoch)

        # dp5 valid for 1 additional hour
        dp5 = DataPoint('m', 'f', {CLUSTER_TAG_KEY: '0'},
                        10800 + se, int(time.time()), 500)
        self.rollups.add(dp5, stat, replay_mode=False)
        self.assertEqual(400, stat.get_rollup_intermediate_value(3600))
        self.assertEqual(400, stat.get_rollup_intermediate_value(7200))
        self.assertEqual(400, stat.get_window_value(3600))
        self.assertEqual(400, stat.get_window_value(7200))
        self.assertEqual(14400, stat.get_check_point(3600))
        self.assertEqual(14400, stat.get_check_point(7200))

        stat.set_window_value(0, dp5.value)
        stat.set_window_epoch(0, dp5.epoch)

    def test_rollup_with_long_sparse_point(self) -> None:
        """
        Tests rollup when a single sparse point spans multiple rollup
        windows
        :return:
        """
        # create the stat object
        stat = SparseSeriesStats()
        se = 3600
        # 1st dp
        dp1 = DataPoint('m', 'f', {CLUSTER_TAG_KEY: '0'},
                        se, int(time.time()), 100)
        self.rollups.add(dp1, stat, replay_mode=False)
        # first time stats is not updated
        self.assertEqual(0, stat.get_rollup_intermediate_value(3600))
        self.assertEqual(0, stat.get_rollup_intermediate_value(7200))
        self.assertEqual(3600, stat.get_check_point(3600, 0))
        self.assertEqual(0, stat.get_check_point(7200, 0))

        stat.set_window_value(0, dp1.value)
        stat.set_window_epoch(0, dp1.epoch)

        # Next data point comes a day later, so dp1 needs to generate
        # multiple roll up data points
        dp2 = DataPoint('m', 'f', {CLUSTER_TAG_KEY: '0'},
                        se + 86400 + 20, int(time.time()), 200)
        self.rollups.add(dp2, stat, replay_mode=False)
        self.assertEqual(20*100,
                         stat.get_rollup_intermediate_value(3600))
        self.assertEqual(3620*100,
                         stat.get_rollup_intermediate_value(7200))
        self.assertEqual(se + 86400, stat.get_check_point(3600, 0))
        self.assertEqual(86400, stat.get_check_point(7200, 0))
        self.assertEqual(100, stat.get_window_value(3600))
        self.assertEqual(100, stat.get_window_value(7200))

        stat.set_window_value(0, dp2.value)
        stat.set_window_epoch(0, dp2.epoch)

    def test_rollups_replay_mode(self) -> None:
        """
        Test rollups during replay mode
        """
        # Create the stat object
        stat = SparseSeriesStats()
        self.mock_sparse_store.reset_mock()
        # First three datapoints will be received in replay mode
        se = 3600
        # 1st dp
        dp1 = DataPoint('m', 'f', {CLUSTER_TAG_KEY: '0'},
                        se, int(time.time()), 200)
        self.rollups.add(dp1, stat, replay_mode=True)
        # first time stats is not updated
        self.assertEqual(0, stat.get_rollup_intermediate_value(3600))
        self.assertEqual(0, stat.get_rollup_intermediate_value(7200))
        self.assertEqual(3600, stat.get_check_point(3600, 0))
        self.assertEqual(0, stat.get_check_point(7200, 0))
        self.assertEqual(0, self.mock_sparse_store.write.call_count)

        stat.set_window_value(0, dp1.value)
        stat.set_window_epoch(0, dp1.epoch)
        stat.set_replay_mode(True)

        dp2 = DataPoint('m', 'f', {CLUSTER_TAG_KEY: '0'},
                        se + 200, int(time.time()), 200)
        self.rollups.add(dp2, stat, replay_mode=True)
        self.assertEqual(200*200,
                         stat.get_rollup_intermediate_value(3600))
        self.assertEqual(200*200,
                         stat.get_rollup_intermediate_value(7200))
        self.assertEqual(se, stat.get_check_point(3600))
        self.assertEqual(0, stat.get_check_point(7200))
        self.assertEqual(0, self.mock_sparse_store.write.call_count)

        stat.set_window_value(0, dp2.value)
        stat.set_window_epoch(0, dp2.epoch)
        stat.set_replay_mode(True)

        dp3 = DataPoint('m', 'f', {CLUSTER_TAG_KEY: '0'},
                        3700 + se, int(time.time()), 200)
        self.rollups.add(dp3, stat, replay_mode=True)
        self.assertEqual(200*(3700-3600),
                         stat.get_rollup_intermediate_value(3600))
        self.assertEqual(200*(3700 + se - 7200),
                         stat.get_rollup_intermediate_value(7200))
        self.assertEqual((200*200 + 200*(3600-200))/3600,
                         stat.get_window_value(3600))
        self.assertEqual((200*200 + 200*(7200-se-200))/(7200-se),
                         stat.get_window_value(7200))
        self.assertEqual(se + 3600, stat.get_check_point(3600))
        self.assertEqual(7200, stat.get_check_point(7200))
        # Even though window values were generated for both 3600 and 7200, no
        # writes should be done since dp3 was received in replay mode
        self.assertEqual(0, self.mock_sparse_store.write.call_count)

        stat.set_window_value(0, dp3.value)
        stat.set_window_epoch(0, dp3.epoch)
        stat.set_replay_mode(True)

        # dp4 comes 1 hour after dp3 and is the first non-replay message.
        # Even though the 3600 rollup value generated is the same as before, it
        # will be written because it's the first value after transition from
        # replay mode to non-replay mode
        dp4 = DataPoint('m', 'f', {CLUSTER_TAG_KEY: '0'},
                        7300 + se, int(time.time()), 200)
        self.rollups.add(dp4, stat, replay_mode=False)
        self.assertEqual(200*100,
                         stat.get_rollup_intermediate_value(3600))
        self.assertEqual(200*100 + 200*(7300-3700),
                         stat.get_rollup_intermediate_value(7200))
        self.assertEqual((100*200 + 200*(7200-3700))/3600,
                         stat.get_window_value(3600))
        self.assertEqual((200*100 + 200*(7200-3700))/(7200-se),
                         stat.get_window_value(7200))
        self.assertEqual(se + 7200, stat.get_check_point(3600))
        self.assertEqual(7200, stat.get_check_point(7200))
        # Even though the 3600 rollup value is the same as the last time, it
        # should still be written
        self.assertEqual(1, self.mock_sparse_store.write.call_count)

        stat.set_window_value(0, dp4.value)
        stat.set_window_epoch(0, dp4.epoch)
        stat.set_replay_mode(False)

    def test_single_datapoint_series(self) -> None:
        """
        Test writing a series to sparse_series_writer which generates a single
        datapoint in its lifetime
        """
        # create the stat object
        stat = SparseSeriesStats()
        self.mock_sparse_store.reset_mock()
        se = 3800
        # 1st dp
        dp1 = DataPoint('m', 'f', {CLUSTER_TAG_KEY: '0'},
                        se, int(time.time()), 100)
        self.rollups.add(dp1, stat, replay_mode=False)
        self.assertEqual(0, stat.get_rollup_intermediate_value(3600))
        self.assertEqual(0, stat.get_rollup_intermediate_value(7200))
        self.assertEqual(3600, stat.get_check_point(3600, 0))
        self.assertEqual(0, stat.get_check_point(7200, 0))
        self.assertEqual(0, self.mock_sparse_store.write.call_count)

        stat.set_window_value(0, dp1.value)
        stat.set_window_epoch(0, dp1.epoch)

        # Add a tombstone value indicating the series is dead. We should
        # generate extra datapoints for windows with intermediate computations,
        # and generate tombstones for each window
        dp_tombstone = DataPoint('m', 'f', {CLUSTER_TAG_KEY: '0'},
                                 se + 600,
                                 int(time.time()), TOMBSTONE_VALUE,
                                 is_special_value=True)
        self.rollups.add(dp_tombstone, stat, replay_mode=False)
        # We should be storing TOMBSTONE_VALUE for next checkpoint + window
        self.assertEqual(TOMBSTONE_VALUE, stat.get_window_value(3600))
        self.assertEqual(3600 + 2*3600, stat.get_window_epoch(3600))
        self.assertEqual(TOMBSTONE_VALUE, stat.get_window_value(7200))
        self.assertEqual(7200 + 7200, stat.get_window_epoch(7200))
        # Four writes should be generated: 2 writes for the intermediate
        # state at next_checkpoint & 2 writes for tombstone values at
        # next_checkpoint + 10m
        self.assertEqual(4, self.mock_sparse_store.write.call_count)
        # 3600 intermediate datapoint
        self.assertEqual(3600 + 3600,
                         self.mock_sparse_store.mock_calls[-4][1][0].epoch)
        self.assertEqual((100*600)/600,
                         self.mock_sparse_store.mock_calls[-4][1][0].value)
        self.assertTrue(
            self.mock_sparse_store.mock_calls[-4][1][0].datasource.endswith(
                '3600'))
        # 3600 tombstone
        self.assertEqual(3600 + 2*3600,
                         self.mock_sparse_store.mock_calls[-3][1][0].epoch)
        self.assertEqual(TOMBSTONE_VALUE,
                         self.mock_sparse_store.mock_calls[-3][1][0].value)
        self.assertTrue(
            self.mock_sparse_store.mock_calls[-3][1][0].datasource.endswith(
                '3600'))
        # 7200 intermediate datapoint
        self.assertEqual(7200,
                         self.mock_sparse_store.mock_calls[-2][1][0].epoch)
        self.assertEqual((100*600)/600,
                         self.mock_sparse_store.mock_calls[-2][1][0].value)
        self.assertTrue(
            self.mock_sparse_store.mock_calls[-2][1][0].datasource.endswith(
                '7200'))
        # 7200 tombstone
        self.assertEqual(7200 + 7200,
                         self.mock_sparse_store.mock_calls[-1][1][0].epoch)
        self.assertEqual(TOMBSTONE_VALUE,
                         self.mock_sparse_store.mock_calls[-1][1][0].value)
        self.assertTrue(
            self.mock_sparse_store.mock_calls[-1][1][0].datasource.endswith(
                '7200'))

        stat.set_window_value(0, dp_tombstone.value)
        stat.set_window_epoch(0, dp_tombstone.epoch)

    def test_partial_windows_on_both_ends(self) -> None:
        """
        Simulate a series whose start & end time result in partial windows at
        the beginning & end. We should not drop the partial windows
        """
        # create the stat object
        stat = SparseSeriesStats()
        self.mock_sparse_store.reset_mock()
        se = 3800

        # Use sum() for computing rollups
        self.context.schema.get_rollup_function = lambda x: ROLLUP_SUM

        # Generate multiple datapoints at regular 10m frequency
        for epoch in range(se, 10800 + 1800, 600):
            dp = DataPoint('m', 'f', {CLUSTER_TAG_KEY: '0'},
                           epoch, int(time.time()), 100)
            self.rollups.add(dp, stat, replay_mode=False)
            stat.set_window_value(0, dp.value)
            stat.set_window_epoch(0, dp.epoch)

        # Add a tombstone value indicating the series is dead. We should
        # generate extra datapoints for windows with intermediate computations,
        # and generate tombstones for each window
        dp_tombstone = DataPoint('m', 'f', {CLUSTER_TAG_KEY: '0'},
                                 10800 + 1800,
                                 int(time.time()), TOMBSTONE_VALUE,
                                 is_special_value=True)
        self.rollups.add(dp_tombstone, stat, replay_mode=False)

        # We should see 4 writes for the 3600 window and 3 writes for the 7200
        # window
        self.assertEqual(7, self.mock_sparse_store.write.call_count)
        rollup_dps_3600 = [call[1][0] for call in
                           self.mock_sparse_store.mock_calls
                           if call[1][0].datasource.endswith('3600')]
        rollup_dps_7200 = [call[1][0] for call in
                           self.mock_sparse_store.mock_calls
                           if call[1][0].datasource.endswith('7200')]
        # Verify that we generated the following values for the 3600
        # datasource:
        #
        # Epoch 7200:  340000
        # Epoch 10800: 360000
        # Epoch 14400: 180000
        # Epoch 18000: TOMBSTONE
        self.assertEqual(rollup_dps_3600[0].epoch, 7200)
        self.assertEqual(rollup_dps_3600[0].value, 100*(7200-se))
        self.assertEqual(rollup_dps_3600[1].epoch, 7200 + 3600)
        self.assertEqual(rollup_dps_3600[1].value, 100*3600)
        self.assertEqual(rollup_dps_3600[2].epoch, 7200 + 2*3600)
        self.assertEqual(rollup_dps_3600[2].value, 100*1800)
        self.assertEqual(rollup_dps_3600[3].epoch, 7200 + 3*3600)
        self.assertEqual(rollup_dps_3600[3].value, TOMBSTONE_VALUE)

        # Verify that we generated the following values for the 7200
        # datasource:
        #
        # Epoch 7200:  340000
        # Epoch 14400: 540000
        # Epoch 18000: TOMBSTONE
        self.assertEqual(rollup_dps_7200[0].epoch, 7200)
        self.assertEqual(rollup_dps_7200[0].value, 100*(7200-se))
        self.assertEqual(rollup_dps_7200[1].epoch, 7200 + 7200)
        self.assertEqual(rollup_dps_7200[1].value, 100*(10800+1800-7200))
        self.assertEqual(rollup_dps_7200[2].epoch, 7200 + 2*7200)
        self.assertEqual(rollup_dps_7200[2].value, TOMBSTONE_VALUE)
