"""
Unittests for sparse series aggregators
"""

import os
from unittest import TestCase

from nbdb.common.data_point import DataPoint
from nbdb.common.data_point import MISSING_POINT_VALUE, TOMBSTONE_VALUE
from nbdb.config.settings import Settings
from nbdb.readapi.down_sampler_sparse_mean import DownSamplerSparseMean
from nbdb.readapi.down_sampler_dense_mean import DownSamplerDenseMean
from nbdb.readapi.sql_parser import FillFunc
from nbdb.readapi.time_series_response import TimeRange


class TestAggregators(TestCase):
    """
    Tests the aggregators with sparse data and verifies the correctness
    """
    def setUp(self):
        Settings.load_yaml_settings(os.path.dirname(__file__) +
                                    '/test_settings.yaml')

    def test_aggregator_base_negative(self) -> None:
        """
        Test for incorrect usage
        """
        aggregator_mean = DownSamplerSparseMean(TimeRange(10, 50, 10),
                                                fill_func=FillFunc.NULL,
                                                fill_value=None,
                                                default_fill_value=None,
                                                trace=False)
        aggregator_mean.process(10, 20)
        aggregator_mean.process(15, 4)
        # out of order processing is not allowed
        self.assertFalse(aggregator_mean.process(12, 4))

        # providing data beyond the defined end_epoch (50) is not allowed
        self.assertFalse(aggregator_mean.process(60, 5))

    def test_mean(self) -> None:
        """
        Test the time-weighted mean aggregator
        """
        aggregator_mean = DownSamplerSparseMean(TimeRange(10, 50, 10),
                                                fill_func=FillFunc.NULL,
                                                fill_value=None,
                                                default_fill_value=None,
                                                trace=False)
        aggregator_mean.process(5, 10)
        aggregator_mean.process(20, 5)
        aggregator_mean.process(40, 10)
        aggregator_mean.complete()
        self.assertEqual([10*10/10,  # 10 - 20
                          5*10/10,   # 20 - 30
                          5*10/10,   # 30 - 40
                          10*10/10,  # 40 - 50
                          ], aggregator_mean.buckets.values)

    def test_mean_bundles_non_overlap(self) -> None:
        """
        Test the time-weighted mean aggregator with non-overlapping bundles.
        Atleast some tombstone markers should be processed.
        """
        aggregator_mean_1 = DownSamplerSparseMean(TimeRange(10, 50, 10),
                                                  fill_func=FillFunc.NULL,
                                                  fill_value=None,
                                                  default_fill_value=None,
                                                  trace=False)
        aggregator_mean_1.data_gap_detection_interval = 10

        # No overlap and some data is missed in between the two bundles
        aggregator_mean_1.process(10, 10, 1000)
        aggregator_mean_1.process(20, TOMBSTONE_VALUE, 1000)
        aggregator_mean_1.process(30, 5, 2000)
        aggregator_mean_1.process(40, 10, 2000)
        aggregator_mean_1.process(50, TOMBSTONE_VALUE, 2000)
        aggregator_mean_1.complete()
        self.assertEqual([10 * 10 / 10, # 10 - 20
                          None,         # 20 - 30, no datapoint
                          5 * 10 / 10,  # 30 - 40
                          10 * 10 / 10, # 40 - 50
                          ], aggregator_mean_1.buckets.values)

        # No overlap and no datapoint is missed in between the two bundles
        aggregator_mean_2 = DownSamplerSparseMean(TimeRange(10, 50, 10),
                                                  fill_func=FillFunc.NULL,
                                                  fill_value=None,
                                                  default_fill_value=None,
                                                  trace=False)
        aggregator_mean_2.data_gap_detection_interval = 10
        aggregator_mean_2.process(10, 10, 1000)
        aggregator_mean_2.process(20, TOMBSTONE_VALUE, 1000)
        aggregator_mean_2.process(20, 5, 2000)
        aggregator_mean_2.process(40, 10, 2000)
        aggregator_mean_2.process(50, TOMBSTONE_VALUE, 2000)
        aggregator_mean_2.complete()
        self.assertEqual([10 * 10 / 10,  # 10 - 20
                          5 * 10 / 10 ,  # 20 - 30
                          5 * 10 / 10,  # 30 - 40
                          10 * 10 / 10,  # 40 - 50
                          ], aggregator_mean_2.buckets.values)

    def test_mean_bundles_overlap(self) -> None:
        """
        Test the time-weighted mean aggregator with overlapping bundles
        All tombstone markers except the last should be neglected
        """
        aggregator_mean1 = DownSamplerSparseMean(TimeRange(10, 50, 10),
                                           fill_func=FillFunc.NULL,
                                           fill_value=None,
                                           default_fill_value=None,
                                           trace=False)
        aggregator_mean1.data_gap_detection_interval = 10
        # Overlap between bundles. Bundle 1000 lasts from [5, 30]. Bundle 2000
        # lasts from [20, 50]
        aggregator_mean1.process(5, 10, 1000)
        aggregator_mean1.process(20, 5, 2000)
        aggregator_mean1.process(30, TOMBSTONE_VALUE, 1000)
        aggregator_mean1.process(40, 10, 2000)
        aggregator_mean1.process(50, TOMBSTONE_VALUE, 2000)
        aggregator_mean1.complete()

        self.assertEqual([10 * 10 / 10,  # 10 - 20
                          5 * 10 / 10,  # 20 - 30
                          5 * 10 / 10,  # 30 - 40
                          10 * 10 / 10,  # 40 - 50
                          ], aggregator_mean1.buckets.values)

        aggregator_mean2 = DownSamplerSparseMean(TimeRange(10, 50, 10),
                                                 fill_func=FillFunc.NULL,
                                                 fill_value=None,
                                                 default_fill_value=None,
                                                 trace=False)
        aggregator_mean2.data_gap_detection_interval = 10
        # Bundle 1000 lasts from [10, 30] and has one value at 10
        # Bundle 2000 lasts from [5, 50] and has one value at 5
        aggregator_mean2.process(5, 10, 2000)
        aggregator_mean2.process(10, 10, 1000)
        aggregator_mean2.process(30, TOMBSTONE_VALUE, 1000)
        aggregator_mean2.process(50, TOMBSTONE_VALUE, 2000)
        aggregator_mean2.complete()

        self.assertEqual([10 * 10 / 10,  # 10 - 20
                          10 * 10 / 10,  # 20 - 30
                          10 * 10 / 10,  # 30 - 40
                          10 * 10 / 10,  # 40 - 50
                          ], aggregator_mean2.buckets.values)

    def test_mean_with_fill_value(self) -> None:
        """
        Test the time-weighted mean aggregator with empty values. The empty
        values should be filled in correctly if a fill function is specified.
        """
        missing_pt_intvl = Settings.inst.sparse_store.\
            heartbeat_scan.data_gap_detection_interval
        tombstone_intvl = Settings.inst.sparse_store.\
            heartbeat_scan.termination_detection_interval

        time_range = TimeRange(missing_pt_intvl,
                               missing_pt_intvl*5 + tombstone_intvl,
                               missing_pt_intvl)

        for fill_func in [FillFunc.CONSTANT, FillFunc.PREVIOUS, FillFunc.NULL]:
            if fill_func == FillFunc.NULL:
                fill_value = None
            else:
                fill_value = 5.0
            aggregator_mean = DownSamplerSparseMean(time_range,
                                                    fill_func=fill_func,
                                                    fill_value=fill_value,
                                                    default_fill_value=None,
                                                    trace=False)
            aggregator_mean.process(0.5 * missing_pt_intvl, 10)
            # Invalid marker returned during last value query. Gets discarded
            # due to legit datapoint for same epoch
            aggregator_mean.process(0.5 * missing_pt_intvl, MISSING_POINT_VALUE)
            aggregator_mean.process(2 * missing_pt_intvl, 15)
            aggregator_mean.process(3 * missing_pt_intvl, MISSING_POINT_VALUE)
            aggregator_mean.process(4 * missing_pt_intvl, 10)
            aggregator_mean.process(4 * missing_pt_intvl + missing_pt_intvl,
                                    TOMBSTONE_VALUE)
            if fill_func == FillFunc.PREVIOUS:
                bucket30_40_val = 15.0  # 20-30 value was 15
                bucket50_60_val = 10.0
                bucket60_70_val = 10.0
            elif fill_func == FillFunc.CONSTANT:
                bucket30_40_val = 5.0
                bucket50_60_val = 5.0
                bucket60_70_val = 5.0
            else:
                bucket30_40_val = None
                bucket50_60_val = None
                bucket60_70_val = None
            aggregator_mean.complete()
            self.assertEqual([10.0,                # 1 - 2
                              15.0,                # 2 - 3
                              bucket30_40_val,     # 3 - 4
                              10.0,                # 4 - 5
                              bucket50_60_val,     # 5 - 6
                              bucket60_70_val,     # 6 - 7
                              ], aggregator_mean.buckets.values)

    def test_mean_with_invalid_markers(self) -> None:
        """
        Test the time-weighted mean aggregator with invalid missing or
        tombstone markers. Invalid markers should be discarded when computing
        aggregates.
        """
        missing_pt_intvl = Settings.inst.sparse_store.\
            heartbeat_scan.data_gap_detection_interval

        time_range = TimeRange(missing_pt_intvl,
                               missing_pt_intvl*5,
                               missing_pt_intvl)

        aggregator_mean = DownSamplerSparseMean(time_range,
                                                fill_func=FillFunc.NULL,
                                                fill_value=None,
                                                default_fill_value=None,
                                                trace=False)
        # Invalid marker returned during last value query
        aggregator_mean.process(0.5 * missing_pt_intvl, MISSING_POINT_VALUE)
        aggregator_mean.process(0.5 * missing_pt_intvl, 10)
        aggregator_mean.process(2 * missing_pt_intvl, 5)
        aggregator_mean.process(3 * missing_pt_intvl, 7)
        aggregator_mean.process(3 * missing_pt_intvl, MISSING_POINT_VALUE)
        # Invalid marker
        aggregator_mean.process(4 * missing_pt_intvl, TOMBSTONE_VALUE)
        # Invalid marker
        aggregator_mean.process(4 * missing_pt_intvl, 10)
        aggregator_mean.complete()
        self.assertEqual([10.0,  # 1 - 2
                          5.0,   # 2 - 3
                          7.0,   # 3 - 4
                          10.0,  # 4 - 5
                          ], aggregator_mean.buckets.values)

    def test_mean_with_negative_values(self) -> None:
        """
        Test the time-weighted mean aggregator with negative values esp when
        some of them use reserved values such as -1 or -3
        """
        aggregator_mean = DownSamplerSparseMean(TimeRange(10, 50, 10),
                                                fill_func=FillFunc.NULL,
                                                fill_value=None,
                                                default_fill_value=None,
                                                trace=False)
        aggregator_mean.process(5, DataPoint.normalize_value(-1))
        aggregator_mean.process(20, DataPoint.normalize_value(-9))
        aggregator_mean.process(40, DataPoint.normalize_value(-3))
        aggregator_mean.process(45, DataPoint.normalize_value(-1))
        aggregator_mean.complete()
        self.assertEqual([-1,          # 10 - 20
                          -9,          # 20 - 30
                          -9,          # 30 - 40
                          (-3 - 1)/2,  # 40 - 50
                          ], aggregator_mean.buckets.values)

    def test_mean_with_early_datapoint(self) -> None:
        """
        Test the time-weighted mean aggregator
        """
        aggregator_mean = DownSamplerSparseMean(TimeRange(100, 150, 10),
                                                fill_func=FillFunc.NULL,
                                                fill_value=None,
                                                default_fill_value=None,
                                                trace=False)
        aggregator_mean.process(20, 5.0)
        aggregator_mean.process(50, 10.0)
        aggregator_mean.process(130, 20.0)
        aggregator_mean.complete()
        self.assertEqual([10.0*10/10,   # 100 - 110
                          10.0*10/10,   # 110 - 120
                          10.0*10/10,   # 120 - 130
                          20.0*10/10,   # 130 - 140
                          20.0*10/10,   # 140 - 150
                          ], aggregator_mean.buckets.values)

    def test_mean_with_single_early_datapoint(self) -> None:
        """
        Test the time-weighted mean aggregator
        """
        aggregator_mean = DownSamplerSparseMean(TimeRange(100, 150, 10),
                                                fill_func=FillFunc.NULL,
                                                fill_value=None,
                                                default_fill_value=None,
                                                trace=False)
        aggregator_mean.process(50, 10.0)
        aggregator_mean.complete()
        self.assertEqual([10.0*10/10,   # 100 - 110
                          10.0*10/10,   # 110 - 120
                          10.0*10/10,   # 120 - 130
                          10.0*10/10,   # 130 - 140
                          10.0*10/10,   # 140 - 150
                          ], aggregator_mean.buckets.values)

    def test_mean_with_duplicate_points(self) -> None:
        """
        Test the time-weighted mean aggregator with duplicate points
        last writer wins, this is not expected but makes the algorithm
        robust
        """
        aggregator_mean = DownSamplerSparseMean(TimeRange(10, 50, 10),
                                                fill_func=FillFunc.NULL,
                                                fill_value=None,
                                                default_fill_value=None,
                                                trace=False)
        aggregator_mean.process(3, 10001)  # ignored because of next point
        aggregator_mean.process(5, 10002)  # ignored because of next point
        aggregator_mean.process(7, 10)
        aggregator_mean.process(12, 8)     # ignored because of next point
        aggregator_mean.process(12, 9)
        aggregator_mean.process(20, 5)     # ignored because of next point
        aggregator_mean.process(20, 7)
        aggregator_mean.process(40, 10)
        aggregator_mean.complete()
        self.assertEqual([(10*2+9*8)/10,  # 10 - 20
                          7*10/10,   # 20 - 30 , last writer wins (7 vs 5)
                          7*10/10,   # 30 - 40
                          10*10/10,  # 40 - 50
                          ], aggregator_mean.buckets.values)

    def test_dense_mean(self) -> None:
        """
        Test the dense mean aggregator with duplicate points
        last writer wins, this is not expected but makes the algorithm
        robust
        """
        aggregator_mean = DownSamplerDenseMean(
            TimeRange(10, 80, 10),
            query_id=None,
            trace=False)
        aggregator_mean.process(8, 70)    # Ignored because outside time range
        aggregator_mean.process(12, 9)
        aggregator_mean.process(20, 5)
        aggregator_mean.process(21, 7)
        aggregator_mean.process(40, 10)
        aggregator_mean.process(42, -6)
        aggregator_mean.process(52, -1)
        aggregator_mean.complete()
        self.assertEqual([9,         # 10 - 20
                          (5+7)/2,   # 20 - 30
                          None,      # 30 - 40
                          (10-6)/2,  # 40 - 50
                          -1,        # 50 - 60
                          None,      # 60 - 70
                          None,      # 70 - 80
                          ], aggregator_mean.buckets.values)
