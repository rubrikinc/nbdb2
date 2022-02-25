"""
Unittests for sparse series aggregators
"""
import os
from typing import Dict, List, Tuple
from unittest import mock, TestCase

from nbdb.common.telemetry import Telemetry
from nbdb.config.settings import Settings
from nbdb.readapi.in_memory_cache import InMemoryCache
from nbdb.readapi.query_cache import QueryCache, QueryCacheEntry
from nbdb.readapi.query_cache import QueryCacheResult
from nbdb.readapi.time_series_response import TimeRange, TimeSeriesGroup
from nbdb.readapi.time_series_response import TimeSeries


class TestQueryCache(TestCase):
    """
    Tests the query cache using in-memory setting only
    """

    def setUp(self) -> None:
        cache_provider = InMemoryCache(1000)
        mock_max_points = mock.MagicMock()
        type(mock_max_points).default = mock.PropertyMock(return_value=1000)
        type(mock_max_points).test = mock.PropertyMock(return_value=1000)
        Settings.load_yaml_settings(os.path.dirname(__file__) +
                                    '/test_druid_settings.yaml')
        Telemetry.inst = None
        Telemetry.initialize()
        self.user = 'test'
        self.query_cache = QueryCache(cache_provider, mock_max_points)

    @staticmethod
    def _create_sample_result(num_fields: int,
                              num_groups: int,
                              num_series: int,
                              time_range: TimeRange,
                              time_shift: int = 0) -> \
            Dict[str, TimeSeriesGroup]:
        """
        Create a dummy result using random data
        :param num_fields: number of fields in result
        :param num_groups: number of groups per field
        :param num_series: number of series per group
        :param time_range:
        :param time_shift: shift time by adding this value to epoch
        :return:
        """
        result: Dict[str, TimeSeriesGroup] = dict()
        for field in range(0, num_fields):
            field_name = 'f_{}'.format(field)
            tsg = TimeSeriesGroup()
            for group in range(0, num_groups):
                group_name = 'g_{}'.format(group)
                ts_list: List[TimeSeries] = list()
                for series in range(0, num_series):
                    series_name = 's_{}'.format(series)
                    points: List[Tuple[int, float]] = \
                        [(e + time_shift, e*10)
                         for e in range(time_range.start,
                                        time_range.end,
                                        time_range.interval)]
                    shifted_range = TimeRange(time_range.start + time_shift,
                                              time_range.end + time_shift,
                                              time_range.interval)
                    ts_list.append(TimeSeries(series_name,
                                              points,
                                              shifted_range))
                tsg.groups[group_name] = ts_list
            result[field_name] = tsg
        return result

    # pylint: disable=too-many-locals
    def test_cache_simple(self) -> None:
        """
        test the simple case , of moving window in cache
        Q1: TimeRange [10 - 20) - miss
        Q2: TimeRange [10 - 30) - partial hit for 10-20, miss for 20-30
        Q3: TimeRange [30 - 40) - miss
        Q4: TimeRange [2, 5) - miss
        Q5: TimeRange [2 - 50) - partial hit for 10-20, 30-40
        Q6: TimeRange [50, 100) - miss
        Q7: TimeRange [2, 100) - partial hit for [48, 100)
        """
        interval = 2
        num_fields = 2
        num_groups = 2
        num_series = 2
        key = 'k1'
        # For this testcase, we choose max_points = 25
        mock_max_points = mock.MagicMock()
        type(mock_max_points).default = mock.PropertyMock(return_value=25)
        type(mock_max_points).test = mock.PropertyMock(return_value=25)
        self.query_cache.max_points = mock_max_points

        r1 = TimeRange(10, 20, interval)
        cache_entries = self.query_cache.get(key, user=self.user)

        self.assertEqual(None, cache_entries)

        result_1 = TestQueryCache._create_sample_result(num_fields,
                                                        num_groups,
                                                        num_series,
                                                        r1)
        self.query_cache.set(key, [QueryCacheEntry(r1, result_1)], user=self.user)

        cache_entries = self.query_cache.get(key, user=self.user)
        cache_result = self.query_cache.prune(cache_entries, r1,
                                              user=self.user)
        self.assertEqual(0, len(cache_result.missed_ranges))
        self.assertEqual(result_1, cache_result.cached_results)

        r2 = TimeRange(10, 30, interval)
        cache_entries = self.query_cache.get(key, user=self.user)
        cache_result = self.query_cache.prune(cache_entries, r2,
                                              user=self.user)
        self.assertEqual([TimeRange(20, 30, interval)],
                         cache_result.missed_ranges)
        self.assertEqual(result_1, cache_result.cached_results)

        r3 = TimeRange(30, 40, interval)
        result_2 = TestQueryCache._create_sample_result(num_fields,
                                                        num_groups,
                                                        num_series,
                                                        r3)
        merged_results = self.query_cache.merge(
            cache_entries, QueryCacheEntry(r3, result_2))
        self.query_cache.set(key, merged_results, user=self.user)

        r4 = TimeRange(2, 5, interval)
        cache_entries = self.query_cache.get(key, user=self.user)
        cache_result = self.query_cache.prune(cache_entries, r4,
                                              user=self.user)
        self.assertEqual([TimeRange(2, 5, interval)],
                         cache_result.missed_ranges)
        result_4 = TestQueryCache._create_sample_result(num_fields,
                                                        num_groups,
                                                        num_series,
                                                        r4)
        merged_results = self.query_cache.merge(
            cache_entries, QueryCacheEntry(r4, result_4))
        self.query_cache.set(key, merged_results, user=self.user)

        r5 = TimeRange(2, 50, interval)
        cache_entries = self.query_cache.get(key, user=self.user)
        cache_result = self.query_cache.prune(cache_entries, r5,
                                              user=self.user)
        self.assertEqual([TimeRange(5, 10, interval),
                          TimeRange(20, 30, interval),
                          TimeRange(40, 50, interval)],
                         cache_result.missed_ranges)

        # fetch the missing ranges
        for missing_range in cache_result.missed_ranges:
            result = self._create_sample_result(num_fields,
                                                num_groups,
                                                num_series,
                                                missing_range)
            merged_results = self.query_cache.merge(
                cache_entries, QueryCacheEntry(missing_range, result))
        self.query_cache.set(key, merged_results, user=self.user)

        cache_entries = self.query_cache.get(key, user=self.user)
        cache_result = self.query_cache.prune(cache_entries, r5,
                                              user=self.user)
        self.assertEqual(0, len(cache_result.missed_ranges))
        # verify we shouldn't have any None values and all values are expected
        self._verify_result(num_fields, num_groups, num_series, cache_result)

        r6 = TimeRange(50, 100, interval)
        cache_entries = self.query_cache.get(key, user=self.user)
        cache_result = self.query_cache.prune(cache_entries, r6,
                                              user=self.user)
        self.assertEqual([TimeRange(50, 100, interval)],
                         cache_result.missed_ranges)

        result_6 = self._create_sample_result(num_fields,
                                              num_groups,
                                              num_series,
                                              r6)
        merged_results = self.query_cache.merge(
            cache_entries, QueryCacheEntry(r6, result_6))
        self.query_cache.set(key, merged_results, user=self.user)
        # Now verify that we only cache the latest 25 datapoints since
        # max_datapoints is 25
        r7 = TimeRange(2, 100, interval)
        cache_entries = self.query_cache.get(key, user=self.user)
        cache_result = self.query_cache.prune(cache_entries, r7,
                                              user=self.user)
        # We should see a miss for [2, 50) because only the latest 25
        # datapoints in the cache result are kept
        self.assertEqual([TimeRange(2, 50, interval)],
                         cache_result.missed_ranges)

    def test_query_cache_mixed_series(self) -> None:
        """
        Tests merging query cache when incremental results return a different
        set of series
        """
        interval = 2
        num_fields = 2
        num_groups = 2
        num_series = 2
        key = 'k1'

        original_range = TimeRange(10, 20, interval)
        cache_entries = self.query_cache.get(key, user=self.user)
        self.assertEqual(None, cache_entries)

        result_1 = TestQueryCache._create_sample_result(num_fields,
                                                        num_groups,
                                                        num_series,
                                                        original_range)
        self.query_cache.set(key, [QueryCacheEntry(original_range, result_1)],
                             user=self.user)

        cache_entries = self.query_cache.get(key, user=self.user)
        cache_result = self.query_cache.prune(cache_entries, original_range,
                                              user=self.user)
        self.assertEqual(0, len(cache_result.missed_ranges))
        self.assertEqual(result_1, cache_result.cached_results)

        expanded_range = TimeRange(10, 30, interval)
        cache_entries = self.query_cache.get(key, user=self.user)
        cache_result = self.query_cache.prune(cache_entries, expanded_range,
                                              user=self.user)
        missed_range = TimeRange(20, 30, interval)
        self.assertEqual([missed_range], cache_result.missed_ranges)
        self.assertEqual(result_1, cache_result.cached_results)
        # increase the series in incremental result, it should be merged still
        result_missing = TestQueryCache._create_sample_result(num_fields,
                                                              num_groups,
                                                              num_series + 1,
                                                              missed_range)
        # Delete the last datapoint for one of the series generated in our
        # sample result. We should still merge later when we find
        # non-overlapping timeseries data for this one series
        del result_missing['f_0']['g_0'][0].points[-1]
        merged_results = self.query_cache.merge(
            cache_entries, QueryCacheEntry(missed_range, result_missing))
        self.query_cache.set(key, merged_results, user=self.user)

        cache_entries = self.query_cache.get(key, user=self.user)
        cache_result = self.query_cache.prune(cache_entries, expanded_range,
                                              user=self.user)
        self.assertEqual(0, len(cache_result.missed_ranges))
        # verify we shouldn't have any None values and all values are expected
        self._verify_result(num_fields, num_groups, num_series + 1,
                            cache_result)

        # Run another merge operation. Deleting one datapoint for one of the
        # series should not cause a merging bug
        expanded_range = TimeRange(10, 40, interval)
        missed_range = TimeRange(30, 40, interval)
        result_missing = TestQueryCache._create_sample_result(num_fields,
                                                              num_groups,
                                                              num_series + 1,
                                                              missed_range)
        merged_results = self.query_cache.merge(
            cache_entries, QueryCacheEntry(missed_range, result_missing))
        self.query_cache.set(key, merged_results, user=self.user)
        cache_entries = self.query_cache.get(key, user=self.user)
        cache_result = self.query_cache.prune(cache_entries, expanded_range,
                                              user=self.user)
        self.assertEqual(0, len(cache_result.missed_ranges))
        # verify we shouldn't have any None values and all values are expected
        self._verify_result(num_fields, num_groups, num_series + 1,
                            cache_result)

    def test_dashboard_refresh(self) -> None:
        """
        Moving window use case
        """
        interval = 2
        dashboard_window = 20
        num_fields = 4
        num_groups = 2
        num_series = 3
        key = 'k2'
        for refresh_interval in range(interval, 4*interval, interval):
            # reset the cache
            self.query_cache.clear(key)
            for start_epoch in range(10, 100, refresh_interval):
                current_range = TimeRange(start_epoch,
                                          start_epoch + dashboard_window,
                                          interval)
                cache_entries = self.query_cache.get(key, user=self.user)
                cached_result = None
                if cache_entries is not None:
                    cached_result = self.query_cache.prune(
                        cache_entries, current_range, user=self.user)
                fetch_range = current_range
                if cached_result is None:
                    # The first load
                    self.assertEqual(10, start_epoch,
                                     'Using refresh_interval: {}'.format(
                                         refresh_interval))
                    cache_entries = []
                else:
                    self.assertEqual(1, len(cached_result.missed_ranges),
                                     'Using refresh_interval: {}'.format(
                                         refresh_interval))
                    fetch_range = cached_result.missed_ranges[0]
                result = self._create_sample_result(num_fields,
                                                    num_groups,
                                                    num_series,
                                                    fetch_range)
                self.query_cache.set(key, [QueryCacheEntry(fetch_range,
                                                           result)],
                                     user=self.user)
                merged_results = self.query_cache.merge(
                    cache_entries, QueryCacheEntry(fetch_range, result))
                self.query_cache.set(key, merged_results, user=self.user)

                cache_entries = self.query_cache.get(key, user=self.user)
                cached_result = self.query_cache.prune(
                    cache_entries, current_range, user=self.user)
                self.assertEqual(0, len(cached_result.missed_ranges))
                # verify the data points
                self._verify_result(num_fields,
                                    num_groups,
                                    num_series,
                                    cached_result)

    def _verify_result(self,
                       num_fields: int,
                       num_groups: int,
                       num_series: int,
                       cache_result: QueryCacheResult):
        """
        Verify the result is as expected
        note at generation time the value was 10*epoch
        :param num_fields:
        :param num_groups:
        :param num_series:
        :param cache_result:
        :return:
        """
        self.assertEqual(num_fields, len(cache_result.cached_results))
        for _, tsg in cache_result.cached_results.items():
            self.assertEqual(num_groups, len(tsg.groups))
            for _, series_list in tsg.groups.items():
                self.assertEqual(num_series, len(series_list))
                for ts in series_list:
                    for epoch, value in ts.points:
                        self.assertEqual(epoch*10, value)
