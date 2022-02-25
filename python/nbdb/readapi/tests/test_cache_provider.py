"""
Unittests for sparse series aggregators
"""
import os
from typing import Dict, List, Tuple
from unittest import TestCase

from mock import Mock

from nbdb.common.telemetry import Telemetry
from nbdb.config.settings import Settings
from nbdb.readapi.cache_provider import CacheProviderInterface
from nbdb.readapi.in_memory_cache import InMemoryCache
from nbdb.readapi.query_cache import QueryCacheEntry
from nbdb.readapi.redis_cache import RedisCache
from nbdb.readapi.time_series_response import TimeRange, TimeSeriesGroup, \
    TimeSeries


class TestCacheProvider(TestCase):
    """
    Tests the Cache providers InMemory and Redis
    """
    def setUp(self) -> None:
        """
        Setup test suite
        :return:
        """
        Settings.load_yaml_settings(os.path.dirname(__file__) +
                                    '/test_settings.yaml')
        Telemetry.inst = None
        Telemetry.initialize()

    def tearDown(self) -> None:
        """
        Stop telemetry instance
        """
        Telemetry.inst = None

    @staticmethod
    def _create_sample_cache_entries(num_fields: int,
                                     num_groups: int,
                                     num_series: int,
                                     time_ranges: List[TimeRange]) -> \
            List[QueryCacheEntry]:
        """
        Create a dummy result using random data
        :param num_fields: number of fields in result
        :param num_groups: number of groups per field
        :param num_series: number of series per group
        :param time_ranges: list of time ranges for which to create entries
        :return:
        """
        entries: List[QueryCacheEntry] = list()
        for time_range in time_ranges:
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
                            [(e, e*10)
                             for e in range(time_range.start,
                                            time_range.end,
                                            time_range.interval)]
                        shifted_range = TimeRange(time_range.start,
                                                  time_range.end,
                                                  time_range.interval)
                        ts_list.append(TimeSeries(series_name,
                                                  points,
                                                  shifted_range))
                    tsg.groups[group_name] = ts_list
                result[field_name] = tsg
            entries.append(QueryCacheEntry(time_range, result))
        return entries

    def test_in_memory_provider(self) -> None:
        """
        Tests the in memory provider
        """
        user = "test"
        cache_provider: CacheProviderInterface = InMemoryCache(cache_size=100)

        self.assertFalse(cache_provider.has('k1'), 'k1 does not exist yet')
        self.assertIsNone(cache_provider.get('k1'), 'k1 does not exist yet')

        entries = TestCacheProvider._create_sample_cache_entries(
            10, 2, 2, [TimeRange(10, 30, 1), TimeRange(40, 60, 1)])
        cache_provider.set('k1', entries, user=user)
        self.assertTrue(cache_provider.has('k1'), 'k1 does exist now')
        self.assertIsNotNone(cache_provider.get('k1', user=user),
                             'k1 does exist now')
        entries_from_cache = cache_provider.get('k1', user=user)
        self.assertEqual(entries, entries_from_cache, 'cache entries do not'
                                                      ' match')

    def test_redis_provider(self) -> None:
        """
        Tests the redis based cache provider by mocking the redis client5``
        """
        user = "test"
        cache_provider: CacheProviderInterface = RedisCache(host='localhost',
                                                            port=1000,
                                                            db_id=0,
                                                            ttl_seconds=10)
        cache_provider.redis_client = Mock()
        cache_provider.redis_client.keys.return_value = None
        cache_provider.redis_client.get.return_value = None

        self.assertFalse(cache_provider.has('k1'), 'k1 does not exist yet')
        self.assertIsNone(cache_provider.get('k1', user=user),
                          'k1 does not exist yet')

        entries = TestCacheProvider._create_sample_cache_entries(
            10, 2, 2, [TimeRange(10, 30, 1), TimeRange(40, 60, 1)])
        cache_provider.set('k1', entries, user=user)
        # verify that the redis_client set method is called with a compressed
        # pickle representation
        key, comp_pickle_str = cache_provider.redis_client.set.call_args[0]
        self.assertEqual('k1', key)
        cache_provider.redis_client.get.return_value = comp_pickle_str
        cache_provider.redis_client.keys.return_value = ['k1'.encode("utf-8")]

        self.assertTrue(cache_provider.has('k1'), 'k1 does exist now')
        self.assertIsNotNone(cache_provider.get('k1', user=user),
                             'k1 does exist now')
        entries_from_cache = cache_provider.get('k1', user=user)
        self.assertEqual(entries, entries_from_cache,
                         'cache entries do not match')
