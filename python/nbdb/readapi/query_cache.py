"""
Query cache module
"""
from __future__ import  annotations

import logging
import threading
from dataclasses import dataclass
from functools import reduce
from typing import List, Dict


from nbdb.config.settings import Settings
from nbdb.common.telemetry import Telemetry, user_time_calls
from nbdb.readapi.cache_provider import CacheProviderInterface
from nbdb.readapi.in_memory_cache import InMemoryCache
from nbdb.readapi.query_cache_entry import QueryCacheEntry
from nbdb.readapi.redis_cache import RedisCache
from nbdb.readapi.time_series_response import TimeSeriesGroup, TimeRange

from pyformance import time_calls

logger = logging.getLogger()


@dataclass
class QueryCacheResult:
    """
    Results generated by the QueryCache
    """

    # multi field response, each field has a corresponding time series group
    cached_results: Dict[str, TimeSeriesGroup]

    # List of time-ranges that were cache miss
    missed_ranges: List[TimeRange]


class QueryCache:
    """
    QueryCache caches results of a query by time intervals
    So a query with moving window but fixed intervals will get partially
    matched

    A query result is a dictionary of expression names to TimeSeriesGroups

    The query cache is query key -> List of non overlapping Query Results
    """

    # Global singleton for QueryCache
    inst: QueryCache = None

    def __init__(self,
                 cache_provider: CacheProviderInterface,
                 max_points: Settings):
        """
        Initialize
        :param cache_size:
        :param max_points:
        """
        self._cache = cache_provider
        self.max_points = max_points

    @staticmethod
    def initialize():
        """
        Creates the singleton class, for now in-memory
        in future we should back it up with redis
        :return:
        """
        if QueryCache.inst is not None:
            raise ValueError('QueryCache already initialized')

        sql_api: Settings = Settings.inst.sql_api
        if sql_api.cache_provider == 'memory':
            logger.info('QueryCache.initialize InMemoryCache (size=%d)',
                        sql_api.query_cache_size)
            cache_provider = InMemoryCache(sql_api.query_cache_size)
        elif sql_api.cache_provider == 'redis':
            logger.info('QueryCache.initialize RedisCache '
                        '(host=%s, port=%d, db_id=%d, ttl_seconds=%d)',
                        sql_api.host,
                        sql_api.port,
                        sql_api.db_id,
                        sql_api.ttl_seconds,
                        )
            cache_provider = RedisCache(sql_api.host,
                                        sql_api.port,
                                        sql_api.db_id,
                                        sql_api.ttl_seconds)
        else:
            raise ValueError('Unknown cache provider ' +
                             sql_api.cache_provider)

        QueryCache.inst = QueryCache(cache_provider, sql_api.points_per_series)

    @time_calls
    def get(self, key: str, user: str) -> List[QueryCacheEntry]:
        """
        Query cache is not thread safe
        :param key:
        :param time_range:
        :return:
        """
        with threading.RLock():
            return self._get(key, user=user)

    def clear(self, key: str) -> None:
        """
        Clears the key from the cache
        :param key:
        """
        with threading.RLock():
            self._cache.clear(key)

    @time_calls
    def set(self,
            key: str,
            cache_entries: List[QueryCacheEntry],
            user: str,
            trace: bool = False,
            trace_id: str = None) \
            -> None:
        """
        Query cache is not thread safe
        :param key:
        :param new_entry:
        :param trace: if true does a verbose logging
        :param trace_id: unique identifier for the trace
        :return:
        """
        with threading.RLock():
            self._set(key, cache_entries, user=user, trace=trace,
                      trace_id=trace_id)

    # pylint: disable-msg=R0914  # Too Many Locals
    @user_time_calls
    def _get(self, key: str,
             # NOTE: 'user' must remain a keyword argument for the
             # user_time_calls() decorator to work
             user: str = None) -> List[QueryCacheEntry]:
        """
        Fetch the requested key from the cache.
        :param key: Query key (minus the time window)
        :param user:
        :return: list of QueryCacheEntry or None
        """
        # check if we have a cache hit
        if not self._cache.has(key):
            if Telemetry.inst is not None:
                Telemetry.inst.registry.meter(
                    'ReadApi.QueryCache.miss',
                    tag_key_values=["User={user}"]
                ).mark()
            return None

        # These are ordered by time
        cache_entries = self._cache.get(key, user=user)
        if Telemetry.inst is not None:
            Telemetry.inst.registry.meter(
                'ReadApi.QueryCache.hit',
                tag_key_values=[f"User={user}"]
            ).mark()

        return cache_entries

    @user_time_calls
    def prune(self,
              cache_entries: List[QueryCacheEntry],
              time_range: TimeRange,
              # NOTE: 'user' must remain a keyword argument for the
              # user_time_calls() decorator to work
              user: str = None) -> QueryCacheResult:
        """
        Prunes the results from the cache entries restricted to the given time
        range.

        :param cache_entries: Results fetched from cache. Method assumes that
        these are ordered by time.
        :param time_range: Time range the results need to be restricted to
        :param user:
        :return: QueryCacheResult or None
        """
        _ = self
        missing_ranges: List[TimeRange] = list()
        # We do not alter the original cached results but instead create a new
        # object with the pruned results. While this may seem suboptimal, the
        # original cached results may be needed later and hence can't be
        # altered.
        prepared_result: Dict[str, TimeSeriesGroup] = dict()
        last_data_range = TimeRange(0, time_range.start, time_range.interval)
        for cache_entry in cache_entries:
            if time_range.end < cache_entry.time_range.start:
                # This cache entry is ahead of the requested time range. No
                # point in inspecting more cache entries
                break
            if time_range.start > cache_entry.time_range.end:
                # Cache entry behind the requested time range. Move on to next
                # entry
                continue

            # If we reach here, cache entry time range intersects with the
            # requested time range. Filter results from the cache entry
            # restricted to the requested time range
            for name, tsg in cache_entry.result.items():
                if name not in prepared_result:
                    # create a new one
                    prepared_result[name] = TimeSeriesGroup()
                target_tsg = prepared_result[name]
                TimeSeriesGroup.partial_merge(
                    target_tsg, tsg, time_range)

            # append missing range at start of cache entry
            if last_data_range.end < cache_entry.time_range.start:
                missing_ranges.append(TimeRange(last_data_range.end,
                                                cache_entry.time_range.start,
                                                last_data_range.interval))
            last_data_range = cache_entry.time_range

        # append the missing range at the end
        if last_data_range.end < time_range.end:
            missing_ranges.append(TimeRange(last_data_range.end,
                                            time_range.end,
                                            time_range.interval))
        # sort and merge the missing ranges
        query_cache_result = QueryCacheResult(prepared_result, missing_ranges)

        # compute ratio of time range that was covered vs total
        miss_time = reduce(lambda _sum, tr:
                           tr.end - tr.start + _sum,
                           missing_ranges, 0)
        total_time = time_range.end - time_range.start
        partial_hit_ratio = None
        if total_time > 0:
            partial_hit_ratio = 1 - miss_time/total_time

        if Telemetry.inst is not None:
            if partial_hit_ratio is not None:
                Telemetry.inst.registry.histogram(
                    'ReadApi.QueryCache.partial_hit_ratio',
                    tag_key_values=[f"User={user}"]
                ).add(partial_hit_ratio)

        return query_cache_result

    # pylint: disable-msg=R0912  # Too Many Branches
    @user_time_calls
    def _set(self,
             key: str,
             cache_entries: List[QueryCacheEntry],
             # NOTE: 'user' must remain a keyword argument for the
             # user_time_calls() decorator to work
             user: str = None,
             trace: bool = False,
             trace_id: str = None) \
            -> None:
        """
        :param key:
        :param cache_entries:
        :param user:
        :param trace: If true does verbose logging
        :param trace_id:
        We drop the older points
        """
        # Figure out the max points to cache per series based on the user
        # issuing the query. If no overrides have been set, we will use the
        # defaults
        max_points = getattr(self.max_points, user, self.max_points.default)
        entry_size = 0
        entry_points = 0
        # Prune all cache entries to only contain max_points
        for entry in cache_entries:
            # Drop points if more than max_points exist
            entry.drop_points(max_points)
            # Update counters
            size, points = entry.size()
            entry_size += size
            entry_points += points

        self._cache.set(key, cache_entries, user=user)
        if trace:
            logger.info('[Trace: %s]: QueryCache._set: key=%s entries=%d',
                        trace_id, key, len(cache_entries))

        if Telemetry.inst is not None:
            Telemetry.inst.registry.gauge(
                'ReadApi.QueryCache.entries',
                tag_key_values=[f"User={user}"]
            ).set_value(self._cache.size())
            Telemetry.inst.registry.histogram(
                'ReadApi.QueryCache.entry_size',
                tag_key_values=[f"User={user}"]
            ).add(entry_size)
            Telemetry.inst.registry.histogram(
                'ReadApi.QueryCache.entry_points',
                tag_key_values=[f"User={user}"]
            ).add(entry_points)

    def merge(self,
              cache_entries: List[QueryCacheEntry],
              new_entry: QueryCacheEntry,
              trace: bool = False,
              trace_id: str = None) -> None:
        """
        Merge new cache entry with existing cache entries, without adhering to
        max_points limits.
        :param cache_entries:
        :param new_entry:
        :param user:
        """
        _ = self
        # We assume that cache entries provided are sorted by time.
        # Insert the new entry at its proper place in the sorted list
        inserted = False
        # pylint: disable=consider-using-enumerate
        for i in range(0, len(cache_entries)):
            if trace:
                logger.info('[Trace: %s]: QueryCache.merge:'
                            ' cache_entry: %s',
                            trace_id, str(cache_entries[i]))
            if cache_entries[i].less(new_entry):
                continue
            if trace:
                logger.info('[Trace: %s]: QueryCache.merge: inserted '
                            'new entry: %s at index %d',
                            trace_id, new_entry, i)
            cache_entries.insert(i, new_entry)
            inserted = True
            break
        if not inserted:
            if trace:
                logger.info('[Trace: %s]: QueryCache.merge: '
                            'appending new_entry: %s',
                            trace_id, new_entry)
            cache_entries.append(new_entry)

        # The cache entries contain the new entry now and they are sorted
        # by time, but we can have consecutive entries that overlap
        # we need to merge the overlapping entries
        merged_results = list()
        # current_entry represents the merged entry so far
        current_entry = None
        # The cache entries are stored by time
        # we try to merge the entry into the cache_entries one by one
        for entry in cache_entries:
            if current_entry is None:
                current_entry = entry
            elif current_entry.overlaps(entry):
                if trace:
                    logger.info('[Trace: %s]: QueryCache.merge: merging '
                                'current_entry:%s with entry: %s',
                                trace_id, current_entry, entry)
                current_entry.merge(entry)
            else:
                merged_results.append(current_entry)
                current_entry = entry
        if current_entry is not None:
            merged_results.append(current_entry)

        return merged_results
