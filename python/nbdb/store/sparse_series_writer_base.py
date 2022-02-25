"""
SparseSeriesData
"""
from __future__ import annotations

import logging
import sys
import time
import copy
from typing import Dict

# Its there not sure why pylint is unable to find it
# pylint: disable-msg=E0611  # No Name In Module
from lru import LRU

from nbdb.common.context import Context
from nbdb.common.data_point import DataPoint
from nbdb.common.data_point import FIELD
from nbdb.common.data_point import MISSING_POINT_VALUE, TOMBSTONE_VALUE
from nbdb.common.telemetry import Telemetry
from nbdb.common.tracing_config import TracingConfig
from nbdb.config.settings import Settings
from nbdb.store.sparse_store import SparseStore
from nbdb.store.sparse_series_stats_base import SeriesStatsBase
from pyformance import time_calls

logger = logging.getLogger()

# pylint: disable=R0902
class SeriesWriterBase:
    """
    This class contains the common methods used in sparse series writer
    and recovery series writer
    """
    # pylint: disable=R0913 # Too Many Arguments
    def __init__(self,
                 sparse_store: SparseStore,
                 sparse_store_settings: Settings,
                 tracing_config_settings: Settings,
                 context: Context,
                 sparse_telemetry_source_id: str,
                 consumer_mode: str):
        self.context = context
        self.sparse_store = sparse_store
        self.sparse_store_settings = sparse_store_settings
        self.sparse_telemetry_source_id = sparse_telemetry_source_id
        self.stats_cache = LRU(sparse_store_settings.stats.cache_size)
        self.data_gap_detection_interval = self.sparse_store_settings.\
            heartbeat_scan.data_gap_detection_interval
        self.termination_detection_interval = self.sparse_store_settings.\
            heartbeat_scan.termination_detection_interval

        self.consumer_mode = consumer_mode
        self.duplicated_data_points = 0

        self._periodic_last_run_times = dict()
        self.tracing_config_settings = tracing_config_settings
        self.download_tracing_config_from_s3 = \
            TracingConfig.inst.download_tracing_config_from_s3

    def generate_data_point_shard_by_metric(self, data_point: DataPoint,
                                            shard: int) -> DataPoint:
        """
        Generate a duplicate datapoint for the cross-cluster datasource.

        Duplicating to the cross-cluster datasources enables fast cross-cluster
        querying.

        NOTE: We assume that the cross-cluster pattern matching has already
        been done & verified before this method is called
        """
        datasource_name = self.context.schema.compose_crosscluster_datasource(
            shard)
        dup_data_point = copy.deepcopy(data_point)
        dup_data_point.datasource = datasource_name
        return dup_data_point

    def _run_after_interval(self, func_name, interval):
        """
        Executes the function func_name if atleast interval seconds has elapsed
        since last run
        :param func_name:
        :param interval:
        :return:
        """
        if func_name not in self._periodic_last_run_times:
            self._periodic_last_run_times[func_name] = 0

        if time.time() - self._periodic_last_run_times[func_name] < interval:
            return
        func = getattr(self, func_name, None)
        func()
        self._periodic_last_run_times[func_name] = time.time()

    @time_calls
    def report_stats_cache_telemetry(self) -> int:
        """
        Expensive operation should be called rarely
        Periodically report the stats telemetry.
        This is expensive operation so should be done sparingly
        :return size in bytes of the stats cache
        """
        cache_size_bytes = sys.getsizeof(self.stats_cache)
        stats_count = 0
        transformed_count = 0
        for stats_key, stats in self.stats_cache.items():
            # count the items and transformations
            stats_count += 1
            if stats.get_pre_transform_value(-1) != -1:
                transformed_count += 1

            # Estimate the size of the stats entry
            # add the size of key and stats primitive types
            cache_size_bytes += sys.getsizeof(stats_key) + sys.getsizeof(stats)

        Telemetry.inst.registry.gauge(
            measurement='SparseSeriesWriter.stats_cache.size_bytes',
            tag_key_values=["Topic=%s" % self.sparse_telemetry_source_id,
                            "ConsumerMode=%s" % self.consumer_mode]
        ).set_value(cache_size_bytes)
        Telemetry.inst.registry.gauge(
            measurement='SparseSeriesWriter.stats_cache.size',
            tag_key_values=["Topic=%s" % self.sparse_telemetry_source_id,
                            "ConsumerMode=%s" % self.consumer_mode]
        ).set_value(stats_count)
        Telemetry.inst.registry.gauge(
            measurement='SparseSeriesWriter.stats_cache.transformed',
            tag_key_values=["Topic=%s" % self.sparse_telemetry_source_id,
                            "ConsumerMode=%s" % self.consumer_mode]
        ).set_value(transformed_count)
        return cache_size_bytes

    def handle_series_start(self, datapoint: DataPoint, stats: SeriesStatsBase):
        """Handle additional logic when a series is created for the first time"""
        # Update the cross-cluster shard value since we have encountered this
        # series for the first time
        cloned_tags = datapoint.tags.copy()
        cloned_tags[FIELD] = datapoint.field
        shard = self.context.schema.get_crosscluster_shard(cloned_tags)
        if TracingConfig.TRACE_ACTIVE:
            logger.info('TRACE: schema.get_crosscluster_shard() tags=%s '
                        'shard=%s', datapoint.tags, shard)
        stats.set_crosscluster_shard(shard)

        # Check if it matches one of the sparseness disabled patterns. If yes,
        # store the info in a stats object
        if self.context.schema.is_sparseness_disabled(cloned_tags):
            stats.set_sparseness_disabled()

    @time_calls
    def heartbeat_scan(self, now: int = None) -> None:
        """
        Expensive operation should be called rarely.
        Scans the entire stats cache periodically and looks for dead series.
        A dead series is explicitly marked with a tombstone value
        :param now: time to compare the stats against, parameterized for
        unittests
        """
        if now is None:
            now = time.time()
        logger.info('heartbeat_scan(): now=%s', now)

        # Num tombstones per datasource
        num_tombstones: Dict[str, int] = dict()
        for stats_key, stats in self.stats_cache.items():
            # heartbeat_scan() can only be invoked in non-replay mode
            replay_mode = False
            tombstone = SeriesWriterBase.\
                check_offline_tombstone(self.termination_detection_interval, \
                                                self.data_gap_detection_interval, \
                                                stats_key, stats, now)
            SeriesWriterBase.\
                remove_stats_cache_entry(self.stats_cache, stats_key, stats)
            if tombstone is not None:
                self.create_marker(tombstone, stats, replay_mode, terminate=True)
                # extract datasource
                datasource = DataPoint.datasource_from_series_id(stats_key)
                if datasource in num_tombstones:
                    num_tombstones[datasource] = num_tombstones[datasource] + 1
                else:
                    num_tombstones[datasource] = 1

        for tombstones in num_tombstones.values():
            Telemetry.inst.registry.meter(
                'RecoverySeriesWriter.markers.tombstones').mark(tombstones)

    def create_marker(self,
                      marker: DataPoint,
                      stats: SeriesStatsBase,
                      replay_mode: bool,
                      terminate: bool = False) -> None:
        """
        Creates a missing / tombstone marker and updates stats appropriately
        :param marker:
        :param stats:
        :param replay_mode:
        :param terminate: IF true then series is terminated and we remove
        stats object from the cache
        """
        if terminate:
            del self.stats_cache[marker.series_id]
        self._write(marker, stats, replay_mode)

    def _write_datapoint(self, data_point: DataPoint, stat: SeriesStatsBase):
        """
        Write datapoint to sparse store. Write an additional cross-cluster
        datapoint if series matches one of the cross-cluster rules
        """
        self.sparse_store.write(data_point)

        # Check if datapoint matches one of the cross-cluster patterns. If
        # so, we duplicate a datapoint to the cross-cluster datasource to
        # enable fast cross-cluster querying
        shard = stat.get_crosscluster_shard()
        if TracingConfig.TRACE_ACTIVE:
            logger.info(
                'TRACE: stat.get_crosscluster_shard, data_point=%s '
                'shard=%s', data_point, shard)
        if shard is not None:
            # Datapoint matched one of the cross-cluster patterns
            dup_data_point = self.generate_data_point_shard_by_metric(
                data_point, shard)
            self.sparse_store.write(dup_data_point)

            if TracingConfig.TRACE_ACTIVE:
                logger.info('TRACE: sparse_series_writer dup_cc_point: '
                            'data_point %s stats:%s', data_point, stat)

            self.duplicated_data_points += 1
            # meter.mark calls are expensive, amortize it across 1000 calls
            if self.duplicated_data_points > 1000:
                Telemetry.inst.registry.meter(
                    'MetricConsumer.duplicated_cc_data_points'
                ).mark(self.duplicated_data_points)
                self.duplicated_data_points = 0

    def _write(self, data_point: DataPoint, stat: SeriesStatsBase,
               replay_mode: bool) -> None:
        """
        Write the data point to store
        :param data_point:
        :param stat:
        """
        raise NotImplementedError(
            'Child class must implement the _write method')

    def flush_writes(self) -> None:
        """
        Flush writes. Blocks till all async writes are complete
        """
        self.sparse_store.flush()

    def reinitialize(self) -> None:
        """
        Prepare the sparse writer to handle replayed datapoints
        """
        # It is possible that the series from the older stats objects map to
        # partititions that are no longer assigned to us.
        #
        # Instead of waiting for heartbeat scan to clean up the older object,
        # which also triggers unnecessary insertions of tombstone markers, we
        # clear up the entire cache and start afresh. This also helps keep our
        # memory usage in check and avoid OOM
        self.stats_cache.clear()
        self.flush_writes()

    @staticmethod
    def check_offline_tombstone(termination_detection_interval,
                                data_gap_detection_interval,
                                series_id,
                                stats,
                                now):
        """
        Check if a tombstone needs to be created to signal a dead series
        :param series_id:
        :param stats:
        :param now: wall clock
        :return: Returns True if tombstone was created
        """
        refresh_epoch = stats.get_refresh_epoch()
        if refresh_epoch <= 0:
            # we cannot create a tombstone if we don't know the history
            return None

        time_since_last_point = now - stats.get_server_rx_time()
        if time_since_last_point <= termination_detection_interval:
            return None

        # If we reached here, it means it has been longer than
        # `termination_detection_interval` since the last point was received
        if stats.is_sparseness_disabled():
            # For series which have sparseness disabled, we don't generate
            # tombstone values.
            return None
        # Generate a tombstone marker indicating series termination and
        # delete stats object
        tombstone_epoch = refresh_epoch + data_gap_detection_interval
        tombstone = DataPoint.from_series_id(
            series_id, tombstone_epoch, TOMBSTONE_VALUE,
            # This is a special value. Make sure it's NOT normalized before
            # storing
            is_special_value=True)
        return tombstone

    @staticmethod
    def remove_stats_cache_entry(stats_cache, series_id, stats):
        """
        Remove stats cache entry which will not be used
        """
        if stats.is_sparseness_disabled():
            # For series which have sparseness disabled, delete the stats object
            del stats_cache[series_id]

    @staticmethod
    def check_inline_missing_points(data_gap_detection_interval,
                                    data_point,
                                    stats):
        """
        Checks for missing points within a single series based on the gap
        between last reported point and the new point received
        :param data_point:
        :param stats:
        """
        refresh_epoch = stats.get_refresh_epoch()
        if refresh_epoch <= 0:
            # we cannot create a missing point marker if we don't know the
            # history
            if TracingConfig.TRACE_ACTIVE:
                logger.info('TRACE: sparse_series_writer.check_inline_'
                            'missing_points: data_point %s no missing '
                            'marker check due to no history stats %s',
                            str(data_point), str(stats))
            return None

        if data_point.epoch-refresh_epoch <= data_gap_detection_interval:
            # The new data point came within the data gap interval
            if TracingConfig.TRACE_ACTIVE:
                logger.info('TRACE: sparse_series_writer.check_inline_'
                            'missing_points: data_point %s within '
                            'data_gap_detection_interval %d'
                            'stats %s',
                            str(data_point),
                            data_gap_detection_interval,
                            str(stats))
            return None

        missing_pt_epoch = refresh_epoch + data_gap_detection_interval
        if TracingConfig.TRACE_ACTIVE:
            logger.info('TRACE: sparse_series_writer.check_inline_'
                        'missing_points: data_point %s outside of '
                        'data_gap_detection_interval %d. '
                        'stats %s creating new missing marker at epoch=%d',
                        str(data_point),
                        data_gap_detection_interval,
                        str(stats),
                        missing_pt_epoch)

        missing_pt_marker = DataPoint(data_point.datasource,
                                data_point.field,
                                data_point.tags,
                                missing_pt_epoch,
                                data_point.server_rx_epoch,
                                MISSING_POINT_VALUE,
                                # This is a special value. Make sure it's
                                # NOT normalized before storing
                                is_special_value=True,
                                series_id=data_point.series_id)

        Telemetry.inst.registry.meter(
            'SparseSeriesWriter.markers.missing_points').mark()
        return missing_pt_marker
