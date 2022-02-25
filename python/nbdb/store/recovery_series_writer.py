"""
SparseSeriesData
"""
from __future__ import annotations

import logging
from typing import List

from nbdb.common.context import Context
from nbdb.common.data_point import DataPoint
from nbdb.common.data_point import MODE_RECOVERY
from nbdb.common.telemetry import Telemetry
from nbdb.common.tracing_config import TracingConfig
from nbdb.config.settings import Settings
from nbdb.store.recovery_series_stats import RecoverySeriesStats
from nbdb.store.sparse_store import SparseStore
from nbdb.store.sparse_series_writer_base import SeriesWriterBase
from nbdb.store.sparse_series_stats_base import SeriesStatsBase

logger = logging.getLogger()

# pylint: disable=R0902
class RecoverySeriesWriter(SeriesWriterBase):
    """
    RecoverySeriesWriter is used in the recovery consumer, which only writes
    markers, i.e. missing value and tombstone. It can be taken as a truncated
    version of the SparseSeriesWriter.
    """
    # pylint: disable=R0913
    def __init__(self,
                 sparse_store: SparseStore,
                 sparse_store_settings: Settings,
                 tracing_config_settings: Settings,
                 context: Context,
                 sparse_telemetry_source_id: str):
        """
        Initialize the SparseSeriesData object and creates an internal thread
        safe cache for stats objects
        """
        SeriesWriterBase.__init__(self, sparse_store, sparse_store_settings, \
                                    tracing_config_settings, context, \
                                    sparse_telemetry_source_id, MODE_RECOVERY)


    def append_metrics(self, data_points: List[DataPoint],
                       replay_mode: bool) -> None:
        """
        Write the provided metrics to the DB
        :param data_points: list of data points to write to db
        :param replay_mode: Indicates whether these are old datapoints being
        reprocessed. If True, we skip writing them and just build local state.
        :return:
        """
        for data_point in data_points:
            self.append(data_point, replay_mode)

        if not replay_mode:
            # Make sure we don't run our usual logic of detecting dead series
            # for replayed datapoints because we can't rely on the value of
            # `server_rx_time` for replayed datapoints
            self._run_after_interval(
                'heartbeat_scan',
                self.sparse_store_settings.heartbeat_scan.interval)

    # pylint: disable-msg=R0912  # Too Many Branches
    def append(self, data_point: DataPoint, replay_mode: bool) -> None:
        """
        Append the data point to the series
        :param data_point: DataPoint
        :param replay_mode: Indicates whether these are old datapoints being
        reprocessed. If True, we skip writing them and just build local state.
        :return: True if the point is actually written
        """

        # Fetch stats object if it exists. Else create
        stats: RecoverySeriesStats = self.get_stats(data_point)
        if TracingConfig.TRACE_ACTIVE:
            logger.info('TRACE: sparse_series_writer.append: '
                        'data_point %s stats:%s', data_point, stats)

        data_point.adjust_epoch(self.context.schema.MIN_STORAGE_INTERVAL)

        missing_pt_marker = SeriesWriterBase.check_inline_missing_points(
                                        self.data_gap_detection_interval,
                                        data_point, stats)

        if missing_pt_marker is not None:
            self.create_marker(missing_pt_marker, stats, replay_mode)

        self.update_stats(data_point, stats)

    def get_stats(self, datapoint: DataPoint) -> RecoverySeriesStats:
        """
        Get the stats for the series_id, if not in cache then load from store
        :param series_id:
        :return:
        """
        series_id = datapoint.series_id
        if series_id not in self.stats_cache:
            # Create Stats object
            # ourselves and cache it.
            stats = RecoverySeriesStats()
            self.stats_cache[series_id] = stats
            # Call handler to perform additional bookkeeping when
            # a series is created for the first time
            self.handle_series_start(datapoint, stats)

            if Telemetry.inst is not None:
                Telemetry.inst.registry.meter(
                    'RecoverySeriesWriter.stats_cache.miss').mark()
        return self.stats_cache[series_id]

    @staticmethod
    def update_stats(data_point: DataPoint,
                     stats: RecoverySeriesStats) -> None:
        """
        Update the stats with the data point
        """
        epoch = stats.get_refresh_epoch()
        if epoch is None or epoch < data_point.epoch:
            epoch = data_point.epoch
        stats.set_refresh_epoch(epoch)

        server_rx_epoch = stats.get_server_rx_time()
        if server_rx_epoch is None or server_rx_epoch < data_point.server_rx_epoch:
            server_rx_epoch = data_point.server_rx_epoch
        stats.set_server_rx_time(server_rx_epoch)

    def _write(self, data_point: DataPoint, stat: SeriesStatsBase,
               replay_mode: bool) -> None:
        """
        Write the data point to store
        :param data_point:
        :param stat:
        """
        if not replay_mode:
            self._write_datapoint(data_point, stat)
