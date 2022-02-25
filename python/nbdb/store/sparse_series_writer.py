"""
SparseSeriesData
"""
from __future__ import annotations

import logging
import sys
import time
import copy
from typing import List, Dict

# Its there not sure why pylint is unable to find it
# pylint: disable-msg=E0611  # No Name In Module
from lru import LRU

from nbdb.common.context import Context
from nbdb.common.data_point import DataPoint
from nbdb.common.data_point import DATA_DROP, DATA_FORCE_WRITE, DATA_WRITE
from nbdb.common.data_point import MISSING_POINT_VALUE, TOMBSTONE_VALUE
from nbdb.common.data_point import MODE_BOTH, MODE_REALTIME, MODE_ROLLUP
from nbdb.common.data_point import FIELD
from nbdb.common.telemetry import Telemetry
from nbdb.common.tracing_config import TracingConfig
from nbdb.config.settings import Settings
from nbdb.store.rollups import Rollups
from nbdb.store.sparse_algo_selector import SparseAlgoSelector
from nbdb.store.sparse_series_stats import SparseSeriesStats
from nbdb.store.sparse_store import SparseStore
from nbdb.store.stream_transforms import StreamTransforms
from nbdb.schema.schema import SHARD_BY_CLUSTER, SHARD_BY_METRIC
from nbdb.store.sparse_series_stats_base import SeriesStatsBase
from nbdb.store.sparse_series_writer_base import SeriesWriterBase

logger = logging.getLogger()


class SparseSeriesWriter(SeriesWriterBase):
    """
    Provide a simple high level interface to write data points for
    a single series and abstracts away the schema implementation of the DB
    This class is responsible for implementing sparseness at an individual
    series level
    The write path interacts with batch_writer through a queue, this is done
    to allow us to create batches of writes to improve the DB write
    performance.
    The read path directly interacts with the DB through the provided DB store
    object.
    All methods are thread safe and are expected to be called
    from multiple threads.
    """

    # pylint: disable=too-many-instance-attributes
    def __init__(self,
                 sparse_store: SparseStore,
                 sparse_store_settings: Settings,
                 tracing_config_settings: Settings,
                 context: Context,
                 protocol: str,
                 sparse_telemetry_source_id: str,
                 past_message_lag: int,
                 future_message_lag: int,
                 consumer_mode: str):
        """
        Initialize the SparseSeriesData object and creates an internal thread
        safe cache for stats objects
        """
        SeriesWriterBase.__init__(self, sparse_store, sparse_store_settings, \
                                    tracing_config_settings, context, \
                                    sparse_telemetry_source_id, consumer_mode)

        self.protocol = protocol
        self.sparse_algo_selector = SparseAlgoSelector(
            sparse_telemetry_settings=sparse_store_settings.sparse_telemetry,
            sparse_telemetry_source_id=sparse_telemetry_source_id,
            context=self.context,
            sparse_store=self.sparse_store,
            consumer_mode=consumer_mode)

        self.write_raw_datapoints = consumer_mode in [MODE_REALTIME, MODE_BOTH]
        self.write_rollup_datapoints = consumer_mode in [MODE_ROLLUP, MODE_BOTH]

        self._transformed_metrics = 0
 
        self.rollups = Rollups(rollup_settings=sparse_store_settings.rollups,
                               schema=self.context.schema,
                               sparse_algo_selector=self.sparse_algo_selector,
                               sparse_store=self.sparse_store,
                               past_message_lag=past_message_lag,
                               future_message_lag=future_message_lag)

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

        # run the periodic activity
        self._run_after_interval(
            'report_stats_cache_telemetry',
            self.sparse_store_settings.stats.telemetry_report_interval_seconds)

        if not replay_mode:
            # Make sure we don't run our usual logic of detecting dead series
            # for replayed datapoints because we can't rely on the value of
            # `server_rx_time` for replayed datapoints
            self._run_after_interval(
                'heartbeat_scan',
                self.sparse_store_settings.heartbeat_scan.interval)

        self._run_after_interval(
            'download_tracing_config_from_s3',
            self.tracing_config_settings.refresh_interval)

    # pylint: disable-msg=R0912  # Too Many Branches
    def append(self, data_point: DataPoint, replay_mode: bool) -> None:
        """
        Append the data point to the series
        :param data_point: DataPoint
        :param replay_mode: Indicates whether these are old datapoints being
        reprocessed. If True, we skip writing them and just build local state.
        :return: True if the point is actually written
        """
        # If stats object didn't exist before and replay_mode is False, it
        # means we are observing this series for the first time. Update the
        # exploration datasource with an entry
        #
        # NOTE: We do not create exploration datapoints during replay_mode,
        # even if the stats object doesn't exist. This is because we are
        # replaying datapoints, and thus some other consumer must have already
        # created the exploration datapoint before.
        if data_point.series_id not in self.stats_cache and not replay_mode:
            self.write_exploration_dp(data_point)

        # Fetch stats object if it exists. Else create
        stats: SparseSeriesStats = self.get_stats(data_point)
        if TracingConfig.TRACE_ACTIVE:
            logger.info('TRACE: sparse_series_writer.append: '
                        'data_point %s stats:%s', data_point, stats)

        if stats.is_sparseness_disabled():
            # Sparseness is disabled for this series. Just write the datapoint
            # as is to our backend data store, without applying any sparseness
            # checks.

            # We do need to keep track of received timestamps so that we can
            # cleanup this series from the stats cache later when it is dead
            self.sparse_algo_selector.update_received_point(
                data_point, stats, window=0, replay_mode=replay_mode)

            if self.consumer_mode == MODE_ROLLUP:
                # We do not support rollups for series which have sparseness
                # disabled.
                if TracingConfig.TRACE_ACTIVE:
                    logger.info('TRACE: sparse_series_writer.append: '
                                'data_point %s has sparseness disabled and '
                                'thus is being dropped on rollup consumer',
                                data_point)
                return
            if replay_mode:
                # Don't write any data in replay mode
                if TracingConfig.TRACE_ACTIVE:
                    logger.info('TRACE: sparse_series_writer.append: '
                                'data_point %s has sparseness disabled but '
                                'dropping because of replay_mode=%s',
                                data_point, replay_mode)
                return

            # If we reached here, we are a realtime/both consumer and it is
            # non-replay mode. Write the data and any additional cross-cluster
            # datapoints
            self._write_datapoint(data_point, stats)
            return
        else:
            # Sparseness is enabled. First, adjust epoch according to expected
            # granularity
            #
            # NOTE: When we support mixed granularities, we should add a method
            # here later to determine the appropriate series granularity. We
            # could even cache the granularity to be used in SparseSeriesStats
            data_point.adjust_epoch(self.context.schema.MIN_STORAGE_INTERVAL)

        if Settings.inst.sparse_store.enable_stream_transforms:
            # note the transforms mutate the data point and stats object
            if StreamTransforms.transform(self.context, data_point, stats):
                if TracingConfig.TRACE_ACTIVE:
                    logger.info('TRACE: sparse_series_writer.append: '
                                'data_point %s transformed',
                                str(data_point))
                # Report the transformation telemetry
                self._transformed_metrics += 1
                if self._transformed_metrics > 1000:
                    Telemetry.inst.registry.meter(
                        'SparseSeriesWriter.transform').mark(
                            self._transformed_metrics)
                    self._transformed_metrics = 0

        if data_point.epoch < stats.get_window_epoch(0):
            if TracingConfig.TRACE_ACTIVE:
                logger.info('TRACE: sparse_series_writer.append: '
                            'data_point %s dropped because its an insert'
                            ' stats %s',
                            str(data_point), str(stats))

            # Generate the telemetry to mark out of order data points
            Telemetry.inst.registry.meter(
                'SparseSeriesWriter.unsupported_inserts').mark()
            # Out of order data-points could happen because of clock jumps etc
            # We just ignore out of order points at this time & track them
            # in our telemetry. Can create alerts for these if the need arises.
            return

        # First check if we missed any datapoints in between this datapoint and
        # the last received datapoint
        missing_pt_marker = SeriesWriterBase.check_inline_missing_points( \
                                        self.data_gap_detection_interval,
                                        data_point, stats)
        if missing_pt_marker is not None:
            self.create_marker(missing_pt_marker, stats, replay_mode)

        # Run our sparseness checks
        if self.consumer_mode != MODE_ROLLUP:
            result = self.sparse_algo_selector.process(data_point, stats,
                                                       replay_mode)
        else:
            # For rollup consumer, we avoid running sparseness checks on raw
            # datapoints. Thus every raw datapoint is treated as unskippable
            result = DATA_WRITE
            # Still need to call update_received_point() which is called
            # internally by sparse_algo_selector.process() because it updates
            # info about last received datapoint. This info is used later to
            # detect dead series
            self.sparse_algo_selector.update_received_point(
                data_point, stats, window=0, replay_mode=replay_mode)

        if result == DATA_DROP:
            # We are skipping writing because of our sparseness checks
            if TracingConfig.TRACE_ACTIVE:
                logger.info('TRACE: sparse_series_writer.append: '
                            'data_point %s dropped because '
                            'write=%s stats %s',
                            str(data_point), result, str(stats))
            return
        if replay_mode:
            # This is a datapoint being reprocessed and thus we don't
            # need to write. However we do need to update the stats objects in
            # order to rebuild state
            self._update_rollups_and_stat_object(data_point, stats,
                                                 replay_mode)
            if TracingConfig.TRACE_ACTIVE:
                logger.info('TRACE: sparse_series_writer.append: '
                            'data_point %s dropped because '
                            'replay_mode=%s stats %s',
                            str(data_point), replay_mode, str(stats))
            return

        if result == DATA_FORCE_WRITE:
            # If we reach here, it means that we have a forced write in non
            # replay mode. We also generate a datapoint for the exploration
            # datasource. That way we can ensure atleast one datapoint exists
            # for each series in a DAILY exploration segment
            self.write_exploration_dp(data_point)

        # Write the original datapoint because it wasn't dropped by our
        # sparseness checks
        self._write(data_point, stats, replay_mode)

    def write_exploration_dp(self, data_point: DataPoint) -> None:
        """
        Generate & store a datapoint for the exploration datasource
        :param data_point: Original received data point
        :return:
        """
        exploration_ds = \
            self.context.schema.exploration_datasources.get(self.protocol)

        # Nothing to do if exploration datasource has not been specified
        if exploration_ds is None:
            return

        exploration_dp = DataPoint(
            exploration_ds, data_point.field, data_point.tags,
            data_point.epoch, data_point.server_rx_epoch, data_point.value)

        # Avoid calling self._write() because it creates rollup values. We
        # don't need rollup values for exploration datapoints.
        self.sparse_store.write(exploration_dp)

    def _write(self, data_point: DataPoint, stat: SeriesStatsBase,
               replay_mode: bool) -> None:
        """
        Write the data point to store
        :param data_point:
        :param stat:
        """
        if self.write_raw_datapoints and not replay_mode:
            # Generate raw writes only if write_raw_datapoints is True and this
            # is a datapoint received in non-replay mode
            self._write_datapoint(data_point, stat)

        if self.write_rollup_datapoints:
            self.rollups.add(data_point, stat, replay_mode)
        self.sparse_algo_selector.update_written_point(data_point, stat)

    def _update_rollups_and_stat_object(self, data_point: DataPoint,
                                        stat: SparseSeriesStats,
                                        replay_mode: bool) -> None:
        """
        Update the rollups & the stats object with the datapoint
        :param data_point:
        :param stat:
        """
        # create the rollup for this data point too
        self.rollups.add(data_point, stat, replay_mode)
        # update the stats now
        self.sparse_algo_selector.update_written_point(data_point, stat)

    def get_stats(self, datapoint: DataPoint) -> SparseSeriesStats:
        """
        Get the stats for the series_id, if not in cache then load from store
        :param series_id:
        :return:
        """
        series_id = datapoint.series_id
        if series_id not in self.stats_cache:
            # Create Stats object
            # ourselves and cache it.
            stats = SparseSeriesStats()
            self.stats_cache[series_id] = stats
            # Call handler to perform additional bookkeeping when
            # a series is created for the first time
            self.handle_series_start(datapoint, stats)

            if Telemetry.inst is not None:
                Telemetry.inst.registry.meter(
                    'SparseSeriesWriter.stats_cache.miss').mark()
        return self.stats_cache[series_id]

    def handle_series_start(self, datapoint: DataPoint, stats: SeriesStatsBase):
        """
        Handle additional logic when a series is created for the first time

        In addition to the parent class logic, we need to set the first
        received epoch. This is needed when merging with the recovery consumer
        """
        super(SparseSeriesWriter, self).handle_series_start(datapoint, stats)
        # Update first received epoch
        stats.set_first_epoch(datapoint.epoch)
