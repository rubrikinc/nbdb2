"""
Rollups
"""
import copy
import logging
from typing import List, Dict

from nbdb.common.data_point import DataPoint, DATA_DROP
from nbdb.common.data_point import MISSING_POINT_VALUE, TOMBSTONE_VALUE
from nbdb.common.metric_parsers import MetricParsers
from nbdb.common.telemetry import Telemetry
from nbdb.common.tracing_config import TracingConfig
from nbdb.config.settings import Settings
from nbdb.schema.schema import Schema, ROLLUP_MEAN, ROLLUP_LAST, \
    ROLLUP_MAX, ROLLUP_SUM, \
    DASHBOARD_QUERIES, ROLLUP_NONE
from nbdb.store.sparse_algo_selector import SparseAlgoSelector
from nbdb.store.sparse_series_stats import SparseSeriesStats
from nbdb.store.sparse_store import SparseStore

from nbdb.schema.schema import SHARD_BY_CLUSTER, SHARD_BY_METRIC

logger = logging.getLogger()


# pylint: disable=too-few-public-methods
class Rollups:
    """
    Streaming rollup values computed as the raw points are processed
    The intermediate values are stored in the stats object.
    The rolled up value is pushed to the store after sparse check
    i.e. rolled up value is also sparse in nature
    """

    def __init__(self,
                 rollup_settings: Settings,
                 schema: Schema,
                 sparse_algo_selector: SparseAlgoSelector,
                 sparse_store: SparseStore,
                 past_message_lag: int=None,
                 future_message_lag: int=None):
        """
        Initialize the Rollups
        :param windows: List of window intervals for which to compute rollup
        :param schema: Schema object, is used to identify rollup function
        :param sparse_algo_selector: Sparse algorithm selector (for computing
        sparseness of the rolled up value)
        :param sparse_store: SparseStore where rolled up value is written after
        sparseness check
        """
        self.should_report_clock_skew: bool = rollup_settings.report_clock_skew
        self.windows: List[int] = schema.rollup_windows
        self.schema: Schema = schema
        self.sparse_algo_selector: SparseAlgoSelector = sparse_algo_selector
        self.sparse_store: SparseStore = sparse_store
        # maintain a count of rollups written per window
        self._telemetry_rollup_count: Dict[int, int] = dict()
        for window in self.windows:
            self._telemetry_rollup_count[window] = 0
        self.duplicated_data_points = 0
        self.past_message_lag = past_message_lag
        self.future_message_lag = future_message_lag

    def add(self, data_point: DataPoint, stat: SparseSeriesStats,
            replay_mode: bool) -> None:
        """
        # TODO: Handle the Tombstones, we will receive tombstone values on
        #  raw data points
        :param data_point:
        :param stat:
        :param replay_mode:
        :return:
        """
        # check if this is a collocated data point
        # no need for a rollup, collocated data point is a duplicate
        # datapoint that is also stored in a collocated datasource
        # same point will also be written to a regular datasource and
        # that will be rolled up
        if data_point.datasource == DASHBOARD_QUERIES:
            return

        # Check if we have any history to compute rollup with
        last_unrolled_epoch = stat.get_window_epoch(window=0)
        if last_unrolled_epoch <= 0:
            # This is the first datapoint received. This datapoint's epoch will
            # become the last unrolled epoch
            last_unrolled_epoch = data_point.epoch

        func_name = stat.get_rollup_function()
        if func_name == ROLLUP_NONE:
            func_name = self.schema.get_rollup_function(data_point.series_id)
            stat.set_rollup_function(func_name)
        for window in self.windows:
            self._add_to_window(window,
                                func_name,
                                data_point,
                                stat,
                                last_unrolled_epoch,
                                replay_mode)

    def _add_to_window(self,
                       window: int,
                       func_name: int,
                       data_point: DataPoint,
                       stat: SparseSeriesStats,
                       last_unrolled_epoch: int,
                       replay_mode: bool) -> None:
        """
        :param window: rollup window interval (in seconds)
        :param func_name: rollup aggregation function to use
        :param data_point: new data point received
        :param stat: stats object
        :param last_unrolled_epoch: Refers to the epoch of the last raw
        datapoint that was written (ie. not dropped due to sparseness)
        :param replay_mode: Boolean indicating whether datapoint was received
        in replay mode or not
        :return:
        """
        # get the last checkpoint for this window
        last_check_point = stat.get_check_point(window, -1)

        if last_check_point == -1:
            last_check_point = Rollups._init_checkpoint(window,
                                                        stat,
                                                        data_point.epoch)
        if data_point.epoch < last_check_point:
            # Don't process if the data point is older than last_check_point
            if TracingConfig.TRACE_ACTIVE:
                logger.info('TRACE: rollups: Skipping raw datapoint because '
                            'older than last checkpoint. data_point=%s '
                            'window=%s last_check_point=%s', data_point,
                            window, last_check_point)
            return

        # check if the new data point is within this rollup window
        next_check_point = last_check_point + window
        while next_check_point <= data_point.epoch:
            # new data point is beyond the checkpoint
            # so we can compute the rollup value for this window
            self._process_check_point(window,
                                      func_name,
                                      last_unrolled_epoch,
                                      last_check_point,
                                      next_check_point,
                                      stat,
                                      data_point,
                                      replay_mode)
            last_check_point = next_check_point
            next_check_point = next_check_point + window

        if data_point.value == TOMBSTONE_VALUE:
            # If we get a tombstone value, we generate a value with the
            # intermediate computations for the next check point, and insert a
            # tombstone right after that
            self._process_tombstone(window,
                                    func_name,
                                    last_unrolled_epoch,
                                    data_point.epoch,
                                    last_check_point,
                                    next_check_point,
                                    stat,
                                    data_point,
                                    replay_mode)
            # Nothing more should be done. Return
            return

        if data_point.epoch < next_check_point:
            # still within the current window, incrementally add the
            # value to the intermediate rollup value
            # last value received was valid for delta epoch
            start_epoch = max(last_check_point, last_unrolled_epoch)
            delta_epoch = data_point.epoch - start_epoch
            self._increment(window, func_name, stat, delta_epoch)
            if TracingConfig.TRACE_ACTIVE:
                logger.info('TRACE: rollups: Raw datapoint within '
                            'next_check_point. Incrementing. data_point=%s '
                            'window=%s next_check_point=%s delta_epoch=%s',
                            data_point, window, next_check_point, delta_epoch)

    @staticmethod
    def _init_checkpoint(window: int,
                         stat: SparseSeriesStats,
                         latest_unrolled_epoch: int) -> int:
        """
        Initialize the checkpoint for the window
        :param window:
        :param stat:
        :param latest_unrolled_epoch: Latest raw epoch to be added to a window.
        :return: checkpoint
        """
        # Initialize the checkpoint
        # checkpoint should be aligned to window boundary
        last_check_point = int(latest_unrolled_epoch / window) * window
        stat.set_check_point(window, last_check_point)
        return last_check_point

    def _process_check_point(self,
                             window: int,
                             func_name: int,
                             last_unrolled_epoch: int,
                             last_check_point: int,
                             next_check_point: int,
                             stat: SparseSeriesStats,
                             data_point: DataPoint,
                             replay_mode: bool) -> None:
        """

        --LC|---IV---NC|-------|-------|--------------

        :param window:
        :param func_name:
        :param last_unrolled_epoch:
        :param last_check_point:
        :param next_check_point:
        :param stat:
        :param data_point:
        :return:
        """
        # restrict the computation of delta epoch to current window only
        start_epoch = max(last_unrolled_epoch, last_check_point)
        delta_epoch = next_check_point - start_epoch
        # increment the rollup algo with last value which was valid for
        # delta epoch
        Rollups._increment(window, func_name, stat, delta_epoch)
        # compute the rolled up value now, and advance the rollup window
        rollup_value = Rollups._compute(window, func_name, stat)
        if rollup_value is None:
            # Create a missing point marker datapoint
            rollup_data_point = self._create_rollup_data_point(
                window, next_check_point, MISSING_POINT_VALUE, data_point,
                # This is a special value. Make sure it's NOT normalized before
                # storing
                is_special_value=True)
        else:
            # Create a data point with the computed rolled up value
            rollup_data_point = self._create_rollup_data_point(
                window, next_check_point, rollup_value, data_point)

        # Now verify if this is sparse
        result = self.sparse_algo_selector.process(rollup_data_point, stat,
                                                   replay_mode, window)
        if TracingConfig.TRACE_ACTIVE:
            logger.info('TRACE: rollups._process_check_point: '
                        'rollup_data_point=%s replay_mode=%s window=%s '
                        'result:%s', rollup_data_point, replay_mode, window,
                        result)

        if result != DATA_DROP:
            self._write_to_store(window, stat, rollup_data_point,
                                 replay_mode)

        # set the new checkpoint
        stat.set_check_point(window, next_check_point)

    # pylint: disable-msg=R0913  # Too Many Arguments
    # pylint: disable-msg=R0914  # Too Many Locals
    def _process_tombstone(self,
                           window: int,
                           func_name: int,
                           last_unrolled_epoch: int,
                           unrolled_tombstone_epoch: int,
                           last_check_point: int,
                           next_check_point: int,
                           stat: SparseSeriesStats,
                           data_point: DataPoint,
                           replay_mode: bool) -> None:
        """
        Process any partial computations and generate datapoints for it before
        generating a tombstone datapoint. This function should only be called
        if a tombstone value was received for the raw series

        Reference illustration:
        --LC|---IV-TB--NC|-------|-------|--------------

        :param window:
        :param func_name:
        :param last_unrolled_epoch:
        :param last_check_point:
        :param next_check_point:
        :param stat:
        :param data_point:
        :return:
        """
        # restrict the computation of delta epoch to current window only
        start_epoch = max(last_unrolled_epoch, last_check_point)
        delta_epoch = unrolled_tombstone_epoch - start_epoch
        # increment the rollup algo with last value which was valid for
        # delta epoch
        Rollups._increment(window, func_name, stat, delta_epoch)
        # compute the rolled up value now, and advance the rollup window
        rollup_value = Rollups._compute(window, func_name, stat)
        if rollup_value is None:
            # No need to create a missing point marker. We will just directly
            # create the tombstone epoch
            rollup_tombstone_epoch = next_check_point
            if TracingConfig.TRACE_ACTIVE:
                logger.info('TRACE: rollups._process_tombstone: Not creating '
                            'partial window because NULL value. '
                            'data_point=%s window=%s replay_mode=%s',
                            data_point, window, replay_mode)
        else:
            # Create a rollup data point using the intermediate state
            rollup_data_point = self._create_rollup_data_point(
                window, next_check_point, rollup_value, data_point)

            if TracingConfig.TRACE_ACTIVE:
                logger.info('TRACE: rollups._process_tombstone: Creating '
                            'partial window value. rollup_data_point=%s '
                            'window=%s replay_mode=%s', rollup_data_point,
                            window, replay_mode)

            self._write_to_store(window, stat, rollup_data_point,
                                 replay_mode)

            # Store the rollup tombstone 10m after the intermediate state
            # rollup datapoint
            rollup_tombstone_epoch = next_check_point + window

        # Create a rollup tombstone datapoint
        rollup_tombstone = self._create_rollup_data_point(
            window, rollup_tombstone_epoch, TOMBSTONE_VALUE, data_point,
            # This is a special value. Make sure it's NOT normalized before
            # storing
            is_special_value=True)
        self._write_to_store(window, stat, rollup_tombstone, replay_mode)

    def generate_data_point_shard_by_metric(self, data_point: DataPoint,
                                            window: int,
                                            shard: int) -> DataPoint:
        """
        Generate a duplicate datapoint for the rollup cross-cluster datasource.

        Duplicating to the cross-cluster datasources enables fast cross-cluster
        querying.

        NOTE: We assume that the cross-cluster pattern matching has already
        been done & verified before this method is called
        """
        datasource_name = self.schema.compose_crosscluster_datasource(
            shard, window)
        dup_data_point = copy.deepcopy(data_point)
        dup_data_point.datasource = datasource_name
        return dup_data_point

    def _write_to_store(self,
                        window: int,
                        stat: SparseSeriesStats,
                        rollup_data_point: DataPoint,
                        replay_mode: bool) -> None:
        """
        Write the data point to store
        :param window:
        :param rollup_data_point:
        """
        if not replay_mode:
            # We skip generating any writes for replay mode datapoints
            self.sparse_store.write(rollup_data_point)

            if TracingConfig.TRACE_ACTIVE:
                logger.info('TRACE: rollups: Writing to store'
                            'rollup_data_point=%s replay_mode=%s window=%s ',
                            rollup_data_point, replay_mode, window)

            # Check if datapoint matches one of the cross-cluster patterns. If
            # so, we duplicate a datapoint to the rollup cross-cluster
            # datasource to enable fast cross-cluster querying
            shard = stat.get_crosscluster_shard()
            if shard is not None:
                # Datapoint matched one of the cross-cluster patterns
                dup_data_point = self.generate_data_point_shard_by_metric(
                        rollup_data_point, window, shard)
                self.sparse_store.write(dup_data_point)

                if TracingConfig.TRACE_ACTIVE:
                    logger.info('TRACE: rollups: Writing dup_cc_point'
                                'dup_data_point=%s replay_mode=%s '
                                'window=%s', dup_data_point, replay_mode,
                                window)

                self.duplicated_data_points += 1
                # meter.mark calls are expensive, amortize it across 1000 calls
                if self.duplicated_data_points > 1000:
                    Telemetry.inst.registry.meter(
                        'MetricConsumer.duplicated_rollup_cc_data_points'
                    ).mark(self.duplicated_data_points)
                    self.duplicated_data_points = 0

        # Report the telemetry to so we can track rollups are generating data
        _rollups_written = self._telemetry_rollup_count[window]
        _rollups_written += 1
        # meter.mark calls are expensive, amortize it across 1000 counts
        if _rollups_written > 1000:
            Telemetry.inst.registry.meter(
                'SparseSeriesWriter.rollups.sparse_writes',
                tag_key_values=[f"Window={window}"]).mark(_rollups_written)
            _rollups_written = 0
        self._telemetry_rollup_count[window] = _rollups_written
        # Update the windows point that got written
        self.sparse_algo_selector.update_written_point(rollup_data_point,
                                                       stat,
                                                       window)

    @staticmethod
    def _increment(window: int,
                   func_name: int,
                   stat: SparseSeriesStats,
                   delta_epoch: int) -> None:
        """

        :param window:
        :param func_name:
        :param stat:
        :param delta_epoch:
        :return:
        """
        last_value = stat.get_window_value(0)
        if last_value < 0:
            # ignore missing data points
            return

        # Keep track of sum of delta epochs. This helps us determine the number
        # of non-NULL datapoints seen in the window
        delta_epoch_sum = stat.get_rollup_intermediate_delta_epoch(window)
        delta_epoch_sum += delta_epoch
        stat.set_rollup_intermediate_delta_epoch(window, delta_epoch_sum)

        if func_name == ROLLUP_MEAN:
            weighted_sum = stat.get_rollup_intermediate_value(window)
            weighted_sum += last_value * delta_epoch
            stat.set_rollup_intermediate_value(window, weighted_sum)
        elif func_name == ROLLUP_MAX:
            max_so_far = stat.get_rollup_intermediate_value(window)
            max_so_far = max(max_so_far, last_value)
            stat.set_rollup_intermediate_value(window, max_so_far)
        elif func_name == ROLLUP_LAST:
            stat.set_rollup_intermediate_value(window, last_value)
        elif func_name == ROLLUP_SUM:
            weighted_sum = stat.get_rollup_intermediate_value(window)
            weighted_sum += last_value * delta_epoch
            stat.set_rollup_intermediate_value(window, weighted_sum)
        else:
            raise ValueError('Unsupported rollup function: ' + str(func_name))

    @staticmethod
    def _compute(window: int,
                 func_name: int,
                 stat: SparseSeriesStats) -> float:
        """

        :param window:
        :param func_name:
        :param stat:
        :return:
        """
        try:
            intermediate_value = stat.get_rollup_intermediate_value(window)
            delta_epoch_sum = stat.get_rollup_intermediate_delta_epoch(window)
            if delta_epoch_sum == 0:
                # No datapoints were seen for this window. Return NULL
                # value
                return None

            if func_name == ROLLUP_MEAN:
                weighted_sum = intermediate_value
                return weighted_sum / delta_epoch_sum
            if func_name == ROLLUP_MAX:
                return intermediate_value
            if func_name == ROLLUP_LAST:
                return intermediate_value
            if func_name == ROLLUP_SUM:
                return intermediate_value
            raise ValueError('Unsupported rollup function: ' + str(func_name))
        finally:
            # reset the intermediate value & delta_epoch for next checkpoint
            Rollups._reset_intermediate_state(window, stat)

    @staticmethod
    def _reset_intermediate_state(window: int,
                                  stat: SparseSeriesStats) -> None:
        """
        Reset the intermediate value & delta_epoch for next checkpoint
        :param window:
        :param stat:
        """
        stat.set_rollup_intermediate_value(window, 0)
        stat.set_rollup_intermediate_delta_epoch(window, 0)

    def _create_rollup_data_point(self,
                                  window: int,
                                  check_point: int,
                                  value: float,
                                  data_point: DataPoint,
                                  is_special_value: bool = False) -> DataPoint:
        """

        :param window: Rollup window
        :param check_point: epoch at which the rollup is created
        :param value: rollup value
        :param data_point: sample data point for the original series
        :param is_special_value: Indicates whether the rollup value is a
        special reserved value. Eg. -1 and -3 are reserved values for missing
        point & tombstone markers
        :return:
        """
        rollup_data_source = self.schema.get_rollup_datasource(window, data_point.tags)

        if self.should_report_clock_skew:
            MetricParsers.detect_and_report_clock_skew(
                data_point.server_rx_epoch, check_point, 'rollups',
                rollup_data_source, '',
                self.past_message_lag, self.future_message_lag)
        return DataPoint(rollup_data_source,
                         data_point.field,
                         data_point.tags,
                         check_point,
                         data_point.server_rx_epoch,
                         value,
                         is_special_value=is_special_value)

