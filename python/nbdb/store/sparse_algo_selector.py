"""
SparseAlgoSelector
"""
import logging
import re
from re import Pattern
from typing import Dict, List

from nbdb.common.context import Context
from nbdb.common.tracing_config import TracingConfig
from nbdb.config.settings import Settings
from nbdb.common.data_point import DataPoint
from nbdb.common.telemetry import Telemetry
from nbdb.schema.schema import Schema, SparseAlgoSetting
from nbdb.store.sparse_algo_last_value_delta import SparseAlgoLastValueDelta
from nbdb.store.sparse_algo_lossless import SparseAlgoLossLess
from nbdb.store.sparse_algo_outlier_type import SparseAlgoOutlierType
from nbdb.store.sparse_algo_percentile_window_delta import \
    SparseAlgoPercentileWindowDelta
from nbdb.store.sparse_store import SparseStore
from nbdb.store.sparse_series_stats import SparseSeriesStats
from nbdb.store.sparse_telemetry import SparseTelemetry

logger = logging.getLogger()


class SparseAlgoSelector:
    """
    Based on regex patterns defined in schema.yaml file
    decides which algorithm to use for a data point
    """

    def __init__(self,
                 context: Context,
                 sparse_telemetry_settings: Settings,
                 sparse_telemetry_source_id: str,
                 sparse_store: SparseStore,
                 consumer_mode: str):
        """
        Create the different sparse algorithms
        :param sparse_telemetry_settings:
        :param consumer_settings:
        :param schema:
        :param sparse_store: Needed for some algos that can generate
        :param consumer_mode: Indicates whether consumer generates realtime
        datapoints, rollup datapoints or both.
        additional data points
        """
        self.context = context
        self.sparse_telemetry = SparseTelemetry(sparse_telemetry_settings,
                                                sparse_telemetry_source_id,
                                                consumer_mode)

        # Compile all the patterns and map them to corresponding algos
        self.pattern_to_algo: Dict[Pattern, SparseAlgoLossLess] = dict()
        self.default_algo: SparseAlgoLossLess = None
        sparse_algos: List[SparseAlgoSetting] = context.schema.sparse_algos()
        for algo_settings in sparse_algos:
            algo = SparseAlgoSelector.instantiate_sparse_algo(
                algo_settings,
                context.schema,
                consumer_mode,
                sparse_store)
            if algo_settings.patterns is None or\
                    len(algo_settings.patterns) <= 0:
                self.default_algo = algo
            for regex in algo_settings.patterns:
                pattern = re.compile(regex)
                self.pattern_to_algo[pattern] = algo

    @staticmethod
    def instantiate_sparse_algo(algo_setting: SparseAlgoSetting,
                                schema: Schema,
                                consumer_mode: str,
                                sparse_store: SparseStore) -> \
            SparseAlgoLossLess:
        """
        Instantiate the sparse algorithm
        :param algo_setting:
        :param schema:
        :param sparse_store:
        :return:
        """
        if algo_setting.algo == SparseAlgoLossLess.__name__:
            return SparseAlgoLossLess(schema, consumer_mode)
        if algo_setting.algo == SparseAlgoLastValueDelta.__name__:
            return SparseAlgoLastValueDelta(schema, consumer_mode,
                                            algo_setting.min_delta)
        if algo_setting.algo == SparseAlgoPercentileWindowDelta.__name__:
            return SparseAlgoPercentileWindowDelta(schema, consumer_mode,
                                                   algo_setting.min_delta,
                                                   algo_setting.quantile,
                                                   algo_setting.window_size)
        if algo_setting.algo == SparseAlgoOutlierType.__name__:
            return SparseAlgoOutlierType(schema, consumer_mode,
                                         sparse_store,
                                         algo_setting)
        raise ValueError('Unsupported algo: {}'.format(algo_setting.algo))

    def process(self,
                data_point: DataPoint,
                stats: SparseSeriesStats,
                replay_mode: bool,
                window: int = 0) -> int:
        """
        Applies the sparse algorithm based on schema.yaml authoring
        Also calls the telemetry to report the drops
        :param data_point:
        :param stats:
        :param replay_mode: Indicates whether data_point is being processed in
        replay/rewind phase or live phase
        :param window: rollup window
        :return: Integer enum indicating whether point should be dropped,
        written or force written.
        """
        algo: SparseAlgoLossLess = self.select_algo(data_point, stats)
        if TracingConfig.TRACE_ACTIVE:
            logger.info('data_point: %s algo: %s',
                        data_point.series_id, algo.__class__.__name__)

        result = algo.process(data_point, stats, window, replay_mode)
        # Report the telemetry
        # We skip per data point telemetry in batch mode for performance
        # reasons. Overall sparseness metrics are captured at sparse converter.
        # TODO: Plumb per algo type metrics for batch conversion
        if not self.context.schema.batch_mode:
            self.sparse_telemetry.report(data_point,
                                         stats,
                                         type(algo).__name__,
                                         result)
        return result

    def update_received_point(self,
                              data_point: DataPoint,
                              stats: SparseSeriesStats,
                              replay_mode: bool,
                              window: int = 0) -> None:
        """
        Update the stats object for points received
        :param data_point:
        :param stats:
        :param window:
        :return:
        """
        algo: SparseAlgoLossLess = self.select_algo(data_point, stats)
        algo.update_received_point(data_point, stats, window, replay_mode)

    def update_written_point(self,
                             data_point: DataPoint,
                             stats: SparseSeriesStats,
                             window: int = 0) -> None:
        """
        Update the stats object with the data point that was written
        :param data_point:
        :param stats:
        :param window:
        """
        algo: SparseAlgoLossLess = self.select_algo(data_point, stats)
        algo.update_written_point(data_point, stats, window)

    def select_algo(self,
                    data_point: DataPoint,
                    stats: SparseSeriesStats) -> SparseAlgoLossLess:
        """
        Select algorithm for the data point
        the selection is cached in the stats object, so the actual matching
        is done only once in the beginning
        :param data_point:
        :param stats:
        :return:
        """
        if stats.get_window_epoch(0) <= 0:
            # First time we need to select an algo
            algo: SparseAlgoLossLess = self.match_algo(data_point)
            if algo is not None:
                stats.set_algo(algo)

            if Telemetry.inst is not None:
                if algo is not None:
                    algo_name = algo.__class__.__name__
                else:
                    algo_name = self.default_algo.__class__.__name__

                Telemetry.inst.registry.meter(
                    measurement='SparseSeriesWriter.algo.series',
                    tag_key_values=[f"Algo={algo_name}"]
                ).mark()

            if algo is not None:
                return algo
        return stats.get_algo(self.default_algo)

    def match_algo(self, data_point: DataPoint) -> SparseAlgoLossLess:
        """
        Match the data point field against the authored regexes
        to choose the sparse algorithm to use
        :param data_point:
        :return specific algorithm or None if no explicit rule exists
        """
        for pattern, algo in self.pattern_to_algo.items():
            if pattern.match(data_point.series_id):
                return algo

        return None

    def get_algo(self, algo_name) -> SparseAlgoLossLess:
        """
        Get the algorithm by the given name
        :param algo_name:
        :return:
        """
        for algo in self.pattern_to_algo.values():
            if algo.__class__.__name__ == algo_name:
                return algo

        raise ValueError('{} not defined in schema'.format(algo_name))
