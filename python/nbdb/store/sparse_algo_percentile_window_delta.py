"""
SparseAlgoPercentileWindowDelta
"""
from nbdb.config.settings import Settings
from nbdb.common.data_point import DataPoint
from nbdb.schema.schema import Schema
from nbdb.store.sparse_algo_last_value_delta import SparseAlgoLastValueDelta
from nbdb.store.sparse_series_stats import SparseSeriesStats
from nbdb.common.math_utils import quantile

class SparseAlgoPercentileWindowDelta(SparseAlgoLastValueDelta):
    """
    Compares the delta change between the current and last value written
    against the quantile in a recent moving window
    This does better than last value delta when we have big spikes
    in data frequently and in that case small relative changes are dropped
    """

    def __init__(self,
                 schema: Schema,
                 consumer_mode: str,
                 min_delta: float,
                 quantile: float,
                 window_size: int):
        SparseAlgoLastValueDelta.__init__(self, schema, consumer_mode,
                                          min_delta)
        self.quantile: float = quantile
        self.window_size: int = window_size

    def has_changed(self,
                    data_point: DataPoint,
                    stats: SparseSeriesStats,
                    window: int
                    ) -> bool:
        """
        Checks if the point has changed significantly compared to recent
        large spikes
        :param data_point:
        :param stats:
        :param window:
        :return:
        """
        moving_window = stats.get_moving_window(window)
        if len(moving_window) > 0:
            quantile_value = quantile(moving_window, self.quantile)
            if quantile_value > 0:
                delta = abs(data_point.value-stats.get_window_value(window)) /\
                        quantile_value
                return delta > self.min_delta
        return SparseAlgoLastValueDelta.has_changed(self,
                                                    data_point,
                                                    stats,
                                                    window)

    def update_received_point(self,
                              data_point: DataPoint,
                              stats: SparseSeriesStats,
                              window: int,
                              replay_mode: bool) -> None:
        """
        Update the stats
        :param data_point:
        :param stats:
        :param window:
        :return:
        """
        SparseAlgoLastValueDelta.update_received_point(
            self, data_point, stats, window, replay_mode)
        stats.append_moving_window_value(window,
                                         data_point.value,
                                         self.window_size)
