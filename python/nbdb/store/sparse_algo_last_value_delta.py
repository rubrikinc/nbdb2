"""
SparseAlgoLastValueDelta
"""
from nbdb.schema.schema import Schema
from nbdb.common.data_point import DataPoint
from nbdb.store.sparse_series_stats import SparseSeriesStats
from nbdb.store.sparse_algo_lossless import SparseAlgoLossLess


class SparseAlgoLastValueDelta(SparseAlgoLossLess):
    """
    Compares the ratio of delta change from last value against last value
    if it is below a certain threshold we drop the data point
    """

    def __init__(self, schema: Schema, consumer_mode: str, min_delta: float):
        """
        Initialize
        :param sparse_algo_settings:
        """
        SparseAlgoLossLess.__init__(self, schema, consumer_mode)
        self.min_delta: float = min_delta

    def has_changed(self,
                    data_point: DataPoint,
                    stats: SparseSeriesStats,
                    window: int
                    ) -> bool:
        """
        Returns true if the point is significant and should be written
        :param data_point:
        :param stats:
        :param window:
        :return: True if the point should be written
        """
        last_value = stats.get_window_value(window)
        if last_value > 0:
            delta = abs(data_point.value - last_value)/last_value
            return delta > self.min_delta

        return SparseAlgoLossLess.has_changed(self, data_point, stats, window)
