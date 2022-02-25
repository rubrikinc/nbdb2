"""
StreamTransforms
"""
from nbdb.common.context import Context
from nbdb.common.data_point import DataPoint
from nbdb.common.data_point import MISSING_POINT_VALUE
from nbdb.schema.schema import TRANSFORM_TYPE, SUFFIX
from nbdb.store.sparse_series_stats import SparseSeriesStats


class StreamTransforms:
    """
    Acts on live stream of data and transforms the values with last known
    state only
    """
    @staticmethod
    def transform(context: Context,
                  data_point: DataPoint,
                  stats: SparseSeriesStats) -> bool:
        """
        Applies the stream transforms based on heuristics
        :param data_point:
        :param stats
        :return True if the metric was transformed
        """
        transform_settings = context.schema.check_transform_type(
            data_point.series_id)
        if transform_settings is None:
            return False

        if transform_settings[TRANSFORM_TYPE] == 'non_negative_derivative':
            StreamTransforms.non_negative_derivative(data_point, stats)
            data_point.field += '.' + transform_settings[SUFFIX]
        return True

    @staticmethod
    def non_negative_derivative(data_point: DataPoint,
                                stats: SparseSeriesStats) -> None:
        """
        Computes a derivative based on last known value
        :param data_point:
        :param stats:
        :return:
        """
        data_point.pre_transform_value = data_point.value
        if (stats.get_refresh_epoch() <= 0 or
                stats.get_pre_transform_value(-1) == -1):
            # unable to compute a derivative when no previous
            # value available
            data_point.value = MISSING_POINT_VALUE
            return

        delta_time = data_point.epoch - stats.get_window_epoch(0)
        if delta_time <= 0:
            data_point.value = 0
            return

        # Note the stats.window_value is actually last derivative value
        # the count value is stored explicitly in stats.data
        data_point.value = (data_point.value -
                            stats.get_pre_transform_value(0))/delta_time
