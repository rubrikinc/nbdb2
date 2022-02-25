"""
SparseSeriesStats
"""
from __future__ import annotations

import json
from typing import List

# Dynamic Attributes
from nbdb.schema.schema import ROLLUP_NONE
from nbdb.store.sparse_series_stats_base import SeriesStatsBase

PRE_TRANSFORM_VALUE = 'pre_transform_value'
WINDOW_VALUE = 'window_value'
WINDOW_EPOCH = 'window_epoch'
WINDOW_EPOCH0 = 'window_epoch0'
WINDOW_CHECK_POINT = 'window_check_point'
WINDOW_MOVING_WINDOW = 'window_moving_window'
WINDOW_ROLLUP_INTERMEDIATE_VALUE = 'window_rollup_intermediate_value'
WINDOW_ROLLUP_INTERMEDIATE_DELTA_EPOCH = \
    'window_rollup_intermediate_delta_epoch'
SPARSE_ALGO = 'sparse_algo'
REPLAY_MODE = 'replay_mode'
ROLLUP_FUNC_NAME = 'rollup_func_name'

FIRST_EPOCH = 'first_epoch'
FIRST_EPOCH_NOT_EXIST = -1

# pylint: disable=R0904 # Too Many Public Methods
class SparseSeriesStats(SeriesStatsBase):
    """
    The stats object maintains basic stats for a given series that are
    required to identify the stored key
    """

    def __str__(self):
        """
        Override the default to string method to provide a more human
        readable value
        :return:
        """
        d = dict(self.__dict__)
        sparse_algo = self.get_algo('None')
        if sparse_algo != 'None':
            d[SPARSE_ALGO] = type(sparse_algo).__name__
        return json.dumps(d, sort_keys=True)

    def set_replay_mode(self, mode: bool) -> SparseSeriesStats:
        """
        Indicates whether last point received was processed in replay mode
        :param mode:
        """
        setattr(self, REPLAY_MODE, mode)
        return self

    def get_replay_mode(self) -> bool:
        """
        Indicates whether last point received was processed in replay mode
        :return:
        """
        return getattr(self, REPLAY_MODE, False)

    def set_window_epoch(self, window: int, epoch: int) -> SparseSeriesStats:
        """

        :param window:
        :param epoch:
        """
        setattr(self, WINDOW_EPOCH + str(window), epoch)
        return self

    def get_window_epoch(self, window: int) -> int:
        """
        :param window
        :return:
        """
        # Special cases for performance since this is the most accessed window
        if window == 0:# pylint: disable=no-else-return
            return getattr(self, WINDOW_EPOCH0, 0)
        else:
            return getattr(self, WINDOW_EPOCH + str(window), 0)

    def set_window_value(self, window: int, value: float) -> SparseSeriesStats:
        """

        :param window:
        :param value:
        :return:
        """
        setattr(self, WINDOW_VALUE + str(window), value)
        return self

    def get_window_value(self, window: int) -> float:
        """

        :param window:
        :param default_value:
        :return:
        """
        return getattr(self, WINDOW_VALUE + str(window), 0)

    def set_rollup_intermediate_value(self,
                                      window: int,
                                      value: float) -> SparseSeriesStats:
        """

        :param window:
        :param value:
        :return:
        """
        setattr(self, WINDOW_ROLLUP_INTERMEDIATE_VALUE + str(window), value)
        return self

    def get_rollup_intermediate_value(self, window: int) -> float:
        """

        :param window:
        :return:
        """
        return getattr(self, WINDOW_ROLLUP_INTERMEDIATE_VALUE + str(window), 0)

    def set_rollup_intermediate_delta_epoch(
            self, window: int, delta_epoch: int) -> SparseSeriesStats:
        """

        :param window:
        :param value:
        :return:
        """
        setattr(self, WINDOW_ROLLUP_INTERMEDIATE_DELTA_EPOCH + str(window),
                delta_epoch)
        return self

    def get_rollup_intermediate_delta_epoch(self, window: int) -> int:
        """

        :param window:
        :return:
        """
        return getattr(self, WINDOW_ROLLUP_INTERMEDIATE_DELTA_EPOCH +
                       str(window), 0)

    def set_check_point(self, window: int, epoch: int) -> SparseSeriesStats:
        """

        :param window:
        :param epoch:
        """
        setattr(self, WINDOW_CHECK_POINT + str(window), epoch)
        return self

    def get_check_point(self, window: int, default_value: int = None) -> int:
        """

        :param window:
        :param default_value:
        :return:
        """
        return getattr(self, WINDOW_CHECK_POINT + str(window), default_value)

    def set_algo(self, algo) -> SparseSeriesStats:
        """

        :param algo:
        """
        setattr(self, SPARSE_ALGO, algo)
        return self

    def get_algo(self, default_algo):
        """
        :param default_algo:
        :return:
        """
        return getattr(self, SPARSE_ALGO, default_algo)

    def set_pre_transform_value(self, value: float) -> SparseSeriesStats:
        """

        :param value:
        """
        setattr(self, PRE_TRANSFORM_VALUE, value)
        return self

    def get_pre_transform_value(self, default_value: float) -> float:
        """

        :return:
        """
        return getattr(self, PRE_TRANSFORM_VALUE, default_value)

    def append_moving_window_value(self,
                                   window: int,
                                   value: int,
                                   window_length: int) -> SparseSeriesStats:
        """

        :param window:
        :param value:
        :param window_length:
        :return:
        """
        moving_window = self.get_moving_window(window)
        moving_window.append(value)
        if len(moving_window) > window_length:
            del moving_window[0]
        return self.set_moving_window_value(window, moving_window)

    def set_moving_window_value(self,
                                window: int,
                                moving_window: List[int]) -> SparseSeriesStats:
        """

        :param window:
        :param moving_window:
        :return:
        """
        setattr(self, WINDOW_MOVING_WINDOW + str(window), moving_window)
        return self

    def get_moving_window(self, window: int) -> List[float]:
        """

        :param window:
        :return:
        """
        return getattr(self, WINDOW_MOVING_WINDOW + str(window), list())

    def get_rollup_function(self) -> int:
        """

        :return:
        """
        return getattr(self, ROLLUP_FUNC_NAME, ROLLUP_NONE)

    def set_rollup_function(self, func_name: int) -> SparseSeriesStats:
        """

        :param func_name:
        :return:
        """
        setattr(self, ROLLUP_FUNC_NAME, func_name)
        return self

    def set_first_epoch(self, epoch: int) -> SeriesStatsBase:
        """
        epoch of the first point received
        used to identify missing data points in replay
        :param epoch:
        """
        if self.get_first_epoch() == FIRST_EPOCH_NOT_EXIST or \
            self.get_first_epoch() > epoch:
            setattr(self, FIRST_EPOCH, epoch)
        return self

    def get_first_epoch(self) -> int:
        """
        epoch of the first point received
        used to identify missing data points in replay
        :return:
        """
        return getattr(self, FIRST_EPOCH, FIRST_EPOCH_NOT_EXIST)

