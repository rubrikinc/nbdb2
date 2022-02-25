"""
DenseFunctions
"""
import logging
from typing import List

import numpy as np

from nbdb.readapi.sql_parser import SqlParser
from nbdb.readapi.time_series_response import TimeSeries

logger = logging.getLogger()


class DenseFunctionsProvider:
    """
    Dense functions expect time-series data that has equal sized intervals
    The data is processed by each group and then for each group by
    each epoch.
    All arguments for each epoch are collected and the function is called
    The returned value of the function is associated with the corresponding
    epoch and group
    """

    @staticmethod
    def timeshift(args: List[List[TimeSeries]]) -> List[TimeSeries]:
        """
        This function only operates on the time axis and not the value axis
        :param args: expects two args in the group
        first must be list of TimeSeries and second is a constant
        """
        if len(args) != 2:
            raise ValueError('timeshift method expects two params, '
                             'first param must result in a list of timeseries '
                             'second param must denote a time constant '
                             '+ve value shifts time into future '
                             '-ve value shifts time into past')

        time_series_list = args[0]
        constant_time_series_list = args[1]
        if len(constant_time_series_list) != 1 and \
                constant_time_series_list[0].series_id != '_constant_':
            raise ValueError('timeshift expects second param to be a'
                             ' constant time str')
        shift_str = constant_time_series_list[0].points[0][1]
        shift_value = SqlParser.time_str_parser(shift_str)
        result_time_series_list = list()
        for time_series in time_series_list:
            result_time_series_list.append(time_series.shift(shift_value))
        return result_time_series_list

    @staticmethod
    def topk(arr: List[float], k: int) -> List[float]:
        """
        returns top k elements in array
        :param arr:
        :param k:
        :return:
        """
        a = np.array(arr)
        return list(a[np.argpartition(a, -k)][-k:])

    @staticmethod
    def as_percentage(arr: List[float]) -> List[float]:
        """
        Compute values as percentage of the sum of arr
        :param arr:
        :return:
        """
        total = np.sum(arr)
        return [a*100/total for a in arr]

    @staticmethod
    def count(arr):
        """
        length of array
        :param arr:
        :return:
        """
        return len(arr)

    @staticmethod
    def shift(arr: List[float], amount: int) -> List[float]:
        """
        shifts the array elements right by amount
        :param arr: array to shift
        :param amount: number of positions to shift by
        +ve is to right -ve is to left
        """
        # ensure that amount is an integer
        amount = int(amount)
        e = np.empty_like(arr)
        if amount >= 0:
            e[:amount] = 0
            e[amount:] = arr[:-amount]
        else:
            e[amount:] = 0
            e[:amount] = arr[-amount:]
        return e

    @staticmethod
    def add(arg1: float, arg2: float) -> float:
        """
        + operator
        :param arg1
        :param arg2
        :return: result of arg1+ arg2
        """
        if arg1 is None or arg2 is None:
            return None
        return arg1 + arg2

    @staticmethod
    def sub(arg1: float, arg2: float) -> float:
        """
        - operator
        :param arg1
        :param arg2
        :return: result of arg1 - arg2
        """
        if arg1 is None or arg2 is None:
            return None
        return arg1 - arg2

    @staticmethod
    def mul(arg1: float, arg2: float) -> float:
        """
        * operator
        :param arg1:
        :param 1rg2:
        :return: result of arg1*arg2 or None if divide by zero happens
        """
        if arg1 is None or arg2 is None:
            return None
        return arg1 * arg2

    @staticmethod
    def div(arg1: float, arg2: float) -> float:
        """
        / operator
        :param arg1:
        :param arg2:
        :return: result of arg1/arg2 or None if divide by zero happens
        """
        if arg2 == 0:
            return None
        if arg1 is None or arg2 is None:
            return None
        return arg1 / arg2

    @staticmethod
    def percent(arg1: float, arg2: float) -> float:
        """
        / operator
        :param arg1:
        :param arg2:
        :return: result of arg1/arg2 or None if divide by zero happens
        """
        if arg2 == 0:
            return None
        if arg1 is None or arg2 is None:
            return None
        return arg1 * 100 / arg2
