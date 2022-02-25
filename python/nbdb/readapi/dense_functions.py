"""
DenseFunctions
"""
import logging
import uuid
from typing import List, Set, Dict, Tuple
import numpy as np

from nbdb.readapi.sql_parser import SqlParser, FillFunc
from nbdb.common.telemetry import Telemetry
from nbdb.readapi.dense_functions_provider import DenseFunctionsProvider
from nbdb.readapi.time_series_response import TimeSeriesGroup, TimeSeries, \
    TimeRange
from pyformance import meter_calls, time_calls

logger = logging.getLogger()


class DenseFunctions:
    """
    Dense functions expect time-series data that has equal sized intervals
    The data is processed by each group and then for each group by
    each epoch.
    All arguments for each epoch are collected and the function is called
    The returned value of the function is associated with the corresponding
    epoch and group
    """
    FUNC_MAP: Dict = {
        '+': 'add',
        '-': 'sub',
        '*': 'mul',
        '/': 'div'
    }

    @staticmethod
    @time_calls
    @meter_calls
    def execute(func_name: str,
                args: List[TimeSeriesGroup],
                fill_func: FillFunc,
                fill_value: float) -> TimeSeriesGroup:
        """
        Executes the function and returns the result
        :param func_name: name of the function, see the supported functions
        :param args: list of TimeSeriesResponse or just constants
        :param fill_func: Function to use to fill in None values
        :param fill_value: Fill value to use if fill_func is constant
        :return: (epochs, numbers) or just a number
        """
        is_transform = False
        if func_name.startswith('t.'):
            is_transform = True
            func_name = func_name.replace('t.', '')

        func = DenseFunctions.lookup_function(func_name)

        # The function execution is applied independently to each group
        # Lets identify all the non-default group names in args
        group_names: Set[str] = set()
        for ts_group in args:
            for group_name in ts_group.groups:
                if group_name != '_':
                    group_names.add(group_name)

        # check if we only have default
        if len(group_names) <= 0:
            group_names.add('_')

        # Process each group independent of other groups
        # call the function on each group args to create a group
        # specific time-series result
        result_time_series_group = TimeSeriesGroup()
        for group_name in group_names:
            group_args = DenseFunctions._get_group_args(args, group_name)
            # check if func_name is transformation or aggregation
            if is_transform:
                result_time_series_list = DenseFunctions._transformation(
                    func_name, func, group_args)
            else:
                result_time_series_list = DenseFunctions._aggregation(
                    func_name, func, group_name, group_args,
                    fill_func, fill_value)

            result_time_series_group[group_name] = result_time_series_list

        return result_time_series_group

    @staticmethod
    def _get_group_args(args: List[TimeSeriesGroup], group_name: str) -> \
            List[List[TimeSeries]]:
        """
        Get the arguments meant for the specified group name
        Each TimeSeriesGroup provided in args has group specific timeseries
        :param args: list of time series groups
        :param group_name:
        :return:
        """
        # get the group specific arguments
        group_args: List[List[TimeSeries]] = list()
        for ts_group in args:
            if group_name in ts_group.groups:
                group_args.append(ts_group.groups[group_name])
            else:
                if group_name != '_':
                    group_args.append(ts_group.groups['_'])
                else:
                    # No argument value for this group
                    group_args.append(None)
        return group_args

    @staticmethod
    def lookup_function(func_name: str):
        """
        Find the implementation of the "func_name"
        We look first for a func by this name in the DenseFunction module
        then we look in the numpy module
        In future we might add support for additional modules
        :param func_name:
        :return:
        """
        # check if this name is mapped
        if func_name in DenseFunctions.FUNC_MAP:
            func_name = DenseFunctions.FUNC_MAP[func_name]

        func = getattr(DenseFunctionsProvider, func_name, None)
        if func is None:
            # Check if its a numpy function
            func = getattr(np, func_name, None)
            if func is None:
                raise ValueError('Unsupported func: {}'.format(func_name))
        return func

    @staticmethod
    def _transformation(func_name: str,
                        func,
                        group_args: List[List[TimeSeries]]) ->\
            List[TimeSeries]:
        """
        :param func_name:
        :param func:
        :param group_args: list of args,
         where an arg is list of timeseries or a constant
        :return:
        """
        # Transformations are applied on individual series
        series_args, time_range = \
            DenseFunctions._prepare_transformation_args(group_args)

        result_time_series_list = list()
        for series_id, series_arg in series_args.items():
            transformed_values = func(*series_arg)
            if func_name in ['diff']:
                # Some functions such as diff generate one less value after
                # transformation than the number of epochs
                epochs = time_range.epochs()[1:]
                # We also need to adjust the time range to exclude the first
                # epoch
                result_time_range = TimeRange(
                    time_range.start + time_range.interval, time_range.end,
                    time_range.interval)
            else:
                epochs = time_range.epochs()
                result_time_range = time_range

            result_time_series_list.append(
                TimeSeries('{}({})'.format(func_name, series_id),
                           list(zip(epochs, transformed_values)),
                           result_time_range),
            )
        return result_time_series_list

    @staticmethod
    def _prepare_transformation_args(group_args: List[List[TimeSeries]]) -> \
            Tuple[List, TimeRange]:
        """
        Converts the list of timeseries into list of arguments that can
        be passed on to the methods
        :param group_args
        :return: List of arguments, where an argument can be a list of values
        or constant. Any string is parsed as potential time string
        """
        # first argument must be actual time series
        tsl1 = group_args[0]
        series_args = dict()
        time_range = None
        for time_series in tsl1:
            if time_series.series_id != '_constant_':
                series_args[time_series.series_id] = list()
                time_range = time_series.time_range

        if len(series_args) <= 0:
            return series_args, time_range

        for time_series_list in group_args:
            for time_series in time_series_list:
                if time_series.series_id == '_constant_':
                    # constant values cannot be per series
                    # return the single value
                    constant = time_series.points[0][1]
                    if isinstance(constant, str):
                        constant = SqlParser.time_str_parser(constant)
                    # add constant to all series args
                    for _, series_arg in series_args.items():
                        series_arg.append(constant)
                    break
                series_args[time_series.series_id].append(
                    time_series.values())

        return series_args, time_range

    @staticmethod
    def get_fill_value(prev_value: float,
                       fill_func: FillFunc, fill_value:
                       float) -> float:
        """
        Fill value to use for current epoch when current value reported is None
        :param prev_value:
        :param fill_func:
        :param fill_value:
        :return: fill value as a substitute for None
        """
        if fill_func == FillFunc.NULL:
            return None

        if fill_func == FillFunc.CONSTANT:
            return fill_value

        if fill_func == FillFunc.PREVIOUS:
            return prev_value

        if fill_func == FillFunc.LINEAR:
            raise ValueError('Linear interpolation not supported yet for fill')

        raise ValueError('Unsupported fill_func: ' + str(fill_func))

    # pylint: disable-msg=R0914 # too_many_locals
    @staticmethod
    def _aggregation(func_name: str,
                     func,
                     group_name: str,
                     group_args: List[List[TimeSeries]],
                     fill_func: FillFunc,
                     fill_value: float) ->\
            List[TimeSeries]:
        """
        Run the function on a single group
        :param func_name: name of the function
        :param func: function object returned by the lookup_function
        :param group_name: Name of the group (used for generating series_id)
        :param group_args: group specific arguments to be passed to the func
        :param fill_func: Function to use to fill in None values
        :param fill_value: Fill value to use if fill_func is constant
        :return: List of time-series as computed by executing the functio
        """
        if func_name == 'timeshift':
            # This is special because it operates on time-axis
            # unlike all other functions that operate on values only
            return DenseFunctionsProvider.timeshift(group_args)

        # We will now apply the function for each epoch
        # get all the epochs for each time series
        # infer the start, end and interval from the timeseries
        start_epoch = 0
        end_epoch = 0
        interval = 0
        for arg in group_args:
            if arg is not None and len(arg) > 0:
                # infer from first time series, its assumed all time-series
                # in a single argument are aligned
                time_series = arg[0]
                if time_series.series_id == '_constant_':
                    # ignore the constant values
                    continue
                ts_start = time_series.start_epoch()
                if ts_start <= 0:
                    # either empty series or a constant value
                    continue
                if start_epoch > 0:
                    start_epoch = min(start_epoch, ts_start)
                else:
                    start_epoch = ts_start
                end_epoch = max(end_epoch, time_series.end_epoch())
                ts_interval = time_series.interval()
                if interval > 0 and interval != ts_interval:
                    raise ValueError('Cannot aggregate two series'
                                     ' with different intervals: {} {}'.
                                     format(interval, ts_interval))
                interval = ts_interval

        # the series_id has to be unique,
        if start_epoch <= 0:
            # Special case, we are not provided with any time-series
            # just constants, lets make interval = 1 to ensure we
            # process the first constant values
            end_epoch = 1
            interval = 1
            # its not a series but a constant, and thats special
            series_name = '_constant_'
        else:
            series_name = DenseFunctions._generate_series_id(func_name,
                                                             group_name,
                                                             group_args)

        time_series = TimeSeries(series_name, list(), TimeRange(start_epoch,
                                                                end_epoch,
                                                                interval))
        for epoch in range(start_epoch, end_epoch, interval):
            # get the argument values for this epoch
            epoch_args = list()
            valid_args = True
            for arg in group_args:
                # extract all values in the arg for the epoch
                values = DenseFunctions._get_epoch_value(arg, epoch)

                if isinstance(values, list):
                    if len(values) <= 0:
                        valid_args = False
                else:
                    if values is None:
                        valid_args = False
                if not valid_args:
                    # we have an invalid arg, so we have to discard
                    # the entire epoch
                    break
                epoch_args.append(values)

            if not valid_args:
                # We got some args due to NULL values.
                # Since we can't run any aggregation functions over
                # invalid set of args. we just use the fill function to
                # populate values
                if fill_func == FillFunc.PREVIOUS:
                    raise ValueError("Fill previous not supported "
                                     "in aggregations")
                result_value = DenseFunctions.get_fill_value(None,
                                                             fill_func,
                                                             fill_value)
            else:
                # execute the actual function
                try:
                    result_value = func(*epoch_args)
                # pylint: disable-msg=W0703  # Broad Except
                except Exception as e:
                    if Telemetry.inst is not None:
                        Telemetry.inst.registry.meter(
                            measurement='ReadApi.DenseFunctions.failure_rate',
                            tag_key_values=[f"Function={func_name}"]).mark(1)
                    logger.error('Failed to apply func: %s on args: %s.'
                                 ' Exception: %s',
                                 func_name,
                                 ','.join([str(e) for e in epoch_args]),
                                 str(e))
                    result_value = None

            time_series.append((epoch, result_value))

        return [time_series]

    @staticmethod
    def _generate_series_id(func_name: str,
                            group_name: str,
                            args: List[List[TimeSeries]]) -> str:
        """
        Generate a series_id based on func_name applied to arguments
        :param func_name:
        :param group_name:
        :param args:
        :return: series id for aggregated/transformed series
        """
        # sort the series_ids and pick the first one
        arg_ids: List[str] = list()
        for tsl in args:
            if len(tsl) == 1:
                # if a single series is passed we use its series_id as is
                arg_ids.append(tsl[0].series_id)
            else:
                # in this case we use the group name instead
                arg_ids.append(group_name)
        return func_name + '(' + ','.join(arg_ids) + ')'

    @staticmethod
    def _get_epoch_value(arg: List[TimeSeries], epoch: int):
        """
        Get the values associated with the specific epoch from all time
        series provided
        :param arg: list of timeseries
        :param epoch: epoch for which all values must be returned
        :return: list of values, or a single value if only one time-series
        is represented
        """
        if arg is None or len(arg) <= 0:
            return None
        values = list()
        for time_series in arg:
            if time_series.series_id == '_constant_':
                constant = time_series.points[0][1]
                if isinstance(constant, str):
                    constant = SqlParser.time_str_parser(constant)
                # return the single value
                return constant
            value = time_series.get_epoch_value(epoch)
            if value is not None:
                values.append(value)
        if len(values) == 1:
            # if the values is a single value then unpack it
            # likely the function expecting this arg only operates
            # on a single value instead of a list
            return values[0]
        return values
