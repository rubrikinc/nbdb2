"""
DenseFunctions
"""
import logging
import re
from typing import List, Tuple, Union

import numpy as np

from nbdb.readapi.dense_functions import DenseFunctions
from nbdb.readapi.sql_parser import SqlParser, FillFunc
from nbdb.readapi.time_series_response import TimeSeries, TimeSeriesGroup, \
    TimeRange

logger = logging.getLogger()


class GraphiteFunctionsProvider:
    """
    Dense functions expect time-series data that has equal sized intervals
    The data is processed by each group and then for each group by
    each epoch.
    All arguments for each epoch are collected and the function is called
    The returned value of the function is associated with the corresponding
    epoch and group
    """
    # MetricTank default is Null
    fill_func = FillFunc.NULL
    fill_value = None

    @staticmethod
    def aliasSub(time_range: TimeRange,
                 tsl: List[TimeSeries], search, replace) -> List[TimeSeries]:
        """
        Runs series names through a regex search/replace.
        &target=aliasSub(ip.*TCP*,"^.*TCP(\\d+)","\1")
        :param time_range
        :param tsl:
        :param search:
        :param replace:
        :return:
        """
        _ = time_range
        pattern = re.compile(search)
        for ts in tsl:
            ts.series_id = pattern.sub(replace, ts.base_series_id)
            # Once alias is applied, base series name changes permanently
            ts.base_series_id = ts.series_id
        return tsl

    @staticmethod
    def alias(time_range: TimeRange,
              tsl: List[TimeSeries], new_name) -> List[TimeSeries]:
        """
        Takes one metric or a wildcard seriesList and a string in quotes.
        Prints the string instead of the metric name in the legend.
        &target=alias(Sales.widgets.largeBlue,"Large Blue Widgets")
        :param time_range:
        :param tsl:
        :param new_name:
        :return:
        """
        _ = time_range

        for ts in tsl:
            ts.series_id = new_name
            # Once alias is applied, base series name changes permanently
            ts.base_series_id = new_name
        return tsl

    @staticmethod
    def aliasByNode(time_range: TimeRange,
                    tsl: List[TimeSeries], *nodes) -> List[TimeSeries]:
        """
        Takes a seriesList and applies an alias derived from one or more
        “node” portion/s of the target name or tags. Node indices are 0
        indexed.
        :param time_range:
        :param tsl:
        :param nodes:
        :return:
        """
        _ = time_range

        sorted_nodes = sorted(nodes)
        for ts in tsl:
            # Use base_series_id instead of series_id because series_id gets
            # updated after applying function names
            ts.series_id = GraphiteFunctionsProvider._get_group_key(
                ts.base_series_id, sorted_nodes)
            # Once alias is applied, base series name changes permanently
            ts.base_series_id = ts.series_id
        return tsl

    @staticmethod
    def sortByName(time_range: TimeRange,
                   tsl: List[TimeSeries],
                   natural=False, reverse=False) -> List[TimeSeries]:
        """
        :param time_range
        :param tsl:
        :return:
        """
        _ = time_range
        if natural:
            raise ValueError('sortByName(): natural=True not supported')
        return sorted(tsl, key=lambda x: x.series_id, reverse=reverse)

    @staticmethod
    def sortByMaxima(time_range: TimeRange,
                     tsl: List[TimeSeries]) -> List[TimeSeries]:
        """
        Takes one metric or a wildcard seriesList.

        Sorts the list of metrics in descending order by the maximum value
        across the time period specified.

        :param time_range
        :param tsl:
        :return:
        """
        _ = time_range
        return sorted(tsl, key=lambda x: np.max(x.non_null_values() or -1),
                      reverse=True)

    @staticmethod
    def sortByMinima(time_range: TimeRange,
                     tsl: List[TimeSeries]) -> List[TimeSeries]:
        """
        Takes one metric or a wildcard seriesList.

        Sorts the list of metrics by the lowest value across the time period
        specified, including only series that have a maximum value greater than
        0.
        :param time_range
        :param tsl:
        :return:
        """
        _ = time_range
        # We can only include series that have a maximum value greater than 0
        tsl = [ts for ts in tsl
               if np.max(ts.non_null_values() or -1) > 0]
        return sorted(tsl, key=lambda x: np.min(x.non_null_values() or -1))

    @staticmethod
    def sortByTotal(time_range: TimeRange,
                    tsl: List[TimeSeries]) -> List[TimeSeries]:
        """
        :param time_range
        :param tsl:
        :return:
        """
        _ = time_range
        return sorted(tsl, key=lambda x: np.sum(x.non_null_values() or -1))

    @staticmethod
    def limit(time_range: TimeRange,
              tsl: List[TimeSeries], n: int) -> List[TimeSeries]:
        """
        :param time_range:
        :param tsl:
        :param n:
        :return:
        """
        _ = time_range
        return tsl[:n]

    @staticmethod
    def changed(time_range: TimeRange,
                tsl: List[TimeSeries]) -> List[TimeSeries]:
        """
        Takes one metric or a wildcard seriesList. Output 1 when the value
        changed, 0 when null or the same

        :param time_range:
        :param tsl:
        :return:
        """
        _ = time_range
        for ts in tsl:
            prev_value = None
            target_points: List[Tuple[int, float]] = list()
            for epoch, value in ts.points:
                target_value = 0
                if (prev_value is not None and value is not None and
                        value != prev_value):
                    # If either value is NULL, we are supposed to return 0
                    target_value = 1
                target_points.append((epoch, target_value))
                prev_value = value
            ts.points = target_points
            ts.series_id = f"changed({ts.series_id})"
        return tsl

    @staticmethod
    def delay(time_range: TimeRange,
              tsl: List[TimeSeries], steps: int) -> List[TimeSeries]:
        """
        This shifts all samples later by an integer number of steps. This can
        be used for custom derivative calculations, among other things. Note:
        this will pad the early end of the data with None for every step
        shifted.

        This complements other time-displacement functions such as timeShift
        and timeSlice, in that this function is indifferent about the step
        intervals being shifted.

        :param time_range:
        :param tsl:
        :param steps:
        :return:
        """
        _ = time_range
        if steps < 0:
            raise ValueError("delay() only supports positive integer values "
                             "for steps. Saw: %d" % steps)
        if steps == 0:
            # Nothing to do
            return tsl

        for ts in tsl:
            # Can't shift by more than the total number of datapoints
            n = min(len(ts.points), steps)
            epochs, values = zip(*ts.points)
            # Shift values to the right by required steps
            values = [None] * n + list(values[:-n])
            ts.points = list(zip(epochs, values))
            ts.series_id = f"delay({ts.series_id}, {steps})"
        return tsl

    @staticmethod
    def _convert_to_series_lists(obj) -> List[List[TimeSeries]]:
        """
        Converts List[TimeSeries] to List[List[TimeSeries]]
        """
        if (isinstance(obj, (tuple, list)) and len(obj) > 0 and
                isinstance(obj[0], list)):
            # Nothing to be done. Object has the right type already
            return obj
        if isinstance(obj, (tuple, list)):
            # This is an object of type List[TimeSeries]. This usually happens
            # when we run a function (say scale()) on a single series.
            # Create a list with a single element obj
            return [obj]
        # We shouldn't reach here
        raise ValueError('Unsupported type for %s' % str(obj))

    @staticmethod
    def maxSeries(time_range: TimeRange,
                  *tsls: List[List[TimeSeries]]) -> List[TimeSeries]:
        """
        :param time_range:
        :param tsls: list of time series lists
        :return:
        """
        _ = time_range
        tsl_combined = []
        # Sometimes we need to convert to List of TSL because *scale() turns
        # TSL to multiple TS
        for tsl in GraphiteFunctionsProvider._convert_to_series_lists(tsls):
            tsl_combined.extend(tsl)
        if len(tsl_combined) <= 0:
            return []
        tsgr = DenseFunctions.execute('max',
                                      [TimeSeriesGroup({'_': tsl_combined})],
                                      GraphiteFunctionsProvider.fill_func,
                                      GraphiteFunctionsProvider.fill_value)
        return GraphiteFunctionsProvider._tsg_to_list(tsgr, False)

    @staticmethod
    def minSeries(time_range: TimeRange,
                  *tsls: List[List[TimeSeries]]) -> List[TimeSeries]:
        """
        :param time_range:
        :param tsls: list of time series lists
        :return:
        """
        _ = time_range
        tsl_combined = []
        for tsl in GraphiteFunctionsProvider._convert_to_series_lists(tsls):
            tsl_combined.extend(tsl)
        if len(tsl_combined) <= 0:
            return []
        tsgr = DenseFunctions.execute('min',
                                      [TimeSeriesGroup({'_': tsl_combined})],
                                      GraphiteFunctionsProvider.fill_func,
                                      GraphiteFunctionsProvider.fill_value)
        return GraphiteFunctionsProvider._tsg_to_list(tsgr, False)

    @staticmethod
    def sumSeries(time_range: TimeRange,
                  *tsls: List[List[TimeSeries]]) -> List[TimeSeries]:
        """
        :param time_range:
        :param tsls: list of time series lists
        :return:
        """
        _ = time_range
        tsl_combined = []
        for tsl in GraphiteFunctionsProvider._convert_to_series_lists(tsls):
            tsl_combined.extend(tsl)
        if len(tsl_combined) <= 0:
            return []

        tsgr = DenseFunctions.execute('sum',
                                      [TimeSeriesGroup({'_': tsl_combined})],
                                      GraphiteFunctionsProvider.fill_func,
                                      GraphiteFunctionsProvider.fill_value)
        return GraphiteFunctionsProvider._tsg_to_list(tsgr, False)

    @staticmethod
    def averageSeries(time_range: TimeRange,
                      *tsls: List[List[TimeSeries]]) -> List[TimeSeries]:
        """
        :param time_range:
        :param tsls: list of time series lists
        :return:
        """
        _ = time_range
        tsl_combined = []
        for tsl in GraphiteFunctionsProvider._convert_to_series_lists(tsls):
            tsl_combined.extend(tsl)
        if len(tsl_combined) <= 0:
            return []

        tsgr = DenseFunctions.execute('average',
                                      [TimeSeriesGroup({'_': tsl_combined})],
                                      GraphiteFunctionsProvider.fill_func,
                                      GraphiteFunctionsProvider.fill_value)
        return GraphiteFunctionsProvider._tsg_to_list(tsgr, False)

    @staticmethod
    def diffSeries(time_range: TimeRange,
                   *tsls: List[List[TimeSeries]]) -> List[TimeSeries]:
        """
        Subtracts series 2 through n from series 1.
        :param time_range:
        :param tsls:
        :return:
        """
        _ = time_range
        tsl_diff = []
        if len(tsls) <= 0 or len(tsls[0]) <= 0:
            return []
        tsl_0 = tsls[0][0]
        if len(tsls[0]) > 1:
            tsl_diff.extend(tsls[0][1:])
        for tsl in tsls[1:]:
            tsl_diff.extend(tsl)
        tsgr_diff = DenseFunctions.execute(
            'sum',
            [TimeSeriesGroup({'_': tsl_diff})],
            GraphiteFunctionsProvider.fill_func,
            GraphiteFunctionsProvider.fill_value)
        tsl_diff_series = GraphiteFunctionsProvider._tsg_to_list(tsgr_diff,
                                                                 False)
        tsgr = DenseFunctions.execute(
            'sub', [TimeSeriesGroup({'_': [tsl_0]}),
                    TimeSeriesGroup({'_': [tsl_diff_series[0]]})],
            GraphiteFunctionsProvider.fill_func,
            GraphiteFunctionsProvider.fill_value)
        return GraphiteFunctionsProvider._tsg_to_list(tsgr, False)

    @staticmethod
    def divideSeries(time_range: TimeRange,
                     dividend_series_list: List[TimeSeries],
                     divisor_series) ->\
            List[TimeSeries]:
        """
        Takes a dividend metric and a divisor metric and draws the division
        result. A constant may not be passed. To divide by a constant,
         use the scale() function (which is essentially a multiplication
         operation) and use the inverse of the dividend. (Division by 8 =
         multiplication by 1/8 or 0.125)
         :param time_range
         :param dividend_series_list
         :param divisor_series
        :return:
        """
        _ = time_range
        if isinstance(divisor_series, list):
            # We may get a list of timeseries for divisor series especially if
            # some other function was applied such as scale(). Make sure that
            # the list only has a single series.
            if len(divisor_series) == 0:
                # Empty divisor series. Return NULLs for each dividend series
                null_points = [(epoch, None) for epoch in
                               range(time_range.start, time_range.end,
                                     time_range.interval)]
                for ts in dividend_series_list:
                    ts.points = null_points
                return dividend_series_list
            if len(divisor_series) != 1:
                raise ValueError('divideSeries(): divisor series should be a '
                                 'single series. We saw %d series' %
                                 len(divisor_series))
            divisor_series = divisor_series[0]

        result = []
        for dividend_series in dividend_series_list:
            tgr = DenseFunctions.execute(
                'div', [TimeSeriesGroup({'_': [dividend_series]}),
                        TimeSeriesGroup({'_': [divisor_series]})],
                GraphiteFunctionsProvider.fill_func,
                GraphiteFunctionsProvider.fill_value)
            result.extend(GraphiteFunctionsProvider._tsg_to_list(tgr, False))
        return result

    @staticmethod
    def divideSeriesLists(time_range, dividend_series_list: List[TimeSeries],
                          divisor_series_list: List[TimeSeries]) ->\
            List[TimeSeries]:
        """
        Iterates over a two lists and divides list1[0] by list2[0], list1[1] by
        list2[1] and so on. The lists need to be the same length
        """
        _ = time_range
        if len(dividend_series_list) != len(divisor_series_list):
            raise ValueError(
                'divideSeriesLists(): dividend series and divisor series '
                'must have same length. Saw dividend_series_len=%d and '
                'divisor_series_len=%d' % (len(dividend_series_list),
                                           len(divisor_series_list)))
        result = []
        for idx, dividend_series in enumerate(dividend_series_list):
            divisor_series = divisor_series_list[idx]
            tgr = DenseFunctions.execute(
                'div', [TimeSeriesGroup({'_': [dividend_series]}),
                        TimeSeriesGroup({'_': [divisor_series]})],
                GraphiteFunctionsProvider.fill_func,
                GraphiteFunctionsProvider.fill_value)

            ts = GraphiteFunctionsProvider._tsg_to_list(tgr, False)[0]
            ts.series_id = (f'divideSeries({dividend_series.series_id},'
                            f'{divisor_series.series_id})')
            # We use the dividend series as the base series ID. If
            # aliasByNode() is applied, Graphite defaults to the first series
            # path found in the query. So picking the dividend series is safe
            ts.base_series_id = dividend_series.base_series_id
            result += [ts]
        return result

    @staticmethod
    def asPercent(time_range: TimeRange,
                  tsl: List[TimeSeries],
                  total: Union[List[TimeSeries], float] = None) -> \
            List[TimeSeries]:
        """
        Calculates a percentage of the total of a wildcard series.
        If total is specified, each series will be calculated as a percentage
        of that total. If total is not specified, the sum of all points in
        the wildcard series will be used instead.
        :param time_range:
        :param tsl:
        :param total:
        :return:
        """
        _ = time_range

        if total is not None and not isinstance(total, list):
            _total_values: List[float] = [total]*time_range.points()
            _total_series_id: str = str(total)
        else:
            _total: List[TimeSeries] = total
            if _total is None:
                # if total is None, then total is sum of tsl
                _total = tsl
            total_tgr = DenseFunctions.execute(
                'sum',
                [TimeSeriesGroup({'_': _total})],
                GraphiteFunctionsProvider.fill_func,
                GraphiteFunctionsProvider.fill_value)
            _total = GraphiteFunctionsProvider._tsg_to_list(total_tgr, False)
            _total_values: List[float] = _total[0].values()
            _total_series_id = None

        tsl_result: List[TimeSeries] = list()
        for ts in tsl:
            points: List[Tuple[int, float]] = list()
            for p, t in zip(ts.points, _total_values):
                if p[1] is None or t is None:
                    points.append((p[0], None))
                else:
                    points.append((p[0], p[1]*100/t))

            if _total_series_id:
                result_series_id = 'asPercent({},{})'.format(ts.series_id,
                                                             _total_series_id)
            else:
                result_series_id = 'asPercent({})'.format(ts.series_id)
            tsl_result.append(TimeSeries(result_series_id, points, time_range,
                                         ts.base_series_id))
        return tsl_result

    @staticmethod
    def nonNegativeDerivative(time_range: TimeRange,
                              tsl: List[TimeSeries]) -> List[TimeSeries]:
        """
        :param time_range:
        :param tsl:
        :return:
        """
        # NOTE: The documentation for nonNegativeDerivative() states that it
        # simply computes running deltas between values, and doesn't normalize
        # for the period of time as a true derivative function would.
        _ = time_range
        tsl_target: List[TimeSeries] = list()
        for ts in tsl:
            prev_value = None
            target_points: List[Tuple[int, float]] = list()
            for epoch, value in ts.points:
                diff = None
                if prev_value is not None and value is not None:
                    diff = value - prev_value
                    if diff < 0:
                        diff = None
                target_points.append((epoch, diff))
                prev_value = value
            new_series_id = f"nonNegativeDerivative({ts.series_id})"
            tsl_target.append(TimeSeries(new_series_id,
                                         target_points,
                                         ts.time_range,
                                         ts.base_series_id))
        return tsl_target

    @staticmethod
    def averageSeriesWithWildcards(time_range: TimeRange,
                                   tsl: List[TimeSeries],
                                   *nodes,) -> \
            List[TimeSeries]:
        """
        groups series by unique names
        :param time_range:
        :param tsl: For graphite the group by is not done by the druidreader
        we expect to see a single group only
        :param nodes:
        """
        # the number of nodes must be same across all series
        return GraphiteFunctionsProvider.aggregateWithWildcards(time_range,
                                                                tsl,
                                                                "average",
                                                                *nodes)

    @staticmethod
    def sumSeriesWithWildcards(time_range: TimeRange,
                               tsl: List[TimeSeries],
                               *nodes)  -> \
            List[TimeSeries]:
        """
        :param time_range:
        :param tsl:
        :param nodes:
        :return:
        """
        # the number of nodes must be same across all series
        return GraphiteFunctionsProvider.aggregateWithWildcards(time_range,
                                                                tsl,
                                                                "sum",
                                                                *nodes)

    @staticmethod
    def countSeries(time_range: TimeRange,
                    tsl: List[TimeSeries]) -> List[TimeSeries]:
        """
        TODO: can be optimized, because actual datapoints carry no value
        :param time_range:
        :param tsl:
        :return:
        """
        # the number of nodes must be same across all series
        count = len(tsl)
        if count <= 0:
            return []
        points = list(zip(time_range.epochs(),
                          [count]*time_range.points()))
        return [TimeSeries('count', points, time_range)]

    @staticmethod
    def countNewSeries(time_range: TimeRange,
                       tsl: List[TimeSeries]) -> List[TimeSeries]:
        """
        Tracks the number of new series that were added across time.

        For example, if series A lasted from [10, 30] and series B lasted from
        [30, 40], the resulting output will be:
        [0, 0], [10, 1], [20, 0], [30, 1], [40, 0]

        :param time_range:
        :param tsl:
        :return:
        """
        counts = {}
        for ts in tsl:
            for epoch, value in ts.points:
                if value is not None:
                    # Found the first non-NULL point. Update counts and break
                    if epoch in counts:
                        counts[epoch] += 1
                    else:
                        counts[epoch] = 1
                    break

        points = [(epoch, counts.get(epoch, 0))
                  for epoch in time_range.epochs()]
        return [TimeSeries('countNewSeries', points, time_range)]

    @staticmethod
    def constantLine(time_range: TimeRange, value) -> List[TimeSeries]:
        """
        :param time_range:
        :param value:
        :return:
        """
        return [TimeSeries('_constant_',
                           [(e, value) for e in time_range.epochs()],
                           time_range)]

    @staticmethod
    def removeEmptySeries(time_range: TimeRange,
                      tsl: List[TimeSeries], xFilesFactor: float=None) \
                              -> List[TimeSeries]:
        """
        : Takes one metric or a wildcard seriesList. Out of all metrics
        : passed, draws only the metrics with not empty data
        : xFilesFactor follows the same semantics as in Whisper storage
        : schemas. Setting it to 0 (the default) means that only a single
        : value in the series needs to be non-null for it to be considered
        : non-empty, setting it to 1 means that all values in the series
        : must be non-null. A setting of 0.5 means that at least half
        : the values in the series must be non-null.
        :param time_range:
        :param tsl:
        :param xFilesFactor
        :return:
        """
        _ = time_range

        if xFilesFactor is None:
            xFilesFactor = 0.0

        ntsl = []
        for ts in tsl:
            threshold = xFilesFactor * len(ts.points)
            if threshold == 0:
                # For the default xFilesFactor value of 0, we need atleast one
                # non-NULL datapoint for the series to not be removed
                threshold = 1
            count = 0
            for p in ts.points:
                if p[1] is not None:
                    count += 1
                    if count >= threshold:
                        break
            if count >= threshold:
                ntsl.append(ts)
        return ntsl

    @staticmethod
    def transformNull(time_range: TimeRange,
                      tsl: List[TimeSeries],
                      default=0,
                      referenceSeries=None) ->\
            List[TimeSeries]:
        """

        :param time_range:
        :param tsl:
        :param default:
        :param referenceSeries:
        :return:
        """
        _ = time_range
        if referenceSeries is not None:
            raise ValueError('referenceSeries is not supported yet')
        for ts in tsl:
            ts.points = [(p[0], p[1] if p[1] is not None else default)
                         for p in ts.points]
            if default == 0:
                ts.series_id = f'transformNull({ts.series_id})'
            else:
                ts.series_id = f'transformNull({ts.series_id},{default})'
        return tsl

    @staticmethod
    def highest(time_range: TimeRange, tsl: List[TimeSeries],
                n: int = 1, func: str = 'average') -> List[TimeSeries]:
        """
        Takes one metric or a wildcard seriesList followed by an integer N and
        an aggregation function. Out of all metrics passed, draws only the N
        metrics with the highest aggregated value over the time period
        specified.

        Recognized aggregation functions include "average", "max", "current".

        :param time_range:
        :param tsl:
        :param n:
        :param func:
        :return:
        """
        _ = time_range
        if func == 'average':
            # Use a non-null average as the sorting function
            def non_null_avg(x):
                non_null_values = [v for _, v in x.points if v is not None]
                if len(non_null_values) == 0:
                    # Not sure what to return
                    return -1
                return float(sum(non_null_values)) / len(non_null_values)
            sort_fn = non_null_avg

        elif func == 'max':
            def non_null_max(x):
                non_null_values = [v for _, v in x.points if v is not None]
                if len(non_null_values) == 0:
                    # Not sure what to return
                    return -1
                return np.max(non_null_values)
            sort_fn = non_null_max

        elif func == 'current':
            sort_fn = (lambda x: -1 if not x.points or x.points[-1][1] is None
                       else x.points[-1][1])
        else:
            raise ValueError('highest: Unsupported func: {}'.format(func))

        sorted_tsl = sorted(tsl, key=sort_fn, reverse=True)
        return sorted_tsl[:n]

    @staticmethod
    def highestAverage(time_range: TimeRange, tsl: List[TimeSeries], n) ->\
            List[TimeSeries]:
        """
        Takes one metric or a wildcard seriesList followed by an integer N.
        Out of all metrics passed, draws only the N metrics with the highest
        average value for the time period specified.
        :param time_range:
        :param tsl:
        :param n:
        :return:
        """
        return GraphiteFunctionsProvider.highest(time_range, tsl, n,
                                                 func='average')

    @staticmethod
    def highestCurrent(time_range: TimeRange, tsl: List[TimeSeries], n) ->\
            List[TimeSeries]:
        """
        Takes one metric or a wildcard seriesList followed by an integer N.
        Out of all metrics passed, draws only the N metrics with the highest
        value at the end of the time period specified.
        :param time_range:
        :param tsl:
        :param n:
        :return:
        """
        return GraphiteFunctionsProvider.highest(time_range, tsl, n,
                                                 func='current')

    @staticmethod
    def highestMax(time_range: TimeRange, tsl: List[TimeSeries], n) ->\
            List[TimeSeries]:
        """
        Takes one metric or a wildcard seriesList followed by an integer N.
        Out of all metrics passed, draws only the N metrics with the highest
        maximum value in the time period specified.
        :param time_range:
        :param tsl:
        :param n:
        :return:
        """
        return GraphiteFunctionsProvider.highest(time_range, tsl, n,
                                                 func='max')


    @staticmethod
    def currentAbove(time_range: TimeRange, tsl: List[TimeSeries], n) ->\
            List[TimeSeries]:
        """
        Takes one metric or a wildcard seriesList followed by a
        constant N. Out of all metrics passed, draws only the
        metrics whose value is above N at the end of the time
        period specified.
        :param time_range:
        :param tsl:
        :param n:
        :return:
        """
        _ = time_range
        return list(filter(lambda ts: ts.points and
                           ts.points[-1][1] is not None and
                           ts.points[-1][1] >= n,
                           tsl))

    @staticmethod
    def averageAbove(time_range: TimeRange, tsl: List[TimeSeries], n) ->\
            List[TimeSeries]:
        """
        Takes one metric or a wildcard seriesList followed by a constant N.
        Out of all metrics passed, draws only the metrics with an average
        value above N for the time period specified.
        period specified.
        :param time_range:
        :param tsl:
        :param n:
        :return:
        """
        _ = time_range
        result: List[TimeSeries] = list()
        for ts in tsl:
            non_null_values = ts.non_null_values()
            if len(non_null_values) > 0 and np.average(non_null_values) >= n:
                result.append(ts)
        return result

    @staticmethod
    def maximumAbove(time_range: TimeRange, tsl: List[TimeSeries], n) ->\
            List[TimeSeries]:
        """
        Takes one metric or a wildcard seriesList followed by a constant n.
        Draws only the metrics with a maximum value above n.
        :param time_range:
        :param tsl:
        :param n:
        :return:
        """
        _ = time_range
        result: List[TimeSeries] = list()
        for ts in tsl:
            non_null_values = ts.non_null_values()
            if len(non_null_values) > 0 and np.max(non_null_values) >= n:
                result.append(ts)
        return result

    @staticmethod
    def exclude(time_range: TimeRange, tsl: List[TimeSeries], pattern) ->\
            List[TimeSeries]:
        """
        Takes one metric or a wildcard seriesList followed by a constant n.
        Draws only the metrics with a maximum value above n.
        :param time_range:
        :param tsl:
        :param pattern:
        :return:
        """
        _ = time_range
        return list(filter(lambda ts: pattern not in ts.series_id, tsl))

    @staticmethod
    def nPercentile(time_range: TimeRange, tsl: List[TimeSeries],
                    n: float) -> List[TimeSeries]:
        """
        Returns n-percent of each series in the seriesList.
        :param time_range:
        :param tsl:
        :param n:
        :return:
        """
        if n < 0 or n > 100:
            raise ValueError(
                f"Provided n={n} not in range [0, 100] in nPercentile()")
        _ = time_range
        for ts in tsl:
            ts.series_id = f"nPercentile({ts.series_id},{n})"
            non_null_values = [v for e, v in ts.points if v is not None]
            if not non_null_values:
                # All values are NULL. Nothing for us to do
                continue

            # Some non-NULL values were found. Compute percentile and update
            # datapoints
            perc = np.percentile(non_null_values, n)
            # Remove above percentile
            ts.points = [(e, perc) for e, _ in ts.points]
        return tsl

    @staticmethod
    def removeBelowPercentile(time_range: TimeRange, tsl: List[TimeSeries],
                              n: int) -> List[TimeSeries]:
        """
        Removes data below the nth percentile from the series or list of series
        provided. Values below this percentile are assigned a value of None.

        :param time_range:
        :param tsl:
        :param n:
        :return:
        """
        return GraphiteFunctionsProvider.removePercentileHelper(
            time_range, tsl, n, remove_above=False)

    @staticmethod
    def removeAbovePercentile(time_range: TimeRange, tsl: List[TimeSeries],
                              n: int) -> List[TimeSeries]:
        """
        Removes data above the nth percentile from the series or list of series
        provided. Values above this percentile are assigned a value of None.

        :param time_range:
        :param tsl:
        :param n:
        :return:
        """
        return GraphiteFunctionsProvider.removePercentileHelper(
            time_range, tsl, n, remove_above=True)

    @staticmethod
    def removePercentileHelper(time_range: TimeRange, tsl: List[TimeSeries],
                               n: int, remove_above: bool) -> List[TimeSeries]:
        """
        Helper to remove data above or below the given percentile
        :param time_range:
        :param tsl:
        :param n:
        :param remove_above: If true, removes values above the computed
        percentile. Otherwise, removes values below.
        :return:
        """
        _ = time_range
        for ts in tsl:
            non_null_values = [v for e, v in ts.points if v is not None]
            if not non_null_values:
                # All values are NULL. Nothing for us to do
                continue

            # Some non-NULL values were found. Compute percentile and remove
            # (ie. NULLify) any values above or below it.
            perc = np.percentile(non_null_values, n)
            if remove_above:
                # Remove above percentile
                ts.points = [
                    (e, v) if v is not None and v <= perc else (e, None)
                    for e, v in ts.points]
            else:
                # Remove below percentile
                ts.points = [
                    (e, v) if v is not None and v >= perc else (e, None)
                    for e, v in ts.points]
        return tsl

    @staticmethod
    def removeBelowValue(time_range: TimeRange, tsl: List[TimeSeries], n) ->\
            List[TimeSeries]:
        """
        Removes data below the given threshold from the series or list of
        series provided. Values below this threshold are assigned a
        value of None.
        :param time_range:
        :param tsl:
        :param n:
        :return:
        """
        _ = time_range
        for ts in tsl:
            ts.points = [(e, v) if v is not None and v >= n else (e, None)
                         for e, v in ts.points]
        return tsl

    @staticmethod
    def removeAboveValue(time_range: TimeRange, tsl: List[TimeSeries], n) ->\
            List[TimeSeries]:
        """
        Removes data above the given threshold from the series or list of
        series provided. Values above this threshold are assigned a value of
        None.
        :param time_range:
        :param tsl:
        :param n:
        :return:
        """
        _ = time_range
        for ts in tsl:
            ts.points = [(e, v) if v is not None and v <= n else (e, None)
                         for e, v in ts.points]
        return tsl

    @staticmethod
    def keepLastValue(time_range: TimeRange, tsl: List[TimeSeries], limit=0)\
            -> List[TimeSeries]:
        """
        Takes one metric or a wildcard seriesList, and optionally a limit to
        the number of ‘None’ values to skip over. Continues the line with the
        last received value when gaps (‘None’ values) appear in your data,
        rather than breaking your line.
        :param time_range:
        :param tsl:
        :param limit: maximum consecutive nones to repeat last value over.
                      0 means no limit is applied, i.e. last value is repeated
                      for all consecutive nones
        :return:
        """
        _ = time_range
        for ts in tsl:
            consecutive_nones = 0
            last_value = None
            new_points: List[Tuple[int, float]] = list()
            for epoch, value in ts.points:
                if value is None:
                    consecutive_nones = consecutive_nones + 1
                    if 0 < limit < consecutive_nones:
                        continue
                    value = last_value
                else:
                    consecutive_nones = 0
                new_points.append((epoch, value))
                last_value = value
            ts.points = new_points
            if limit == 0:
                ts.series_id = f'keepLastValue({ts.series_id})'
            else:
                ts.series_id = f'keepLastValue({ts.series_id},{limit})'
        return tsl

    # pylint: disable-msg=R0914  # Too Many Locals
    @staticmethod
    def movingHelper(time_range: TimeRange,
                     tsl: List[TimeSeries],
                     intervalString: str,
                     func: str,
                     caller_name: str) -> List[TimeSeries]:
        """
        Helper for applying func over a moving window
        :param time_range:
        :param tsl:
        :param intervalString:
        :param func:
        :return:
        """
        # This is essentially group by time done by DruidReader
        # being performed again
        interval = SqlParser.time_str_parser(intervalString)
        time_range_out = TimeRange(time_range.start, time_range.end, interval)

        # Check if its a numpy function
        func_impl = getattr(np, func, None)
        if func_impl is None:
            raise ValueError('summarize: Unsupported func: {}'.format(func))

        tslr: List[TimeSeries] = list()
        for ts in tsl:
            interval_points = max(int(time_range_out.interval /
                                      ts.time_range.interval), 1)
            new_points: List[Tuple[int, float]] = list()
            for i in range(0, ts.num_points()):
                if i < interval_points - 1:
                    window_start_idx = 0
                else:
                    window_start_idx = i - interval_points + 1
                values = [p[1] for p in ts.points[window_start_idx:i+1]
                          if p[1] is not None]
                if values:
                    agg_value = float(func_impl(values))
                else:
                    agg_value = None
                new_points.append((ts.points[i][0], agg_value))
            new_series_id = (
                f'{caller_name}({ts.series_id},"{intervalString}")')
            tslr.append(TimeSeries(new_series_id, new_points, time_range_out))
        return tslr

    @staticmethod
    def movingAverage(time_range: TimeRange,
                      tsl: List[TimeSeries],
                      windowSize: str) -> List[TimeSeries]:
        """
        Graphs the moving average of a metric (or metrics) over a fixed number
        of past points, or a time interval.

        Takes one metric or a wildcard seriesList followed by a number N of
        datapoints or a quoted string with a length of time like '1hour' or
        '5min'

        Graphs the average of the preceding datapoints for each point on the
        graph.
        """
        return GraphiteFunctionsProvider.movingHelper(
            time_range, tsl, windowSize, func='average',
            caller_name='movingAverage')

    @staticmethod
    def summarize(time_range: TimeRange,
                  tsl: List[TimeSeries],
                  intervalString: str,
                  func='sum',
                  alignToFrom=False) -> List[TimeSeries]:
        """
        Summarize the data into interval buckets of a certain size.
        By default, the contents of each interval bucket are summed together.
        This is useful for counters where each increment represents a discrete
        event and retrieving a “per X” value requires summing all the events in
        that interval.
        Specifying ‘average’ instead will return the mean for each bucket,
        which can be more useful when the value is a gauge that represents a
        certain value in time.

        This function can be used with aggregation functions average, median,
        sum, min, max, diff, stddev, count, range, multiply & last.

        By default, buckets are calculated by rounding to the nearest interval.

        This works well for intervals smaller than a day. For example, 22:32
        will end up in the bucket 22:00-23:00 when the interval=1hour.

        Passing alignToFrom=true will instead create buckets starting at the
        from time. In this case, the bucket for 22:32 depends on the from time.
        If from=6:30 then the 1hour bucket for 22:32 is 22:30-23:30.
        :param time_range:
        :param tsl:
        :param intervalString:
        :param func:
        :param alignToFrom:
        :return:
        """
        # This is essentially group by time done by DruidReader
        # being performed again
        interval_req = SqlParser.time_str_parser(intervalString)

        # User can specify any interval but we can't go lower than the interval
        # specified for the raw series
        selected_interval = max(interval_req, time_range.interval)

        # Select the time range based on the selected interval and alignToFrom
        start = time_range.start if alignToFrom else \
            int(time_range.start/selected_interval) * selected_interval
        time_range_out = TimeRange(start, time_range.end, selected_interval)

        # Sometimes Graphite has multiple names for the same function
        translated_func = {
            'avg': 'average'
        }
        func = translated_func.get(func, func)

        # Check if its a numpy function
        func_impl = getattr(np, func, None)
        if func_impl is None:
            raise ValueError('summarize: Unsupported func: {}'.format(func))

        tslr: List[TimeSeries] = list()
        for ts in tsl:
            # Values in a bucket are aggregated. We use the bucket start
            # timestamp as the epoch when reporting the aggregated value. For
            # example, for the bucket [22:00, 23:00), we will aggregate all
            # values with epoch in that range and generate a final aggregated
            # value with epoch = 22:00
            #
            # Also note that the bucket interval [22:00, 23:00) is a half open
            # interval and does not include the datapoint with epoch = 23:00
            bucket_start_epoch = start
            new_points: List[Tuple[int, float]] = list()
            idx = 0
            num_points = ts.num_points()
            while idx < num_points and bucket_start_epoch <= time_range.end:
                values: List[float] = list()
                # Keep accumulating values for the current bucket. A datapoint
                # lies in the current bucket if it has an epoch less than the
                # bucket end epoch
                while (idx < num_points and
                       ts.points[idx][0] < (bucket_start_epoch +
                                            selected_interval)):
                    # We are only interested in non-NULL values
                    if ts.points[idx][1] is not None:
                        values += [ts.points[idx][1]]
                    idx += 1

                if len(values) > 0:
                    agg_value = float(func_impl(values))
                else:
                    # No non-null values were seen for this bucket
                    agg_value = None
                new_points.append((bucket_start_epoch, agg_value))
                bucket_start_epoch += selected_interval
            new_series_id = (
                f'summarize({ts.series_id}, "{intervalString}", "{func}")')
            tslr.append(TimeSeries(new_series_id, new_points, time_range_out,
                                   ts.base_series_id))
        return tslr

    @staticmethod
    def integral(time_range: TimeRange, tsl: List[TimeSeries]) ->\
            List[TimeSeries]:
        """
        This will show the sum over time, sort of like a continuous addition
        function. Useful for finding totals or trends in metrics that are
        collected per minute.
        :return:
        """
        _ = time_range
        for ts in tsl:
            last_val = 0
            points = list()
            for point in ts.points:
                if point[1] is not None:
                    last_val = point[1] + last_val
                points.append((point[0], last_val))
            ts.points = points
            ts.series_id = f"integral({ts.series_id})"
        return tsl

    @staticmethod
    def isNonNull(time_range: TimeRange, tsl: List[TimeSeries]) -> \
            List[TimeSeries]:
        """

        :param time_range:
        :param tsl:
        :return:
        """
        _ = time_range
        for ts in tsl:
            ts.points = [(p[0], 0 if p[1] is None else 1) for p in ts.points]
            ts.series_id = f"isNonNull({ts.series_id})"
        return tsl

    @staticmethod
    def scaleHelper(time_range: TimeRange, tsl: List[TimeSeries],
                    factor, func_name: str,
                    factor_value: str) -> List[TimeSeries]:
        """
        Helper for scale() and scaleToSeconds()
        :param time_range
        :param tsl:
        :param factor
        :return:
        """
        _ = time_range
        for ts in tsl:
            points = list()
            for point in ts.points:
                if point[1] is None:
                    points.append((point[0], None))
                else:
                    points.append((point[0], point[1]*factor))
            ts.points = points
            ts.series_id = f"{func_name}({ts.series_id},{factor_value})"
        return tsl

    @staticmethod
    def scale(time_range: TimeRange, tsl: List[TimeSeries], factor) ->\
            List[TimeSeries]:
        """
        This will show the sum over time, sort of like a continuous addition
        function. Useful for finding totals or trends in metrics that are
        collected per minute.
        :param time_range
        :param tsl:
        :param factor
        :return:
        """
        return GraphiteFunctionsProvider.scaleHelper(time_range, tsl, factor,
                                                     'scale', str(factor))

    @staticmethod
    def offset(time_range: TimeRange, tsl: List[TimeSeries], factor) -> \
            List[TimeSeries]:
        """
        This will show the sum over time, sort of like a continuous addition
        function. Useful for finding totals or trends in metrics that are
        collected per minute.
        :param time_range
        :param tsl:
        :param factor
        :return:
        """
        _ = time_range
        for ts in tsl:
            points = list()
            for point in ts.points:
                if point[1] is None:
                    points.append((point[0], None))
                else:
                    points.append((point[0], point[1] + factor))
            ts.points = points
            ts.series_id = f"offset({ts.series_id},{factor})"
        return tsl

    @staticmethod
    def absolute(time_range: TimeRange, tsl: List[TimeSeries]) -> \
            List[TimeSeries]:
        """
        Takes one metric or a wildcard seriesList and applies the mathematical
        abs function to each datapoint transforming it to its absolute value.
        :param time_range
        :param tsl:
        :return:
        """
        _ = time_range
        for ts in tsl:
            points = list()
            for point in ts.points:
                if point[1] is None:
                    points.append((point[0], None))
                else:
                    points.append((point[0], abs(point[1])))
            ts.points = points
            ts.series_id = f"absolute({ts.series_id})"
        return tsl

    @staticmethod
    def scaleToSeconds(time_range: TimeRange, tsl: List[TimeSeries], seconds) ->\
            List[TimeSeries]:
        """
        Takes one metric or a wildcard seriesList and returns
        “value per seconds” where seconds is a last argument to this functions.
        Useful in conjunction with derivative or integral function if you want
        to normalize its result to a known resolution for arbitrary retentions
        :param time_range
        :param tsl:
        :param seconds
        :return:
        """
        return GraphiteFunctionsProvider.scaleHelper(
            time_range, tsl, seconds/time_range.interval, 'scaleToSeconds',
            str(seconds))

    @staticmethod
    def aggregateWithWildcards(time_range: TimeRange,
                               tsl: List[TimeSeries],
                               func_name,
                               *nodes) ->\
            List[TimeSeries]:
        """
        :param time_range:
        :param tsl:
        :param func_name:
        :param nodes:
        :return:
        """
        _ = time_range
        if len(tsl) <= 0:
            return []
        num_nodes = len(tsl[0].series_id.split('.'))
        group_by_nodes = [i for i in range(num_nodes) if i not in nodes]
        return GraphiteFunctionsProvider._group_by_nodes(tsl,
                                                         group_by_nodes,
                                                         func_name)

    @staticmethod
    def group(time_range: TimeRange, *tsls) -> List[TimeSeries]:
        """
        Takes an arbitrary number of seriesLists and adds them to a single
        seriesList. This is used to pass multiple seriesLists to a function
        which only takes one
        :param time_range:
        :param tsls:
        :return:
        """
        _ = time_range
        tslr = list()
        for tsl in tsls:
            tslr.extend(tsl)
        return tslr

    @staticmethod
    def groupByNode(time_range: TimeRange,
                    tsl: List[TimeSeries],
                    node,
                    func_name) -> \
            List[TimeSeries]:
        """
        :param time_range:
        :param tsl:
        :param node:
        :param func_name:
        :return:
        """
        _ = time_range
        return GraphiteFunctionsProvider._group_by_nodes(tsl,
                                                         [node],
                                                         func_name)

    @staticmethod
    def groupByNodes(time_range: TimeRange,
                     tsl: List[TimeSeries],
                     func_name, *nodes) -> \
            List[TimeSeries]:
        """
        :param time_range:
        :param tsl:
        :param node:
        :param func_name:
        :return:
        """
        _ = time_range
        return GraphiteFunctionsProvider._group_by_nodes(tsl,
                                                         nodes,
                                                         func_name)

    @staticmethod
    def _group_by_nodes(tsl: List[TimeSeries], group_by_nodes, func_name) ->\
            List[TimeSeries]:
        """
        Generates a group key by using the nodes indices specified and
        aggregating each group using func_name
        :param tsl: list of timeseries
        :param group_by_nodes: list of node indicies to generate group key from
        :param func_name: name of aggregate function to apply
        :return: TimeSeriesGroup
        """
        if len(tsl) <= 0:
            return []
        tsg = TimeSeriesGroup()
        sorted_nodes = sorted(group_by_nodes)
        for ts in tsl:
            group_key = GraphiteFunctionsProvider._get_group_key(
                ts.base_series_id, sorted_nodes)
            tsg.add_time_series(group_key, ts)
        _numpy_func = func_name
        if _numpy_func == 'avg':
            _numpy_func = 'average'
        if _numpy_func == 'minSeries':
            _numpy_func = 'min'
        if _numpy_func == 'maxSeries':
            _numpy_func = 'max'
        if _numpy_func == 'sumSeries':
            _numpy_func = 'sum'

        # group by is often use with other high level functions
        # its easier to just call them directly here
        if func_name in ('diffSeries', 'diff'):
            tsgr = TimeSeriesGroup()
            for group_name, group_tsl in tsg.groups.items():
                tslr = GraphiteFunctionsProvider.diffSeries(None, group_tsl)
                tsgr.add_time_series(group_name, tslr[0])
        else:
            # not a known graphite function, lets call the DenseFunctions
            # to see if we have an implementation there
            tsgr = DenseFunctions.execute(_numpy_func, [tsg],
                                          GraphiteFunctionsProvider.fill_func,
                                          GraphiteFunctionsProvider.fill_value)
        return GraphiteFunctionsProvider._tsg_to_list(tsgr, True)

    @staticmethod
    def _tsg_to_list(tsg: TimeSeriesGroup, group_key_as_series_id=False) ->\
            List[TimeSeries]:
        """
        Converts a time-series-group into a single time series list
        Graphite api deals with time series list only
        Perhaps we should remove the concept of TimeSeriesGroup from
        SQL API as well.
        :param tsg:
        :param group_key_as_series_id:
        :return:
        """
        tsl = list()
        for group_key, tslg in tsg.groups.items():
            if len(tslg) != 1:
                raise ValueError('Unexpected state: only one series per group '
                                 'expected after aggregation. Found: {}'.
                                 format(len(tslg)))
            ts = tslg[0]
            if group_key_as_series_id:
                ts.series_id = group_key
                ts.base_series_id = group_key
            tsl.append(ts)
        return tsl

    @staticmethod
    def _get_group_key(series_id: str, sorted_nodes: List[int]) -> str:
        """
        Generates a group key based on indexed node lookup on the dot separated
        series_id
        :param series_id:
        :param nodes:
        :return:
        """
        node_parts = series_id.split('.')
        group_key = ''
        p = -1
        for n in sorted_nodes:
            if n < p:
                raise ValueError(
                    '_get_group_key requires sorted nodes: {}'.format(
                        ','.join(sorted_nodes)))
            if n >= len(node_parts):
                break
            group_key += node_parts[n] + '.'
        if len(group_key) <= 0:
            return group_key
        if group_key[-1] == '.':
            return group_key[:-1]
        return group_key
