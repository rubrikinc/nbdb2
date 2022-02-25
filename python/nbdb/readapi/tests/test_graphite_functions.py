"""
unittests for sqlapi
"""
from typing import List
from unittest import TestCase

from nbdb.common.telemetry import Telemetry
from nbdb.readapi.graphite_functions_provider import GraphiteFunctionsProvider
from nbdb.readapi.time_series_response import TimeSeries, TimeRange


class TestGraphiteFunctions(TestCase):
    """
    Unit test for SQLApi, mocks sparse_series_reader
    """
    def setUp(self):
        Telemetry.inst = None

    def test_aliasSub(self) -> None:
        """
        Runs series names through a regex search/replace.
        &target=aliasSub(ip.*TCP*,"^.*TCP(\\d+)","\1")
        """
        tsl: List[TimeSeries] = list()
        time_range = TimeRange(10, 40, 10)
        tsl.append(TimeSeries('s1_search_value',
                              [(10, 10), (20, 20), (30, 30)],
                              time_range))
        result_tsls = GraphiteFunctionsProvider.aliasSub(time_range,
                                                         tsl,
                                                         'search_value',
                                                         'replace_value')
        for ts in result_tsls:
            self.assertEqual('s1_replace_value', ts.series_id)

        # Verify aliasSub works even after applying summarize(). It should
        # remove any function names from the series ID
        tsl: List[TimeSeries] = list()
        time_range = TimeRange(10, 40, 10)
        tsl.append(TimeSeries('s1.s2.s3.s4',
                              [(10, 10), (20, 20), (30, 30)],
                              time_range))
        tsl = GraphiteFunctionsProvider.summarize(time_range, tsl, '20')
        result_tsls = GraphiteFunctionsProvider.aliasSub(
            time_range, tsl, 's1', 'x1')
        for ts in result_tsls:
            self.assertEqual('x1.s2.s3.s4', ts.series_id)

        # Verify aliasSub works even after applying groupByNodes() and
        # summarize(). It should remove any function names from the series ID
        tsl: List[TimeSeries] = list()
        time_range = TimeRange(10, 40, 10)
        tsl.append(TimeSeries('s1.s2.s3.s4',
                              [(10, 10), (20, 20), (30, 30)],
                              time_range))
        tsl = GraphiteFunctionsProvider.summarize(time_range, tsl, '20')
        tsl = GraphiteFunctionsProvider.groupByNodes(time_range, tsl, 'avg',
                                                     0, 1, 3)
        result_tsls = GraphiteFunctionsProvider.aliasSub(
            time_range, tsl, 's1', 'x1')
        for ts in result_tsls:
            self.assertEqual('x1.s2.s4', ts.series_id)

        # Verify aliasSub works even after applying
        # averageSeriesWithWildcards(). It should remove any function names
        # from the series ID
        tsl: List[TimeSeries] = list()
        time_range = TimeRange(10, 40, 10)
        tsl.append(TimeSeries('s1.s2.s3.s4',
                              [(10, 10), (20, 20), (30, 30)],
                              time_range))
        tsl = GraphiteFunctionsProvider.averageSeriesWithWildcards(time_range,
                                                                   tsl, 2)
        result_tsls = GraphiteFunctionsProvider.aliasSub(
            time_range, tsl, 's1', 'x1')
        for ts in result_tsls:
            self.assertEqual('x1.s2.s4', ts.series_id)

        # Verify aliasSub works even after applying asPercent(). It should
        # remove any function names from the series ID
        tsl: List[TimeSeries] = list()
        time_range = TimeRange(10, 40, 10)
        tsl.append(TimeSeries('s1.s2.s3.s4',
                              [(10, 10), (20, 20), (30, 30)],
                              time_range))
        tsl = GraphiteFunctionsProvider.asPercent(time_range, tsl, 100)
        result_tsls = GraphiteFunctionsProvider.aliasSub(
            time_range, tsl, 's1', 'x1')
        for ts in result_tsls:
            self.assertEqual('x1.s2.s3.s4', ts.series_id)

        # Verify aliasSub works even after applying nonNegativeDerivative(). It
        # should remove any function names from the series ID
        tsl: List[TimeSeries] = list()
        time_range = TimeRange(10, 40, 10)
        tsl.append(TimeSeries('s1.s2.s3.s4',
                              [(10, 10), (20, 20), (30, 30)],
                              time_range))
        tsl = GraphiteFunctionsProvider.nonNegativeDerivative(time_range, tsl)
        result_tsls = GraphiteFunctionsProvider.aliasSub(
            time_range, tsl, 's1', 'x1')
        for ts in result_tsls:
            self.assertEqual('x1.s2.s3.s4', ts.series_id)

    def test_alias(self) -> None:
        """
        Takes one metric or a wildcard seriesList and a string in quotes.
        Prints the string instead of the metric name in the legend.
        &target=alias(Sales.widgets.largeBlue,"Large Blue Widgets")
        """
        tsl: List[TimeSeries] = list()
        time_range = TimeRange(10, 40, 10)
        tsl.append(TimeSeries('s1_search_value',
                              [(10, 10), (20, 20), (30, 30)],
                              time_range))
        result_tsls = GraphiteFunctionsProvider.alias(time_range,
                                                      tsl,
                                                      's1_new_name')
        for ts in result_tsls:
            self.assertEqual('s1_new_name', ts.series_id)

    def test_aliasByNode(self) -> None:
        """
        Takes a seriesList and applies an alias derived from one or more
        “node” portion/s of the target name or tags. Node indices are 0
        indexed.
        """
        tsl: List[TimeSeries] = list()
        time_range = TimeRange(10, 40, 10)
        tsl.append(TimeSeries('s1.s2.s3.s4',
                              [(10, 10), (20, 20), (30, 30)],
                              time_range))
        result_tsls = GraphiteFunctionsProvider.aliasByNode(
            time_range, tsl, 0, 2)
        for ts in result_tsls:
            self.assertEqual('s1.s3', ts.series_id)

        # Verify aliasByNode works even after applying summarize()
        tsl: List[TimeSeries] = list()
        time_range = TimeRange(10, 40, 10)
        tsl.append(TimeSeries('s1.s2.s3.s4',
                              [(10, 10), (20, 20), (30, 30)],
                              time_range))
        tsl = GraphiteFunctionsProvider.summarize(time_range, tsl, '20')
        result_tsls = GraphiteFunctionsProvider.aliasByNode(
            time_range, tsl, 0, 2)
        for ts in result_tsls:
            self.assertEqual('s1.s3', ts.series_id)

        # Verify aliasByNode works even after applying groupByNodes() and
        # summarize(). It should remove any function names from the series ID
        tsl: List[TimeSeries] = list()
        time_range = TimeRange(10, 40, 10)
        tsl.append(TimeSeries('s1.s2.s3.s4',
                              [(10, 10), (20, 20), (30, 30)],
                              time_range))
        tsl = GraphiteFunctionsProvider.summarize(time_range, tsl, '20')
        tsl = GraphiteFunctionsProvider.groupByNodes(time_range, tsl, 'avg',
                                                     0, 1, 3)
        result_tsls = GraphiteFunctionsProvider.aliasByNode(
            time_range, tsl, 0, 2)
        for ts in result_tsls:
            self.assertEqual('s1.s4', ts.series_id)

        # Verify aliasByNode works even after applying
        # averageSeriesWithWildcards() 
        tsl: List[TimeSeries] = list()
        time_range = TimeRange(10, 40, 10)
        tsl.append(TimeSeries('s1.s2.s3.s4',
                              [(10, 10), (20, 20), (30, 30)],
                              time_range))
        tsl = GraphiteFunctionsProvider.averageSeriesWithWildcards(time_range,
                                                                   tsl, 2)
        result_tsls = GraphiteFunctionsProvider.aliasByNode(
            time_range, tsl, 0, 2)
        for ts in result_tsls:
            self.assertEqual('s1.s4', ts.series_id)

        # Verify aliasByNode works even after applying asPercent() 
        tsl: List[TimeSeries] = list()
        time_range = TimeRange(10, 40, 10)
        tsl.append(TimeSeries('s1.s2.s3.s4',
                              [(10, 10), (20, 20), (30, 30)],
                              time_range))
        tsl = GraphiteFunctionsProvider.asPercent(time_range, tsl, 100)
        result_tsls = GraphiteFunctionsProvider.aliasByNode(
            time_range, tsl, 0, 2)
        for ts in result_tsls:
            self.assertEqual('s1.s3', ts.series_id)

        # Verify aliasByNode works even after applying nonNegativeDerivative() 
        tsl: List[TimeSeries] = list()
        time_range = TimeRange(10, 40, 10)
        tsl.append(TimeSeries('s1.s2.s3.s4',
                              [(10, 10), (20, 20), (30, 30)],
                              time_range))
        tsl = GraphiteFunctionsProvider.nonNegativeDerivative(time_range, tsl)
        result_tsls = GraphiteFunctionsProvider.aliasByNode(
            time_range, tsl, 0, 2)
        for ts in result_tsls:
            self.assertEqual('s1.s3', ts.series_id)

        # Verify aliasByNode works even after applying divideSeriesLists()
        time_range = TimeRange(10, 40, 10)
        dividend_series: List[TimeSeries] = list()
        dividend_series.append(TimeSeries('s1.s2.s3.s4',
                                          [(10, 10), (20, 20), (30, 30)],
                                          time_range))
        divisor_series: List[TimeSeries] = list()
        divisor_series.append(TimeSeries('s5.s6.s7.s8',
                                         [(10, 10), (20, 20), (30, 30)],
                                         time_range))
        tsl = GraphiteFunctionsProvider.divideSeriesLists(time_range,
                                                          dividend_series,
                                                          dividend_series)
        result_tsls = GraphiteFunctionsProvider.aliasByNode(
            time_range, tsl, 0, 2)
        # We should pick the dividend series while aliasing
        for ts in result_tsls:
            self.assertEqual('s1.s3', ts.series_id)

    def test_sortByName(self) -> None:
        """
        sorts series by name
        """
        tsl: List[TimeSeries] = list()
        time_range = TimeRange(10, 40, 10)
        tsl.append(TimeSeries('s3',
                              [(10, 10), (20, 20), (30, 30)],
                              time_range))
        tsl.append(TimeSeries('s1',
                              [(10, 10), (20, 20), (30, 30)],
                              time_range))
        tsl.append(TimeSeries('s2',
                              [(10, 10), (20, 20), (30, 30)],
                              time_range))
        result_tsls = GraphiteFunctionsProvider.sortByName(time_range, tsl)
        self.assertEqual(['s1', 's2', 's3'],
                         [ts.series_id for ts in result_tsls])

        result_tsls = GraphiteFunctionsProvider.sortByName(time_range, [])
        self.assertEqual(0, len(result_tsls))

    def test_sortByTotal(self) -> None:
        """
        sort series by total
        """
        tsl: List[TimeSeries] = list()
        time_range = TimeRange(10, 40, 10)
        tsl.append(TimeSeries('s3',
                              [(10, 3), (20, 4), (30, 5)],
                              time_range))
        tsl.append(TimeSeries('s1',
                              [(10, 10), (20, 3), (30, 5)],
                              time_range))
        tsl.append(TimeSeries('s2',
                              [(10, 0), (20, 1), (30, 12)],
                              time_range))
        result_tsls = GraphiteFunctionsProvider.sortByTotal(time_range, tsl)
        self.assertEqual(['s3', 's2', 's1'],
                         [ts.series_id for ts in result_tsls])

        # Test with empty list
        result_tsls = GraphiteFunctionsProvider.sortByTotal(time_range, [])
        self.assertEqual(0, len(result_tsls))

        # Test with None values
        tsl: List[TimeSeries] = list()
        time_range = TimeRange(10, 40, 10)
        tsl.append(TimeSeries('s3',
                              [(10, None), (20, None), (30, None)],
                              time_range))
        tsl.append(TimeSeries('s1',
                              [(10, None), (20, None), (30, 5)],
                              time_range))
        tsl.append(TimeSeries('s2',
                              [(10, 0), (20, 1), (30, 12)],
                              time_range))
        result_tsls = GraphiteFunctionsProvider.sortByTotal(time_range, tsl)
        self.assertEqual(['s3', 's1', 's2'],
                         [ts.series_id for ts in result_tsls])

    def test_sortByMaxima(self) -> None:
        """
        sort series by maximum value
        """
        tsl: List[TimeSeries] = list()
        time_range = TimeRange(10, 40, 10)
        tsl.append(TimeSeries('s3',
                              [(10, 3), (20, 4), (30, 5)],
                              time_range))
        tsl.append(TimeSeries('s1',
                              [(10, 10), (20, 3), (30, 5)],
                              time_range))
        tsl.append(TimeSeries('s2',
                              [(10, 0), (20, 1), (30, 12)],
                              time_range))
        result_tsls = GraphiteFunctionsProvider.sortByMaxima(time_range, tsl)
        self.assertEqual(['s2', 's1', 's3'],
                         [ts.series_id for ts in result_tsls])

        # Test with empty list
        result_tsls = GraphiteFunctionsProvider.sortByMaxima(time_range, [])
        self.assertEqual(0, len(result_tsls))

        # Test with None values
        tsl: List[TimeSeries] = list()
        time_range = TimeRange(10, 40, 10)
        tsl.append(TimeSeries('s3',
                              [(10, None), (20, None), (30, None)],
                              time_range))
        tsl.append(TimeSeries('s1',
                              [(10, None), (20, None), (30, 5)],
                              time_range))
        tsl.append(TimeSeries('s2',
                              [(10, 0), (20, 1), (30, 12)],
                              time_range))
        result_tsls = GraphiteFunctionsProvider.sortByMaxima(time_range, tsl)
        self.assertEqual(['s2', 's1', 's3'],
                         [ts.series_id for ts in result_tsls])

    def test_sortByMinima(self) -> None:
        """
        sort series by minimum value
        """
        tsl: List[TimeSeries] = list()
        time_range = TimeRange(10, 40, 10)
        tsl.append(TimeSeries('s3',
                              [(10, 3), (20, 4), (30, -5)],
                              time_range))
        tsl.append(TimeSeries('s1',
                              [(10, 10), (20, 3), (30, 5)],
                              time_range))
        tsl.append(TimeSeries('s2',
                              [(10, 0), (20, 1), (30, -12)],
                              time_range))
        tsl.append(TimeSeries('s4',
                              [(10, -10), (20, -20), (30, 0)],
                              time_range))
        result_tsls = GraphiteFunctionsProvider.sortByMinima(time_range, tsl)
        # s4 should be ignored because its max value is <= 0
        self.assertEqual(['s2', 's3', 's1'],
                         [ts.series_id for ts in result_tsls])

        # Test with empty list
        result_tsls = GraphiteFunctionsProvider.sortByMinima(time_range, [])
        self.assertEqual(0, len(result_tsls))

        # Test with None values
        tsl: List[TimeSeries] = list()
        time_range = TimeRange(10, 40, 10)
        tsl.append(TimeSeries('s3',
                              [(10, None), (20, None), (30, None)],
                              time_range))
        tsl.append(TimeSeries('s1',
                              [(10, None), (20, None), (30, 5)],
                              time_range))
        tsl.append(TimeSeries('s2',
                              [(10, 0), (20, -1), (30, 12)],
                              time_range))
        result_tsls = GraphiteFunctionsProvider.sortByMinima(time_range, tsl)
        # s3 will be excluded because it has no non-NULL value > 0
        self.assertEqual(['s2', 's1'],
                         [ts.series_id for ts in result_tsls])

    def test_limit(self) -> None:
        """
        limit number of series returned
        """
        tsl: List[TimeSeries] = list()
        time_range = TimeRange(10, 40, 10)
        tsl.append(TimeSeries('s1',
                              [(10, 3), (20, 4), (30, 5)],
                              time_range))
        tsl.append(TimeSeries('s2',
                              [(10, 10), (20, 3), (30, 5)],
                              time_range))
        tsl.append(TimeSeries('s3',
                              [(10, 0), (20, 1), (30, 12)],
                              time_range))
        tsl.append(TimeSeries('s4',
                              [(10, 0), (20, 1), (30, 12)],
                              time_range))
        result_tsls = GraphiteFunctionsProvider.limit(time_range, tsl, 1)
        self.assertEqual(['s1'], [ts.series_id for ts in result_tsls])

        result_tsls = GraphiteFunctionsProvider.limit(time_range, tsl, 4)
        self.assertEqual(['s1', 's2', 's3', 's4'],
                         [ts.series_id for ts in result_tsls])

        result_tsls = GraphiteFunctionsProvider.limit(time_range, tsl, 5)
        self.assertEqual(['s1', 's2', 's3', 's4'],
                         [ts.series_id for ts in result_tsls])

        result_tsls = GraphiteFunctionsProvider.limit(time_range, tsl, -1)
        self.assertEqual(['s1', 's2', 's3'],
                         [ts.series_id for ts in result_tsls])

        result_tsls = GraphiteFunctionsProvider.limit(time_range, tsl, -2)
        self.assertEqual(['s1', 's2'],
                         [ts.series_id for ts in result_tsls])

        result_tsls = GraphiteFunctionsProvider.limit(time_range, tsl, -5)
        self.assertEqual([], [ts.series_id for ts in result_tsls])

        result_tsls = GraphiteFunctionsProvider.limit(time_range, tsl, 0)
        self.assertEqual(0, len(result_tsls))

    def test_changed(self) -> None:
        """
        Test changed() function
        """
        tsl: List[TimeSeries] = list()
        time_range = TimeRange(10, 80, 10)
        # Verify changed() with a series which has multiple datapoints where:
        # a. Both prev value and curr value are NULL: New value is 0
        # b. Prev value is NULL but curr value is not NULL: New value is 0
        # c. Prev value is not NULL but curr value is NULL: New value is 0
        # d. Both values are not NULL, but not different: New value is 0
        # e. Both values are not NULL and are different: New value is 1
        tsl.append(TimeSeries('s1',
                              [(10, 3), (20, None), (30, None), (40, 5),
                               (50, 5), (60, 6), (70, 1)],
                              time_range))
        result_tsls = GraphiteFunctionsProvider.changed(time_range, tsl)
        self.assertEqual([(10, 0), (20, 0), (30, 0), (40, 0), (50, 0),
                          (60, 1), (70, 1)],
                         result_tsls[0].points)

    def test_delay(self) -> None:
        """
        Test delay() function
        """
        tsl: List[TimeSeries] = list()
        time_range = TimeRange(10, 50, 10)
        # delay() basic sanity
        tsl.append(TimeSeries('s1',
                              [(10, 3), (20, 4), (30, 5), (40, None)],
                              time_range))
        # delay() with steps=0 should result in same series
        result_tsls = GraphiteFunctionsProvider.delay(time_range, tsl, 0)
        self.assertEqual([(10, 3), (20, 4), (30, 5), (40, None)],
                         result_tsls[0].points)

        # delay() with steps=1
        result_tsls = GraphiteFunctionsProvider.delay(time_range, tsl, 1)
        self.assertEqual([(10, None), (20, 3), (30, 4), (40, 5)],
                         result_tsls[0].points)

        # delay() where steps is greater than series length
        result_tsls = GraphiteFunctionsProvider.delay(time_range, tsl, 5)
        self.assertEqual([(10, None), (20, None), (30, None), (40, None)],
                         result_tsls[0].points)

        # Make sure error is raised when delay() is called with negative steps
        with self.assertRaises(ValueError):
            result_tsls = GraphiteFunctionsProvider.delay(time_range, tsl, -1)

    def test_maxSeries(self) -> None:
        """
        Test the maxSeries() function
        """
        time_range = TimeRange(10, 40, 10)
        tsls: List[List[TimeSeries]] = list()
        tsls.append([TimeSeries('s1',
                                [(10, 10), (20, 20), (30, 30)],
                                time_range)])
        tsls.append([TimeSeries('s2',
                                [(10, 4), (20, 8), (30, 8)],
                                time_range)])
        tsls.append([TimeSeries('s3',
                                [(10, 5), (20, 10), (30, 20)],
                                time_range)])
        result_tsls = GraphiteFunctionsProvider.maxSeries(time_range, *tsls)
        self.assertEqual(1, len(result_tsls))
        self.assertEqual([(10, 10), (20, 20), (30, 30)], result_tsls[0].points)
        self.assertEqual("max(_)", result_tsls[0].series_id)

        # With empty data
        result_tsls = GraphiteFunctionsProvider.maxSeries(time_range, [])
        self.assertEqual(0, len(result_tsls))

        # With single series
        tsl = [TimeSeries('s1', [(10, 10), (20, 20), (30, 30)], time_range)]
        result_tsls = GraphiteFunctionsProvider.maxSeries(
            time_range, *GraphiteFunctionsProvider.scale(time_range, tsl, 2))
        self.assertEqual(1, len(result_tsls))

        # With None data
        tsls: List[List[TimeSeries]] = list()
        tsls.append([TimeSeries('s1',
                                [(10, 10), (20, None), (30, None)],
                                time_range)])
        tsls.append([TimeSeries('s2',
                                [(10, 4), (20, 8), (30, None)],
                                time_range)])
        tsls.append([TimeSeries('s3',
                                [(10, 5), (20, 10), (30, None)],
                                time_range)])
        result_tsls = GraphiteFunctionsProvider.maxSeries(None, *tsls)
        self.assertEqual(1, len(result_tsls))
        self.assertEqual([(10, 10), (20, 10), (30, None)],
                         result_tsls[0].points)

    def test_minSeries(self) -> None:
        """
        Test the minSeries() function
        """
        time_range = TimeRange(10, 40, 10)
        tsls: List[List[TimeSeries]] = list()
        tsls.append([TimeSeries('s1',
                                [(10, 10), (20, 20), (30, 30)],
                                time_range)])
        tsls.append([TimeSeries('s2',
                                [(10, 4), (20, 8), (30, 8)],
                                time_range)])
        tsls.append([TimeSeries('s3',
                                [(10, 5), (20, 10), (30, 20)],
                                time_range)])
        result_tsls = GraphiteFunctionsProvider.minSeries(time_range, *tsls)
        self.assertEqual(1, len(result_tsls))
        self.assertEqual([(10, 4), (20, 8), (30, 8)], result_tsls[0].points)
        self.assertEqual("min(_)", result_tsls[0].series_id)

        # With empty data
        result_tsls = GraphiteFunctionsProvider.minSeries(time_range, [])
        self.assertEqual(0, len(result_tsls))

        # With single series
        tsl = [TimeSeries('s1', [(10, 10), (20, 20), (30, 30)], time_range)]
        result_tsls = GraphiteFunctionsProvider.minSeries(
            time_range, *GraphiteFunctionsProvider.scale(time_range, tsl, 2))
        self.assertEqual(1, len(result_tsls))

        # With None data
        tsls: List[List[TimeSeries]] = list()
        tsls.append([TimeSeries('s1',
                                [(10, 10), (20, None), (30, None)],
                                time_range)])
        tsls.append([TimeSeries('s2',
                                [(10, 4), (20, 8), (30, None)],
                                time_range)])
        tsls.append([TimeSeries('s3',
                                [(10, 5), (20, 10), (30, None)],
                                time_range)])
        result_tsls = GraphiteFunctionsProvider.minSeries(None, *tsls)
        self.assertEqual(1, len(result_tsls))
        self.assertEqual([(10, 4), (20, 8), (30, None)],
                         result_tsls[0].points)

    def test_sumSeries(self) -> None:
        """
        Test sumSeries method
        """
        time_range = TimeRange(10, 40, 10)
        tsls: List[List[TimeSeries]] = list()
        tsls.append([TimeSeries('s1',
                                [(10, 10), (20, 20), (30, 30)],
                                time_range)])
        tsls.append([TimeSeries('s2',
                                [(10, 4), (20, 8), (30, 8)],
                                time_range)])
        tsls.append([TimeSeries('s3',
                                [(10, 5), (20, 10), (30, 20)],
                                time_range)])
        result_tsls = GraphiteFunctionsProvider.sumSeries(time_range, *tsls)
        self.assertEqual(1, len(result_tsls))
        self.assertEqual([(10, 19), (20, 38), (30, 58)], result_tsls[0].points)
        self.assertEqual("sum(_)", result_tsls[0].series_id)

        # With empty data
        result_tsls = GraphiteFunctionsProvider.sumSeries(time_range, [])
        self.assertEqual(0, len(result_tsls))

        # With single series
        tsl = [TimeSeries('s1', [(10, 10), (20, 20), (30, 30)], time_range)]
        result_tsls = GraphiteFunctionsProvider.sumSeries(
            time_range, *GraphiteFunctionsProvider.scale(time_range, tsl, 2))
        self.assertEqual(1, len(result_tsls))

        # With None data
        tsls: List[List[TimeSeries]] = list()
        tsls.append([TimeSeries('s1',
                                [(10, 10), (20, None), (30, 30)],
                                time_range)])
        tsls.append([TimeSeries('s2',
                                [(10, 4), (20, 8), (30, 8)],
                                time_range)])
        tsls.append([TimeSeries('s3',
                                [(10, 5), (20, 10), (30, 20)],
                                time_range)])
        result_tsls = GraphiteFunctionsProvider.sumSeries(None, *tsls)
        self.assertEqual(1, len(result_tsls))
        self.assertEqual([(10, 19), (20, 18), (30, 58)],
                         result_tsls[0].points)

    def test_averageSeries(self) -> None:
        """
        Test averageSeries method
        """
        time_range = TimeRange(10, 40, 10)
        tsls: List[List[TimeSeries]] = list()
        tsls.append([TimeSeries('s1',
                                [(10, 10), (20, 20), (30, 30)],
                                time_range)])
        tsls.append([TimeSeries('s2',
                                [(10, 3), (20, 6), (30, 7)],
                                time_range)])
        tsls.append([TimeSeries('s3',
                                [(10, 5), (20, 10), (30, 20)],
                                time_range)])
        result_tsls = GraphiteFunctionsProvider.averageSeries(time_range,
                                                              *tsls)
        self.assertEqual(1, len(result_tsls))
        self.assertEqual([(10, 6), (20, 12), (30, 19)], result_tsls[0].points)
        self.assertEqual("average(_)", result_tsls[0].series_id)

        # With empty data
        result_tsls = GraphiteFunctionsProvider.averageSeries(time_range, [])
        self.assertEqual(0, len(result_tsls))

        # With single series
        tsl = [TimeSeries('s1', [(10, 10), (20, 20), (30, 30)], time_range)]
        result_tsls = GraphiteFunctionsProvider.averageSeries(
            time_range, *GraphiteFunctionsProvider.scale(time_range, tsl, 2))
        self.assertEqual(1, len(result_tsls))

        # With None data
        tsls: List[List[TimeSeries]] = list()
        tsls.append([TimeSeries('s1',
                                [(10, 10), (20, None), (30, 30)],
                                time_range)])
        tsls.append([TimeSeries('s2',
                                [(10, 3), (20, 8), (30, 7)],
                                time_range)])
        tsls.append([TimeSeries('s3',
                                [(10, 5), (20, 10), (30, 20)],
                                time_range)])
        result_tsls = GraphiteFunctionsProvider.averageSeries(None, *tsls)
        self.assertEqual(1, len(result_tsls))
        self.assertEqual([(10, 6), (20, 9), (30, 19)],
                         result_tsls[0].points)

    def test_diffSeries(self) -> None:
        """
        Test Diff Series method
        """
        time_range = TimeRange(10, 40, 10)
        tsls: List[List[TimeSeries]] = list()
        tsls.append([TimeSeries('s1',
                                [(10, 10), (20, 20), (30, 30)],
                                time_range)])
        tsls.append([TimeSeries('s2',
                                [(10, 4), (20, 8), (30, 8)],
                                time_range)])
        tsls.append([TimeSeries('s3',
                                [(10, 5), (20, 10), (30, 20)],
                                time_range)])
        result_tsls = GraphiteFunctionsProvider.diffSeries(time_range, *tsls)
        self.assertEqual(1, len(result_tsls))
        self.assertEqual([(10, 1), (20, 2), (30, 2)], result_tsls[0].points)

        # With None data
        tsls: List[List[TimeSeries]] = list()
        tsls.append([TimeSeries('s1',
                                [(10, 10), (20, None), (30, 30)],
                                time_range)])
        tsls.append([TimeSeries('s2',
                                [(10, 4), (20, 8), (30, 8)],
                                time_range)])
        tsls.append([TimeSeries('s3',
                                [(10, 5), (20, 10), (30, 20)],
                                time_range)])
        result_tsls = GraphiteFunctionsProvider.diffSeries(None, *tsls)
        self.assertEqual(1, len(result_tsls))
        self.assertEqual([(10, 1), (20, None), (30, 2)], result_tsls[0].points)

        # With all empty series
        tsls: List[List[TimeSeries]] = list()
        tsls.append([])
        tsls.append([])
        tsls.append([])
        result_tsls = GraphiteFunctionsProvider.diffSeries(time_range, *tsls)
        self.assertEqual(0, len(result_tsls))

    def test_divideSeries(self) -> None:
        """
        Takes a dividend metric and a divisor metric and draws the division
        result. A constant may not be passed. To divide by a constant,
         use the scale() function (which is essentially a multiplication
         operation) and use the inverse of the dividend. (Division by 8 =
         multiplication by 1/8 or 0.125)
        """
        time_range = TimeRange(10, 40, 10)
        tsl_dividend: List[TimeSeries] = list()
        tsl_dividend.append(TimeSeries('s1',
                                       [(10, 20), (20, 40), (30, 60)],
                                       time_range))
        tsl_dividend.append(TimeSeries('s1',
                                       [(10, 30), (20, 50), (30, 90)],
                                       time_range))
        tsl_divisor: TimeSeries = TimeSeries('s1',
                                             [(10, 10), (20, 20), (30, 30)],
                                             time_range)
        result_tsls = GraphiteFunctionsProvider.divideSeries(time_range,
                                                             tsl_dividend,
                                                             tsl_divisor)
        self.assertEqual(2, len(result_tsls))
        self.assertEqual([(10, 2), (20, 2), (30, 2)], result_tsls[0].points)
        self.assertEqual([(10, 3), (20, 2.5), (30, 3)], result_tsls[1].points)

        tsl_dividend: List[TimeSeries] = list()
        tsl_dividend.append(TimeSeries('s1',
                                       [(10, 20), (20, None), (30, 60)],
                                       time_range))
        tsl_divisor: TimeSeries = TimeSeries('s1',
                                             [(10, 10), (20, 20), (30, 30)],
                                             time_range)
        result_tsls = GraphiteFunctionsProvider.divideSeries(time_range,
                                                             tsl_dividend,
                                                             tsl_divisor)
        self.assertEqual(1, len(result_tsls))
        self.assertEqual([(10, 2), (20, None), (30, 2)], result_tsls[0].points)

        # Divide by empty series. We should get all NULLs
        result_tsls = GraphiteFunctionsProvider.divideSeries(time_range,
                                                             tsl_dividend, [])
        self.assertEqual(1, len(result_tsls))
        self.assertEqual([(10, None), (20, None), (30, None)],
                         result_tsls[0].points)

    def test_divideSeriesLists(self) -> None:
        """
        Iterates over a two lists and divides list1[0] by list2[0], list1[1] by
        list2[1] and so on. The lists need to be the same length
        """
        time_range = TimeRange(10, 40, 10)
        tsl_dividend: List[TimeSeries] = list()
        tsl_dividend.append(TimeSeries('s1',
                                       [(10, 20), (20, 40), (30, 60)],
                                       time_range))
        tsl_dividend.append(TimeSeries('s2',
                                       [(10, 30), (20, 50), (30, 90)],
                                       time_range))
        tsl_divisor: List[TimeSeries] = list()
        tsl_divisor.append(TimeSeries('s3',
                                      [(10, 10), (20, 20), (30, 30)],
                                      time_range))
        tsl_divisor.append(TimeSeries('s4',
                                      [(10, 10), (20, 20), (30, 30)],
                                      time_range))
        result_tsls = GraphiteFunctionsProvider.divideSeriesLists(
            time_range, tsl_dividend, tsl_divisor)
        self.assertEqual(2, len(result_tsls))
        self.assertEqual('divideSeries(s1,s3)', result_tsls[0].series_id)
        self.assertEqual([(10, 2), (20, 2), (30, 2)], result_tsls[0].points)
        self.assertEqual('divideSeries(s2,s4)', result_tsls[1].series_id)
        self.assertEqual([(10, 3), (20, 2.5), (30, 3)], result_tsls[1].points)

        # Divide by series containing NULLs
        tsl_divisor: List[TimeSeries] = list()
        tsl_divisor.append(TimeSeries('s3',
                                      [(10, None), (20, 20), (30, 30)],
                                      time_range))
        tsl_divisor.append(TimeSeries('s4',
                                      [(10, None), (20, 20), (30, 30)],
                                      time_range))
        result_tsls = GraphiteFunctionsProvider.divideSeriesLists(
            time_range, tsl_dividend, tsl_divisor)
        self.assertEqual(2, len(result_tsls))
        self.assertEqual('divideSeries(s1,s3)', result_tsls[0].series_id)
        self.assertEqual([(10, None), (20, 2), (30, 2)], result_tsls[0].points)
        self.assertEqual('divideSeries(s2,s4)', result_tsls[1].series_id)
        self.assertEqual([(10, None), (20, 2.5), (30, 3)], result_tsls[1].points)

        # Divide by series containing zeroes
        tsl_divisor: List[TimeSeries] = list()
        tsl_divisor.append(TimeSeries('s3',
                                      [(10, 0), (20, 20), (30, 30)],
                                      time_range))
        tsl_divisor.append(TimeSeries('s4',
                                      [(10, 0), (20, 20), (30, 30)],
                                      time_range))
        result_tsls = GraphiteFunctionsProvider.divideSeriesLists(
            time_range, tsl_dividend, tsl_divisor)
        self.assertEqual(2, len(result_tsls))
        self.assertEqual('divideSeries(s1,s3)', result_tsls[0].series_id)
        self.assertEqual([(10, None), (20, 2), (30, 2)], result_tsls[0].points)
        self.assertEqual('divideSeries(s2,s4)', result_tsls[1].series_id)
        self.assertEqual([(10, None), (20, 2.5), (30, 3)], result_tsls[1].points)

    def test_asPercent(self) -> None:
        """
        Calculates a percentage of the total of a wildcard series.
        If total is specified, each series will be calculated as a percentage
        of that total. If total is not specified, the sum of all points in
        the wildcard series will be used instead.
        """
        time_range = TimeRange(10, 40, 10)

        # Simple case asPercent of self
        tsl: List[TimeSeries] = list()
        tsl.append(TimeSeries('s1', [(10, 2), (20, 4), (30, 3)], time_range))
        tsl.append(TimeSeries('s2', [(10, 1), (20, 2), (30, 2)], time_range))
        tsl.append(TimeSeries('s3', [(10, 3), (20, 2), (30, 1)], time_range))
        tsl.append(TimeSeries('s4', [(10, 4), (20, 2), (30, 4)], time_range))
        result = GraphiteFunctionsProvider.asPercent(time_range, tsl)

        self.assertEqual(4, len(result))
        self.assertEqual([(10, 20), (20, 40), (30, 30)], result[0].points)
        self.assertEqual([(10, 10), (20, 20), (30, 20)], result[1].points)
        self.assertEqual([(10, 30), (20, 20), (30, 10)], result[2].points)
        self.assertEqual([(10, 40), (20, 20), (30, 40)], result[3].points)

        # Simple case asPercent of a constant
        tsl: List[TimeSeries] = list()
        tsl.append(TimeSeries('s1', [(10, 2), (20, 4), (30, 3)], time_range))
        tsl.append(TimeSeries('s2', [(10, 1), (20, 2), (30, 2)], time_range))
        tsl.append(TimeSeries('s3', [(10, 3), (20, 2), (30, 1)], time_range))
        tsl.append(TimeSeries('s4', [(10, 4), (20, 2), (30, 4)], time_range))
        result = GraphiteFunctionsProvider.asPercent(time_range, tsl, 100)

        self.assertEqual(4, len(result))
        self.assertEqual([(10, 2), (20, 4), (30, 3)], result[0].points)
        self.assertEqual([(10, 1), (20, 2), (30, 2)], result[1].points)
        self.assertEqual([(10, 3), (20, 2), (30, 1)], result[2].points)
        self.assertEqual([(10, 4), (20, 2), (30, 4)], result[3].points)

        # asPercent of another total series
        tot: List[TimeSeries] = list()
        tot.append(TimeSeries('t1', [(10, 4), (20, 8), (30, 6)], time_range))
        tot.append(TimeSeries('t2', [(10, 2), (20, 4), (30, 4)], time_range))
        tot.append(TimeSeries('t3', [(10, 6), (20, 4), (30, 2)], time_range))
        tot.append(TimeSeries('t4', [(10, 8), (20, 4), (30, 8)], time_range))
        result = GraphiteFunctionsProvider.asPercent(time_range, tsl, tot)

        self.assertEqual(4, len(result))
        self.assertEqual([(10, 10), (20, 20), (30, 15)], result[0].points)
        self.assertEqual([(10, 5), (20, 10), (30, 10)], result[1].points)
        self.assertEqual([(10, 15), (20, 10), (30, 5)], result[2].points)
        self.assertEqual([(10, 20), (20, 10), (30, 20)], result[3].points)

        # asPercent with None values
        tsl: List[TimeSeries] = list()
        tsl.append(TimeSeries('s1', [(10, 2), (20, 4), (30, 3)], time_range))
        tsl.append(TimeSeries('s2', [(10, 1), (20, 2), (30, 2)], time_range))
        tsl.append(TimeSeries('s3', [(10, None), (20, 2), (30, 1)], time_range))
        tsl.append(TimeSeries('s4', [(10, 4), (20, 2), (30, 4)], time_range))
        result = GraphiteFunctionsProvider.asPercent(time_range, tsl)

        self.assertEqual(4, len(result))
        self.assertEqual([(10, 200/7), (20, 40), (30, 30)], result[0].points)
        self.assertEqual([(10, 100/7), (20, 20), (30, 20)], result[1].points)
        self.assertEqual([(10, None), (20, 20), (30, 10)], result[2].points)
        self.assertEqual([(10, 400/7), (20, 20), (30, 40)], result[3].points)

        # asPercent of another total series with Nones
        tot: List[TimeSeries] = list()
        tot.append(TimeSeries('t1', [(10, 4), (20, 8), (30, 6)], time_range))
        tot.append(TimeSeries('t2', [(10, 2), (20, 4), (30, 4)], time_range))
        tot.append(TimeSeries('t3', [(10, 2), (20, 4), (30, 2)], time_range))
        tot.append(TimeSeries('t4', [(10, None), (20, 4), (30, 8)], time_range))
        result = GraphiteFunctionsProvider.asPercent(time_range, tsl, tot)

        self.assertEqual(4, len(result))
        self.assertEqual([(10, 200/8), (20, 20), (30, 15)], result[0].points)
        self.assertEqual([(10, 100/8), (20, 10), (30, 10)], result[1].points)
        self.assertEqual([(10, None), (20, 10), (30, 5)], result[2].points)
        self.assertEqual([(10, 400/8), (20, 10), (30, 20)], result[3].points)

    def test_nonNegativeDerivative(self) -> None:
        """
        computes a non negative difference between consecutive points
        """
        time_range = TimeRange(10, 40, 10)

        tsl: List[TimeSeries] = list()
        tsl.append(TimeSeries('s1', [(10, 2), (20, 4), (30, 6)], time_range))
        tsl.append(TimeSeries('s2', [(10, 4), (20, 2), (30, 4)], time_range))
        result = GraphiteFunctionsProvider.nonNegativeDerivative(time_range,
                                                                 tsl)

        self.assertEqual(2, len(result))
        self.assertEqual([(10, None), (20, 2), (30, 2)], result[0].points)
        self.assertEqual("nonNegativeDerivative(s1)", result[0].series_id)
        self.assertEqual([(10, None), (20, None), (30, 2)], result[1].points)
        self.assertEqual("nonNegativeDerivative(s2)", result[1].series_id)

        # with None Values
        tsl: List[TimeSeries] = list()
        tsl.append(TimeSeries('s1',
                              [(10, 2), (20, 2), (30, 6),
                               (40, None), (50, 6), (60, 8)],
                              time_range))
        result = GraphiteFunctionsProvider.nonNegativeDerivative(time_range,
                                                                 tsl)

        self.assertEqual(1, len(result))
        self.assertEqual([(10, None), (20, 0), (30, 4),
                          (40, None), (50, None), (60, 2)],
                         result[0].points)

    # pylint: disable-msg=R0915  # Too Many Statements
    def test_groupByNodes(self) -> None:
        """
        average value with wild cards
        """
        time_range = TimeRange(10, 40, 10)

        # groupByNodes('avg', ...)
        tsl: List[TimeSeries] = list()
        for n1 in range(2):
            for n2 in range(2):
                for n3 in range(2):
                    t = n1*100 + n2*10 + n3
                    tsl.append(TimeSeries('s1.n1_{n1}.n1_{n2}.n3_{n3}.count'.
                                          format(n1=n1, n2=n2, n3=n3),
                                          [(10, 2+t), (20, 4+t), (30, 6+t)],
                                          time_range))
        result = GraphiteFunctionsProvider.groupByNodes(
            time_range, tsl, 'avg', 0, 1, 4)

        result = sorted(result, key=lambda x: x.series_id)

        self.assertEqual(2, len(result))
        self.assertEqual('s1.n1_0.count', result[0].series_id)
        self.assertEqual([(10, 2 + (1+10+11)/4),
                          (20, 4 + (1+10+11)/4),
                          (30, 6 + (1+10+11)/4)],
                         result[0].points)
        self.assertEqual('s1.n1_1.count', result[1].series_id)
        self.assertEqual([(10, 2 + (1+10+11)/4 + 100),
                          (20, 4 + (1+10+11)/4 + 100),
                          (30, 6 + (1+10+11)/4 + 100)],
                         result[1].points)

        # groupByNodes('minSeries', ...)
        result = GraphiteFunctionsProvider.groupByNodes(
            time_range, tsl, 'minSeries', 0, 1, 4)

        result = sorted(result, key=lambda x: x.series_id)

        self.assertEqual(2, len(result))
        self.assertEqual('s1.n1_0.count', result[0].series_id)
        self.assertEqual([(10, 2), (20, 4), (30, 6)],
                         result[0].points)
        self.assertEqual('s1.n1_1.count', result[1].series_id)
        self.assertEqual([(10, 2+100), (20, 4+100), (30, 6+100)],
                         result[1].points)

        # groupByNodes('min', ...)
        result_min = GraphiteFunctionsProvider.groupByNodes(
            time_range, tsl, 'min', 0, 1, 4)
        result_min = sorted(result_min, key=lambda x: x.series_id)
        self.assertEqual(result, result_min)

        # groupByNodes('maxSeries', ...)
        result = GraphiteFunctionsProvider.groupByNodes(
            time_range, tsl, 'maxSeries', 0, 1, 4)

        result = sorted(result, key=lambda x: x.series_id)

        self.assertEqual(2, len(result))
        self.assertEqual('s1.n1_0.count', result[0].series_id)
        self.assertEqual([(10, 2+11), (20, 4+11), (30, 6+11)],
                         result[0].points)
        self.assertEqual('s1.n1_1.count', result[1].series_id)
        self.assertEqual([(10, 2+111), (20, 4+111), (30, 6+111)],
                         result[1].points)

        # groupByNodes('max', ...)
        result_max = GraphiteFunctionsProvider.groupByNodes(
            time_range, tsl, 'max', 0, 1, 4)
        result_max = sorted(result_max, key=lambda x: x.series_id)
        self.assertEqual(result, result_max)

        # groupByNodes('sumSeries', ...)
        result = GraphiteFunctionsProvider.groupByNodes(
            time_range, tsl, 'sumSeries', 0, 1, 4)

        result = sorted(result, key=lambda x: x.series_id)

        self.assertEqual(2, len(result))
        self.assertEqual('s1.n1_0.count', result[0].series_id)
        self.assertEqual([(10, 2*4 + (1+10+11)),
                          (20, 4*4 + (1+10+11)),
                          (30, 6*4 + (1+10+11))],
                         result[0].points)
        self.assertEqual('s1.n1_1.count', result[1].series_id)
        self.assertEqual([(10, 2*4 + 100*4 + (1+10+11)),
                          (20, 4*4 + 100*4 + (1+10+11)),
                          (30, 6*4 + 100*4 + (1+10+11))],
                         result[1].points)

        # groupByNodes('sum', ...)
        result_sum = GraphiteFunctionsProvider.groupByNodes(
            time_range, tsl, 'sum', 0, 1, 4)
        result_sum = sorted(result_sum, key=lambda x: x.series_id)
        self.assertEqual(result, result_sum)

        # groupByNodes('diffSeries', ...)
        result = GraphiteFunctionsProvider.groupByNodes(
            time_range, tsl, 'diffSeries', 0, 1, 4)

        result = sorted(result, key=lambda x: x.series_id)

        self.assertEqual(2, len(result))
        self.assertEqual('s1.n1_0.count', result[0].series_id)
        self.assertEqual([(10, -2*2 - (1+10+11)),
                          (20, -4*2 - (1+10+11)),
                          (30, -6*2 - (1+10+11))],
                         result[0].points)
        self.assertEqual('s1.n1_1.count', result[1].series_id)
        self.assertEqual([(10, -2*2 - 100*2 - (1+10+11)),
                          (20, -4*2 - 100*2 - (1+10+11)),
                          (30, -6*2 - 100*2 - (1+10+11))],
                         result[1].points)

        # groupByNodes('diff', ...)
        result_diff = GraphiteFunctionsProvider.groupByNodes(
            time_range, tsl, 'diff', 0, 1, 4)
        result_diff = sorted(result_diff, key=lambda x: x.series_id)
        self.assertEqual(result, result_diff)

        # With all None values at an epoch
        tsl: List[TimeSeries] = list()
        for n1 in range(2):
            for n2 in range(2):
                for n3 in range(2):
                    t = n1*100 + n2*10 + n3
                    tsl.append(TimeSeries('s1.n1_{n1}.n1_{n2}.n3_{n3}.count'.
                                          format(n1=n1, n2=n2, n3=n3),
                                          [(10, 2+t), (20, None), (30, 6+t)],
                                          time_range))
        result = GraphiteFunctionsProvider.groupByNodes(
            time_range, tsl, 'avg', 0, 1, 4)

        result = sorted(result, key=lambda x: x.series_id)

        self.assertEqual(2, len(result))
        self.assertEqual('s1.n1_0.count', result[0].series_id)
        self.assertEqual([(10, 2 + (1+10+11)/4),
                          (20, None),
                          (30, 6 + (1+10+11)/4)],
                         result[0].points)
        self.assertEqual('s1.n1_1.count', result[1].series_id)
        self.assertEqual([(10, 2 + (1+10+11)/4 + 100),
                          (20, None),
                          (30, 6 + (1+10+11)/4 + 100)],
                         result[1].points)

        # With one None series at all epochs
        tsl: List[TimeSeries] = list()
        for n1 in range(2):
            for n2 in range(2):
                for n3 in range(2):
                    t = n1*100 + n2*10 + n3
                    if n1 == 1 and n2 == 1 and n3 == 1:
                        t = None
                    tsl.append(TimeSeries('s1.n1_{n1}.n1_{n2}.n3_{n3}.count'.
                                          format(n1=n1, n2=n2, n3=n3),
                                          [(10, t), (20, t), (30, t)],
                                          time_range))
        result = GraphiteFunctionsProvider.groupByNodes(
            time_range, tsl, 'avg', 0, 1, 4)

        result = sorted(result, key=lambda x: x.series_id)

        self.assertEqual(2, len(result))
        self.assertEqual('s1.n1_0.count', result[0].series_id)
        self.assertEqual([(10, (1+10+11)/4),
                          (20, (1+10+11)/4),
                          (30, (1+10+11)/4)],
                         result[0].points)
        self.assertEqual('s1.n1_1.count', result[1].series_id)
        self.assertEqual([(10, 103.66666666666667),
                          (20, 103.66666666666667),
                          (30, 103.66666666666667)],
                         result[1].points)


    def test_averageSeriesWithWildcards(self) -> None:
        """
        average value with wild cards
        """
        time_range = TimeRange(10, 40, 10)

        tsl: List[TimeSeries] = list()
        for n1 in range(2):
            for n2 in range(2):
                for n3 in range(2):
                    t = n1*100 + n2*10 + n3
                    tsl.append(TimeSeries('s1.n1_{n1}.n1_{n2}.n3_{n3}.count'.
                                          format(n1=n1, n2=n2, n3=n3),
                                          [(10, 2+t), (20, 4+t), (30, 6+t)],
                                          time_range))
        result = GraphiteFunctionsProvider.averageSeriesWithWildcards(
            time_range, tsl, 2, 3)

        result = sorted(result, key=lambda x: x.series_id)

        self.assertEqual(2, len(result))
        self.assertEqual('s1.n1_0.count', result[0].series_id)
        self.assertEqual([(10, 2 + (1+10+11)/4),
                          (20, 4 + (1+10+11)/4),
                          (30, 6 + (1+10+11)/4)],
                         result[0].points)
        self.assertEqual('s1.n1_1.count', result[1].series_id)
        self.assertEqual([(10, 2 + (1+10+11)/4 + 100),
                          (20, 4 + (1+10+11)/4 + 100),
                          (30, 6 + (1+10+11)/4 + 100)],
                         result[1].points)

        # With all None values at an epoch
        tsl: List[TimeSeries] = list()
        for n1 in range(2):
            for n2 in range(2):
                for n3 in range(2):
                    t = n1*100 + n2*10 + n3
                    tsl.append(TimeSeries('s1.n1_{n1}.n1_{n2}.n3_{n3}.count'.
                                          format(n1=n1, n2=n2, n3=n3),
                                          [(10, 2+t), (20, None), (30, 6+t)],
                                          time_range))
        result = GraphiteFunctionsProvider.averageSeriesWithWildcards(
            time_range, tsl, 2, 3)

        result = sorted(result, key=lambda x: x.series_id)

        self.assertEqual(2, len(result))
        self.assertEqual('s1.n1_0.count', result[0].series_id)
        self.assertEqual([(10, 2 + (1+10+11)/4),
                          (20, None),
                          (30, 6 + (1+10+11)/4)],
                         result[0].points)
        self.assertEqual('s1.n1_1.count', result[1].series_id)
        self.assertEqual([(10, 2 + (1+10+11)/4 + 100),
                          (20, None),
                          (30, 6 + (1+10+11)/4 + 100)],
                         result[1].points)

        # With one None series at all epochs
        tsl: List[TimeSeries] = list()
        for n1 in range(2):
            for n2 in range(2):
                for n3 in range(2):
                    t = n1*100 + n2*10 + n3
                    if n1 == 1 and n2 == 1 and n3 == 1:
                        t = None
                    tsl.append(TimeSeries('s1.n1_{n1}.n1_{n2}.n3_{n3}.count'.
                                          format(n1=n1, n2=n2, n3=n3),
                                          [(10, t), (20, t), (30, t)],
                                          time_range))
        result = GraphiteFunctionsProvider.averageSeriesWithWildcards(
            time_range, tsl, 2, 3)

        result = sorted(result, key=lambda x: x.series_id)

        self.assertEqual(2, len(result))
        self.assertEqual('s1.n1_0.count', result[0].series_id)
        self.assertEqual([(10, (1+10+11)/4),
                          (20, (1+10+11)/4),
                          (30, (1+10+11)/4)],
                         result[0].points)
        self.assertEqual('s1.n1_1.count', result[1].series_id)
        self.assertEqual([(10, 103.66666666666667),
                          (20, 103.66666666666667),
                          (30, 103.66666666666667)],
                         result[1].points)

    def test_sumSeriesWithWildcards(self) -> None:
        """
        sum with wildcards
        """
        time_range = TimeRange(10, 40, 10)

        tsl: List[TimeSeries] = list()
        for n1 in range(2):
            for n2 in range(2):
                for n3 in range(2):
                    t = n1*100 + n2*10 + n3
                    tsl.append(TimeSeries('s1.n1_{n1}.n1_{n2}.n3_{n3}.count'.
                                          format(n1=n1, n2=n2, n3=n3),
                                          [(10, 2+t), (20, 4+t), (30, 6+t)],
                                          time_range))
        result = GraphiteFunctionsProvider.sumSeriesWithWildcards(
            time_range, tsl, 2, 3)

        result = sorted(result, key=lambda x: x.series_id)

        self.assertEqual(2, len(result))
        self.assertEqual('s1.n1_0.count', result[0].series_id)
        self.assertEqual([(10, 8 + (1+10+11)),
                          (20, 16 + (1+10+11)),
                          (30, 24 + (1+10+11))],
                         result[0].points)
        self.assertEqual('s1.n1_1.count', result[1].series_id)
        self.assertEqual([(10, 8 + (1+10+11) + 400),
                          (20, 16 + (1+10+11) + 400),
                          (30, 24 + (1+10+11) + 400)],
                         result[1].points)

        # With all None values at an epoch
        tsl: List[TimeSeries] = list()
        for n1 in range(2):
            for n2 in range(2):
                for n3 in range(2):
                    t = n1*100 + n2*10 + n3
                    tsl.append(TimeSeries('s1.n1_{n1}.n1_{n2}.n3_{n3}.count'.
                                          format(n1=n1, n2=n2, n3=n3),
                                          [(10, 2+t), (20, None), (30, 6+t)],
                                          time_range))
        result = GraphiteFunctionsProvider.sumSeriesWithWildcards(
            time_range, tsl, 2, 3)

        result = sorted(result, key=lambda x: x.series_id)

        self.assertEqual(2, len(result))
        self.assertEqual('s1.n1_0.count', result[0].series_id)
        self.assertEqual([(10, 8 + (1+10+11)),
                          (20, None),
                          (30, 24 + (1+10+11))],
                         result[0].points)
        self.assertEqual('s1.n1_1.count', result[1].series_id)
        self.assertEqual([(10, 8 + (1+10+11) + 400),
                          (20, None),
                          (30, 24 + (1+10+11) + 400)],
                         result[1].points)

        # With one None series at all epochs
        tsl: List[TimeSeries] = list()
        for n1 in range(2):
            for n2 in range(2):
                for n3 in range(2):
                    t = n1*100 + n2*10 + n3
                    if n1 == 1 and n2 == 1 and n3 == 1:
                        t = None
                    tsl.append(TimeSeries('s1.n1_{n1}.n1_{n2}.n3_{n3}.count'.
                                          format(n1=n1, n2=n2, n3=n3),
                                          [(10, t), (20, t), (30, t)],
                                          time_range))
        result = GraphiteFunctionsProvider.sumSeriesWithWildcards(
            time_range, tsl, 2, 3)

        result = sorted(result, key=lambda x: x.series_id)

        self.assertEqual(2, len(result))
        self.assertEqual('s1.n1_0.count', result[0].series_id)
        self.assertEqual([(10, (1+10+11)),
                          (20, (1+10+11)),
                          (30, (1+10+11))],
                         result[0].points)
        self.assertEqual('s1.n1_1.count', result[1].series_id)
        self.assertEqual([(10, 311),
                          (20, 311),
                          (30, 311)],
                         result[1].points)

    def test_countSeries(self) -> None:
        """
        count across all series
        """
        time_range = TimeRange(10, 40, 10)

        tsl: List[TimeSeries] = list()
        for n1 in range(2):
            for n2 in range(2):
                for n3 in range(2):
                    t = n1*100 + n2*10 + n3
                    tsl.append(TimeSeries('s1.n1_{n1}.n1_{n2}.n3_{n3}.count'.
                                          format(n1=n1, n2=n2, n3=n3),
                                          [(10, 2+t), (20, 4+t), (30, 6+t)],
                                          time_range))
        result = GraphiteFunctionsProvider.countSeries(
            time_range, tsl)

        self.assertEqual(1, len(result))
        self.assertEqual([(10, 8), (20, 8), (30, 8)], result[0].points)

        tsl: List[TimeSeries] = list()
        for n1 in range(2):
            for n2 in range(2):
                for n3 in range(2):
                    t = n1*100 + n2*10 + n3
                    if n1 == 1 and n2 == 1 and n3 == 1:
                        t = None
                    tsl.append(TimeSeries('s1.n1_{n1}.n1_{n2}.n3_{n3}.count'.
                                          format(n1=n1, n2=n2, n3=n3),
                                          [(10, t), (20, t), (30, t)],
                                          time_range))
        result = GraphiteFunctionsProvider.countSeries(
            time_range, tsl)

        self.assertEqual(1, len(result))
        self.assertEqual([(10, 8), (20, 8), (30, 8)], result[0].points)

    def test_countNewSeries(self) -> None:
        """
        Count new series added across time
        """
        time_range = TimeRange(10, 40, 10)

        tsl: List[TimeSeries] = list()
        # Series s1 is active in time range [10, 30]
        tsl.append(TimeSeries('s1',
                              [(10, 42), (20, 52), (30, 62)],
                              time_range))
        # Series s2 is active in time range [20, 30], excluding the NULL
        # datapoint at epoch=10
        tsl.append(TimeSeries('s2',
                              [(10, None), (20, 20), (30, 30)],
                              time_range))
        result = GraphiteFunctionsProvider.countNewSeries(time_range, tsl)

        self.assertEqual(1, len(result))
        self.assertEqual([(10, 1), (20, 1), (30, 0)], result[0].points)

        # countNewSeries() with empty results
        result = GraphiteFunctionsProvider.countNewSeries(time_range, [])
        self.assertEqual(1, len(result))
        self.assertEqual([(10, 0), (20, 0), (30, 0)], result[0].points)

    def test_constantLine(self) -> None:
        """
        constant line
        """
        time_range = TimeRange(10, 40, 10)
        result = GraphiteFunctionsProvider.constantLine(time_range, 10)

        self.assertEqual(1, len(result))
        self.assertEqual([(10, 10), (20, 10), (30, 10)], result[0].points)

    def test_removeEmptySeries(self) -> None:
        """
        transforms null to default value
        """
        time_range = TimeRange(10, 40, 10)
        tsl: List[TimeSeries] = [TimeSeries('s1',
                                            [(10, 2), (20, None), (30, 6)],
                                            time_range)]
        result = GraphiteFunctionsProvider.removeEmptySeries(time_range, tsl)

        self.assertEqual(1, len(result))
        self.assertEqual([(10, 2), (20, None), (30, 6)], result[0].points)

        result = GraphiteFunctionsProvider.removeEmptySeries(time_range, tsl, 0.75)

        self.assertEqual(0, len(result))

        tsl: List[TimeSeries] = [TimeSeries('s1',
                                            [(10, 2), (20, 7), (30, 6)],
                                            time_range)]
        result = GraphiteFunctionsProvider.removeEmptySeries(time_range, tsl, 0.75)

        self.assertEqual(1, len(result))
        self.assertEqual([(10, 2), (20, 7), (30, 6)], result[0].points)

        result = GraphiteFunctionsProvider.removeEmptySeries(time_range, tsl, 1.0)

        self.assertEqual(1, len(result))
        self.assertEqual([(10, 2), (20, 7), (30, 6)], result[0].points)

        tsl: List[TimeSeries] = [TimeSeries('s1',
                                            [(10, None), (20, None), (30, None)],
                                            time_range)]
        result = GraphiteFunctionsProvider.removeEmptySeries(time_range, tsl)

        self.assertEqual(0, len(result))

        tsl: List[TimeSeries] = [TimeSeries('s1',
                                        [(10, 2), (20, 7), (30, None), (40, 8)],
                                        time_range)]
        result = GraphiteFunctionsProvider.removeEmptySeries(time_range, tsl, 0.75)

        self.assertEqual(1, len(result))
        self.assertEqual([(10, 2), (20, 7), (30, None), (40, 8)], result[0].points)

        result = GraphiteFunctionsProvider.removeEmptySeries(time_range, tsl, 0.8)

        self.assertEqual(0, len(result))

    def test_transformNull(self) -> None:
        """
        transforms null to default value
        """
        time_range = TimeRange(10, 40, 10)
        tsl: List[TimeSeries] = [TimeSeries('s1',
                                            [(10, 2), (20, None), (30, 6)],
                                            time_range)]
        result = GraphiteFunctionsProvider.transformNull(time_range, tsl)

        self.assertEqual(1, len(result))
        self.assertEqual([(10, 2), (20, 0), (30, 6)], result[0].points)
        self.assertEqual("transformNull(s1)", result[0].series_id)

        tsl: List[TimeSeries] = [TimeSeries('s1',
                                            [(10, 2), (20, None), (30, 6)],
                                            time_range)]
        result = GraphiteFunctionsProvider.transformNull(time_range, tsl, 7)

        self.assertEqual(1, len(result))
        self.assertEqual([(10, 2), (20, 7), (30, 6)], result[0].points)
        self.assertEqual("transformNull(s1,7)", result[0].series_id)

    def test_highest(self) -> None:
        """
        top series with highest aggregate function value
        """
        # TEST 1: Highest max
        time_range = TimeRange(10, 40, 10)
        tsl: List[TimeSeries] = [
            TimeSeries('s1', [(10, None), (20, None), (30, 6)], time_range),
            TimeSeries('s2', [(10, 2), (20, None), (30, 7)], time_range),
            TimeSeries('s3', [(10, 2), (20, 6), (30, 5)], time_range),
            TimeSeries('s4', [(10, None), (20, None), (30, None)], time_range)
        ]
        result = GraphiteFunctionsProvider.highestMax(time_range, tsl, 1)

        self.assertEqual(1, len(result))
        # s2 has the max element
        self.assertEqual('s2', result[0].series_id)
        self.assertEqual([(10, 2), (20, None), (30, 7)], result[0].points)

        # TEST 2: Highest average
        result = GraphiteFunctionsProvider.highestAverage(time_range, tsl, 1)

        self.assertEqual(1, len(result))
        # s1 has the best average despite just one non-NULL value
        self.assertEqual('s1', result[0].series_id)
        self.assertEqual([(10, None), (20, None), (30, 6)], result[0].points)

        # TEST 3: Highest current. Covered in test_highestCurrent()

    def test_highestCurrent(self) -> None:
        """
        top series with highest current value
        """
        time_range = TimeRange(10, 40, 10)
        tsl: List[TimeSeries] = [TimeSeries('s1',
                                            [(10, 2), (20, None), (30, 6)],
                                            time_range),
                                 TimeSeries('s2',
                                            [(10, 2), (20, None), (30, 7)],
                                            time_range),
                                 TimeSeries('s3',
                                            [(10, 2), (20, 10), (30, 5)],
                                            time_range)
                                 ]
        result = GraphiteFunctionsProvider.highestCurrent(time_range, tsl, 1)

        self.assertEqual(1, len(result))
        self.assertEqual('s2', result[0].series_id)
        self.assertEqual([(10, 2), (20, None), (30, 7)], result[0].points)

        tsl: List[TimeSeries] = [TimeSeries('s1',
                                            [(10, 2), (20, None), (30, 6)],
                                            time_range),
                                 TimeSeries('s2',
                                            [(10, 2), (20, None), (30, None)],
                                            time_range),
                                 TimeSeries('s3',
                                            [(10, 2), (20, 10), (30, 5)],
                                            time_range)
                                 ]
        result = GraphiteFunctionsProvider.highestCurrent(time_range, tsl, 1)

        self.assertEqual(1, len(result))
        self.assertEqual('s1', result[0].series_id)
        self.assertEqual([(10, 2), (20, None), (30, 6)], result[0].points)

        # Test with empty series
        tsl: List[TimeSeries] = [TimeSeries('s1', [], time_range)]
        result = GraphiteFunctionsProvider.highestCurrent(time_range, tsl, 1)

        self.assertEqual(1, len(result))
        self.assertEqual('s1', result[0].series_id)
        self.assertEqual(0, len(result[0].points))

    def test_currentAbove(self) -> None:
        """
        all series with latest value above
        """
        time_range = TimeRange(10, 40, 10)
        tsl: List[TimeSeries] = [TimeSeries('s1',
                                            [(10, 2), (20, None), (30, 6)],
                                            time_range),
                                 TimeSeries('s2',
                                            [(10, 2), (20, None), (30, 7)],
                                            time_range),
                                 TimeSeries('s3',
                                            [(10, 2), (20, 10), (30, 5)],
                                            time_range)
                                 ]
        result = GraphiteFunctionsProvider.currentAbove(time_range, tsl, 6)

        self.assertEqual(2, len(result))
        self.assertEqual('s1', result[0].series_id)
        self.assertEqual([(10, 2), (20, None), (30, 6)], result[0].points)
        self.assertEqual('s2', result[1].series_id)
        self.assertEqual([(10, 2), (20, None), (30, 7)], result[1].points)

        tsl: List[TimeSeries] = [TimeSeries('s1',
                                            [(10, 2), (20, None), (30, 6)],
                                            time_range),
                                 TimeSeries('s2',
                                            [(10, 2), (20, None), (30, None)],
                                            time_range),
                                 TimeSeries('s3',
                                            [(10, 2), (20, 10), (30, 5)],
                                            time_range)
                                 ]
        result = GraphiteFunctionsProvider.currentAbove(time_range, tsl, 6)

        self.assertEqual(1, len(result))
        self.assertEqual('s1', result[0].series_id)
        self.assertEqual([(10, 2), (20, None), (30, 6)], result[0].points)

    def test_averageAbove(self) -> None:
        """
        all series with average value above
        """
        time_range = TimeRange(10, 40, 10)
        tsl: List[TimeSeries] = [TimeSeries('s1',
                                            [(10, 2), (20, None), (30, 6)],
                                            time_range),
                                 TimeSeries('s2',
                                            [(10, 2), (20, None), (30, 7)],
                                            time_range),
                                 TimeSeries('s3',
                                            [(10, 2), (20, 1), (30, 4)],
                                            time_range)
                                 ]
        result = GraphiteFunctionsProvider.averageAbove(time_range, tsl, 4)

        self.assertEqual(2, len(result))
        self.assertEqual('s1', result[0].series_id)
        self.assertEqual([(10, 2), (20, None), (30, 6)], result[0].points)
        self.assertEqual('s2', result[1].series_id)
        self.assertEqual([(10, 2), (20, None), (30, 7)], result[1].points)

        tsl: List[TimeSeries] = [TimeSeries('s1',
                                            [(10, 2), (20, None), (30, 6)],
                                            time_range),
                                 TimeSeries('s2',
                                            [(10, 2), (20, None), (30, None)],
                                            time_range),
                                 TimeSeries('s3',
                                            [(10, 2), (20, 1), (30, 4)],
                                            time_range)
                                 ]
        result = GraphiteFunctionsProvider.averageAbove(time_range, tsl, 4)

        self.assertEqual(1, len(result))
        self.assertEqual('s1', result[0].series_id)
        self.assertEqual([(10, 2), (20, None), (30, 6)], result[0].points)

        # all None values
        tsl: List[TimeSeries] = [TimeSeries('s1',
                                            [(10, None), (20, None),
                                             (30, None)],
                                            time_range),
                                 TimeSeries('s2',
                                            [(10, None), (20, None),
                                             (30, None)],
                                            time_range),
                                 TimeSeries('s3',
                                            [(10, 2), (20, 1), (30, 4)],
                                            time_range)
                                 ]
        result = GraphiteFunctionsProvider.averageAbove(time_range, tsl, 4)

        self.assertEqual(0, len(result))

    def test_maximumAbove(self) -> None:
        """
        all series with max above
        """
        time_range = TimeRange(10, 40, 10)
        tsl: List[TimeSeries] = [TimeSeries('s1',
                                            [(10, 2), (20, None), (30, 6)],
                                            time_range),
                                 TimeSeries('s2',
                                            [(10, 2), (20, None), (30, 7)],
                                            time_range),
                                 TimeSeries('s3',
                                            [(10, 2), (20, 1), (30, 4)],
                                            time_range)
                                 ]
        result = GraphiteFunctionsProvider.maximumAbove(time_range, tsl, 6)

        self.assertEqual(2, len(result))
        self.assertEqual('s1', result[0].series_id)
        self.assertEqual([(10, 2), (20, None), (30, 6)], result[0].points)
        self.assertEqual('s2', result[1].series_id)
        self.assertEqual([(10, 2), (20, None), (30, 7)], result[1].points)

        tsl: List[TimeSeries] = [TimeSeries('s1',
                                            [(10, 2), (20, None), (30, 6)],
                                            time_range),
                                 TimeSeries('s2',
                                            [(10, 2), (20, None), (30, None)],
                                            time_range),
                                 TimeSeries('s3',
                                            [(10, 2), (20, 1), (30, 4)],
                                            time_range)
                                 ]
        result = GraphiteFunctionsProvider.maximumAbove(time_range, tsl, 6)

        self.assertEqual(1, len(result))
        self.assertEqual('s1', result[0].series_id)
        self.assertEqual([(10, 2), (20, None), (30, 6)], result[0].points)

    def test_exclude(self) -> None:
        """
        exclude all series that match pattern
        """
        time_range = TimeRange(10, 40, 10)
        tsl: List[TimeSeries] = [TimeSeries('s1',
                                            [(10, 2), (20, None), (30, 6)],
                                            time_range),
                                 TimeSeries('s2_pat',
                                            [(10, 2), (20, None), (30, 7)],
                                            time_range),
                                 TimeSeries('s3_pat',
                                            [(10, 2), (20, 1), (30, 4)],
                                            time_range)
                                 ]
        result = GraphiteFunctionsProvider.exclude(time_range, tsl, 'pat')

        self.assertEqual(1, len(result))
        self.assertEqual('s1', result[0].series_id)
        self.assertEqual([(10, 2), (20, None), (30, 6)], result[0].points)

    def test_nPercentile(self) -> None:
        """
        Return n-th percentile
        """
        time_range = TimeRange(10, 60, 10)
        tsl: List[TimeSeries] = [
            TimeSeries('s1', [(10, 10), (20, None), (30, 20),
                              (40, 30), (50, 40)], time_range),
            TimeSeries('s2', [(10, 20), (20, None), (30, None),
                              (40, None), (50, 10)], time_range),
            TimeSeries('s3', [(10, 10), (20, 20), (30, 30),
                              (40, 40), (50, 50)], time_range),
            TimeSeries('s4', [(10, None), (20, None), (30, None),
                              (40, None), (50, None)], time_range)
        ]
        result = GraphiteFunctionsProvider.nPercentile(time_range, tsl, 50)

        self.assertEqual(4, len(result))
        self.assertEqual('nPercentile(s1,50)', result[0].series_id)
        # 50th percentile of s1 should be 25
        self.assertEqual([(10, 25.0), (20, 25.0), (30, 25.0),
                          (40, 25.0), (50, 25.0)], result[0].points)
        self.assertEqual('nPercentile(s2,50)', result[1].series_id)
        # 50th percentile of s2 should be 15.0
        self.assertEqual([(10, 15.0), (20, 15.0), (30, 15.0),
                          (40, 15.0), (50, 15.0)], result[1].points)
        self.assertEqual('nPercentile(s3,50)', result[2].series_id)
        # 50th percentile of s3 should be 30
        self.assertEqual([(10, 30.0), (20, 30.0), (30, 30.0),
                          (40, 30.0), (50, 30.0)], result[2].points)
        self.assertEqual('nPercentile(s4,50)', result[3].series_id)
        # s4 is all NULL. So should be returned as is
        self.assertEqual([(10, None), (20, None), (30, None),
                          (40, None), (50, None)], result[3].points)

    def test_removeBelowPercentile(self) -> None:
        """
        remove datapoints below percentile
        """
        time_range = TimeRange(10, 60, 10)
        tsl: List[TimeSeries] = [
            TimeSeries('s1', [(10, 10), (20, None), (30, 20),
                              (40, 30), (50, 40)], time_range),
            TimeSeries('s2', [(10, 20), (20, None), (30, None),
                              (40, None), (50, 30)], time_range),
            TimeSeries('s3', [(10, 10), (20, 20), (30, 30),
                              (40, 40), (50, 50)], time_range),
            TimeSeries('s4', [(10, None), (20, None), (30, None),
                              (40, None), (50, None)], time_range)
        ]
        result = GraphiteFunctionsProvider.removeBelowPercentile(time_range,
                                                                 tsl, 75)

        self.assertEqual(4, len(result))
        self.assertEqual('s1', result[0].series_id)
        # 75th percentile of s1 should be 32.5
        self.assertEqual([(10, None), (20, None), (30, None),
                          (40, None), (50, 40)], result[0].points)
        self.assertEqual('s2', result[1].series_id)
        # 75th percentile of s2 should be 27.5
        self.assertEqual([(10, None), (20, None), (30, None),
                          (40, None), (50, 30)], result[1].points)
        self.assertEqual('s3', result[2].series_id)
        # 75th percentile of s3 should be 40
        self.assertEqual([(10, None), (20, None), (30, None),
                          (40, 40), (50, 50)], result[2].points)
        self.assertEqual('s4', result[3].series_id)
        # s4 is all NULL. So should be returned as is
        self.assertEqual([(10, None), (20, None), (30, None),
                          (40, None), (50, None)], result[3].points)

    def test_removeAbovePercentile(self) -> None:
        """
        remove datapoints above percentile
        """
        time_range = TimeRange(10, 60, 10)
        tsl: List[TimeSeries] = [
            TimeSeries('s1', [(10, 10), (20, None), (30, 20),
                              (40, 30), (50, 40)], time_range),
            TimeSeries('s2', [(10, 20), (20, None), (30, None),
                              (40, None), (50, 30)], time_range),
            TimeSeries('s3', [(10, 10), (20, 20), (30, 30),
                              (40, 40), (50, 50)], time_range),
            TimeSeries('s4', [(10, None), (20, None), (30, None),
                              (40, None), (50, None)], time_range)
        ]
        result = GraphiteFunctionsProvider.removeAbovePercentile(time_range,
                                                                 tsl, 75)

        self.assertEqual(4, len(result))
        self.assertEqual('s1', result[0].series_id)
        # 75th percentile of s1 should be 32.5
        self.assertEqual([(10, 10), (20, None), (30, 20),
                          (40, 30), (50, None)], result[0].points)
        self.assertEqual('s2', result[1].series_id)
        # 75th percentile of s2 should be 27.5
        self.assertEqual([(10, 20), (20, None), (30, None),
                          (40, None), (50, None)], result[1].points)
        self.assertEqual('s3', result[2].series_id)
        # 75th percentile of s3 should be 40
        self.assertEqual([(10, 10), (20, 20), (30, 30),
                          (40, 40), (50, None)], result[2].points)
        self.assertEqual('s4', result[3].series_id)
        # s4 is all NULL. So should be returned as is
        self.assertEqual([(10, None), (20, None), (30, None),
                          (40, None), (50, None)], result[3].points)

    def test_removeBelowValue(self) -> None:
        """
        remove datapoints below value
        """
        time_range = TimeRange(10, 40, 10)
        tsl: List[TimeSeries] = [TimeSeries('s1',
                                            [(10, 2), (20, None), (30, 6)],
                                            time_range),
                                 TimeSeries('s2',
                                            [(10, 2), (20, None), (30, 7)],
                                            time_range),
                                 TimeSeries('s3',
                                            [(10, 2), (20, 1), (30, 4)],
                                            time_range)
                                 ]
        result = GraphiteFunctionsProvider.removeBelowValue(time_range, tsl, 4)

        self.assertEqual(3, len(result))
        self.assertEqual('s1', result[0].series_id)
        self.assertEqual([(10, None), (20, None), (30, 6)], result[0].points)
        self.assertEqual('s2', result[1].series_id)
        self.assertEqual([(10, None), (20, None), (30, 7)], result[1].points)
        self.assertEqual('s3', result[2].series_id)
        self.assertEqual([(10, None), (20, None), (30, 4)], result[2].points)

    def test_removeAboveValue(self) -> None:
        """
        remove values above value
        """
        time_range = TimeRange(10, 40, 10)
        tsl: List[TimeSeries] = [TimeSeries('s1',
                                            [(10, 2), (20, None), (30, 6)],
                                            time_range),
                                 TimeSeries('s2',
                                            [(10, 2), (20, None), (30, 7)],
                                            time_range),
                                 TimeSeries('s3',
                                            [(10, 2), (20, 1), (30, 4)],
                                            time_range)
                                 ]
        result = GraphiteFunctionsProvider.removeAboveValue(time_range, tsl, 4)

        self.assertEqual(3, len(result))
        self.assertEqual('s1', result[0].series_id)
        self.assertEqual([(10, 2), (20, None), (30, None)], result[0].points)
        self.assertEqual('s2', result[1].series_id)
        self.assertEqual([(10, 2), (20, None), (30, None)], result[1].points)
        self.assertEqual('s3', result[2].series_id)
        self.assertEqual([(10, 2), (20, 1), (30, 4)], result[2].points)

    def test_keepLastValue(self) -> None:
        """
        repeat last value
        """
        time_range = TimeRange(10, 70, 10)
        tsl: List[TimeSeries] = [TimeSeries('s1',
                                            [(10, 2), (20, None),
                                             (30, None), (40, None),
                                             (50, None), (60, None)],
                                            time_range)
                                 ]
        result = GraphiteFunctionsProvider.keepLastValue(time_range, tsl, 3)

        self.assertEqual(1, len(result))
        self.assertEqual('keepLastValue(s1,3)', result[0].series_id)
        self.assertEqual([(10, 2), (20, 2), (30, 2), (40, 2)], result[0].points)

    def test_movingAverage(self) -> None:
        """
        Test movingAverage()
        """
        time_range = TimeRange(20, 80, 10)
        tsl: List[TimeSeries] = [TimeSeries('s1',
                                            [(20, 2), (30, 3),
                                             (40, 4), (50, None),
                                             (60, None), (70, 7)],
                                            time_range)
                                 ]
        result = GraphiteFunctionsProvider.movingAverage(time_range, tsl, '20')
        self.assertEqual(1, len(result))
        self.assertEqual('movingAverage(s1,"20")', result[0].series_id)
        self.assertEqual([(20, 2), (30, 2.5), (40, 3.5), (50, 4), (60, None),
                          (70, 7)], result[0].points)

    def test_summarize(self) -> None:
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
        """
        time_range = TimeRange(20, 80, 10)
        tsl: List[TimeSeries] = [TimeSeries('s1',
                                            [(20, 2), (30, 3),
                                             (40, 4), (50, 5),
                                             (60, 6), (70, 7)],
                                            time_range)
                                 ]
        # default is sum summary
        result = GraphiteFunctionsProvider.summarize(time_range,
                                                     tsl,
                                                     '20')

        self.assertEqual(1, len(result))
        self.assertEqual('summarize(s1, "20", "sum")', result[0].series_id)
        self.assertEqual([(20, 5), (40, 9), (60, 13)], result[0].points)

        # Test when summarize interval is smaller than time series interval
        result = GraphiteFunctionsProvider.summarize(time_range, tsl, '1')
        self.assertEqual(1, len(result))
        self.assertEqual('summarize(s1, "1", "sum")', result[0].series_id)
        self.assertEqual(tsl[0].points, result[0].points)

        # mean summary
        result = GraphiteFunctionsProvider.summarize(time_range,
                                                     tsl,
                                                     '20',
                                                     'average')

        self.assertEqual(1, len(result))
        self.assertEqual('summarize(s1, "20", "average")', result[0].series_id)
        self.assertEqual([(20, 2.5), (40, 4.5), (60, 6.5)], result[0].points)

        # Also test that 'avg' is the same as 'average'
        result = GraphiteFunctionsProvider.summarize(time_range,
                                                     tsl,
                                                     '20',
                                                     'avg')

        self.assertEqual(1, len(result))
        self.assertEqual('summarize(s1, "20", "average")', result[0].series_id)
        self.assertEqual([(20, 2.5), (40, 4.5), (60, 6.5)], result[0].points)

        # Median and alignToFrom
        tsl: List[TimeSeries] = [TimeSeries('s1',
                                            [(20, 2), (30, 3), (40, 4),
                                             (50, 5), (60, 6), (70, 7)],
                                            time_range)
                                 ]
        # default is sum summary
        result = GraphiteFunctionsProvider.summarize(time_range,
                                                     tsl,
                                                     '30',
                                                     'median',
                                                     True)

        self.assertEqual(1, len(result))
        self.assertEqual('summarize(s1, "30", "median")', result[0].series_id)
        self.assertEqual([(20, 3), (50, 6)], result[0].points)

        # None value with sum
        tsl: List[TimeSeries] = [TimeSeries('s1',
                                            [(20, 2), (30, None),
                                             (40, None), (50, 5),
                                             (60, None), (70, None)],
                                            time_range)
                                 ]
        # default is sum summary
        result = GraphiteFunctionsProvider.summarize(time_range,
                                                     tsl,
                                                     '20')

        self.assertEqual(1, len(result))
        self.assertEqual('summarize(s1, "20", "sum")', result[0].series_id)
        self.assertEqual([(20, 2), (40, 5), (60, None)], result[0].points)

        # None value with average
        tsl: List[TimeSeries] = [TimeSeries('s1',
                                            [(20, 2), (30, None),
                                             (40, None), (50, 5),
                                             (60, None), (70, None)],
                                            time_range)
                                 ]
        result = GraphiteFunctionsProvider.summarize(time_range,
                                                     tsl,
                                                     '20',
                                                     'average')

        self.assertEqual(1, len(result))
        self.assertEqual('summarize(s1, "20", "average")', result[0].series_id)
        self.assertEqual([(20, 2), (40, 5), (60, None)], result[0].points)

    def test_integral(self) -> None:
        """
        This will show the sum over time, sort of like a continuous addition
        function. Useful for finding totals or trends in metrics that are
        collected per minute.
        """
        time_range = TimeRange(20, 80, 10)
        tsl: List[TimeSeries] = [TimeSeries('s1',
                                            [(20, 2), (30, 3),
                                             (40, 4), (50, 5),
                                             (60, 6), (70, 7)],
                                            time_range)
                                 ]
        result = GraphiteFunctionsProvider.integral(time_range, tsl)

        self.assertEqual(1, len(result))
        self.assertEqual('integral(s1)', result[0].series_id)
        self.assertEqual([(20, 2), (30, 5), (40, 9),
                          (50, 14), (60, 20), (70, 27)], result[0].points)

        # With some None values
        tsl: List[TimeSeries] = [TimeSeries('s1',
                                            [(20, 2), (30, 3),
                                             (40, None), (50, 5),
                                             (60, None), (70, 7)],
                                            time_range)
                                 ]
        result = GraphiteFunctionsProvider.integral(time_range, tsl)

        self.assertEqual(1, len(result))
        self.assertEqual('integral(s1)', result[0].series_id)
        self.assertEqual([(20, 2), (30, 5), (40, 5),
                          (50, 10), (60, 10), (70, 17)], result[0].points)

        # With all None values
        tsl: List[TimeSeries] = [TimeSeries('s1',
                                            [(20, None), (30, None),
                                             (40, None), (50, None),
                                             (60, None), (70, None)],
                                            time_range)
                                 ]
        result = GraphiteFunctionsProvider.integral(time_range, tsl)

        self.assertEqual(1, len(result))
        self.assertEqual('integral(s1)', result[0].series_id)
        self.assertEqual([(20, 0), (30, 0), (40, 0),
                          (50, 0), (60, 0), (70, 0)], result[0].points)

    def test_isNonNull(self) -> None:
        """
        This will show the sum over time, sort of like a continuous addition
        function. Useful for finding totals or trends in metrics that are
        collected per minute.
        """
        time_range = TimeRange(10, 40, 10)
        tsl: List[TimeSeries] = [TimeSeries('s1',
                                            [(10, 2), (20, 3), (30, 4)],
                                            time_range)]
        result = GraphiteFunctionsProvider.isNonNull(time_range, tsl)

        self.assertEqual(1, len(result))
        self.assertEqual('isNonNull(s1)', result[0].series_id)
        self.assertEqual([(10, 1), (20, 1), (30, 1)], result[0].points)

        # with Nones
        tsl: List[TimeSeries] = [TimeSeries('s1',
                                            [(10, 2), (20, None), (30, 4)],
                                            time_range)]
        result = GraphiteFunctionsProvider.isNonNull(time_range, tsl)

        self.assertEqual(1, len(result))
        self.assertEqual('isNonNull(s1)', result[0].series_id)
        self.assertEqual([(10, 1), (20, 0), (30, 1)], result[0].points)

    def test_scale(self) -> None:
        """
        This will show the sum over time, sort of like a continuous addition
        function. Useful for finding totals or trends in metrics that are
        collected per minute.
        """
        time_range = TimeRange(10, 40, 10)
        tsl: List[TimeSeries] = [TimeSeries('s1',
                                            [(10, 2), (20, 3), (30, 4)],
                                            time_range)]
        result = GraphiteFunctionsProvider.scale(time_range, tsl, 2)

        self.assertEqual(1, len(result))
        self.assertEqual('scale(s1,2)', result[0].series_id)
        self.assertEqual([(10, 4), (20, 6), (30, 8)], result[0].points)

        # with Nones
        tsl: List[TimeSeries] = [TimeSeries('s1',
                                            [(10, 2), (20, None), (30, 4)],
                                            time_range)]
        result = GraphiteFunctionsProvider.scale(time_range, tsl, 0.5)

        self.assertEqual(1, len(result))
        self.assertEqual('scale(s1,0.5)', result[0].series_id)
        self.assertEqual([(10, 1), (20, None), (30, 2)], result[0].points)

    def test_offset(self) -> None:
        """
        This will show the sum over time, sort of like a continuous addition
        function. Useful for finding totals or trends in metrics that are
        collected per minute.
        """
        time_range = TimeRange(10, 40, 10)
        tsl: List[TimeSeries] = [TimeSeries('s1',
                                            [(10, 2), (20, 3), (30, 4)],
                                            time_range)]
        result = GraphiteFunctionsProvider.offset(time_range, tsl, 2)

        self.assertEqual(1, len(result))
        self.assertEqual('offset(s1,2)', result[0].series_id)
        self.assertEqual([(10, 4), (20, 5), (30, 6)], result[0].points)

        # with Nones
        tsl: List[TimeSeries] = [TimeSeries('s1',
                                            [(10, 2), (20, None), (30, 4)],
                                            time_range)]
        result = GraphiteFunctionsProvider.offset(time_range, tsl, -1)

        self.assertEqual(1, len(result))
        self.assertEqual('offset(s1,-1)', result[0].series_id)
        self.assertEqual([(10, 1), (20, None), (30, 3)], result[0].points)

    def test_absolute(self) -> None:
        """
        This will show the sum over time, sort of like a continuous addition
        function. Useful for finding totals or trends in metrics that are
        collected per minute.
        """
        time_range = TimeRange(10, 40, 10)
        tsl: List[TimeSeries] = [TimeSeries('s1',
                                            [(10, 2), (20, -3), (30, -4)],
                                            time_range)]
        result = GraphiteFunctionsProvider.absolute(time_range, tsl)

        self.assertEqual(1, len(result))
        self.assertEqual('absolute(s1)', result[0].series_id)
        self.assertEqual([(10, 2), (20, 3), (30, 4)], result[0].points)

        # with Nones
        tsl: List[TimeSeries] = [TimeSeries('s1',
                                            [(10, 2), (20, None), (30, -4)],
                                            time_range)]
        result = GraphiteFunctionsProvider.absolute(time_range, tsl)

        self.assertEqual(1, len(result))
        self.assertEqual('absolute(s1)', result[0].series_id)
        self.assertEqual([(10, 2), (20, None), (30, 4)], result[0].points)

    def test_scaleToSeconds(self) -> None:
        """
        Takes one metric or a wildcard seriesList and returns
        “value per seconds” where seconds is a last argument to this functions.
        Useful in conjunction with derivative or integral function if you want
        to normalize its result to a known resolution for arbitrary retentions
        """
        time_range = TimeRange(10, 40, 10)
        tsl: List[TimeSeries] = [TimeSeries('s1',
                                            [(10, 20), (20, 30), (30, 40)],
                                            time_range)]
        result = GraphiteFunctionsProvider.scaleToSeconds(time_range, tsl, 5)

        self.assertEqual(1, len(result))
        self.assertEqual('scaleToSeconds(s1,5)', result[0].series_id)
        self.assertEqual([(10, 10), (20, 15), (30, 20)], result[0].points)

        # with Nones
        tsl: List[TimeSeries] = [TimeSeries('s1',
                                            [(10, 20), (20, None), (30, 40)],
                                            time_range)]
        result = GraphiteFunctionsProvider.scaleToSeconds(time_range, tsl, 5)

        self.assertEqual(1, len(result))
        self.assertEqual('scaleToSeconds(s1,5)', result[0].series_id)
        self.assertEqual([(10, 10), (20, None), (30, 20)], result[0].points)

    def test_aggregateWithWildcards(self) -> None:
        """
        Takes one metric or a wildcard seriesList and returns
        “value per seconds” where seconds is a last argument to this functions.
        Useful in conjunction with derivative or integral function if you want
        to normalize its result to a known resolution for arbitrary retentions
        """
        time_range = TimeRange(10, 40, 10)
        tsl: List[TimeSeries] = [TimeSeries('s1.n1_0.n2',
                                            [(10, 20), (20, 30), (30, 40)],
                                            time_range),
                                 TimeSeries('s1.n1_1.n2',
                                            [(10, 30), (20, 40), (30, 50)],
                                            time_range)
                                 ]
        result = GraphiteFunctionsProvider.aggregateWithWildcards(time_range,
                                                                  tsl,
                                                                  'sum',
                                                                  1)

        self.assertEqual(1, len(result))
        self.assertEqual('s1.n2', result[0].series_id)
        self.assertEqual([(10, 50), (20, 70), (30, 90)], result[0].points)

        result = GraphiteFunctionsProvider.aggregateWithWildcards(time_range,
                                                                  tsl,
                                                                  'maxSeries',
                                                                  1)

        self.assertEqual(1, len(result))
        self.assertEqual('s1.n2', result[0].series_id)
        self.assertEqual([(10, 30), (20, 40), (30, 50)], result[0].points)

        result = GraphiteFunctionsProvider.aggregateWithWildcards(time_range,
                                                                  tsl,
                                                                  'minSeries',
                                                                  1)

        self.assertEqual(1, len(result))
        self.assertEqual('s1.n2', result[0].series_id)
        self.assertEqual([(10, 20), (20, 30), (30, 40)], result[0].points)

        result = GraphiteFunctionsProvider.aggregateWithWildcards(time_range,
                                                                  tsl,
                                                                  'diffSeries',
                                                                  1)

        self.assertEqual(1, len(result))
        self.assertEqual('s1.n2', result[0].series_id)
        self.assertEqual([(10, -10), (20, -10), (30, -10)], result[0].points)

        # With Nones
        time_range = TimeRange(10, 40, 10)
        tsl: List[TimeSeries] = [TimeSeries('s1.n1_0.n2',
                                            [(10, 20), (20, 30), (30, 40)],
                                            time_range),
                                 TimeSeries('s1.n1_1.n2',
                                            [(10, 30), (20, None), (30, 50)],
                                            time_range)
                                 ]
        result = GraphiteFunctionsProvider.aggregateWithWildcards(time_range,
                                                                  tsl,
                                                                  'sum',
                                                                  1)

        self.assertEqual(1, len(result))
        self.assertEqual('s1.n2', result[0].series_id)
        self.assertEqual([(10, 50), (20, 30), (30, 90)], result[0].points)

        result = GraphiteFunctionsProvider.aggregateWithWildcards(time_range,
                                                                  tsl,
                                                                  'maxSeries',
                                                                  1)

        self.assertEqual(1, len(result))
        self.assertEqual('s1.n2', result[0].series_id)
        self.assertEqual([(10, 30), (20, 30), (30, 50)], result[0].points)

        result = GraphiteFunctionsProvider.aggregateWithWildcards(time_range,
                                                                  tsl,
                                                                  'minSeries',
                                                                  1)

        self.assertEqual(1, len(result))
        self.assertEqual('s1.n2', result[0].series_id)
        self.assertEqual([(10, 20), (20, 30), (30, 40)], result[0].points)

        result = GraphiteFunctionsProvider.aggregateWithWildcards(time_range,
                                                                  tsl,
                                                                  'diffSeries',
                                                                  1)

        self.assertEqual(1, len(result))
        self.assertEqual('s1.n2', result[0].series_id)
        self.assertEqual([(10, -10), (20, None), (30, -10)], result[0].points)
