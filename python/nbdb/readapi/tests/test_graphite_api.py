"""
unittests for sqlapi
"""
import os
import mock
from collections import namedtuple
from typing import Dict, List
from unittest import TestCase
from unittest.mock import Mock

from werkzeug.datastructures import ImmutableMultiDict
from nbdb.common.data_point import MISSING_POINT_VALUE
from nbdb.common.metric_parsers import TOKEN_TAG_PREFIX, TOKEN_COUNT
from nbdb.common.telemetry import Telemetry
from nbdb.common.thread_pools import ThreadPools
from nbdb.config.settings import Settings
from nbdb.readapi.druid_reader import DruidReader
from nbdb.readapi.graphite_api import GraphiteApi
from nbdb.readapi.graphite_response import GraphiteResponse
from nbdb.readapi.query_cache import QueryCache, QueryCacheResult
from nbdb.readapi.query_cache_entry import QueryCacheEntry
from nbdb.readapi.sql_parser import FillFunc
from nbdb.readapi.tests.test_sql_api_druid import DruidResult
from nbdb.readapi.time_series_response import TimeRange
from nbdb.readapi.time_series_response import TimeSeriesGroup, TimeSeries
from nbdb.schema.schema import Schema


class TestGraphiteApi(TestCase):
    """
    Unit test for SQLApi, mocks sparse_series_reader
    """
    def setUp(self) -> None:
        """
        Setup the test suite, loads the test_settings.yaml and instantiates
        ThreadPools
        """
        Settings.load_yaml_settings(os.path.dirname(__file__) +
                                    '/test_druid_settings.yaml')
        self.schema = Schema.load_from_file(
            os.path.dirname(__file__) + '/test_schema.yaml')
        QueryCache.inst = None
        Telemetry.inst = None
        DruidReader.inst = None
        Telemetry.initialize()
        DruidReader.instantiate(Settings.inst.Druid.connection_string)
        DruidReader.inst.druid_schema = Mock()
        ThreadPools.instantiate()
        QueryCache.initialize()

    def tearDown(self) -> None:
        """
        Stop all the thread pools, blocking wait
        """
        ThreadPools.inst.stop()
        QueryCache.inst = None
        Telemetry.inst = None
        DruidReader.inst = None

    @staticmethod
    def _get_range_based_on_token_value(tkn_name: str, query: str):
        if tkn_name + '=' not in query:
            return range(4)
        i = int(query.split(tkn_name + '=')[1].split('\'')[1].split('_')[1])
        return range(i, i+1, 1)

    @staticmethod
    def cluster_range(query):
        """
        Checks if node_value is wild card or specific value
        creates a range iterator based on that
        :param query:
        :return: range iterator
        """
        # Metric could be one of two:
        # clusters.* or graphite_flat.clusters.*
        clusters_tkn = 'tkn2' if 'graphite_flat' in query else 'tkn1'
        return TestGraphiteApi._get_range_based_on_token_value(clusters_tkn,
                                                               query)

    @staticmethod
    def node_range(query):
        """
        Checks if node_value is wild card or specific value
        creates a range iterator based on that
        :param node_value:
        :return: range iterator
        """
        # Metric could be one of two:
        # clusters.* or graphite_flat.clusters.*
        node_tkn = 'tkn3' if 'graphite_flat' in query else 'tkn2'
        return TestGraphiteApi._get_range_based_on_token_value(node_tkn, query)

    @staticmethod
    def process_range(query):
        """
        Checks if process_value is wild card or specific value
        creates a range iterator based on that
        :param query:
        :return: range iterator
        """
        # Metric could be one of two:
        # clusters.* or graphite_flat.clusters.*
        proc_tkn = 'tkn5' if 'graphite_flat' in query else 'tkn4'
        return TestGraphiteApi._get_range_based_on_token_value(proc_tkn, query)

    @staticmethod
    def row(column_names, column_values) -> Dict:
        """Create dict mimicking JSON object"""
        return dict(zip(column_names, column_values))

    @staticmethod
    def _query_like_result(query):
        """
        Supports queries against flat series
        clusters.c_1.n_1.Diamond.p_1.user.percent
        :param query:
        :return:
        """
        columns = ['ts'] + \
                  [TOKEN_TAG_PREFIX + str(i) for i in range(6)] + \
                  ['field', TOKEN_COUNT, 'value']
        # check if its the Last value query
        if 'LATEST' in query:
            if 'value IN' in query:
                # We are trying to search for missing or tombstone markers.
                # Provide empty results
                yield None

            # last value query, we should only return one row per series
            for c in TestGraphiteApi.cluster_range(query):
                for n in TestGraphiteApi.node_range(query):
                    for p in TestGraphiteApi.process_range(query):
                        yield TestGraphiteApi.row(
                            columns,
                            [10*1000,
                             'clusters',
                             'cl_{}'.format(c),
                             'nd_{}'.format(n),
                             'Diamond',
                             'pr_{}'.format(p),
                             'user',
                             'percent',
                             7,  # Token count
                             10])
        else:
            for i in range(30, 300):
                for c in TestGraphiteApi.cluster_range(query):
                    for n in TestGraphiteApi.node_range(query):
                        for p in TestGraphiteApi.process_range(query):
                            yield TestGraphiteApi.row(
                                columns,
                                [i * 1000,
                                 'clusters',
                                 'cl_{}'.format(c),
                                 'nd_{}'.format(n),
                                 'Diamond',
                                 'pr_{}'.format(p),
                                 'user',
                                 'percent',
                                 7,  # Token count
                                 30 + i + c + n])

    @staticmethod
    def set_druid_dimensions(token_count) -> None:
        """
        Supports the dimensions for clusters.c_1.n_1.Diamond.p_1.user.percent
        :param token_count
        """
        DruidReader.inst.druid_schema.get_dimensions.return_value = \
        ['field', TOKEN_COUNT] + [TOKEN_TAG_PREFIX + str(i)
                                  for i in range(token_count - 1)]

    def druid_execute(self, query, datasource, user):
        """
        Mocks the Druid execute query
        :param query:
        :return:
        """
        _, _, _ = self, datasource, user
        if 'Diamond' in query:
            return DruidResult(TestGraphiteApi._query_like_result(query))
        raise ValueError('No synthetic data for query: {}'.format(query))

    def execute_graphite(self, graphite_api):
        """
        Helper to execute Graphite query
        """
        # Copy logic from execute_graphite()
        # pylint: disable-msg=W0212  # Protected Access
        interval, sparseness_disabled = GraphiteApi._best_fit_interval(
            graphite_api.parsers[0], self.schema,
            graphite_api.computed_interval,
            self.schema.MIN_STORAGE_INTERVAL)
        time_range = TimeRange.aligned_time_range(graphite_api.start_epoch,
                                                  graphite_api.end_epoch,
                                                  interval)
        cache_raw = GraphiteApi.results_cache_invalidating_funcs_exist(
            graphite_api.parsers[0].args)
        return graphite_api.execute_internal(self.schema,
                                             graphite_api.parsers[0],
                                             time_range, cache_raw,
                                             sparseness_disabled)

    def test_simple_graphite(self) -> None:
        """
        Test simple sql
        :return:
        """
        self.set_druid_dimensions(7)
        DruidReader.execute = TestGraphiteApi.druid_execute
        graphite_api = GraphiteApi(
            self.schema,
            ImmutableMultiDict({
                'target': 'clusters.*.*.Diamond.*.user.percent',
                'from': '-6h',
                'until': 'now',
                'maxDataPoints': '100'}),
            min_interval=1, user='test')
        result = self.execute_graphite(graphite_api)
        tsl = result['_'].groups['_']
        self.assertEqual(64, len(tsl))

        # Test with absolute timestamps
        graphite_api = GraphiteApi(
            self.schema,
            ImmutableMultiDict({
                'target': 'clusters.*.*.Diamond.*.user.percent',
                'from': '12:16_20210601',
                'until': '18:16_20210601',
                'maxDataPoints': '100'}),
            min_interval=1, user='test')
        result = self.execute_graphite(graphite_api)
        tsl = result['_'].groups['_']
        self.assertEqual(64, len(tsl))

        # Test with missing maxDataPoints
        graphite_api = GraphiteApi(
            self.schema,
            ImmutableMultiDict({
                'target': 'clusters.*.*.Diamond.*.user.percent',
                'from': '-6h',
                'until': 'now'}),
            min_interval=1, user='test')
        result = self.execute_graphite(graphite_api)
        tsl = result['_'].groups['_']
        self.assertEqual(64, len(tsl))

        graphite_api = GraphiteApi(
            self.schema,
            ImmutableMultiDict({
                'target': 'clusters.*.*.Diamond.*.user.percent',
                'from': '-1200s',
                'maxDataPoints': '100'}),
            min_interval=1, user='test')
        result = self.execute_graphite(graphite_api)
        tsl = result['_'].groups['_']
        self.assertEqual(64, len(tsl))

    @staticmethod
    def _flat_schema_query(query):
        _ = query
        flat_series = 'clusters.123.RVM.d1.d2.d3.d4.d5.d6.d7.d8.d9.d10.d11.d12'
        tokens = flat_series.split('.')
        token_count = len(tokens) - 1  # last one is field
        columns = (['ts'] +
                   [TOKEN_TAG_PREFIX + str(i) for i in range(token_count)] +
                   ['field', TOKEN_COUNT, 'value'])
        for i in range(1, 2):
            yield TestGraphiteApi.row(columns,
                                      [i * 1000, *tokens, token_count, 100])

    def druid_flat_schema_execute(self, query, datasource, user):
        """
        Mocks the Druid execute query
        :param query:
        :return:
        """
        _, _, _ = self, datasource, user
        return DruidResult(TestGraphiteApi._flat_schema_query(query))

    def test_flat_field_graphite(self) -> None:
        """
        Test simple sql
        :return:
        """
        flat_series = 'clusters.123.RVM.d1.d2.d3.d4.d5.d6.d7.d8.d9.d10.d11.d12'
        self.set_druid_dimensions(len(flat_series.split('.')))
        DruidReader.execute = TestGraphiteApi.druid_flat_schema_execute
        graphite_api = GraphiteApi(
            self.schema,
            ImmutableMultiDict({
                'target': 'clusters.123.*.d1.d2.d3.d4.d5.*.d7.d8.d9.d10.*.d12',
                'from': '-6h',
                'until': 'now',
                'maxDataPoints': '100'}),
            min_interval=1, user='test')
        result = self.execute_graphite(graphite_api)
        tsl = result['_'].groups['_']
        self.assertEqual(flat_series,
                         tsl[0].series_id)

    def test_flat_field_graphite_with_set_patterns(self) -> None:
        """
        Test simple graphite with set patterns
        :return:
        """
        flat_series = 'clusters.123.RVM.d1.d2.d3.d4.d5.d6.d7.d8.d9.d10.d11.d12'
        self.set_druid_dimensions(len(flat_series.split('.')))
        DruidReader.execute = TestGraphiteApi.druid_flat_schema_execute
        graphite_api = GraphiteApi(
            self.schema,
            ImmutableMultiDict({
                'target':
                'clusters.123.*.d1.d2.d3.d4.d5.{*6,d*}.d7.d8.d9.d10.*.d12',
                'from': '-6h',
                'until': 'now',
                'maxDataPoints': '100'}),
            min_interval=1, user='test')
        result = self.execute_graphite(graphite_api)
        tsl = result['_'].groups['_']
        self.assertEqual(flat_series,
                         tsl[0].series_id)

    def test_results_caching_disabled_for_certain_functions(self):
        """
        Verify results caching gets disabled if certain functions are used.
        """
        # All these functions should be disabled
        for func_name in ['countSeries', 'countNewSeries', 'highest',
                          'highestAverage', 'highestCurrent', 'highestMax',
                          'integral', 'limit', 'summarize', 'movingAverage']:
            graphite_api = GraphiteApi(
                self.schema,
                ImmutableMultiDict({
                    'target': 'sumSeries('\
                    '%s(clusters.*.*.*.*.uptime))' % func_name,
                    'from': 'now-6h',
                    'until': 'now',
                    'maxDataPoints': '100'}),
                min_interval=1, user='test')
            self.assertTrue(GraphiteApi.results_cache_invalidating_funcs_exist(
                graphite_api.parsers[0].args))

        # All these functions should be disabled. These functions require an
        # argument
        for func_name in ['currentAbove', 'averageAbove', 'maximumAbove',
                          'nPercentile', 'removeBelowPercentile',
                          'removeAbovePercentile']:
            graphite_api = GraphiteApi(
                self.schema,
                ImmutableMultiDict({
                    'target': 'sumSeries('\
                    '%s(clusters.*.*.*.*.uptime, 95))' % func_name,
                    'from': 'now-6h',
                    'until': 'now',
                    'maxDataPoints': '100'}),
                min_interval=1, user='test')
            self.assertTrue(GraphiteApi.results_cache_invalidating_funcs_exist(
                graphite_api.parsers[0].args))

        # removeEmptySeries(ABC, xFilesFactor) should be disabled if
        # xFilesFactor != 0.0
        graphite_api = GraphiteApi(
            self.schema,
            ImmutableMultiDict({
                'target': 'sumSeries('\
                'removeEmptySeries(clusters.*.*.*.*.uptime, 0.2))',
                'from': 'now-6h',
                'until': 'now',
                'maxDataPoints': '100'}),
            min_interval=1, user='test')
        self.assertTrue(GraphiteApi.results_cache_invalidating_funcs_exist(
            graphite_api.parsers[0].args))

        # removeEmptySeries(ABC, 0) should not be disabled
        graphite_api = GraphiteApi(
            self.schema,
            ImmutableMultiDict({
                'target': 'sumSeries('\
                'removeEmptySeries(clusters.*.*.*.*.uptime, 0))',
                'from': 'now-6h',
                'until': 'now',
                'maxDataPoints': '100'}),
            min_interval=1, user='test')
        self.assertFalse(GraphiteApi.results_cache_invalidating_funcs_exist(
            graphite_api.parsers[0].args))

        # removeEmptySeries(ABC) should not be disabled
        graphite_api = GraphiteApi(
            self.schema,
            ImmutableMultiDict({
                'target': 'sumSeries('\
                'removeEmptySeries(clusters.*.*.*.*.uptime))',
                'from': 'now-6h',
                'until': 'now',
                'maxDataPoints': '100'}),
            min_interval=1, user='test')
        self.assertFalse(GraphiteApi.results_cache_invalidating_funcs_exist(
            graphite_api.parsers[0].args))

    def test_sparseness_disabled_interval_calculation(self) -> None:
        """
        Verify that we compute the right intervals based on whether sparseness
        is enabled or disabled
        """
        # Test interval for a series which doesn't match one of the sparseness
        # patterns
        graphite_api = GraphiteApi(
            self.schema,
            ImmutableMultiDict({
                'target': 'sumSeries(clusters.*.*.Diamond.*.cpu_percent)',
                'from': '-6h',
                'until': 'now',
                'maxDataPoints': '100'}),
            min_interval=1, user='test')
        # pylint: disable-msg=W0212  # Protected Access
        interval, sparseness_disabled = GraphiteApi._best_fit_interval(
            graphite_api.parsers[0], self.schema,
            graphite_api.computed_interval,
            self.schema.MIN_STORAGE_INTERVAL)

        # Interval should be 6h / maxDataPoints = 21600 seconds / 100 = 216
        self.assertEqual((6 * 60 * 60)/100, interval)
        self.assertEqual(False, sparseness_disabled)

        # Test interval for a series which matchs one of the sparseness
        # patterns
        graphite_api = GraphiteApi(
            self.schema,
            ImmutableMultiDict({
                'target': 'sumSeries('\
                'clusters.*.*.service_1.process_1.m_1)',
                'from': '-6h',
                'until': 'now',
                'maxDataPoints': '100'}),
            min_interval=1, user='test')
        # pylint: disable-msg=W0212  # Protected Access
        interval, sparseness_disabled = GraphiteApi._best_fit_interval(
            graphite_api.parsers[0], self.schema,
            graphite_api.computed_interval,
            self.schema.MIN_STORAGE_INTERVAL)

        # maxDataPoints=100 and a 6h query window would result in an interval
        # of 216s, but this will get rounded up to 240s so that it would be a
        # multiple of the 60s storage interval for this series
        self.assertEqual(240, interval)
        self.assertEqual(True, sparseness_disabled)

        # Test interval for a series which matchs one of the sparseness
        # patterns but with a very high value of maxDataPoints specified
        graphite_api = GraphiteApi(
            self.schema,
            ImmutableMultiDict({
                'target': 'sumSeries('\
                'clusters.*.*.service_1.process_1.m_1)',
                'from': '-6h',
                'until': 'now',
                'maxDataPoints': '21600'}),
            min_interval=1, user='test')
        # pylint: disable-msg=W0212  # Protected Access
        interval, sparseness_disabled = GraphiteApi._best_fit_interval(
            graphite_api.parsers[0], self.schema,
            graphite_api.computed_interval,
            self.schema.MIN_STORAGE_INTERVAL)

        # Interval should be 60s
        self.assertEqual(60, interval)
        self.assertEqual(True, sparseness_disabled)

        # Verify that we raise an error when we try to issue a query which
        # combines series which have sparseness disabled with series which have
        # sparseness enabled
        with self.assertRaises(ValueError):
            graphite_api = GraphiteApi(
                self.schema,
                ImmutableMultiDict({
                    'target': 'sumSeries(group('\
                    'clusters.*.*.service_1.process_1.m_1,'\
                    'clusters.*.*.Diamond.m_2))',
                    'from': '-6h',
                    'until': 'now',
                    'maxDataPoints': '100'}),
                min_interval=1, user='test')
            # pylint: disable-msg=W0212  # Protected Access
            interval, sparseness_disabled = GraphiteApi._best_fit_interval(
                graphite_api.parsers[0], self.schema,
                graphite_api.computed_interval,
                self.schema.MIN_STORAGE_INTERVAL)

        # Verify that we raise an error when the query contains multiple series
        # which have sparseness disabled, but don't have the same storage
        # interval
        with self.assertRaises(ValueError):
            graphite_api = GraphiteApi(
                self.schema,
                ImmutableMultiDict({
                    'target': 'sumSeries(group('\
                    'clusters.*.*.service_1.process_1.m_1,'\
                    'clusters.*.*.Diamond.num_fds))',
                    'from': '-6h',
                    'until': 'now',
                    'maxDataPoints': '100'}),
                min_interval=1, user='test')
            # pylint: disable-msg=W0212  # Protected Access
            interval, sparseness_disabled = GraphiteApi._best_fit_interval(
                graphite_api.parsers[0], self.schema,
                graphite_api.computed_interval,
                self.schema.MIN_STORAGE_INTERVAL)

    def test_adjust_time_range(self):
        """
        Verify time range is adjusted if certain functions are specified
        """
        orig_time_range = TimeRange(40, 100, 10)
        # No shifting should happen for regular functions
        graphite_api = GraphiteApi(
            self.schema,
            ImmutableMultiDict({
                'target': 'sumSeries(clusters.*.*.*.*.uptime)',
                'from': 'now-6h',
                'until': 'now',
                'maxDataPoints': '100'}),
            min_interval=1, user='test')
        self.assertEqual(
            orig_time_range,
            GraphiteApi.adjust_time_range_if_needed(
                orig_time_range, graphite_api.parsers[0]))

        # For nonNegativeDerivative(), we should shift by a single datapoint
        graphite_api = GraphiteApi(
            self.schema,
            ImmutableMultiDict({
                'target': 'nonNegativeDerivative(clusters.*.*.*.*.uptime)',
                'from': 'now-6h',
                'until': 'now',
                'maxDataPoints': '100'}),
            min_interval=1, user='test')
        self.assertEqual(
            TimeRange(30, 100, 10),
            GraphiteApi.adjust_time_range_if_needed(
                orig_time_range, graphite_api.parsers[0]))

    def test_averageSeriesWithWildcards(self) -> None:
        """

        :return:
        """
        self.set_druid_dimensions(7)
        DruidReader.execute = TestGraphiteApi.druid_execute
        graphite_api = GraphiteApi(
            self.schema,
            ImmutableMultiDict({
                'target': 'averageSeriesWithWildcards('
                        'clusters.*.*.Diamond.*.user.percent, 2)',
                'from': '30',
                'until': '300',
                'maxDataPoints': '27'}),
            min_interval=1, user='test')
        result = self.execute_graphite(graphite_api)
        tsl = result['_'].groups['_']
        self.assertEqual(16, len(tsl))
        for ts in tsl:
            c = int(ts.series_id.split('.')[1].split('_')[1])
            p = int(ts.series_id.split('.')[3].split('_')[1])
            series_id = 'clusters.cl_{}.Diamond.pr_{}.' \
                        'user.percent'.format(c, p)
            self.assertEqual(series_id, ts.series_id)
            self.assertEqual(27, len(ts.points))
            self.assertEqual(list(zip(range(30, 300, 10),
                                      range(66 + c, 336 + c, 10))), ts.points)

    def test_groupByNode(self) -> None:
        """

        :return:
        """
        self.set_druid_dimensions(7)
        DruidReader.execute = TestGraphiteApi.druid_execute
        # TEST1: Test with non-NULL datapoints
        graphite_api = GraphiteApi(
            self.schema,
            ImmutableMultiDict({
                'target': 'groupByNode('
                        'clusters.*.*.Diamond.*.user.percent, 1,'
                        ' \'average\')',
                'from': '30',
                'until': '300',
                'maxDataPoints': '27'}),
            min_interval=1, user='test')
        result = self.execute_graphite(graphite_api)
        tsl = result['_'].groups['_']
        self.assertEqual(4, len(tsl))
        for ts in tsl:
            c = int(ts.series_id.split('_')[1])
            series_id = 'cl_{}'.format(c)
            self.assertEqual(series_id, ts.series_id)
            self.assertEqual(27, len(ts.points))
            self.assertEqual(list(zip(range(30, 300, 10),
                                      range(66 + c, 336 + c, 10))), ts.points)

        # TEST2: Test with NULL datapoints
        for func in ['average', 'sum', 'min', 'max']:
            graphite_api = GraphiteApi(
                self.schema,
                ImmutableMultiDict({
                    'target': 'groupByNode('
                            'removeAboveValue('
                            'clusters.*.*.Diamond.*.user.percent,' '0), '
                            '1, \'%s\')' % func,
                    'from': '30',
                    'until': '300',
                    'maxDataPoints': '27'}),
                min_interval=1, user='test')
            result = self.execute_graphite(graphite_api)
            tsl = result['_'].groups['_']
            self.assertEqual(4, len(tsl))
            for ts in tsl:
                c = int(ts.series_id.split('_')[1])
                series_id = 'cl_{}'.format(c)
                self.assertEqual(series_id, ts.series_id)
                self.assertEqual(27, len(ts.points))
                self.assertEqual(list(zip(range(30, 300, 10), [None]*27)),
                                 ts.points)

    def test_groupByNodes(self) -> None:
        """

        :return:
        """
        self.set_druid_dimensions(7)
        DruidReader.execute = TestGraphiteApi.druid_execute
        graphite_api = GraphiteApi(
            self.schema,
            ImmutableMultiDict({
                'target': 'groupByNodes('
                        'clusters.*.*.Diamond.*.user.percent,'
                        ' \'average\', 1,3)',
                'from': '30',
                'until': '300',
                'maxDataPoints': '27'}),
            min_interval=1, user='test')
        result = self.execute_graphite(graphite_api)
        tsl = result['_'].groups['_']
        self.assertEqual(4, len(tsl))
        for ts in tsl:
            c = int(ts.series_id.split('.')[0].split('_')[1])
            series_id = 'cl_{}.Diamond'.format(c)
            self.assertEqual(series_id, ts.series_id)
            self.assertEqual(27, len(ts.points))
            self.assertEqual(list(zip(range(30, 300, 10),
                                      range(66 + c, 336 + c, 10))), ts.points)

    # pylint: disable=too-many-arguments
    # pylint: disable-msg=W0613  # Unused Argument
    @staticmethod
    def get_field_WithDiffSeries(schema: Schema,
                                 datasource: str,
                                 field: str,
                                 filters: List,
                                 time_range: TimeRange,
                                 groupby: List[str],
                                 query_id: str,
                                 trace: bool,
                                 fill_func: FillFunc,
                                 fill_value: float,
                                 create_flat_series_id: bool,
                                 sparseness_disabled: bool,
                                 user: str = None
                                 ):
        """
        get_field for the test_groupByNodeWithDiffSeries
        TODO: replace with patch
        :param datasource:
        :param field:
        :param filters:
        :param time_range:
        :param groupby:
        :param query_id:
        :param trace:
        :param fill_func:
        :param fill_value:
        :param create_flat_series_id:
        :param sparseness_disabled:
        :return:
        """
        # unused variables in this test
        _ = schema
        _ = datasource
        _ = field
        _ = filters
        _ = time_range
        _ = groupby
        _ = query_id
        _ = trace
        _ = fill_func
        _ = fill_value
        _ = create_flat_series_id
        _ = sparseness_disabled
        return TimeSeriesGroup({'_': [
            TimeSeries('clusters.c1.n1.Diamond.memTotal',
                       [(10, 15), (20, 25), (30, 35)],
                       TimeRange(10, 40, 10)),
            TimeSeries('clusters.c1.n1.Diamond.memFree',
                       [(10, 10), (20, 20), (30, 30)],
                       TimeRange(10, 40, 10)),
            TimeSeries('clusters.c1.n2.Diamond.memTotal',
                       [(10, 26), (20, 26), (30, 36)],
                       TimeRange(10, 40, 10)),
            TimeSeries('clusters.c1.n2.Diamond.memFree',
                       [(10, 20), (20, 20), (30, 30)],
                       TimeRange(10, 40, 10)),
        ]})

    def test_groupByNodeWithDiffSeries(self) -> None:
        """
        Tests groupByNode using diffSeries as a secondary function
        diffSeries is another graphite function,
        the default implementation of groupByNode leverages the numpy
        functions (sum , mean) etc. However diffSeries is specific and the
        equivalent function subtract from numpy doesn't match the required
        syntax. diffSeries leverages the numpy but adds additional business
        logic to handle the special case.
        This test verifies the code path of calling a graphite function
        in groupByNode
        :return:
        """
        DruidReader.inst.get_field = TestGraphiteApi.get_field_WithDiffSeries
        graphite_api = GraphiteApi(
            self.schema,
            ImmutableMultiDict({
                'target': 'groupByNode(clusters.c1.*.Diamond.*,2,\'diffSeries\')',
                'from': '30',
                'until': '300',
                'maxDataPoints': '27'}),
            min_interval=1, user='test')
        result = self.execute_graphite(graphite_api)
        tsl = result['_'].groups['_']
        self.assertEqual(2, len(tsl))
        for (ts, seried_id, value) in zip(tsl, ['n1', 'n2'], [5, 6]):
            self.assertEqual(seried_id, ts.series_id)
            self.assertEqual(list(zip(range(10, 40, 10), [value]*3)),
                             ts.points)

    # pylint: disable-msg=W0613  # Unused Argument
    @staticmethod
    def get_field_WithNonNegativeDerivative(
            schema: Schema,
            datasource: str,
            field: str,
            filters: List,
            time_range: TimeRange,
            groupby: List[str],
            query_id: str,
            trace: bool,
            fill_func: FillFunc,
            fill_value: float,
            create_flat_series_id: bool,
            sparseness_disabled: bool,
            user: str = None):
        """
        get_field for the test_groupByNodeWithDiffSeries
        TODO: replace with patch
        """
        # unused variables in this test
        del schema, datasource, field, filters, groupby, query_id
        del trace, fill_func, fill_value, create_flat_series_id
        del sparseness_disabled

        final_value: int = 1000 - time_range.points() * 10
        if time_range.start == 30:
            # Create a series that first decreases then increases
            final_dec_point_value: int = 1000 - 4 * 10
            final_inc_point_value: int = (time_range.points()-4)*10 + \
                                         final_dec_point_value
            n3_points = list(
                zip(time_range.epochs(),
                    list(range(1000, final_dec_point_value, -10)) +
                    list(range(final_dec_point_value,
                               final_inc_point_value,
                               10))))
        else:
            # this is incremental load case, the series is now increasing only
            n3_points = list(zip(time_range.epochs(),
                                 range(1, time_range.points()*10, 10)))

        return TimeSeriesGroup({'_': [
            # Delta=100 throughout
            TimeSeries('clusters.c1.n1.Diamond.uptime.seconds',
                       list(zip(time_range.epochs(),
                                range(15, time_range.points()*100, 100))),
                       time_range),
            # Delta=-10 throughout
            TimeSeries('clusters.c1.n2.Diamond.uptime.seconds',
                       list(zip(time_range.epochs(),
                                range(1000, final_value, -10))),
                       time_range),
            # Delta=-10, 10, 20
            TimeSeries('clusters.c1.n3.Diamond.uptime.seconds',
                       n3_points,
                       time_range),
        ]})

    def test_nonNegativeDerivativeWithQueryCache(self) -> None:
        """
        Tests the nonNegativeDerivative() graphite function.

        The official documentation states that only running deltas are
        returned, without normalizing for the time periods as a true derivative
        would do.

        Therefore to maintain correctness, we use numpy.diff() to compute the
        results
        :return:
        """
        DruidReader.inst.get_field = \
            TestGraphiteApi.get_field_WithNonNegativeDerivative
        graphite_api = GraphiteApi(
            self.schema,
            ImmutableMultiDict({
                'target':
                    'nonNegativeDerivative('
                        'clusters.c1.*.Diamond.uptime.seconds)',
                'from': '30',
                'until': '290',
                'maxDataPoints': '26'}),
            min_interval=1, user='test')
        graphite_response: GraphiteResponse = graphite_api.execute_graphite()
        response: List = graphite_response.response
        self.assertEqual(3, len(response))

        self.assertTrue('n1' in response[0]['target'])
        self.assertEqual([[v, e]
                          for v, e in list(zip([None] + [100]*25,
                                               range(30, 290, 10)))],
                         response[0]['datapoints'])

        self.assertTrue('n2' in response[1]['target'])
        self.assertEqual([[v, e]
                          for v, e in list(zip([None]*26,
                                               range(30, 290, 10)))],
                         response[1]['datapoints'])

        self.assertTrue('n3' in response[2]['target'])
        self.assertEqual([[v, e]
                          for v, e in
                          list(zip([None]*5, range(30, 80, 10))) +
                          list(zip([10]*22, range(80, 290, 10)))],
                         response[2]['datapoints'])

        # Query again but asking for one extra data point
        # with cache enabled nonNegativeDerivative will get called
        # with a single data point only, and its not possible to compute
        # non-negative derivative for that. This case is handled special
        # The query cache returned missing_range is expanded in past by one
        # point, so a derivative can be computed from the two raw points
        # With the above special case we should get a valid response
        # even when we move the window by one point only
        graphite_api = GraphiteApi(
            self.schema,
            ImmutableMultiDict({
                'target':
                    'nonNegativeDerivative('
                    'clusters.c1.*.Diamond.uptime.seconds)',
                'from': '30',
                'until': '300',
                'maxDataPoints': '27'}),
            min_interval=1, user='test')
        graphite_response: GraphiteResponse = graphite_api.execute_graphite()
        response: List = graphite_response.response

        self.assertEqual(3, len(response))

        self.assertTrue('n1' in response[0]['target'])
        self.assertEqual([[v, e]
                          for v, e in list(zip([None] + [100]*26,
                                               range(30, 300, 10)))],
                         response[0]['datapoints'])

        self.assertTrue('n2' in response[1]['target'])
        self.assertEqual([[v, e]
                          for v, e in list(zip([None]*27,
                                               range(30, 300, 10)))],
                         response[1]['datapoints'])

        self.assertTrue('n3' in response[2]['target'])
        self.assertEqual([[v, e]
                          for v, e in
                          list(zip([None]*5, range(30, 80, 10))) +
                          list(zip([10]*22, range(80, 300, 10)))],
                         response[2]['datapoints'])

    def test_groupByNodes_averageSeriesWithWildCards(self) -> None:
        """

        :return:
        """
        self.set_druid_dimensions(7)
        DruidReader.execute = TestGraphiteApi.druid_execute
        graphite_api = GraphiteApi(
            self.schema,
            ImmutableMultiDict({
                'target': 'groupByNode('
                        'averageSeriesWithWildcards('
                        'clusters.*.*.Diamond.*.user.percent, 2),'
                        ' 1, \'sum\')',
                'from': '30',
                'until': '300',
                'maxDataPoints': '27'}),
            min_interval=1, user='test')
        result = self.execute_graphite(graphite_api)
        tsl = result['_'].groups['_']
        self.assertEqual(4, len(tsl))
        for ts in tsl:
            c = int(ts.series_id.split('_')[1])
            self.assertTrue(c in list(range(4)))
            self.assertEqual(27, len(ts.points))
            self.assertEqual(list(zip(range(30, 300, 10),
                                      range(264 + c*4, 1346 + c*4, 40))),
                             ts.points)

    def test_sumseries(self) -> None:
        """

        :return:
        """
        self.set_druid_dimensions(7)
        DruidReader.execute = TestGraphiteApi.druid_execute
        graphite_api = GraphiteApi(
            self.schema,
            ImmutableMultiDict({
                'target': 'sumSeries('
                        ' limit('
                        '     keepLastValue('
                        '         sortByName('
                        '             clusters.*.*.Diamond.pr_1.user.percent'
                        '         ),'
                        '         100),'
                        ' 1),'
                        ' limit('
                        '     keepLastValue('
                        '         sortByName('
                        '             clusters.*.*.Diamond.pr_2.user.percent'
                        '         ),'
                        '         100),'
                        ' 1)'
                        ')',
                'from': '30',
                'until': '300',
                'maxDataPoints': '27'}),
            min_interval=1, user='test')
        result = self.execute_graphite(graphite_api)
        tsl = result['_'].groups['_']
        self.assertEqual(1, len(tsl))
        for ts in tsl:
            self.assertEqual(27, len(ts.points))
            self.assertEqual(list(zip(range(30, 300, 10),
                                      range(129, 669, 20))),
                             ts.points)

    def test_diffseries(self) -> None:
        """

        :return:
        """
        self.set_druid_dimensions(7)
        DruidReader.execute = TestGraphiteApi.druid_execute
        graphite_api = GraphiteApi(
            self.schema,
            ImmutableMultiDict({
            'target': 'diffSeries('
                      ' limit('
                      '     keepLastValue('
                      '         sortByName('
                      '             graphite_flat.clusters.*.*.Diamond.pr_0.user.percent'
                      '         ),'
                      '         100),'
                      ' 1),'
                      ' limit('
                      '     keepLastValue('
                      '         sortByName('
                      '             graphite_flat.clusters.*.*.Diamond.pr_1.user.percent'
                      '         ),'
                      '         100),'
                      ' 1),'
                      ' limit('
                      '     keepLastValue('
                      '         sortByName('
                      '             graphite_flat.clusters.*.*.Diamond.pr_2.user.percent'
                      '         ),'
                      '         100),'
                      ' 1),'
                      ' limit('
                      '     keepLastValue('
                      '         sortByName('
                      '             graphite_flat.clusters.*.*.Diamond.pr_3.user.percent'
                      '         ),'
                      '         100),'
                      ' 1)'
                      ')',
            'from': '30',
            'until': '300',
            'maxDataPoints': '27'}),
            min_interval=1, user='test')
        result = self.execute_graphite(graphite_api)
        tsl = result['_'].groups['_']
        self.assertEqual(1, len(tsl))
        for ts in tsl:
            self.assertEqual(27, len(ts.points))
            self.assertEqual(list(zip(range(30, 300, 10),
                                      range(-129, -669, -20))),
                             ts.points)

    def test_as_percent(self) -> None:
        """

        :return:
        """
        self.set_druid_dimensions(7)
        DruidReader.execute = TestGraphiteApi.druid_execute
        graphite_api = GraphiteApi(
            self.schema,
            ImmutableMultiDict({
                'target':
                    'asPercent('
                    ' sumSeries(clusters.cl_3.*.Diamond.*.user.percent),'
                    ' sumSeries(clusters.*.*.Diamond.*.user.percent)'
                    ')',
                'from': '30',
                'until': '300',
                'maxDataPoints': '27'}),
            min_interval=1, user='test')
        result = self.execute_graphite(graphite_api)
        tsl = result['_'].groups['_']
        self.assertEqual(1, len(tsl))
        for ts in tsl:
            self.assertEqual(27, len(ts.points))
            self.assertEqual(list(zip(range(30, 300, 10),
                                      [25]*len(ts.points))),
                             [(e, int(v)) for e, v in ts.points])

    @staticmethod
    def _query_with_none_values(query):
        """
        Creates a result with some none values
        :return:
        """
        columns = ['ts', 'tkn0', 'tkn1', 'tkn2', 'tkns', 'field', 'value']
        # check if its the Last value query
        if 'LATEST' in query:
            if 'value IN' in query:
                # We are trying to search for missing or tombstone markers.
                yield TestGraphiteApi.row(
                    columns,
                    [10*1000,
                     'clusters',
                     'c0',
                     'n0',
                     3,  # token count
                     'count',
                     MISSING_POINT_VALUE])
            else:
                # We are trying to search for the latest non-marker value
                # Return empty row
                yield None
        else:
            for i in range(10, 100):
                value = i
                if i % 10 == 0:
                    value = MISSING_POINT_VALUE
                yield TestGraphiteApi.row(
                    columns,
                    [i * 1000,
                     'clusters',
                     'c0',
                     'n0',
                     3,  # token count
                     'count',
                     value])

    def druid_execute_with_none_values(self, query, datasource, user):
        """
        Mocks the Druid execute query
        :param query:
        :return:
        """
        _, _, _ = self, datasource, user
        return DruidResult(TestGraphiteApi._query_with_none_values(query))

    def test_keep_last_value(self) -> None:
        """

        :return:
        """
        self.set_druid_dimensions(3)
        DruidReader.execute = TestGraphiteApi.druid_execute_with_none_values
        graphite_api = GraphiteApi(
            self.schema,
            ImmutableMultiDict({
                'target':
                    'keepLastValue(clusters.cl_3.*.Diamond.*.user.percent, 10)',
                'from': '10',
                'until': '100',
                'maxDataPoints': '90'}),
            min_interval=1, user='test')
        result = self.execute_graphite(graphite_api)
        tsl = result['_'].groups['_']
        self.assertEqual(1, len(tsl))
        for ts in tsl:
            self.assertEqual(90, len(ts.points))
            for e, v in ts.points:
                if e == 10:
                    # The first epoch is NULL because there is no valid
                    # non-marker value before it.
                    self.assertEqual(v, None)
                elif e % 10 == 0:
                    self.assertEqual(e-1, v)
                else:
                    self.assertEqual(e, v)

    def test_none_values(self) -> None:
        """
        Test none values, None values are downsampled by druid_reader
        so we have to hook into the DruidReader.execute
        :return:
        """
        self.set_druid_dimensions(3)
        DruidReader.execute = TestGraphiteApi.druid_execute_with_none_values
        graphite_api = GraphiteApi(
            self.schema,
            ImmutableMultiDict({
                'target':
                    ' clusters.c0.n0.count,',
                'from': '10',
                'until': '100',
                'maxDataPoints': '90'}),
            min_interval=1, user='test')
        result = self.execute_graphite(graphite_api)
        tsl = result['_'].groups['_']
        self.assertEqual(1, len(tsl))
        for ts in tsl:
            self.assertEqual(90, len(ts.points))
            for e, v in ts.points:
                if e == 10:
                    # The first epoch is NULL because there is no valid
                    # non-marker value before it.
                    self.assertEqual(v, None)
                elif e % 10 == 0:
                    self.assertTrue(v is None)
                else:
                    self.assertEqual(e, v)

    def test_series_name_in_aggregate_queries(self) -> None:
        """
        Tests the series_id generated for the aggregate queries
        The query cache depends on deterministic series_ids to function
        correctly
        """
        self.set_druid_dimensions(7)
        DruidReader.execute = TestGraphiteApi.druid_execute
        graphite_api = GraphiteApi(
            self.schema,
            ImmutableMultiDict({
                'target': 'highestCurrent('
                        ' sumSeriesWithWildcards('
                        '    clusters.*.*.Diamond.*.user.percent,'
                        '    4'
                        ' ),'
                        ' 1'
                        '),',
                'from': '30',
                'until': '300',
                'maxDataPoints': '27'}),
            min_interval=1, user='test')
        result = self.execute_graphite(graphite_api)
        tsl = result['_'].groups['_']
        self.assertEqual(1, len(tsl))
        for ts in tsl:
            self.assertEqual(27, len(ts.points))
            self.assertEqual('clusters.cl_3.nd_3.Diamond.user.percent',
                             ts.series_id)

    def test_raw_data_caching(self) -> None:
        """
        Test whether raw data caching works as expected
        :return:
        """
        self.set_druid_dimensions(7)
        DruidReader.execute = TestGraphiteApi.druid_execute
        QueryCache.inst = None
        QueryCache.inst = mock.MagicMock()

        # We need a query having a function forcing the use of raw data
        # caching. nPercentile() is one of those functions
        query = ImmutableMultiDict({
            'target': 'nPercentile(clusters.*.*.Diamond.*.user.percent, 95)',
            'from': '-6h',
            'until': 'now',
            'maxDataPoints': '100'
        })
        # TEST 1: Total miss
        QueryCache.inst.get.return_value = None
        graphite_api = GraphiteApi(self.schema, query, min_interval=1,
                                   user='test')
        result = self.execute_graphite(graphite_api)
        # Since this was a total miss, we should have fetched data from Druid
        # and updated the cache
        self.assertEqual(1, QueryCache.inst.set.call_count)

        # TEST 2: Total hit
        QueryCache.inst.get.return_value = [
            QueryCacheEntry(TimeRange(10, 50, 10), result)]
        QueryCache.inst.prune.return_value = QueryCacheResult(
            result, missed_ranges=[])
        graphite_api = GraphiteApi(self.schema, query, min_interval=1,
                                   user='test')
        _ = self.execute_graphite(graphite_api)
        # Since this was a total hit, the Druid execute() call count should not
        # change and QueryCache set should not be called
        self.assertEqual(1, QueryCache.inst.set.call_count)

        # TEST 3: Partial hit
        test_missed_ranges = [TimeRange(10, 30, 10), TimeRange(30, 50, 10)]
        QueryCache.inst.get.return_value = [
            QueryCacheEntry(TimeRange(0, 10, 10), result)]
        QueryCache.inst.prune.return_value = QueryCacheResult(
            result, missed_ranges=test_missed_ranges)

        graphite_api = GraphiteApi(self.schema, query, min_interval=1,
                                   user='test')
        _ = self.execute_graphite(graphite_api)
        # Since this was a partial hit, we should be updating the cache one
        # more time
        self.assertEqual(2, QueryCache.inst.set.call_count)
        # Verify that the missing ranges were stored correctly in the cache
        self.assertEqual(
            test_missed_ranges[0],
            QueryCache.inst.merge.mock_calls[-2][1][1].time_range)
        self.assertEqual(
            test_missed_ranges[1],
            QueryCache.inst.merge.mock_calls[-1][1][1].time_range)
