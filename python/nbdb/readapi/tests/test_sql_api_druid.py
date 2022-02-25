"""
unittests for sqlapi
"""
import os
import time
from collections import namedtuple
from typing import Dict, List
from unittest import TestCase
from unittest.mock import Mock, patch

import cProfile
import moz_sql_parser
import numpy as np

from nbdb.readapi.sql_parser import FillFunc
from nbdb.readapi.time_series_response import TimeSeriesGroup, TimeSeries, \
    TimeRange

from nbdb.common.telemetry import Telemetry

from nbdb.readapi.druid_reader import DruidReader
from nbdb.readapi.query_cache import QueryCache

from nbdb.schema.schema import Schema

from nbdb.common.thread_pools import ThreadPools
from nbdb.config.settings import Settings
from nbdb.readapi.sql_api import SqlApi


# pylint: disable-msg=R0903  # Too Few Public Methods
# We are mocking this class, so don't need to implement all methods
class DruidResult:
    """
    Replicates the Druid api cursor behavior
    for mocking the result
    """

    def __init__(self, result):
        """
        Initialize
        :param result: generator of rows
        """
        self._result = result

    def fetchone(self):
        """
        Return the next row in the generator
        :return:
        """
        try:
            row = next(self._result)
            return row
        except StopIteration:
            return None

    def fetchmany(self, size=None):
        """
        Return the next row in the generator
        :return:
        """
        # Ignore req size & return 1 row in testing env
        del size
        row = self.fetchone()
        return [row] if row else None


class TestSqlApiDruid(TestCase):
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
    def row(column_names, column_values) -> Dict:
        """Create dict mimicking JSON object"""
        return dict(zip(column_names, column_values))

    @staticmethod
    def _query_result(query):
        """
        Returns a generator of rows based on the query
        Mocks the behavior of a druid client
        Validates that the query is valid sql
        :param query:
        :return:
        """
        # Validate the query, it should be valid SQL
        # Moz-parser doesn't understand druid methods
        # MILLIS_TO_TIMESTAMP OR TIMESTAMP_TO_MILLIS
        # so we replace these with MAX just to ensure
        # parser can validate the rest of the SQL
        moz_sql_parser.parse(
            query.replace('MILLIS_TO_TIMESTAMP', 'MAX').
            replace('TIMESTAMP_TO_MILLIS', 'MAX'))

        column_names = ["ts", "node", "value"]
        # check if its the Last value query
        if 'LATEST' in query:
            # last value query, we should only return one row per series
            yield TestSqlApiDruid.row(column_names, [10000, 'n1', 20])
            yield TestSqlApiDruid.row(column_names, [5000, 'n2', 30])
        else:
            yield TestSqlApiDruid.row(column_names, [20000, 'n1', 30])
            yield TestSqlApiDruid.row(column_names, [25000, 'n1', 40])
            yield TestSqlApiDruid.row(column_names, [15000, 'n2', 40])
            yield TestSqlApiDruid.row(column_names, [35000, 'n2', 50])

    @staticmethod
    def _query_perf_result(query):
        """
        Returns a large number of rows based
        Mocks the behavior of a druid client
        :param query:
        :return:
        """
        column_names = ["ts", "node", "value"]
        # check if its the Last value query
        if 'LATEST' in query:
            # last value query, we should only return one row per series
            yield TestSqlApiDruid.row(column_names, [10*1000, 'n1', 10])
            yield TestSqlApiDruid.row(column_names, [5*1000, 'n2', 30])
        else:
            for i in range(10, 40000):
                yield TestSqlApiDruid.row(column_names, [i*1000, 'n1', 30 + i])
                yield TestSqlApiDruid.row(column_names, [i*1000, 'n2', 50 + i])

    @staticmethod
    def _query_like_result(query):
        column_names = ["ts", "node", "value"]
        # check if its the Last value query
        if 'LATEST' in query:
            # last value query, we should only return one row per series
            for c in range(2):
                for n in range(2):
                    yield TestSqlApiDruid.row(
                        column_names,
                        [10*1000, '{}.{}.Diamond.user.percent'.format(c, n),
                         10])
        else:
            for i in range(10, 400):
                for c in range(2):
                    for n in range(2):
                        yield TestSqlApiDruid.row(
                            column_names,
                            [i * 1000,
                             '{}.{}.Diamond.user.percent'.format(c, n),
                             30 + i + c + n])

    def druid_execute(self, query, datasource, user):
        """
        Mocks the Druid execute query
        :param query:
        :return:
        """
        _, _, _ = self, datasource, user
        if 'Diamond.user.percent' in query:
            return DruidResult(TestSqlApiDruid._query_like_result(query))
        if 'performance' in query:
            return DruidResult(TestSqlApiDruid._query_perf_result(query))
        return DruidResult(TestSqlApiDruid._query_result(query))

    def test_simple_sql(self) -> None:
        """
        Test simple sql
        :return:
        """
        DruidReader.inst.druid_schema.get_dimensions.return_value = ['node']
        DruidReader.execute = TestSqlApiDruid.druid_execute
        sql_api = SqlApi(self.schema,
                         "SELECT "
                         "measurement_rather_really_long_measurement_name_1."
                         "field_rather_really_long_field_name_too_1 "
                         "FROM m "
                         "WHERE "
                         "cluster = 'cluster-uuid-x-y-z' AND"
                         " time > 1583092800 AND"
                         " time < 1584169200",
                         user='test')
        result = sql_api.execute_influx(epoch_precision='ms')
        self.assertTrue(result is not None)

    def test_simple_sql_with_default_group(self) -> None:
        """
        Test simple sql
        :return:
        """
        DruidReader.inst.druid_schema.get_dimensions.return_value = ['node']
        DruidReader.execute = TestSqlApiDruid.druid_execute
        sql_api = SqlApi(
            self.schema,
            "SELECT "
            "mean(measurement_rather_really_long_measurement_name_1."
            "field_rather_really_long_field_name_too_1) as f "
            "FROM m "
            "WHERE "
            "cluster = 'cluster-uuid-x-y-z' AND"
            " time > 1583092800 AND"
            " time < 1584169200 group by time('2d')",
            user='test')
        result = sql_api.execute_internal()
        aligned_start = int(1583092800/172800)*172800
        aliend_end = int(1584169200/172800)*172800
        self.assertTrue(list(zip(range(aligned_start, aliend_end, 172800),
                                 [45]*7)),
                        result['f'].groups['_'][0].points)

    def test_multi_field_sql_with_filters_and_groups(self) -> None:
        """
        test the group by clause
        """
        DruidReader.inst.druid_schema.get_dimensions.return_value = ['node']
        DruidReader.execute = TestSqlApiDruid.druid_execute

        sql_api = SqlApi(self.schema,
                         "select mean(machine.user) as mu from tsdb"
                         " where time>=10 and time < 40 and"
                         " (node='n1' or node='n2') group by time(5),node",
                         user='test')
        result = sql_api.execute_internal()
        self.assertEqual([(10, 20.0), (15, 20.0), (20, 30.0),
                          (25, 40.0), (30, 40.0), (35, 40.0)],
                         result['mu'].groups['node:n1'][0].points)
        self.assertEqual([(10, 30.0), (15, 40.0), (20, 40.0),
                          (25, 40.0), (30, 40.0), (35, 50.0)],
                         result['mu'].groups['node:n2'][0].points)

    def test_constants_sql(self) -> None:
        """
        Operates on constants only and not time-series
        """
        # Get a constant literal back
        sql_api = SqlApi(self.schema,
                         'select 10 as f '
                         'from m where time>0 group by time(1)',
                         user='test')
        result = sql_api.execute_internal()
        self.assertEqual((0, 10), result['f']['_'][0][0])

        # + operator
        sql_api = SqlApi(self.schema,
                         'select 10+20 as f '
                         'from m where time>0 group by time(1)',
                         user='test')
        result = sql_api.execute_internal()
        self.assertEqual([(0, 30)], result['f']['_'][0].points)

        # - operator
        sql_api = SqlApi(self.schema,
                         'select 10-20 as f '
                         'from m where time>0 group by time(1)',
                         user='test')
        result = sql_api.execute_internal()
        self.assertEqual([(0, -10)], result['f']['_'][0].points)

        # * operator
        sql_api = SqlApi(self.schema,
                         'select 10*20 as f '
                         'from m where time>0 group by time(1)',
                         user='test')
        result = sql_api.execute_internal()
        self.assertEqual([(0, 200)], result['f']['_'][0].points)

        # / operator
        sql_api = SqlApi(self.schema,
                         'select 20/10 as f '
                         'from m where time>0 group by time(1)',
                         user='test')
        result = sql_api.execute_internal()
        self.assertEqual([(0, 2)], result['f']['_'][0].points)

        # multiple fields
        sql_api = SqlApi(self.schema,
                         'select 1 as f1, 2 as f2 '
                         'from m where time>0 group by time(1)',
                         user='test')
        result = sql_api.execute_internal()
        self.assertEqual((0, 1), result['f1']['_'][0][0])
        self.assertEqual((0, 2), result['f2']['_'][0][0])

        # operating on time strings
        sql_api = SqlApi(self.schema,
                         'select \'1h\'/10 as f1 '
                         'from m where time>0 group by time(1)',
                         user='test')
        result = sql_api.execute_internal()
        self.assertEqual((0, 360.0), result['f1']['_'][0][0])

    def test_numpy_methods(self) -> None:
        """
        Test the numpy methods are callable
        """
        sql_api = SqlApi(self.schema,
                         'select sin(10) as f from tsdb '
                         'where time>0 group by time(1)',
                         user='test')
        result = sql_api.execute_internal()
        self.assertEqual((0, np.sin(10)), result['f']['_'][0][0])

    def test_unsupported_method(self) -> None:
        """
        Verify unknown methods raise an error
        :return:
        """
        sql_api = SqlApi(self.schema,
                         'select invalid(10) as f from tsdb'
                         ' where time> 0 group by time(1)',
                         user='test')
        self.assertRaises(ValueError, sql_api.execute_internal)

    @patch('nbdb.readapi.sql_api.DruidReader')
    def test_multi_arg_numpy_methods(self, mock_sparse_series_reader) -> None:
        """
        test numpy methods that require multiple arguments
        """
        sql_api = SqlApi(self.schema,
                         'select percentile("user", 70) as f from tsdb'
                         ' where time> 0 group by time(1)',
                         user='test')
        mock_sparse_series_reader.inst.get_field.return_value = \
            TimeSeriesGroup({'_': [
                TimeSeries('s1', [(10, 1), (20, 2)], TimeRange(10, 30, 10)),
                TimeSeries('s2', [(10, 2), (20, 3)], TimeRange(10, 30, 10)),
                TimeSeries('s3', [(10, 3), (20, 4)], TimeRange(10, 30, 10)),
                TimeSeries('s4', [(10, 4), (20, 5)], TimeRange(10, 30, 10)),
                TimeSeries('s5', [(10, 5), (20, 6)], TimeRange(10, 30, 10)),
                TimeSeries('s6', [(10, 6), (20, 7)], TimeRange(10, 30, 10)),
                TimeSeries('s7', [(10, 7), (20, 8)], TimeRange(10, 30, 10)),
                TimeSeries('s8', [(10, 8), (20, 9)], TimeRange(10, 30, 10)),
                TimeSeries('s9', [(10, 9), (20, 10)], TimeRange(10, 30, 10)),
                TimeSeries('s10', [(10, 10), (20, 11)], TimeRange(10, 30, 10)),
            ]})
        result = sql_api.execute_internal()
        self.assertEqual((10,
                          np.percentile([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 70)),
                         result['f']['_'][0][0])
        self.assertEqual((20,
                          np.percentile([2, 3, 4, 5, 6, 7, 8, 9, 10, 11], 70)),
                         result['f']['_'][0][1])

    @patch('nbdb.readapi.sql_api.DruidReader')
    def test_field_sql(self, mock_sparse_series_reader) -> None:
        """
        Operates on fields that require fetching time-series
        """
        mock_sparse_series_reader.inst.get_field.return_value = \
            TimeSeriesGroup({'_': [
                TimeSeries('s1', [(10, 20), (20, 0), (30, 30)],
                           TimeRange(10, 40, 10))
            ]})

        sql_api = SqlApi(self.schema,
                         'select "user" from tsdb where time>0 '
                         'group by time(1)',
                         user='test')
        result = sql_api.execute_internal()
        self.assertEqual(1, len(result['user'].groups['_']))
        self.assertEqual([(10, 20), (20, 0), (30, 30)],
                         result['user'].groups['_'][0].points)

    # pylint: disable-msg=R0913  # Too many arguments
    # pylint: disable-msg=W0613  # Unused Argument
    def get_field(self,
                  schema: Schema,
                  datasource: str,
                  field: str,
                  filters: dict,
                  time_range: TimeRange,
                  groupby: List[str],
                  query_id: str,
                  trace: bool,
                  fill_func: FillFunc,
                  fill_value: float,
                  create_flat_series_id: bool,
                  sparseness_disabled: bool,
                  user: str = None
                  ) -> TimeSeriesGroup:
        """
        Get the time-series associated with the field grouped by the time
        interval provided
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
        # The mock function doesn't use these params
        _ = self
        _ = schema
        _ = datasource
        _ = filters
        _ = time_range
        _ = groupby
        _ = query_id
        _ = trace
        _ = fill_func
        _ = fill_value
        _ = create_flat_series_id
        _ = sparseness_disabled

        if field == 'user':
            return TimeSeriesGroup({'_': [
                TimeSeries('s1', [(10, 1), (20, 2), (30, 3)],
                           TimeRange(10, 40, 10))
            ]})
        if field == 'system':
            return TimeSeriesGroup({'_': [
                TimeSeries('s2', [(10, 3), (20, 6), (30, 9)],
                           TimeRange(10, 40, 10))
            ]})
        raise ValueError('Unsupported field {} in the get_field mock'.
                         format(field))

    @patch('nbdb.readapi.sql_api.DruidReader')
    def test_multi_field_sql(self, mock_sparse_series_reader) -> None:
        """
        Operates on fields that require fetching time-series
        """
        mock_sparse_series_reader.inst.get_field.side_effect = self.get_field
        sql_api = SqlApi(self.schema,
                         'select "user", "system" from tsdb'
                         ' where time>0 group by time(1)',
                         user='test')
        result = sql_api.execute_internal()
        self.assertEqual(TimeSeries('s1', [(10, 1), (20, 2), (30, 3)],
                                    TimeRange(10, 40, 10)),
                         result['user']['_'][0])
        self.assertEqual(TimeSeries('s2', [(10, 3), (20, 6), (30, 9)],
                                    TimeRange(10, 40, 10)),
                         result['system']['_'][0])

    @patch('nbdb.readapi.sql_api.DruidReader')
    def test_complex_multi_field_expression(self, mock_sparse_series_reader)\
            -> None:
        """
        Executes a complex expression evaluation on multiple fields
        that require fetching time-series
        """
        mock_sparse_series_reader.inst.get_field.side_effect = self.get_field
        sql_api = SqlApi(self.schema,
                         'select system*100/"user"+20 as f1'
                         ' from tsdb'
                         ' where time>0 group by time(1)',
                         user='test')
        result = sql_api.execute_internal()
        self.assertEqual([(10, 320), (20, 320), (30, 320)],
                         result['f1']['_'][0].points)

    @patch('nbdb.readapi.sql_api.DruidReader')
    def test_numpy_methods_on_fields(self, mock_sparse_series_reader) -> None:
        """
        Test calling numpy methods
        :param mock_sparse_series_reader:
        """
        mock_sparse_series_reader.inst.get_field.side_effect = self.get_field
        sql_api = SqlApi(self.schema,
                         'select t.sin("user") as f2 from tsdb '
                         'where time>0 group by time(1)',
                         user='test')
        result = sql_api.execute_internal()
        self.assertEqual([(x[0], np.sin(x[1])) for x in [(10, 1),
                                                         (20, 2),
                                                         (30, 3)]],
                         result['f2']['_'][0].points)

    @patch('nbdb.readapi.sql_api.DruidReader')
    def test_multi_series_field_sql(self, mock_sparse_series_reader) -> None:
        """
        test returning a multi-series field without any aggregation
        """
        mock_sparse_series_reader.inst.get_field.return_value = \
            TimeSeriesGroup({'_': [
                TimeSeries('s1', [(10, 100), (20, 200), (30, 300)],
                           TimeRange(10, 30, 10)),
                TimeSeries('s1', [(10, 20), (20, 0), (30, 30)],
                           TimeRange(10, 30, 10)),
                TimeSeries('s1', [(10, 10), (20, 10), (30, 10)],
                           TimeRange(10, 30, 10))
            ]})

        sql_api = SqlApi(self.schema,
                         'select "user" from tsdb where time>0'
                         ' group by time(1)',
                         user='test')
        result = sql_api.execute_internal()
        self.assertEqual([(10, 100), (20, 200), (30, 300)],
                         result['user']['_'][0].points)
        self.assertEqual([(10, 20), (20, 0), (30, 30)],
                         result['user']['_'][1].points)
        self.assertEqual([(10, 10), (20, 10), (30, 10)],
                         result['user']['_'][2].points)

    @patch('nbdb.readapi.sql_api.DruidReader')
    def test_multi_series_np_functions_sql(self, mock_sparse_series_reader)\
            -> None:
        """
        Test numpy method that operate on multiple series
        """
        mock_sparse_series_reader.inst.get_field.return_value = \
            TimeSeriesGroup({'_': [
                TimeSeries('s1', [(10, 20), (20, 10), (30, 30)],
                           TimeRange(10, 40, 10)),
                TimeSeries('s1', [(10, 10), (20, 10), (30, 10)],
                           TimeRange(10, 40, 10)),
                TimeSeries('s1', [(10, 5), (20, 10), (30, 20)],
                           TimeRange(10, 40, 10)),
                TimeSeries('s1', [(10, 5), (20, 10), (30, 20)],
                           TimeRange(10, 40, 10))
            ]})

        sql_api = SqlApi(self.schema,
                         'select mean("user") as mu from tsdb'
                         ' where time>0 group by time(1)',
                         user='test')
        result = sql_api.execute_internal()
        self.assertEqual([(10, 10), (20, 10), (30, 20)],
                         result['mu']['_'][0].points)

        sql_api = SqlApi(self.schema,
                         'select sum("user") as mu from tsdb'
                         ' where time>0 group by time(1)',
                         user='test')
        result = sql_api.execute_internal()
        self.assertEqual([(10, 40), (20, 40), (30, 80)],
                         result['mu']['_'][0].points)

        sql_api = SqlApi(self.schema,
                         'select std("user") as mu from tsdb'
                         ' where time>0 group by time(1)',
                         user='test')
        result = sql_api.execute_internal()
        self.assertEqual([(10, 6.123724356957945),
                          (20, 0.0),
                          (30, 7.0710678118654755)],
                         result['mu']['_'][0].points)

    @patch('nbdb.readapi.sql_api.DruidReader')
    def test_groups(self, mock_sparse_series_reader) -> None:
        """
        test the group by clause
        """
        mock_sparse_series_reader.inst.get_field.return_value = \
            TimeSeriesGroup({
                'n1': [
                    TimeSeries('s1', [(10, 20), (20, 10), (30, 30)],
                               TimeRange(10, 40, 10)),
                    TimeSeries('s2', [(10, 10), (20, 10), (30, 10)],
                               TimeRange(10, 40, 10))
                ],
                'n2': [
                    TimeSeries('s1', [(10, 20), (20, 10), (30, 30)],
                               TimeRange(10, 40, 10)),
                    TimeSeries('s2', [(10, 10), (20, 10), (30, 10)],
                               TimeRange(10, 40, 10))
                ],
                'n3': [
                    TimeSeries('s1', [(10, 20), (20, 10), (30, 30)],
                               TimeRange(10, 40, 10)),
                    TimeSeries('s2', [(10, 10), (20, 10), (30, 10)],
                               TimeRange(10, 40, 10))
                ]
            })

        sql_api = SqlApi(self.schema,
                         'select mean("user") as mu from tsdb'
                         ' where time>0 group by time(1),node',
                         user='test')
        result = sql_api.execute_internal()
        self.assertEqual([(10, 15.0), (20, 10.0), (30, 20.0)],
                         result['mu']['n1'][0].points)
        self.assertEqual([(10, 15.0), (20, 10.0), (30, 20.0)],
                         result['mu']['n2'][0].points)
        self.assertEqual([(10, 15.0), (20, 10.0), (30, 20.0)],
                         result['mu']['n3'][0].points)

    @patch('nbdb.readapi.sql_api.DruidReader')
    def test_timeshift(self, mock_sparse_series_reader) -> None:
        """
        test the time shift method
        :param mock_sparse_series_reader:
        """
        mock_sparse_series_reader.inst.get_field.return_value = \
            TimeSeriesGroup({'_': [
                TimeSeries('s1', [(10, 3), (20, 6), (30, 9)],
                           TimeRange(10, 30, 10)),
                TimeSeries('s2', [(10, 4), (20, 7), (30, 10)],
                           TimeRange(10, 30, 10)),
                TimeSeries('s3', [(10, 5), (20, 8), (30, 11)],
                           TimeRange(10, 30, 10)),
            ]})

        sql_api = SqlApi(self.schema,
                         'select timeshift("user", \'1h\' ) as mu from tsdb'
                         ' where time>=50 and time < 100 group by time(10)',
                         user='test')
        result = sql_api.execute_internal()
        self.assertEqual([(3610, 3.0), (3620, 6.0), (3630, 9.0)],
                         result['mu']['_'][0].points)
        self.assertEqual([(3610, 4.0), (3620, 7.0), (3630, 10.0)],
                         result['mu']['_'][1].points)
        self.assertEqual([(3610, 5.0), (3620, 8.0), (3630, 11.0)],
                         result['mu']['_'][2].points)

        sql_api = SqlApi(self.schema,
                         'select timeshift("user", -2) as mu from tsdb '
                         'where time>=10 and time <= 30 group by time(10)',
                         user='test')
        result = sql_api.execute_internal()
        self.assertEqual([(8, 3.0), (18, 6.0), (28, 9.0)],
                         result['mu']['_'][0].points)
        self.assertEqual([(8, 4.0), (18, 7.0), (28, 10.0)],
                         result['mu']['_'][1].points)
        self.assertEqual([(8, 5.0), (18, 8.0), (28, 11.0)],
                         result['mu']['_'][2].points)

    @patch('nbdb.readapi.sql_api.DruidReader')
    def test_complex_timeshift(self, mock_sparse_series_reader) -> None:
        """
        Test timeshift method when applied to some fields
        """
        mock_sparse_series_reader.inst.get_field.return_value = \
            TimeSeriesGroup({'_': [
                TimeSeries('s1', [(10, 3.0), (20, 6.0), (30, 9.0)],
                           TimeRange(10, 30, 10)),
            ]})

        sql_api = SqlApi(self.schema,
                         'select "user"/timeshift("user", -10) as mu from tsdb '
                         'where time>=10 and time <= 30 group by time(10)',
                         user='test')
        result = sql_api.execute_internal()
        self.assertEqual([(10, 3/6), (20, 6/9)],
                         result['mu']['_'][0].points)

    @patch('nbdb.readapi.sql_api.DruidReader')
    def test_execute_influx(self, mock_sparse_series_reader) -> None:
        """
        Test the execute_influx method with a multi-series response
        :param mock_sparse_series_reader:
        """
        mock_sparse_series_reader.inst.get_field.return_value = \
            TimeSeriesGroup({'_': [
                TimeSeries('s1', [(10, 3.0), (20, 6.0), (30, 9.0)],
                           TimeRange(10, 30, 10)),
                TimeSeries('s2', [(10, 4.0), (20, 7.0), (30, 10.0)],
                           TimeRange(10, 30, 10)),
                TimeSeries('s3', [(10, 5.0), (20, 8.0), (30, 11.0)],
                           TimeRange(10, 30, 10)),
            ]})
        sql_api = SqlApi(self.schema,
                         'select "user" as mu from tsdb '
                         'where time>=10 and time <= 30 group by time(10)',
                         user='test')
        result = sql_api.execute_influx('s')
        # The series name in the InfluxQL response consists of the field name
        # and any functions on top of them
        self.assertEqual('mu',
                         result.response['results'][0]['series'][0]['name'])
        self.assertEqual([(10, 3), (20, 6), (30, 9)],
                         result.response['results'][0]['series'][0]['values'])
        self.assertEqual('mu',
                         result.response['results'][0]['series'][1]['name'])
        self.assertEqual([(10, 4), (20, 7), (30, 10)],
                         result.response['results'][0]['series'][1]['values'])
        self.assertEqual('mu',
                         result.response['results'][0]['series'][2]['name'])
        self.assertEqual([(10, 5), (20, 8), (30, 11)],
                         result.response['results'][0]['series'][2]['values'])

    @patch('nbdb.readapi.sql_api.DruidReader')
    def test_function_gradient(self, mock_sparse_series_reader) -> None:
        """
        test returning a multi-series field without any aggregation
        """
        tsl = list()
        # generate series with changing gradients
        for s in range(1, 4):
            points = list()
            v = 1
            time_range = TimeRange(10, 1000, 10)
            # gradient=10
            for e in range(time_range.start, 500, time_range.interval):
                points.append((e, v))
                v = v + 10*s
            # negative gradient
            for e in range(500, 600, 10):
                points.append((e, v))
                v = v - 10
            # gradient=20
            for e in range(600, time_range.end, time_range.interval):
                points.append((e, v))
                v = v + 20*s
            tsl.append(TimeSeries('s' + str(s), points, time_range))

        mock_sparse_series_reader.inst.get_field.return_value = \
            TimeSeriesGroup({'_': tsl})

        sql_api = SqlApi(self.schema,
                         'select t.clip(t.gradient("user"), 0, None) as g'
                         ' from tsdb where time>0'
                         ' group by time(1)',
                         user='test')
        result = sql_api.execute_internal()
        self.assertEqual(3, len(result['g'].groups['_']))
        for s in range(1, 4):
            ts = result['g'].groups['_'][s-1]
            self.assertEqual('clip(gradient(s{}))'.format(s), ts.series_id)
            for e in range(time_range.start, 500, time_range.interval):
                self.assertEqual(10*s, ts.get_epoch_value(e))
            for e in range(510, 600, time_range.interval):
                self.assertEqual(0, ts.get_epoch_value(e))
            for e in range(610, time_range.end, time_range.interval):
                self.assertEqual(20*s, ts.get_epoch_value(e))

    @patch('nbdb.readapi.sql_api.DruidReader')
    def test_function_integral(self, mock_sparse_series_reader) -> None:
        """
        test returning a multi-series field without any aggregation
        """
        mock_sparse_series_reader.inst.get_field.return_value = \
            TimeSeriesGroup({'_': [
                TimeSeries('s1',
                           list(zip(range(10, 100, 10), [3]*10)),
                           TimeRange(10, 100, 10)),
            ]})

        sql_api = SqlApi(self.schema,
                         'select t.cumsum("user") as g'
                         ' from tsdb where time>0'
                         ' group by time(1)',
                         user='test')
        result = sql_api.execute_internal()
        self.assertEqual(list(zip(range(10, 100, 10),
                                  range(3, 30, 3))),
                         result['g'].groups['_'][0].points)

    @patch('nbdb.readapi.sql_api.DruidReader')
    def test_function_diff(self, mock_sparse_series_reader) -> None:
        """
        test returning the results of diff() on a multi-series query
        """
        tsl = list()
        # generate series with changing deltas
        for s in range(1, 4):
            points = list()
            v = 1
            time_range = TimeRange(10, 1000, 10)
            # delta=10
            for e in range(time_range.start, 500, time_range.interval):
                points.append((e, v))
                v = v + 10*s
            # negative delta
            for e in range(500, 600, 10):
                points.append((e, v))
                v = v - 10
            # delta=20
            for e in range(600, time_range.end, time_range.interval):
                points.append((e, v))
                v = v + 20*s
            tsl.append(TimeSeries('s' + str(s), points, time_range))

        mock_sparse_series_reader.inst.get_field.return_value = \
            TimeSeriesGroup({'_': tsl})

        sql_api = SqlApi(self.schema,
                         'select t.clip(t.diff("user"), 0, None) as g'
                         ' from tsdb where time>0'
                         ' group by time(1)',
                         user='test')
        result = sql_api.execute_internal()
        self.assertEqual(3, len(result['g'].groups['_']))
        for s in range(1, 4):
            ts = result['g'].groups['_'][s-1]
            self.assertEqual('clip(diff(s{}))'.format(s), ts.series_id)
            # Since numpy.diff() generates one less value after transformation,
            # the value corresponding to the first epoch should be NULL
            self.assertEqual(None, ts.get_epoch_value(time_range.start))

            # Verify values after the first epoch. They should not be NULL
            for e in range(time_range.start + time_range.interval, 500,
                           time_range.interval):
                self.assertEqual(10*s, ts.get_epoch_value(e))
            for e in range(510, 600, time_range.interval):
                self.assertEqual(0, ts.get_epoch_value(e))
            for e in range(610, time_range.end, time_range.interval):
                self.assertEqual(20*s, ts.get_epoch_value(e))


    # pylint: disable-msg=R0913 # Too many arguments
    def get_counter_fields(self,
                           schema: Schema,
                           datasource: str,
                           field: str,
                           filters: dict,
                           time_range: TimeRange,
                           groupby: List[str],
                           query_id: str,
                           trace: bool,
                           fill_func: FillFunc,
                           fill_value: float,
                           create_flat_series_id: bool,
                           sparseness_disabled: bool,
                           user: str = None
                           ) -> TimeSeriesGroup:
        """
        Get the time-series associated with the field grouped by the time
        interval provided
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
        # The mock function doesn't use these params
        _ = self
        _ = schema
        _ = datasource
        _ = filters
        _ = time_range
        _ = groupby
        _ = query_id
        _ = trace
        _ = fill_func
        _ = fill_value
        _ = create_flat_series_id
        _ = sparseness_disabled

        if field == 'enqueue':
            return TimeSeriesGroup({'_': [
                TimeSeries('s1',
                           list(zip(range(10, 100, 10), range(5, 50, 5))),
                           TimeRange(10, 100, 10)),
                TimeSeries('s2',
                           list(zip(range(10, 100, 10), range(6, 60, 6))),
                           TimeRange(10, 100, 10))
            ]})
        if field == 'dequeue':
            return TimeSeriesGroup({'_': [
                TimeSeries('s1',
                           list(zip(range(10, 100, 10), range(3, 30, 3))),
                           TimeRange(10, 100, 10)),
                TimeSeries('s2',
                           list(zip(range(10, 100, 10), range(2, 20, 2))),
                           TimeRange(10, 100, 10))
            ]})
        raise ValueError('Unsupported field {} in the get_field mock'.
                         format(field))

    @patch('nbdb.readapi.sql_api.DruidReader')
    def test_function_subtract_series(self, mock_sparse_series_reader) -> None:
        """
        test returning a multi-series field without any aggregation
        """
        mock_sparse_series_reader.inst.get_field = self.get_counter_fields

        sql_api = SqlApi(self.schema,
                         'select t.subtract("enqueue", "dequeue") as g'
                         ' from tsdb where time>0'
                         ' group by time(1)',
                         user='test')
        result = sql_api.execute_internal()
        self.assertEqual(2, len(result['g'].groups['_']))
        self.assertEqual(list(zip(range(10, 100, 10),
                                  range(2, 20, 2))),
                         result['g'].groups['_'][0].points)
        self.assertEqual(list(zip(range(10, 100, 10),
                                  range(4, 40, 4))),
                         result['g'].groups['_'][1].points)

    @patch('nbdb.readapi.sql_api.DruidReader')
    def test_function_divide_series(self, mock_sparse_series_reader) -> None:
        """
        test returning a multi-series field without any aggregation
        """
        mock_sparse_series_reader.inst.get_field = self.get_counter_fields

        sql_api = SqlApi(self.schema,
                         'select t.divide("enqueue", "dequeue") as g'
                         ' from tsdb where time>0'
                         ' group by time(1)',
                         user='test')
        result = sql_api.execute_internal()
        self.assertEqual(2, len(result['g'].groups['_']))
        self.assertEqual(list(zip(range(10, 100, 10),
                                  [5/3]*10)),
                         result['g'].groups['_'][0].points)
        self.assertEqual(list(zip(range(10, 100, 10),
                                  [6/2]*10)),
                         result['g'].groups['_'][1].points)

    @patch('nbdb.readapi.sql_api.DruidReader')
    def test_function_as_percentage(self, mock_sparse_series_reader) -> None:
        """
        test returning a multi-series field without any aggregation
        """
        mock_sparse_series_reader.inst.get_field.return_value = \
            TimeSeriesGroup({'_': [
                TimeSeries('s1',
                           list(zip(range(10, 110, 10),
                                    [20, 20, 20, 20, 20, 40, 10, 10, 20, 20])),
                           TimeRange(10, 110, 10)),
            ]})

        sql_api = SqlApi(self.schema,
                         'select t.as_percentage("enqueue") as g'
                         ' from tsdb where time>0'
                         ' group by time(1)',
                         user='test')
        result = sql_api.execute_internal()
        self.assertEqual(1, len(result['g'].groups['_']))
        self.assertEqual(list(zip(range(10, 110, 10),
                                  [10, 10, 10, 10, 10, 20, 5, 5, 10, 10])),
                         result['g'].groups['_'][0].points)

    @patch('nbdb.readapi.sql_api.DruidReader')
    def test_function_count(self, mock_sparse_series_reader) -> None:
        """
        test returning a multi-series field without any aggregation
        """
        tsl = list()
        for s in range(1, 1000):
            tsl.append(TimeSeries('s' + str(s),
                                  list(zip(range(10, 1000, 10),
                                           range(s, s*100, s))),
                                  TimeRange(10, 1000, 10)))

        mock_sparse_series_reader.inst.get_field.return_value = \
            TimeSeriesGroup({'_': tsl})

        sql_api = SqlApi(self.schema,
                         'select count("enqueue") as g'
                         ' from tsdb where time>0'
                         ' group by time(1)',
                         user='test')
        result = sql_api.execute_internal()
        self.assertEqual(1, len(result['g'].groups['_']))
        self.assertEqual(list(zip(range(10, 1000, 10),
                                  [999]*100)),
                         result['g'].groups['_'][0].points)

    @patch('nbdb.readapi.sql_api.DruidReader')
    def test_function_abs(self, mock_sparse_series_reader) -> None:
        """
        test returning a multi-series field without any aggregation
        """
        tsl = list()
        for s in range(1, 3):
            tsl.append(TimeSeries('s' + str(s),
                                  list(zip(range(10, 110, 10),
                                           range(-10, 10, 2))),
                                  TimeRange(10, 110, 10)))

        mock_sparse_series_reader.inst.get_field.return_value = \
            TimeSeriesGroup({'_': tsl})

        sql_api = SqlApi(self.schema,
                         'select t.abs("enqueue") as g'
                         ' from tsdb where time>0'
                         ' group by time(1)',
                         user='test')
        result = sql_api.execute_internal()
        self.assertEqual(2, len(result['g'].groups['_']))
        self.assertEqual(list(zip(range(10, 110, 10),
                                  [10, 8, 6, 4, 2, 0, 2, 4, 6, 8, 10])),
                         result['g'].groups['_'][0].points)

    @patch('nbdb.readapi.sql_api.DruidReader')
    def test_function_shift_series(self, mock_sparse_series_reader) -> None:
        """
        test returning a multi-series field without any aggregation
        """
        mock_sparse_series_reader.inst.get_field.return_value = \
            TimeSeriesGroup({'_': [
                TimeSeries('s1',
                           list(zip(range(10, 100, 10), range(2, 20, 2))),
                           TimeRange(10, 100, 10)),
            ]})
        sql_api = SqlApi(self.schema,
                         'select t.shift("user", 3) as g'
                         ' from tsdb where time>0'
                         ' group by time(1)',
                         user='test')
        result = sql_api.execute_internal()
        self.assertEqual(1, len(result['g'].groups['_']))
        self.assertEqual(list(zip(range(10, 100, 10),
                                  [0]*3 + list(range(2, 14, 2)))),
                         result['g'].groups['_'][0].points)

        sql_api = SqlApi(self.schema,
                         'select t.shift("user", -3) as g'
                         ' from tsdb where time>0'
                         ' group by time(1)',
                         user='test')
        result = sql_api.execute_internal()
        self.assertEqual(1, len(result['g'].groups['_']))
        self.assertEqual(list(zip(range(10, 100, 10),
                                  list(range(8, 20, 2)) + [0]*3)),
                         result['g'].groups['_'][0].points)

    @patch('nbdb.readapi.sql_api.DruidReader')
    def test_function_topk_series(self, mock_sparse_series_reader) -> None:
        """
        test top k method
        """
        _ = self
        tsl = list()
        for s in range(1, 10):
            tsl.append(TimeSeries('s' + str(s),
                                  list(zip(range(10, 1000, 10),
                                           range(s, s*100, s))),
                                  TimeRange(10, 1000, 10)))

        mock_sparse_series_reader.inst.get_field.return_value = \
            TimeSeriesGroup({'_': tsl})

        sql_api = SqlApi(self.schema,
                         'select topk("user", 2) as g'
                         ' from tsdb where time>0'
                         ' group by time(1)',
                         user='test')
        result = sql_api.execute_internal()
        self.assertEqual(list(zip(range(10, 1000, 10),
                                  map(list, zip(range(8, 800, 8),
                                                range(9, 900, 9))))),
                         result['g'].groups['_'][0].points)

    @staticmethod
    def _query_multi_tag_group_by_result(query):
        """
        Returns a large number of rows based
        Mocks the behavior of a druid client
        :param query:
        :return:
        """
        column_names = ["ts", "node", "partition", "value"]
        # check if its the Last value query
        if 'LATEST' in query:
            # last value query, we should only return one row per series
            for n in range(2):
                for p in range(4):
                    yield TestSqlApiDruid.row(
                        column_names,
                        [10*1000, 'n' + str(n), 'p' + str(p), 10*100 + n*10+p])
        else:
            for i in range(10, 40):
                for n in range(2):
                    for p in range(4):
                        yield TestSqlApiDruid.row(
                            column_names,
                            [i * 1000, 'n' + str(n), 'p' + str(p), i*100 +
                             n*10+p])

    def druid_execute_for_multi_tag_group_by(self, query, datasource, user):
        """
        Mocks the Druid execute query
        :param query:
        :return:
        """
        _, _, _ = self, datasource, user
        return DruidResult(TestSqlApiDruid._query_multi_tag_group_by_result(
            query))

    def test_multi_tag_group_by(self) -> None:
        """
        Tests the druid reader performance through the sqlapi interface
        :return:
        """
        DruidReader.inst.druid_schema.get_dimensions.return_value =\
            ['node', 'partition']
        DruidReader.execute = \
            TestSqlApiDruid.druid_execute_for_multi_tag_group_by

        sql_api = SqlApi(self.schema,
                         "select value as mu from tsdb"
                         " where time>=10 and time < 40 and"
                         " (node='n1' or node='n2') group by time(10),node",
                         user='test')
        result = sql_api.execute_internal()
        self.assertEqual(2, len(result['mu'].groups))

    def test_performance(self) -> None:
        """
        Tests the druid reader performance through the sqlapi interface
        :return:
        """
        DruidReader.inst.druid_schema.get_dimensions.return_value = ['node']
        DruidReader.execute = TestSqlApiDruid.druid_execute

        sql_api = SqlApi(self.schema,
                         "select mean(machine.performance) as mu from tsdb"
                         " where time>=10 and time < 40000 and"
                         " (node='n1' or node='n2') group by time(10),node",
                         user='test')
        pr = cProfile.Profile()
        enable_profile = False
        if enable_profile:
            pr.enable()
        t = time.time()
        for _ in range(10):
            sql_api.execute_internal(nocache=True)
        t = time.time() - t
        self.assertLess(t, 8,
                        'Last documented runtime: 6 s. '
                        'Please look into performance impact')
        print(t)
        if enable_profile:
            pr.disable()
            pr.print_stats(sort="tottime")
