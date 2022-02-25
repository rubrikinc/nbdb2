"""
unittests for sqlapi
"""
import os
from collections import namedtuple
from typing import Dict
from unittest import TestCase
from unittest.mock import Mock

from nbdb.readapi.tests.test_sql_api_druid import DruidResult


from nbdb.common.telemetry import Telemetry

from nbdb.readapi.druid_reader import DruidReader
from nbdb.readapi.query_cache import QueryCache

from nbdb.schema.schema import Schema

from nbdb.common.thread_pools import ThreadPools
from nbdb.config.settings import Settings
from nbdb.readapi.sql_api import SqlApi


class TestSqlApiFlatSchemaDruid(TestCase):
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

        # For this test the static method test_like_query will generate the
        # data acting as mock for Druid cluster
        DruidReader.inst.druid_schema.get_dimensions.return_value = []
        DruidReader.execute = self.druid_execute

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

    def _query_like_result(self, query):
        # LIKE query must have a LIKE operator
        self.assertTrue('LIKE' in query, 'Not a like query')
        column_names = ['ts', 'field', 'value']
        # check if its the Last value query
        if 'LATEST' in query:
            # last value query, we should only return one row per series
            for c in range(2):
                for n in range(2):
                    yield TestSqlApiFlatSchemaDruid.row(
                        column_names,
                        [1000*1000,
                         '{}.{}.Diamond.user.percent'.format(c, n), 10])
        else:
            for i in range(1000, 1400):
                for c in range(2):
                    for n in range(2):
                        yield TestSqlApiFlatSchemaDruid.row(
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
        del datasource, user
        if 'Diamond.user.percent' in query:
            return DruidResult(self._query_like_result(query))
        raise ValueError('Mocked query not supported')

    def test_field_regex_series(self) -> None:
        """
        Test aggregating over a flat field using pattern matching
        """
        _ = self

        sql_api = SqlApi(self.schema,
                         'select'
                         ' mean("clusters.%.%.Diamond.user.percent") as g'
                         ' from tsdb where time>=1000 and time < 1400'
                         ' group by time(200)',
                         user='test')
        result = sql_api.execute_internal()
        self.assertEqual([(1000, 1131.5), (1200, 1331.5)],
                         result['g'].groups['_'][0].points)
