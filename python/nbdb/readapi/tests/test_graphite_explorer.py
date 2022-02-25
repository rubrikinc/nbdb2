"""
unittests for sqlapi
"""
import os
from typing import Dict, List
from unittest import TestCase
from unittest.mock import Mock

from nbdb.common.metric_parsers import FIELD, TOKEN_TAG_PREFIX, TOKEN_COUNT
from nbdb.common.telemetry import Telemetry
from nbdb.config.settings import Settings
from nbdb.readapi.graphite_explorer import GraphiteExplorer
from nbdb.readapi.tests.test_sql_api_druid import DruidResult
from nbdb.schema.schema import Schema


class TestGraphiteExplorer(TestCase):
    """
    Unit test for GraphiteExplorer, mocks sparse_series_reader
    """
    def setUp(self) -> None:
        """
        Setup the test suite, loads the test settings and schema
        """
        Settings.load_yaml_settings(os.path.dirname(__file__) +
                                    '/test_druid_settings.yaml')
        self.schema = Schema.load_from_file(
            os.path.dirname(__file__) + '/test_schema.yaml')
        self.mock_druid_reader = Mock()
        Telemetry.inst = None
        Telemetry.initialize()
        self.graphite_explorer = GraphiteExplorer(
            self.mock_druid_reader, Settings.inst.sparse_store.sparse_algos)
        self.user = "test"
        self.queries = []

    def tearDown(self) -> None:
        """
        Stop telemetry
        """
        Telemetry.inst = None
        self.queries = []

    @staticmethod
    def row(column_names: List, column_values: List) -> Dict:
        """Create dict mimicking JSON object"""
        return dict(zip(column_names, column_values))

    @staticmethod
    def _exploration_query_result(query: str):
        """Mimic Druid query result returning 1 leaf row and 1 non-leaf row"""
        num_tokens = 6
        if f'{TOKEN_COUNT} > {num_tokens}' in query:
            # Non-leaf query
            column_names = [f"{TOKEN_TAG_PREFIX}{i}"
                            for i in range(num_tokens)]
            yield TestGraphiteExplorer.row(
                column_names,
                'clusters.ABC.RVM1.Diamond.uptime.random'.split('.'))
        else:
            # Leaf node query
            column_names = [f"{TOKEN_TAG_PREFIX}{i}"
                            for i in range(num_tokens - 1)] + [FIELD]
            yield TestGraphiteExplorer.row(
                column_names,
                'clusters.ABC.RVM1.Diamond.uptime.seconds'.split('.'))

    def druid_execute(self, query: str, datasource: str, user: str):
        """Mimic Druid query result"""
        _, _, _ = self, datasource, user
        query_result = TestGraphiteExplorer._exploration_query_result(query)
        return DruidResult(query_result)

    def test_browse_dot_schema(self) -> None:
        """
        Tests the browse_dot_schema() method
        """
        self.mock_druid_reader.execute = self.druid_execute

        # TEST 1: Make sure exploration queries for the first token returns a
        # static response
        output = self.graphite_explorer.browse_dot_schema(self.schema, '*',
                                                          10, 20, self.user)
        self.assertEqual(len(output), 2)
        self.assertEqual(output[0]['id'], 'clusters')
        self.assertEqual(output[0]['text'], 'clusters')
        self.assertEqual(output[1]['id'], 'internal')
        self.assertEqual(output[1]['text'], 'internal')

        # TEST2: Run exploration query for clusters.abc.RVM1.Diamond.uptime.*
        metric = 'clusters.abc.RVM1.Diamond.uptime.*'
        output = self.graphite_explorer.browse_dot_schema(self.schema, metric,
                                                          10, 20, self.user)
        # We should see 2 results: one non-leaf and one leaf
        self.assertEqual(len(output), 2)
        self.assertEqual(output[0]['text'], 'random')
        self.assertEqual(output[0]['allowChildren'], 1)
        self.assertEqual(output[0]['expandable'], 1)
        self.assertEqual(output[0]['leaf'], 0)

        self.assertEqual(output[1]['text'], 'seconds')
        self.assertEqual(output[1]['allowChildren'], 0)
        self.assertEqual(output[1]['expandable'], 0)
        self.assertEqual(output[1]['leaf'], 1)

    def druid_execute_empty_result(self, query: str, datasource: str,
                                   user: str):
        """
        Mimic Druid query result returning zero rows. Additionally store all
        queries that were run
        """
        del datasource, user
        self.queries += [query]
        return DruidResult(iter([]))

    def test_exploration_query_with_set_pattern(self):
        """
        Test exploration query with set patterns
        """
        # TEST 1: Run exploration query with set pattern. Make sure we convert
        # the set pattern into multiple OR clauses
        self.mock_druid_reader.execute = self.druid_execute_empty_result
        metric = 'clusters.abc.RVM1.Diamond.uptime.{sec*,random}'
        self.graphite_explorer.browse_dot_schema(
            self.schema, metric, start_epoch=10, end_epoch=20, user=self.user)

        # We should see 2 results: one non-leaf and one leaf
        non_leaf_clause_str = "(tkn5 LIKE 'sec%' OR tkn5 = 'random')"
        leaf_clause_str = "(field LIKE 'sec%' OR field = 'random')"
        self.assertEqual(len(self.queries), 2)
        self.assertTrue(non_leaf_clause_str in self.queries[0])
        self.assertTrue(leaf_clause_str in self.queries[1])
