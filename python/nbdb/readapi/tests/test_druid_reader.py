"""
unittests for DruidReader
"""
import os
from unittest import TestCase

from nbdb.common.metric_parsers import TOKEN_TAG_PREFIX, TOKEN_COUNT
from nbdb.config.settings import Settings
from nbdb.common.thread_pools import ThreadPools
from nbdb.readapi.druid_reader import DruidReader
from nbdb.readapi.graphite_api import GraphiteApi
from nbdb.readapi.GraphiteParser import GraphiteParser
from nbdb.schema.schema import Schema


class TestDruidReader(TestCase):
    """
    Unittests for DruidReader
    """

    def setUp(self) -> None:
        """
        Setup a mocked sparse-time-series for testing
        :return:
        """
        Settings.load_yaml_settings(os.path.dirname(__file__) +
                                    '/test_settings.yaml')
        self.schema = Schema.load_from_file(
            os.path.dirname(__file__) + '/test_schema.yaml')
        ThreadPools.instantiate()
        DruidReader.instantiate(Settings.inst.Druid.connection_string)

    def tearDown(self) -> None:
        """
        Stop all the thread pools, blocking wait
        """
        ThreadPools.inst.stop()

    @staticmethod
    def get_dimensions(metric):
        """
        Helper to get dummy Druid dimensions
        """
        token_count = len(metric.split("."))
        return ['field', TOKEN_COUNT] + \
            [TOKEN_TAG_PREFIX + str(i) for i in range(token_count - 1)]

    @staticmethod
    def get_built_query(datasource, metric, start_epoch, end_epoch):
        """
        Get built Druid query
        """
        metric = GraphiteParser.convert_set_patterns(metric)
        # pylint: disable-msg=W0212  # Protected access
        _, field, _, filters = GraphiteApi._convert_flat_field_to_filters(
            metric)
        # pylint: disable-msg=W0212  # Protected access
        return DruidReader.inst._build_query(
            datasource, field, filters, start_epoch, end_epoch,
            TestDruidReader.get_dimensions(metric))

    def test_build_query(self) -> None:
        """
        Test the _build_query() method
        """
        _ = self
        datasource = "test_ds"
        start_epoch, end_epoch = 10, 20
        sparse_query_row_limit = 10000000

        # TEST 1: Simple metric
        metric = "clusters.XYZ.ABC.process_1.uptime.seconds"
        built_query = TestDruidReader.get_built_query(
            datasource, metric, start_epoch, end_epoch)
        exp_query = (
            f"SELECT TIMESTAMP_TO_MILLIS(__time) AS ts, "
            f"field,tkns,tkn0,tkn1,tkn2,tkn3,tkn4, \"value\" from "
            f"\"{datasource}\" where "
            f"MILLIS_TO_TIMESTAMP({start_epoch}000) < __time AND "
            f"__time <= MILLIS_TO_TIMESTAMP({end_epoch}000) AND "
            f"(field = \'seconds\') AND "
            f"((tkn0=\'clusters\') AND "
            f"(tkn1=\'XYZ\') AND "
            f"(tkn2=\'ABC\') AND "
            f"(tkn3=\'process_1\') AND "
            f"(tkn4=\'uptime\') AND "
            f"(tkns=\'6\')) "
            f"LIMIT {sparse_query_row_limit}"
        )
        self.assertEqual(built_query, exp_query)

        # TEST 2: Set pattern in a non-last token
        metric = "clusters.XYZ.ABC.{process_1,process_2}.uptime.seconds"
        built_query = TestDruidReader.get_built_query(
            datasource, metric, start_epoch, end_epoch)
        exp_query = (
            f"SELECT TIMESTAMP_TO_MILLIS(__time) AS ts, "
            f"field,tkns,tkn0,tkn1,tkn2,tkn3,tkn4, \"value\" from "
            f"\"{datasource}\" where "
            f"MILLIS_TO_TIMESTAMP({start_epoch}000) < __time AND "
            f"__time <= MILLIS_TO_TIMESTAMP({end_epoch}000) AND "
            f"(field = \'seconds\') AND "
            f"((tkn0=\'clusters\') AND "
            f"(tkn1=\'XYZ\') AND "
            f"(tkn2=\'ABC\') AND "
            # Set pattern should be converted to multiple clauses separated by
            # OR operator
            f"((tkn3=\'process_1\') OR (tkn3=\'process_2\')) AND "
            f"(tkn4=\'uptime\') AND "
            f"(tkns=\'6\')) "
            f"LIMIT {sparse_query_row_limit}"
        )
        self.assertEqual(built_query, exp_query)

        # TEST 3: Set pattern in a last token
        metric = "clusters.XYZ.ABC.process_1.uptime.{millis,seconds}"
        built_query = TestDruidReader.get_built_query(
            datasource, metric, start_epoch, end_epoch)
        exp_query = (
            f"SELECT TIMESTAMP_TO_MILLIS(__time) AS ts, "
            f"field,tkns,tkn0,tkn1,tkn2,tkn3,tkn4, \"value\" from "
            f"\"{datasource}\" where "
            f"MILLIS_TO_TIMESTAMP({start_epoch}000) < __time AND "
            f"__time <= MILLIS_TO_TIMESTAMP({end_epoch}000) AND "
            # Set pattern in field should be converted to multiple clauses
            # separated by OR operator
            f"(field = \'millis\' OR field = \'seconds\') AND "
            f"((tkn0=\'clusters\') AND "
            f"(tkn1=\'XYZ\') AND "
            f"(tkn2=\'ABC\') AND "
            f"(tkn3=\'process_1\') AND "
            f"(tkn4=\'uptime\') AND "
            f"(tkns=\'6\')) "
            f"LIMIT {sparse_query_row_limit}"
        )
        self.assertEqual(built_query, exp_query)
