
"""
TestTracingConfig
"""
import os
import sys
from unittest import TestCase
from unittest.mock import Mock

import botocore
import yaml

from nbdb.common.context import Context
from nbdb.common.metric_parsers import MetricParsers
from nbdb.common.telemetry import Telemetry
from nbdb.common.tracing_config import TracingConfig
from nbdb.schema.schema import Schema
from nbdb.config.settings import Settings


class TestTracingConfig(TestCase):
    """
    Test tracing config helper
    """

    def setUp(self) -> None:
        schema = Schema.load_from_file(os.path.dirname(__file__) +
                              '/test_schema.yaml')
        self.context = Context(schema=schema)
        Settings.load_yaml_settings(os.path.dirname(__file__) +
                                    '/test_settings.yaml')
        # Mock boto3 client
        self.mock_client = Mock()
        sys.modules["nbdb.common.tracing_config"].client = self.mock_client
        self.mock_client.get_object.side_effect = \
            botocore.exceptions.ClientError(operation_name="Testing",
                                            error_response={})
        if Telemetry.inst is None:
            Telemetry.initialize()
        TracingConfig.initialize(Settings.inst.tracing_config)
        self.tracing_config = TracingConfig.inst

    def mock_client_get_object(self, config) -> None:
        """
        Mock boto3 s3 client get_object response to return given YAML config
        """

        mock_body = Mock()
        mock_body.read.return_value = yaml.dump(config)

        self.mock_client.get_object.side_effect = None
        self.mock_client.get_object.return_value = {'Body': mock_body}

    def test_tracing_config_match(self) -> None:
        """
        Tests tracing config match methods
        """
        # TEST1: Test exactly matching tracing config
        parser = MetricParsers(self.context, 'influx')
        msg = 'procstat,group_id=abhishek-kumar,process_name=ksoftirqd/6,' \
              'Cluster=c0 '\
              'write_count=0 1582326360000000000\n'

        data_points = parser.parse_influx_metric_values(msg)
        self.assertEqual(2, len(data_points))

        # Try tracing config for this metric
        config = [
            {'group_id': 'abhishek-kumar',
             'process_name': 'ksoftirqd/6',
             'field': 'procstat.write_count'}
        ]
        self.mock_client_get_object(config)
        self.tracing_config.download_tracing_config_from_s3()

        field_and_tags = data_points[0].tags.copy()
        field_and_tags.update({'field': data_points[0].field})
        self.assertEqual(True, self.tracing_config.match(field_and_tags))

        # TEST2: Try slightly different tracing config and verify it doesn't
        # match
        config = [
            {'group_id': 'random_dude',
             'process_name': 'ksoftirqd/6',
             'field': 'procstat.write_count'}
        ]
        self.mock_client_get_object(config)
        self.tracing_config.download_tracing_config_from_s3()
        self.assertEqual(False, self.tracing_config.match(field_and_tags))

        # TEST3: Try a trace config containing a single matching element. It
        # should still match the metric
        config = [
            {'field': 'procstat.write_count'}
        ]
        self.mock_client_get_object(config)
        self.tracing_config.download_tracing_config_from_s3()
        self.assertEqual(True, self.tracing_config.match(field_and_tags))
