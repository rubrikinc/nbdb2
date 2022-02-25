"""
Unit tests for JsonParser
"""
import json
import os
import tempfile
from unittest import TestCase

from nbdb.batch_converter.batch_json_parser import BatchJsonParser
from nbdb.common.data_point import DATAPOINT_VERSION
from nbdb.schema.schema import Schema


class TestBatchJsonParser(TestCase):
    """
    Test cases for JsonParser.
    """
    def setUp(self):
        prod_schema_path = os.path.join(
            os.path.dirname(__file__), "../..", "config", "schema_prod.yaml")
        self.schema = Schema.load_from_file(prod_schema_path)
        self.cluster_id = "clusterId"
        self.batch_filter_file = os.path.join(
            os.path.dirname(__file__), "batch_metrics_filter.yaml")

    def test_flat_series_parsing(self):
        """
        Flat series parsing.
        """
        file = tempfile.NamedTemporaryFile('w+')
        json.dump({
            "name": "Node.Service.Series.count",
            "timestamp": 60 * 10**9,
            "value": 10,
            "field": "value",
            "tags": {}
            },
            file)
        file.write("\n")
        json.dump({
            "name": "Node.Service.Series.count",
            "timestamp": 120 * 10**9,
            "value": 20,
            "field": "value",
            "tags": {}
            },
            file)
        file.write("\n")
        file.flush()
        parser = self._get_parser_for_path(file.name)
        # Context manager is needed to close the file handle
        # pylint: disable=unnecessary-comprehension
        with parser:
            dps = [dp for dp in parser]
        self.assertEqual(parser.num_skipped, 0)
        self.assertEqual(parser.num_flat, 2)
        self.assertEqual(len(dps), 2)
        first, second = dps
        expected_tags = \
            {'tkn0': 'clusters',
             'tkn1': 'clusterId',
             'tkn2': 'Node',
             'tkn3': 'Service',
             'tkn4': 'Series',
             'tkns': '6',
             DATAPOINT_VERSION: 12345}
        # Test properties of first point
        self.assertEqual(first.epoch, 60)
        self.assertEqual(first.server_rx_epoch, 60)
        self.assertEqual(first.value, 10)
        self.assertEqual(first.tags, expected_tags)
        self.assertEqual(first.field, "count")
        self.assertEqual(first.datasource, "all_metrics_t_64")
        # Test properties of second point
        self.assertEqual(second.epoch, 120)
        self.assertEqual(second.server_rx_epoch, 120)
        self.assertEqual(second.value, 20)
        self.assertEqual(second.tags, expected_tags)
        self.assertEqual(second.field, "count")
        self.assertEqual(second.datasource, "all_metrics_t_64")

    def test_non_flat_series_are_skipped(self):
        """
        Right now, we only support parsing of flat graphite style metrics.
        """
        file = tempfile.NamedTemporaryFile('w+')
        # This will be skipped since tags are not empty
        json.dump({
            "name": "Node.Service.Series.count",
            "timestamp": 60 * 10**9,
            "value": 10,
            "field": "value",
            "tags": {"key": "value"}
            },
            file)
        file.write("\n")
        # This will be skipped since it doesn't have nodeID
        json.dump({
            "name": "ObjectCounter",
            "timestamp": 60 * 10**9,
            "value": 10,
            "field": "value",
            "tags": {}
            },
            file)
        file.write("\n")
        # This will be skipped since field name is not "value"
        json.dump({
            "name": "Node.Service.Series.count",
            "timestamp": 60 * 10**9,
            "value": 10,
            "field": "counter",
            "tags": {}
            },
            file)
        file.write("\n")
        # This will be skipped because the metric isn't whitelisted
        json.dump({
            "name": "Node.Random.Series.count",
            "timestamp": 60 * 10**9,
            "value": 10,
            "field": "value",
            "tags": {}
            }, file)
        file.write("\n")
        # All OK. This will be written
        json.dump({
            "name": "Node.Service.Series.count",
            "timestamp": 60 * 10**9,
            "value": 10,
            "field": "value",
            "tags": {}
            },
            file)
        file.write("\n")
        file.flush()
        parser = self._get_parser_for_path(file.name)
        # Context manager is needed to close the file handle
        # pylint: disable=unnecessary-comprehension
        with parser:
            dps = [dp for dp in parser]
        self.assertEqual(parser.num_skipped, 4)
        self.assertEqual(parser.num_flat, 2)
        self.assertEqual(parser.num_tagged, 3)
        self.assertEqual(len(dps), 1)
        self.assertEqual(dps[0].value, 10)

    def test_flat_series_from_file(self):
        """
        Verify parsing on a small sample from a file
        """
        parser = self._get_parser_for_test_file("flat_only_52.json")
        # Context manager is needed to close the file handle
        # pylint: disable=unnecessary-comprehension
        with parser:
            # Modify parser filter rules to allow all metrics for this
            # testcase
            parser.filter_rules["graphite"] = {
                "whitelist_rules": ["*"],
                "blacklist_rules": []
            }
            dps = [dp for dp in parser]
            self.assertEqual(len(dps), 34)
            self.assertEqual(parser.num_skipped, 0)
            for data_point in dps:
                self.assertEqual(data_point.tags["tkn0"], "clusters")
                self.assertEqual(data_point.tags["tkn1"], self.cluster_id)
                self.assertEqual(
                    data_point.tags["tkn2"], "vm-machine-opp4oy-nyne5zw")

    def _get_parser_for_test_file(self, test_filename):
        path = os.path.join(os.path.dirname(__file__), test_filename)
        return self._get_parser_for_path(path)

    def _get_parser_for_path(self, path):
        return BatchJsonParser(
            datasource_getter_fn=self.schema.get_datasource,
            min_storage_interval=60,
            cluster_id=self.cluster_id,
            filepath=path,
            batch_filter_file=self.batch_filter_file,
            bundle_creation_time=12345,
        )
