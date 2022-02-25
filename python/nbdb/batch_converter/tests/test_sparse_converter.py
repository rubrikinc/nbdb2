"""
Unittest for SparseConverter
"""

import json
import os
import tempfile
from unittest import TestCase

from nbdb.common.context import Context
from nbdb.common.data_point import MODE_ROLLUP
from nbdb.config.settings import Settings
from nbdb.batch_converter.sparse_converter import SparseConverter
from nbdb.common.data_point import DATAPOINT_VERSION
from nbdb.store.file_backed_sparse_store import FileBackedSparseStore
from nbdb.schema.schema import Schema


class TestSparseConverter(TestCase):
    """
    Test SparseConverter
    """
    def setUp(self):
        self.filestore_dir = os.path.join(tempfile.mkdtemp(), "filestore")
        self.schema_path = os.path.join(
            os.path.dirname(__file__), "../..", "config", "schema_batch_prod.yaml")
        self.settings_path = os.path.join(
            os.path.dirname(__file__), "../..", "config", "settings_batch_consumer.yaml")
        self.batch_filter_file = os.path.join(
            os.path.dirname(__file__), "batch_metrics_filter.yaml")
        settings = Settings.load_yaml_settings(self.settings_path)
        schema = Schema.load_from_file(self.schema_path)
        context = Context(schema=schema)
        self.converter = SparseConverter(
            context, settings, min_storage_interval=60, is_local=True)
        self.cluster_id = "cluster123"

    def test_52_flat_only(self):
        """
        Test conversion for a small file based batch JSON with flat schema
        """
        batch_json_path = \
            os.path.join(os.path.dirname(__file__), "flat_only_52.json")
        self.converter.convert(
            cluster_id=self.cluster_id,
            batch_json_path=batch_json_path,
            output_dir_path=self.filestore_dir,
            batch_filter_file=self.batch_filter_file,
            bundle_creation_time=12345,
            compress=False,
            consumer_mode=MODE_ROLLUP,
            progress_callback=lambda x,y: None)
        manifest_map = FileBackedSparseStore.parse_datasources_from_manifest(
            self.filestore_dir)
        self.assertGreaterEqual(len(manifest_map), 1)
        for ds_file_info in manifest_map.values():
            filepath = os.path.join(
                self.filestore_dir, ds_file_info.filename)
            self._verify_flat_json(filepath)

    def _verify_flat_json(self, json_filepath: str) -> None:
        """
        Verify that fields required in Druid schema are present
        in the final sparse JSON.
        """
        with open(json_filepath, 'r') as json_file:
            for line in json_file:
                parsed = json.loads(line)
                # Required fields must be present
                self.assertIn("time", parsed)
                self.assertIn("value", parsed)
                self.assertIn("field", parsed)
                self.assertIn(DATAPOINT_VERSION, parsed)
                # Tokens 1-3 should be clusters, $clusterId, $nodeId
                self.assertEqual(parsed["tkn0"], "clusters")
                self.assertEqual(parsed["tkn1"], self.cluster_id)
                self.assertEqual(parsed["tkn2"], "vm-machine-opp4oy-nyne5zw")
