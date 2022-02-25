"""
Unittest for SparseSeriesData class
"""

import os
import time
from unittest import TestCase
from unittest.mock import Mock

from nbdb.common.context import Context
from nbdb.common.data_point import DATAPOINT_VERSION, DataPoint
from nbdb.common.data_point import TOMBSTONE_VALUE, MODE_BOTH
from nbdb.common.telemetry import Telemetry
from nbdb.common.tracing_config import TracingConfig
from nbdb.config.settings import Settings
from nbdb.schema.schema import Schema
from nbdb.store.batch_sparse_series_writer import BatchSparseSeriesWriter


class TestSparseSeriesWriter(TestCase):
    """
    Test the write path by writing data and verifying that only sparse
    points that conform to appropriate smoothing are written to the db
    """

    def setUp(self) -> None:
        """
        Setup a partitioned mocked sparse-time-series for testing
        :return:
        """
        Settings.load_yaml_settings(os.path.dirname(__file__) +
                                    '/test_settings.yaml')
        schema = Schema.load_from_file(
            os.path.dirname(__file__) + '/test_schema.yaml', batch_mode=True)
        self.context = Context(schema=schema)
        Telemetry.inst = Mock()
        TracingConfig.inst = Mock()
        self.sparse_series_writer = BatchSparseSeriesWriter(
            sparse_store=Mock(),
            sparse_store_settings=Settings.inst.sparse_store,
            tracing_config_settings=Settings.inst.tracing_config,
            context=self.context,
            protocol=Settings.inst.realtime_metric_consumer.metric_protocol,
            sparse_telemetry_source_id="test",
            past_message_lag=10**9,
            future_message_lag=10**9,
            consumer_mode=MODE_BOTH
        )
        self.bundle_creation_time = 123
        self.exploration_ds = schema.exploration_datasources.get(
            Settings.inst.realtime_metric_consumer.metric_protocol)

    def generate_datapoints(self, nodes: int, fields: int, epoch: int) -> None:
        """
        :param nodes:
        :param fields:
        :param epoch:
        """
        for n in range(nodes):
            for f in range(fields):
                tags = {'node': 'n_' + str(n),
                        DATAPOINT_VERSION: self.bundle_creation_time}
                data_points = [DataPoint('batch_m',
                                         'f_' + str(f),
                                         tags,
                                         epoch,
                                         int(time.time()),
                                         epoch*10)]
                self.sparse_series_writer.append_metrics(data_points,
                                                         replay_mode=False)

    def test_add_tombstone_markers(self) -> None:
        """
        Tests that a tombstone marker is explicitly inserted for all series
        after a bundle has been processed
        """
        self.sparse_series_writer.reinitialize()
        self.sparse_series_writer.sparse_store.reset_mock()
        # Generate 4 series (2 nodes, 2 fields)
        nodes, fields = 2, 2
        epoch = 10
        dg_interval = self.sparse_series_writer.data_gap_detection_interval
        self.generate_datapoints(nodes, fields, epoch)

        # We should see a tombstone for all four series after
        # add_tombstone_markers() is called
        self.sparse_series_writer.add_tombstone_markers(
            self.bundle_creation_time)
        exp_tombstones = []
        for node in range(nodes):
            for field in range(fields):
                tags = {'node': 'n_%d' % node,
                        DATAPOINT_VERSION: self.bundle_creation_time}
                # Raw datapoints tombstone
                raw_tombstone = DataPoint(
                    'batch_m', 'f_%d' % field, tags,
                    epoch + dg_interval, int(time.time()), TOMBSTONE_VALUE,
                    is_special_value=True)
                exp_tombstones += [raw_tombstone.to_druid_json_str()]

                # Both 3600 & 7200 datasources should generate a partial window
                # datapoint, followed by tombstone datapoints
                for window in (3600, 7200):
                    timestamp = (int((epoch + dg_interval) / window) * window +
                                 2 * window)
                    rollup_tombstone = DataPoint(
                        'batch_m', 'f_%d' % field, tags,
                        timestamp, int(time.time()), TOMBSTONE_VALUE,
                        is_special_value=True)
                    exp_tombstones += [rollup_tombstone.to_druid_json_str()]

        all_tombstones = \
            [call[1][0].to_druid_json_str()
             for call in
             self.sparse_series_writer.sparse_store.write.mock_calls
             if call[1][0].value == TOMBSTONE_VALUE]

        self.assertEqual(sorted(exp_tombstones), sorted(all_tombstones))
