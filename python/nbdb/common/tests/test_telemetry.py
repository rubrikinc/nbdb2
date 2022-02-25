"""
Test MultiRegistry
"""
import os
from unittest import TestCase
from nbdb.common.telemetry import Telemetry
from nbdb.config.settings import Settings


class TestMultiRegistry(TestCase):
    """
    Tests the MultiRegistry object methods
    """

    def setUp(self) -> None:
        """
        Setup the string table and create metric keys for validation
        :return:
        """
        Settings.load_yaml_settings(os.path.dirname(__file__) +
                                    '/test_settings.yaml')
        Telemetry.inst = None
        Telemetry.initialize()

    def tearDown(self) -> None:
        Telemetry.inst = None

    def test_metric_types(self) -> None:
        """
        Test various metric types
        """
        tag_key_values = ["Env=test"]
        Telemetry.inst.registry.counter(
            "sparse_counter", tag_key_values=tag_key_values).inc()
        Telemetry.inst.registry.gauge(
            "sparse_gauge", tag_key_values=tag_key_values).set_value(1)
        Telemetry.inst.registry.histogram(
            "sparse_histogram", tag_key_values=tag_key_values).add(1)
        Telemetry.inst.registry.meter(
            "sparse_meter", tag_key_values=tag_key_values).mark(1)

        exp_fields = {
            "counter": ["count"],
            "gauge": ["value"],
            "histogram": ['75_percentile', '95_percentile', '999_percentile',
                          '99_percentile', 'avg', 'count', 'max', 'min',
                          'std_dev'],
            "meter": ['15m_rate', '1m_rate', '5m_rate', 'count', 'mean_rate'],
        }

        # Verify both Graphite & Influx format
        Telemetry.inst.registry.extra_tags = ["Name=gandalf"]
        for protocol in ("graphite", "influx"):
            metrics = Telemetry.inst.registry.dump_metrics(protocol)
            for metric_type in ("counter", "gauge", "histogram", "meter"):
                metric_name = f"sparse_{metric_type}"
                if protocol == 'graphite':
                    # We dump a flattened key in Graphite ignoring tag keys
                    key = f"{metric_name}.test"
                else:
                    # For Influx, we generate a tagged format. We also add the
                    # extra tags as the first tags
                    key = f"{metric_name},Name=gandalf,Env=test"
                self.assertTrue(key in metrics,
                                f"Key {key} not found for protocol {protocol}")
                self.assertEqual(sorted(metrics[key].keys()),
                                 exp_fields[metric_type])
