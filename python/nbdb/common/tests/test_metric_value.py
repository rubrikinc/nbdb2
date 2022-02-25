"""
TestMetricValue
"""
from unittest import TestCase

from nbdb.common.metric_key import MetricKey
from nbdb.common.metric_value import MetricValue
from nbdb.string_table.in_memory_string_table import InMemoryStringTable


class TestMetricValue(TestCase):
    """
    Tests the MetricValue object related methods
    """

    def setUp(self) -> None:
        """
        Setup the string table and create metric keys for validation
        :return:
        """
        self.string_table = InMemoryStringTable()
        self.key = MetricKey('f', {})
        self.tokenized_key = self.key.generate_tokenized_metric_key(
            self.string_table)

    def test_valid_metric_values(self):
        """
        Test the valid cases for a metric value and make sure they
        are stored appropriately
        :return:
        """
        metric_value = MetricValue(self.tokenized_key, 100, 10.0)

        self.assertEqual(metric_value.value, 10.0)
        self.assertEqual(metric_value.epoch, 100)
        self.assertEqual(self.tokenized_key.generate_series_id(),
                         metric_value.key.generate_series_id())

    def test_invalid_metric_values(self):
        """
        Test the incorrect usage
        :return:
        """
        # Invalid MetricValue with string value
        self.assertRaises(AssertionError, MetricValue,
                          self.tokenized_key, 100, '10')
        # Invalid MetricValue with untokenized key
        self.assertRaises(AssertionError, MetricValue, self.key, 100, 10)

        # Invalid MetricValue with invalid epoch
        self.assertRaises(AssertionError, MetricValue,
                          self.tokenized_key, 0, 10)
