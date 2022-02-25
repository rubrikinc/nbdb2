"""
TestMetricKey
"""

from unittest import TestCase

from nbdb.common.metric_key import MetricKey
from nbdb.string_table.in_memory_string_table import InMemoryStringTable


class TestMetricKey(TestCase):
    """
    TestMetricKey Unittests
    """

    def setUp(self):
        """
        Setup the string table and generate test keys
        :return:
        """
        self.string_table = InMemoryStringTable()
        self.metric_key = MetricKey('measurement.field',
                                    {'k1': 'v1', 'k2': 'v2'})
        self.tokenized_metric_key = self.metric_key.\
            generate_tokenized_metric_key(self.string_table)
        self.series_id = self.tokenized_metric_key.generate_series_id()

    def test_metric_key_tokenization(self) -> None:
        """
        test tokenized keys are distinguishable from untokenized key
        :return:
        """
        self.assertFalse(self.metric_key.is_tokenized(),
                         'self.metric_key is not tokenized')
        self.assertTrue(self.tokenized_metric_key.is_tokenized(),
                        'self.t_mk is tokenized')

    def test_series_id_generation(self) -> None:
        """
        Test the key generation
        """
        # we should get the exact same series_id if we were to generate the
        # same metric_key again but with tags in reverse order
        metric_key_2 = MetricKey('measurement.field', {'k2': 'v2', 'k1': 'v1'})
        tokenized_metric_key_2 = metric_key_2.\
            generate_tokenized_metric_key(self.string_table)
        series_id_2 = tokenized_metric_key_2.generate_series_id()

        self.assertEqual(self.series_id, series_id_2,
                         'Series Id should be same for the same metric key')
