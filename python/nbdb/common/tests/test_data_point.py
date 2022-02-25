"""
TestMetricValue
"""
import json
import time
from unittest import TestCase
from nbdb.common.data_point import DataPoint


class TestDataPoint(TestCase):
    """
    Tests the MetricValue object related methods
    """

    def setUp(self) -> None:
        """
        Setup the string table and create metric keys for validation
        :return:
        """

    def test_valid_data_point(self) -> None:
        """
        Test the valid cases for a metric value and make sure they
        are stored appropriately
        """
        data_point = DataPoint('measurement',
                               'field',
                               {'tg1': 'tv1', 'tg2': 'tv2'},
                               10000,
                               int(time.time()),
                               1.4)
        self.assertEqual('measurement', data_point.datasource)
        self.assertEqual('field', data_point.field)
        self.assertEqual('tv1', data_point.tags['tg1'])
        self.assertEqual(10000, data_point.epoch)
        self.assertEqual(1.4, data_point.value)

    def test_json(self) -> None:
        """
        Test the json generation from the data point
        """
        data_point = DataPoint('measurement',
                               'field',
                               {'tg1': 'tv1', 'tg2': 'tv2'},
                               10000,
                               int(time.time()),
                               1.4)
        json_str = data_point.to_druid_json_str()
        druid_point = json.loads(json_str)
        self.assertEqual(data_point.field, druid_point['field'])
        self.assertEqual(data_point.epoch, druid_point['time'])
        self.assertEqual(data_point.value, druid_point['value'])
        self.assertEqual(data_point.tags['tg1'], druid_point['tg1'])
        self.assertEqual(data_point.tags['tg2'], druid_point['tg2'])

    def test_graphite_flat_json(self) -> None:
        """
        Test the json generation for a Graphite flat data point
        """
        data_point = DataPoint('measurement', 'field', {},
                               10000, int(time.time()), 1.4)
        json_str = data_point.to_druid_json_str()
        druid_point = json.loads(json_str)
        self.assertEqual(data_point.field, druid_point['field'])
        self.assertEqual(data_point.epoch, druid_point['time'])
        self.assertEqual(data_point.value, druid_point['value'])
        self.assertEqual(data_point.tags, {})

    def test_datasource_from_series_id(self) -> None:
        """
        Tests datasource extraction from series_id
        """
        data_point = DataPoint('measurement', 'field', {},
                               10000, int(time.time()), 1.4)
        datasource = DataPoint.datasource_from_series_id(data_point.series_id)
        self.assertEqual(data_point.datasource, datasource)

        # TEST1: Make sure series ID with no field is not parsed
        self.assertRaises(ValueError,
                          DataPoint.datasource_from_series_id, 'abc')

        # TEST2: Make sure series ID which has '=' in the tag value
        # gets parsed correctly
        series_id = 'abc|tk=MVID=Vol1|p95'
        new_dp = DataPoint.from_series_id(series_id, 10, 10)
        self.assertEqual(new_dp.series_id, series_id)

    def test_data_point_from_json(self) -> None:
        """
        Tests extracting a data point from json representation
        """
        data_point = DataPoint('measurement', 'field', {},
                               10000, int(time.time()), 1.4)
        data_point1 = DataPoint.from_series_id(data_point.series_id,
                                               data_point.epoch,
                                               data_point.value)
        self.assertEqual(data_point.to_druid_json_str(),
                         data_point1.to_druid_json_str())
        self.assertRaises(ValueError,
                          DataPoint.from_series_id, 'abc', 1, 1.0)
