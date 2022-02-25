"""
Unit tests for static.py
"""

import unittest

import numpy as np
import pandas as pd


from nbdb.anomaly.static import Static


class TestStatic(unittest.TestCase):
    """
    Unit tests for static threshold based anomaly detection.
    """
    def setUp(self):
        np.random.seed(1)

    def test_check_no_anomalies_found_for_normal_data(self):
        """For data without anomalies, verify empty list is returned"""
        anomaly_finder = Static({'threshold': 4})
        normal_series = pd.Series(np.random.randn(100,), index=range(100))
        baseline = normal_series.values
        anomalies = anomaly_finder.find_anomalies(baseline, normal_series)
        self.assertEqual(0, len(anomalies))

    def test_check_no_anomalies_found_for_flatline_data(self):
        """For data which is flat, verify empty list is returned"""
        anomaly_finder = Static({'threshold': 4})
        normal_series = pd.Series(np.ones(100,), index=range(100))
        baseline = normal_series.values
        anomalies = anomaly_finder.find_anomalies(baseline, normal_series)
        self.assertEqual(0, len(anomalies))

    def test_check_anomalies_found_for_anomalous_data_gt(self):
        """For data with anomalies, verify correct anomaly returned (gt)"""
        anomaly_finder = Static({'threshold': 4})
        anomalous_series = pd.Series(np.random.randn(100,), index=range(100))
        anomalous_series[95:99] = 5
        baseline = np.random.randn(100,)
        anomalies = anomaly_finder.find_anomalies(baseline, anomalous_series)
        self.assertEqual(1, len(anomalies))
        self.assertEqual(95, anomalies[0].timewindow.start)
        self.assertEqual(98, anomalies[0].timewindow.end)

    def test_check_anomalies_found_for_anomalous_data_lt(self):
        """For data with anomalies, verify correct anomaly returned (lt)"""
        anomaly_finder = Static({'threshold': -4, 'comparator_fn': 'lt'})
        anomalous_series = pd.Series(np.random.randn(100,), index=range(100))
        anomalous_series[95:99] = -5
        baseline = np.random.randn(100,)
        anomalies = anomaly_finder.find_anomalies(baseline, anomalous_series)
        self.assertEqual(1, len(anomalies))
        self.assertEqual(95, anomalies[0].timewindow.start)
        self.assertEqual(98, anomalies[0].timewindow.end)
