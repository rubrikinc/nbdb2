"""
Unit tests for DistHist
"""
import os
import random
import time
import unittest
from typing import List, Tuple

from nbdb.common.data_point import DataPoint

from nbdb.schema.schema import Schema

from nbdb.common.tracing_config import TracingConfig
from nbdb.store.dist_hist import DistHist
from nbdb.readapi.time_series_response import TimeRange
from nbdb.config.settings import Settings


class TestDistHist(unittest.TestCase):
    """
    Tests the DistHist
    """

    def setUp(self):
        """
        Initialize the Settings and Schema
        :return:
        """
        Schema.load_from_file(os.path.dirname(__file__) + '/test_schema.yaml')
        Settings.load_yaml_settings(os.path.dirname(__file__) +
                                    '/test_settings.yaml')
        if TracingConfig.inst is None:
            TracingConfig.initialize(Settings.inst.tracing_config)

    def test_dist_hist(self):
        """
        Test kde with different distributions
        """
        # unimodal distribution
        modes: List[Tuple[float, float, float]] = [(200, 1, 99)]
        self.dist_hist_with_modes(modes, (0, 1000))

        modes: List[Tuple[float, float, float]] = [(20, 1, 99)]
        self.dist_hist_with_modes(modes, (0, 100))

        # multi-modal distribution
        modes: List[Tuple[float, float, float]] = [(20, 1, 50),  # 50% prob
                                                   (60, 1, 80),  # 30% prob
                                                   (90, 1, 98)]   # 19% prob]
        self.dist_hist_with_modes(modes, (0, 100))

        # multi-modal distribution with many modes
        modes: List[Tuple[float, float, float]] = [(120, 4, 20),  # 20% prob
                                                   (900, 4, 40),  # 20% prob
                                                   (1600, 4, 60),  # 20% prob
                                                   (2100, 4, 80),  # 20% prob
                                                   (3000, 4, 98)]  # 18% prob
        self.dist_hist_with_modes(modes, (0, 4000))

    def dist_hist_with_modes(self,
                             modes: List[Tuple[float, float, float]],
                             data_range: Tuple[float, float]):
        """
        Tests the discrete kde is able to learn the distribution
        """
        points = DistHist.generate_multi_modal_series(
            TimeRange(0, 5000, 1),
            'test',
            'outlier',
            {},
            modes,
            data_range
        )
        dist_hist = DistHist(pattern='.*outlier',
                             bin_count=1000,
                             band_computation_points=2000,
                             max_band_width=0.05,
                             min_band_probability=0.01,
                             min_band_probability_change=0.001)
        for p in points:
            dist_hist.fit(p.value)
            if dist_hist.bands.is_ready():
                # verify the bands
                for mode in modes:
                    print('Mode: u={} s={} p={}%'.format(mode[0], mode[1],
                                                         mode[2]))

                for band in dist_hist.bands.bands:
                    print('Band: {} - {}, {}%'.format(int(band[0]),
                                                      int(band[1]),
                                                      int(band[2] * 100)))

                self.assertLessEqual(len(modes), len(dist_hist.bands.bands),
                                     'Expected 1 or more band'
                                     ' for each of the modes')
                # all the modes should be covered by the bands
                for mode in modes:
                    found = False
                    for band in dist_hist.bands.bands:
                        if band[0] <= mode[0] <= band[1]:
                            found = True
                            break
                    self.assertTrue(found,
                                    'Expected mode :u={} s={} to be covered '
                                    'by a band to cover one of the modes'.
                                    format(mode[0], mode[1]))
                break

        # confirm an outlier is not processed in band
        for mode in modes:
            self.assertTrue(dist_hist.bands.get_band(mode[0] + 1) is not None,
                            'Value close to mean :{}'
                            ' is expected to be in band'.format(mode[0] + 1))

        for mode in modes:
            self.assertTrue(dist_hist.bands.get_band(mode[0] + 10 * mode[1])
                            is None,
                            'Value 10 sigma: {} from '
                            'mean should not be in band'.format(
                                mode[0] + 10*mode[1]))

        outliers = 0
        for p in points:
            band: Tuple[float, float, float] = dist_hist.bands.get_band(p.value)
            if band is None:
                outliers += 1

        # compute the outlier probability
        outlier_probability = outliers/len(points)
        print('OutlierProbability:', outlier_probability)
        self.assertLess(outlier_probability, 0.1,
                        'Outlier probability: {} observed is too high'
                        .format(outlier_probability))

    def test_with_narrow_bands(self) -> None:
        """
        Generates data with very narrow bands
        :return:
        """
        dist_hist = DistHist(pattern='.*outlier',
                             bin_count=1000,
                             band_computation_points=2000,
                             max_band_width=20,
                             min_band_probability=0.01,
                             min_band_probability_change=0.001)
        points = list()
        for epoch in range(1, 4000, 1):
            value = 100
            if epoch % 100 == 99:
                value = random.uniform(95,105)
            dp = DataPoint('m', 'f', {}, epoch, int(time.time()), value)
            points.append(dp)
            if dist_hist.fit(dp.value):
                # verify the bands
                for band in dist_hist.bands.bands:
                    print('Band: {} - {}, {}%'.format(int(band[0]),
                                                      int(band[1]),
                                                      int(band[2] * 100)))

        outliers = 0
        for p in points:
            band: Tuple[float, float, float] = dist_hist.bands.get_band(p.value)
            if band is None:
                outliers += 1

        # compute the outlier probability
        outlier_probability = outliers/len(points)
        print('OutlierProbability:', outlier_probability)
        self.assertLess(outlier_probability, 0.1,
                        'Outlier probability: {} observed is too high'
                        .format(outlier_probability))
