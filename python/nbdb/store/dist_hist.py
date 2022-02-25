"""
OutlierMetric type, distribution learning
"""
import logging
import random
import time
from typing import Tuple, List, Dict

import distogram
from nbdb.common.data_point import DataPoint
from nbdb.store.bands import Bands
from nbdb.readapi.time_series_response import TimeRange

logger = logging.getLogger()


class DistHist:
    """
    Based on www.jmlr.org/papers/volume11/ben-haim10a/ben-haim10a.pdf
    Streaming Parallel Decision Trees

    Given a stream of data estimates the PDF of the data

    Using this PDF we identify narrow bands of region with high density of data
    """

    def __init__(self,
                 pattern: str,
                 bin_count: int,
                 band_computation_points: int,
                 max_band_width: float,
                 min_band_probability: float,
                 min_band_probability_change: float):
        """
        Initialize the Distogram
        :param pattern: Patterns that captures the series set
        :param bin_count:
        :param band_computation_points:
        :param max_band_width:
        :param min_band_probability:
        :param min_band_probability_change:
        """
        self.pattern: str = pattern
        self.bin_count: int = bin_count
        self.band_computation_points: int = band_computation_points
        self.h = distogram.Distogram(bin_count=bin_count, weighted_diff=False)
        self.bands: Bands = Bands(pattern,
                                  max_band_width,
                                  min_band_probability,
                                  min_band_probability_change)

    def is_ready(self) -> bool:
        """
        Has the bands been computed and is it ready to process the data
        :return: True if bands are available
        """
        logger.debug('DistHist.is_ready: ready=%d count=%d bin_count=%d ',
                     self.bands.is_ready(), self.get_count(), self.bin_count)
        return self.bands.is_ready()

    def fit(self, value: float) -> bool:
        """
        Fits the value into the distogram
        :param value:
        :returns True if new set of bands were computed
        """
        # this method is expensive:
        # TODO: For stead state to reduce the overhead we should add sampling
        # logic, not every point received needs to update the distogram
        # However during initial warmup all points should be fed
        self.h = distogram.update(self.h, value)
        # Check if we have atleast the bin count and the band recomputation
        # count is met
        if self.get_count() > self.bin_count and \
                self.get_count() % self.band_computation_points >= \
                self.band_computation_points - 1:
            logger.info('Regenerating bands: %s '
                        'count: %d bin_count:%d bins:%d min:%f max:%f',
                        self.pattern,
                        self.get_count(),
                        self.bin_count,
                        len(self.h.bins),
                        self.h.min,
                        self.h.max)
            x, pdf = self.get_pdf(self.bin_count)
            self.bands.find_bands(x, pdf)
            self.bands.log()
            return True
        return False

    def get_pdf(self, bins) -> Tuple[List[float], List[float]]:
        """
        Generates a PDF with the given number of bins
        :param bins:
        :return: list of x values and list of corresponding probabilities
        """
        count = self.get_count()
        ucount = bins
        if ucount > len(self.h.bins):
            ucount = len(self.h.bins)
        # the distogram may throw an error if bin_count configured is more
        # than actual computed bins, however this particular check doesn't
        # seem to add any value. Its also not possible to know apriori the
        # bins computed will be.
        # the histogram returned is reasonable quality when bin_count is higher
        # then len(h.bins). So we are effectively disabling the check
        if self.h.bin_count > len(self.h.bins):
            self.h.bin_count = len(self.h.bins)
        hist = distogram.histogram(self.h, ucount=ucount)
        # restore the original bin_count
        self.h.bin_count = self.bin_count
        return [x for (x, _) in hist], [y/count for (_, y) in hist]

    def get_count(self) -> int:
        """
        Number of data points reported so far
        :return:
        """
        return distogram.count(self.h)

    @staticmethod
    def generate_multi_modal_series(time_range: TimeRange,
                                    datasource: str,
                                    field: str,
                                    tags: Dict[str, str],
                                    modes: List[Tuple[float, float, float]],
                                    data_range: Tuple[float, float]
                                    ) -> List[DataPoint]:
        """
        Generates a list of data points
        :param time_range:
        :param datasource:
        :param field:
        :param tags:
        :param modes:
        :param data_range:
        :return: List of data points from the multi modal distribution
        """
        data_points: List[DataPoint] = list()
        for epoch in time_range.epochs():
            band_selector = random.uniform(0, 100)
            value: float = None
            for mode in modes:
                if band_selector < mode[2]:
                    value = min(data_range[1],
                                max(data_range[0],
                                    random.gauss(mode[0], mode[1])))
                    break
            if value is None:
                value = random.uniform(data_range[0], data_range[1])
            data_points.append(DataPoint(datasource,
                                         field,
                                         tags,
                                         epoch,
                                         int(time.time()),
                                         value))
        return data_points
