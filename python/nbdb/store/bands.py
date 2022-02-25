"""
Wrapper around the sklearn KernelDensityEstimator
"""
import logging
from typing import List, Tuple

logger = logging.getLogger()


class Bands:
    """
    Creates a Kernel Density estimator and fits data in a streaming fashion
    The kernel is fitted periodically as new data arrives to adjust the bands
    """
    def __init__(self,
                 pattern: str,
                 max_band_width: float,
                 min_band_probability: float,
                 min_band_probability_change: float):
        """
        Initialize the bands
        :param pattern : pattern for the bands for logging purpose
        :param max_band_width: ratio of the data range
        :param min_band_probability:
        :param min_band_probability_change:
        """
        self.pattern: str = pattern
        self.min_band_probability: float = min_band_probability
        self.max_band_width: float = max_band_width
        self.min_band_probability_change: float = min_band_probability_change
        self.bands: List[Tuple[float, float, float]] = None

    def is_ready(self) -> bool:
        """
        Checks if the kde is processed and ready for use
        :return: True if ready
        """
        return self.bands is not None

    def get_band(self, value: float) -> Tuple[float, float, float]:
        """
        Get the band that covers the value if any
        :param value:
        :return:
        """
        if not self.is_ready():
            raise ValueError('Bands are not computed yet')
        return Bands._get_band(value, self.bands)

    @staticmethod
    def _band_probability(scores: List[float]) -> float:
        """
        Compute the probability of the band values
        :param scores:
        :return:
        """
        return sum(scores)

    @staticmethod
    def _get_band(x: float,
                  bands: List[Tuple[float, float, float]]) ->\
            Tuple[float, float, float]:
        """
        Get the band that covers the given x value
        :param bands:
        :return:
        """
        for band in bands:
            if band[0] <= x <= band[1]:
                return band
        return None

    def find_bands(self, x_values: List[float], scores: List[float]) -> None:
        """
        Find the bands with data points that cover high density areas of
        the distribution subject to various constraints.
        :param x_values: list of x values sorted in increasing order
        :param scores: list of corresponding density values for x
        """
        if len(x_values) != len(scores):
            raise ValueError('Incorrect PDF values provided, length of x_'
                             'values must match the length of scores')

        # compute the band width value
        max_band_width = self.max_band_width * (x_values[-1] - x_values[0])

        # organize the PDF as a list of tuples
        # [index, X-value, density value]
        score_tuples: List[Tuple[int, float, float]] = \
            list(zip(range(len(x_values)), x_values, scores))

        # Sort the points by density in decreasing order
        sorted_score_tuples = sorted(score_tuples,
                                     key=lambda x: x[2],
                                     reverse=True)

        # Band is a tuple of low-value, high-value and probability of data in
        # the range covered by the band
        bands: List[Tuple[float, float, float]] = list()

        # these bands are computed but discarded because they did not meet
        # the minimum required probability
        disc_bands: List[Tuple[float, float, float]] = list()

        # this is a heuristic O(n) algorithm, we iterate over all points in the
        # the PDF once and try to fit them into a band.
        # The points are processed in decreasing order of density
        # In other words we start with the highest peak and try to build a
        # band around it. If the band meets the qualifying criteria we insert
        # it into bands list, if not then the band is discarded (along with
        # all the points covered by the band). points in discarded band will
        # not be considered for another band. This is why its a heuristic
        for i, x, _ in sorted_score_tuples:
            # check if this value is already captured by a band
            if Bands._get_band(x, bands) is not None:
                # already captured by a band
                continue
            # Check if this value is covered by a discarded band, if so we
            # ignore it
            if Bands._get_band(x, disc_bands) is not None:
                # already captured by a band
                continue

            # The peak of the band is at the index i
            # we expand the band on both sides left (lower value) and
            # right (higher value) one index increment at a time
            # the band stops expanding when the incremental increase in
            # probability is less then the configured
            # minimum_band_probability_change. i.e. expanding the band
            # doesn't improve the probability value of the band significantly
            # enough.
            peak = i
            # previous low index of band, prior to increment
            prev_bl = peak
            # previous high index of band, prior to increment
            prev_bh = peak
            # previous width of the band
            # (x value at prev_bh - x value at prev_bl)
            prev_bw = 0
            # Band probability
            bp = 0
            while True:
                # New low index of band >=0
                bl = max(prev_bl - 1, 0)
                # New high index of band <= maximum points in pdf
                bh = min(prev_bh + 1, len(scores) - 1)
                # low value of band
                xl = score_tuples[bl][1]
                # high value of band
                xh = score_tuples[bh][1]
                # If the previous band width is greater than 0
                # then compute the band probability
                if prev_bh != prev_bl:
                    bp = Bands._band_probability(scores[prev_bl:prev_bh])

                # check if the higher x value is already covered by an existing
                # band (if so then stop expanding)
                if Bands._get_band(xh, bands) is None:
                    # compute the new band probability after expanding to right
                    bpn = Bands._band_probability(scores[prev_bl:bh])

                    # the delta increase band probability must be greater
                    # than the configured threshold, for us to keep expanding
                    # the band
                    if bp == 0 or (bpn-bp)/bp > \
                            self.min_band_probability_change:
                        prev_bh = bh
                # check if the lower x value is already covered by an existing
                # band (if so then stop expanding)
                if Bands._get_band(xl, bands) is None:
                    # compute the probability with the increase
                    bpn = Bands._band_probability(scores[bl:prev_bh])
                    # expand the band if delta increase in probability is
                    # greater then the configured threshold
                    if bp == 0 or (bpn-bp)/bp > \
                            self.min_band_probability_change:
                        prev_bl = bl

                bw = score_tuples[prev_bh][1] - score_tuples[prev_bl][1]
                if bw <= prev_bw:
                    # We stop the loop because the band stops expanding
                    # Because of one of the following reasons
                    # 1) We have hit the boundaries
                    # 2) Hit another band boundary
                    # 3) Increasing the band doesn't significantly increase the
                    #    probability
                    break
                # We also stop the loop if the band has met the
                # maximum band width allowed limit
                if bw > max_band_width:
                    # Max bandwidth reached
                    break
                prev_bw = bw
            # Compute the probability of the computed band
            bp = Bands._band_probability(
                scores[prev_bl:prev_bh])
            band: Tuple[float, float, float] = (score_tuples[prev_bl][1],
                                                score_tuples[prev_bh][1],
                                                bp)

            # Allowed bands must meet a minimum band probability
            if bp > self.min_band_probability:
                bands.append(band)
            else:
                # probability too low to add a band, no need to look at rest
                disc_bands.append(band)
                # If we rejected 3 bands because of low probability
                # its unlikely we are going to find any more, so we can stop
                # the search early.
                # Note this is a heuristic optimization because we are
                # are searching for bands with individual points sorted by
                # density, if the distribution has lot of narrow isolated peaks
                # then we might miss a real band. However this would be a
                # degenerate case and we are favoring speed of execution
                # a critical requirement instead of trying to find the optimal
                # bands
                if len(disc_bands) > 3:
                    break
        self.bands = bands

    def log(self) -> None:
        """
        Log the bands
        """
        for band in self.bands:
            logger.info('[%s]: band: [%.2f-%.2f] : %.2f',
                        self.pattern,
                        band[0],
                        band[1],
                        band[2])
