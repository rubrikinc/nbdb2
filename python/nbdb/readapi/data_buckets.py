"""
DataBuckets
"""
from typing import List

from nbdb.readapi.time_series_response import TimeRange


class DataBuckets:
    """
    Represents a list of bucket values
    The bucket values are expected to be computed iteratively
    """

    def __init__(self, time_range: TimeRange):
        """
        Initialize the bucket list and sets the first bucket index
        :param time_range: query time range
        """
        self.time_range = time_range
        # Index of the current bucket being computed
        self.index: int = 0
        # Total number of buckets
        self.count: int = time_range.points()
        # The list holds values of buckets that have been computed
        self.values: List[float] = list()
        # Current bucket's value being computed
        self.value: float = None
        # Current buckets time interval during which valid data was reported
        # for none handling the self.interval_valid_datapoints is more accurate
        # then self.time_range.interval
        # if there are no None values then
        # self.interval_valid_datapoints = self.time_range.interval
        self.interval_valid_datapoints = 0

    def complete(self) -> None:
        """
        Completes the calculation of current bucket and advances the bucket
        index and initializes the values for the next bucket calculation
        """
        self.values.append(self.value)
        # reset the next bucket value
        self.value = None
        self.interval_valid_datapoints = 0
        self.index = self.index + 1

    def current_bucket_start_epoch(self) -> int:
        """
        Get the current bucket start epoch
        :return:
        """
        return self.time_range.start + self.index * self.time_range.interval

    def current_bucket_end_epoch(self) -> int:
        """
        Get the current bucket end epoch
        :return:
        """
        return self.current_bucket_start_epoch() + self.time_range.interval
