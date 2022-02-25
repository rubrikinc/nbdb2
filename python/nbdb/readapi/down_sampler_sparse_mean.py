"""
DownSamplerSparseMean
"""
import logging

from nbdb.readapi.down_sampler_sparse_base import DownSamplerSparseBase
from nbdb.readapi.sql_parser import FillFunc
from nbdb.readapi.time_series_response import TimeRange

logger = logging.getLogger(__name__)


class DownSamplerSparseMean(DownSamplerSparseBase):
    """
    Time-weighted mean aggregator for sparse time series
    """

    # pylint: disable-msg=R0913  # Too Many Arguments
    def __init__(self,
                 time_range: TimeRange,
                 fill_func: FillFunc,
                 fill_value: float,
                 default_fill_value: float,
                 query_id: str = None,
                 trace: bool = False):
        """
        Computes a time-weighted mean of a sparse time series
        :param time_range:
        :param fill_value: default value to use
        :param query_id: Id of the query for which this aggregator is created
        :param trace: if true then log a verbose trace
        """
        DownSamplerSparseBase.__init__(self, time_range, fill_func, fill_value,
                                       default_fill_value, query_id, trace)

    # Note do not put telemetry primitives on these methods
    # high volume methods, keep the overhead to absolute min
    def process_sparse_data(self, delta_epoch: int, value: float) -> None:
        """
        Compute the time-weighted mean for this interval
        :param delta_epoch: time interval in secs for which the value is valid
        :param value: float value or can be None if missing data
        """
        if value is not None:
            # Check if the bucket is uninitialized, we have a valid value now
            # so initialize to 0
            if self.buckets.value is None:
                self.buckets.value = 0
            self.buckets.value = self.buckets.value + delta_epoch * value
        if self.trace:
            logger.info('[Trace: %s] '
                        'DownSamplerSparseMean.process_sparse_data: '
                        'Bucket: %d BucketValue: %s DeltaEpoch: %d value: %s',
                        self.query_id,
                        self.buckets.index, str(self.buckets.value),
                        delta_epoch, str(value))

    # Note do not put telemetry primitives on these methods
    # high volume methods, keep the overhead to absolute min
    def complete_sparse_data(self) -> None:
        """
        This method should be called after all the samples for the current
        bucket values are processed. It computes the final value for the bucket
        which in this case is the time weighted mean
        :return:
        """
        if self.buckets.value is not None and \
                self.buckets.interval_valid_datapoints > 0:
            self.buckets.value = (self.buckets.value /
                                  self.buckets.interval_valid_datapoints)

        if self.trace:
            logger.info('[Trace: %s] '
                        'DownSamplerSparseMean.complete_sparse_data: '
                        'Bucket: %d BucketValue: %s Interval: %d',
                        self.query_id,
                        self.buckets.index, str(self.buckets.value),
                        self.time_range.interval)
