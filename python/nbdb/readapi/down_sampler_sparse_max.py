"""
DownSamplerSparseMax
"""
import logging

from nbdb.readapi.down_sampler_sparse_base import DownSamplerSparseBase
from nbdb.readapi.sql_parser import FillFunc
from nbdb.readapi.time_series_response import TimeRange

logger = logging.getLogger(__name__)


class DownSamplerSparseMax(DownSamplerSparseBase):
    """
    Max aggregator for sparse time series
    """
    # Note do not put telemetry primitives on these methods
    # high volume methods, keep the overhead to absolute min
    def process_sparse_data(self, delta_epoch: int, value: float) -> None:
        """
        Compute the time-weighted mean for this interval
        :param delta_epoch: time interval in secs for which the value is valid
        :param value: float value or can be None if missing data
        """
        if value is not None:
            self.buckets.value = max(value, self.buckets.value)
        if self.trace:
            logger.info('[Trace: %s] '
                        'DownSamplerSparseMax.process_sparse_data: '
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
        # nothing to do for max
        if self.trace:
            logger.info('[Trace: %s] '
                        'DownSamplerSparseMax.complete_sparse_data: '
                        'Bucket: %d BucketValue: %s Interval: %d',
                        self.query_id,
                        self.buckets.index, str(self.buckets.value),
                        self.time_range.interval)
