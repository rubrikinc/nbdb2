"""
DownSamplerDenseMean
"""
import logging

from nbdb.readapi.down_sampler_dense_base import DownSamplerDenseBase

logger = logging.getLogger(__name__)


class DownSamplerDenseMean(DownSamplerDenseBase):
    """
    Mean aggregator for dense time series
    """
    # Note do not put telemetry primitives on these methods
    # high volume methods, keep the overhead to absolute min
    def process_data(self, value: float) -> None:
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
            self.buckets.value += value
            self.buckets.interval_valid_datapoints += 1
        if self.trace:
            logger.info('[Trace: %s] DownSamplerDenseMean.process_data: '
                        'Bucket: %d BucketValue: %s value: %s',
                        self.query_id,
                        self.buckets.index, str(self.buckets.value),
                        str(value))

    # Note do not put telemetry primitives on these methods
    # high volume methods, keep the overhead to absolute min
    def complete_data(self) -> None:
        """
        This method should be called after all the samples for the current
        bucket values are processed. It computes the final value for the bucket
        which in this case is the non-NULL mean
        :return:
        """
        if self.buckets.value is not None and \
                self.buckets.interval_valid_datapoints > 0:
            self.buckets.value = (self.buckets.value /
                                  self.buckets.interval_valid_datapoints)

        if self.trace:
            logger.info('[Trace: %s] DownSamplerDenseMean.complete_data: '
                        'Bucket: %d BucketValue: %s Interval: %d',
                        self.query_id,
                        self.buckets.index, str(self.buckets.value),
                        self.time_range.interval)
