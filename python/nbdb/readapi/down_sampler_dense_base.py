"""
DownSamplerBase
"""
import logging
from typing import List, Tuple

from nbdb.common.data_point import DataPoint
from nbdb.readapi.data_buckets import DataBuckets
from nbdb.readapi.time_series_response import TimeRange

logger = logging.getLogger(__name__)


# pylint: disable-msg=R0902  # Too Many Instance Attributes
class DownSamplerDenseBase:
    """
    FUTURE: We should write these methods in Cython
    Base dense aggregator class, all aggregators must inherit from this
    provides functionality around managing the intervals and invokes
    computation for each interval
    This is thread-unsafe, and should not be called from multiple threads
    """

    def __init__(self,
                 time_range: TimeRange,
                 query_id: str,
                 trace: bool
                 ):
        """
        Initialize the object
        :param fill_value: default value to use
        :param query_id: Id of the query for which this aggregator is created
        :param trace: if true then log a verbose trace
        """
        self.time_range: TimeRange = time_range
        self.last_value: float = None
        self.last_epoch: int = None
        self.last_datapoint_version: int = None
        self.query_id: str = query_id
        self.trace: bool = trace
        self.points: List[Tuple[int, float, int]] = list()
        self.buckets: DataBuckets = DataBuckets(time_range)

    # Note do not put telemetry primitives on these methods
    # high volume methods, keep the overhead to absolute min
    def process(self, epoch: int, value: float,
                datapoint_version: int = None) -> None:
        """
        Process a data point. Note the epochs may not be sorted
        we have to sort them before computing the group by aggregate
        This method just caches the points in memory
        :param epoch:
        :param value:
        :return:
        """
        self.points.append((epoch, value, datapoint_version))

    # Note do not put telemetry primitives on these methods
    # high volume methods, keep the overhead to absolute min
    # pylint: disable-msg=R0913  # Too Many Arguments
    def _post_ordering_process(self, epoch: int, value: float,
                               datapoint_version: int,
                               query_id: str, trace: bool) -> bool:
        """
        Process a data point, the process method must be called in
        increasing order of epochs. This method assumes such an order
        :param epoch: can be less than query start epoch
        :param value:
        :param datapoint_version: Indicates the bundle version which generated
        this datapoint. We use the version to decide whether this marker can be
        invalidated because we have already seen newer bundle datapoints
        :param query_id:
        :param trace:
        :return: True if successful else False
        """
        if self.last_epoch is not None and epoch < self.last_epoch:
            logger.error('epoch: %d < last_epoch: %d last_value: %f new data '
                         'points must be processed in inc order of time ',
                         epoch, self.last_epoch, self.last_value)
            return False
        if epoch > self.time_range.end:
            logger.error('epoch:%d > end_epoch:%d data point provided past '
                         'aggregator range', epoch, self.time_range.end)
            return False

        if self.last_datapoint_version is not None:
            # Datapoint versions are available. Compare to the last non-skipped
            # datapoint version
            if datapoint_version is None:
                # Last datapoint had a version but this one doesn't. We should
                # never run into these situations. But if we do, we will ignore
                # non-versioned datapoints
                return True
            if datapoint_version < self.last_datapoint_version:
                # If the current version is lower, then we can skip this
                # because we have already seen newer bundle datapoints
                return True

        # We normalize values before storing to druid, have to denormalize
        # before consuming. See comments on the normalize_value method
        #
        # This is the right spot to denormalize because we have already dealt
        # with missing point & tombstone markers by now
        if value is not None and value < 0:
            value = DataPoint.denormalize_value(value)
            if trace and query_id:
                logger.info('[Trace: %s] Den DP (%d %f)', query_id, epoch,
                            value)

        # Note epoch can be older than start_epoch. We want to ignore any
        # datapoints outside [start_epoch, end_epoch)
        # Get the current bucket epoch range [cb_start_epoch, cb_end_epoch)
        cb_start_epoch = self.buckets.time_range.start +\
                         self.buckets.index * self.buckets.time_range.interval
        cb_end_epoch = cb_start_epoch + self.buckets.time_range.interval

        while (self.buckets.index < self.buckets.count and
               epoch >= cb_start_epoch):
            if epoch < cb_end_epoch:
                # We have a point which lies exactly within the current bucket
                # boundaries. We can process it safely
                self.process_data(value)
                # Since we have processed the datapoint in the right bucket, we
                # are done
                break

            # Datapoint lies outside the current bucket. Keep moving the bucket
            # forward till we find something that fits
            self.complete_data()
            self.buckets.complete()
            cb_start_epoch = self.buckets.time_range.start + \
                             self.buckets.index * \
                             self.buckets.time_range.interval
            cb_end_epoch = cb_start_epoch + self.buckets.time_range.interval

        self.last_epoch = epoch
        self.last_value = value
        self.last_datapoint_version = datapoint_version
        return True

    # Note do not put telemetry primitives on these methods
    # high volume methods, keep the overhead to absolute min
    def complete(self, query_id: str = None, trace: bool = False) -> None:
        """
        This method must be called after all points are processed
        to finish any incomplete buckets
        :return:
        """
        # So far we have only cached points in memory because they may
        # not be sorted
        # We sort based on epoch and datapoint version only
        self.points.sort(
            key=lambda point: (point[0], point[2]))

        if trace and query_id:
            logger.info('[Trace: %s] Datapoints: %s', query_id, self.points)

        # Now process the data in order
        for epoch, value, datapoint_version in self.points:
            self._post_ordering_process(epoch, value, datapoint_version,
                                        query_id, trace)

        # Last datapoint received may be far from the bucket end. Run through
        # the remaining buckets and complete them
        while self.buckets.index < self.buckets.count:
            self.complete_data()
            self.buckets.complete()

    def process_data(self, value: float) -> None:
        """
        Process the current nonsparse sample
        :param value: Value
        """
        raise NotImplementedError('process_data() must be implemented')

    def complete_data(self) -> None:
        """
        Computes the current bucket value after all nonsparse samples are
        processed
        """
        raise NotImplementedError('complete_data() must be implemented')
