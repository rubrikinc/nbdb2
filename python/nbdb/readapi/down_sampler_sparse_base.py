"""
DownSamplerBase
"""
import logging
from typing import List, Tuple

from nbdb.common.data_point import DataPoint
from nbdb.common.data_point import MISSING_POINT_VALUE, TOMBSTONE_VALUE
from nbdb.config.settings import Settings
from nbdb.readapi.data_buckets import DataBuckets
from nbdb.readapi.dense_functions import DenseFunctions
from nbdb.readapi.time_series_response import TimeRange
from nbdb.readapi.sql_parser import FillFunc

logger = logging.getLogger(__name__)


# pylint: disable-msg=R0902  # Too Many Instance Attributes
class DownSamplerSparseBase:
    """
    FUTURE: We should write these methods in Cython
    Base aggregator class, all aggregators must inherit from this
    provides functionality around managing the intervals and invokes
    computation for each interval
    This is thread-unsafe, and should not be called from multiple threads
    """

    def __init__(self,
                 time_range: TimeRange,
                 fill_func: FillFunc,
                 fill_value: float,
                 default_fill_value: float,
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
        self.fill_func: FillFunc = fill_func
        self.fill_value: float = fill_value
        self.last_value: float = default_fill_value
        self.last_epoch: int = None
        self.last_datapoint_version: int = None
        self.query_id: str = query_id
        self.trace: bool = trace
        self.points: List[Tuple[int, float, int]] = list()
        self.buckets: DataBuckets = DataBuckets(time_range)

        self.data_gap_detection_interval = Settings.inst.sparse_store.\
            heartbeat_scan.data_gap_detection_interval
        self.termination_detection_interval = Settings.inst.sparse_store.\
            heartbeat_scan.termination_detection_interval

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

    def _process_missing_and_tombstone_marker(
            self, epoch: int, value: float) -> (bool, float):
        """
        Processes a missing / tombstone marker. It will check whether the marker
        needs to be skipped due to invalidation, or if not, return the
        modified value to be used after applying the fill function.

        :param epoch: Epoch of the marker
        :param value: Marker value. Can be missing marker value or tombstone
        value

        :return: Boolean indicating whether marker can be skipped. This
        can happen when a series migrates from one consumer to another, and
        thus generates a false marker.
        :return: Float which indicates the modified value to be used. Marker
        values are special values which need to be modified using fill
        functions.
        """
        # NOTE: We rely on the sorting of datapoints in the complete() method
        # before this function is called.
        #
        # This function assumes that the missing or tombstone markers will be
        # processed after legit datapoints for the same epochs. The sorting
        # must guarantee that for the same epochs, the markers come later.
        skipped: bool = False
        modified_value: float = value
        if value == MISSING_POINT_VALUE:
            # If a series migrates from one consumer to another temporarily,
            # and then comes back, the former will see a gap and thus might
            # insert missing markers when valid datapoints have already been
            # written by the latter consumer.
            #
            # Thus we check if there are legitimate datapoints before this
            # tombstone within the gap detection interval. If so, we mark
            # this tombstone as invalid. The interval used for checking must
            # match the interval used for creating the marker in
            # check_inline_missing_points()
            if (self.last_epoch is not None and
                    (epoch - self.last_epoch <
                     self.data_gap_detection_interval)):
                # Missing marker is invalid. Don't process
                skipped = True
                return skipped, modified_value

        elif value == TOMBSTONE_VALUE:
            # If a series migrates from one consumer to another permanently,
            # then the old consumer will think the series is dead and insert a
            # tombstone
            #
            # Thus we check if there are legitimate datapoints before this
            # tombstone within the gap detection interval. If so, we mark this
            # tombstone as invalid. The interval used for checking must match
            # the interval used for creating the marker in
            # check_offline_tombstone()
            if (self.last_epoch is not None and
                    (epoch - self.last_epoch <
                     self.data_gap_detection_interval)):
                # Tombstone marker is invalid. Don't process
                skipped = True
                return skipped, modified_value

        # This is a NULL value that needs to be filled
        modified_value = DenseFunctions.get_fill_value(self.last_value,
                                                       self.fill_func,
                                                       self.fill_value)
        return skipped, modified_value

    # Note do not put telemetry primitives on these methods
    # high volume methods, keep the overhead to absolute min
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

        if value in [MISSING_POINT_VALUE, TOMBSTONE_VALUE]:
            skipped, value = self._process_missing_and_tombstone_marker(
                epoch, value)
            if skipped:
                # No need to process marker
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

        # Note epoch can be older than start_epoch
        # we do not want to use max(epoch, start_epoch)
        # because newer datapoints take precedence over older data points
        # and we may get more than 1 data point older than start_epoch

        # Since the point could be earlier than start epoch
        # lets make sure we only consider data valid from start_epoch
        # get the current bucket epoch range [cb_start_epoch, cb_end_epoch)
        cb_start_epoch = self.buckets.time_range.start +\
                         self.buckets.index * self.buckets.time_range.interval
        cb_end_epoch = cb_start_epoch + self.buckets.time_range.interval

        while self.buckets.index < self.buckets.count and\
                epoch > cb_start_epoch:
            # we have a new point that starts after the bucket start_epoch
            # so now we can process data for this bucket
            # align the data_epoch and last_epoch to be within bucket
            # time range
            data_epoch = min(cb_end_epoch, epoch)
            data_last_epoch = max(self.last_epoch, cb_start_epoch) \
                if self.last_epoch is not None else cb_start_epoch

            delta_epoch = data_epoch - data_last_epoch
            if delta_epoch > 0:
                # Now we process the last_value received as valid for
                # delta_epoch
                self.process_sparse_data(delta_epoch, self.last_value)
                # update the valid interval for the bucket
                if self.last_value is not None:
                    self.buckets.interval_valid_datapoints += delta_epoch
            self.last_epoch = data_epoch
            if epoch < cb_end_epoch:
                # the point reported is within this bucket, so we are
                # done processing this point
                break
            # the point reported completely covered this bucket
            self.complete_sparse_data()
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
        #
        # We sort based on epoch and boolean indicating whether the value is a
        # missing marker or tombstone. If there are two datapoints with the same
        # epoch, but one has a marker value, we want the marker to be processed
        # later.
        self.points.sort(
            key=lambda point:
            (point[0], point[1] in [MISSING_POINT_VALUE, TOMBSTONE_VALUE],
             point[2]))

        if trace and query_id:
            logger.info('[Trace: %s] Datapoints: %s', query_id, self.points)

        # Now process the data
        for epoch, value, datapoint_version in self.points:
            self._post_ordering_process(epoch, value, datapoint_version,
                                        query_id, trace)

        # last_epoch may be older than query_start_epoch
        # in processing stage we have to process and ignore any unnecessary
        # datapoints that might be sent
        if self.last_epoch is not None:
            self.last_epoch = max(self.time_range.start, self.last_epoch)
        else:
            self.last_epoch = self.time_range.start
        while self.buckets.index < self.buckets.count:
            delta_epoch = self.buckets.current_bucket_end_epoch() - \
                          self.last_epoch
            self.process_sparse_data(delta_epoch, self.last_value)
            if self.last_value is not None:
                self.buckets.interval_valid_datapoints += delta_epoch
            self.last_epoch = self.buckets.current_bucket_end_epoch()
            self.complete_sparse_data()
            self.buckets.complete()

    def process_sparse_data(self, delta_epoch: int, value: float) -> None:
        """
        Process the sparse sample
        :param delta_epoch: Amount of time for which value is valid
        :param value: Value
        """
        raise NotImplementedError('process_sparse_data must be implemented')

    def complete_sparse_data(self) -> None:
        """
        Computes the current bucket value after all sparse samples are
        processed
        """
        raise NotImplementedError('complete_sparse_data must be implemented')
