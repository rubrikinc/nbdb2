"""
TimeSeriesResponse
"""
from __future__ import annotations

import datetime
import json
import logging
import math
from dataclasses import dataclass, field
from typing import List, Dict, Tuple

from sortedcontainers import SortedSet

logger = logging.getLogger(__name__)


@dataclass
class TimeRange:
    """
    Represents a bounded range of time with group by interval
    """
    start: int
    end: int
    interval: int

    def overlap(self, t: TimeRange) -> bool:
        """
        Checks if t overlaps with self
        :param t:
        :return: true if it does false otherwise
        """
        return self.start <= t.start < self.end or\
               t.start <= self.start < t.end

    def merge(self, t: TimeRange) -> None:
        """
        Merge the overlapping range t with self
        Raises an assert if t doesn't overlap with self
        :param t:
        """
        assert self.overlap(t), 'Cannot merge non-overlapping time range'
        self.start = min(self.start, t.start)
        self.end = max(self.end, t.end)

    def points(self) -> int:
        """
        Number of points represented by this time range
        :return:
        """
        return int((self.end - self.start)/self.interval)

    def epochs(self) -> range:
        """
        Get an iterator on epochs represented by the time range
        :return:
        """
        return range(self.start, self.end, self.interval)

    def __str__(self) -> str:
        """
        :return: string representation of the TimeRange
        """
        return '[{},{}) I:{} points: {}'.format(
            self.start, self.end, self.interval, self.points())

    @staticmethod
    def sort_and_merge_ranges(time_ranges: List[TimeRange]):
        """
        Sort and merge an unordered list of time ranges
        :param time_ranges: list of unordered possibly overlapping ranges
        :return: sorted and disjointed set of ranges
        """
        sorted_ranges = sorted(time_ranges, key=lambda r: r.start)
        merged_ranges = list()
        current_range = None
        for time_range in sorted_ranges:
            if current_range is None:
                current_range = time_range
                continue
            if current_range.overlap(time_range):
                current_range.merge(time_range)
            else:
                merged_ranges.append(current_range)
                current_range = time_range
        if current_range is not None:
            merged_ranges.append(current_range)
        return merged_ranges

    @staticmethod
    def aligned_time_range(start_epoch: int, end_epoch: int, interval: int) ->\
            TimeRange:
        """
        Expands The time range and aligns to a multiple of the interval
        """
        if start_epoch is None or \
                end_epoch is None or \
                interval is None:
            # We need the full time range to align
            raise ValueError('Unable to create AlignedTimeRange with'
                             ' None values. '
                             'start_epoch={} end_epoch={} interval={}'.
                             format(start_epoch, end_epoch, interval))
        aligned_start_epoch = int(start_epoch / interval) * interval
        aligned_end_epoch = int(end_epoch / interval) * interval
        if aligned_start_epoch < start_epoch:
            aligned_start_epoch = aligned_start_epoch + interval
        if aligned_end_epoch < end_epoch:
            aligned_end_epoch = aligned_end_epoch + interval

        return TimeRange(aligned_start_epoch, aligned_end_epoch, interval)


class TimeSeries:
    """
    Time series is a single series identified by its descriptive series-id
    and list of data points (epoch:int, value:float)
    """
    # By default, Python uses a dict to store an object’s instance attributes.
    # This can be helpful as it allows setting arbitrary new attributes at
    # runtime.
    #
    # However a dict ends up requiring more memory. Therefore if you create
    # lots of objects, they end up eating more memory. __slots__ can be used to
    # bypass this and directly define a fixed set of attributes using tuples,
    # which will use less memory
    #
    # This makes a difference for cross-cluster queries where we end up using
    # ~20% less memory than without __slots__. Eg. A query for
    # clusters.*.*.Diamond.cassandratablestats.Node.Status.* uses 3.1g without
    # __slots__ and 2.5g with __slots__
    __slots__ = ("series_id", "points", "time_range", "base_series_id")

    def __init__(self,
                 series_id: str,
                 points: List[Tuple[int, float]],
                 time_range: TimeRange,
                 base_series_id: str = None):
        """
        Construct the TimeSeries, note its not a dataclass
        because we have to make sure time_range is cloned
        and we don't keep a reference
        :param series_id:
        :param points:
        :param time_range:
        :param base_series_id: Original series name
        """
        # Series ID refers to the updated series name after applying functions
        self.series_id: str = series_id
        # Base Series ID refers to the name of the original series. If not
        # provided during initialization, we default to the series ID
        self.base_series_id: str = base_series_id or series_id
        self.points: List[Tuple[int, float]] = points
        if time_range is None:
            self.time_range = None
        else:
            self.time_range: TimeRange = TimeRange(time_range.start,
                                                   time_range.end,
                                                   time_range.interval)

    def __str__(self):
        """
        Human friendly representation of the object
        :return: string
        """
        return json.dumps({'series_id' : self.series_id,
                           'points': self.points,
                           'time_range': str(self.time_range)
                           })

    def __eq__(self, other):
        """
        Compare self series and other
        :param other:
        :return:
        """
        for attr in self.__slots__:
            if getattr(self, attr) != getattr(other, attr):
                return False
        return True

    def __getitem__(self, item: int) -> Tuple[int, float]:
        """
        [] for the groups
        :param item:
        :return:
        """
        return self.points.__getitem__(item)

    def __setitem__(self, key: int, item: Tuple[int, float]) -> None:
        """
        [] for the values
        :param key:
        :param item:
        :return:
        """
        self.points[key] = item

    def append(self, item: Tuple[int, float]) -> None:
        """
        Add a data point
        :param item:
        """
        self.points.append(item)

    def num_points(self) -> int:
        """
        Returns the number of points in the time series
        :return: number of points
        """
        return len(self.points)

    def start_epoch(self) -> int:
        """
        Get the start epoch of the series
        :return:
        """
        if self.time_range is None:
            return 0
        return self.time_range.start

    def end_epoch(self) -> int:
        """
        Get the end epoch of the series
        :return:
        """
        if self.time_range is None:
            return 0
        return self.time_range.end

    def interval(self) -> int:
        """
        Get the interval for the series
        :return:
        """
        if self.time_range is None:
            return 0
        return self.time_range.interval

    def index(self, epoch: int) -> int:
        """
        Index of the point with given epoch
        :param epoch:
        :return:
        """
        return int((epoch - self.start_epoch())/self.interval())

    def get_epoch_value(self, epoch: int, fill_value: float = None) -> float:
        """
        get the value for the specified epoch
        :param epoch:
        :param fill_value: to use when we have missing data
        :return:
        """
        if self.num_points() <= 0:
            return fill_value

        index = self.index(epoch)
        if index < 0 or index >= len(self.points):
            return fill_value

        return self.points[index][1]

    def get_points(self, time_range: TimeRange) \
            -> List[Tuple[int, float]]:
        """

        :param time_range:
        :return:
        """
        if self.num_points() <= 0:
            return None

        # Restrict the requested range to what is covered by time-series
        start_epoch = self.start_epoch()
        end_epoch = self.end_epoch()
        req_start_epoch = max(start_epoch, time_range.start)
        req_end_epoch = min(end_epoch, time_range.end)

        # check if we have any valid data left
        if req_end_epoch <= req_start_epoch:
            return None

        interval = self.interval()
        start_index = int((req_start_epoch - start_epoch)/interval)
        end_index = int((req_end_epoch - start_epoch)/interval)
        return self.points[start_index:end_index]

    def values(self) -> List[float]:
        """
        List of values only
        :return: list of floating point values
        """
        return [point[1] for point in self.points]

    def non_null_values(self) -> List[float]:
        """
        Returns a list of non-null values
        :return: empty list is returned if all values are null
        """
        return list(filter(lambda x: x is not None,
                    map(lambda point: point[1],
                        self.points)))

    def append_series(self,
                      ts: TimeSeries,
                      time_range: TimeRange = None,
                      fill_value: float = None) -> None:
        """
        Append the points restricted to time_range in ts to self
        fill any gap with fill_value and return the missing time range
        :param ts:
        :param time_range:
        :param fill_value:
        """
        if ts.num_points() <= 0:
            # no-op, trying to append an empty series
            return None

        if self.num_points() <= 0:
            # If we are empty then simply extend and we are done
            if time_range is None:
                self.points.extend(ts.points)
                self.time_range = ts.time_range
            else:
                points_in_range = ts.get_points(time_range)
                if points_in_range is not None:
                    self.points.extend(points_in_range)
                if self.num_points() > 0:
                    # Always keep the time range in sync with the actual points
                    if self.time_range is not None:
                        self.time_range = TimeRange(min(self.time_range.start,
                                                        ts.time_range.start),
                                                    max(self.time_range.end,
                                                        ts.time_range.end),
                                                    time_range.interval)
                    else:
                        self.time_range = ts.time_range
            return None

        assert self.interval() == ts.interval(), \
            'cannot append series with different intervals: self={} ts={}'.\
                format(self.interval(), ts.interval())

        # since this is append only, we only fetch data that begins at the end
        # of current list, any overlap or data earlier than start of self
        # is ignored
        # identify the restricted time range for ts series
        _ts_restricted_range = ts.time_range \
            if time_range is None\
            else TimeRange(max(ts.start_epoch(), time_range.start),
                           min(ts.end_epoch(), time_range.end),
                           time_range.interval)

        if self.end_epoch() < ts.start_epoch():
            logger.debug("Merging on disjoint time series: self=%s, ts=%s, "
                         "time_range=%s", self, ts, time_range)
            # Fill any missing range with fill value
            missing_range = TimeRange(self.end_epoch(),
                                      _ts_restricted_range.start,
                                      self.interval)
            self.points.extend([(epoch, fill_value)
                                for epoch in range(missing_range.start,
                                                   missing_range.end,
                                                   self.interval())
                                ])

        # Now merge the points in ts not covered by self
        points = ts.get_points(TimeRange(self.time_range.end,
                                         _ts_restricted_range.end,
                                         self.time_range.interval))
        if points is not None and len(points) > 0:
            self.points.extend(points)
            self.time_range.end = max(time_range.end, _ts_restricted_range.end)

    @staticmethod
    def merge(ts1: TimeSeries, ts2: TimeSeries, fill_value: float = None) -> \
            TimeSeries:
        """
        Merges the two time series into a single timeseries
        Fills in missing data with 0
        :param ts1:
        :param ts2:
        :param fill_value: Fill value for any gaps in data
        :return:
        """
        assert ts1.interval() == ts2.interval(), \
            'cannot merge with different intervals: ts1={} ts2={}'.format(
                ts1.interval(), ts2.interval())

        # identify the series which starts earlier
        tsl, tsg = (ts1, ts2) \
            if ts1.start_epoch() < ts2.start_epoch() \
            else (ts2, ts1)

        tsm = TimeSeries(tsl.series_id, list(), None)
        tsm.append_series(tsl)
        if tsg.num_points() > 0:
            tsm.append_series(tsg, tsg.time_range, fill_value)
        return tsm

    def transform(self, func_name, func) -> TimeSeries:
        """
        Run the transformation function on the data points
        :param func_name:
        :param func:
        :return:
        """
        # transformation is done on the values so extract the values

        values = [point[1] for point in self.points]
        transformed_values = func(values)
        transformed_points = list(zip(self.time_range.epochs(),
                                      transformed_values))
        return TimeSeries('{}({})'.format(func_name, self.series_id),
                          transformed_points,
                          self.time_range)

    def shift(self, shift_time) -> TimeSeries:
        """
        Generate a new timeseries that is shifted in time from current series
        :param shift_time:
        :return:
        """
        shifted_points = list()
        for point in self.points:
            shifted_points.append((point[0] + shift_time, point[1]))
        shifted_series = TimeSeries('timeshift({},{})'.format(self.series_id,
                                                              shift_time),
                                    shifted_points,
                                    TimeRange(
                                        self.time_range.start + shift_time,
                                        self.time_range.end + shift_time,
                                        self.time_range.interval
                                    ))
        return shifted_series

    @staticmethod
    def _is_almost_equal(val0, val1, gap_pct: int = 0) -> bool:
        """Compare whether the values are equal with given gap_pct allowed"""
        if val0 is None or val1 is None:
            return False
        if val0 == val1:
            return True
        gap: float = abs(val1 - val0)
        min_abs_val: float = min(abs(val0), abs(val1))
        # if min value is 0, return False as the ratio is INF
        return ((gap / min_abs_val) * 100 <= gap_pct) if min_abs_val else False

    @staticmethod
    def diff_points(ts0: TimeSeries,
                    ts1: TimeSeries,
                    non_marker_lookback_val_allowed_gap_pct: int = 0) -> Dict:
        """
        If the time-series points are equal - return an empty dict.
        Otherwise, return a dict like:
          {
            'left_orphans': list of points (epoch, value) only in ts0
            'right_orphans': list of points (epoch, value) only in ts1
            'diffs': list of (epoch, val0, val1) for all epochs that have
                     a different value in ts0 vs. ts1
          }
        """
        if ts0.points == ts1.points:
            return {}

        epochs = [SortedSet(point[0] for point in ts0.points),
                  SortedSet(point[0] for point in ts1.points)]

        diff_dict = {}

        orphan_epochs = [epochs[0] - epochs[1],
                         epochs[1] - epochs[0]]
        if orphan_epochs[0]:
            diff_dict['left_orphans'] = [point for point in ts0.points
                                         if point[0] in orphan_epochs[0]]
        if orphan_epochs[1]:
            diff_dict['right_orphans'] = [point for point in ts1.points
                                          if point[0] in orphan_epochs[1]]

        diff_dict['diffs'] = []
        checking_head = True
        for epoch in epochs[0].intersection(epochs[1]):
            val = [ts0.get_epoch_value(epoch),
                   ts1.get_epoch_value(epoch)]
            if val[0] == val[1]:
                checking_head = False  # once matched - do strict equal checks
                continue
            # the value for this epoch is different between the time series.
            # if we're still checking the head of values - use allowed-gap-pct
            # in the comparison, otherwise do a strict equality check
            if not checking_head or not TimeSeries._is_almost_equal(
                    val[0], val[1], non_marker_lookback_val_allowed_gap_pct):
                diff_dict['diffs'].append(
                    (epoch, ts0.index(epoch), ts1.index(epoch), val[0], val[1]))
        if not diff_dict['diffs']:
            diff_dict.pop('diffs')

        return diff_dict


@dataclass
class TimeSeriesGroup:
    """
    TimeSeriesGroup is a map of group name to List of time-series
    """
    groups: Dict[str, List[TimeSeries]] = field(default_factory=dict)

    def __getitem__(self, item: str) -> List[TimeSeries]:
        """
        [] for the groups
        :param item:
        :return:
        """
        return self.groups.__getitem__(item)

    def __setitem__(self, key: str, item: List[TimeSeries]) -> None:
        """
        [] for the values
        :param key:
        :param item:
        :return:
        """
        self.groups[key] = item

    def add_time_series(self, group_key: str, ts: TimeSeries) -> None:
        """
        Add the time series to the specific group, creates the group key
        if it doesnt exist
        :param group_key:
        :param ts:
        :return:
        """
        if group_key not in self.groups:
            self.groups[group_key] = list()
        self.groups[group_key].append(ts)

    def num_points(self) -> int:
        """
        Returns the number of points in the time series
        :return: number of points
        """
        points = 0
        for _, time_series_list in self.groups.items():
            for time_series in time_series_list:
                if isinstance(time_series, TimeSeries):
                    series_points = time_series.num_points()
                else:
                    series_points = len(time_series)
                points = max(points, series_points)
        return points

    @staticmethod
    def create_default_group_single_series(value, time_range: TimeRange) \
            -> TimeSeriesGroup:
        """
        Creates a default TimeSeries group from a single value
        :param value:
        :param time_range:
        :return:
        """
        time_series = TimeSeries('_constant_', [(0, value)], time_range)
        return TimeSeriesGroup({'_': [time_series]})

    @staticmethod
    def to_dict(time_series_list: List[TimeSeries]) -> Dict[str, TimeSeries]:
        """

        :param time_series_list:
        :return:
        """
        ts_dict = dict()
        for time_series in time_series_list:
            ts_dict[time_series.series_id] = time_series
        return ts_dict

    @staticmethod
    def partial_merge_series(target_list: List[TimeSeries],
                             source_list: List[TimeSeries],
                             time_range: TimeRange) -> List[TimeSeries]:
        """
        Merges the data points covered by time_range in source_list
        into target_list
        :param target_list:
        :param source_list:
        :param time_range:
        :return: Target list
        """
        td = TimeSeriesGroup.to_dict(target_list)
        sd = TimeSeriesGroup.to_dict(source_list)
        for series_id, sts in sd.items():
            if sts.num_points() <= 0:
                continue
            if series_id not in td:
                # append any missed range in the beginning and end
                td[series_id] = TimeSeries(series_id, list(), None)
            td[series_id].append_series(sts, time_range)
        # append any range missed in the end
        return list(td.values())

    @staticmethod
    def partial_merge(target: TimeSeriesGroup,
                      source: TimeSeriesGroup,
                      time_range: TimeRange) -> None:
        """
        Merge the time slice [start-end] from source to target
        :param target:
        :param source:
        :param time_range:
        """
        for group_name, sl in source.groups.items():
            if group_name not in target.groups:
                target.groups[group_name] = list()
                target.groups[group_name] = \
                    TimeSeriesGroup.partial_merge_series(
                        target.groups[group_name],
                        sl,
                        time_range)

    def merge(self, tsg_other: TimeSeriesGroup) -> None:
        """
        Merge the data points in time series in tsg_other with self
        The merge is performed group wise and with matching series_id
        Groups that are present in tsg_other and not present in self
        are inserted into self
        New series in a group in tsg_other not present in the same group
        in self are inserted into the corresponding group
        :param tsg_other: Other time_series_group to merge
        """
        # loop through t1, add or merge
        for group_name, sl_other in tsg_other.groups.items():
            if group_name not in self.groups:
                # missing group just insert as is
                self.groups[group_name] = sl_other
                continue
            # merge the timeseries in this group by matching
            # against corresponding timeseries in the same group in self
            sl_self: List[TimeSeries] = self.groups[group_name]
            # ideally we should change the TimeSeriesGroup data structure
            # to store a dictionary of timeseries rather than list
            sld_self: Dict[str, TimeSeries] = TimeSeriesGroup.to_dict(sl_self)
            # scan all the series from the other group
            # so we can make sure all series in other group are merged
            for ts_other in sl_other:
                if ts_other.series_id not in sld_self:
                    # this series is missing in self list, just append
                    sl_self.append(ts_other)
                    continue
                # this series exists in self, we need to merge
                ts_self: TimeSeries = sld_self[ts_other.series_id]
                ts_merged: TimeSeries = TimeSeries.merge(ts_self, ts_other)
                ts_self.points = ts_merged.points
                ts_self.time_range = ts_merged.time_range

    def drop_points(self, max_points: int) -> None:
        """
        Drop older datapoints for all the timeseries in group if they have more
        than `max_points`

        :param max_points: Limit on the number of datapoints each timeseries
        can have.
        """
        for ts_list in self.groups.values():
            for ts in ts_list:
                if ts.num_points() > max_points:
                    shift = ts.num_points() - max_points
                    ts.points = ts.points[shift:]
                    ts.time_range = TimeRange(ts.points[0][0],
                                              ts.time_range.end,
                                              ts.time_range.interval)

    def log(self, prefix: str) -> None:
        """
        log the time series data
        :param prefix:
        """
        for group, tsl in self.groups.items():
            TimeSeriesGroup.log_tsl(prefix + ' ' + group, tsl)

    @staticmethod
    def log_tsl(prefix, tsl: List[TimeSeries]) -> None:
        """
        Log the list of timeseries
        :param prefix:
        :param tsl:
        """
        for ts in tsl:
            logger.info('%s: %s %s',
                        prefix,
                        ts.series_id,
                        ','.join(['({},{})'.format(p[0], p[1])
                                  for p in ts.points]))

    @staticmethod
    def diff(tsg0: TimeSeriesGroup,
             tsg1: TimeSeriesGroup,
             non_marker_lookback_val_allowed_gap_pct: int = 0) -> Dict:
        """
        If tsg0 == tsg1, returns an empty dict.
        Otherwise, returns a dict like:
          {
            "left_orphans": {},  # time series data that exists only in tsg0
            "right_orphans": {},  # time series data that exists only in tsg1
            "diffs": {}  # time series data that exists in both, but differs
          }
        Only relevant entries would exist in the dict, e.g. if there are no
        right orphans, then "right_orphans" would not exist in the dict.

        :param tsg0: time series group (left)
        :param tsg1: time series group (right)
        :param non_marker_lookback_val_allowed_gap_pct: the allowed gap in
            comparison of non-marker lookback values, e.g. 20 will allow up
            to 20% of difference between values, so if the values compared are
            4.3 and 4.0:
              4.3 - 4.0 = 0.3   # abs gap
              0.3 / 4.0 = 7.5%  # ratio between abs gap and min abs value
              ==> here, the compare would be successful since 7.5% <= 20%
        :param verbose_orphans: include the whole time-series data for orphans
            (i.e. time series that exists in one tsg but not in the other,
            for a common group name)
        """
        # create an empty map per kind - any map that remains empty will be
        # removed before return (i.e. there was no diff found for that kind)
        diff_dict = {
            'left_orphans': {},
            'right_orphans': {},
            'diffs': {}
        }

        tsg_group_names = [set(tsg0.groups), set(tsg1.groups)]

        # add left and right orphans for whole groups which don't intersect
        # note: since it would be too verbose to include the full list of
        # time series, and not very useful as well - specify only the count
        # of time series for that group
        if tsg_group_names[0] != tsg_group_names[1]:
            for group in tsg_group_names[0] - tsg_group_names[1]:
                diff_dict['left_orphans'][group] = len(tsg0.groups[group])
            for group in tsg_group_names[1] - tsg_group_names[0]:
                diff_dict['right_orphans'][group] = len(tsg1.groups[group])

        # compare common groups
        for group in tsg_group_names[0].intersection(tsg_group_names[1]):
            ts_dict = [TimeSeriesGroup.to_dict(tsg0.groups[group]),
                       TimeSeriesGroup.to_dict(tsg1.groups[group])]

            series_ids = [SortedSet(ts_dict[0]), SortedSet(ts_dict[1])]

            # create an empty map for this group - if it remains empty (i.e. no
            # diffs found for this group), it will be removed at loop bottom
            for kind in set(diff_dict):
                diff_dict[kind][group] = {}

            # add left and right orphans, i.e. all timeseries which are not
            # in the intersection of tsg0 and ts1
            if series_ids[0] != series_ids[1]:
                orphan_series_ids = [series_ids[0] - series_ids[1],
                                     series_ids[1] - series_ids[0]]
                if orphan_series_ids[0]:
                    diff_dict['left_orphans'][group]['orphan_series_cnt'] = \
                        len(orphan_series_ids[0])
                if orphan_series_ids[1]:
                    diff_dict['right_orphans'][group]['orphan_series_cnt'] = \
                        len(orphan_series_ids[1])

            # compare common time-series
            for series_id in series_ids[0].intersection(series_ids[1]):
                ts_diff = TimeSeries.diff_points(
                    ts_dict[0][series_id], ts_dict[1][series_id],
                    non_marker_lookback_val_allowed_gap_pct)
                for kind in set(ts_diff):
                    diff_dict[kind][group][series_id] = ts_diff[kind]

            # remove empty maps for this group
            for kind in set(diff_dict):
                if not diff_dict[kind][group]:
                    diff_dict[kind].pop(group)

        # remove empty maps
        for kind in set(diff_dict):
            if not diff_dict[kind]:
                diff_dict.pop(kind)

        return diff_dict


@dataclass
class InfluxSeriesResponse:
    """
        TimeSeriesResponse captures the result of a query against the sparse
        metric store. The response object captures the result as a dense matrix
        Grafana expects
        {"results": [{
                        "statement_id": 0,
                        "series": [{
                                    "name": "result",
                                    "columns": ["time", "value"],
                                    "values": values
                                  }]
                     }]
        }
    """
    response: dict

    @staticmethod
    def create_response(epoch_precision: str,
                        results: Dict[str, TimeSeriesGroup]) ->\
            InfluxSeriesResponse:
        """
        Converts the TimeSeriesGroup to TimeSeriesResponse object
        """
        response = dict()
        response['results'] = list()

        field_id = 0
        for field_name, ts_group in results.items():
            field_result = dict()
            field_result['statement_id'] = field_id
            field_id = field_id + 1
            field_result['series'] = list()
            for group_name, ts_list in ts_group.groups.items():
                for time_series in ts_list:
                    group_series = dict()
                    group_series['name'] = field_name
                    if group_name != '_':
                        tags = dict([p.split(':') for p in
                                     group_name.split(',')])
                        group_series['tags'] = tags

                    group_series['columns'] = ['time', 'value']
                    group_series['values'] = InfluxSeriesResponse.process_time(
                        time_series, epoch_precision)
                    field_result['series'].append(group_series)
            response['results'].append(field_result)

        return InfluxSeriesResponse(response)

    @staticmethod
    def process_time(time_series, epoch_precision: str):
        """
        Process the time component in each data point of the time-series
        depending on the precision requested
        :param time_series: timeseries
        :param epoch_precision: valid values , 's', 'ms' or None
        :return: datapoints that are desired precision for time values,
        or if precision was none, then time value is formatted date string
        """
        should_generate_dates = epoch_precision is None
        epoch_multiplier = 1
        if epoch_precision is not None:
            if epoch_precision == 'ms':
                epoch_multiplier = 1000
            elif epoch_precision == 's':
                epoch_multiplier = 1
            else:
                raise ValueError('Unsupported epoch_precision: {}'.format(
                    epoch_precision))

        processed_points = list()
        for point in time_series.points:
            if should_generate_dates:
                epoch = datetime.datetime.fromtimestamp(point[0]).isoformat()
            else:
                epoch = point[0]*epoch_multiplier

            # special handling of Not a number, grafana doesn't understand it
            if point[1] is not None and math.isnan(point[1]):
                value = None
            else:
                value = point[1]
            processed_points.append((epoch, value))

        return processed_points
