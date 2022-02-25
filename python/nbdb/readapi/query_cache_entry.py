"""
QueryCacheEntry
"""

from __future__ import  annotations

from dataclasses import dataclass
from typing import Dict, Tuple

from nbdb.readapi.time_series_response import TimeRange, TimeSeriesGroup


@dataclass
class QueryCacheEntry:
    """
    A data entry for a query for a contiguous period of time
    """
    time_range: TimeRange
    result: Dict[str, TimeSeriesGroup]

    def __str__(self):
        """
        Human readable representation for logging
        :return:
        """
        return 'QE[time_range: {}, result: {}'.format(
            self.time_range,
            '\n'.join(['{}={}'.format(key, str(tsg))
                       for key, tsg in self.result.items()])
        )

    def size(self) -> Tuple[int, int]:
        """
        Estimate the size of the entry
        :return: size in bytes and points
        """
        size = 0
        points = 0
        for name, tsg in self.result.items():
            size = size + len(name)
            for group, time_series_list in tsg.groups.items():
                size = size + len(group)
                for time_series in time_series_list:
                    size = size + len(time_series.series_id)
                    size = size + time_series.num_points()*4
                    points = points + time_series.num_points()
        return size * 2, points

    def overlaps(self, entry: QueryCacheEntry) -> bool:
        """
        Checks if the time range overlaps between the two entries
        :param entry:
        :return:
        """
        if self.time_range.start <= entry.time_range.start \
                <= self.time_range.end:
            return True

        if entry.time_range.start <= self.time_range.start \
                <= entry.time_range.end:
            return True

        return False

    def less(self, entry: QueryCacheEntry) -> bool:
        """
        Returns true if self start time is earlier than entry
        :param entry:
        :return:
        """
        return self.time_range.start < entry.time_range.start

    def merge(self, e: QueryCacheEntry) -> None:
        """
        Merges the data in entry into the current entry
        :param e:
        """
        assert self.overlaps(e), 'Only overlapping entries can merge'
        assert self.time_range.interval == e.time_range.interval, \
            'Only entries with same interval can be merged: ' \
            'self: {} e: {}'.format(self.time_range.interval,
                                    e.time_range.interval)

        for name, tsge in e.result.items():
            if name not in self.result:
                self.result[name] = tsge
                continue
            tsgs = self.result[name]
            tsgs.merge(tsge)

        merged_range = TimeRange(min(self.time_range.start,
                                     e.time_range.start),
                                 max(self.time_range.end,
                                     e.time_range.end),
                                 self.time_range.interval)

        self.time_range = merged_range

    def drop_points(self, max_points: int) -> None:
        """
        Drop older points if more than `max_points` exist in cache entry.
        :param max_points:
        """
        # We need to adjust the cache entry time range if it exceeds max points
        if self.time_range.points() > max_points:
            # Iterate over all timeseries groups and drop points
            for tsge in self.result.values():
                tsge.drop_points(max_points)

            # Shift global cache entry time range
            shift = self.time_range.points() - max_points
            merged_range = TimeRange(self.time_range.start + shift *
                                     self.time_range.interval,
                                     self.time_range.end,
                                     self.time_range.interval)
            self.time_range = merged_range
