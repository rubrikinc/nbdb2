"""
RecoverySeriesStats
"""

from nbdb.store.sparse_series_stats_base import SeriesStatsBase

class RecoverySeriesStats(SeriesStatsBase):
    """
    The stats object maintains basic stats for a given series that are
    used to recovery markers. The data in the stats is a subset of
    the sparse series stats, and the essential data is the refresh epoch.
    The referesh epoch help to determine whether a marker is needed.
    """
