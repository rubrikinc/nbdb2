"""
BatchSparseSeriesWriter for Batch Processing
"""
from nbdb.common.context import Context
from nbdb.common.data_point import DataPoint
from nbdb.common.data_point import TOMBSTONE_VALUE
from nbdb.config.settings import Settings
from nbdb.store.sparse_series_stats import SparseSeriesStats
from nbdb.store.sparse_series_writer import SparseSeriesWriter
from nbdb.store.sparse_store import SparseStore


class BatchSparseSeriesWriter(SparseSeriesWriter):
    """
    SparseSeriesWriter for Batch Processing
    We add TOMBSTONE markers for each series after we have processed the entire
    bundle
    """

    def add_tombstone_markers(self, bundle_creation_time: int) -> None:
        """
        Add tombstone markers for all stats objects in the stats cache
        This function is expensive and called at the end of batch processing

        :param bundle_creation_time: Seconds since epoch when bundle was
        created.
        """
        for series_id, stats in self.stats_cache.items():
            tombstone_epoch = (stats.get_refresh_epoch() +
                               self.data_gap_detection_interval)

            tombstone = DataPoint.from_series_id(
                series_id, tombstone_epoch, TOMBSTONE_VALUE,
                # This is a special value. Make sure it's NOT normalized before
                # storing
                is_special_value=True,
                datapoint_version=bundle_creation_time)

            # For batch ingest, there is no concept of replay mode.
            self._write(tombstone, stats, replay_mode=False)
