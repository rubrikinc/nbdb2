"""DenseConsumerTelmetry"""

from nbdb.common.telemetry import Telemetry
from nbdb.store.file_backed_sparse_store import FilestoreManifest
from nbdb.batch_consumer.utils import DenseBatchMessage


class DenseConsumerTelmetryHelper:
    """
    Telemetry helper for dense consumer.
    """

    def __init__(self, telemetry: Telemetry):
        self.telemetry = telemetry
        self.drop_ratio_hist = self.telemetry.registry.histogram(
            'DenseBatchConsumer.sparse_converter.drop_ratio')

    def update_on_successful_conversion(self,
                                        message: DenseBatchMessage,
                                        manifest: FilestoreManifest):
        """
        Update metrics post successful dense -> sparse conversion.
        """
        self._update_drop_ratio(manifest)
        # Update size histograms
        self._update_dense_input_size_histogram(message.size_bytes)
        self._update_sparse_output_size_histogram(manifest.total_size_bytes)
        # Update points histograms
        self._update_dense_points_histogram(manifest.total_points_processed)
        self._update_sparse_points_histogram(manifest.total_points_written)
        # Update per datasource metrics
        self._update_per_datasource_points_dist(manifest)

    def update_conversion_rate(self, duration_secs: float, points: int):
        """Update metrics to track rate of conversion."""
        _ = duration_secs
        self.telemetry.registry.meter(
            "DenseBatchConsumer.sparse_converter.rate").mark(points)

    def _update_per_datasource_points_dist(self,
                                           manifest: FilestoreManifest):
        for (datasource, info) in manifest.datasource_info.items():
            rate_meter = self.telemetry.registry.meter(
                f"DenseBatchConsumer.sparse_output.datasources.{datasource}."
                "processed"
            )
            rate_meter.mark()
            # This meter helps us track points written per datasource
            points_meter = self.telemetry.registry.meter(
                f"DenseBatchConsumer.sparse_output.datasources.{datasource}."
                "points_written"
            )
            points_meter.mark(info.points_written)

    def _update_sparse_output_size_histogram(self, size_bytes: int):
        size_mbs = int(size_bytes / (1024 ** 2))
        bucket = self.get_log_bucket(size_mbs)
        self.telemetry.registry.meter(
            f"DenseBatchConsumer.sparse_size_histogram_mbs.{bucket}").mark()

    def _update_dense_input_size_histogram(self, size_bytes: int):
        size_mbs = int(size_bytes / (1024 ** 2))
        bucket = self.get_log_bucket(size_mbs)
        self.telemetry.registry.meter(
            f"DenseBatchConsumer.dense_size_histogram_mbs.{bucket}").mark()

    def _update_dense_points_histogram(self, num_points: int):
        points_millions = int(num_points / (10 ** 6))
        bucket = self.get_log_bucket(points_millions)
        self.telemetry.registry.meter(
            f"DenseBatchConsumer.dense_points_histogram_mil.{bucket}").mark()

    def _update_sparse_points_histogram(self, num_points: int):
        points_millions = int(num_points / (10 ** 6))
        bucket = self.get_log_bucket(points_millions)
        self.telemetry.registry.meter(
            f"DenseBatchConsumer.sparse_points_histogram_mil.{bucket}").mark()

    def _update_drop_ratio(self, manifest: FilestoreManifest):
        if manifest.total_points_processed > 0:
            drop_ratio: float = 1 - (manifest.total_points_written /
                                    manifest.total_points_processed)
            self.drop_ratio_hist.add(drop_ratio)

    @staticmethod
    def get_log_bucket(n: int):
        """Get log 2 bucket for a number."""
        if n == 0: # pylint: disable=no-else-return
            return 0
        else:
            return 1 << (n.bit_length() - 1)
