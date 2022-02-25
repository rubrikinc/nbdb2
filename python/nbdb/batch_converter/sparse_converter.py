"""
SparseConverter
"""
import logging
import time
from typing import Callable

from pyformance import time_calls

from nbdb.batch_converter.batch_json_parser import BatchJsonParser
from nbdb.common.context import Context
from nbdb.common.telemetry import Telemetry
from nbdb.common.tracing_config import TracingConfig
from nbdb.config.settings import Settings
from nbdb.store.file_backed_sparse_store import FileBackedSparseStore
from nbdb.store.batch_sparse_series_writer import  BatchSparseSeriesWriter

logger = logging.getLogger()

class SparseConverter: # pylint: disable=too-few-public-methods
    """
    SparseConverter applies sparseness transformations to a AnomalyDB batch
    JSON and produces a FileBackedSparseStore, which can directly be ingested
    into AnomalyDB.
    """
    def __init__(self,
                 context: Context,
                 settings: Settings,
                 min_storage_interval: int,
                 is_local: bool = False):
        """
        Initialize SparseConverter

        :param schema_path: Path of the schema yaml file
        :param settings_path: Path of the settings yaml file
        """
        self.min_storage_interval = min_storage_interval
        self.context = context
        if TracingConfig.inst is None:
            TracingConfig.initialize(settings.tracing_config, is_local)
        if Telemetry.inst is None:
            Telemetry.initialize()
        self.settings = settings
        self.sparse_store_settings = self.settings.sparse_store
        self.tracing_config_settings = self.settings.tracing_config

    @time_calls
    def convert(self,
                cluster_id: str,
                batch_json_path: str,
                output_dir_path: str,
                batch_filter_file: str,
                bundle_creation_time: int,
                compress: bool,
                consumer_mode: str,
                progress_callback: Callable[[float, int], None]
                ) -> FileBackedSparseStore:
        """
        Convert batch JSON to FileBackedSparseStore.

        :param progress_callback:
            Function to be called periodically.
            Takes arguments (duration_secs, points_processed), where
            `points_processed` is the number of points processed in the
            last `duration_secs` seconds
        """
        (batch_parser, file_backed_store, sparse_writer) = \
            self._init_deps(
                cluster_id, batch_json_path, output_dir_path,
                batch_filter_file, bundle_creation_time, consumer_mode,
                compress)
        start_time = time.time()

        NUM_ITERATIONS_TO_LOG = 100000
        num_processed_since_logging = 0
        num_processed_total = 0
        logging_start_time = time.time()

        with batch_parser:
            for data_point in batch_parser:
                sparse_writer.append(data_point, replay_mode=False)
                num_processed_since_logging += 1
                num_processed_total += 1
                if num_processed_since_logging >= NUM_ITERATIONS_TO_LOG:
                    time_taken = time.time() - logging_start_time
                    logging_start_time = time.time()
                    num_processed_since_logging = 0
                    progress_callback(time_taken, NUM_ITERATIONS_TO_LOG)
                    logger.info(
                        "Processed %d datapoints in %f seconds. Total datapoints"
                        " so far: %d", NUM_ITERATIONS_TO_LOG, time_taken,
                        num_processed_total)

            # Add tombstone markers after processing the entire bundle.
            # Tombstone validation will be done by the read side, where it will
            # deal with bundle overlaps and conflicts
            sparse_writer.add_tombstone_markers(bundle_creation_time)
            sparse_writer.flush_writes()
        file_backed_store.set_total_points_processed(num_processed_total)
        file_backed_store.close()
        end_time = time.time()

        logger.info("Conversion took %d seconds", end_time - start_time)
        logger.info("Number of data points processed - flat: %d,"
                        " tagged - %d, skipped - %d",
                    batch_parser.num_flat,
                    batch_parser.num_tagged,
                    batch_parser.num_skipped)
        manifest_map = FileBackedSparseStore.parse_datasources_from_manifest(
            output_dir_path)
        logger.info("Number of data points written - %d",
                    file_backed_store.points_written)
        pct_written = (file_backed_store.points_written /
                        (batch_parser.num_flat +
                         batch_parser.num_tagged -
                         batch_parser.num_skipped))
        logger.info("Percentage reduction - %.2f%%", (1 - pct_written) * 100)
        logger.info("File backed store manifest map - %s", manifest_map)

        return file_backed_store

    def _init_deps(self,
                   cluster_id: str,
                   batch_json_path: str,
                   output_dir_path: str,
                   batch_filter_file: str,
                   bundle_creation_time: int,
                   consumer_mode: str,
                   compress: bool) -> \
        (BatchJsonParser, FileBackedSparseStore, BatchSparseSeriesWriter):
        batch_parser = BatchJsonParser(
            datasource_getter_fn=self.context.schema.get_datasource,
            min_storage_interval=self.min_storage_interval,
            cluster_id=cluster_id,
            filepath=batch_json_path,
            batch_filter_file=batch_filter_file,
            bundle_creation_time=bundle_creation_time)

        file_backed_store = FileBackedSparseStore(output_dir_path, compress)
        dense_settings = self.settings.dense_batch_consumer
        sparse_settings = self.settings.sparse_batch_consumer
        sparse_writer = BatchSparseSeriesWriter(
            sparse_store=file_backed_store,
            sparse_store_settings=self.sparse_store_settings,
            tracing_config_settings=self.tracing_config_settings,
            context=self.context,
            protocol=self.settings.sparse_batch_consumer.metric_protocol,
            sparse_telemetry_source_id=sparse_settings.topic,
            past_message_lag=dense_settings.past_message_lag,
            future_message_lag=dense_settings.future_message_lag,
            consumer_mode=consumer_mode
        )
        return (batch_parser, file_backed_store, sparse_writer)
