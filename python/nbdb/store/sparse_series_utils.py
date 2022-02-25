"""
Utils fns for sparse series and streamline dependencies for pypy
"""
import logging

from nbdb.common.context import Context
from nbdb.common.data_point import MODE_RECOVERY
from nbdb.config.settings import Settings
from nbdb.store.sparse_kafka_store import SparseKafkaStore
from nbdb.store.sparse_series_writer import SparseSeriesWriter
from nbdb.store.recovery_series_writer import RecoverySeriesWriter

logger = logging.getLogger()

def init_kafka_sparse_series_writer(context: Context,
                                    consumer_mode: str) -> SparseSeriesWriter:
    """
    Avoid SparseSeriesWriter dependency on SparseKafkaStore and hence libraries
    like confluent_kafka not supported in pypy

    Example:- We can safely call SparseConverter in pypy which in turn
    uses SparseSeriesWriter backed by FileBackedSparse.

    Assuming Settings is initialized, creates the SparseSeriesData object
    and corresponding required objects. This is not a part of SparseSeriesWriter
    class
    This is a convenience method for initializing from a common place
    :return: SparseSeriesData object
    """
    logger.info('Connecting to the Kafka DataPointsStore')
    sparse_kafka_store = SparseKafkaStore(Settings.inst.sparse_kafka.connection_string)

    logger.info('Instantiating SparseSeriesWriter')

    if consumer_mode == MODE_RECOVERY:
        sparse_telemetry_source_id = Settings.inst.metric_consumer.topic

        return RecoverySeriesWriter(
            sparse_kafka_store,
            Settings.inst.sparse_store,
            Settings.inst.tracing_config,
            context,
            sparse_telemetry_source_id)

    past_message_lag = Settings.inst.metric_consumer.past_message_lag
    future_message_lag = Settings.inst.metric_consumer.future_message_lag

    if context.schema.batch_mode:
        sparse_telemetry_source_id = \
            Settings.inst.sparse_batch_consumer.topic
        protocol = Settings.inst.sparse_batch_consumer.metric_protocol
    else:
        sparse_telemetry_source_id = Settings.inst.metric_consumer.topic
        protocol = Settings.inst.metric_consumer.metric_protocol

    return SparseSeriesWriter(
        sparse_kafka_store,
        Settings.inst.sparse_store,
        Settings.inst.tracing_config,
        context,
        protocol,
        sparse_telemetry_source_id,
        past_message_lag,
        future_message_lag,
        consumer_mode)
