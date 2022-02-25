"""
Main entry point for the write path, starts the metric consumers
that queries the kafka topics for new metric writes
"""
import logging

from absl import app
from absl import flags

from nbdb.common.data_point import MODE_BOTH, MODE_REALTIME, MODE_ROLLUP,\
                                    MODE_RECOVERY
from nbdb.common.entry_point import EntryPoint
from nbdb.common.kafka_utils import KafkaUtils
from nbdb.common.thread_pools import ThreadPools
from nbdb.common.tracing_config import TracingConfig
from nbdb.config.settings import Settings
from nbdb.metric_consumer.metric_consumer import MetricConsumer
from nbdb.metric_consumer.recovery_consumer import RecoveryConsumer
from nbdb.schema.schema_creator import SchemaCreator
from nbdb.store.sparse_series_utils import init_kafka_sparse_series_writer

FLAGS = flags.FLAGS
flags.DEFINE_enum("consumer_mode", default=MODE_BOTH,
                  enum_values=[MODE_REALTIME, MODE_ROLLUP, MODE_BOTH],
                  help="Consumer mode to run")

logger = logging.getLogger()


class MetricConsumerApp(EntryPoint):
    """
    Entry point class for the metric consumer
    """

    def __init__(self):
        EntryPoint.__init__(self)

    def wait_for_service_ready(self) -> None:
        # Wait for connections to be up
        super().wait_for_service_ready()

        # Wait for Kafka topic for streaming metrics to be ready
        KafkaUtils.wait_for_topic_status(
            Settings.inst.metric_consumer.kafka_brokers,
            Settings.inst.metric_consumer.topic,
            exists=True
        )

    def start(self):
        """
        Initialize the write path and start the consumers
        :return:
        """
        logger.info("Creating the SparseSeriesWriter")
        TracingConfig.initialize(Settings.inst.tracing_config)
        sparse_series_writer = init_kafka_sparse_series_writer(
            self.context, FLAGS.consumer_mode)

        logger.info("Starting the schema creator")
        SchemaCreator.start_sync(self.context)

        logger.info("Creating the metric_consumers")
        consumer = MetricConsumer(self.context,
                                  Settings.inst.metric_consumer,
                                  sparse_series_writer,
                                  FLAGS.consumer_mode)

        if FLAGS.consumer_mode == MODE_REALTIME:
            recovery_series_writer = \
                    init_kafka_sparse_series_writer(self.context,\
                                        MODE_RECOVERY)
            recovery_consumer = RecoveryConsumer(self.context,
                                        Settings.inst.metric_consumer,
                                        recovery_series_writer)
            consumer.set_recovery_consumer(recovery_consumer)

        consumer.start_consumers(ThreadPools.inst.metric_consumer_pool,
                                 Settings.inst.metric_consumer.num_consumers)

if __name__ == "__main__":
    app.run(MetricConsumerApp().run)
