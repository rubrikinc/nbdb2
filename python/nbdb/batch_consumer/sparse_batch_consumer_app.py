"""
Consumer for sparse batch data ingest messages.
"""
import logging
from typing import List

from absl import app
from nbdb.common.consumer_base import ConsumerBase
from nbdb.batch_consumer.sparse_batch_consumer import SparseBatchConsumer
from nbdb.common.entry_point import EntryPoint
from nbdb.common.kafka_utils import KafkaUtils
from nbdb.common.thread_pools import ThreadPools
from nbdb.config.settings import Settings
from nbdb.schema.schema import Schema
from nbdb.batch_consumer.sparse_consumer_telemetry_helper import SparseConsumerTelemetryHelper
from nbdb.common.telemetry import Telemetry


logger = logging.getLogger()


class SparseBatchConsumerApp(EntryPoint):
    """Entry point class for sparse batch consumer"""

    def __init__(self):
        EntryPoint.__init__(self)

    def wait_for_service_ready(self) -> None:
        """ Wait for connections to be up """
        super().wait_for_service_ready()

        # Wait for Kafka topic for batch metrics to be ready
        KafkaUtils.wait_for_topic_status(
            Settings.inst.sparse_batch_consumer.kafka_brokers,
            Settings.inst.sparse_batch_consumer.topic,
            exists=True)

    # override
    def load_schema(self, schema_mapping: List) -> Schema:
        """
        Instantiate Schema from a yaml file.
        """
        # We are only loading the first schema file for batch ingestion write
        # path
        _, schema_file = schema_mapping[0].split(":")
        return Schema.load_from_file(schema_file, batch_mode=True)

    def start(self):
        """
        Start sparse batch consumers
        :return:
        """

        # We do not create druid schema for batch ingest explictly.
        # This is because Druid doesn't support creating "empty" datasources
        # Datasources are created as we create ingest tasks with schema
        # validation
        logger.info("Creating the sparse batch consumer")
        ConsumerBase.start_consumers_sync(
            create_consumer_fn=lambda: SparseBatchConsumer(
                context=self.context,
                telemetry_helper=SparseConsumerTelemetryHelper(Telemetry.inst),
                batch_consumer_settings=Settings.inst.sparse_batch_consumer,
                druid_settings=Settings.inst.Druid),
            thread_pool=ThreadPools.inst.sparse_batch_consumer_pool,
            num_consumers=Settings.inst.sparse_batch_consumer.num_consumers)

if __name__ == "__main__":
    app.run(SparseBatchConsumerApp().run)
