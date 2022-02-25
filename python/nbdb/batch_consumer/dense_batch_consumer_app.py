"""
Consumer for dense batch data ingest messages.
"""
import logging
from typing import List

from absl import app, flags
from nbdb.batch_consumer.dense_batch_consumer import DenseBatchConsumer
from nbdb.common.data_point import MODE_BOTH, MODE_REALTIME, MODE_ROLLUP
from nbdb.common.entry_point import EntryPoint
from nbdb.common.kafka_utils import KafkaUtils
from nbdb.common.thread_pools import ThreadPools
from nbdb.config.settings import Settings
from nbdb.schema.schema import Schema

logger = logging.getLogger()

FLAGS = flags.FLAGS
flags.DEFINE_string('batch_filter_file', None, 'Batch filter rules file path')
flags.DEFINE_enum("consumer_mode", default=MODE_BOTH,
                  enum_values=[MODE_REALTIME, MODE_ROLLUP, MODE_BOTH],
                  help="Consumer mode to run")


class DenseBatchConsumerApp(EntryPoint):
    """Entry point class for dense batch consumer"""

    def __init__(self):
        EntryPoint.__init__(self)

    def wait_for_service_ready(self) -> None:
        """ Wait for connections to be up """
        super().wait_for_service_ready()

        # Wait for Kafka topic for batch metrics to be ready
        KafkaUtils.wait_for_topic_status(
            Settings.inst.dense_batch_consumer.kafka_brokers,
            Settings.inst.dense_batch_consumer.topic,
            exists=True)

    # override
    def load_schema(self, schema_mapping: List) -> Schema:
        """
        Instantiate Schema from a yaml file.
        """
        # Schema mapping consists of multiple database to schema path mappings
        # Eg. --schema_mapping default:config/schema.yaml
        #     --schema_mapping batch:config/batch_schema.yaml
        _ = self

        # We are only loading the first schema file for batch ingestion write
        # path
        _, schema_file = schema_mapping[0].split(":")
        return Schema.load_from_file(schema_file, batch_mode=True)

    def start(self):
        """
        Start dense batch consumers
        :return:
        """

        logger.info("Creating the dense batch consumer")
        consumer = DenseBatchConsumer(
            context=self.context,
            sparse_batch_settings=Settings.inst.sparse_batch_consumer,
            dense_batch_settings=Settings.inst.dense_batch_consumer,
            setting_file=FLAGS.setting_file,
            schema_mapping=FLAGS.schema_mapping,
            batch_filter_file=FLAGS.batch_filter_file,
            consumer_mode=FLAGS.consumer_mode
        )
        consumer.start_consumers(
            ThreadPools.inst.dense_batch_consumer_pool,
            Settings.inst.dense_batch_consumer.num_consumers)

if __name__ == "__main__":
    app.run(DenseBatchConsumerApp().run)
