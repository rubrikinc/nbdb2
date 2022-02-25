"""
SparseKafkaStore
"""
import logging
from typing import List

from confluent_kafka import Producer
from nbdb.config.settings import Settings
from pyformance import time_calls, meter_calls

from nbdb.common.data_point import DataPoint
from nbdb.common.telemetry import Telemetry
from nbdb.store.sparse_store import SparseStore

logger = logging.getLogger()


class SparseKafkaStore(SparseStore):
    """
    This class implements the SparseStore interface for Kafka
    """
    def __init__(self, connection_config: dict):
        """
        Initialize the object
        :param connection_config:
        """
        self.producer: Producer = \
            SparseKafkaStore.instantiate_producer(connection_config)
        self.points_written = 0

    @staticmethod
    def instantiate_producer(conf) -> Producer:
        """
        Create a new producer based on the settings
        :return: kafka producer
        """
        kconf = {
            # 'debug': "all"
            # 'bootstrap.servers': 'localhost:9092',
            'bootstrap.servers': conf.bootstrap.servers,
            'enable.idempotence': conf.enable.idempotence,
            'queue.buffering.max.messages': conf.queue.buffering.max.messages,
            'queue.buffering.max.kbytes': conf.queue.buffering.max.kbytes,
            'queue.buffering.max.ms': conf.queue.buffering.max.ms,
            'message.send.max.retries': conf.message.send.max.retries,
            'compression.codec': conf.compression.codec,
            'delivery.report.only.error': conf.delivery.report.only.error,
            'request.required.acks': conf.request.required.acks,
            'message.timeout.ms': conf.message.timeout.ms
        }
        return Producer(kconf)

    def write(self, data_point: DataPoint) -> None:
        """
        Write the item to the queue, this thread may block if the queue is full
        :param data_point:
        """
        if Settings.inst.logging.log_metrics:
            logger.info("SparseKafkaStore output: datasource: %s dp: %s",
                        data_point.datasource, data_point.to_druid_json_str())

        schema_settings = Settings.inst.schema_creator
        while True:
            try:
                topic = "%s_%s" % (schema_settings.druid_topic_prefix,
                                   data_point.datasource)
                self.producer.produce(topic,
                                      data_point.to_druid_json_str().
                                      encode('utf-8'))
                break
            except BufferError:
                # This is the main source of back pressure
                # giving the producer a chance to drain some of the queue
                # but with a timeout of 100ms, to avoid too long a block
                Telemetry.inst.registry.meter(
                    'SparseKafkaStore.put_batch.buffer_full').mark(1)
                queue_size = self.producer.flush(0.1)
                Telemetry.inst.registry.gauge(
                    'SparseKafkaStore.put_batch.queue_size').\
                    set_value(queue_size)

        # important to call poll after producing a bunch
        # this gives the client a chance to acknowledge previous successful
        # writes, even though we didn't really care
        self.producer.poll(0)
        self.points_written += 1
        if self.points_written > 1000:
            Telemetry.inst.registry.meter('SparseKafkaStore.write_calls').\
                mark(self.points_written)
            self.points_written = 0

    def flush(self) -> None:
        """
        Flush the current batch and block for producer to flush its buffers
        """
        self.producer.flush()
