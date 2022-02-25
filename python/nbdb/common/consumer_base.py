"""
Indexing Consumer
"""
import logging
import time
import traceback
from typing import Optional, List, Callable
from concurrent.futures.thread import ThreadPoolExecutor

import confluent_kafka
from confluent_kafka import TopicPartition, Consumer, KafkaException
from nbdb.common.telemetry import Telemetry
from nbdb.common.thread_pools import ThreadPools

logger = logging.getLogger()


class ConsumerBase:
    """
    Base class for all kakfa consumers
    """

    def __init__(self,
                 topic,
                 servers,
                 group_id,
                 deserializer,
                 # Default to Kafka's default of 5 minutes
                 max_poll_interval_ms: int = 300000):
        """Init.

        :param topic: Kafka topic from which to fetch index docs
        :param servers: Kafka broker server URLs to talk to.
        :param group_id: Kafka consumer group id
        :param deserializer: lambda for de-serializing values
        """
        self.name = self.__class__.__name__
        self.topic = topic
        self.servers = servers
        self.group_id = group_id
        self.deserializer = deserializer
        self.max_poll_interval_ms = max_poll_interval_ms
        # Boolean which indicates whether the offset belonging to the current
        # message being processed can be marked as committed
        self._commit_req = False
        self._commit_offsets: Optional[List[TopicPartition]] = None

        self.consumer: Consumer = None
        self.terminate = False

    def mark_commit_req(self, offsets: Optional[List[TopicPartition]] = None):
        """
        Used by child classes to indicate when to commit offsets

        :param offsets: Optional offsets to commit. If not defined, the
                        assigned partitions' current offset is commited.
        """
        self._commit_req = True
        self._commit_offsets = offsets

    def on_assign(self, consumer: Consumer, partitions: List[TopicPartition]):
        """
        Callback invoked on a successful partition re-assignment.
        Override in child class if you want to do something special
        """
        _, _, _ = self, consumer, partitions

    def start_consuming(self):
        """
        Starts the single threaded kafka consumer for fetching index docs
        :return:
        """
        conf = {'bootstrap.servers': self.servers,
                'group.id': self.group_id,
                'auto.offset.reset': 'earliest',
                # We require the consumer to manually commit offsets
                'enable.auto.commit': False,
                #'auto.commit.interval.ms': 60000,
                'fetch.min.bytes': 1024*1024,
                'max.poll.interval.ms': self.max_poll_interval_ms}
        consumer = Consumer(conf)
        self.consumer = consumer

        consumer.subscribe([self.topic], on_assign=self.on_assign)

        logger.info('Consumer started')

        while not ThreadPools.inst.is_app_exiting() and not self.terminate:
            msg = consumer.poll(timeout=1)
            if msg is None:
                continue
            if msg.error():
                Telemetry.inst.registry.meter(
                    self.name + '.consumer_poll_failed').mark()
                logger.error('%s: Failed to process: %s '
                             'Exception: %s ST: %s',
                             self.name, msg, msg.error(),
                             traceback.format_exc())
            else:
                # Proper message
                try:
                    self.process_item(self.deserializer(msg.value()),
                                      msg.partition(), msg.offset())
                    if self._commit_req:
                        # Commit requested. Commit offsets in a blocking manner
                        if self._commit_offsets is not None:
                            consumer.commit(asynchronous=False,
                                            offsets=self._commit_offsets)
                        else:
                            consumer.commit(asynchronous=False)
                        self._commit_req = False
                        self._commit_offsets = None

                # pylint: disable-msg=W0703 # Broad Except
                except Exception as err:
                    # This is main consumer loop, under no circumstance this
                    # loop should break as it will create an operational outage
                    # So we have to catch all possible exceptions and log /
                    # report them. But keep accepting new messages that are
                    # correctly formatted
                    Telemetry.inst.registry.meter(
                        measurement=f'{self.name}.process_items_failed',
                        tag_key_values=[f"Topic={self.topic}"]
                    ).mark()
                    logger.error('%s: Failed to process: %s '
                                 'Exception: %s ST: %s',
                                 self.name, msg.value(), err,
                                 traceback.format_exc())

            # Check if the app is exiting before consuming a new message
            if ThreadPools.inst.is_app_exiting():
                logger.info('ConsumerBase: %s.start_consuming: '
                            'ThreadPool.is_app_exiting returned True'
                            '. Quitting the consumer loop', __name__)
                break

    def process_item(self, message, partition, offset) -> None:
        """
        Process the item provided
        :param message:
        :param partition:
        :param offset:
        :return:
        """
        raise NotImplementedError(
            'Child class must implement the process_item method')

    def start_consumers(self,
                        thread_pool: ThreadPoolExecutor,
                        num_consumers: int, wait: bool=True) -> None:
        """
        Start num_consumer threads, each thread will instantiate a separate
        kafka consumer
        :param thread_pool: ThreadPoolExecutor to use for submitting consumers
        :param num_consumers: Number of consumers to create
        This thread will block until all consumers are finished
        """
        logger.info('ConsumerBase: %s.start_consumers: '
                    'Starting %d consumer threads', __name__, num_consumers)

        consumer_futures = list()
        for _ in range(num_consumers):
            consumer_futures.append(
                thread_pool.submit(self.start_consuming))

        if wait:
            for consumer_future in consumer_futures:
                consumer_future.result()

    @staticmethod
    def start_consumers_sync(create_consumer_fn: Callable[[], 'ConsumerBase'],
                             thread_pool: ThreadPoolExecutor,
                             num_consumers: int) -> None:
        """
        Start num_consumer threads, each thread will instantiate a separate
        kafka consumer
        :param thread_pool: ThreadPoolExecutor to use for submitting consumers
        :param num_consumers: Number of consumers to create
        This thread will block until all consumers are finished
        """
        logger.info('ConsumerBase: %s.start_consumers: '
                    'Starting %d consumer threads', __name__, num_consumers)
        consumer_futures = list()
        for _ in range(num_consumers):
            consumer = create_consumer_fn()
            consumer_futures.append(
                thread_pool.submit(
                    consumer.start_consuming))

        for consumer_future in consumer_futures:
            consumer_future.result()

    @staticmethod
    def _rewind_partitions(consumer: Consumer,
                           committed_partitions: List[TopicPartition],
                           duration_secs: int) -> None:
        """
        Rewind or reset assigned partitions to older offsets.

        This method first searches for offsets which are older than the current
        offset by `duration_secs`. Then it resets the offsets to the desired
        value, so that the consumer can run in replay mode.
        """
        logger.info("_rewind_partitions(): Duration %d secs", duration_secs)
        timestamp_ms = int((time.time() - duration_secs) * 1000)
        search_partitions = [TopicPartition(p.topic, p.partition, timestamp_ms)
                             for p in committed_partitions]
        try:
            output_partitions = consumer.offsets_for_times(search_partitions)
        except KafkaException as e:
            logger.error("Unable to find old partition offsets. "
                         "Error %s, ST: %s", str(e), traceback.format_exc())
            # This shouldn't happen except in the case where there have been
            # zero messages generated for the assigned partition. In such a
            # case, we can quietly continue
        else:
            # Successfully found the old offsets. Change the current offsets
            # to point to the old offsets
            # NOTE: Can't use seek() which throws an error. Recommended
            # solution is to use assign():
            # https://github.com/edenhill/librdkafka/wiki/
            # Manually-setting-the-consumer-start-offset
            consumer.assign(output_partitions)
            logger.info("_rewind_partitions(): assign offsets successful")

