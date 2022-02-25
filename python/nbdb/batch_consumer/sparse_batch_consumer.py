"""
SparseBatchConsumer
"""
import json
import logging
import time
from dataclasses import dataclass
from collections import deque
from typing import List, Dict, Set
import random
import string
from datetime import datetime, timedelta

from confluent_kafka import TopicPartition
from pyformance import time_calls

from nbdb.common.consumer_base import ConsumerBase
from nbdb.common.context import Context
from nbdb.common.druid_admin import DruidAdmin, DruidTaskStatus, \
    DruidTaskTypeFilter, DruidTaskStateFilter
from nbdb.config.settings import SparseBatchConsumerSettings, DruidSettings
from nbdb.batch_consumer.utils import SparseBatchMessage, TaskIdUtils
from nbdb.batch_consumer.buffered_message_handler import BufferedMessageHandler
from nbdb.batch_consumer.sparse_consumer_telemetry_helper import SparseConsumerTelemetryHelper


logger = logging.getLogger()


class SparseBatchConsumer(ConsumerBase):
    """
    SparseBatchConsumer processes batch ingest requests from a kafka
    topic and loads the data in Druid.

    Messages sent to this topic reference data which is already in
    sparse form.
    """

    # Polling for running tasks to complete can take a while, so set
    # a higher limit
    MAX_POLL_INTERVAL_MS = 3600000

    def __init__(self,
                 context: Context,
                 batch_consumer_settings: SparseBatchConsumerSettings,
                 druid_settings: DruidSettings,
                 telemetry_helper: SparseConsumerTelemetryHelper):
        """
        Initialize the consumer
        :param consumer_settings:
        """
        if not context.schema.batch_mode:
            raise RuntimeError("Schema must be loaded in batch mode.")

        self.context = context
        self.batch_settings = batch_consumer_settings
        self.druid_settings = druid_settings
        self.telemetry_helper = telemetry_helper
        self.druid_admin = DruidAdmin(druid_settings.connection_string)
        self.datasources = \
            [name for (name, _, _) in self.context.schema.datasources()]
        self.buffered_message_handler = self._create_buffered_message_handler()

        ConsumerBase.__init__(self,
                              batch_consumer_settings.topic,
                              batch_consumer_settings.kafka_brokers,
                              batch_consumer_settings.group,
                              lambda x: x.decode('utf-8'),
                              max_poll_interval_ms=\
                                  SparseBatchConsumer.MAX_POLL_INTERVAL_MS)
        self.items_received = 0
        logging.info("Batch consumer loaded. Datasources in "
                     "schema: %s", self.datasources)

    def on_assign(self, consumer, partitions) -> None:
        """
        Callback invoked on a successful partition re-assignment.

        If a consumer crashes or if partitions are rebalanced, we
        may have in-flight Druid tasks that are not yet committed.

        On re-assignment, wait for all in-flight Druid batch ingest
        tasks to complete, and move the consumer to the last completed
        offset.
        """
        # Fetch committed offsets for assigned partitions
        committed_partitions = consumer.committed(partitions)
        # Wait until all tasks belonging to our partitions are
        # completed
        partition_to_completed_offset = \
            self._drain_partitions(committed_partitions)
        # If any partition has a completed offset higher than committed
        # offset, advance it
        completed_partitions = committed_partitions
        for partition in completed_partitions:
            partition_id = partition.partition
            if partition_id in partition_to_completed_offset:
                committed_offset = partition.offset
                completed_offset = partition_to_completed_offset[partition_id]
                # Start consuming from completed offset + 1
                partition.offset = max(committed_offset, completed_offset) + 1
                logger.info("Moved partition %d to offset %d. "
                            "Committed offset = %d, completed offset = %d",
                            partition_id,
                            partition.offset,
                            committed_offset,
                            completed_offset)
        consumer.assign(completed_partitions)
        self.buffered_message_handler = self._create_buffered_message_handler()

    def process_item(self, message, partition, offset) -> None:
        """
        Process the metric message

        :param message: Metric message
        :param partition: Partition from which the Kafka message was fetched
        :param offset: Offset of the message within the Kafka partition
        """
        message = SparseBatchMessage.from_serialized_json(message)
        logger.info("Received batch message: %s. Partition: %d, Offset: %d",
                    message, partition, offset)
        if message.datasource not in self.datasources:
            raise ValueError(
                f"Unexpected datasource {message.datasource}.")

        self.enqueue_message_sync(partition=partition, offset=offset,
                                  msg=message)
        logger.info("Submitted enqueued message. msg: %s", message)

    def enqueue_message_sync(self, partition: int, offset: int,
                             msg: SparseBatchMessage) -> None:
        """
        Enqueue a new message in a blocking fashion.

        If the queue is full, we pause consuming from partitions and wait until
        a slot frees up.
        """
        start_time = time.time()
        self.buffered_message_handler.process()
        paused = False
        partitions: List[TopicPartition] = []

        # Try enqueuing. If no free slots are available, we do some extra work
        while not self.buffered_message_handler.enqueue_message(
                partition, offset, msg):
            # There are no slots available. We should pause all partitions to
            # avoid getting kicked out by Kafka brokers
            if not paused:
                logger.info("Pausing all partitions")
                paused = True
                partitions = self.consumer.assignment()
            self.consumer.pause(partitions)

            # Process tasks to make space for message
            self.buffered_message_handler.process()
            time.sleep(10)

        # If we got here, we have successfully enqueued. Update timers
        end_time = time.time()
        self.buffered_message_handler.telemetry_helper.update_enqueue_timer(
            secs=end_time - start_time, datasource=msg.datasource)

        # If we had paused, let's unpause to resume processing
        if paused:
            self.consumer.resume(partitions)

    def _create_buffered_message_handler(self) -> BufferedMessageHandler:
        return BufferedMessageHandler(
            telemetry_helper=self.telemetry_helper,
            sparse_settings=self.batch_settings,
            druid_settings=self.druid_settings,
            druid_admin=self.druid_admin,
            kafka_topic=self.batch_settings.topic,
            commit_fn=self.mark_commit_req
        )

    @time_calls
    def _drain_partitions(self,
                          partitions: List[TopicPartition]
                          ) -> Dict[int, int]:
        """
        Given a list of partitions, wait until all Druid tasks belonging
        to these partitions are complete.

        :param partitions: Partitions to wait for along with their last
                           committed offsets
        :return: Dict of (partition, offset), where the offset corresponds
                 to that of the last completed task for the partition.
        """
        partitions_set: set = {partition.partition for partition in partitions}
        end_time = datetime.utcnow()
        start_time = end_time - timedelta(
            days=self.batch_settings.num_days_to_fetch_on_assign)
        created_time_filter = (start_time, end_time)
        # Drain all tasks
        self._wait_for_running_tasks_to_complete(
            partitions_set,
            created_time_filter)
        # Now that all tasks are complete, get the last completed offset for
        # our partitions
        completed_offsets = SparseBatchConsumer._get_completed_offsets(
            self.druid_admin, partitions_set, created_time_filter)
        return completed_offsets

    @staticmethod
    def _get_completed_offsets(druid_admin: DruidAdmin,
                               partitions: Set[int],
                               time_filter: (datetime, datetime)
                               ) -> Dict[int, int]:
        """
        :param druid_admin:
        :param partitions: Set of partitions to get offsets for
        :param time_filter: Fetch tasks for this time interval
        :return: Dict of (partition, offset), where offset is the last
                 completed offset for the partition
        """
        completed_tasks = \
            druid_admin.get_tasks(
                    created_time_filter=time_filter,
                    state_filter=DruidTaskStateFilter.COMPLETE,
                    type_filter=DruidTaskTypeFilter.NATIVE_INGEST_PARALLEL
                    )
        result = {}
        for (task_id, _) in completed_tasks:
            parsed_task = TaskIdUtils.parse_task_id(task_id)
            if parsed_task:
                # Filter out partitions that don't belong to us
                if parsed_task.partition in partitions:
                    task_max_offset = max(parsed_task.offsets)
                    result[parsed_task.partition] = max(
                        result.get(parsed_task.partition, task_max_offset),
                        task_max_offset)
        return result

    def _wait_for_running_tasks_to_complete(
        self,
        partitions: Set[int],
        time_filter: (datetime, datetime)) -> None:
        """
        For specified partitions, wait till all in-flight batch ingest
        tasks are complete in Druid.

        :param time_filter: Fetch tasks for this time interval
        """
        while True:
            running_tasks = []
            incomplete_states = [state for state in DruidTaskStateFilter
                                 if state != DruidTaskStateFilter.COMPLETE]
            # It's cheaper to do multiple calls to get incomplete tasks, since
            # the list of complete tasks can be huge, and we call this function
            # pretty frequently
            for state in incomplete_states:
                tasks = self.druid_admin.get_tasks(
                    created_time_filter=time_filter,
                    state_filter=state,
                    type_filter=DruidTaskTypeFilter.NATIVE_INGEST_PARALLEL)
                running_tasks.extend([task for (task, _) in tasks])

            partitions_with_running_tasks: Set[int] = set()
            for task_id in running_tasks:
                parsed_task = TaskIdUtils.parse_task_id(task_id)
                if parsed_task:
                    partitions_with_running_tasks.add(parsed_task.partition)

            pending_tasks = partitions_with_running_tasks & partitions

            self.telemetry_helper.update_pending_tasks(len(pending_tasks))

            # Intersect our partitions with partitions with running tasks
            if pending_tasks:
                # We have running tasks in our partitions still
                logger.info("Found running tasks for partitions %s. "
                            "Sleeping for 60 seconds ...", partitions)
                time.sleep(60)
            else:
                logger.info("All running tasks for partitions %s finished",
                            partitions)
                break
        # Now that tasks are complete, set gauge to 0
        self.telemetry_helper.update_pending_tasks(num_pending_tasks=0)

