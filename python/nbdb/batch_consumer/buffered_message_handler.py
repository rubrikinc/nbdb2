"""BufferedMessageHandler."""
# pylint: disable=no-else-return

from typing import Deque, List, Dict, Optional, Callable, Any
from dataclasses import dataclass
from enum import Enum
import logging
from collections import deque
import time

from confluent_kafka import TopicPartition
from pyformance import time_calls

from nbdb.common.druid_admin import DruidAdmin, DruidTaskStatus
from nbdb.config.settings import DruidSettings, SparseBatchConsumerSettings
from nbdb.batch_consumer.utils import SparseBatchMessage, TaskIdUtils
from nbdb.batch_consumer.sparse_consumer_telemetry_helper import SparseConsumerTelemetryHelper
from nbdb.common.telemetry import meter_failures
from nbdb.batch_consumer.utils import InflightMessage, InflightTaskInfo, InflightMessageStatus

logger = logging.getLogger()


class BufferedMessageHandler: #pylint: disable=too-many-instance-attributes
    """
    Buffer multiple ingest messages in memory and process them,
    combining multiple messages when possible.

    Once messages are processed, commit them.
    """

    def __init__(self, # pylint: disable=too-many-arguments
                 telemetry_helper: SparseConsumerTelemetryHelper,
                 sparse_settings: SparseBatchConsumerSettings,
                 druid_settings: DruidSettings,
                 druid_admin: DruidAdmin,
                 kafka_topic: str,
                 commit_fn: Callable[[List[TopicPartition]], None]):
        self.telemetry_helper = telemetry_helper
        self.druid_settings = druid_settings
        self.druid_admin = druid_admin
        self.kafka_topic = kafka_topic
        self.commit_fn = commit_fn

        self.max_inflight_tasks = sparse_settings.max_inflight_tasks
        self.max_inflight_messages = sparse_settings.max_inflight_messages
        self.max_messages_to_combine = sparse_settings.max_messages_to_combine
        # Dict of partition ID to all buffered messages for that
        # partition.
        self.inflight_messages: Dict[int, Deque[InflightMessage]] = {}
        # Dict of task_id -> task_info for all in-flight tasks
        self.inflight_tasks: Dict[str, InflightTaskInfo] = {}
        # stats
        self.total_tasks: int = 0

    @time_calls
    def process(self):
        """
        Process inflight tasks and messages, and schedule tasks if needed.
        """
        logger.info("Running BufferedMessageHandler.process. Inflight "
                    "tasks: %d, Inflight messages: %d",
                    self._num_inflight_tasks(),
                    self._num_inflight_messages())
        self._validate_inflight_tasks_invariants()
        # Maintain inflight messages and tasks
        self._maintain()
        # Trigger new tasks if needed
        self._schedule_tasks()
        logger.info("Ran BufferedMessageHandler.process. Inflight "
                    "tasks: %d, Inflight messages: %d",
                    self._num_inflight_tasks(),
                    self._num_inflight_messages())
        # Report telemetry
        self.telemetry_helper.update_inflight_messages_gauge(
            self.max_inflight_messages,
            self._messages_by_datasource(self._get_inflight_messages())
        )
        self.telemetry_helper.update_inflight_tasks_gauge(
            self.max_inflight_tasks,
            self._tasks_by_datasource())

        self._validate_inflight_tasks_invariants()

    @time_calls
    def enqueue_message(self,
                        partition: int,
                        offset: int,
                        msg: SparseBatchMessage) -> bool:
        """Enque a new message.

        :return: True if the message was enqueued
        """
        # If max inflight messages are queued, return False
        if self._num_inflight_messages() >= self.max_inflight_messages:
            return False
        else:
            if partition not in self.inflight_messages:
                self.inflight_messages[partition] = deque()
            q: Deque = self.inflight_messages[partition]
            if len(q) > 0:
                (top_partition, top_offset) = q[-1].partition, q[-1].offset
                if partition != top_partition:
                    raise ValueError(f"Expected partition {top_partition},"
                                     f" found{partition}")
                if offset <= top_offset:
                    raise ValueError(
                        f"Received non-increasing offset {offset} (current:"
                        f" {top_offset})")
            q.append(InflightMessage.create_from_kafka_message(
                msg=msg, partition=partition, offset=offset))
            logger.info("Enqueued message. partition: %d, offset: %d, msg: %s",
                        partition, offset, msg)
            return True

    @time_calls
    @meter_failures
    def _maintain(self):
        """
        Check and update status of inflight tasks and messages.

        For messages that have been successfully processed, commit
        their offsets.
        """
        # Update status of all inflight tasks
        self._update_inflight_task_status(self.druid_admin,
                                          self.inflight_tasks.values())

        # Update status of all inflight messages
        self._update_inflight_messages(self.inflight_tasks.values(),
                                       self._get_inflight_messages())

        # Remove processed messages from inflight messages
        removed_offsets = self._prune_inflight_messages()

        # Remove tasks which no longer are references by inflight messages
        self._prune_inflight_tasks()

        # Commit partitions if needed
        if len(removed_offsets) > 0:
            self._commit_completed_offsets(removed_offsets)

    @time_calls
    @meter_failures
    def _schedule_tasks(self) -> Optional[List[str]]:
        """
        Schedule waiting tasks if there are free slots.

        :return: List of task ids which were scheduled
        """
        # If waiting inflight messages are empty, return
        waiting_messages = [msg for msg in self._get_inflight_messages()
                            if msg.status == InflightMessageStatus.Waiting]
        if len(waiting_messages) == 0:
            logger.info("Nothing to schedule ...")
            return None
        # If maximum inflight tasks are running, return
        if self._num_inflight_tasks() >= self.max_inflight_tasks:
            logger.info("Inflight tasks list full, not scheduling ...")
            return None
        # Group waiting messages by datasource, and schedule until we have
        # slots for datasources which don't have an existing task
        msgs_by_datasource = self._messages_by_datasource(
            waiting_messages)

        tasks = []
        for (datasource, messages) in msgs_by_datasource.items():
            # An existing task is already running for the datasource,
            # so do not schedule
            if datasource in self._tasks_by_datasource():
                continue
            if self._num_inflight_tasks() < self.max_inflight_tasks:
                # If we have more messages than we can combine, trim the list
                messages_to_schedule = \
                    messages[:self.max_messages_to_combine]
                if len(messages_to_schedule) > 1:
                    logger.info(
                        "Combining messages for datasource %s. Messages: %s",
                        datasource, messages_to_schedule)
                tasks.append(
                    self._create_batch_ingest_task(
                        datasource, messages_to_schedule))
                for msg in messages_to_schedule:
                    msg.transition_to_running(tasks[-1].task_id)
            else:
                # No more slots to schedule, return
                break
        for task in tasks:
            self.inflight_tasks[task.task_id] = task
        return [task.task_id for task in tasks]

    def _num_inflight_messages(self) -> int:
        """
        :return: Total number of inflight messages. This includes
                 successfully processed messages which haven't been
                 pruned yet.
        """
        return sum(
            [len(q) for q in self.inflight_messages.values()])

    def _num_inflight_tasks(self) -> int:
        """
        :return: Total number of inflight tasks. This includes
                 terminated tasks which haven't been
                 pruned yet.
        """
        return len(self.inflight_tasks)

    def _get_inflight_messages(self) -> List[InflightMessage]:
        """
        :return: List of all inflight messages.
        """
        result = []
        for q in self.inflight_messages.values():
            result.extend(q)
        return result

    @staticmethod
    def _messages_by_datasource(
        messages: List[InflightMessage]) -> Dict[str, List[InflightMessage]]:
        """
        :return: Dict of datasource -> list of inflight messages for the
                 datasource
        """
        result = {}
        for msg in messages:
            datasource = msg.kafka_message.datasource
            if datasource not in result:
                result[datasource] = []
            result[datasource].append(msg)
        return result

    @staticmethod
    def _extract_one(items: List[Any]) -> Any:
        items_set = set(items)
        if len(items_set) != 1:
            raise ValueError(f"Expected one item, found {items_set}")
        else:
            for item in items_set:
                return item

    def _tasks_by_datasource(self) -> Dict[str, InflightTaskInfo]:
        """
        :return: Dict of datasource -> list of inflight tasks for the
                 datasource
        """
        result = {}
        for task in self.inflight_tasks.values():
            datasource = task.datasource
            result[datasource] = task
        return result

    @time_calls
    @meter_failures
    def _create_batch_ingest_task(
        self,
        datasource: str,
        messages: List[InflightMessage]) -> InflightTaskInfo:
        """
        Create a batch ingest task for one or more messages.

        :return: Info for the newly created task.
        """
        assert len(messages) > 0
        paths = []
        partition = self._extract_one([msg.partition for msg in messages])
        datasource = self._extract_one(
            [msg.kafka_message.datasource for msg in messages])
        location_type = self._extract_one(
            [msg.kafka_message.location_type for msg in messages])
        paths = [msg.kafka_message.path for msg in messages]
        offsets = [msg.offset for msg in messages]

        # We encode information about partition and offsets in the task ID
        # so that we can recover in cases of consumer crashes
        task_id = TaskIdUtils.create_task_id(
            partition, offsets, datasource, attempt_num=0)

        # TODO: Add retries for druid API errors/add metrics
        self.druid_admin.create_batch_ingest_task(
            task_id=task_id,
            druid_settings=self.druid_settings,
            # TODO: Remove hardcoding
            query_granularity=60,
            segment_granularity=86400,
            location_type=location_type,
            paths=paths,
            datasource=datasource
        )

        logger.info("Successfully created batch ingest task for "
                    "datasource: %s, partition: %d, offsets: %s",
                    datasource, partition, offsets)
        return InflightTaskInfo(
            task_id=task_id, datasource=datasource, partition=partition,
            offsets=offsets, status=DruidTaskStatus.RUNNING,
            start_ts_epoch=time.time(), last_checked_ts_epoch=None
        )

    def _commit_completed_offsets(self,
                                  completed_offsets: Dict[str, str]):
        assert len(completed_offsets) > 0
        to_commit_offsets = []
        for (partition, offset) in completed_offsets.items():
            partition_offset = TopicPartition(
                topic=self.kafka_topic,
                partition=partition,
                offset=offset)
            to_commit_offsets.append(partition_offset)
            logger.info("Committing offset %d for partition %d",
                        offset, partition)
        self.commit_fn(to_commit_offsets)

    def _prune_inflight_tasks(self):
        """Remove tasks which are no longer references from any message."""
        messages = self._get_inflight_messages()
        referenced_task_ids: set = {x.inflight_task_id for x in messages \
                                   if x.inflight_task_id is not None}
        current_task_ids = set(self.inflight_tasks.keys())
        task_ids_to_remove = current_task_ids.difference(referenced_task_ids)
        for task_id in task_ids_to_remove:
            task: InflightTaskInfo = self.inflight_tasks[task_id]
            assert task.status.is_terminal()
            self.total_tasks += 1
            logger.info("Removing task %s from inflight tasks", task_id)
            # Update telemetry
            self.telemetry_helper.update_on_task_prune(task)
            del self.inflight_tasks[task_id]

    def _prune_inflight_messages(self) -> Dict[int, int]:
        """Remove processed messages from the head of queue.

        :return: Dict of partition -> largest pruned offset
        """
        result = {}
        for (partition, q) in self.inflight_messages.items():
            q: Deque[InflightMessage]
            while len(q) > 0:
                top = q[0]
                if top.status.is_terminal():
                    logger.info("Removing message %s from inflight messages.",
                                top)
                    self.telemetry_helper.update_on_message_prune(top)
                    q.popleft()
                    result[partition] = top.offset
                else:
                    break
        return result

    def _validate_inflight_tasks_invariants(self):
        datasources = [x.datasource for x in self.inflight_tasks.values()]
        assert len(datasources) == len(set(datasources)), (
            f"Found tasks with duplicate datasources: {self.inflight_tasks}")

    @classmethod
    @time_calls
    @meter_failures
    def _update_inflight_task_status(cls,
                                     druid_admin: DruidAdmin,
                                     tasks: List[InflightTaskInfo]) -> None:
        """
        Check the current status of the specified druid task and update it
        in-place.

        :param druid_admin:
        :param task_info: Task to update
        """
        for task in tasks:
            status = druid_admin.get_task_status(task.task_id)
            if task.status != status:
                logger.info("Status of task %s changed from %s to %s",
                            task.task_id, task.status, status)
            task.status = status
            task.last_checked_ts_epoch = int(time.time())

    @staticmethod
    def _update_inflight_messages(tasks: List[InflightTaskInfo],
                                  messages: List[InflightMessage]):
        task_id_map = {}
        for task in tasks:
            task_id_map[task.task_id] = task
        for message in messages:
            if message.inflight_task_id is not None:
                task = task_id_map[message.inflight_task_id]
                status = InflightMessageStatus.from_druid_status(
                    task.status)
                if message.status != status:
                    logger.info("Status of message at partition: %d, "
                                "offset: %d changed from %s to %s",
                                message.partition, message.offset,
                                message.status, status)
                message.status = status
