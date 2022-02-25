"""Tests for BufferedMessageHandler"""
# pylint: disable=redefined-outer-name
# pylint: disable=protected-access
# pylint: disable=pointless-string-statement
# pylint: disable=invalid-name

from dataclasses import dataclass
from typing import List, Optional, Dict, Any
import time
import random
import hashlib
import os
import pytest

from confluent_kafka import TopicPartition

from nbdb.batch_consumer.buffered_message_handler import BufferedMessageHandler, \
    InflightMessageStatus, InflightMessage, \
    InflightTaskInfo
from nbdb.batch_consumer.utils import SparseBatchMessage, TaskIdUtils
from nbdb.config.settings import DruidSettings, Settings
from nbdb.common.druid_admin import DruidTaskStatus
from nbdb.common.telemetry import Telemetry
from nbdb.batch_consumer.sparse_consumer_telemetry_helper import SparseConsumerTelemetryHelper


@dataclass
class DruidBatchIngestTaskInfo:
    """Information about an ongoing batch ingest task in fake druid."""
    task_id: str
    start_time: int
    task_dur_secs: int
    terminal_status: DruidTaskStatus = DruidTaskStatus.SUCCESS


class FakeDruid:
    """A fake implementation of Druid for BufferedMessageHandler."""

    # Immediately mark tasks as complete
    DEFAULT_TASK_DURATION_SECS = 0

    def __init__(self):
        self.batch_ingest_tasks: Dict[str, DruidBatchIngestTaskInfo] = {}
        self.task_dur = FakeDruid.DEFAULT_TASK_DURATION_SECS

    def create_batch_ingest_task(self, # pylint: disable=too-many-arguments
                                task_id: str,
                                druid_settings: DruidSettings,
                                query_granularity: int,
                                segment_granularity: int,
                                datasource: str,
                                location_type: str,
                                paths: List[str]) -> None:
        """Create a batch ingest task in Druid."""
        _ = druid_settings
        _ = segment_granularity
        _ = query_granularity
        _ = location_type
        _ = datasource
        _ = paths
        self.batch_ingest_tasks[task_id] = DruidBatchIngestTaskInfo(
            task_id=task_id,
            start_time=time.time(),
            task_dur_secs=self.task_dur
        )

    def get_task_status(self, task_id: str) -> Optional[DruidTaskStatus]:
        """Get status of a task in Druid."""
        if task_id not in self.batch_ingest_tasks:
            return None
        curr_time = time.time()
        task: DruidBatchIngestTaskInfo = self.batch_ingest_tasks[task_id]
        # pylint: disable=no-else-return
        if curr_time < task.start_time + task.task_dur_secs:
            return DruidTaskStatus.RUNNING
        else:
            return task.terminal_status

    def set_task_dur(self, secs: int) -> None:
        """Override task duration for new tasks"""
        self.task_dur = secs

    def mark_task_done(self,
                       task_id,
                       status: DruidTaskStatus = \
                           DruidTaskStatus.SUCCESS) -> None:
        """Mark a task as completed in Druid."""
        assert status.is_terminal()
        task = self.batch_ingest_tasks[task_id]
        task.task_dur_secs = (time.time() - task.start_time)
        task.terminal_status = status

class FakeKafka:
    """
    Kafka fake which support a single active consumer.
    """

    def __init__(self):
        self.messages: Dict[int, List[Any]] = {}
        self.committed: Dict[int, int] = {}
        # partition id to last consumed
        self.offsets: Dict[int, int] = {}
        self.consumed: Dict[int, int] = {}
        self.partitions_set = False
        self.topic = "fake_topic"

    def produce(self, msg: Any, key: Optional[str] = None) -> (int, int):
        """
        Produce a message.

        :return: Tuple of (partition, offset)
        """
        self._validate()
        if not key:
            partition = random.randint(1, self._num_partitions())
        else:
            partition = int(
                hashlib.md5(key.encode("utf-8")).hexdigest(), 16) % \
                    self._num_partitions()
        offset = self.offsets[partition] + 1
        self.offsets[partition] = offset
        self.messages[partition].append(msg)
        assert self.messages[partition][offset] == msg
        return (partition, offset)

    def consume(self) -> (int, int, Any):
        """
        Consume a message.

        :return: Tuple of (partition, offset, message)
        """
        self._validate()
        partitions = list(self.messages.keys())
        while True:
            for partition in partitions:
                offset = self.consumed[partition] + 1
                messages = self.messages[partition]
                if offset < len(messages):
                    self.consumed[partition] = offset
                    return (partition, offset, messages[offset])
            time.sleep(1)

    def commit(self, offsets: List[TopicPartition]) -> None:
        """Commit offsets in fake kafka."""
        self._validate()
        for tp in offsets:
            assert tp.topic == self.topic
            partition = tp.partition
            offset = tp.offset
            self.committed[partition] = offset

    def rewind_to_committed(self):
        """Rewind consumer to committed offsets."""
        self._validate()
        for partition in self.messages:
            self.consumed[partition] = self.committed[partition]

    def set_partitions(self, num: int) -> None:
        """Initialize partitions in fake kafka."""
        if self.partitions_set:
            raise RuntimeError("Cannot change partitions")
        self.partitions_set = True
        for i in range(1, num+1):
            self.messages[i] = []
            self.offsets[i] = -1
            self.consumed[i] = -1
            self.committed[i] = -1

    def _num_partitions(self) -> int:
        self._validate()
        return len(self.messages)

    def _validate(self) -> None:
        assert self.partitions_set, "Partitions must be set before use"


@pytest.fixture
def fake_druid_admin() -> FakeDruid:
    """Fixture for fake druid."""
    return FakeDruid()

@pytest.fixture
def fake_kafka() -> FakeKafka:
    """Fixture for fake kafka."""
    return FakeKafka()


@dataclass
class Deps:
    """depdendencies for BufferedMessageHandler tests"""
    kafka: FakeKafka
    druid_admin: FakeDruid
    handler: BufferedMessageHandler


@pytest.fixture
def deps(fake_druid_admin: FakeDruid, fake_kafka: FakeKafka) -> Deps:
    """Fixture for test dependencies."""
    settings = Settings.load_yaml_settings(os.path.dirname(__file__) +
                                           '/test_settings.yaml')
    telemetry = Telemetry()
    telemetry_helper = SparseConsumerTelemetryHelper(telemetry=telemetry)
    handler = BufferedMessageHandler(
        telemetry_helper=telemetry_helper,
        sparse_settings=settings.sparse_batch_consumer,
        druid_settings=None,
        kafka_topic=fake_kafka.topic,
        druid_admin=fake_druid_admin,
        commit_fn=fake_kafka.commit
    )
    return Deps(
        kafka=fake_kafka,
        druid_admin=fake_druid_admin,
        handler=handler
    )

def _produce_and_validate_msg(kafka: FakeKafka, message, key) -> (int, int):
    """
    :return: Tuple of (partition, offset)
    """
    num_partitions = len(kafka.messages.keys())
    (produced_partition, produced_offset) = kafka.produce(message, key=key)
    (partition, offset, msg) = kafka.consume()
    assert produced_partition == partition
    assert produced_offset == offset
    assert msg == message
    assert 1 <= partition <= num_partitions
    return (partition, offset)

def test_fake_kafka(deps: Deps):
    """Test FakeKafka."""
    deps.kafka.set_partitions(5)
    (partition1, offset1) = _produce_and_validate_msg(deps.kafka, "a", "key")
    assert offset1 == 0
    (partition2, offset2) = _produce_and_validate_msg(deps.kafka, "b", "key")
    assert offset2 == 1
    # Same partition since we are using the same key
    assert partition1 == partition2
    # Since we haven't committed anything, we'll go back to first message
    deps.kafka.rewind_to_committed()
    (partition, offset, msg) = deps.kafka.consume()
    assert partition == partition1
    assert offset == 0
    assert msg == "a"
    deps.kafka.commit(
        [TopicPartition(deps.kafka.topic, partition=partition, offset=offset)])
    (partition, offset, msg) = deps.kafka.consume()
    assert partition == partition1
    assert offset == 1
    assert msg == "b"
    # We'll start at offset=1 since we've committed 0
    deps.kafka.rewind_to_committed()
    (partition, offset, msg) = deps.kafka.consume()
    assert partition == partition1
    assert offset == 1
    assert msg == "b"

def test_handler_process_empty(deps: Deps):
    """Test process doesn't error out on empty data structures."""
    deps.kafka.set_partitions(5)
    deps.handler.process()

def _enqueue_and_validate_message(handler: BufferedMessageHandler,
                                  msg: Any,
                                  partition: int,
                                  offset: int) -> None:
    assert handler.enqueue_message(partition, offset, msg)
    inflight_message: InflightMessage =\
        handler.inflight_messages[partition][-1]
    assert inflight_message.kafka_message == msg
    assert inflight_message.partition == partition
    assert inflight_message.offset == offset
    assert inflight_message.inflight_task_id is None
    assert inflight_message.status == InflightMessageStatus.Waiting


def test_handler_enqueue(deps: Deps):
    """Test BufferedMessageHandler.enqueue_message."""
    deps.kafka.set_partitions(5)
    msg = _create_sparse_msg("d1")
    (partition, offset) = deps.kafka.produce(msg, key="key")
    deps.handler.max_inflight_messages = 1
    _enqueue_and_validate_message(deps.handler, msg, partition, offset)
    # Since inflight limit is reached, enqueue will fail
    assert not deps.handler.enqueue_message(partition, offset, msg)
    deps.handler.max_inflight_messages = 2
    # Since partition+offset are the same, enqueue will fail
    with pytest.raises(ValueError):
        _enqueue_and_validate_message(deps.handler, msg, partition, offset)
    msg = _create_sparse_msg("d2")
    (partition, offset) = deps.kafka.produce(msg, key="key")
    _enqueue_and_validate_message(deps.handler, msg, partition, offset)

def _schedule_and_validate_task(deps: Deps) -> None:
    msg = _create_sparse_msg("d1")
    (partition, offset) = deps.kafka.produce(msg, key="key")
    _enqueue_and_validate_message(deps.handler, msg, partition, offset)
    # no-op since no task is scheduled
    old_messages = deps.handler.inflight_messages
    old_tasks = deps.handler.inflight_tasks
    assert len(old_tasks) == 0
    deps.handler._maintain()
    assert deps.handler.inflight_tasks == old_tasks
    assert deps.handler.inflight_messages == old_messages
    # Schedule tasks
    task_ids = deps.handler._schedule_tasks()
    assert len(task_ids) == 1
    task = TaskIdUtils.parse_task_id(task_ids[0])
    assert task.offsets == [offset]
    assert task.partition == partition
    assert deps.druid_admin.get_task_status(task_ids[0]) == \
        DruidTaskStatus.SUCCESS
    # Since task is complete, inflight message and task should be removed,
    # and the message should be committed
    deps.handler._maintain()
    assert len(deps.handler.inflight_tasks) == 0
    assert len(deps.handler.inflight_messages[partition]) == 0
    assert deps.kafka.committed[partition] == offset

def test_handler_schedule_single(deps: Deps):
    """Test scheduling of tasks when messages cannot be combined."""
    deps.kafka.set_partitions(5)
    # Immediately mark tasks as complete
    deps.druid_admin.set_task_dur(0)
    assert deps.handler.total_tasks == 0
    _schedule_and_validate_task(deps)
    assert deps.handler.total_tasks == 1
    _schedule_and_validate_task(deps)
    assert deps.handler.total_tasks == 2
    _schedule_and_validate_task(deps)
    assert deps.handler.total_tasks == 3

def test_handler_schedule_combine(deps: Deps): # pylint: disable=too-many-locals
    """Test scheduling of tasks when messages can be combined."""
    deps.kafka.set_partitions(5)
    # Set task completition time to large so that we have
    # a chance to combine tasks
    deps.druid_admin.set_task_dur(2 << 32)
    deps.handler.max_inflight_tasks = 2
    deps.handler.max_inflight_messages = 10

    assert deps.handler.total_tasks == 0
    msg = _create_sparse_msg("d1")
    (p1, o1) = deps.kafka.produce(msg, key="key")
    _enqueue_and_validate_message(deps.handler, msg, p1, o1)
    msg = _create_sparse_msg("d2")
    (p2, o2) = deps.kafka.produce(msg, key="key")
    _enqueue_and_validate_message(deps.handler, msg, p2, o2)
    """
    Inflight tasks: []
    Inflight messages: []
    """
    deps.handler.process()
    """
    Inflight tasks: d1 -> [o1], d2 -> [o2]
    Inflight messages: o1 [RUNNING], o2 [RUNNING]
    """
    assert deps.handler._num_inflight_tasks() == 2
    # Add more messages for d1 and d2
    msg = _create_sparse_msg("d1")
    (p3, o3) = deps.kafka.produce(msg, key="key")
    _enqueue_and_validate_message(deps.handler, msg, p3, o3)
    msg = _create_sparse_msg("d2")
    (p4, o4) = deps.kafka.produce(msg, key="key")
    _enqueue_and_validate_message(deps.handler, msg, p4, o4)
    msg = _create_sparse_msg("d1")
    (p5, o5) = deps.kafka.produce(msg, key="key")
    _enqueue_and_validate_message(deps.handler, msg, p5, o5)
    # Both tasks are still on-going
    deps.handler.process()
    """
    Inflight tasks: d1 -> [o1], d2 -> [o2]
    Inflight messages: o1 [RUNNING], o2 [RUNNING], o3,o4,o5[WAITING]
    """

    assert deps.handler._num_inflight_tasks() == 2
    # Now, mark task d1 as complete
    task_d1 = _get_task_for_datasource(
        deps.handler.inflight_tasks.values(), "d1")
    deps.druid_admin.mark_task_done(task_d1.task_id)

    deps.handler.process()
    """
    Inflight tasks: d1 -> [o3, o5], d2 -> [o2]
    Inflight messages: o2 [RUNNING], o3 [RUNNING], o4 [WAITING], o5 [RUNNING]
    """
    # The next task of d1 should contain two messages
    task_d1_combined = _get_task_for_datasource(
        deps.handler.inflight_tasks.values(), "d1")
    assert task_d1.task_id != task_d1_combined.task_id
    parsed = TaskIdUtils.parse_task_id(task_d1_combined.task_id)
    assert parsed.offsets == [o3, o5]
    # Mark all tasks done
    for task_id in deps.druid_admin.batch_ingest_tasks:
        deps.druid_admin.mark_task_done(task_id)
    # Make all tasks immediately finish
    deps.druid_admin.set_task_dur(0)
    deps.handler.process()
    """
    Inflight tasks: d1-> [o3, o5], d2 -> [o4]
    Inflight messages: o4 [RUNNING], o5 [SUCCESS]

    Note that while the task for d1 is done, it isn't pruned
    since message o5 still points to it
    """
    combined_messages = _get_messages_for_task(
        deps.handler._get_inflight_messages(),
        task_d1_combined.task_id)
    assert deps.handler._num_inflight_tasks() == 2
    assert deps.handler._num_inflight_messages() == 2
    assert len(combined_messages) == 1
    assert combined_messages[0].status == InflightMessageStatus.Succeeded
    assert combined_messages[0].offset == o5

    deps.handler.process()
    """
    Inflight tasks: []
    Inflight messages: []
    """
    assert deps.handler._num_inflight_messages() == 0
    assert deps.handler._num_inflight_tasks() == 0

def _get_messages_for_task(msgs: List[InflightMessage],
                           task_id: str) -> List[InflightMessage]:
    result = []
    for msg in msgs:
        if msg.inflight_task_id == task_id:
            result.append(msg)
    return result

def _get_task_for_datasource(
    tasks: List[InflightTaskInfo],
    datasource: str) -> InflightTaskInfo:
    for task in tasks:
        if task.datasource == datasource:
            return task
    raise ValueError("Couldn't find task for datasource")

def _create_sparse_msg(datasource: str) -> SparseBatchMessage:
    return SparseBatchMessage(location_type="",
                              path="",
                              datasource=datasource,
                              start_ts_epoch=-1,
                              end_ts_epoch=-1)
