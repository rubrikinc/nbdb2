"""Sparse consumer utils."""
from dataclasses import dataclass
import json
from typing import List, Optional
import random
import string
import logging
from enum import Enum

from nbdb.common.druid_admin import DruidTaskStatus

logger = logging.getLogger()


SUPPORTED_LOCATION_TYPES = ["local", "s3"]
LOCATION_TYPE_KEY = "location_type"
PATH_KEY = "path"
DATASOURCE_NAME_KEY = "datasource"
START_TS_EPOCH_KEY = "start_ts"
END_TS_EPOCH_KEY = "end_ts"


@dataclass
class DenseBatchMessage:
    """Message to trigger batch ingest"""
    cluster_id: str
    batch_json_s3_key: str
    size_bytes: int

    @staticmethod
    def from_serialized_json(serialized: str) -> 'DenseBatchMessage':
        """Create DenseBatchMessage from json string."""
        raw_dict = json.loads(serialized)
        return DenseBatchMessage(
            cluster_id=raw_dict["cluster_id"],
            batch_json_s3_key=raw_dict["batch_json_s3_key"],
            # size_bytes was added later, so use a default if it
            # doesn't exist. This can be removed once all old messages
            # without size_bytes have been processed
            size_bytes=int(raw_dict.get("size_bytes", 1024 ** 2))
        )


@dataclass
class SparseBatchMessage:
    """Message to trigger batch ingest"""
    location_type: str
    path: str
    datasource: str
    start_ts_epoch: int
    end_ts_epoch: int

    def to_serialized_json(self) -> str:
        """
        Serialize message using a json serializer.
        """
        return json.dumps({
            LOCATION_TYPE_KEY: self.location_type,
            PATH_KEY: self.path,
            DATASOURCE_NAME_KEY: self.datasource,
            START_TS_EPOCH_KEY: self.start_ts_epoch,
            END_TS_EPOCH_KEY: self.end_ts_epoch
        })

    @staticmethod
    def from_serialized_json(serialized: str) -> 'BatchMessage':
        """
        Parse BatchMessage from a serialized json string.
        """
        raw_dict = json.loads(serialized)
        location_type = raw_dict[LOCATION_TYPE_KEY]
        if location_type not in SUPPORTED_LOCATION_TYPES:
            raise ValueError(f"Location type {location_type} not supported.")
        return SparseBatchMessage(
            location_type=location_type,
            path=raw_dict[PATH_KEY],
            datasource=raw_dict[DATASOURCE_NAME_KEY],
            start_ts_epoch=int(raw_dict[START_TS_EPOCH_KEY]),
            end_ts_epoch=int(raw_dict[END_TS_EPOCH_KEY])
        )


@dataclass
class ParsedTaskId:
    """Parsed dataclass created from druid task IDs"""
    task_id: str
    partition: int
    offsets: List[int]


class TaskIdUtils:
    """
    Utilities to çreate and parse batch ingest task IDs for Druid.

    We encode information about kafka partitions/offsets in the ID
    so that we can recover state by looking at Druid task IDs in
    case of consumer restarts.
    """

    TASK_ID_PREFIX = "nbdb_batch_ingest_task"
    TASK_ID_SEPARATOR = "___"
    OFFSET_SEPARATOR = "_"
    TASK_ID_TEMPLATE = ("{prefix}{sep}{partition}{sep}{offsets}{sep}{datasource}"
                        "{sep}{attempt_num}{sep}{random_suffix}")
    TASK_ID_PARTITION_POS = 1
    TASK_ID_OFFSET_POS = 2

    @staticmethod
    def create_task_id(partition: int,
                       offsets: List[int],
                       datasource: str,
                       attempt_num: int) -> str:
        """
        Encode partition, offset and attempt number in the task id of
        the batch ingest task so that we can use this information on
        consumer restarts.
        """
        # We add a random suffix to avoid collision with very old
        # tasks
        suffix = ''.join(random.choices(
            string.ascii_letters + string.digits, k=16))
        offsets = TaskIdUtils.OFFSET_SEPARATOR.join(
            [str(x) for x in offsets]
        )
        task_id = TaskIdUtils.TASK_ID_TEMPLATE.format(
            prefix=TaskIdUtils.TASK_ID_PREFIX,
            sep=TaskIdUtils.TASK_ID_SEPARATOR,
            partition=partition,
            offsets=offsets,
            datasource=datasource,
            attempt_num=attempt_num,
            random_suffix=suffix)

        # IDs should be parseable
        assert TaskIdUtils.parse_task_id(task_id) is not None
        return task_id

    @staticmethod
    def parse_task_id(task_id: str) -> Optional[ParsedTaskId]:
        """
        Parse and destructure task_id. Returns None if task is not a
        AnomalyDB batch ingest task.

        :return: Tuple of (partition, offset)
        """
        try:
            if not task_id.startswith(TaskIdUtils.TASK_ID_PREFIX):
                raise ValueError(f"Unknown batch task {task_id}.")
            parts = task_id.split(TaskIdUtils.TASK_ID_SEPARATOR)
            offsets_str = parts[TaskIdUtils.TASK_ID_OFFSET_POS]
            offsets = [int(x) for x in
                    offsets_str.split(TaskIdUtils.OFFSET_SEPARATOR)]

            return ParsedTaskId(
                partition=int(parts[TaskIdUtils.TASK_ID_PARTITION_POS]),
                offsets=offsets,
                task_id=task_id)
        except Exception as e: # pylint: disable=broad-except
            logger.warning("Couldn't parse task ID %s", task_id, exc_info=e)
            return None


@dataclass
class InflightTaskInfo:
    """Information about an inflight batch ingest druid task"""
    task_id: str
    datasource: str
    partition: int
    offsets: List[int]
    status: DruidTaskStatus
    start_ts_epoch: int
    last_checked_ts_epoch: int

    def duration(self) -> Optional[float]:
        """returns duration of completed task."""
        if self.last_checked_ts_epoch: # pylint: disable=no-else-return
            return max(0, self.last_checked_ts_epoch - self.start_ts_epoch)
        else:
            return None


class InflightMessageStatus(Enum):
    """Enum for inflight message status"""
    Waiting = "Waiting"
    Running = "Running"
    Succeeded = "Succeeded"
    Failed = "Failed"

    _ignore_ = ['_DRUID_STATUS_TRANSLATION_MAP']

    def is_terminal(self) -> bool:
        """
        :return: True if the inflight message has been processed.
        """
        return self in (InflightMessageStatus.Succeeded,
                        InflightMessageStatus.Failed)

    @staticmethod
    def from_druid_status(status: DruidTaskStatus) -> 'InflightMessageStatus':
        """Convert DruidStatus to InflightMessageStatus."""
        return InflightMessageStatus.DRUID_STATUS_TRANSLATION_MAP[status]

# Static members for `Enum`s have to be defined outside the Enum definition
InflightMessageStatus.DRUID_STATUS_TRANSLATION_MAP = {
    DruidTaskStatus.RUNNING: InflightMessageStatus.Running,
    DruidTaskStatus.FAILED: InflightMessageStatus.Failed,
    DruidTaskStatus.SUCCESS: InflightMessageStatus.Succeeded,
    # Assume task failed if we cannot find id
    DruidTaskStatus.NOT_FOUND: InflightMessageStatus.Failed
}


@dataclass
class InflightMessage:
    """Information about an in-memory batch message"""
    kafka_message: SparseBatchMessage
    partition: int
    offset: int
    status: InflightMessageStatus
    # Id of the inflight task associated with this message
    inflight_task_id: Optional[str]

    def transition_to_running(self, task_id: str):
        """Transition message status from waiting to running"""
        assert self.status is InflightMessageStatus.Waiting
        self.status = InflightMessageStatus.Running
        self.inflight_task_id = task_id
        logger.info("Moved message %s to Running", self)


    @staticmethod
    def create_from_kafka_message(msg: SparseBatchMessage,
                                  partition: int,
                                  offset: int):
        """Create an InflightMessage from a sparse batch message."""
        return InflightMessage(
            kafka_message=msg,
            partition=partition,
            offset=offset,
            status=InflightMessageStatus.Waiting,
            inflight_task_id=None
        )
