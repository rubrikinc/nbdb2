"""SparseConsumerTelemetryHelper"""

from typing import Dict, List

from pyformance.meters.gauge import Gauge

from nbdb.common.telemetry import Telemetry
from nbdb.batch_consumer.utils import InflightMessage, InflightTaskInfo

BMF = "SparseBatchConsumer.BufferedMessageHandler"

class SparseConsumerTelemetryHelper:
    """Helper to emit telemetry from sparse batch consumer"""

    def __init__(self, telemetry: Telemetry):
        self.telemetry = telemetry
        self.inflight_messages_gauges: Dict[str, Gauge] = {}
        self.inflight_tasks_gauges: Dict[str, Gauge] = {}

    def update_pending_tasks(self, num_pending_tasks: int):
        """Update pending tasks gauge."""
        self.telemetry.registry\
            .gauge("SparseBatchConsumer.pending_tasks_on_startup")\
            .set_value(num_pending_tasks)

    def update_enqueue_timer(self, secs: float, datasource: int):
        """Updater timers for enqueue operation."""
        # Update global timer
        # pylint: disable=protected-access
        self.telemetry.registry\
            .timer(f"{BMF}.enqueue_time")\
            ._update(secs)
        # Update per datasource timer
        self.telemetry.registry\
            .timer(
                f"{BMF}.datasource.{datasource}.enqueue_time")\
            ._update(secs)

    def update_inflight_messages_gauge(
        self,
        max_messages: int,
        messages: Dict[str, List[InflightMessage]]):
        """
        :param: messages Dict of (datasource, List[InflightMessage])
        """
        self.telemetry.registry.gauge(
            f"{BMF}.max_inflight_messages").set_value(max_messages)
        num_inflight_messages = sum([len(msgs) for msgs in messages.values()])
        self.telemetry.registry\
            .gauge(f"{BMF}.num_inflight_messages")\
            .set_value(num_inflight_messages)
        # Update per datasource gauges
        for datasource in messages:
            if datasource not in self.inflight_messages_gauges:
                self.inflight_messages_gauges[datasource] = \
                    self.telemetry.registry.gauge(
                        f"{BMF}.datasource.{datasource}.inflight_messages")
        for (datasource, gauge) in self.inflight_messages_gauges.items():
            if datasource in messages:
                gauge.set_value(len(messages[datasource]))
            else:
                gauge.set_value(0)

    def update_inflight_tasks_gauge(self,
                                    max_tasks: int,
                                    tasks: Dict[str, InflightTaskInfo]):
        """
        :param: tasks Dict of (datasource, InflightTaskInfo)
        """
        self.telemetry.registry.gauge(
            f"{BMF}.max_inflight_tasks").set_value(max_tasks)
        self.telemetry.registry\
            .gauge(f"{BMF}.num_inflight_tasks")\
            .set_value(len(tasks))
        # Update per datasource gauges
        for datasource in tasks:
            if datasource not in self.inflight_tasks_gauges:
                self.inflight_tasks_gauges[datasource] = \
                    self.telemetry.registry.gauge(
                        f"{BMF}.datasource.{datasource}.inflight_tasks")
        for (datasource, gauge) in self.inflight_messages_gauges.items():
            if datasource in tasks:
                # Update with number of messages that this task is processing
                gauge.set_value(len(tasks[datasource].offsets))
            else:
                gauge.set_value(0)

    def update_on_task_prune(self, task: InflightTaskInfo):
        """update metrics on task prune."""
        # pylint: disable=protected-access
        if task.status.is_terminal():
            duration = task.duration()
            if duration:
                # Update global timer
                self.telemetry.registry\
                    .timer(f"{BMF}.task_duration.{task.status}")\
                    ._update(duration)
                # Update per datasource timer
                self.telemetry.registry\
                    .timer(f"{BMF}.datasource.{task.datasource}."
                        f"task_duration.{task.status}")\
                    ._update(duration)

    def update_on_message_prune(self, message: InflightMessage):
        """update metrics on message prune."""
        if message.status.is_terminal():
            # Update global meter. Note that we don't update time since
            # the task timer gives us enough information
            self.telemetry.registry\
                .meter(f"{BMF}.processed_messages.{message.status}")\
                .mark()
            # Update per datasource meter
            self.telemetry.registry\
                .meter(f"{BMF}.datasource.{message.kafka_message.datasource}"
                       f".processed_messages.{message.status}")\
                .mark()
