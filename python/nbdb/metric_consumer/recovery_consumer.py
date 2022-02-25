"""
RecoveryConsumer
"""

import logging
import time
from typing import List

from confluent_kafka import TopicPartition, Consumer

from redis import Redis

from nbdb.common.context import Context
from nbdb.common.data_point import CLUSTER_TAG_KEY, NODE_TAG_KEY
from nbdb.common.data_point import TOKEN_TAG_PREFIX
from nbdb.common.data_point import MODE_RECOVERY
from nbdb.common.telemetry import Telemetry
from nbdb.config.settings import Settings
from nbdb.common.metric_parsers import MetricParsers
from nbdb.common.consumer_base import ConsumerBase
from nbdb.store.recovery_series_writer import RecoverySeriesWriter


logger = logging.getLogger()

TELEMETRY_POINTS_BATCH_SIZE = 1000

# pylint: disable-msg=R0902
class RecoveryConsumer(ConsumerBase):
    """
    AnomalyDB inserts special markers TOMBSTONE_VALUE and MISSING_POINT_VALUE
    to handle series termination and missing data within series. To correctly
    generate such markers, consumers need to keep track of certain state for
    each series over the last hour.

    This state is stored in-memory. When a realtime consumer crashes, we lose
    such state. Since the realtime consumer starts consuming new metrics after
    a crash, it is possible that we end up skipping markers for series because
    of missing state.

    For rollup consumers, we solve the problem by forcing it to replay old
    metrics to rebuild state before it can consume new metrics. But for
    realtime consumers, we can't run replay mode because we have write lag
    guarantees to support for new metrics.

    So instead we use RecoveryConsumer which replays old metrics in a
    background thread. It is responsible for replaying to rebuild state and
    then insert missing TOMBSTONE & MISSING_VALUE markers.
    """

    def __init__(self,
                 context: Context,
                 consumer_settings: Settings,
                 recovery_series_writer: RecoverySeriesWriter):
        """
        Initialize the consumer
        :param context:
        :param consumer_settings:
        :param recovery_series_writer:
        """
        self.settings = consumer_settings
        protocol = consumer_settings.metric_protocol
        self.metric_parsers = MetricParsers(context, protocol, \
                            consumer_settings.past_message_lag, \
                            consumer_settings.future_message_lag)
        self.consumer_mode = MODE_RECOVERY
        # We need unique group names for each topic consumer and each consumer
        # mode. Unique group names ensure that one consumer's rebalancing
        # doesn't end up calling on_assign() for some other consumer group.
        # MODE_REALTIME is used in order to obtain the kafka offset of the
        # partitions
        group_name = "%s_%s_%s" % (consumer_settings.group_prefix,
                                   consumer_settings.topic, self.consumer_mode)
        self.group_name = group_name

        # We use a higher value of 15m for max_poll_interval than the default
        # 5m. This is because the consumer performs certain operations like
        # heartbeat_scan(). Sometimes heartbeat_scan() can take 5+ mins, and
        # using max_poll_interval=5m causes our consumer to be kicked out.
        # Having consumers kicked out results in rebalancing operations and
        # ultimately OOM for the remaining consumers.
        ConsumerBase.__init__(self,
                              consumer_settings.topic,
                              consumer_settings.kafka_brokers,
                              group_name,
                              lambda x: x.decode('utf-8'),
                              max_poll_interval_ms=900000)
        self.recovery_series_writer = recovery_series_writer

        # The recovery consumer replays old metrics (1.5 hours before current
        # offset) to rebuild state and thus insert missing / tombstone markers
        # correctly. current_offset refers to the last committed offset for
        # the prev realtime consumer before it crashed. For any metric before
        # this current offset, replay_mode will be True and we will not write
        # any metrics to Druid
        self.partition_begin_offsets = {}

        self.partition_end_offsets = {}

        self.partition_recovery_in_progress = {}
        self.active_partition_count = 0

        self.items_received = 0
        self.non_replay_items_received = 0
        self.last_flush_epoch = 0

        self.redis_client = Redis(host=consumer_settings.redis_host,
                                        port=consumer_settings.redis_port)

        self.committed_partitions = []

    @staticmethod
    def generate_recovery_offset_key(topic, partition):
        """
        Generate the redis key with topic and partition
        """
        return '%s_%s' % (topic, partition)

    def retrieve_recovery_offset(self, topic, partition, realtime_offset):
        """
        We store the starting offset from which a RecoveryConsumer is supposed
        to start in Redis. By storing in Redis, we can detect cases where the
        realtime consumer crashed before the recovery thread could complete.

        RecoveryConsumer will store these offsets to Redis before beginning its
        recovery logic. And it will delete once it is done.

        If the offsets exist in Redis, it means that the previous recovery
        thread did not complete. So it picks up from the starting offsets of
        the previous thread.

        If the offsets don't exist, it means that the previous recovery thread
        was completed or this is the first time a consumer has started.

        In either case, we will store the realtime_offset.

        :param topic: topic of the events
        :param partition: partition ID
        :param realtime_offset: the current offset
        """
        redis_key = RecoveryConsumer.generate_recovery_offset_key(topic, partition)
        redis_offset = self.redis_client.get(redis_key)
        if redis_offset is None:
            redis_offset = realtime_offset
            # Persist non-trivial offset only
            if realtime_offset > 0:
                self.redis_client.set(redis_key, redis_offset)
        return int(redis_offset)

    def set_partition_begin_offset(self, partition, offset):
        """
        Set the begin offset of the partition
        """
        self.partition_begin_offsets[partition] = offset

    def remove_recovery_offsets(self):
        """
        When the recovery task is completed, the recovery worker removes the
        offset of the partition which tracks the recovery task
        """
        for partition in self.partition_begin_offsets:
            redis_key = '%s_%s' % (self.group_name, partition)
            self.redis_client.delete(redis_key)

    def set_committed_partitions(self, committed_partitions):
        """
        Store the committed offsets of the realtime consumer.
        This method is called from the realtime consumer.
        """
        self.committed_partitions = committed_partitions

    def on_assign(self, consumer: Consumer, partitions: List[TopicPartition]):
        """
        Obtain the committed partitions from Kafka, and use it as the default
        value to obtain the persisted partitions from redis. Use the offsets
        of persisted partitions to rewind the consumer to prepare replay.
        """
        logger.info("on_assign(): Start. consumer_mode=%s", self.consumer_mode)

        start_time = time.time()

        persisted_partitions = []
        for p in self.committed_partitions:
            if p is None:
                continue
            topic = p.topic
            partition = p.partition
            current_realtime_offset = max(p.offset, 0)
            # Obtain the begin offset of the partition from redis, default value
            # is current_realtime_offset
            begin_offset = self.retrieve_recovery_offset(topic, partition, \
                                                    current_realtime_offset)
            persisted_partitions.append(TopicPartition(topic, partition, \
                                                    begin_offset))
            self.partition_begin_offsets[partition] = begin_offset
            self.partition_end_offsets[partition] = current_realtime_offset
            self.partition_recovery_in_progress[partition] = True
            self.active_partition_count += 1

        duration_secs = 1.5 * 60 * 60
        RecoveryConsumer._rewind_partitions(consumer, persisted_partitions,
                                          duration_secs)
        self.reinitialize()

        time_taken = time.time() - start_time
        Telemetry.inst.registry.\
            meter('RecoveryConsumer.on_assign.time_taken').mark(time_taken)
        logger.info("on_assign(): Finished")

    def reinitialize(self):
        """
        Reinitialize after rebalancing
        """
        self.recovery_series_writer.reinitialize()
        self.non_replay_items_received = 0
        self.last_flush_epoch = 0

    def is_partition_processable(self, partition, offset):
        """
        Checks the offsets for the given partition and checks whether they
        fall within the recovery window. 
        If they are greater than the end offsets for all partitions, we can
        shutdown the recovery consumer.
        """
        if self.partition_recovery_in_progress[partition]:
            # The replay for this partition ends at
            # partition_end_offsets[partition]
            if offset >= self.partition_end_offsets[partition] - 1:
                # When the partition replay just finish, mark the partition
                # as inactive and update the active_partition_count
                self.partition_recovery_in_progress[partition] = False
                self.active_partition_count -= 1
                # When there is no active partition, terminate the recovery
                # consumer
                if self.active_partition_count == 0:
                    self.terminate = True
                    self.remove_recovery_offsets()
            else:
                return True
        return False

    def process_item(self, message, partition, offset) -> None:
        """
        Process the metric message
        :param message: Metric message
        :param partition: Partition from which the Kafka message was fetched
        :param offset: Offset of the message within the Kafka partition
        """
        if not self.is_partition_processable(partition, offset):
            return

        replay_mode = offset < self.partition_begin_offsets[partition]

        # for debug purpose we might want to view the specific log message
        if Settings.inst.logging.log_metrics:
            logger.info(message)

        # telemetry calls per item are expensive, lets count up and report
        # every 1000 points to amortize the cost
        self.items_received += 1
        if self.items_received % TELEMETRY_POINTS_BATCH_SIZE == 0:
            Telemetry.inst.registry.meter(
                'RecoveryConsumer.process_item_calls',
                tag_key_values=[f'Topic={self.topic}',
                                f'ConsumerMode={self.consumer_mode}']
            ).mark(TELEMETRY_POINTS_BATCH_SIZE)
            self.items_received = 0

        data_points = self.metric_parsers.parse(message)

        # If cluster_uuid whitelist has been configured, whittle down the list
        # of parsed metrics to only the relevant ones
        if self.settings.whitelist_cluster_uuid:
            exp_cluster_uuid = self.settings.whitelist_cluster_uuid
            if self.settings.metric_protocol != 'graphite':
                # Cluster UUID should be present as a tag
                data_points = list(filter(
                    lambda v: v.tags.get(CLUSTER_TAG_KEY) == exp_cluster_uuid,
                    data_points))

                exp_node_id = self.settings.whitelist_node_id
                if exp_node_id:
                    # Node name should be present as a tag
                    data_points = list(filter(
                        lambda v: v.tags.get(NODE_TAG_KEY) == exp_node_id,
                        data_points))
            else:
                # If we are storing metrics in Graphite flat format, there will
                # be no cluster UUID token. We assume the cluster UUID is the
                # tkn1
                # Cluster UUID should be present as a tag
                data_points = list(filter(
                    lambda v: v.tags.get(TOKEN_TAG_PREFIX + '1') == \
                              exp_cluster_uuid,
                    data_points))

                exp_node_id = self.settings.whitelist_node_id
                if exp_node_id:
                    # Node name should be present as a tag
                    data_points = list(filter(
                        lambda v: v.tags.get(TOKEN_TAG_PREFIX + '2') == \
                                  exp_node_id,
                        data_points))

        if data_points:
            self.recovery_series_writer.append_metrics(data_points, \
                                                        replay_mode)

        # If replay mode is ON, we do not commit our offsets till we are all
        # "caught up" and have rebuilt our state.
        if replay_mode:
            return

        self.non_replay_items_received += 1
        current_epoch = time.time()
        time_since_last_flush = current_epoch - self.last_flush_epoch
        if time_since_last_flush > self.settings.flush_writes_period_secs:
            self.recovery_series_writer.flush_writes()
            self.last_flush_epoch = current_epoch

