"""
MetricConsumer
"""

import logging
import time
from typing import List

from confluent_kafka import TopicPartition, Consumer

from nbdb.common.consumer_base import ConsumerBase
from nbdb.common.context import Context
from nbdb.common.data_point import DataPoint
from nbdb.common.data_point import MISSING_POINT_VALUE
from nbdb.common.data_point import CLUSTER_TAG_KEY, NODE_TAG_KEY
from nbdb.common.data_point import TOKEN_TAG_PREFIX
from nbdb.common.data_point import MODE_REALTIME
from nbdb.common.telemetry import Telemetry
from nbdb.config.settings import Settings
from nbdb.common.metric_parsers import MetricParsers
from nbdb.store.sparse_series_writer import SparseSeriesWriter

from nbdb.store.sparse_series_stats import SparseSeriesStats
from nbdb.store.sparse_series_writer_base import SeriesWriterBase

logger = logging.getLogger()

TELEMETRY_POINTS_BATCH_SIZE = 1000


class MetricConsumer(ConsumerBase):
    """
    MetricConsumer connects to the kafka topic.
    """

    def __init__(self,
                 context: Context,
                 consumer_settings: Settings,
                 sparse_series_writer: SparseSeriesWriter,
                 consumer_mode: str):
        """
        Initialize the consumer
        :param consumer_settings:
        :param sparse_series_writer:
        """
        self.settings = consumer_settings
        protocol = consumer_settings.metric_protocol
        self.metric_parsers = MetricParsers(context, protocol, \
                            consumer_settings.past_message_lag, \
                            consumer_settings.future_message_lag)
        self.consumer_mode = consumer_mode
        # We need unique group names for each topic consumer and each consumer
        # mode. Unique group names ensure that one consumer's rebalancing
        # doesn't end up calling on_assign() for some other consumer group.
        group_name = "%s_%s_%s" % (consumer_settings.group_prefix,
                                   consumer_settings.topic, consumer_mode)
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
        self.sparse_series_writer = sparse_series_writer
        self.last_replay_mode = {}
        self.items_received = 0
        self.non_replay_items_received = 0
        self.last_flush_epoch = 0
        # Mapping between the partition name to the last committed offset by
        # the previously assigned consumer
        # Note that consumer in realtime mode does not need this mapping
        self.last_consumer_committed_offsets = {}
        self.start_timestamp = None
        self.recovery_consumer = None

        self.termination_detection_interval = \
            Settings.inst.sparse_store.heartbeat_scan.termination_detection_interval
        self.data_gap_detection_interval = \
            Settings.inst.sparse_store.heartbeat_scan.data_gap_detection_interval

    def set_recovery_consumer(self, recovery_consumer):
        """
        Set the recovery consumer
        """
        self.recovery_consumer = recovery_consumer

    def on_assign(self, consumer: Consumer, partitions: List[TopicPartition]):
        """
        Save partition commit offsets after a successful partition
        re-assignment. This is later used to determine whether a message is
        simply being replayed or is a brand new message
        """
        logger.info("on_assign(): Start. consumer_mode=%s", self.consumer_mode)

        start_time = time.time()
        # Clear any previously stored offsets
        self.last_consumer_committed_offsets.clear()

        # Fetch committed offsets for assigned partitions
        committed_partitions = consumer.committed(partitions)

        if self.consumer_mode == MODE_REALTIME:
            # This consumer only handles realtime data. For realtime data, we
            # spawn a child thread of recovery consumer in a light-weighted
            # replay mode to generate missing markers.
            # With that we avoid write lags for realtime data
            #
            # We still clear stats cache and reinitialize
            self.reinitialize()

            # Commenting this to disable recovery consumers as this is 
            # causing issues with realtime consumers
            #self.recovery_consumer.set_committed_partitions(
            #                                        committed_partitions)

            #self.recovery_consumer.start_consumers(
            #                    ThreadPools.inst.recovery_consumer_pool,
            #                    Settings.inst.metric_consumer.num_consumers,
            #                    False)

            return

        # Store committed offset for all assigned partitions, and move the
        # current pointer to 4h ago so that we can replay messages & build
        # state
        for p in committed_partitions:
            self.last_consumer_committed_offsets[p.partition] = p.offset

        # Search for offsets that were active 4.5h ago. We are making the
        # assumption that broker wall clock is not too different from
        # metric consumer wall clock.
        duration_secs = 4.5 * 60 * 60
        MetricConsumer._rewind_partitions(consumer, committed_partitions,
                                          duration_secs)
        self.reinitialize()

        time_taken = time.time() - start_time
        Telemetry.inst.registry.\
            meter('MetricConsumer.on_assign.time_taken').mark(time_taken)
        logger.info("on_assign(): Finished")

    def reinitialize(self):
        """
        Reinitialize after rebalancing
        """
        self.sparse_series_writer.reinitialize()
        self.last_replay_mode.clear()
        self.non_replay_items_received = 0
        self.last_flush_epoch = 0

    @staticmethod
    def copy_stats(dest, src):
        """
        Copy of the values of another stats
        """
        dest.set_refresh_epoch(src.get_refresh_epoch())
        dest.set_server_rx_time(src.get_server_rx_time())
        dest.set_crosscluster_shard(src.get_crosscluster_shard())
        if src.is_sparseness_disabled():
            dest.set_sparseness_disabled()    

    def merge_stats_dict(self, realtime_stats_cache, recovery_stats_cache):
        """
        merge the stats in recovery_stats_cache to realtime_stats_cache
        param: realtime_stats_cache
        param: recovery_stats_cache
        """
        marker_list = []
        for stats_key, recovery_stats in recovery_stats_cache.items():
            # When the stats_key appears in recovery but not in realtime,
            # use the refresh epoch of recovery for stats_key
            if stats_key not in realtime_stats_cache:
                realtime_stats = SparseSeriesStats()
                MetricConsumer.copy_stats(realtime_stats, recovery_stats)
                realtime_stats_cache[stats_key] = realtime_stats
                continue

            # Try to generate tombstone
            realtime_stats = realtime_stats_cache[stats_key]
            marker = SeriesWriterBase.check_offline_tombstone(
                        self.termination_detection_interval, \
                        self.data_gap_detection_interval, \
                        stats_key, recovery_stats, \
                        realtime_stats.get_first_epoch())
            if marker is not None:
                self.sparse_series_writer.create_marker(marker, realtime_stats,
                                                        False)
                marker_list.append(marker)
                continue

            epoch_gap = realtime_stats.get_first_epoch() \
                        - recovery_stats.get_refresh_epoch()
            # When the epoch gap between realtime_stats and recovery_stats
            # is above the threshold, create a MISSING_POINT_VALUE
            if epoch_gap > self.data_gap_detection_interval:
                marker = DataPoint.from_series_id(
                            stats_key, \
                            recovery_stats.get_refresh_epoch() + \
                                self.data_gap_detection_interval, \
                            MISSING_POINT_VALUE, \
                            is_special_value=True)
                self.sparse_series_writer.create_marker(marker, recovery_stats, False)
                marker_list.append(marker)
        return marker_list

    def process_item(self, message, partition, offset) -> None:
        """
        Process the metric message
        :param message: Metric message
        :param partition: Partition from which the Kafka message was fetched
        :param offset: Offset of the message within the Kafka partition
        """
        # TODO: Add func to register hooks for msgs

        # When there is terminated recovery consumer, merge the stats
        if self.recovery_consumer is not None and \
            self.recovery_consumer.terminate:
            self.merge_stats_dict(self.sparse_series_writer.stats_cache, \
                self.recovery_consumer.recovery_series_writer.stats_cache)
            self.recovery_consumer = None

        # Check if we are in replay mode, i.e. this is an old message being
        # reprocessed for rebuilding state, or is this a new message.
        if self.consumer_mode == MODE_REALTIME:
            # We forego replay mode processing for realtime metrics to avoid
            # write lags
            replay_mode = False
        else:
            replay_mode = (offset <=
                           self.last_consumer_committed_offsets[partition])

        self.last_replay_mode[partition] = replay_mode

        # for debug purpose we might want to view the specific log message
        if Settings.inst.logging.log_metrics:
            logger.info(message)

        # telemetry calls per item are expensive, lets count up and report
        # every 1000 points to amortize the cost
        self.items_received += 1
        if self.items_received % TELEMETRY_POINTS_BATCH_SIZE == 0:
            Telemetry.inst.registry.meter(
                'MetricConsumer.process_item_calls',
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
            # Run sparseness checks and decide whether to write data
            self.sparse_series_writer.append_metrics(data_points,
                                                     replay_mode)

        # If replay mode is ON, we do not commit our offsets till we are all
        # "caught up" and have rebuilt our state.
        if replay_mode:
            return

        # Otherwise every 60 secs, we flush our async sparse writes.
        # Once completed, we then commit our offsets
        self.non_replay_items_received += 1
        current_epoch = time.time()
        time_since_last_flush = current_epoch - self.last_flush_epoch
        if time_since_last_flush > self.settings.flush_writes_period_secs:
            self.sparse_series_writer.flush_writes()
            self.mark_commit_req()
            self.last_flush_epoch = current_epoch

