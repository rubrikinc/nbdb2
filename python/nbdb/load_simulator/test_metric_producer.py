"""
TestMetricProducer
"""

import json
import logging
import os
import random
import time

from absl import app, flags

from retrying import retry

import kafka
from kafka.admin import KafkaAdminClient, NewTopic
from pyformance import meter_calls, time_calls

from nbdb.batch_consumer.utils import SparseBatchMessage
from nbdb.batch_consumer.dense_batch_consumer import DenseBatchMessage
from nbdb.common.context import Context
from nbdb.common.entry_point import EntryPoint
from nbdb.common.kafka_utils import KafkaUtils
from nbdb.config.settings import Settings
from nbdb.schema.schema_creator import SchemaCreator


logger = logging.getLogger()

flags.DEFINE_integer('start_cluster_id', 0, 'start cluster id to use')
flags.DEFINE_integer('setup', 0, 'Create the schema and topics')
flags.DEFINE_boolean('local', False, 'Local mode create a clean schema')


class TestStreamMetricProducer:
    """
    This is a test class for generating test metric load against a local kafka
    """
    # Disabling this warning, because we probably will add more methods
    # to this class when we do benchmarking
    # pylint: disable=too-few-public-methods

    def __init__(self,
                 context: Context,
                 producer_settings: Settings,
                 start_cluster_id: str):
        """
        Initialize the kafka metric producer
        :param producer_settings: Producer settings
        :param cluster:
        """
        self.context = context
        self.producer_settings = producer_settings
        self.producer = kafka.KafkaProducer(bootstrap_servers=
                                            producer_settings.kafka_brokers,
                                            batch_size=16384*10
                                            )
        self.start_cluster_id = start_cluster_id

    @meter_calls
    @time_calls
    def send_message(self, msg: str, partition_id: int) -> None:
        """
        Send the message to the kafka
        :param msg:
        :param partition_id
        """
        self.producer.send(
            self.producer_settings.topic,
            value=msg.encode('utf-8'),
            partition=partition_id)

    def generate_random_value(self, last_value):
        """
        Generate a new random value based on last_value using a random
        walk
        :param last_value:
        :return: new random value in proximity of last value
        """
        if last_value > 0:
            return max(self.producer_settings.min_value,
                       min(self.producer_settings.max_value,
                           int(last_value +
                               random.uniform(
                                   self.producer_settings.min_change,
                                   self.producer_settings.max_change)
                               )
                           )
                       )
        return int(random.uniform(self.producer_settings.min_value,
                                  self.producer_settings.max_value))

    def produce_non_cluster_series(self) -> None:
        """
        Generates a predictable series for catch all case
        doesn't have an explicit cluster id defined
        """
        if not hasattr(self.producer_settings, 'predictable'):
            logger.info("Not producing predictable series")
            return

        p = self.producer_settings.predictable
        logger.info('TestMetricProducer.produce_non_cluster_series')
        logger.info('TimeRange: [%d - %d]', p.start_epoch, p.end_epoch)
        logger.info('Measurement: catch_all')

        msg_fmt = 'catch_all,custom=v_1 f_0={value} {epoch}'

        # enough points so that both rollup windows get something
        points = 240
        for epoch in range(p.start_epoch,
                           p.start_epoch + points * p.interval,
                           p.interval):
            field_value = epoch
            msg = msg_fmt.format(value=field_value,
                                 epoch=epoch * 1000000000)
            self.send_message(msg, 0)

        msg_flt = 'clusters.{cid}.{nid}.catch_all.f_0 {value} {epoch}'
        for epoch in range(p.start_epoch,
                           p.start_epoch + points * p.interval,
                           p.interval):
            for cid in range(2):
                for nid in range(2):
                    msg = msg_flt.format(cid='c' + str(cid),
                                         nid='n' + str(nid),
                                         value=epoch,
                                         epoch=epoch)
                    self.send_message(msg, 0)

        logger.info('TestMetricProducer.produce_non_cluster_series: DONE')

    def produce_outlier_series(self):
        """
        Generates a predictable series for outlier series
        Creates a multi-node, multi-cluster high dimensional series
        with a multi-modal (multi-band) and outliers
        """
        if not hasattr(self.producer_settings, 'predictable'):
            logger.info("Not producing predictable series")
            return

        p = self.producer_settings.predictable
        uuid_suffix = '5f7a6d1-78d4-4bee-a49b-e6d9eaf1c810'
        logger.info('TestMetricProducer.produce_outlier_series')
        logger.info('TimeRange: [%d - %d]', p.start_epoch, p.end_epoch)
        logger.info('Measurement: flat_schema')
        logger.info('fields: outlier')
        logger.info('clusters: [%d - %x]%s', 0, 16, uuid_suffix)
        logger.info('nodes: n_[%d - %d]', 0, p.nodes)

        msg_flt = ('clusters.{cluster_tv}.{node_tv}.flat_schema.'
                   'outlier {value} {epoch}')

        # Generate a multi-modal distribution of data with outliers
        points = 240
        for epoch in range(p.start_epoch,
                           p.start_epoch + points * p.interval,
                           p.interval):
            for cluster in range(16):
                for node in range(p.nodes):
                    band_selector = random.uniform(0, 100)
                    if band_selector < 50:
                        # band 1 with 50% prob
                        field_value = max(0, random.gauss(10, 1))
                    elif band_selector < 80:
                        # band 2 with 30% prob
                        field_value = max(0, random.gauss(90, 1))
                    elif band_selector < 99:
                        # band 3 with 19% prob
                        field_value = max(0, random.gauss(200, 1))
                    else:
                        # outlier with 1 % prob
                        field_value = random.uniform(0, 1000)
                    partition_id = \
                        cluster % \
                        self.producer_settings.num_partitions
                    msg = msg_flt.format(cluster_tv=
                                         format(cluster, 'x') +
                                         uuid_suffix,
                                         node_tv='n_' + str(node),
                                         value=field_value,
                                         epoch=epoch)
                    self.send_message(msg, partition_id)
            logger.info('TestMetricProducer.produce_outlier_series: %d',
                        epoch)

    def produce_multi_cluster_series(self):
        """
        Generates a predictable series for multiple clusters
        used to test cluster shards
        :return:
        """
        if not hasattr(self.producer_settings, 'predictable'):
            logger.info("Not producing predictable series")
            return

        p = self.producer_settings.predictable
        uuid_suffix = '5f7a6d1-78d4-4bee-a49b-e6d9eaf1c810'
        logger.info('TestMetricProducer.produce_multi_cluster_series')
        logger.info('TimeRange: [%d - %d]', p.start_epoch, p.end_epoch)
        logger.info('Measurement: sharded_data_source')
        logger.info('fields: f_[%d - %d]', 0, p.fields)
        logger.info('clusters: [%d - %x]%s', 0, 16, uuid_suffix)
        logger.info('nodes: n_[%d - %d]', 0, p.nodes)

        msg_flt = ('clusters.{cluster_tv}.{node_tv}.flat_schema.'
                   '{field} {value} {epoch}')
        msg_fmt = ('sharded_data_source,'
                   '{cluster_tk}={cluster_tv},{node_tk}={node_tv} '
                   '{field}={value} {epoch}')

        # enough points so that both rollup windows get something
        points = 240
        for epoch in range(p.start_epoch,
                           p.start_epoch + points * p.interval,
                           p.interval):
            for cluster in range(16):
                for node in range(p.nodes):
                    for field in range(p.fields):
                        flat_field = 'f_{}.c_{}.n_{}.count'.\
                            format(field, cluster, node)
                        tagged_field = 'f_{}.count'.format(field)
                        field_value = epoch
                        partition_id = \
                            cluster % \
                            self.producer_settings.num_partitions
                        msg = msg_fmt.format(cluster_tk='cluster',
                                             cluster_tv=
                                             format(cluster, 'x') +
                                             uuid_suffix,
                                             node_tk='node',
                                             node_tv='n_' + str(node),
                                             field=tagged_field,
                                             value=field_value,
                                             epoch=epoch * 1000000000)
                        self.send_message(msg, partition_id)
                        msg = msg_flt.format(cluster_tv=
                                             format(cluster, 'x') +
                                             uuid_suffix,
                                             node_tv='n_' + str(node),
                                             field=flat_field,
                                             value=field_value,
                                             epoch=epoch)
                        self.send_message(msg, partition_id)
            logger.info('TestMetricProducer.produce_multi_cluster_series: %d',
                        epoch)

    def produce_predictable_series(self):
        """
        Generates a predictable series whose values can be validated
        through the read query test for an end to end correctness test
        :return:
        """
        if not hasattr(self.producer_settings, 'predictable'):
            logger.info("Not producing predictable series")
            return

        p = self.producer_settings.predictable
        logger.info('TestMetricProducer.produce_predictable_series')
        logger.info('TimeRange: [%d - %d]', p.start_epoch, p.end_epoch)
        logger.info('Measurement: %s', p.measurement)
        logger.info('fields: f_[%d - %d]', 0, p.fields)
        logger.info('clusters: c_[%d - %d]', 0, p.clusters)
        logger.info('nodes: n_[%d - %d]', 0, p.nodes)
        logger.info('values_start=%d value_inc=%d]', p.value_start,
                    p.value_increment)

        msg_flt = ('clusters.{cluster_tv}.{node_tv}.flat_schema.'
                   '{field} {value} {epoch}')
        msg_fmt = ('{measurement},'
                   '{cluster_tk}={cluster_tv},{node_tk}={node_tv} '
                   '{field}={value} {epoch}')
        msg_eph = ('{measurement},'
                   '{cluster_tk}={cluster_tv},{node_tk}={node_tv},'
                   '{ephemeral_tk}={ephemeral_tv} '
                   '{field}={value} {epoch}')

        value = p.value_start
        for epoch in range(p.start_epoch, p.end_epoch, p.interval):
            for cluster in range(p.clusters):
                for node in range(p.nodes):
                    for field in range(p.fields):
                        for field_type in ['p99',
                                           'm1_rate',
                                           'regular.count',
                                           'intermittent.count',
                                           'ephemeral.count',
                                           'transform.count',
                                           'flat.dot.schema.count',
                                           'clock_skewed_6d.count']:
                            flat_field = 'f_{}.c_{}.n_{}.{}'.\
                                format(field, cluster, node, field_type)
                            tagged_field = 'f_{}.{}'.format(field, field_type)
                            field_value = value + node*10
                            point_idx = int((epoch - p.start_epoch) /
                                            p.interval)
                            ephemeral_id = None
                            if field_type == 'regular.count':
                                # verify loss less
                                field_value = epoch
                            if field_type == 'intermittent.count':
                                # We drop 5 consecutive data points
                                # after every 15 data points
                                if point_idx % 40 > 20:
                                    # drop
                                    logger.info('intermittent.count: dropping'
                                                ' epoch: %d pidx: %d',
                                                epoch, point_idx)
                                    continue
                                field_value = epoch
                            if field_type == 'ephemeral.count':
                                field_value = epoch
                                ephemeral_id = int(point_idx/5)
                            if field_type == 'transform.count':
                                field_value = epoch

                            sepoch = epoch
                            if field_type == 'clock_skewed_6d.count':
                                # skew the clock in the past by 6 days
                                sepoch = epoch - 6*864000

                            if field_type.startswith('flat'):
                                field_value = epoch
                                msg = msg_flt.format(cluster_tv=
                                                     'c_' + str(cluster),
                                                     node_tv='n_' + str(node),
                                                     field=flat_field,
                                                     value=field_value,
                                                     # graphite flat timestamp
                                                     # is in seconds
                                                     epoch=sepoch)

                            elif ephemeral_id is None:
                                msg = msg_fmt.format(measurement=p.measurement,
                                                     cluster_tk='cluster',
                                                     cluster_tv=
                                                     'c_' + str(cluster),
                                                     node_tk='node',
                                                     node_tv='n_' + str(node),
                                                     field=tagged_field,
                                                     value=field_value,
                                                     epoch=sepoch * 1000000000)
                            else:
                                msg = msg_eph.format(measurement=p.measurement,
                                                     cluster_tk='cluster',
                                                     cluster_tv=
                                                     'c_' + str(cluster),
                                                     node_tk='node',
                                                     node_tv='n_' + str(node),
                                                     ephemeral_tk='ephemeral',
                                                     ephemeral_tv=ephemeral_id,
                                                     field=tagged_field,
                                                     value=field_value,
                                                     epoch=epoch * 1000000000)

                            partition_id = \
                                cluster % \
                                self.producer_settings.num_partitions
                            self.send_message(msg, partition_id)
            logger.info('TestMetricProducer.produce_predictable_series: %d %d',
                        epoch, value)
            value = value + p.value_increment

    def produce_live_stream(self):
        """
        Generates a continuous ongoing datastream
        """
        last_series_value = dict()
        logger.info('TestMetricProducer.start_producing with '
                    'prefix: %s start_cluster_id: %s',
                    self.producer_settings.metric_prefix,
                    self.start_cluster_id)
        logger.info('Producing %d - %d epochs @ %d interval ',
                    self.producer_settings.epoch_start,
                    self.producer_settings.epoch_end,
                    self.producer_settings.interval_seconds)
        logger.info('Producing %d fields '
                    'for %d nodes per cluster '
                    'for %d clusters ',
                    self.producer_settings.num_fields,
                    self.producer_settings.num_nodes,
                    self.producer_settings.num_clusters)
        logger.info('Producing values using random walk over [%d, %d] '
                    'with step range of [%d,%d]',
                    self.producer_settings.min_value,
                    self.producer_settings.max_value,
                    self.producer_settings.min_change,
                    self.producer_settings.max_change)
        mid_value = (self.producer_settings.min_change +
                     self.producer_settings.max_change)/2
        msg_fmt = ('{datasource},{cluster_tk}={cluster_tv},'
                   '{node_tk}={node_tv} {field}={value} {epoch}')

        cluster_tk = self.producer_settings.cluster_tag_key
        node_tk = self.producer_settings.node_tag_key

        start_time = time.time()
        total_data_points = 0
        last_epoch_points = 0
        epoch = self.producer_settings.epoch_start

        while epoch < self.producer_settings.epoch_end or \
                self.producer_settings.live:
            last_epoch_start_time = time.time()
            for cluster_id in range(self.start_cluster_id,
                                    self.start_cluster_id +
                                    self.producer_settings.num_clusters):
                for node_id in range(self.producer_settings.num_nodes):
                    for datasource, _, _ in self.context.schema.datasources():
                        if datasource != 'graphite_flat' and \
                                not datasource.startswith(
                                self.producer_settings.metric_prefix):
                            continue
                        for field_id in range(
                                self.producer_settings.num_fields + 1):
                            point_idx = int((epoch -
                                             self.producer_settings.epoch_start) /
                                            self.producer_settings.interval_seconds)
                            if field_id == self.producer_settings.num_fields \
                                    and point_idx % 2 == 0:
                                # Skip alternate points for the last field
                                continue

                            if datasource == 'graphite_flat':
                                field_name = 'process.p_{}.cpu_percent'.\
                                    format(field_id)
                            else:
                                field_name = self.producer_settings.\
                                                 field_prefix + str(field_id)
                            if self.producer_settings.use_random_walk:
                                # use the random walk i.e. next value is
                                # based on last value
                                series = '{}{}{}{}'.format(datasource,
                                                           field_name,
                                                           cluster_id,
                                                           node_id)
                                last_value = last_series_value[series] \
                                    if series in last_series_value \
                                    else mid_value
                            else:
                                # next value is independent of last_value
                                last_value = -1
                            value = self.generate_random_value(last_value)

                            if self.producer_settings.use_random_walk:
                                last_series_value[series] = value
                            cluster_value = '{}{}'.format(
                                self.producer_settings.cluster_tag_value_prefix,
                                cluster_id
                            )
                            # ensure we have a unique node value
                            node_value = '{}{}{}'.format(
                                self.producer_settings.node_tag_value_prefix,
                                cluster_id,
                                node_id
                            )
                            msg = msg_fmt.format(datasource=datasource,
                                                 cluster_tk=cluster_tk,
                                                 cluster_tv=cluster_value,
                                                 node_tk=node_tk,
                                                 node_tv=node_value,
                                                 field=field_name,
                                                 value=value,
                                                 epoch=epoch*1000000000)
                            partition_id = \
                                cluster_id % \
                                self.producer_settings.num_partitions
                            self.send_message(msg, partition_id)
                            total_data_points = total_data_points + 1
                            last_epoch_points = last_epoch_points + 1
            duration = time.time() - last_epoch_start_time
            if duration > 0:
                throughput = last_epoch_points / duration
                logger.info('Epoch: %d throughput %d DPS', epoch, throughput)
                last_epoch_points = 0
            epoch = epoch + self.producer_settings.interval_seconds
            # for live data check if we have reached current time
            if self.producer_settings.live:
                interval_before_now = int(time.time()) - \
                                    self.producer_settings.interval_seconds
                if epoch >= interval_before_now:
                    logger.info('Generating live stream... sleeping now')
                    time.sleep(int(epoch - interval_before_now))
            # Sleep for 1 seconds, to simulate a periodic burst
            if self.producer_settings.throttle_time > 0:
                time.sleep(self.producer_settings.throttle_time)
            else:
                time.sleep(1)
        duration = time.time() - start_time
        throughput = total_data_points / duration
        logger.info('throughput: %d DPS', throughput)
        logger.info('TestMetricProducer.start_producing: finished')

    def start_producing(self):
        """
        Starts the single threaded kafka producer
        :return:
        """
        self.produce_outlier_series()
        self.produce_non_cluster_series()
        self.produce_multi_cluster_series()
        self.produce_predictable_series()
        self.produce_live_stream()


class TestBatchMetricProducer:
    """Produce batch ingest messages"""

    def __init__(self, sparse_producer_settings, dense_producer_settings):
        self.sparse_producer_settings = sparse_producer_settings
        self.dense_producer_settings = dense_producer_settings
        self.sparse_producer = kafka.KafkaProducer(
            bootstrap_servers=sparse_producer_settings.kafka_brokers,
            batch_size=16384*10)
        self.dense_producer = kafka.KafkaProducer(
            bootstrap_servers=dense_producer_settings.kafka_brokers,
            batch_size=16384*10)

    def send_sparse_message(self, msg: str, key=None) -> None:
        """
        Send the message to the kafka
        :param msg:
        :param partition_id
        """
        self.sparse_producer.send(
            self.sparse_producer_settings.topic,
            value=msg.encode('utf-8'),
            key=key.encode('utf-8') if key else None
        )
        logger.info("Send sparse batch ingest message %s", msg)

    def send_dense_message(self, msg: str, partition=None) -> None:
        """
        Send the message to the kafka
        :param msg:
        :param partition_id
        """
        self.dense_producer.send(
            self.dense_producer_settings.topic,
            value=msg.encode('utf-8'),
            partition=partition
        )
        logger.info("Send dense batch ingest message %s", msg)

    def _produce_local_5_3_sample_messages(self):
        batch_producer_settings = Settings.inst.test_sparse_batch_producer
        datasources = \
            batch_producer_settings.testcases.tc_5_3_sample.datasources
        for datasource in datasources:
            file_path = \
                os.path.join(
                    # Navigate to $NBDB_REPO_ROOT/python
                    # Druid root is $NBDB_REPO_ROOT/python/dist/druid/
                    #               imply-<VERSION>/
                    "../../../",
                    "nbdb",
                    "load_simulator",
                    "batch_consumer_testcases",
                    "5_3_sample",
                    f"{datasource}.json.gz"
                )
            message = {
                "location_type": "local",
                "path": file_path,
                "datasource": datasource,
                # Timestamps are unused right now
                "start_ts": -1,
                "end_ts": -1
                }
            message_serialized = json.dumps(message)
            # Verify that message can be parsed
            SparseBatchMessage.from_serialized_json(message_serialized)
            self.send_sparse_message(message_serialized, key=datasource)

    def produce_local_messages(self):
        """
        Create messages for batch json files for local testcases.
        """
        self._produce_local_5_3_sample_messages()

    def _produce_s3_5_3_sample_messages(self):
        batch_producer_settings = Settings.inst.test_sparse_batch_producer
        s3_prefix = batch_producer_settings.testcases_s3_prefix
        datasources = \
            batch_producer_settings.testcases.tc_5_3_sample.datasources
        for datasource in datasources:
            message = {
                "location_type": "s3",
                "path": s3_prefix + "5_3_sample/" + datasource + ".json.gz",
                "datasource": datasource,
                # Timestamps are unused right now
                "start_ts": -1,
                "end_ts": -1
                }
            message_serialized = json.dumps(message)
            # Verify that message can be parsed
            SparseBatchMessage.from_serialized_json(message_serialized)
            self.send_sparse_message(message_serialized, key=datasource)

    def produce_s3_messages(self):
        """
        Create messages for batch json files for s3 testcases.
        """
        self._produce_s3_5_3_sample_messages()

    def _produce_dense_s3_5_3_sample_messages(self):
        dense_producer_settings = self.dense_producer_settings
        num_partitions = dense_producer_settings.num_partitions
        partition = 0
        testcase = dense_producer_settings.testcases.tc_5_3_sample
        cluster_id = testcase.cluster_id
        partition = (partition + 1) % num_partitions
        message = {
                "cluster_id": cluster_id,
                "batch_json_s3_key": testcase.s3_key,
                "size_bytes": 1024 ** 3
            }
        message_serialized = json.dumps(message)
        # Verify that message can be parsed
        DenseBatchMessage.from_serialized_json(message_serialized)
        self.send_dense_message(message_serialized, partition=partition)

    def produce_dense_s3_message(self):
        self._produce_dense_s3_5_3_sample_messages()

    def start_producing(self):
        """
        Produce batch ingest messages.
        """
        self.produce_local_messages()
        self.produce_s3_messages()
        self.produce_dense_s3_message()


class MetricProducerApp(EntryPoint):
    """
    Entry point class for the metric consumer
    """

    def __init__(self):
        EntryPoint.__init__(self)

    @staticmethod
    @retry(stop_max_attempt_number=4, wait_fixed=30000)
    def ensure_topic_partitions_exist(client, topic_name, exp_count) -> None:
        """Check if topic partition count matches expected count."""
        partition_ids = client.get_partition_ids_for_topic(topic_name)
        logger.info("Expected %d partitions for topic, saw %d partitions",
                    exp_count, len(partition_ids))
        assert len(partition_ids) == exp_count, \
            "Expected partitions don't exist"

    # Since we try to recreate a topic right after deleting it, we sometimes
    # run into errors. Retry for a while
    @staticmethod
    @retry(stop_max_attempt_number=4, wait_fixed=15000)
    def create_topic(bootstrap_servers, topic_name, num_partitions,
                     replication_factor) -> None:
        """Create the requested topic"""
        admin = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
        logger.info("Trying to create topic %s", topic_name)
        topic_list = [NewTopic(name=topic_name, num_partitions=num_partitions,
                               replication_factor=replication_factor,
                               topic_configs={'retention.ms': 10800000,
                                              'segment.ms': 10800000})]
        admin.create_topics(topic_list)

        KafkaUtils.wait_for_topic_status(bootstrap_servers, topic_name,
                                         exists=True)
        logger.info("Successfully created topic %s", topic_name)

    @staticmethod
    def delete_topic(bootstrap_servers, topic_name) -> None:
        """Delete previously existing topic."""
        admin = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
        try:
            admin.delete_topics([topic_name], timeout_ms=120000)
        except kafka.errors.UnknownTopicOrPartitionError as e:
            # Topic doesn't exist
            logger.info('IGNORE: Failed to delete topic: %s Exception: %s.',
                        topic_name, str(e))

        KafkaUtils.wait_for_topic_status(bootstrap_servers, topic_name,
                                         exists=False)
        logger.info("Deleted topic %s", topic_name)

    @staticmethod
    def prepare_for_testing(context: Context, local: bool) -> None:
        """
        Clear Kafka topics, ScyllaDB tables, ES index for a fresh test
        run.
        :param local: In local run we clean the schema and creates afresh
        """
        stream_producer_settings = Settings.inst.test_metric_producer
        sparse_batch_producer_settings = \
            Settings.inst.test_sparse_batch_producer
        dense_batch_producer_settings = \
            Settings.inst.test_dense_batch_producer
        if local:
            # In local mode we always start with clean slate
            # lets make sure there is no garbage left from a previous run
            client = kafka.KafkaClient(stream_producer_settings.kafka_brokers)
            if len(client.topics) > 0:
                logger.warning('TestMetricProducer: In Local Mode we expect'
                               'to start with a clean slate. Found the '
                               'following topics: %s are leftover from '
                               'an older run. \n Please run:\n'
                               'sudo docker system prune -a\n'
                               'and try again', ','.join(client.topics))
                # raise ValueError('unclean state found: {}'.format(
                #     ','.join(client.topics)))

        # Wait for overlord to be up
        druid_conn_settings = Settings.inst.Druid.connection_string
        MetricProducerApp.wait_for_conn_up(
            druid_conn_settings.overlord_ip,
            druid_conn_settings.overlord_port,
            timeout_secs=180)

        # Create the sparse topics and druid specs
        SchemaCreator.create(context=context, clean=local)

        if local:
            MetricProducerApp._recreate_streaming_producer_topic(
                stream_producer_settings)
            MetricProducerApp._recreate_batch_producer_topic(
                sparse_batch_producer_settings)
            MetricProducerApp._recreate_batch_producer_topic(
                dense_batch_producer_settings)

        # Wait for metrics topic to exist with necessary partitions
        logger.info("Waiting for topic %s partitions to exist",
                    stream_producer_settings.topic)
        client = kafka.KafkaClient(stream_producer_settings.kafka_brokers)
        MetricProducerApp.ensure_topic_partitions_exist(
            client,
            stream_producer_settings.topic,
            stream_producer_settings.num_partitions)
        MetricProducerApp.ensure_topic_partitions_exist(
            client,
            sparse_batch_producer_settings.topic,
            sparse_batch_producer_settings.num_partitions)

    def wait_for_service_ready(self) -> None:
        # Wait for connections to be up
        super(MetricProducerApp, self).wait_for_service_ready()

        if flags.FLAGS.setup == 1 or flags.FLAGS.local:
            logger.info('Setup enabled preparing for testing')
            # Wait for Kafka brokers to be ready
            MetricProducerApp.prepare_for_testing(self.context,
                                                  flags.FLAGS.local)
        else:
            logger.info('\n\n****** WARNING ********\n\n')
            logger.info('*\tWARNING: setup not defined, '
                        'so skipping prepare for testing')
            logger.info('\n\n******* WARNING *******\n\n')

    def start(self):
        """
        Initialize the write path and start the consumers
        :return:
        """
        logger.info("Creating the TestMetricProducer")
        batch_metric_producer = TestBatchMetricProducer(
            Settings.inst.test_sparse_batch_producer,
            Settings.inst.test_dense_batch_producer
        )
        stream_metric_producer = TestStreamMetricProducer(
            self.context,
            Settings.inst.test_metric_producer,
            flags.FLAGS.start_cluster_id
        )

        logger.info("Starting to produce batch metrics")
        batch_metric_producer.start_producing()
        logger.info("Starting to produce stream metrics")
        stream_metric_producer.start_producing()
        logger.info('Done generating all data. Sleeping indefinitely now')
        time.sleep(86400*1000)

    @staticmethod
    def _recreate_streaming_producer_topic(stream_producer_settings):
        logger.info('LocalMode: Deleting the %s topic and recreating',
                    stream_producer_settings.topic)
        # Clear old messages in Kafka topics used by metrics consumer
        MetricProducerApp.delete_topic(
            stream_producer_settings.kafka_brokers,
            stream_producer_settings.topic)
        # Recreate the topic
        MetricProducerApp.create_topic(
            stream_producer_settings.kafka_brokers,
            stream_producer_settings.topic,
            num_partitions=stream_producer_settings.num_partitions,
            replication_factor=stream_producer_settings.repl_factor)

    @staticmethod
    def _recreate_batch_producer_topic(settings):
        logger.info('LocalMode: Deleting the %s topic and recreating',
                    settings.topic)
        # Clear old messages in Kafka topics used by metrics consumer
        MetricProducerApp.delete_topic(
            settings.kafka_brokers,
            settings.topic)
        # Recreate the topic
        MetricProducerApp.create_topic(
            settings.kafka_brokers,
            settings.topic,
            num_partitions=settings.num_partitions,
            replication_factor=settings.repl_factor)


if __name__ == "__main__":
    app.run(MetricProducerApp().run)
