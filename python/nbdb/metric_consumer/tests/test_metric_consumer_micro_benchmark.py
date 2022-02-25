"""
Unit tests for MetricConsumer
"""
import cProfile
import logging
import os
import sys
import time
import unittest

from mock import Mock

from nbdb.common.context import Context
from nbdb.common.data_point import CLUSTER_TAG_KEY, NODE_TAG_KEY
from nbdb.common.data_point import MODE_REALTIME, MODE_ROLLUP
from nbdb.store.sparse_kafka_store import SparseKafkaStore
from nbdb.common.telemetry import Telemetry
from nbdb.common.tracing_config import TracingConfig
from nbdb.schema.schema import Schema
from nbdb.store.sparse_series_writer import SparseSeriesWriter
from nbdb.config.settings import Settings
from nbdb.metric_consumer.metric_consumer import MetricConsumer


class TestProducer:
    """
    Mocks Kafka producer, just counts messages and bytes
    """

    def __init__(self):
        """

        """
        self.count = 0
        self.bytes = 0

    def produce(self, measurement, msg) -> None:
        """

        :param measurement:
        :param msg:
        :return:
        """
        _ = measurement
        self.count = self.count + 1
        self.bytes = self.bytes + len(msg)*2

    def poll(self, timeout):
        """
        Do nothing
        :param timeout:
        :return:
        """
        _ = self
        _ = timeout

    def flush(self, timeout=None) -> int:
        """
        do nothing
        :param timeout:
        :return: queue size
        """
        _ = self
        _ = timeout
        return 0

    def reset(self) -> None:
        """
        Reset the counters
        :return:
        """
        self.count = 0
        self.bytes = 0

    def avg_msg_size(self) -> float:
        """
        Report the average size of message received
        :return:
        """
        return 0 if self.count == 0 else int(1.0 * self.bytes/ self.count)


class TestMetricConsumerMicroBenchmark(unittest.TestCase):
    """
    Basic Single Threaded test for MetricConsumer
    This does not test for concurrency or persistence

    """

    def setUp(self):
        """
        Initialize the MetricConsumer
        :return:
        """
        root = logging.getLogger()
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(logging.INFO)
        root.addHandler(handler)

        Settings.load_yaml_settings(os.path.dirname(__file__) +
                                    '/test_settings.yaml')
        schema = Schema.load_from_file(
            os.path.dirname(__file__) + '/test_schema.yaml')
        self.context = Context(schema=schema)

        # instantiate the entire write path and mock only external dependencies
        if Telemetry.inst is None:
            Telemetry.initialize()
        if TracingConfig.inst is None:
            TracingConfig.initialize(Settings.inst.tracing_config)

    def init_consumer(self, consumer_mode: str):
        """
        Create consumer in given mode
        """
        SparseKafkaStore.instantiate_producer = Mock()

        self.sparse_kafka_store = SparseKafkaStore(
            Settings.inst.sparse_kafka.connection_string)
        # Mock the kafka producer explicitly so we measure perf of everything
        # till this point
        self.sparse_kafka_store.producer = TestProducer()
        self.sparse_series_writer = SparseSeriesWriter(
            sparse_store=self.sparse_kafka_store,
            sparse_store_settings=Settings.inst.sparse_store,
            tracing_config_settings=Settings.inst.tracing_config,
            context=self.context,
            protocol=Settings.inst.metric_consumer.metric_protocol,
            sparse_telemetry_source_id="test",
            past_message_lag=10**9,
            future_message_lag=10**9,
            consumer_mode=consumer_mode)
        self.consumer = MetricConsumer(self.context,
                                       Settings.inst.metric_consumer,
                                       self.sparse_series_writer,
                                       MODE_REALTIME)
        self.consumer.last_consumer_committed_offsets['A'] = 0

    def tearDown(self) -> None:
        """
        :return:
        """

    @staticmethod
    def create_kafka_msg(str_msg):
        """Create Kafka like message tuple with partition & offset"""
        # Pick an offset higher than the committed offset to ensure this
        # message isn't treated like a replayed message.
        offset = 20
        return (str_msg, 'A', offset)

    @staticmethod
    def _generate_messages(num_clusters, num_nodes, points, interval):
        """
        Generate dummy data
        :param num_clusters:
        :param num_nodes:
        :return:
        """
        end_epoch = int(time.time())
        start_epoch = end_epoch - points*interval
        msgs = list()
        for c in range(0, num_clusters):
            for n in range(0, num_nodes):
                for e in range(start_epoch, end_epoch, interval):
                    msg = 'Diamond.cassandratablestats.Stats.Stat.' \
                          'CloudStorage.' \
                          'cluster-uuid-x-y-z.' \
                          'numOracleDbs,' \
                          '{}=f5888c22-9651-4cd0-8e3a-90367d9242c{},' \
                          '{}=RVM123{} process.cpu_percent={} {}'.\
                          format(CLUSTER_TAG_KEY, c,
                                 NODE_TAG_KEY, n,
                                 e,
                                 e*1000000000)
                    msgs.append(
                        TestMetricConsumerMicroBenchmark.create_kafka_msg(
                            msg))
        return msgs

    def _process(self, msgs):
        """

        :param msgs:
        :return:
        """
        for msg, partition, offset in msgs:
            self.consumer.process_item(msg, partition, offset)

    def run_benchmark(self, consumer_mode: str):
        """
        Run benchmark
        """
        self.init_consumer(consumer_mode)
        enable_profile = False
        pr = cProfile.Profile()
        msgs = TestMetricConsumerMicroBenchmark._generate_messages(
            num_clusters=10, num_nodes=10, points=1000, interval=600)

        t_proc_time = time.time()
        if enable_profile:
            pr.enable()
        self._process(msgs)
        # Not sure why pylint thinks this member doesnt exist, it does !
        # pylint: disable=no-member
        self.sparse_kafka_store.flush()
        t_proc_time = time.time() - t_proc_time
        if enable_profile:
            pr.disable()
            pr.print_stats(sort="tottime")

        throughput = int(len(msgs)/t_proc_time)
        throughput_kbps = int(self.sparse_kafka_store.producer.bytes /
                              (1024*t_proc_time))
        avg_msg_size = int(self.sparse_kafka_store.producer.avg_msg_size())
        return throughput, throughput_kbps, avg_msg_size

    def test_realtime_throughput(self):
        """
        Checks that the realtime throughput is as expected
        :return:
        """
        throughput, throughput_kbps, avg_msg_size = self.run_benchmark(
            MODE_REALTIME)

        reference_throughput = 8000
        reference_kbps = 8000
        reference_msg_size = 498

        performance_drop = int(100 - throughput*100/reference_throughput)
        performance_drop_kbps = int(100 - throughput_kbps*100/reference_kbps)

        print('Realtime Throughput: {} msgs/s {} KBps {} bytes/msg'.
              format(throughput, throughput_kbps, avg_msg_size))
        print('Realtime Drop: Throughput: {} % KBps: {}%'.
              format(performance_drop, performance_drop_kbps))

        self.assertEqual(reference_msg_size, avg_msg_size,
                         'This test assumes a {} msg size, looks like it'
                         ' changed. Please double check perf with original '
                         'value and then update the reference profile'.
                         format(reference_msg_size))

        last_profile = \
            "\n ncalls  tottime  percall  cumtime  percall " \
            "filename:lineno(function)"\
            "\n 224700    0.770    0.000    0.987    0.000 " \
            "data_point.py:31(__init__)"\
            "\n 100000    0.613    0.000    1.911    0.000 " \
            "metric_parsers.py:28(parse_influx_metric_values)"\
            "\n 200400    0.573    0.000    0.864    0.000 " \
            "data_point.py:69(to_druid_json_str)"\
            "\n 2346305    0.502    0.000    0.502    0.000 " \
            "{built-in method builtins.getattr}"\
            "\n 399600    0.498    0.000    2.923    0.000 " \
            "rollups.py:67(_add_to_window)"\
            "\n 200400    0.475    0.000    1.587    0.000 " \
            "sparse_kafka_store.py:86(write)"\
            "\n 249601    0.403    0.000    0.548    0.000 " \
            "meter.py:41(mark)"\
            "\n 649700    0.398    0.000    0.555    0.000 " \
            "sparse_series_stats.py:103(get_window_epoch)"\
            "\n 199900    0.344    0.000    3.973    0.000 " \
            "rollups.py:47(add)"\
            "\n 648000    0.321    0.000    0.455    0.000 " \
            "sparse_series_stats.py:120(get_window_value)"\
            "\n 423000    0.317    0.000    0.948    0.000 " \
            "rollups.py:202(_increment)"\
            "\n 400200    0.312    0.000    0.312    0.000 " \
            "{method 'match' of 're.Pattern' objects}"\
            "\n 100000    0.273    0.000    4.178    0.000 " \
            "sparse_series_writer.py:232(check_inline_tombstone)"\
            "\n 199900    0.259    0.000    6.853    0.000 " \
            "sparse_series_writer.py:312(_write)"\
            "\n 325200    0.257    0.000    0.641    0.000 " \
            "sparse_algo_selector.py:90(select_algo)"\
            "\n 399600    0.246    0.000    0.339    0.000 " \
            "sparse_series_stats.py:158(get_check_point)"\
            "\n 849600    0.237    0.000    0.237    0.000 " \
            "{built-in method builtins.setattr}"\
            "\n 100000    0.233    0.000    9.566    0.000 " \
            "sparse_series_writer.py:109(append)"\
            "\n 200000    0.213    0.000    0.306    0.000" \
            " metric_parsers.py:108(_parse_key_value_pairs)"\
            "\n 199800    0.195    0.000    0.530    0.000" \
            " schema.py:357(get_rollup_function)"\
            "\n 124800    0.191    0.000    0.647    0.000 " \
            "sparse_telemetry.py:33(report)"\
            "\n 100000    0.188    0.000   11.948    0.000 " \
            "metric_consumer.py:53(process_item)"\
            "\n 200400    0.186    0.000    0.186    0.000 " \
            "data_point.py:80(<listcomp>)"\
            "\n 624600    0.186    0.000    0.186    0.000 " \
            "{method 'split' of 'str' objects}"\
            "\n 200400    0.172    0.000    0.520    0.000 " \
            "sparse_algo_lossless.py:73(update_written_point)"\
            "\n 124800    0.158    0.000    1.812    0.000 " \
            "sparse_algo_selector.py:56(process)"\
            "\n 748803    0.146    0.000    0.146    0.000 " \
            "moving_average.py:37(add)"
        msg = '\n MetricParser throughput (msgs/s) dropped by {} % ' \
              '\n throughput (kbps) dropped by {} % ' \
              '\n Make sure your CPU usage is not high' \
              '\n Please set enable_profile=True and test again' \
              '\n For reference the following profile had' \
              ' throughput of {} msgs/s {} kbps \n{}'.\
            format(performance_drop,
                   performance_drop_kbps,
                   reference_throughput,
                   reference_kbps,
                   last_profile)
        self.assertGreater(20, performance_drop, msg)
        self.assertGreater(20, performance_drop_kbps, msg)
        self.sparse_kafka_store.producer.reset()

    def test_rollup_throughput(self):
        """
        Checks that the rollup throughput is as expected
        :return:
        """
        throughput, throughput_kbps, avg_msg_size = self.run_benchmark(
            MODE_ROLLUP)

        reference_throughput = 8000
        reference_kbps = 186
        reference_msg_size = 508

        performance_drop = int(100 - throughput*100/reference_throughput)
        performance_drop_kbps = int(100 - throughput_kbps*100/reference_kbps)

        print('Rollup Throughput: {} msgs/s {} KBps {} bytes/msg'.
              format(throughput, throughput_kbps, avg_msg_size))
        print('Rollup Drop: Throughput: {} % KBps: {}%'.
              format(performance_drop, performance_drop_kbps))

        self.assertEqual(reference_msg_size, avg_msg_size,
                         'This test assumes a {} msg size, looks like it'
                         ' changed. Please double check perf with original '
                         'value and then update the reference profile'.
                         format(reference_msg_size))

        last_profile = \
            "\n ncalls  tottime  percall  cumtime  percall " \
            "filename:lineno(function)"\
            "\n 224700    0.770    0.000    0.987    0.000 " \
            "data_point.py:31(__init__)"\
            "\n 100000    0.613    0.000    1.911    0.000 " \
            "metric_parsers.py:28(parse_influx_metric_values)"\
            "\n 200400    0.573    0.000    0.864    0.000 " \
            "data_point.py:69(to_druid_json_str)"\
            "\n 2346305    0.502    0.000    0.502    0.000 " \
            "{built-in method builtins.getattr}"\
            "\n 399600    0.498    0.000    2.923    0.000 " \
            "rollups.py:67(_add_to_window)"\
            "\n 200400    0.475    0.000    1.587    0.000 " \
            "sparse_kafka_store.py:86(write)"\
            "\n 249601    0.403    0.000    0.548    0.000 " \
            "meter.py:41(mark)"\
            "\n 649700    0.398    0.000    0.555    0.000 " \
            "sparse_series_stats.py:103(get_window_epoch)"\
            "\n 199900    0.344    0.000    3.973    0.000 " \
            "rollups.py:47(add)"\
            "\n 648000    0.321    0.000    0.455    0.000 " \
            "sparse_series_stats.py:120(get_window_value)"\
            "\n 423000    0.317    0.000    0.948    0.000 " \
            "rollups.py:202(_increment)"\
            "\n 400200    0.312    0.000    0.312    0.000 " \
            "{method 'match' of 're.Pattern' objects}"\
            "\n 100000    0.273    0.000    4.178    0.000 " \
            "sparse_series_writer.py:232(check_inline_tombstone)"\
            "\n 199900    0.259    0.000    6.853    0.000 " \
            "sparse_series_writer.py:312(_write)"\
            "\n 325200    0.257    0.000    0.641    0.000 " \
            "sparse_algo_selector.py:90(select_algo)"\
            "\n 399600    0.246    0.000    0.339    0.000 " \
            "sparse_series_stats.py:158(get_check_point)"\
            "\n 849600    0.237    0.000    0.237    0.000 " \
            "{built-in method builtins.setattr}"\
            "\n 100000    0.233    0.000    9.566    0.000 " \
            "sparse_series_writer.py:109(append)"\
            "\n 200000    0.213    0.000    0.306    0.000" \
            " metric_parsers.py:108(_parse_key_value_pairs)"\
            "\n 199800    0.195    0.000    0.530    0.000" \
            " schema.py:357(get_rollup_function)"\
            "\n 124800    0.191    0.000    0.647    0.000 " \
            "sparse_telemetry.py:33(report)"\
            "\n 100000    0.188    0.000   11.948    0.000 " \
            "metric_consumer.py:53(process_item)"\
            "\n 200400    0.186    0.000    0.186    0.000 " \
            "data_point.py:80(<listcomp>)"\
            "\n 624600    0.186    0.000    0.186    0.000 " \
            "{method 'split' of 'str' objects}"\
            "\n 200400    0.172    0.000    0.520    0.000 " \
            "sparse_algo_lossless.py:73(update_written_point)"\
            "\n 124800    0.158    0.000    1.812    0.000 " \
            "sparse_algo_selector.py:56(process)"\
            "\n 748803    0.146    0.000    0.146    0.000 " \
            "moving_average.py:37(add)"
        msg = '\n MetricParser throughput (msgs/s) dropped by {} % ' \
              '\n throughput (kbps) dropped by {} % ' \
              '\n Make sure your CPU usage is not high' \
              '\n Please set enable_profile=True and test again' \
              '\n For reference the following profile had' \
              ' throughput of {} msgs/s {} kbps \n{}'.\
            format(performance_drop,
                   performance_drop_kbps,
                   reference_throughput,
                   reference_kbps,
                   last_profile)
        self.assertGreater(20, performance_drop, msg)
        self.assertGreater(20, performance_drop_kbps, msg)
        self.sparse_kafka_store.producer.reset()
