"""
Main entry point for the write path, starts the metric consumers
that queries the kafka topics for new metric writes
"""
import logging
import random

from absl import app

from nbdb.common.entry_point import EntryPoint
from nbdb.common.metric_parsers import MetricParsers
from nbdb.common.thread_pools import ThreadPools
from nbdb.store.sparse_series_utils import init_kafka_sparse_series_writer
from nbdb.store.sparse_series_writer import SparseSeriesWriter

logger = logging.getLogger()


class TestMetricGeneratorApp(EntryPoint):
    """
    Entry point class for the metric consumer
    """

    def __init__(self):
        EntryPoint.__init__(self)

    def start(self):
        """
        Initialize the write path and start the consumers
        :return:
        """
        logger.info("Creating the SparseSeriesWriter")
        # pylint: disable-msg=W0201
        # Attribute Defined Outside Init
        self.sparse_series_writer = init_kafka_sparse_series_writer(
            context=self.context)
        self.metric_parsers = MetricParsers(self.context, 'influx')

        logger.info("Creating the metric_consumers")

        consumer_futures = list()
        for process in range(10):
            consumer_futures.append(
                ThreadPools.inst.metric_consumer_pool.submit(
                    TestMetricGeneratorApp.generate_test_data,
                    self.sparse_series_writer,
                    process))

        logger.info('All consumers scheduled')
        for consumer_future in consumer_futures:
            consumer_future.result()
        logger.info('All consumers finished')

        ThreadPools.inst.stop()

    @staticmethod
    def generate_test_data(sparse_series_writer: SparseSeriesWriter,
                           metric_parsers: MetricParsers,
                           process: int) -> None:
        """
        Generates dummy data that is pushed to the store
        """
        last_series_value = dict()
        for epoch in range(1, 100):
            for node in range(10):
                series = '{}{}'.format(node, process)
                last_value = last_series_value[series] \
                    if series in last_series_value else 500
                value = max(0, min(1000,
                                   int(last_value +
                                       random.uniform(-5, 5))))
                last_series_value[series] = value

                msg = 'service_2.process,node={},' \
                      'process={} test_field_8={} {}'. \
                    format(node, process, value, epoch)
                logger.info('generated: %s', msg)
                metric_values = metric_parsers.parse_influx_metric_values(msg)
                sparse_series_writer.append_metrics(metric_values)


if __name__ == "__main__":
    app.run(TestMetricGeneratorApp().run)
