"""
TestMetricProducer
"""

import json
import logging
import random
import time
from concurrent.futures import wait, FIRST_COMPLETED

from pyformance import time_calls, meter_calls

import requests
from absl import app

from nbdb.common.entry_point import EntryPoint
from nbdb.common.simple_throttle import SimpleThrottle
from nbdb.common.telemetry import Telemetry
from nbdb.common.thread_pools import ThreadPools
from nbdb.config.settings import Settings
from nbdb.load_simulator.metric_reader import MetricReader
from nbdb.load_simulator.test_dashboards import TestDashboards

logger = logging.getLogger()


class TestMetricReader:
    """
    This is a test class for reading metrics from Flask
    """
    def __init__(self, reader_settings):
        """
        Initialize the Test metric reader
        :param reader_settings: Test reader settings
        """
        self.reader_settings = reader_settings
        self.metric_query_pool = ThreadPools.inst.test_reader_pool
        self.simple_throttle = SimpleThrottle(
            20, reader_settings.sleep_time,
            reader_settings.reset_time)
        self.benchmark_type = self.reader_settings.benchmark_type

    def check_query(self, query: str):
        """Check that query is successful and output is not empty."""
        params = {'q': query}
        url = "https://{}/query".format(self.reader_settings.flask_addr)
        req = requests.get(url, params=params)
        if not req.ok:
            req.raise_for_status()

        return json.loads(req.text)

    @time_calls
    @meter_calls
    def run_query(self, node_num: int, proc_num: int, start_epoch: int,
                  end_epoch: int):
        """Create an InfluxQL query based on parameters and run it"""
        settings = self.reader_settings
        base_query = ("SELECT {func}(\"{measurement}.{field_name}\") FROM m "
                      "WHERE time >= {start_epoch} AND time <= {end_epoch}")
        if self.benchmark_type == "simple_queries":
            # Run simple queries which require no aggregation
            query = base_query.format(func="mean",
                                      measurement=settings.measurement,
                                      field_name=settings.field_name,
                                      start_epoch=start_epoch,
                                      end_epoch=end_epoch)
            # Add WHERE clauses for node & process
            query += (" AND {node_tk}='{node_val}' AND {proc_tk}='{proc_val}'".
                      format(node_tk=settings.node_tag_key,
                             node_val=node_num,
                             proc_tk=settings.process_tag_key,
                             proc_val=proc_num))

        elif self.benchmark_type in ["agg_queries", "agg_downsampled_queries"]:
            # Run queries which require aggregation
            query = base_query.format(func="mean",
                                      measurement=settings.measurement,
                                      field_name=settings.field_name,
                                      start_epoch=start_epoch,
                                      end_epoch=end_epoch)

            if self.benchmark_type == "agg_downsampled_queries":
                # Add a group by since downsampling was required
                assert settings.downsampling_interval_mins
                query += " GROUP BY time({}m)".format(
                    settings.downsampling_interval_mins)
        else:
            assert False, "Unsupported benchmark: %s" % self.benchmark_type

        return self.check_query(query)

    def throttle(self, output: dict):
        """Throttle read queries based on benchmark type."""
        if self.benchmark_type == "simple_queries":
            # For simple queries, we throttle based on the number of queries
            # being run
            self.simple_throttle.throttle(1)
        else:
            # For other benchmarks, we throttle based on the number of
            # datapoints
            datapoints = output["results"][0]["series"][0]["values"][1]
            if self.benchmark_type == "agg_queries":
                # No downsampling. We throttle based on the number of
                # datapoints
                self.simple_throttle.throttle(len(datapoints))

            elif self.benchmark_type == "agg_downsampled_queries":
                # For downsampled queries, we try to derive the number of
                # unaggregated datapoints that would have existed
                num_datapoints = (len(datapoints) *
                                  self.reader_settings.downsampling_interval_mins)
                self.simple_throttle.throttle(num_datapoints)

    def start_reading(self):
        """
        Starts a single threaded query reader
        :return:
        """
        logger.info('Reading %d - %d epochs for %d nodes and %d processes:',
                    self.reader_settings.start_epoch,
                    self.reader_settings.end_epoch,
                    self.reader_settings.num_nodes,
                    self.reader_settings.num_processes)

        start_epoch = self.reader_settings.start_epoch
        end_epoch = self.reader_settings.end_epoch

        pending_futures = set()
        last_rate_reset = time.time()
        while True:
            node_num = random.randint(0, self.reader_settings.num_nodes)
            proc_num = random.randint(0, self.reader_settings.num_processes)

            future = self.metric_query_pool.submit(
                self.run_query, node_num, proc_num, start_epoch, end_epoch)
            pending_futures.add(future)

            if (len(pending_futures) >=
                    self.reader_settings.max_pending_futures):
                done_futures, pending_futures = wait(
                    pending_futures, return_when=FIRST_COMPLETED)

                for done_future in done_futures:
                    if done_future.exception() is None:
                        # Query passed. Throttle based on the benchmark type
                        logger.debug("Query passed: %s", done_future.result())
                        self.throttle(done_future.result())

                        Telemetry.inst.registry.meter(
                            'TestMetricReader.successful_queries').mark()
                    else:
                        # Query failed
                        logger.info("Query failed: %s", done_future.exception())
                        Telemetry.inst.registry.meter(
                            'TestMetricReader.failed_queries').mark()

            logger.info("Finished submitting query for node %s & process %s",
                        node_num, proc_num)

            # every 10 seconds increase the rate
            if time.time() - last_rate_reset > 30:
                self.simple_throttle.rate_limit = \
                    self.simple_throttle.rate_limit + 5
                self.simple_throttle.rate_limit = \
                    min(self.simple_throttle.rate_limit,
                        self.reader_settings.rate_limit)
                logger.info('New rate limit: %d',
                            self.simple_throttle.rate_limit)
                last_rate_reset = time.time()
                Telemetry.inst.registry.gauge('TestMetricReader.rate_limit').\
                    set_value(self.simple_throttle.rate_limit)


class MetricReaderApp(EntryPoint):
    """
    Entry point class for the metric reader
    """

    def start(self):
        """
        Initialize the write path and start the consumers
        :return:
        """
        logger.info("Creating the TestMetricReader")
        if hasattr(Settings.inst, 'test_dashboards'):
            metric_reader = MetricReader(Settings.inst.test_metric_reader)
            test_dashboards = TestDashboards(self.context,
                                             Settings.inst.test_dashboards,
                                             metric_reader)
            test_dashboards.run()
        else:
            test_metric_reader = TestMetricReader(
                Settings.inst.test_metric_reader)

            logger.info("Starting to read metrics")
            test_metric_reader.start_reading()
            logger.info('Done reading all data. Sleeping indefinitely now')
            time.sleep(86400*1000)


if __name__ == "__main__":
    app.run(MetricReaderApp().run)
