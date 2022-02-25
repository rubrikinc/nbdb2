"""
TestMetricProducer
"""

import logging
import time
from concurrent.futures import wait

from pyformance import time_calls, meter_calls

from nbdb.common.context import Context
from nbdb.common.telemetry import Telemetry
from nbdb.config.settings import Settings
from nbdb.common.thread_pools import ThreadPools
from nbdb.load_simulator.metric_reader import MetricReader

logger = logging.getLogger()


class TestDashboards:
    """
    This is a test class for reading metrics from Flask
    """
    def __init__(self,
                 context: Context,
                 dashboard_setting: Settings,
                 metric_reader: MetricReader):
        """
        Initialize the Test metric reader
        :param dashboard_setting: Test reader settings
        """
        self.context = context
        self.metric_reader = metric_reader
        self.dashboard_setting = dashboard_setting
        self.metric_query_pool = ThreadPools.inst.test_reader_pool
        self._query_templates = list()
        self.gen_templates()

    def gen_templates(self):
        """
        Generate template queries to simulate the dashboard
        """
        logger.info('gen_templates: using schema datasources: %s',
                    ','.join([d for d, _, _ in
                                self.context.schema.datasources()]))
        for datasource, _, _ in self.context.schema.datasources():
            if not datasource.startswith(
                    self.dashboard_setting.measurement_prefix):
                continue
            logger.info('gen_templates: datasource=%s', datasource)
            for field in range(0, self.dashboard_setting.fields):
                logger.info('gen_templates: field=%s', str(field))
                field_name = '{}{}'.format(
                    self.dashboard_setting.field_prefix, field)
                base_query = "SELECT mean(" \
                             "\"" + datasource + "." + field_name + \
                             "\")" \
                             " FROM tsdb " \
                             " WHERE " \
                             " time >= {start_epoch} AND" \
                             " time < {end_epoch} AND" \
                             " cluster='" +\
                             self.dashboard_setting.cluster_prefix +\
                             "{cluster}' " \
                             " group by time('{interval}')"
                self._query_templates.append(base_query)
        logger.info('Generated query_templates: %s',
                    '\n'.join(self._query_templates))

    @meter_calls
    @time_calls
    def load(self, cluster, start_epoch, end_epoch, interval):
        """
        Load the dashboard for the specific cluster
        :param cluster:
        :param start_epoch:
        :param end_epoch:
        :param interval:
        :return:
        """
        queries = [template.format(start_epoch=start_epoch,
                                   end_epoch=end_epoch,
                                   cluster=cluster,
                                   interval=interval)
                   for template in self._query_templates]
        logger.info('\tdashboard-refresh: %s [%d-%d) queries: %d',
                    cluster, start_epoch, end_epoch, len(queries))
        futures = list()
        for query in queries:
            futures.append(self.metric_query_pool.submit(
                self.metric_reader.run_query, query))
        # block to load the full dashboard
        wait(futures)
        logger.info('\tdashboard-loaded: %s [%d-%d) futures: %d',
                    cluster, start_epoch, end_epoch, len(futures))

    def run(self):
        """
        We gradually increase the number of dashboards
        Each dashboard once created (does a full load) and then refreshes
        by moving the load window by refresh points
        Each dashboard is tied to a new cluster so it will not hit the cache
        on flask app
        :return:
        """
        end_epoch = int(time.time()) - self.dashboard_setting.required_lag
        start_epoch = end_epoch - self.dashboard_setting.last_hours*3600
        points_per_query = self.dashboard_setting.points_per_query
        interval = int((end_epoch - start_epoch) / points_per_query)
        refresh_points = self.dashboard_setting.refresh_points

        cluster_start = self.dashboard_setting.cluster_start
        cluster_max = self.dashboard_setting.cluster_max
        cluster_inc_step = self.dashboard_setting.cluster_inc_step
        cluster_inc_after_iterations = self.dashboard_setting.cluster_inc_after_iterations
        cluster_count = cluster_start
        iteration = 0

        while True:
            logger.info('LOADING: Dashboards=%d [%d - %d, I=%d] - '
                        'Next WIN [%d - %d] RefreshInterval=%d',
                        cluster_count,
                        start_epoch, end_epoch, interval,
                        start_epoch + refresh_points*interval,
                        end_epoch + refresh_points*interval,
                        refresh_points*interval)
            Telemetry.inst.registry.gauge('TestDashboards.clusters'). \
                set_value(cluster_count)
            futures = list()

            # stagger the load over refresh period/4
            num_dash = self.dashboard_setting.dashboards_per_cluster * \
                       cluster_count
            dashboard_load_gap_ms = refresh_points*interval*1000/(num_dash*4)

            schedule_time = time.time()
            for cluster in range(0, cluster_count):
                for _ in range(0,
                               self.dashboard_setting.dashboards_per_cluster):
                    futures.append(self.metric_query_pool.submit(
                        self.load, cluster, start_epoch, end_epoch, interval))
                time.sleep(dashboard_load_gap_ms/1000)

            schedule_time = time.time()-schedule_time

            logger.info('%d Dashboards scheduled to load in %d s',
                        cluster_count,
                        schedule_time)
            load_time = time.time()
            wait(futures)
            load_time = time.time() - load_time
            logger.info('%d Dashboards finished loading in %d s',
                        cluster_count,
                        load_time)

            start_epoch = start_epoch + refresh_points*interval
            end_epoch = end_epoch + refresh_points*interval

            iteration = iteration + 1
            if iteration > cluster_inc_after_iterations:
                cluster_count = cluster_count + cluster_inc_step
                cluster_count = min(cluster_count, cluster_max)

            # make sure we don't get ahead of current time
            lag = time.time() - end_epoch
            if lag < self.dashboard_setting.required_lag:
                time.sleep(self.dashboard_setting.required_lag - lag)
