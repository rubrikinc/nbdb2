"""
DruidAdmin module
"""
from __future__ import annotations

import json
import logging
import os
import re
import urllib.parse
from datetime import datetime
from typing import List, Optional, Tuple
from enum import Enum

import requests
from dateutil.relativedelta import relativedelta
from nbdb.config.settings import Settings, DruidSettings
from pyformance import time_calls, meter_calls

# The granularity strings we support in Druid admin. The actual list of
# supported granularities can be much bigger. See the following link:
# https://druid.apache.org/docs/latest/querying/granularities.html
SUPPORTED_GRANULARY_STRINGS = ["NONE", "HOUR", "DAY", "WEEK", "MONTH"]
GRANULARITY_STRING_TO_SECS = {
    "NONE": None,
    "HOUR": 60*60,
    "DAY": 24*60*60,
    "WEEK": 7*24*60*60,
    "MONTH": 31*24*60*60,
}

# We can define reingest tasks to operate on either a WEEK of data or a MONTH
# of data at a time.
REINGEST_PERIODS_SUPPORTED = ["WEEK", "MONTH"]

LATE_MESSAGE_DROP_PERIOD = "P30D"
EARLY_MESSAGE_DROP_PERIOD = "P2D"

logger = logging.getLogger()


class DruidTaskStatus(Enum):
    """Enum for druid task status"""
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    NOT_FOUND = "NOT_FOUND"

    def is_terminal(self) -> bool:
        """
        :return: True if the status represents a terminal status
        """
        return self != DruidTaskStatus.RUNNING

class DruidTaskStateFilter(Enum):
    """Enum for available task state filters"""
    RUNNING = "running"
    COMPLETE = "complete"
    WAITING = "waiting"
    PENDING = "pending"

class DruidTaskTypeFilter(Enum):
    """Enum for available task type filters"""
    NATIVE_INGEST_PARALLEL = "index_parallel"

class DruidAdmin:
    """
    Simple wrapper that provides high level management of druid
    schema based on the co-ordinator and overlord api

    Goal of this wrapper is to mask the druid api complexity and provide
    a simpler clean abstraction for managing the schema
    """

    def __init__(self, connection_string: Settings):
        """
        initialize the connection
        :param connection_string:
        """
        self.connection_string: Settings = connection_string
        self.router_url: str = 'http://{}:{}'.format(
            self.connection_string.router_ip,
            self.connection_string.router_port)
        self.overlord_url: str = 'http://{}:{}'.format(
            self.connection_string.overlord_ip,
            self.connection_string.overlord_port)

    def fetch_supervisors(self) -> List[str]:
        """
        Queries the overlord for a list of active supervisor specs
        :return: list of data sources
        """
        resp = requests.get('{}/druid/indexer/v1/supervisor'.format(
            self.router_url))
        if resp.status_code != 200:
            raise ValueError('Failed to fetch data_sources:'
                             ' status={} resp={}'.format(resp.status_code,
                                                         resp.json()))
        return resp.json()

    def hard_reset_supervisors(self,
                               data_source_pattern: str,
                               test_only: bool) -> None:
        """
        hard reset supervisor that match the given pattern
        These are async tasks so the method will return immediately
        Verify the result on the Druid Unified console
        :param data_source_pattern:
        :param test_only: If true just logs the actions, but doesnt actually
                          drop the segments. Use it to verify the regex first
        """
        data_source_matcher = re.compile(data_source_pattern)
        datasources = self.fetch_supervisors()
        for datasource in datasources:
            if data_source_matcher.match(datasource):
                if test_only:
                    logger.info('DruidAdmin.hard_reset_supervisor: %s '
                                'test only', datasource)
                else:
                    self._hard_reset_supervisor(datasource)

    def delete_supervisor(self, datasource: str) -> None:
        """
        Delete the supervisor
        :param datasource: Name of datasource
        """
        # first do a hard reset of the supervisor
        # this is a precaution in case we are trying to recreate the supervisor
        self._hard_reset_supervisor(datasource)
        resp = requests.post(
            '{}/druid/indexer/v1/supervisor/{}/terminate'.format(
                self.overlord_url, datasource))
        if resp.status_code != 200:
            # check if its because the spec doesn't exist
            if 'Cannot find any supervisor' not in resp.text:
                raise ValueError('Failed to terminate supervisor:'
                                 ' {} status={} resp={}'.
                                 format(datasource,
                                        resp.status_code,
                                        resp.text))
        logger.info('DruidAdmin.delete_supervisor: '
                    'data_source=%s status=%d resp=%s',
                    datasource, resp.status_code, resp.text)

    def delete_supervisors(self, data_source_pattern: str, test_only: bool) \
            -> None:
        """
        Delete datasources that match the given pattern
        These are async tasks so the method will return immediately
        Verify the result on the Druid Unified console
        :param data_source_pattern:
        :param test_only: If true just logs the actions, but doesnt actually
                          drop the segments. Use it to verify the regex first
        """
        data_source_matcher = re.compile(data_source_pattern)
        datasources = self.fetch_supervisors()
        for datasource in datasources:
            if data_source_matcher.match(datasource):
                if test_only:
                    logger.info('DruidAdmin.delete_supervisor: %s test only',
                                datasource)
                else:
                    self.delete_supervisor(datasource)

    # pylint: disable=R0913  # Too Many Arguments
    def update_data_sources(self, data_source_pattern: str,
                            kafka_brokers: str,
                            datasource_settings: Settings,
                            query_granularity,
                            segment_granularity,
                            test_only: bool) -> None:
        """
        Updates Kafka ingest spec for datasources matching the given pattern.

        This method only applies to realtime datasources which read from Kafka.
        Batch datasources do not read from Kafka and will remain unaffected.

        These are async tasks so the method will return immediately
        Verify the result on the Druid Unified console
        :param data_source_pattern:
        :param test_only: If true just logs the actions, but doesnt actually
                          update the datasource. Use it to verify regex
        """
        data_source_matcher = re.compile(data_source_pattern)
        datasources = self.fetch_supervisors()
        for datasource in datasources:
            if data_source_matcher.match(datasource):
                if test_only:
                    logger.info('DruidAdmin.update_data_source: %s test only',
                                datasource)
                else:
                    self.create_data_source(kafka_brokers, datasource,
                                            datasource_settings,
                                            query_granularity,
                                            segment_granularity)

    def update_crosscluster_mm_worker_strategy(self, data_source_pattern: str,
                                               test_only: bool) -> None:
        """
        Updates crosscluster MM worker selection strategy

        This method only applies to realtime datasources which read from Kafka.
        Batch datasources do not read from Kafka and will remain unaffected.

        These are async tasks so the method will return immediately
        Verify the result on the Druid Unified console
        :param data_source_pattern:
        :param test_only: If true just logs the actions, but doesnt actually
                          update the datasource. Use it to verify regex
        """
        category_affinity = {}
        data_source_matcher = re.compile(data_source_pattern)
        datasources = self.fetch_supervisors()
        for datasource in datasources:
            if data_source_matcher.match(datasource):
                category_affinity[datasource] = "crosscluster"

        if not category_affinity:
            category_affinity = None
        select_spec = {
            "selectStrategy": {
                "type": "equalDistributionWithCategorySpec",
                "workerCategorySpec": {
                    "strong": True,
                    "categoryMap": {
                        "index_kafka": {
                            "defaultCategory": "regular",
                            "categoryAffinity": category_affinity
                        }
                    }
                }
            }
        }
        if test_only:
            logger.info('DruidAdmin.update_mm_crosscluster_worker_strategy: '
                        'test only')
            logger.info('Select_spec: %s', json.dumps(select_spec))
            return

        # Post the spec to the supervisor
        resp = requests.post(
            '{}/druid/indexer/v1/worker'.format(self.overlord_url),
            json=select_spec
        )
        if resp.status_code != 200:
            raise ValueError('Failed to update worker strategy '
                             ' status={} resp={}'.format(resp.status_code,
                                                         resp.json()))
        logger.info('DruidAdmin.update_mm_crosscluster_worker_strategy: '
                    'status=%d', resp.status_code)

    # pylint: disable=R0913  # Too Many Arguments
    def reingest_data(self, data_source_pattern: str,
                      datasource_settings: Settings,
                      query_granularity: str,
                      segment_granularity: str,
                      start_date: str,
                      reingest_period: str,
                      num_reingest_periods: int,
                      test_only: bool) -> None:
        """
        Create reingest tasks for datasources that match the given pattern.

        These are async tasks so the method will return immediately
        Verify the result on the Druid Unified console
        :param data_source_pattern:
        :param test_only: If true just logs the actions, but doesnt actually
                          update the datasource. Use it to verify regex
        """
        start_date = datetime.strptime(start_date, "%Y-%m-%d")
        data_source_matcher = re.compile(data_source_pattern)
        datasources = self.fetch_data_sources()
        for datasource in datasources:
            if data_source_matcher.match(datasource):
                if test_only:
                    logger.info('DruidAdmin.update_data_source: %s test only',
                                datasource)
                else:
                    # We create one task for each week / month of data. Asking
                    # one task to handle more than 1 month of data causes it to
                    # run too slowly and restricts us from parallelizing to
                    # improve speed.
                    for period in range(num_reingest_periods):
                        if reingest_period == "MONTH":
                            interval_start_date = (start_date +
                                                   relativedelta(months=period))
                            # Use interval of exactly one month
                            interval_end_date = (interval_start_date +
                                                 relativedelta(months=1))
                        elif reingest_period == "WEEK":
                            interval_start_date = (start_date +
                                                   relativedelta(weeks=period))
                            # Use interval of exactly one week
                            interval_end_date = (interval_start_date +
                                                 relativedelta(weeks=1))
                        else:
                            assert False, "Unsupported period"

                        self.create_reingest_task(datasource,
                                                  datasource_settings,
                                                  query_granularity,
                                                  segment_granularity,
                                                  interval_start_date,
                                                  interval_end_date)

    def delete_data_source(self, datasource: str) -> None:
        """
        Deletes the data source permanently deleting data
        :param datasource:
        :return:
        """
        resp = requests.delete(
            '{}/druid/coordinator/v1/datasources/{}'.format(
                self.overlord_url, datasource))
        if resp.status_code != 200:
            # check if its because the spec doesn't exist
            raise ValueError('Failed to terminate data_source:'
                             ' {} status={} resp={}'.
                             format(datasource,
                                    resp.status_code,
                                    resp.text))
        resp = requests.delete(
            '{}/druid/coordinator/v1/datasources/{}?kill=true&interval=1000/3000'.format(
                self.overlord_url, datasource))
        if resp.status_code != 200:
            # check if its because the spec doesn't exist
            raise ValueError('Failed to terminate data_source:'
                             ' {} status={} resp={}'.
                             format(datasource,
                                    resp.status_code,
                                    resp.text))

        logger.info('DruidAdmin.delete_data_source: '
                    'data_source=%s status=%d resp=%s',
                    datasource, resp.status_code, resp.text)

    def delete_data_sources(self, data_source_pattern: str, test_only: bool) \
            -> None:
        """
        Delete datasources that match the given pattern
        These are async tasks so the method will return immediately
        Verify the result on the Druid Unified console
        :param data_source_pattern:
        :param test_only: If true just logs the actions, but doesnt actually
                          drop the segments. Use it to verify the regex first
        """
        data_source_matcher = re.compile(data_source_pattern)
        datasources = self.fetch_data_sources()
        for datasource in datasources:
            if data_source_matcher.match(datasource):
                if test_only:
                    logger.info('DruidAdmin.delete_data_sources: %s test only',
                                datasource)
                else:
                    self.delete_data_source(datasource)

    def fetch_data_sources(self) -> List[str]:
        """
        Queries the overlord for a list of active supervisor specs
        :return: list of data sources
        """
        resp = requests.get('{}/druid/coordinator/v1/datasources'.format(
            self.router_url))
        if resp.status_code != 200:
            raise ValueError('Failed to fetch data_sources:'
                             ' status={} resp={}'.format(resp.status_code,
                                                         resp.json()))
        return resp.json()

    def create_reingest_task(self, # pylint: disable=too-many-arguments
                             datasource: str,
                             datasource_settings: Settings,
                             query_granularity: str,
                             segment_granularity: str,
                             start_date: datetime,
                             end_date: datetime) -> None:
        """
        Create a task to reingest Druid data
        :param datasource: Name of datasource to be created
        :param datasource_settings: datasource ingestion spec configuration
        :param query_granularity:
        :param segment_granularity:
        """
        # Default query & segment granularities are 10m and 1d respectively
        if query_granularity is None:
            query_granularity = 600
        if segment_granularity is None:
            segment_granularity = 86400

        start_date = start_date.strftime("%Y-%m-%d")
        end_date = end_date.strftime("%Y-%m-%d")
        logger.info('DruidAdmin.reingest_data: '
                    'data_source=%s interval=[%s,%s], '
                    'query_granularity=%s, segment_granularity=%s',
                    datasource, start_date, end_date, query_granularity,
                    segment_granularity)

        spec = {
            "type": "index_parallel",
            "ioConfig": {
                "type": "index_parallel",
                "inputSource": {
                    "type": "druid",
                    "dataSource": datasource,
                    "interval": "%sT00:00:00/%sT00:00:00" % (start_date, end_date)
                },
                # We want to make sure that the old segments get deleted after
                # reindexing.
                "appendToExisting": False
            },
            "tuningConfig": {
                "type": "index_parallel",
                "partitionsSpec": {
                    "type": "dynamic",
                    "maxRowsPerSegment": datasource_settings.maxRowsPerSegment
                },
                # We use maxRowsPerSegment=600K for realtime ingest tasks but
                # we will use a higher value here to speed up reingest.
                "maxRowsInMemory": 1000000
            },
            "dataSchema": DruidAdmin._create_dataschema_dict(
                datasource,
                query_granularity=query_granularity,
                segment_granularity=segment_granularity,
                intervals=["%s/%s" % (start_date, end_date)])
        }
        task_spec = {
            "type": "index_parallel",
            "spec": spec
        }
        # Post the spec to the supervisor
        resp = requests.post(
            '{}/druid/indexer/v1/task'.format(self.overlord_url),
            json=task_spec
        )
        if resp.status_code != 200:
            raise ValueError('Failed to reingest data:'
                             ' {} status={} resp={}'.format(datasource,
                                                            resp.status_code,
                                                            resp.json()))
        logger.info('DruidAdmin.reingest_data: '
                    'data_source=%s interval=[%s,%s], status=%d resp=%s',
                    datasource, start_date, end_date, resp.status_code,
                    resp.json())

    @time_calls
    @meter_calls
    def create_data_source(self, # pylint: disable=too-many-arguments
                           kafka_brokers: str,
                           datasource: str,
                           datasource_settings: Settings,
                           query_granularity=None,
                           segment_granularity=None) -> None:
        """
        Create a data source to ingest from a Kafka topic
        :param kafka_brokers: csv string of ip:port
        :param datasource: Name of datasource to be created
        :param datasource_settings: datasource ingestion spec configuration
        :param query_granularity:
        :param segment_granularity:
        """
        # Default query & segment granularities are 10m and 1d respectively
        if query_granularity is None:
            query_granularity = 600
        if segment_granularity is None:
            segment_granularity = 86400

        schema_settings = Settings.inst.schema_creator
        topic_name = "%s_%s" % (schema_settings.druid_topic_prefix, datasource)
        topic_spec = {
            "type": "kafka",
            "ioConfig": {
                "type": "kafka",
                "consumerProperties": {
                    "bootstrap.servers": kafka_brokers,
                },
                "topic": topic_name,
                "inputFormat": {
                    "type": "json"
                },
                "useEarliestOffset": True,
                # Drop metrics older than a given period
                "lateMessageRejectionPeriod":
                datasource_settings.lateMessageRejectionPeriod,
                # Drop metrics which are too early
                "earlyMessageRejectionPeriod":
                datasource_settings.earlyMessageRejectionPeriod,
            },
            "tuningConfig": {
                "type": "kafka",
                "maxRowsInMemory": datasource_settings.maxRowsInMemory,
                "maxRowsPerSegment": datasource_settings.maxRowsPerSegment,
                "resetOffsetAutomatically": True
            },
            "dataSchema": DruidAdmin._create_dataschema_dict(
                datasource,
                query_granularity=query_granularity,
                segment_granularity=segment_granularity)
        }
        # Post the spec to the supervisor
        resp = requests.post(
            '{}/druid/indexer/v1/supervisor'.format(self.overlord_url),
            json=topic_spec
        )
        if resp.status_code != 200:
            raise ValueError('Failed to create data_source:'
                             ' {} status={} resp={}'.format(topic_name,
                                                            resp.status_code,
                                                            resp.json()))
        logger.info('DruidAdmin.create_data_source: '
                    'data_source=%s status=%d resp=%s',
                    datasource, resp.status_code, resp.json())

        # Create the compaction spec
        # skip_offset_from_latest = DruidAdmin._seconds_to_granularity_string(
        #    segment_granularity)
        # self.create_compaction_spec(datasource,
        #                             datasource_settings.maxRowsPerSegment,
        #                             skip_offset_from_latest
        #                             )

    # pylint: disable=R0913  # Too Many Arguments
    def manage_retention_rules(self,
                               data_source_pattern: str,
                               retention_interval: str,
                               num_replicas: int,
                               delete: bool, test_only: bool) -> None:
        """
        Create data retention rules for the datasources matching the pattern.

        We achieve this using 1 load rule and 1 drop rule. The load rule
        will ensure the necessary number of replicas exist. The drop rule
        will ensure data is retained only for the requested duration.

        These are async tasks so the method will return immediately
        Verify the result on the Druid Unified console

        :param data_source_pattern:
        :param retention_interval:  ISO-8601 Intervals (P1M, P1W, P1D etc)
        :param delete : If true it deletes the rule, otherwise creates it
        :param test_only: If true just logs the actions, but doesnt actually
                          drop the segments. Use it to verify the regex first
        """
        data_source_matcher = re.compile(data_source_pattern)
        datasources = self.fetch_data_sources()
        for datasource in datasources:
            if data_source_matcher.match(datasource):
                if test_only:
                    logger.info('retention_rule : %s (TestOnly)', datasource)
                else:
                    self._manage_retention_rule(datasource, retention_interval,
                                                num_replicas, delete)

    def compact(self,
                data_source_pattern: str,
                max_rows_per_segment: int,
                skip_offset_from_latest: str,
                test_only: bool) -> None:
        """
        Create the compaction spec for the datasources that match the pattern
        These are async tasks so the method will return immediately
        Verify the result on the Druid Unified console
        :param data_source_pattern:
        :param max_rows_per_segment:
        :param skip_offset_from_latest:
        :param test_only: If true just logs the actions, but doesnt actually
                          drop the segments. Use it to verify the regex first
        """
        data_source_matcher = re.compile(data_source_pattern)
        datasources = self.fetch_data_sources()
        for datasource in datasources:
            if data_source_matcher.match(datasource):
                if test_only:
                    logger.info('compacting : %s (TestOnly)', datasource)
                else:
                    self._create_compaction_spec(datasource,
                                                 max_rows_per_segment,
                                                 skip_offset_from_latest)

    # pylint: disable=too-many-arguments
    def drop_matching_segments(self,
                               data_source_pattern: str,
                               start_date: datetime,
                               end_date: datetime,
                               filter_by_interval: str,
                               test_only: bool) -> None:
        """
        Drop segments matching given criteria for all datasources
        that match the pattern.

        These are async tasks so the method will return immediately
        Verify the result on the Druid Unified console
        :param data_source_pattern:
        :param start_date:
        :param end_date:
        :param drop: If true then drop the segments otherwise load them
        :param test_only: If true just logs the actions, but doesnt actually
                          drop the segments. Use it to verify the regex first
        """
        data_source_matcher = re.compile(data_source_pattern)
        datasources = self.fetch_data_sources()
        for datasource in datasources:
            if data_source_matcher.match(datasource):
                self._drop_matching_segments(datasource, start_date, end_date,
                                             filter_by_interval, test_only)

    # pylint: disable=too-many-arguments
    # pylint: disable=too-many-locals
    def _drop_matching_segments(self,
                                data_source: str,
                                start_date: datetime,
                                end_date: datetime,
                                filter_by_interval: str,
                                test_only: bool) -> None:
        """
        Drop segments from the datasource that are within given interval
        :param data_source:
        :param start_date: The start date after which segments are removed.
                           time component is ignored
        :param end_date: The end date before which segments are removed.
                         time component is ignored
        :param filter_by_interval:
        :param test_only: If true just logs the actions, but doesnt actually
                          drop the segments. Use it to verify the regex first
        """
        # We query for segments in the requested range with the desired
        # interval range
        interval_start_date = start_date
        if filter_by_interval == "HOUR":
            filter_delta = relativedelta(hours=1)
        elif filter_by_interval == "DAY":
            filter_delta = relativedelta(days=1)
        elif filter_by_interval == "WEEK":
            filter_delta = relativedelta(weeks=1)
        else:
            filter_delta = relativedelta(months=1)
        interval_end_date = interval_start_date + filter_delta

        segment_date_pattern = (
            r"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}.\d+Z)_"
            r"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}.\d+Z)_"
            r"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}.\d+Z)")

        while interval_end_date <= end_date:
            # Query for segments within the current search interval
            url = '{}/druid/coordinator/v1/datasources/{}/intervals/{}_{}'
            resp = requests.get(url.format(
                self.overlord_url, data_source,
                interval_start_date.strftime('%Y-%m-%dT00:00:00'),
                interval_end_date.strftime('%Y-%m-%dT00:00:00')))

            if resp.status_code not in [200, 204]:
                raise ValueError('Failed to find segments for data_source:'
                                 ' {} status={} resp={}'.
                                 format(data_source,
                                        resp.status_code,
                                        resp.text()))

            all_segment_ids = resp.json()
            logger.info('DruidAdmin._drop_matching_segments:'
                        ' [%s, %s] %d segments found',
                        interval_start_date, interval_end_date,
                        len(all_segment_ids))

            # We can get segments of different granularities within this
            # current interval. Filter and find segments with the specified
            # filter interval granularity
            matching_segment_ids = []
            for segment_id in all_segment_ids:
                # The dates with Segment ID look like the following:
                # "2020-07-01T00:00:00.000Z_2020-08-01T00:00:00.000Z_"
                # "2020-07-01T03:48:52.121Z
                #
                # The first and second dates are the segment start and end
                # dates. The last date represents the segment version
                segment_start_date, segment_end_date = \
                    [datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%S.%fZ")
                     for date_str in
                     re.search(segment_date_pattern, segment_id).groups()[:2]]

                if segment_start_date + filter_delta == segment_end_date:
                    # This segment has the desired segment granularity
                    matching_segment_ids += [segment_id]

            logger.info('DruidAdmin._drop_matching_segments:'
                        ' [%s, %s] %d matching segments found',
                        interval_start_date, interval_end_date,
                        len(matching_segment_ids))

            if test_only:
                logger.info('DruidAdmin._drop_matching_segments: '
                            '[%s, %s] testOnly %d segments would be deleted',
                            interval_start_date, interval_end_date,
                            len(matching_segment_ids))
            else:
                # Now delete the matching segments
                if len(matching_segment_ids) > 0:
                    url = '{}/druid/coordinator/v1/datasources/{}/markUnused'
                    resp = requests.post(
                        url.format(self.overlord_url, data_source),
                        json={"segmentIds": matching_segment_ids}
                    )

                if resp.status_code not in [200, 204]:
                    raise ValueError('Failed to drop segments for data_source:'
                                     ' {} status={} resp={}'.
                                     format(data_source,
                                            resp.status_code,
                                            resp.text()))

                logger.info('DruidAdmin._drop_matching_segments: '
                            'data_source=%s interval=[%s,%s] status=%d '
                            'deleted=%d',
                            data_source, interval_start_date,
                            interval_end_date, resp.status_code,
                            len(matching_segment_ids))

            # Increment interval search range
            interval_start_date = interval_end_date
            interval_end_date = interval_start_date + filter_delta


    def manage_all_segments(self, # pylint: disable=too-many-arguments
                            data_source_pattern: str,
                            start_date: datetime,
                            end_date: datetime,
                            drop: bool,
                            test_only: bool) -> None:
        """
        Drop/load ALL segments within the defined range for all data sources
        that match the pattern.

        These are async tasks so the method will return immediately
        Verify the result on the Druid Unified console
        :param data_source_pattern:
        :param start_date:
        :param end_date:
        :param drop: If true then drop the segments otherwise load them
        :param test_only: If true just logs the actions, but doesnt actually
                          drop the segments. Use it to verify the regex first
        """
        data_source_matcher = re.compile(data_source_pattern)
        datasources = self.fetch_data_sources()
        for datasource in datasources:
            if data_source_matcher.match(datasource):
                self._manage_data_source_segments(datasource,
                                                  start_date,
                                                  end_date,
                                                  drop,
                                                  test_only)


    # TODO(abhay): Check for datasource existence before triggering task?
    def create_batch_ingest_task(self, # pylint: disable=too-many-arguments
                                 task_id: str,
                                 druid_settings: DruidSettings,
                                 query_granularity: int,
                                 segment_granularity: int,
                                 datasource: str,
                                 location_type: str,
                                 paths: List[str]) -> None:
        """
        Create a batch ingest task.

        :param druid_settings: druid settings
        :param datasource: Name of the target datasource where the data will
        be ingested.
        :param location_type: Type of location for the data to be ingested.
        Supported values: ["local", "s3"]
        :param path: List of paths which will be ingested. Each path must point
        to a .json or a .json.gz file.

        """
        task_spec = \
        {
            "id": task_id,
            "type": "index_parallel",
            "spec": {
                "dataSchema": DruidAdmin._create_dataschema_dict(
                    datasource,
                    query_granularity=query_granularity,
                    segment_granularity=segment_granularity),
                "ioConfig": {
                    "type": "index_parallel",
                    # If False, batch ingest will overwrite existing
                    # overlapping segements. We don't want that, since our
                    # segments can contain data for multiple clusters
                    "appendToExisting": True,
                    "inputSource": \
                        DruidAdmin._validate_and_create_batch_input_source(
                            location_type,
                            paths
                        ),
                    "inputFormat": {
                        "type": "json"
                    }
                },
                "tuningConfig": {
                    "type": "index_parallel",
                    "maxNumConcurrentSubTasks": \
                        druid_settings.batch_ingest.maxNumConcurrentSubTasks,
                    "maxRowsInMemory": \
                        druid_settings.datasource.maxRowsInMemory,
                    "maxRowsPerSegment": \
                        druid_settings.datasource.maxRowsPerSegment,
                }
            }
        }
        resp = requests.post(
            f"{self.overlord_url}/druid/indexer/v1/task",
            json=task_spec
        )
        debug_str = (f"task_spec={task_spec},"
                     f"status={resp.status_code},"
                     f"resp={resp.json()}")
        if resp.status_code != 200:
            raise ValueError("Failed to create batch ingest task: "
                             f"{debug_str}")
        logger.info("DruidAdmin.create_batch_ingest_task: %s", debug_str)
        assert resp.json()["task"] == task_id

    def get_task_status(self, task_id: str) -> Optional[DruidTaskStatus]:
        """
        Get current status of a task.

        :param task_id: Id of the task
        :return: Progress dict. None if task is not found.
        """
        # Task ID has special characters, so escape them
        quoted_id = urllib.parse.quote(task_id)
        resp = requests.get(
            f"{self.overlord_url}/druid/indexer/v1/task/{quoted_id}/status"
        )
        debug_str = (f"task_id={task_id}, "
                     f"status={resp.status_code}, resp={resp.json()}")
        logger.info("DruidAdmin.get_task_status: %s", debug_str)
        # pylint: disable=no-else-return
        if resp.status_code == 404:
            return DruidTaskStatus.NOT_FOUND
        elif resp.status_code == 200:
            # Possible values are RUNNING, SUCCESS, and FAILED
            # Reference: https://github.com/apache/druid/blob/master/core/src/
            #            main/java/org/apache/druid/indexer/TaskStatus.java
            return DruidTaskStatus[resp.json()["status"]["status"]]
        else:
            raise ValueError(f"DruidAdmin.get_task_status: {debug_str}")

    def get_tasks(self,
                  created_time_filter: Optional[
                      Tuple[datetime, datetime]] = None,
                  state_filter: Optional[DruidTaskStateFilter] = None,
                  type_filter: Optional[DruidTaskTypeFilter] = None
                  ) -> List[Tuple[str, DruidTaskStatus]]:
        """
        Get tasks matching the specified filters.

        :param created_time_filter: Tuple of (start_time, end_time) to filter
                                    tasks. datetime must be in UTC with no
                                    explicit timezone set
        :param state_filter: If specified, filter tasks by this state
        :param type_filter: If specified, filter tasks by this type
        :return: List of (task_id, status) tuples
        """
        if created_time_filter is not None:
            (start_time, end_time) = created_time_filter
            if not (start_time.tzinfo is None and end_time.tzinfo is None):
                raise ValueError("Datetime must not have timezone. Got: "
                                 f"{created_time_filter}")
        params = {}
        if created_time_filter is not None:
            params["createdTimeInterval"] = \
                f"{start_time.isoformat()}_{end_time.isoformat()}"
        if state_filter is not None:
            params["state"] = state_filter.value
        if type_filter is not None:
            params["type"] = type_filter.value
        resp = requests.get(
            f"{self.overlord_url}/druid/indexer/v1/tasks",
            params=params
        )
        if resp.status_code != 200:
            raise ValueError('Failed to get tasks:'
                             ' params={} status={} resp={}'.format(
                                 params, resp.status_code, resp.json()))
        logger.debug('DruidAdmin.get_tasks: '
                     'params=%s status=%d resp=%s',
                     params, resp.status_code, resp.json())
        assert isinstance(resp.json(), list)
        task_list = []
        for task_json in resp.json():
            task_id = task_json["id"]
            task_status = DruidTaskStatus[task_json["status"]]
            task_list.append((task_id, task_status))
        return task_list

    # pylint: disable=too-many-arguments
    def _manage_data_source_segments(self,
                                     data_source: str,
                                     start_date: datetime,
                                     end_date: datetime,
                                     drop: bool,
                                     test_only: bool) -> None:
        """
        Drop segments from the datasource that are within given interval
        :param data_source:
        :param start_date: The start date after which segments are removed.
                           time component is ignored
        :param end_date: The end date before which segments are removed.
                         time component is ignored
        :param drop:
        :param test_only: If true just logs the actions, but doesnt actually
                          drop the segments. Use it to verify the regex first
        """
        # Post the interval to be marked unused
        # example: 1970-01-01T00:00:00/2020-06-10T00:00:00
        interval_date_str: str = '{}T00:00:00/{}T00:00:00'.\
            format(start_date.strftime('%Y-%m-%d'),
                   end_date.strftime('%Y-%m-%d'))
        logger.info('DruidAdmin.manage_data_source_segments: '
                    'data_source=%s interval=%s drop=%s',
                    data_source, interval_date_str, drop)
        if not test_only:
            command = 'markUnused' if drop else 'markUsed'
            resp = requests.post(
                '{}/druid/coordinator/v1/datasources/{}/{}'.format(
                    self.overlord_url, data_source, command),
                json={"interval": interval_date_str}
            )
            if resp.status_code not in [200, 204]:
                raise ValueError('Failed to drop segments for data_source:'
                                 ' {} status={} resp={}'.
                                 format(data_source,
                                        resp.status_code,
                                        resp.text()))
            if resp.status_code == 200:
                logger.info('DruidAdmin.manage_data_source_segments: '
                            'data_source=%s interval=%s status=%d resp=%s',
                            data_source, interval_date_str,
                            resp.status_code, resp.json())
            else:
                logger.info('DruidAdmin.manage_data_source_segments: '
                            'data_source=%s interval=%s status=%d NO RESP',
                            data_source, interval_date_str,
                            resp.status_code)

        else:
            logger.info('DruidAdmin.manage_data_source_segments:'
                        ' testOnly no segment changed')


    @staticmethod
    def _seconds_to_granularity_string(seconds: int) -> str:
        """
        Maps the seconds to druid granularity strings
        :param seconds:
        :return: Druid supported granularity strings
        """
        if seconds < 3600:
            return 'PT30M'
        if seconds < 3600*6:
            return 'PT1H'
        if seconds < 86400:
            return 'PT6H'
        if seconds < 86400*7:
            return 'P1D'
        if seconds < 86400*30:
            return 'P1W'
        return 'P1M'

    def _create_compaction_spec(self,
                                data_source: str,
                                max_rows_per_segment: int,
                                skip_offset_from_latest: str = 'P1D') -> None:
        """

        :param data_source:
        :param max_rows_per_segment: Max number of rows per segment after
                                     compaction.
        :param skip_offset_from_latest: The offset for searching segments to
                                        be compacted. Strongly recommended
                                        to set for realtime dataSources.
        """
        compaction_spec = {
            "dataSource": data_source,
            "skipOffsetFromLatest": skip_offset_from_latest,
            "maxRowsPerSegment": max_rows_per_segment,
            # Compaction will be skipped if the total size of all segments in
            # the last granularity period exceeds this value. The default value
            # of 400 MB is insufficient for rollup datasources which often
            # contain upto 600 MB of data. Picking 1 GB as our limit
            "inputSegmentSizeBytes": 1073741824
        }

        resp = requests.post(
            '{}/druid/coordinator/v1/config/compaction'.format(
                self.overlord_url),
            json=compaction_spec
        )
        if resp.status_code not in [200, 204]:
            raise ValueError('Failed to create compaction spec for datasource:'
                             ' {} status={}'.format(data_source,
                                                    resp.status_code))
        logger.info('DruidAdmin.create_compaction_spec: '
                    'data_source=%s max_rows_per_segment=%d '
                    'skip_offset_from_latest=%s status=%d',
                    data_source, max_rows_per_segment, skip_offset_from_latest,
                    resp.status_code)

    def _manage_retention_rule(self, data_source: str,
                               retention_interval: str,
                               num_replicas: int,
                               delete: bool) -> None:
        """
        Create load and drop rules for the requested data source.

        The load rule is used to create desired number of replicas. The drop
        rule is used to purge data older than `retention_interval`. Note that
        the drop rule only causes the data to be dropped from the historical
        cache. Data is still maintained in deep storage.
        :param data_source:
        :param retention_interval:  ISO-8601 Intervals (P1M, P1W, P1D etc)
        :param delete : If true it deletes the rule, otherwise creates it
        """
        if not delete:
            # We create two rules: load and drop rules. The drop rule must be
            # before the load rule because Druid selects the first matching
            # rule. In case of an old segment, the drop rule will be matched
            # and therefore applied. In case of a recent segment, the drop rule
            # will not match and therefore the load rule will get selected.
            retention_spec = [
                {
                    "type": "dropBeforeByPeriod",
                    "period": retention_interval
                },
                {
                    "type": "loadForever",
                    "tieredReplicants": {
                        "_default_tier": num_replicas
                    }
                }
            ]
        else:
            # Setting empty rules will delete the previously added rules
            retention_spec = []

        resp = requests.post(
            '{}/druid/coordinator/v1/rules/{}'.format(
                self.overlord_url, data_source),
            json=retention_spec
        )

        if resp.status_code not in [200, 204]:
            raise ValueError('Failed to manage retention rule for datasource:'
                             ' {} spec={} status={}'.format(
                                 data_source, json.dumps(retention_spec),
                                 resp.status_code))
        logger.info('DruidAdmin.manage_retention_rule: '
                    'data_source=%s retention_spec=%s status=%d',
                    data_source, json.dumps(retention_spec), resp.status_code)

    def _hard_reset_supervisor(self, datasource: str) -> None:
        """
        Performs a hard reset on supervisor
        Hard resetting a supervisor will lead to data loss or data duplication.

        The reason for using this operation is to recover from a state in which
        the supervisor ceases operating due to missing offsets.

        I understand that resetting dashboard_queries_1_t_16 will clear
        checkpoints and therefore lead to data loss or duplication.
        I understand that this operation cannot be undone.
        :param datasource:
        """
        resp = requests.post(
            '{}/druid/indexer/v1/supervisor/{}/reset'.format(
                self.overlord_url, datasource))
        if resp.status_code != 200:
            # check if its because the spec doesn't exist
            if 'Cannot find any supervisor' not in resp.text:
                raise ValueError('Failed to hard reset supervisor:'
                                 ' {} status={} resp={}'.
                                 format(datasource,
                                        resp.status_code,
                                        resp.text))
        logger.info('DruidAdmin.hard_reset_supervisor: '
                    'data_source=%s status=%d resp=%s',
                    datasource, resp.status_code, resp.text)

    @staticmethod
    def _create_dataschema_dict(datasource: str,
                                query_granularity,
                                segment_granularity,
                                intervals=None) -> dict:
        if isinstance(query_granularity, int):
            query_granularity_spec = {
                "type": "duration",
                "duration": query_granularity*1000
            }
        else:
            assert query_granularity in SUPPORTED_GRANULARY_STRINGS
            query_granularity_spec = query_granularity

        if isinstance(segment_granularity, int):
            segment_granularity_spec = {
                "type": "duration",
                "duration": segment_granularity*1000
            }
        else:
            assert segment_granularity in SUPPORTED_GRANULARY_STRINGS
            segment_granularity_spec = segment_granularity

        return {
            "dataSource": datasource,
            "granularitySpec": {
                "type": "uniform",
                "queryGranularity": query_granularity_spec,
                "segmentGranularity": segment_granularity_spec,
                "rollup": False,
                "intervals": intervals
            },
            "timestampSpec": {
                "column": "time",
                "format": "posix"
            },
            "dimensionsSpec": {
                "dimensions": [],
                "dimensionExclusions": [
                    "time",
                    "value"
                ]
            },
            "metricsSpec": [
                {
                    "name": "value",
                    "fieldName": "value",
                    "type": "doubleMin"
                }
            ]
        }

    @staticmethod
    def _validate_and_create_batch_input_source(location_type: str,
                                                paths: List[str]) -> dict:
        """
        Validate and create input_source dict to be used in batch
        ingest tasks.

        :param location_type: Must be one of ["local", "s3"]
        :param paths: Path of the file that is to be ingested in the task
        """
        # pylint: disable=no-else-return
        if location_type == "local":
            assert len(paths) == 1
            path = paths[0]
            (dirname, filename) = os.path.split(path)
            return {
                "type": "local",
                "baseDir": dirname,
                "filter": filename
            }
        elif location_type == "s3":
            return {
                "type": "s3",
                "uris": paths
            }
        else:
            raise ValueError(f"Unsupported location type {location_type}")
