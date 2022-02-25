"""
DruidAdmin module
"""
from __future__ import annotations

import json
import logging
import threading
import time
from typing import List, Dict

import requests

from nbdb.config.settings import Settings
from nbdb.common.data_point import MEASUREMENT

from pyformance import time_calls, meter_calls

logger = logging.getLogger()


class DruidSchema:
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
        self.tables: Dict = dict()
        self.schema_refresh_epoch: int = 0

    def load_schema(self):
        """
        Synchronized loading of schema
        :return:
        """
        if time.time() - self.schema_refresh_epoch <= \
                self.connection_string.schema_refresh_time:
            return

        with threading.RLock():
            if time.time() - self.schema_refresh_epoch > \
                    self.connection_string.schema_refresh_time:
                self._load_schema()

    @time_calls
    @meter_calls
    def _load_schema(self):
        """
        Loads the full schema
        :return:
        """
        logger.info('LOADING SCHEMA: last_refresh: %d '
                    'delta_since_last_refresh: %d',
                    self.schema_refresh_epoch,
                    time.time() - self.schema_refresh_epoch)
        query = "SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE " \
                "FROM INFORMATION_SCHEMA.COLUMNS " \
                "WHERE TABLE_SCHEMA='druid' and DATA_TYPE!='TIMESTAMP'"
        resp = requests.post('{}/druid/v2/sql'.format(self.router_url),
                             json={'query': query})
        if resp.status_code != 200:
            # check if its because the spec doesn't exist
            raise ValueError('Failed to lookup schema for druid'
                             ' status={} resp={}'.
                             format(resp.status_code, resp.text))
        rows = resp.json()
        tables = dict()
        for row in rows:
            table_name = row['TABLE_NAME']
            if table_name not in tables:
                tables[table_name] = {
                    'dimensions' : [],
                    'metrics': []
                }
            column_name = row['COLUMN_NAME']
            data_type = row['DATA_TYPE']
            if data_type == 'VARCHAR':
                tables[table_name]['dimensions'].append(column_name)
            else:
                tables[table_name]['metrics'].append(column_name)

        self.tables = tables
        self.schema_refresh_epoch = time.time()
        logger.info('DruidAdmin: Schema loaded: %d %s',
                    self.schema_refresh_epoch, json.dumps(self.tables))

    @time_calls
    @meter_calls
    def get_dimensions(self, datasource: str) -> List[str]:
        """
        Get all the dimensions for the datasource
        :param datasource:
        :return:
        """
        # check if its been more than a minute since we refreshed
        # schema
        self.load_schema()
        if datasource not in self.tables:
            logger.error("Datasource %s not found", datasource)
            raise ValueError('get_dimensions: Unknown datasource: {}'
                             ' known datasources: {}'.
                             format(datasource, ','.join(self.tables.keys())))
        return self.tables[datasource]['dimensions']

    @time_calls
    @meter_calls
    def get_dimension_values(self, datasource: str, dimension: str,
                             extra_and_clauses: Dict[str, str] = None) \
            -> List[str]:
        """
        Figure out the dimension values from the given table in Druid
        :param datasource: Druid datasource name
        :param dimension: Dimension column name
        :param extra_and_clauses: Any additional filter clauses
        """
        self.load_schema()
        if datasource not in self.tables:
            logger.error("Datasource %s not found", datasource)
            raise ValueError('get_dimension_values: '
                             'Unknown datasource: {} known datasources: {}'.
                             format(datasource, ','.join(self.tables.keys())))

        filter_clauses_str = ""
        if extra_and_clauses:
            for key, value in extra_and_clauses.items():
                filter_clauses_str += f" AND \"{key}\" = '{value}'"

        # Note that we use an interval of 1 day
        query = "SELECT DISTINCT(\"{dimension}\") FROM \"{datasource}\" "\
                "WHERE \"{dimension}\" IS NOT NULL AND "\
                "__time >= CURRENT_TIMESTAMP - INTERVAL '1' DAY "\
                "{filter_clauses_str} "\
                "LIMIT 100".format(dimension=dimension, datasource=datasource,
                                   filter_clauses_str=filter_clauses_str)
        logger.info("Running %s", query)
        # TODO: We should use DruidReader instead of querying over HTTP
        resp = requests.post('{}/druid/v2/sql'.format(self.router_url),
                             json={'query': query})

        if resp.status_code != 200:
            raise ValueError('Failed to load dim values status={} resp={}'.
                             format(resp.status_code, resp.text))
        return [row[dimension] for row in resp.json()]

    def get_measurements(self, exploration_datasource: str) -> List[str]:
        """
        Return the list of measurements from the Druid exploration table.
        :param exploration_datasource: Name of the exploration datasource
        """
        return self.get_dimension_values(exploration_datasource, MEASUREMENT)

    @time_calls
    @meter_calls
    def get_fields(self, datasource: str, measurement: str) -> List[str]:
        """
        Figure out the unique fields in the given table.
        :param datasource: Druid datasource name
        :param prefix_pattern: Influx measurement used as a search pattern
        """
        self.load_schema()
        if datasource not in self.tables:
            logger.error("Datasource %s not found", datasource)
            raise ValueError('get_fields: '
                             'Unknown datasource: {} known datasources: {}'.
                             format(datasource, ','.join(self.tables.keys())))

        # Note that we use an interval of 1 day
        query = "SELECT DISTINCT(\"field\") FROM \"{}\" "\
                "WHERE __time >= CURRENT_TIMESTAMP - INTERVAL '1' DAY "\
                "AND \"{}\" = '{}' "\
                "LIMIT 100".format(datasource, MEASUREMENT, measurement)
        logger.info("Running %s", query)
        # TODO: We should use DruidReader instead of querying over HTTP
        resp = requests.post('{}/druid/v2/sql'.format(self.router_url),
                             json={'query': query})

        if resp.status_code != 200:
            raise ValueError('Failed to load fields status={} resp={}'.
                             format(resp.status_code, resp.text))
        return [row['field'] for row in resp.json()]


    @time_calls
    @meter_calls
    def get_metrics(self, datasource: str) -> List[str]:
        """
        Get all the metrics for the datasource
        :param datasource:
        :return:
        """
        self.load_schema()
        if datasource not in self.tables:
            logger.error("Datasource %s not found", datasource)
            raise ValueError('get_metrics: '
                             'Unknown datasource: {} known datasources: {}'.
                             format(datasource, ','.join(self.tables.keys())))
        return self.tables[datasource]['metrics']
