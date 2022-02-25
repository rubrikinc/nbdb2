"""
SparseSeriesReader
"""
from __future__ import annotations

import itertools
import json
import os
import logging
import random
import threading
import time
from typing import List, Dict

import orjson
import requests
from nbdb.common.tdigest_store import TDigestStore
from nbdb.common.atomic import AtomicNumber
from nbdb.common.data_point import DATAPOINT_VERSION, FIELD, VALUE
from nbdb.common.data_point import TOKEN_TAG_PREFIX, TOKEN_COUNT
from nbdb.common.data_point import MISSING_POINT_VALUE, TOMBSTONE_VALUE
from nbdb.common.druid_schema import DruidSchema
from nbdb.common.telemetry import Telemetry, user_time_calls
from nbdb.config.settings import Settings
from nbdb.readapi.dense_functions import DenseFunctions
from nbdb.readapi.SparseSeriesReaderInterface import \
    SparseSeriesReaderInterface
from nbdb.readapi.sql_parser import FillFunc
from nbdb.readapi.time_series_response import TimeSeriesGroup, TimeSeries, \
    TimeRange
from nbdb.schema.schema import Schema
from pydruid.db.api import Connection, Cursor, apply_parameters, check_closed
from pydruid.db import exceptions

from nbdb.schema.schema import SHARD_BY_CLUSTER, SHARD_BY_METRIC

logger = logging.getLogger(__name__)

# The original datapoint version column name begins with an
# underscore. Druid JSON reader can't parse column names
# beginning with underscores, so we use an alias
DATAPOINT_VERSION_ALIAS = "datapoint_version"
ALIASES = {
    DATAPOINT_VERSION: DATAPOINT_VERSION_ALIAS
}

# Username used by anomaly.rubrik.com while issuing queries to AnomalyDB
DASHBOARD_USER = "anomalydb_grafana"

PERCENTILES_LIST = [0, 50, 90, 95, 99, 100]

# Full history of forced-write interval changes in write path.
# This is used by read path to determine the correct lookback period to use
# for a given query - depending on the query's time range start.
#
# Below ==> journal of TIMESTAMPS for write interval changes
#
FORCE_WRITE_INTERVAL_CHANGE_TS_2022_12_21_8AM_UTC = 1640073600
#
# Below ==> journal of VALUES for write interval changes
#
FORCE_WRITE_INTERVAL_USED_BEFORE_2021_12_21_8AM_UTC = 12 * 3600  # 12 hours
FORCE_WRITE_INTERVAL_CURRENT = 30 * 60  # 30 minutes

td_store: TDigestStore = TDigestStore()

get_field_last_percentiles_report_ts = 0
get_field_last_percentiles_report_lock = threading.Lock()

MARKERS_FLOAT_LIST = [float(m) for m in [MISSING_POINT_VALUE, TOMBSTONE_VALUE]]

# optimize_lookback stats
optimize_lookback_agg_t_on = AtomicNumber(0.0)
optimize_lookback_agg_t_off = AtomicNumber(0.0)
optimize_lookback_agg_t_check = AtomicNumber(0.0)
optimize_lookback_hits = AtomicNumber(0)
optimize_lookback_misses = AtomicNumber(0)

# optimize_lookback config
optimize_lookback_conf_lock = threading.Lock()  # TODO(optimize_lookback) remove
optimize_lookback_conf_last_read_ts = 0
optimize_lookback_conf_file = '/tmp/optimize_lookback.conf'
optimize_lookback_conf_default = {
    "duplicate_pct": 0,
    "default_optimize_lookback": 0,
    "non_marker_lookback_val_allowed_gap_pct": 0
}
optimize_lookback_conf = optimize_lookback_conf_default


def _optimize_lookback_validate_config(config) -> bool:
    if not isinstance(config, dict):
        logger.error('Invalid optimize_lookback config: must be a dict')
        return False
    if set(config) != set(optimize_lookback_conf):
        logger.error('Invalid optimize_lookback config: keys mismatch')
        return False
    try:
        duplicate_pct = int(config['duplicate_pct'])
        if duplicate_pct < 0 or duplicate_pct > 100:
            logger.error('Invalid optimize_lookback config: '
                         'duplicate_pct must be in range [0,100]')
            return False
        default_optimize_lookback = int(config['default_optimize_lookback'])
        if default_optimize_lookback not in [0, 1]:
            logger.error('Invalid optimize_lookback config: '
                         'default_optimize_lookback must be 0 or 1')
            return False
        gap_pct = int(config['non_marker_lookback_val_allowed_gap_pct'])
        if gap_pct < 0:
            logger.error('Invalid optimize_lookback config: '
                         'non_marker_lookback_val_allowed_gap_pct must be >= 0')
            return False
    except ValueError as e:
        logger.error(f'Invalid optimize_lookback config: invalid value type: '
                     f'{str(e)}')
        return False
    return True


def _optimize_lookback_roll_dice() -> (bool, int, int):
    global optimize_lookback_conf_last_read_ts
    global optimize_lookback_conf

    # read the optimize_lookback config, unless it was read in the last 1 min.
    # note: keep using the current config upon any error.
    now = time.time()
    with optimize_lookback_conf_lock:
        if now > optimize_lookback_conf_last_read_ts + 60:
            optimize_lookback_conf_last_read_ts = now
            if os.path.exists(optimize_lookback_conf_file):
                try:
                    with open(optimize_lookback_conf_file, 'r',
                              encoding='utf-8') as f:
                        config = json.loads(f.read())
                        if _optimize_lookback_validate_config(config) and \
                                optimize_lookback_conf != config:
                            logger.info(f'LOOKBACK config (new): {config} '
                                        f'[was: {optimize_lookback_conf}]')
                            optimize_lookback_conf = config
                except (OSError, json.JSONDecodeError):
                    # something wrong with file - keep using current config
                    logger.error('Failed to read lookback config')

            elif optimize_lookback_conf != optimize_lookback_conf_default:
                # reset config to default config, as the file does not exist
                logger.info(f'LOOKBACK config (reset): '
                            f'{optimize_lookback_conf_default} '
                            f'[was: {optimize_lookback_conf}]')
                optimize_lookback_conf = optimize_lookback_conf_default

        # roll the dice with success rate == percentage from config file
        pct: int = optimize_lookback_conf['duplicate_pct']
        return (
            False if pct == 0 else random.randint(0, 99) < pct,
            optimize_lookback_conf['default_optimize_lookback'],
            optimize_lookback_conf['non_marker_lookback_val_allowed_gap_pct']
        )


class DruidCursor(Cursor):
    """
    Same as pydruid.db.api.Cursor but with much faster response parsing
    """
    def _stream_query(self, query):
        """
        Stream rows from the Druid query response in chunks.

        This method will yield rows as the data is returned in chunks from the
        server.

        We override the parent _stream_query() for the following reasons:
        1. _stream_query() gets a string response in arbitrary chunk sizes and
        then calls the very slow rows_from_chunks() methods to search for JSON
        object boundaries.

        Druid SQL exposes a parameter called resultFormat. With
        resultFormat=objectLines, we can ask Druid to add newlines at object
        boundaries. This makes it very easy to load JSON objects in chunks
        without needing the expensive rows_from_chunks()

        2. It tries to create a namedtuple object from the JSON row. We don't
        care about this and save time by directly working with JSON rows.
        """
        self.description = None

        headers = {"Content-Type": "application/json"}

        payload = {"query": query, "context": self.context,
                   "header": self.header,
                   "resultFormat": "objectLines"}

        auth = (
            requests.auth.HTTPBasicAuth(self.user, self.password)
            if self.user else None)
        r = requests.post(
            self.url,
            stream=True,
            headers=headers,
            json=payload,
            auth=auth,
            verify=self.ssl_verify_cert,
        )
        if r.encoding is None:
            r.encoding = "utf-8"
        # raise any error messages
        if r.status_code != 200:
            try:
                payload = r.json()
            # pylint: disable-msg=W0703  # Broad Exception
            except Exception:
                payload = {
                    "error": "Unknown error",
                    "errorClass": "Unknown",
                    "errorMessage": r.text,
                }
            msg = "{error} ({errorClass}): {errorMessage}".format(**payload)
            raise exceptions.ProgrammingError(msg)

        # Load data in chunks of 8K bytes to avoid loading large responses in
        # memory
        for line in r.iter_lines(chunk_size=8092, decode_unicode=True):
            if line:
                yield orjson.loads(line)
            else:
                # We can get empty lines sometimes because there is an \n extra
                # at the end of a chunk or at the end of the response
                continue

    def fetchone(self):
        """
        Fetch the next row of a query result set, returning a single sequence,
        or `None` when no more data is available.

        Override the parent method to avoid unnecessary decorator invocations
        """
        try:
            return self.next()
        except StopIteration:
            return None

    def fetchmany(self, size=None):
        """
        Fetch the next set of rows of a query result, returning a sequence of
        sequences (e.g. a list of tuples). An empty sequence is returned when
        no more rows are available.

        Override the parent method to avoid unnecessary decorator invocations
        """
        size = size or self.arraysize
        return list(itertools.islice(self._results, size))

    @check_closed
    def execute(self, operation, parameters=None):
        """
        Override to use our faster implementation of _stream_query()

        Check the documentation for _stream_query() to see why it is better
        """
        query = apply_parameters(operation, parameters)
        self._results = self._stream_query(query)
        return self


class DruidConnection(Connection):
    """
    Same as pydruid.db.api.Connection but uses the much faster DruidCursor
    """
    def cursor(self):
        """Return a new DruidCursor Object instead of the parent Cursor"""
        cursor = DruidCursor(
            self.url,
            self.user,
            self.password,
            self.context,
            self.header,
            self.ssl_verify_cert,
        )

        self.cursors.append(cursor)
        return cursor


class DruidReader(SparseSeriesReaderInterface):
    """
    This class provides the read interface to the sparse data stored in the
    store
    """

    inst: DruidReader = None

    def __init__(self, connection_string: Settings):
        """
        Initialize the reader object for druid
        :param connection_string: URL of druid
        :param schema:
        """
        self.connection_string = connection_string
        self.druid_schema = DruidSchema(connection_string)
        self.conn = None
        self.cc_conn = None

    @staticmethod
    def instantiate(connection_string: Settings):
        """
        Instantiate the singleton
        :param connection_string
        :param schema
        """
        if DruidReader.inst is not None:
            logger.warning("Trying to re-instantiate DruidReader")
            return

        DruidReader.inst = DruidReader(connection_string)

    def connect(self):
        """
        Establish a connection to the druid cluster
        :return:
        """
        self.conn = DruidConnection(
            host=self.connection_string.router_ip,
            port=self.connection_string.router_port,
            path=self.connection_string.read_path,
            scheme=self.connection_string.scheme)

        # cc_conn is specifically used for cross cluster queries
        # use separate broker+router nodes for single-cluster queries &
        # cross-cluster queries. This is for performance isolation and to
        # ensure that dashboard queries are not negatively impacted due
        # to high cross-cluster query load
        self.cc_conn = DruidConnection(
            host=self.connection_string.cc_router_ip,
            port=self.connection_string.cc_router_port,
            path=self.connection_string.read_path,
            scheme=self.connection_string.scheme)

        self.druid_schema.load_schema()

    def get_connection(self, use_cross_cluster_conn: bool):
        """
        Select Druid router connection based on requirements.

        :param use_cross_cluster_conn: Boolean indicating whether cross-cluster
        connection must be used
        """
        return self.cc_conn if use_cross_cluster_conn else self.conn

    def execute(self, query, datasource, user):
        """
        Connect to druid, obtain a cursor and execute the query
        :param query:
        :param datasource:
        :param user:
        :return:
        """
        # We want to use the crosscluster broker+router nodes if the query is
        # for cross-cluster datasources AND it is not coming in from
        # anomaly.rubrik.com
        use_cross_cluster_conn = (
            Schema.is_crosscluster_datasource(datasource) and
            user != DASHBOARD_USER)
        if self.get_connection(use_cross_cluster_conn) is None:
            self.connect()
        logger.info('Executing: %s', query)
        return self.get_connection(use_cross_cluster_conn).execute(query)

    def get_datasource(self,
                       schema: Schema,
                       measurement_prefix: str,
                       tags: Dict[str, str],
                       cluster_id: str,
                       interval: int,
                       protocol: str,
                       trace: bool=False,
                       query_id: str=None) -> str:
        """
        Get the datasource based on the measurement prefix, cluster shard
        and rollup window
        :param measurement_prefix: measurement prefix that is used to identify
        datasource based on prefix matching rules
        :param tags:
        :param cluster_id: optional , can be None
        :param interval: required
        :param protocol: Either 'graphite' or 'influx'
        :return:
        """
        # first check if interval is large enough to fit in rollup windows
        selected_window = 0
        for window in schema.rollup_windows:
            if interval >= window:
                selected_window = max(selected_window, window)
        
        shard_tag_value = schema.get_shard_tag_value(measurement_prefix, tags,
                                                     protocol)
        if trace:
            logger.info('[Trace: %s] get_datasource(): tags=%s cluster_id=%s '
                        'shard_tag_value=%s measurement_prefix=%s '
                        'protocol=%s selected_window=%s',
                        query_id, tags, cluster_id, shard_tag_value,
                        measurement_prefix, protocol, selected_window)

        # We have two kinds of datasources: those sharded by cluster ID and
        # those sharded by metric. We use the former for single cluster
        # queries and the latter for cross-cluster queries. If
        # shard_tag_value is *, it means it is a cross-cluster query
        if (shard_tag_value is not None and
                (shard_tag_value == '*' or '*' in shard_tag_value)):
            # We allow both clusters.* and clusters.a* queries, and treat them
            # both as cross-cluster queries
            select_mode = SHARD_BY_METRIC
        else:
            select_mode = SHARD_BY_CLUSTER

        if selected_window > 0:
            return schema.get_rollup_datasource(selected_window, tags,
                                                select_mode)

        # NOTE: Disabling the dashboard datasources for now
        # First check if we have a dashboard datasource defined
        # datasource = schema.get_dashboard_datasource(tags, cluster_id)
        # if datasource is not None:
        #     return datasource
        return schema.get_datasource(measurement_prefix, tags, protocol,
                                     select_mode)

    # pylint: disable-msg=R0913  # Too many arguments
    @user_time_calls
    def get_field(self,
                  schema: Schema,
                  datasource: str,
                  field: str,
                  filters: Dict,
                  time_range: TimeRange,
                  groupby: List[str],
                  query_id: str,
                  trace: bool,
                  fill_func: FillFunc,
                  fill_value: float,
                  create_flat_series_id: bool,
                  sparseness_disabled: bool,
                  lookback_sec: int = -1,  # -1: default, else: lookback to use
                  optimize_lookback: int = -1,  # -1: default, 0: False, 1: True
                  out_druid_query: list = [False],  # [True]: fill in the query
                  user: str = None
                  ) -> TimeSeriesGroup:
        """
        Run the query and return the timeseries
        :param datasource:
        :param field:
        :param filters:
        :param time_range:
        :param groupby: list of fields to group the results by
        :param query_id: unique string assigned to the specific query
        :param trace: if true log detailed execution trace
        :param fill_func: Function to use to fill in None values
        :param fill_value: Fill value to use if fill_func is constant
        :param create_flat_series_id: Generates a series id that matches
                                      graphite flat schema
        :param sparseness_disabled:
        :param lookback_sec: If -1, use the default lookback. Otherwise,
                             given value overrides default lookback value.
        :param optimize_lookback:
          Default behavior (-1):
            - Roll the dice to see if we can duplicate the query and run
              get_field() twice - once with optimize_lookback=1 (ON) and once
              with optimize_lookback=0 (OFF). The config file decides which %
              of queries should be duplicated. If the dice show a value >=
              configured duplicate_pct - run only with optimization OFF. If the
              dice show a value smaller than duplicate_pct - duplicate query.
            - Compare the results from both calls, and log an error upon
              mismatch.
            - Return the result from get_field() with optimize_lookback=0 (OFF).
          If optimize_lookback is either 0 (OFF) or 1 (ON):
            - Execution will continue with the given mode.
            - Currently, this can only be done by explicitly specifying
              "||optimize_lookback=true" or "||optimize_lookback=false" in
              the query target (similar to tracing). This allows to turn the
              optimization ON or OFF when running benchmark_read_query.py.
        :param out_druid_query: the passed parameter must be either [False] or
              [True]. if [True], the query issued to druid would be returned in
              index 0 of out_druid_query (i.e. it would replace the True).
        :param user: must be a kwarg for the user_time_calls() decorator to work
        :return: list of epochs & list of list of values corresponding to
                 epochs
        The field depending on the filters may map to multiple series
        we return the result of each series as list of values
        Where len(epochs) = (end_epoch - start_epoch)/interval
        """
        # TODO(optimize_lookback) cleanup feature evaluation code
        if not sparseness_disabled and optimize_lookback == -1:
            # roll the dice to determine whether we should duplicate the query
            # and run with both optimize_lookback=ON and optimize_lookback=OFF,
            # or run only the default mode (set by the config)
            duplicate, default_optimize_lookback, gap_pct = \
                _optimize_lookback_roll_dice()
            if not duplicate:
                # run only using the default optimize_lookback setting
                t0 = time.perf_counter()

                tsg: TimeSeriesGroup = self.get_field(
                    schema, datasource, field, filters, time_range, groupby,
                    query_id, trace, fill_func, fill_value,
                    create_flat_series_id, sparseness_disabled, lookback_sec,
                    default_optimize_lookback,
                    user=user)

                t1 = time.perf_counter()
                td_store_key = ('get_field_optimize_lookback_on'
                                if default_optimize_lookback else
                                'get_field_optimize_lookback_off')
                td_store.update(td_store_key, t1 - t0)

                # log get_field() duration percentiles every 10 min
                # note: acquire the lock non-blocking, i.e. if it's already
                # locked - just skip the logging without blocking
                now = time.time()
                global get_field_last_percentiles_report_ts
                if now > get_field_last_percentiles_report_ts + 600 and \
                        get_field_last_percentiles_report_lock.acquire(False):
                    try:
                        if now > get_field_last_percentiles_report_ts + 600:
                            get_field_last_percentiles_report_ts = now
                            pctls_on = td_store.get_percentiles(
                                'get_field_optimize_lookback_on',
                                PERCENTILES_LIST)
                            pctls_off = td_store.get_percentiles(
                                'get_field_optimize_lookback_off',
                                PERCENTILES_LIST)
                            logger.info(
                                f'PCTLS get_field_optimize_lookback_on: '
                                f'{pctls_on}')
                            logger.info(
                                f'PCTLS get_field_optimize_lookback_off: '
                                f' {pctls_off}')
                    except:
                        logger.error('PCTLS failed to retrieve percentiles!')
                    finally:
                        get_field_last_percentiles_report_lock.release()
                return tsg

            t0 = time.perf_counter()

            # call get_field() with optimize_lookback=ON
            druid_query_ref_on = [True]
            tsg_on: TimeSeriesGroup = self.get_field(
                schema, datasource, field, filters, time_range, groupby,
                query_id, trace, fill_func, fill_value,
                create_flat_series_id, sparseness_disabled, lookback_sec,
                1,  # optimize_lookback=ON
                druid_query_ref_on,  # out_druid_query
                user=user)

            t1 = time.perf_counter()

            # call get_field() with optimize_lookback=OFF
            tsg_off: TimeSeriesGroup = self.get_field(
                schema, datasource, field, filters, time_range, groupby,
                query_id, trace, fill_func, fill_value,
                create_flat_series_id, sparseness_disabled, lookback_sec,
                0,  # optimize_lookback=OFF
                user=user)

            t2 = time.perf_counter()

            # update stats
            t_on = t1 - t0
            agg_t_on = optimize_lookback_agg_t_on.add_value(t_on)
            td_store.update('get_field_optimize_lookback_on', t_on)
            td_store.update('get_field_optimize_lookback_on_duplicate', t_on)
            cdf_on = td_store.cdf('get_field_optimize_lookback_on_duplicate',
                                  t_on)
            pctls_on = td_store.get_percentiles(
                'get_field_optimize_lookback_on_duplicate', PERCENTILES_LIST)

            t_off = t2 - t1
            agg_t_off = optimize_lookback_agg_t_off.add_value(t_off)
            td_store.update('get_field_optimize_lookback_off', t_off)
            td_store.update('get_field_optimize_lookback_off_duplicate', t_off)
            cdf_off = td_store.cdf('get_field_optimize_lookback_off_duplicate',
                                   t_off)
            pctls_off = td_store.get_percentiles(
                'get_field_optimize_lookback_off_duplicate', PERCENTILES_LIST)

            # check if both calls returned the same result
            diff_results = TimeSeriesGroup.diff(tsg_on, tsg_off, gap_pct)
            t3 = time.perf_counter()
            t_check = t3 - t2
            agg_t_check = optimize_lookback_agg_t_check.add_value(t_check)

            if diff_results:
                misses = optimize_lookback_misses.increment_value()
                logger.error(
                    'LOOKBACK miss! {"t_ON": %.3f, "t_OFF": %.3f, '
                    '"t_CHK": %.3f, "hits": %d, "misses": %d, '
                    '"agg_t_ON": %.3f, "agg_t_OFF": %.3f, "agg_t_CHK": %.3f, '
                    '"cdf_ON": %.1f, "cdf_OFF": %.1f, '
                    '"pctls_ON": %s, "pctls_OFF": %s, '
                    '"druid_query": "%s", '
                    '"groups_ON": %s, "groups_OFF": %s, '
                    '"series_cnt_ON": %d, "series_cnt_OFF": %d, '
                    '"diff_results": %s}',
                    t_on, t_off, t_check,
                    optimize_lookback_hits.get_value(), misses,
                    agg_t_on, agg_t_off, agg_t_check,
                    cdf_on, cdf_off, str(pctls_on), str(pctls_off),
                    druid_query_ref_on[0],
                    str(sorted(list(tsg_on.groups))),
                    str(sorted(list(tsg_off.groups))),
                    sum([len(ts_list) for ts_list in tsg_on.groups.values()]),
                    sum([len(ts_list) for ts_list in tsg_off.groups.values()]),
                    json.dumps(diff_results))
            else:
                hits = optimize_lookback_hits.increment_value()
                logger.info(
                    'LOOKBACK hit! {"t_ON": %.3f, "t_OFF": %.3f, '
                    '"t_CHK": %.3f, "hits": %d, "misses": %d, '
                    '"agg_t_ON": %.3f, "agg_t_OFF": %.3f, "agg_t_CHK": %.3f, '
                    '"cdf_ON": %.1f, "cdf_OFF": %.1f, '
                    '"pctls_ON": %s, "pctls_OFF": %s, '
                    '"druid_query": "%s"}',
                    t_on, t_off, t_check,
                    hits, optimize_lookback_misses.get_value(),
                    agg_t_on, agg_t_off, agg_t_check,
                    cdf_on, cdf_off, str(pctls_on), str(pctls_off),
                    druid_query_ref_on[0])

            # return the results - use the default optimize lookback config
            # to determine which results to return
            return tsg_on if default_optimize_lookback else tsg_off

        datasource_tel = datasource.replace('.', '_')
        Telemetry.inst.registry.meter(
            measurement='ReadApi.DruidReader.get_field.datasource',
            tag_key_values=[f"Datasource={datasource_tel}"]).mark()
        # pylint: disable-msg=R0914  # Too many local variables
        # Generate the Druid Query
        dimensions = self.druid_schema.get_dimensions(datasource)

        if create_flat_series_id:
            # we need to sort the token dimensions numerically
            # to ensure we can properly reconstruct the flat series id
            # this is absolutely required for graphite group by queries to work
            token_dimensions = sorted([d
                                       for d in dimensions
                                       if d.startswith(TOKEN_TAG_PREFIX) and
                                       d != TOKEN_COUNT],
                                      key=
                                      lambda d: int(d[len(TOKEN_TAG_PREFIX):]))
            # Append the datapoint version dimension if batch mode. Our filter
            # above would have removed it from the original list of dimensions
            # returned by Druid
            if schema.batch_mode:
                token_dimensions.append(DATAPOINT_VERSION)
            # Append the field dimension
            token_dimensions.append('field')
            dimensions = token_dimensions
        else:
            dimensions = sorted(dimensions)

        down_sampling_function = schema.get_down_sampling_function(
            datasource, field)

        # We use different cursors for all queries we need to execute
        cursors = {}
        if sparseness_disabled:
            # If sparseness is disabled, we don't need to fetch the marker &
            # non-marker info for timestamps before the requested time window
            lookback_sec = 0  # no lookback needed - use 0 as lookback
        elif optimize_lookback:
            # We need to fetch the marker and non-marker as described below,
            # but we do it via inline lookback on the original query, using an
            # expanded time range, rather than issuing 2 extra lookback queries.
            if lookback_sec == -1:  # use default lookback
                lookback_sec = self._get_lookback_period_sec(schema, datasource,
                                                             time_range.start)
        else:
            if lookback_sec == -1:  # use default lookback
                lookback_sec = self._get_lookback_period_sec(schema, datasource,
                                                             time_range.start)
            # We need to figure out the latest tombstone & missing markers just
            # before the query range start. This is needed to confirm
            # whether a series is missing in a given time range because it is
            # actually dead or it's not dead because we found non-marker values
            # in the query after this one.
            cursors['last_val_marker'] = self._execute_last_value_query(
                schema, datasource, field, filters, time_range.start,
                lookback_sec, dimensions, marker_value=True, user=user)

            # We need to figure out the latest non-marker values before the
            # query range start. This is needed because it is possible for a
            # series to have zero values in Druid for a given time range due to
            # sparseness. Since we use a forced write interval of 12h, we need
            # to check for 12h before the query window start to confirm if a
            # series is actually alive
            cursors['last_val_non_marker'] = self._execute_last_value_query(
                schema, datasource, field, filters, time_range.start,
                lookback_sec, dimensions, marker_value=False, user=user)

        if optimize_lookback:
            # Note: preserve time window used in _build_last_value_query():
            #   MILLIS_TO_TIMESTAMP({segment_start_epoch}) < __time AND
            #   __time <= MILLIS_TO_TIMESTAMP({start_epoch}) AND '
            # TODO: LIMIT needs to be better handled
            druid_query = (
                f'SELECT TIMESTAMP_TO_MILLIS(__time) AS ts, '
                f'{DruidReader._build_dimension_string(dimensions, ALIASES)}'
                f'"{VALUE}" '
                f'FROM "{datasource}" '
                f'WHERE '
                f'__time > MILLIS_TO_TIMESTAMP({(time_range.start - lookback_sec) * 1000}) AND '
                f'__time <= MILLIS_TO_TIMESTAMP({time_range.end * 1000}) AND '
                f'{DruidReader._get_filter_clause(filters, field)} '
                f'LIMIT 10000000'
            )
            if out_druid_query and out_druid_query[0]:  # in arg: [True]
                out_druid_query[0] = druid_query  # out arg: [druid_query]
            try:
                cursors['sparse_data'] = self.execute(druid_query, datasource,
                                                      user)
            except Exception as e:
                raise ValueError(f'druid query:\n\t {druid_query}\n failed '
                                 f'with {str(e)}') from e
        else:
            # Query to fetch sparse data in the requested time range
            cursors['sparse_data'] = self._execute_query(
                datasource, field, filters, time_range.start, time_range.end,
                dimensions, user=user)

        t_aggregator_process = time.perf_counter()
        series_groups = dict()
        aggregations = dict()
        samples_processed = {}
        default_fill_value = None
        if fill_func == FillFunc.CONSTANT:
            default_fill_value = fill_value

        datapoint_version_dim = ALIASES.get(DATAPOINT_VERSION,
                                            DATAPOINT_VERSION)
        # When creating series ID, we explicitly exclude DATAPOINT_VERSION.
        # This is because we want different bundle datapoints for the same
        # series to be processed together.
        series_id_dims = [ALIASES.get(dimension, dimension)
                          for dimension in dimensions
                          if dimension != DATAPOINT_VERSION]
        # We record Druid fetch time per cursor
        t_druid_fetch = {}
        fetch_batch_size = 1000

        # (optimize_lookback)
        # last_markers_map[series_id][0] - remembers the last marker value
        # last_markers_map[series_id][1] - remembers the last non-marker value
        last_markers_map = {}

        # if no lookback is needed (i.e. lookback is 0) - avoid extra
        # processing in loop by disabling optimize_lookback
        if lookback_sec == 0:
            optimize_lookback = 0

        for cursor_name, cursor in cursors.items():
            t_druid_fetch[cursor_name] = 0
            samples_processed[cursor_name] = 0
            t_s = time.perf_counter()
            # Fetch 1K rows at a time to amortize the telemetry cost
            rows = cursor.fetchmany(size=fetch_batch_size)
            t_druid_fetch[cursor_name] += time.perf_counter() - t_s
            idx = 0

            while rows:
                row = rows[idx]
                samples_processed[cursor_name] += 1
                epoch = int(row['ts']/1000)
                # we normalize values before storing to druid,
                # have to denormalize before consuming.
                # See comments on the normalize_value method
                value = row['value']  # value is always the last one based
                # Fetch the datapoint version if available. We use it for
                # tombstone invalidation
                if row.get(datapoint_version_dim):
                    attr_value = row[datapoint_version_dim]
                    datapoint_version = int(attr_value) if attr_value else None
                else:
                    datapoint_version = None
                if trace:
                    logger.info('[Trace: %s] Row: %s', query_id, row)
                if create_flat_series_id:
                    # flat schema series_id is concatenation of dimensions
                    dimension_values = [row[dim] for dim in series_id_dims
                                        if row[dim] != '']

                    series_id = '.'.join(dimension_values)
                else:
                    # for sql_api explicitly list the dimension name
                    series_id = datasource + '.' + \
                                '.'.join([
                                    dim + '=' + row[dim]
                                    for dim in series_id_dims])

                if series_id not in aggregations:
                    aggregations[series_id] = \
                        SparseSeriesReaderInterface.instantiate_aggregator(
                            down_sampling_function,
                            time_range,
                            fill_func,
                            fill_value,
                            default_fill_value,
                            sparseness_disabled,
                            query_id,
                            trace)
                    if optimize_lookback:
                        last_markers_map[series_id] = [(0, 0, None),
                                                       (0, 0, None)]

                    # Add the series to appropriate group name
                    if len(groupby) > 0:
                        group_name = ','.join([
                            '{}:{}'.format(dimension, row[dimension])
                            for dimension in groupby
                        ])
                    else:
                        # default group
                        group_name = '_'
                    if group_name not in series_groups:
                        series_groups[group_name] = list()
                    series_groups[group_name].append(series_id)

                # Get the aggregator and process the data point
                aggregator = aggregations[series_id]

                #
                # Note: if optimize_lookback is ON, this loop will only
                # iterate over a single cursor since we use a single query (not
                # 3 cursors, where 2 are lookback query results). The original
                # query is run with padded time range [start - lookback, end] to
                # do inline lookback and avoid the expensive lookback queries.
                #
                # In order to simulate the original behavior, the following
                # invariant must be kept: aggregations[series_id] must only
                # contain the LAST marker and LAST non-marker data points. In
                # order to keep this invariant:
                #  1. Memorize last-seen marker and non-marker data points as
                #     the loop iterates over results (per series_id) in:
                #     - last_marker[series_id]
                #     - last_non_marker[series_id]
                #  2. Once the loop is done, go over all memorized points (for
                #     all seen series_id's) and insert them into
                #     aggregations[series_id]. There would only be upto 1 marker
                #     data point (last_marker[series_id]) and upto 1 non-marker
                #     data point (last_non_marker[series_id]).
                #
                # collect all data points that belong to the original query
                # window, regardless of optimize_lookback
                #
                if not optimize_lookback:
                    aggregator.process(epoch, value,
                                       datapoint_version=datapoint_version)
                else:
                    # (optimize_lookback) process points from orig query window
                    #
                    # TODO(optimize_lookback) below check should be >= rather
                    #  than >, but since this only affects the results when
                    #  sparseness is disabled - preserve the existing behavior,
                    #  i.e. use the same time window used in _build_query():
                    #   MILLIS_TO_TIMESTAMP({start_epoch}) < __time AND
                    #   __time <= MILLIS_TO_TIMESTAMP({end_epoch})
                    if epoch > time_range.start:
                        aggregator.process(epoch, value,
                                           datapoint_version=datapoint_version)
                    # (optimize_lookback) keep only the latest marker seen
                    elif float(value) in MARKERS_FLOAT_LIST:
                        if epoch > last_markers_map[series_id][0][0]:
                            last_markers_map[series_id][0] = (epoch, value,
                                                              datapoint_version)
                    # (optimize_lookback) keep only the latest non-marker seen
                    elif epoch > last_markers_map[series_id][1][0]:
                        last_markers_map[series_id][1] = (epoch, value,
                                                          datapoint_version)

                # Check if next row exists or else fetch the next batch
                idx += 1
                if idx >= len(rows):
                    t_s = time.perf_counter()
                    rows = cursor.fetchmany(size=fetch_batch_size)
                    t_druid_fetch[cursor_name] += time.perf_counter() - t_s
                    idx = 0

        # (optimize_lookback) delayed-insert of all memorized marker and
        # non-marker data points seen in processing loop, for all seen series_id
        if optimize_lookback:
            for series_id, arr in last_markers_map.items():
                for (epoch, value, ver) in arr:
                    if epoch:
                        aggregations[series_id].process(epoch, value, ver)

        # Aggregator processing time equals total time - fetch times for all
        # cursors
        t_aggregator_process = (time.perf_counter() - t_aggregator_process -
                                sum(t_druid_fetch.values()))
        t_aggregator_complete = time.perf_counter()

        time_series_group = DruidReader._prepare_time_series_group(
            time_range, fill_func, fill_value, series_groups, aggregations,
            query_id, trace)

        t_aggregator_complete = time.perf_counter() - t_aggregator_complete

        for cursor_name in t_druid_fetch:
            fetch_time = t_druid_fetch[cursor_name]
            Telemetry.inst.registry.histogram(
                f'ReadApi.DruidReader.get_field_{cursor_name}_fetch_time',
                tag_key_values=[f"User={user}"]
            ).add(fetch_time)

            num_processed = samples_processed[cursor_name]
            Telemetry.inst.registry.meter(
                f'ReadApi.DruidReader.{cursor_name}_samples_processed',
                tag_key_values=[f"User={user}"]
            ).mark(num_processed)

        Telemetry.inst.registry.histogram(
            measurement='ReadApi.DruidReader.get_field_process_time',
            tag_key_values=[f"User={user}"]
        ).add(t_aggregator_process)
        Telemetry.inst.registry.histogram(
            measurement='ReadApi.DruidReader.get_field_complete_time',
            tag_key_values=[f"User={user}"]
        ).add(t_aggregator_complete)

        return time_series_group

    @staticmethod
    def _prepare_time_series_group(time_range: TimeRange,
                                   fill_func,
                                   fill_value,
                                   series_groups: Dict,
                                   aggregations: Dict,
                                   query_id: str,
                                   trace: bool
                                   ) -> TimeSeriesGroup:
        """

        :param time_range:
        :param series_groups:
        :param aggregations:
        :param fill_func:
        :param fill_value:
        :param query_id:
        :param trace:
        :return:
        """
        # TODO : Future optimization, all the execution can assume
        # that the points at same index share same epoch
        # so this list can be generated at the time of returning the computed
        # result to the user and we can avoid the memory and computation
        # overhead of managing these epochs
        epochs = list(time_range.epochs())
        time_series_group = TimeSeriesGroup()
        for group_name, series_ids in series_groups.items():
            time_series_group.groups[group_name] = list()
            for series_id in series_ids:
                aggregator = aggregations[series_id]
                aggregator.complete(query_id, trace)
                logger.debug("Appending series %s values to group %s: %s",
                             group_name, series_id, aggregator.buckets.values)

                # Now the aggregator is complete lets substitute any None
                # with appropriate fill values
                # This is the last place we have to do fill_value
                # from now onwards there will be no missing values
                # so downstream code doesn't have to worry about substituting
                # with Nones. However since None is a fill_value so
                # downstream code does have to handle and ignore None
                aggregated_values = list()
                prev_value = 0
                for value in aggregator.buckets.values:
                    if value is None:
                        aggregated_values.append(
                            DenseFunctions.get_fill_value(
                                prev_value, fill_func, fill_value))
                    else:
                        aggregated_values.append(value)
                    prev_value = value

                series_time_series = TimeSeries(
                    series_id,
                    list(zip(epochs, aggregated_values)),
                    time_range)
                time_series_group.groups[group_name].append(series_time_series)
        return time_series_group

    # pylint: disable-msg=W0613  # Unused argument
    @user_time_calls
    def _execute_last_value_query(
            self,
            schema: Schema,
            datasource: str,
            field: str,
            filters: Dict[str, str],
            start_epoch: int, lookback_sec: int, dimensions: List[str],
            marker_value: bool,
            # NOTE: 'user' must remain a keyword argument for the
            # user_time_calls() decorator to work
            user: str = None) -> DruidCursor:
        """
        Builds and executes last_value query
        :param datasource:
        :param field:
        :param filters:
        :param start_epoch:
        :param dimensions:
        :param marker_value: Boolean indicating whether we should specifically
        search for marker values or not
        :return:
        """
        last_value_query = DruidReader._build_last_value_query(schema,
                                                               datasource,
                                                               field,
                                                               filters,
                                                               start_epoch,
                                                               lookback_sec,
                                                               dimensions,
                                                               marker_value)
        try:
            return self.execute(last_value_query, datasource, user)
        except Exception as e:
            raise ValueError('druid last_value_query:\n\t {}\n failed with '
                             '{}'.format(last_value_query, str(e)))

    # pylint: disable-msg=W0613  # Unused argument
    @user_time_calls
    def _execute_query(self,
                       datasource: str,
                       field: str,
                       filters: Dict[str, str],
                       start_epoch: int,
                       end_epoch: int,
                       dimensions: List[str],
                       # NOTE: 'user' must remain a keyword argument for the
                       # user_time_calls() decorator to work
                       user: str = None) -> DruidCursor:
        """
        Builds and executes last_value query
        :param datasource:
        :param field:
        :param filters:
        :param start_epoch:
        :param end_epoch:
        :param dimensions:
        :return:
        """
        last_value_query = DruidReader._build_query(datasource,
                                                    field,
                                                    filters,
                                                    start_epoch,
                                                    end_epoch,
                                                    dimensions)
        try:
            return self.execute(last_value_query, datasource, user)
        except Exception as e:
            raise ValueError('druid query:\n\t {}\n failed with {}'.format(
                last_value_query, str(e)))

    @staticmethod
    def _build_dimension_string(orig_dimensions: List[str],
                                aliases: Dict[str, str]) -> str:
        """
        Generates dimension column strings while handling aliases.
        """
        dimension_string = ''
        for orig_dimension in orig_dimensions:
            if orig_dimension in aliases:
                # This dimension has an alias
                dimension_string += '%s AS %s,' % (orig_dimension,
                                                   aliases[orig_dimension])
            else:
                dimension_string += '%s,' % orig_dimension

        return dimension_string

    @staticmethod
    def _build_query(datasource: str,
                     field: str,
                     filters: Dict,
                     start_epoch: int,
                     end_epoch: int,
                     dimensions: List[str]) -> str:
        """
        Build the druid query for fetching the data
        :param datasource:
        :param field:
        :param filters:
        :param start_epoch:
        :param end_epoch:
        :param dimensions:
        :return:
        """
        filter_clause = DruidReader._get_filter_clause(filters, field)
        # we have to query all the dimensions so we can construct
        # the dense series appropriately
        if len(dimensions) > 0:
            dimension_string = DruidReader._build_dimension_string(dimensions,
                                                                   ALIASES)
        else:
            dimension_string = ''
        # Note the start epoch is not equal check, because the last_value
        # query will get us the value at start epoch
        # TODO: LIMIT needs to be better handled
        query = 'SELECT TIMESTAMP_TO_MILLIS(__time) AS ts, ' \
                '{dimensions} "{value}" ' \
                'from "{datasource}" ' \
                'where ' \
                'MILLIS_TO_TIMESTAMP({start_epoch}) < __time AND ' \
                '__time <= MILLIS_TO_TIMESTAMP({end_epoch}) AND ' \
                '{filters} ' \
                'LIMIT 10000000'.\
            format(value=VALUE,
                   dimensions=dimension_string,
                   datasource=datasource,
                   start_epoch=start_epoch*1000,
                   end_epoch=end_epoch*1000,
                   filters=filter_clause)
        return query

    @staticmethod
    def _build_last_value_query(schema: Schema,
                                datasource: str,
                                field: str,
                                filters: Dict,
                                start_epoch: int,
                                lookback_sec: int,
                                dimensions: List[str],
                                marker_value: bool) -> str:
        """
        This query fetches the starting value for all series
        :param datasource:
        :param field:
        :param filters:
        :param start_epoch:
        :param dimensions:
        :param marker_value: Boolean indicating whether we should specifically
        search for marker values or not
        :return: query
        """
        filter_clause = DruidReader._get_filter_clause(filters, field)
        # we have to query all the dimensions so we can construct
        # the dense series appropriately
        if len(dimensions) > 0:
            dimension_string = DruidReader._build_dimension_string(dimensions,
                                                                   ALIASES)
            group_by_clause = 'GROUP BY ' + ','.join(dimensions)
        else:
            dimension_string = ''
            group_by_clause = ''

        markers = [MISSING_POINT_VALUE, TOMBSTONE_VALUE]
        markers_list_str = ",".join(str(m) for m in markers)

        value_op = 'IN' if marker_value else 'NOT IN'
        value_clause = '"value" %s (%s)' % (value_op, markers_list_str)

        last_value_guaranteed_epoch = start_epoch - \
            lookback_sec

        # TODO: LIMIT needs to be better handled
        query = 'SELECT LATEST(TIMESTAMP_TO_MILLIS(__time)) AS ts, ' \
                '{dimensions} ' \
                'LATEST("{value}") AS "{value}" ' \
                'from "{datasource}" ' \
                'where ' \
                'MILLIS_TO_TIMESTAMP({segment_start_epoch}) < __time AND ' \
                '__time <= MILLIS_TO_TIMESTAMP({start_epoch}) AND ' \
                '{value_clause} AND ' \
                '{filters} ' \
                '{group_by} LIMIT 1000000'.\
            format(value=VALUE,
                   dimensions=dimension_string,
                   datasource=datasource,
                   segment_start_epoch=last_value_guaranteed_epoch*1000,
                   start_epoch=start_epoch*1000,
                   value_clause=value_clause,
                   filters=filter_clause,
                   group_by=group_by_clause)
        return query

    _FILTER_OP_MAP = {
        'and': ' AND ',
        'or': ' OR '
    }

    @staticmethod
    def _get_filter_clause(filters: dict, field: str) -> str:
        """
        Generate SQL filter clause from parsed filters
        This is reverse of moz-sql-parser
        :param filters:
        :param field:
        :return:
        """
        filter_clause = DruidReader._get_clause(filters)
        if filter_clause is None:
            filter_clause = ''

        # make sure field is not just a wild card match
        # in that case field should be dropped from filter clause
        if field == '*':
            return filter_clause

        field_clauses = []
        # We could have a set pattern in the field. The separator is ";" .
        # GraphiteParser.convert_set_patterns() will convert {a,b,c} to {a;b;c}
        words = field.strip('{').strip('}').split(";")
        for word in words:
            # Replace wildcard pattern with appropriate % symbol in Druid
            word_mod = word.replace('*', '%')
            field_op = 'LIKE' if '%' in word_mod else '='
            field_clauses += [f'{FIELD} {field_op} \'{word_mod}\'']

        field_clause_str = '({})'.format(' OR '.join(field_clauses))
        if filter_clause == '':
            filter_clause = field_clause_str
        else:
            filter_clause = field_clause_str + ' AND ' + filter_clause
        return filter_clause

    @staticmethod
    def _get_clause(filters: dict) -> str:
        """
        Generate the combined clause based on multiple key: values defined
        in the filters.
        :param filters:
        :return:
        """
        if filters is None:
            return None
        clause = ''
        for key, values in filters.items():
            clause = clause + DruidReader._parse_filter_clause(key, values)
        if clause == '':
            return None
        return clause

    @staticmethod
    def _parse_filter_clause(key: str, values: list) -> str:
        """
        Parse the key: values to a single string clause
        :param key:
        :param values:
        :return:
        """
        if key == 'literal':
            return "'{}'".format(values)
        clause_values = list()
        if isinstance(values, list):
            for value in values:
                if isinstance(value, dict):
                    clause_value = DruidReader._get_clause(value)
                    if clause_value is not None:
                        clause_values.append(clause_value)
                else:
                    clause_values.append(value)
        else:
            clause_values = [values]

        if len(clause_values) > 0:
            op = DruidReader._FILTER_OP_MAP[key] \
                if key in DruidReader._FILTER_OP_MAP else key
            # skip any time based filters
            if 'time' not in clause_values:
                if "'*'" in clause_values:
                    # * means match everything, in that case we can skip
                    # the clause
                    return ''
                # check if clause value contains a substring * pattern
                if DruidReader.is_like_clause(clause_values):
                    clause_values_mod = [clause_value.replace('*', '%')
                                         for clause_value in clause_values]
                    return '({})'.format(' LIKE '.join(clause_values_mod))
                return '({})'.format(op.join(clause_values))

        return ''

    @staticmethod
    def _get_lookback_period_sec(schema, datasource, time_range_start):
        # The lookback interval should be equal to the forced-write interval
        # used by write path at time of ingestion <= time_range_start.
        # If time_range_start is "recent enough", return the latest/current
        # force-write interval being used by write path.
        if time_range_start < FORCE_WRITE_INTERVAL_CHANGE_TS_2022_12_21_8AM_UTC:
            lookback_sec = FORCE_WRITE_INTERVAL_USED_BEFORE_2021_12_21_8AM_UTC
        else:
            lookback_sec = FORCE_WRITE_INTERVAL_CURRENT
        return min(lookback_sec, schema.get_forced_rewrite_interval(datasource))

    @staticmethod
    def is_like_clause(clause_values: List[str]) -> bool:
        """
        checks if any of the clause_values has a * in them
        if so then its a like query not an = check
        :param clause_values:
        :return: True if a value has '*' in it
        """
        for clause_value in clause_values:
            if '*' in clause_value:
                return True
        return False
