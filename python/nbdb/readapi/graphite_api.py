"""
Graphite API module
"""

import json
import logging
import math
from typing import List, Dict, Tuple

from nbdb.common.data_point import FIELD
from nbdb.common.telemetry import Telemetry
from nbdb.common.metric_parsers import TOKEN_TAG_PREFIX, TOKEN_COUNT, \
    CLUSTERED_FLAT_METRIC_PREFIX
from nbdb.readapi.druid_reader import DruidReader
from nbdb.readapi.GraphiteParser import GraphiteParser
from nbdb.readapi.graphite_functions_provider import GraphiteFunctionsProvider
from nbdb.readapi.graphite_response import GraphiteResponse, NumpyEncoder
from nbdb.readapi.query_cache import QueryCache, QueryCacheEntry, \
    QueryCacheResult
from nbdb.readapi.time_series_response import TimeRange, TimeSeries
from nbdb.readapi.time_series_response import TimeSeriesGroup
from nbdb.readapi.sql_parser import SqlParser
from nbdb.schema.schema import Schema, BATCH_DATASOURCE_PREFIX
from werkzeug.datastructures import ImmutableMultiDict
from pyformance import meter_calls, time_calls

logger = logging.getLogger(__name__)


class GraphiteApi: # pylint: disable=too-few-public-methods
    """
    Parses and executes a graphite api
    """
    def __init__(self, schema: Schema, query: ImmutableMultiDict,
                 min_interval: int, user: str):
        """
        Query is the form submitted to the render api
        {
            'target': 'internal',
            'from': '-6h',
            'until': 'now',
            'format', 'json',
            'maxDataPoints': '563'
        }
        :param query:
        :param min_interval: minimum interval allowed
        """
        self.schema = schema
        self.query = query
        self.user = user
        targets = query.getlist('target')
        logger.info('GraphiteApi.__init__: query: %s, targets: %s',
                    json.dumps(query), '|'.join(targets))
        self.parsers: List[GraphiteParser] = list()
        for target in targets:
            self.parsers.append(GraphiteParser(target))
        start_epoch = SqlParser.time_str_parser(query['from'])
        end_epoch = SqlParser.time_str_parser(query.get('until', 'now'))
        # If maxDataPoints is not provided, we use a default value assuming 10m
        # granularity
        default_max_datapoints = int(
            math.ceil(float(end_epoch - start_epoch)/600))
        max_datapoints = int(query.get('maxDataPoints',
                                       default_max_datapoints))
        self.computed_interval = int((end_epoch - start_epoch) / max_datapoints)
        self.start_epoch = start_epoch
        self.end_epoch = end_epoch

    @time_calls
    @meter_calls
    def execute_graphite(self) -> GraphiteResponse:
        """
        Execute the multi target query and returns the result
        :return:
        """
        responses: List = list()
        for parser in self.parsers:
            # Figure out the right time range to be used for query.
            # Make sure interval is atleast equal to min_interval or a multiple
            # of min_interval. min_interval is 10 mins for all series except
            # those which have sparseness disabled
            interval, sparseness_disabled = GraphiteApi._best_fit_interval(
                parser, self.schema, self.computed_interval,
                self.schema.MIN_STORAGE_INTERVAL)
            time_range = TimeRange.aligned_time_range(self.start_epoch,
                                                      self.end_epoch, interval)

            graphite_response = self._execute_graphite(self.schema, parser,
                                                       time_range,
                                                       sparseness_disabled)
            if parser.trace:
                logger.info('[Trace: %s]: GraphiteApi.execute_graphite:'
                            ' response=%s',
                            parser.query_id,
                            json.dumps(graphite_response.response,
                                       cls=NumpyEncoder))
            responses.extend(graphite_response.response)
        logger.info('GraphiteApi.execute_graphite: query: %s responses: %d',
                    json.dumps(self.query), len(responses))
        return GraphiteResponse(responses)

    @staticmethod
    def find_sparseness_config(args,
                               schema: Schema,
                               field_sparseness_disabled: Dict[str, bool],
                               field_interval: Dict[str, int]) -> None:
        """
        Recursively find fields in the parse tree and update info. Info
        includes whether sparseness has been disabled and if yes, the expected
        series storage interval
        """
        for arg in args:
            if 'func' in arg:
                # Recurse through the function arguments and update info
                GraphiteApi.find_sparseness_config(arg['args'], schema,
                                                   field_sparseness_disabled,
                                                   field_interval)
            if 'field' in arg:
                # This is a field
                # Construct tags from each field
                _, field, _, filters = \
                    GraphiteApi._convert_flat_field_to_filters(arg[FIELD])

                tags = dict()
                SqlParser.extract_tag_conditions_from_filters(filters, tags)
                tags[FIELD] = field

                # Check if sparseness has been disabled for the series being
                # queried and if so, find the expected storage granularity
                storage_interval = schema.get_sparseness_disabled_interval(tags)
                sparseness_disabled = storage_interval is not None
                field_sparseness_disabled[arg['field']] = sparseness_disabled
                if sparseness_disabled:
                    field_interval[arg['field']] = storage_interval

    @staticmethod
    def _best_fit_interval(parser: GraphiteParser, schema: Schema,
                           computed_interval: int,
                           min_interval: int) -> Tuple[int, bool]:
        """
        Select an interval that is atleast min_interval or a multiple
        of min_interval.

        Also checks if query target has any series with sparseness disabled. If
        so, we adjust the interval based on the expected reporting interval for
        such series.

        :param computed_interval: Interval computed using maxDataPoints
        :param min_interval: Minimum interval possible, equal to the storage
        granularity of the underlying series

        :return: Chosen interval and whether sparseness is disabled
        """
        # Find all fields in the parse tree and check if they have sparseness
        # disabled. Additionally if they have sparseness disabled, we also
        # determine the expected storage interval
        field_sparseness_disabled = {}
        field_interval = {}
        GraphiteApi.find_sparseness_config(parser.args, schema,
                                           field_sparseness_disabled,
                                           field_interval)

        # Make sure that all fields in the query either have sparseness
        # disabled or enabled. We do not allow grouping multiple fields with
        # sparseness enabled & disabled in the same query
        if len(set(field_sparseness_disabled.values())) != 1:
            raise ValueError(
                "Graphite query %s trying to group series together "
                "series with sparseness enabled & disabled" % parser.target)

        if len(set(field_interval.values())) > 1:
            raise ValueError(
                "Graphite query %s trying to group multiple "
                "sparseness-disabled series with different storage "
                "intervals" % parser.target)

        # At this point, either all series have sparseness disabled or all
        # of them have it enabled. Checking for the first series is good
        # enough to figure out if sparseness is disabled
        sparseness_disabled = (list(field_sparseness_disabled.values())[0] if
            parser.sparseness_disabled == -1 else parser.sparseness_disabled)

        if sparseness_disabled:
            # Sparseness is disabled. Use series storage interval as the min
            # interval
            min_interval = list(field_interval.values())[0]

        if computed_interval < min_interval:
            # Make sure interval is atleast 10 mins or the series storage
            # interval
            return min_interval, sparseness_disabled

        # Make sure interval is a multiple of 10 mins or the series storage
        # interval
        return int(math.ceil(float(computed_interval)/min_interval) *
                   min_interval), sparseness_disabled

    @time_calls
    @meter_calls
    def _execute_graphite(self,
                          schema: Schema,
                          parser: GraphiteParser,
                          time_range: TimeRange,
                          sparseness_disabled: bool) -> GraphiteResponse:
        """
        Prepares and returns a graphite api response
        :param parser:
        :return:
        """
        Telemetry.inst.registry.histogram(
            measurement='ReadApi.graphite_api.points_requested',
            tag_key_values=[f"User={self.user}"]
        ).add(time_range.points())

        if QueryCache.inst is None or parser.disable_cache:
            logger.info('graphite_api.query_cache disabled')
            return GraphiteResponse.create_response(
                self.execute_internal(schema, parser, time_range,
                                      False, sparseness_disabled))

        # We have two forms of caching:
        # a. Results caching: Results for an entire target query are cached. We
        # then try to fetch data for missing time ranges, and blindly combine
        # these results with the existing cached results.
        #
        # b. Raw data caching: In this case, we do not cache results for a
        # target query. We instead cache raw data for each "field" or series
        # pattern (eg. a.b.*). Raw data is fetched for missing time ranges, and
        # combined with previously cached raw data.
        #
        # For most queries, results caching works fine and helps Read API
        # save compute cycles by not needing to run any Graphite functions.
        #
        # However for queries containing functions like countSeries() or
        # nPercentile(), results caching leads to incorrect results. Eg. If
        # nPercentile() is used, you can't blindly combine results for two time
        # ranges [X, Y] and [Y, Z].
        #
        # For such functions, we use raw data caching
        cache_raw = GraphiteApi.results_cache_invalidating_funcs_exist(
            parser.args)

        # Lookup the query cache first
        cache_key = self._cache_key(schema, parser.target, time_range)
        cache_entries = None
        if not cache_raw:
            if parser.repopulate_cache:
                # User has requested clearing of any previously existing cache
                # entries for the query, and asked us to repopulate with the
                # fresh set of results
                QueryCache.inst.clear(cache_key)
                cache_entries = None
            else:
                cache_entries = QueryCache.inst.get(cache_key, self.user)

        if cache_raw or cache_entries is None:
            if parser.trace:
                if cache_entries is None:
                    logger.info('[Trace: %s]: GraphiteApi: query_cache_miss '
                                'for %s', parser.query_id, cache_key)
                else:
                    # We are using raw data caching
                    logger.info('[Trace: %s]: GraphiteApi: skipping results '
                                'cache because of use of '
                                'invalidating function for %s',
                                parser.query_id, cache_key)
            # we got a total miss
            result = self.execute_internal(self.schema, parser, time_range,
                                           cache_raw, sparseness_disabled)
            if not cache_raw:
                QueryCache.inst.set(cache_key,
                                    [QueryCacheEntry(time_range, result)],
                                    self.user, trace=parser.trace,
                                    trace_id=parser.query_id)
            return GraphiteResponse.create_response(result)

        # Prune the results of the cached results
        query_cache_result = QueryCache.inst.prune(cache_entries, time_range,
                                                   user=self.user)
        if not query_cache_result.missed_ranges:
            # No missing ranges. Total hit
            return GraphiteResponse.create_response(
                query_cache_result.cached_results)

        # We have a partial hit, fetch the missing ranges and merge
        logger.info('Cache miss: %s', ','.join(
            [str(r) for r in query_cache_result.missed_ranges]))
        points_to_fetch = 0

        for missing_range in query_cache_result.missed_ranges:
            points_to_fetch += missing_range.points()
            # For certain functions, we require more points than the missing
            # time range will fetch. Eg. nonNegativeDerivative() needs one more
            # point to compute derivatives
            missing_range = GraphiteApi.adjust_time_range_if_needed(
                missing_range, parser)

            # Fetch the missing range
            range_result = self.execute_internal(schema, parser, missing_range,
                                                 cache_raw,
                                                 sparseness_disabled)
            new_entry = QueryCacheEntry(missing_range, range_result)
            cache_entries = QueryCache.inst.merge(cache_entries, new_entry,
                                                  self.user)

        query_cache_result: QueryCacheResult = \
            QueryCache.inst.prune(cache_entries, time_range, user=self.user)
        QueryCache.inst.set(cache_key, cache_entries, self.user,
                            trace=parser.trace, trace_id=parser.query_id)

        Telemetry.inst.registry.histogram(
            measurement='ReadApi.graphite_api.points_to_fetch',
            tag_key_values=[f"User={self.user}"]
        ).add(points_to_fetch)

        return GraphiteResponse.create_response(
            query_cache_result.cached_results)

    @staticmethod
    def adjust_time_range_if_needed(time_range, parser):
        """
        Left shift time range if certain functions which need more data exist.

        Certain functions like nonNegativeDerivative() can require more data
        than what will be normally fetched to compute the right results.
        """
        if 'nonNegativeDerivative' in parser.target:
            # We cannot compute with 1 point, nonNegativeDerivative and
            # similar functions need the previous epoch as well
            # we will expand the missing_range by 1 point in past
            return TimeRange(time_range.start - time_range.interval,
                             time_range.end, time_range.interval)
        return time_range

    # Visible for testing
    def execute_internal(self,
                         schema: Schema,
                         parser: GraphiteParser,
                         time_range: TimeRange,
                         cache_raw: bool,
                         sparseness_disabled: bool) -> \
            Dict[str, TimeSeriesGroup]:
        """
        :param parser: Single graphite target
        :param time_range: TimeRange over which the query should be executed
        :return:
        """
        if parser.trace:
            logger.info('[Trace: %s]: GraphiteApi.execute: %s',
                        parser.query_id, time_range)
        # recursively execute the methods
        result: List[List[TimeSeries]] = self._execute_args(
            schema, parser.args, time_range, cache_raw, sparseness_disabled,
            parser.repopulate_cache, parser.trace, parser.lookback_sec,
            parser.optimize_lookback, parser.query_id)

        if len(result) <= 0:
            return dict()
        if len(result) > 1:
            raise ValueError('Unexpected state: expected only one '
                             'time series list. Found: {}'.format(len(result)))
        tsg_result: Dict[str, TimeSeriesGroup] = dict()
        tsg_result['_'] = TimeSeriesGroup({'_': result[0]})
        return tsg_result

    # pylint: disable-msg=R0913  # Too many arguments
    def _execute_args(self,
                      schema: Schema,
                      args,
                      time_range: TimeRange,
                      cache_raw: bool,
                      sparseness_disabled: bool,
                      repopulate_cache: bool,
                      trace: bool,
                      lookback_sec: int,
                      optimize_lookback: int,
                      query_id: str) -> List:
        """

        :param args:
        :param time_range
        :param trace
        :param query_id
        :return:
        """
        arg_results = list()
        for arg in args:
            if 'func' in arg:
                arg_results.append(self._execute_func(
                    schema, arg, time_range, cache_raw, sparseness_disabled,
                    repopulate_cache, trace, lookback_sec, optimize_lookback,
                    query_id))
            elif 'field' in arg:
                arg_results.append(self._execute_field(
                    schema, arg, time_range, cache_raw, sparseness_disabled,
                    repopulate_cache, trace, lookback_sec, optimize_lookback,
                    query_id))
            elif 'literal' in arg:
                arg_results.append(arg['literal'])
            else:
                raise ValueError('Unsupported argument: ' + json.dumps(arg))
        return arg_results

    # pylint: disable-msg=R0913  # Too many arguments
    def _execute_field(self,
                       schema: Schema,
                       field_exp: dict,
                       time_range: TimeRange,
                       cache_raw: bool,
                       sparseness_disabled: bool,
                       repopulate_cache: bool,
                       trace: bool,
                       lookback_sec: int,
                       optimize_lookback: int,
                       query_id: str) -> List[TimeSeries]:
        """
        Fetch "field" (ie. sparse series) from Druid and convert to dense form.
        If cache_raw is True, we will be caching the output from DruidReader.
        So first we will check if any cached results exist, and fetch only the
        missing data.

        :param field_exp:
        :param time_range:
        :param cache_raw:
        :param sparseness_disabled:
        :param repopulate_cache:
        :param trace:
        :param query_id:
        :return:
        """
        if not cache_raw:
            # Raw results are not cached. Fetch data
            tsgr = self._fetch_series_from_druid(schema, field_exp, time_range,
                                                 sparseness_disabled, trace,
                                                 lookback_sec,
                                                 optimize_lookback,
                                                 query_id)
        else:
            # Raw results are being cached. Check cache
            cache_key = self._cache_key(schema, field_exp[FIELD], time_range)
            query_cache_result = None
            if repopulate_cache:
                # User has requested clearing of any previously existing cache
                # entries for the query, and asked us to repopulate with a
                # fresh set of results
                QueryCache.inst.clear(cache_key)
            else:
                # Fetch from cache
                cache_entries = QueryCache.inst.get(cache_key, self.user)
                if cache_entries is not None:
                    query_cache_result = QueryCache.inst.prune(
                        cache_entries, time_range, user=self.user)

            if query_cache_result is None:
                # Total miss. Fetch data and update cache
                tsgr = self._fetch_series_from_druid(schema, field_exp,
                                                     time_range,
                                                     sparseness_disabled,
                                                     trace,
                                                     lookback_sec,
                                                     optimize_lookback,
                                                     query_id)
                new_entry = QueryCacheEntry(time_range, {'_': tsgr})
                QueryCache.inst.set(cache_key, [new_entry], self.user, trace,
                                    query_id)

            elif not query_cache_result.missed_ranges:
                # Total hit
                tsgr = query_cache_result.cached_results['_']

            else:
                # We have a partial hit, fetch the missing ranges and merge
                for missing_range in query_cache_result.missed_ranges:
                    tsgr = self._fetch_series_from_druid(schema, field_exp,
                                                         missing_range,
                                                         sparseness_disabled,
                                                         trace,
                                                         lookback_sec,
                                                         optimize_lookback,
                                                         query_id)
                    new_entry = QueryCacheEntry(missing_range, {'_': tsgr})
                    cache_entries = QueryCache.inst.merge(cache_entries,
                                                          new_entry, self.user)

                # We prune the merged cache entries to the required time range
                query_cache_result: QueryCacheResult = \
                    QueryCache.inst.prune(cache_entries, time_range,
                                          user=self.user)
                # We also update the backend with the merged cache entries
                QueryCache.inst.set(cache_key, cache_entries, self.user,
                                    trace=trace, trace_id=query_id)
                tsgr = query_cache_result.cached_results['_']

        if len(tsgr.groups) <= 0:
            return []

        # we do not do any group by operation in druid_reader
        # group by is based on the graphite functions so we just
        # pass the default list of all series matched by the field pattern
        tsl: List[TimeSeries] = tsgr.groups['_']
        return tsl

    # pylint: disable-msg=R0913  # Too many arguments
    def _fetch_series_from_druid(self,
                                 schema: Schema,
                                 field_exp: dict,
                                 time_range: TimeRange,
                                 sparseness_disabled: bool,
                                 trace: bool,
                                 lookback_sec: int,
                                 optimize_lookback: int,
                                 query_id: str) -> TimeSeriesGroup:
        """
        Fetch sparse series from Druid and convert to dense form.

        :param field_exp:
        :param time_range:
        :param sparseness_disabled:
        :param trace:
        :param query_id:
        :return:
        """
        measurement_prefix, field, cluster_id, filters = \
            GraphiteApi._convert_flat_field_to_filters(field_exp[FIELD])

        tags = dict()
        SqlParser.extract_tag_conditions_from_filters(filters, tags)
        tags[FIELD] = field
        if trace:
            logger.info('[Trace: %s] tags=%s cluster_id=%s '
                        'measurement_prefix=%s', query_id, tags, cluster_id,
                        measurement_prefix)
        datasource = DruidReader.inst.get_datasource(schema,
                                                     measurement_prefix,
                                                     tags,
                                                     cluster_id,
                                                     time_range.interval,
                                                     protocol='graphite',
                                                     trace=trace,
                                                     query_id=query_id)

        tsgr = DruidReader.inst.get_field(schema,
                                          datasource,
                                          field,
                                          filters,
                                          time_range,
                                          [],
                                          query_id,
                                          trace,
                                          GraphiteFunctionsProvider.fill_func,
                                          GraphiteFunctionsProvider.fill_value,
                                          True,
                                          sparseness_disabled,
                                          lookback_sec,
                                          optimize_lookback,
                                          user=self.user
                                          )
        if trace:
            tsgr.log('[Trace: {}]: get_field({})'.format(query_id,
                                                         field))

        if len(tsgr.groups) > 1:
            raise ValueError('Expected only default group from DruidReader.'
                             'Found: {} groups instead'
                             .format(','.join(tsgr.groups.keys())))
        return tsgr

    # pylint: disable-msg=R0913  # Too many arguments
    def _execute_func(self,
                      schema: Schema,
                      func,
                      time_range: TimeRange,
                      cache_raw: bool,
                      sparseness_disabled: bool,
                      repopulate_cache: bool,
                      trace: bool,
                      lookback_sec: int,
                      optimize_lookback: int,
                      query_id: str) -> List[TimeSeries]:
        """

        :param func:
        :param time_range
        :param trace
        :param query_id
        :return:
        """
        arg_results = self._execute_args(
            schema, func['args'], time_range, cache_raw, sparseness_disabled,
            repopulate_cache, trace, lookback_sec, optimize_lookback, query_id)
        # check if its a graphite function
        func_name = func['func']
        func_impl = getattr(GraphiteFunctionsProvider, func['func'], 'unknown')
        if func_impl == 'unknown':
            raise ValueError('Unsupported func: {}'.format(func_name))

        # The first argument must always be time_range
        arg_results.insert(0, time_range)
        if trace:
            logger.info('[Trace: %s] Func %s, arg_results: %s', query_id, func,
                        arg_results)
        tslr = func_impl(*arg_results)
        if trace:
            TimeSeriesGroup.log_tsl('[Trace: {}]: {}'.format(query_id,
                                                             func),
                                    tslr)
        return tslr

    @staticmethod
    def results_cache_invalidating_funcs_exist(args) -> bool:
        """
        Check if target contains any functions that would invalidate the use of
        results cache. We will cache raw data in such cases.

        :param args:
        :param time_range
        :param trace
        :param query_id
        :return:
        """
        for arg in args:
            if 'func' in arg:
                func_name = arg['func']
                if func_name in ['countSeries', 'countNewSeries',
                                 'highest', 'highestAverage',
                                 'highestCurrent', 'highestMax',
                                 'currentAbove', 'averageAbove',
                                 'maximumAbove', 'nPercentile',
                                 'removeBelowPercentile',
                                 'removeAbovePercentile', 'integral',
                                 'limit', 'summarize', 'movingAverage']:

                    # Merging cached results from two different time ranges
                    # leads to incorrect results if any of these functions are
                    # being used.
                    return True
                if func_name == 'removeEmptySeries':
                    # removeEmptySeries(XXX, xFilesFactor) where xFilesFactor
                    # is not equal to 0.0. It is not safe to use the query
                    # cache results in this case.
                    #
                    # For example, say we ran removeEmptySeries(X, 0.5) on the
                    # series X which has the following datapoints for the time
                    # range [0, 30]:
                    # [(0, None), (10, None), (20, 100.0), (30, 100.0)]
                    # The series will not be removed because it has atleast
                    # 50% non-NULL datapoints.
                    #
                    # Now if the same query comes in for the time range
                    # [0, 20], the right result is an empty list []. This is
                    # because there are 1/3 non-NULL datapoints in the time
                    # range [0, 20] which is not sufficient to meet the 50%
                    # non-NULL criteria.
                    #
                    # But suppose if we had fetched the old cached result for
                    # the time range [0, 30], and pruned it to the time range
                    # [0, 20], we would end up returning the incorrect result:
                    # (0, None), (10, None), (20, 100.0)]
                    if (len(arg['args']) > 1 and
                            'literal' in arg['args'][1] and
                            arg['args'][1]['literal'] != 0.0):
                        return True

                # Some other function. We have to recursively look at its
                # arguments and confirm that none of them use any functions
                # which invalidate the cache
                if GraphiteApi.results_cache_invalidating_funcs_exist(
                        arg['args']):
                    return True

        return False

    @staticmethod
    def _convert_flat_field_to_filters(field: str) \
            -> Tuple[str, str, str, Dict]:
        """
        We parse the first three tokens as tags to reduce the impact of
        LIKE query
        :param field:
        :return:
        """
        tokens = field.split('.')
        filters = {'and': []}

        for i in range(len(tokens) - 1):
            if tokens[i].startswith('{') and tokens[i].endswith('}'):
                # This is a set pattern. Eg. {InfluxDB,Telegraf}
                # We need to convert this to multiple OR clauses
                clauses = []
                # The separator is ";" . GraphiteParser.convert_set_patterns()
                # will convert {a,b,c} to {a;b;c}
                words = tokens[i].strip('{').strip('}').split(";")
                for word in words:
                    clauses.append({'=': [TOKEN_TAG_PREFIX + str(i),
                                          {'literal': word}]})
                filters['and'].append({'or': clauses})
            else:
                filters['and'].append({'=': [TOKEN_TAG_PREFIX + str(i),
                                             {'literal': tokens[i]}]})
        # add the dot count tag, to make sure we don't match fields
        # that share the same prefix
        filters['and'].append({'=': [TOKEN_COUNT,
                                     {'literal': str(len(tokens))}]})

        if field.startswith(CLUSTERED_FLAT_METRIC_PREFIX):
            # Last token is sub-field
            sub_field = tokens[-1]
            cluster_id = tokens[1]
            # We treat the metric after taking out the clusters prefix, cluster
            # ID and node name to be the measurement for Graphite. Measurement
            # also includes the last token
            measurement_prefix = '.'.join(tokens[3:])
        else:
            # Not clusters.* metrics. We treat the whole flat metric as the
            # measurement prefix, including the last token which gets treated
            # as the field
            measurement_prefix = '.'.join(tokens)
            # Last token is sub-field
            sub_field = tokens[-1]
            # There is no concept of a cluster ID
            cluster_id = None
        return measurement_prefix, sub_field, cluster_id, filters

    @staticmethod
    def _cache_key(schema: Schema, target: str, time_range: TimeRange) -> str:
        """
        Create a cache key that is independent of time range
        :param target
        :param time_range
        :return: cache key str
        """
        schema_prefix = \
            BATCH_DATASOURCE_PREFIX if schema.batch_mode else ""
        # By including the schema_prefix in the key, we logically isolate
        # the caches for live and batch datasources. This ensures that we
        # don't mix live and batch data for the same series. While the raw
        # source of the live/batch will be the same given a series, the
        # data we end up writing in Druid might be different due to different
        # sparseness algorithms.
        #
        # This does mean that we can double cache data for series which are
        # queried from both the live and batch datasources. We should revisit
        # this when we start auto-merging series from live and batch
        # datasources.
        key = '{schema_prefix}.{target}.{interval}.{fill_func}'.\
            format(schema_prefix=schema_prefix,
                   target=target,
                   interval=time_range.interval,
                   fill_func=GraphiteFunctionsProvider.fill_func
                   )
        return key
