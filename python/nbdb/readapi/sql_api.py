"""
SqlAPi and related exceptions
"""
import logging
from concurrent.futures import Future
from typing import List, Dict
from pyformance import meter_calls, time_calls

from nbdb.common.data_point import FIELD
from nbdb.common.metric_parsers import CLUSTER_TAG_KEY
from nbdb.common.telemetry import Telemetry
from nbdb.common.thread_pools import ThreadPools
from nbdb.readapi.druid_reader import DruidReader
from nbdb.readapi import SparseSeriesReaderInterface
from nbdb.readapi.dense_functions import DenseFunctions
from nbdb.readapi.query_cache import QueryCache, QueryCacheEntry
from nbdb.readapi.sql_parser import SqlParser
from nbdb.readapi.time_series_response import TimeSeriesGroup, \
    InfluxSeriesResponse, TimeRange
from nbdb.schema.schema import Schema

logger = logging.getLogger(__name__)


class SqlApi:
    """
    Parses and executes a SQL statement
    Note this uses multiple threads to compute various expressions
    independently in parallel
    """
    # pylint: disable=too-few-public-methods
    # Might refactor this class to swap out the group by and filters

    def __init__(self, schema: Schema, query: str,
                 user: str, trace: bool = False):
        """
        Parses the SQL query and raises a SyntaxError for any parsing related
        failure
        Validates that only supported queries are parsed
        :param query
        :param trace: If true then logs the full execution of the query
        :param user: Username
        :return: parsed AST
        """
        # Store the original query for reference and logging
        self.schema = schema
        self._query = query
        self._query_id = str(hash(query))
        self.user = user
        self.trace = trace
        self.parser = SqlParser(query)
        if trace:
            logger.info('[Trace: %s] query : %s', self._query_id, query)
            logger.info('[Trace: %s] parser: %s',
                        self._query_id, str(self.parser))

    @time_calls
    @meter_calls
    def execute_influx(self, epoch_precision: str, nocache: bool = False,
                       repopulate_cache: bool = False) -> \
            InfluxSeriesResponse:
        """
        execute the query and return the Influx response object
        epoch_precision: The precision level (ms, s) at which to return
        the time epoch. If None , the time should be formatted as isoformat
        date string
        :param epoch_precision
        :param nocache
        :param repopulate_cache
        :return:
        """
        results = self.execute_internal(nocache, repopulate_cache)
        return InfluxSeriesResponse.create_response(epoch_precision,
                                                    results)

    # Visible for testing
    def execute_internal(
            self, nocache: bool = False,
            repopulate_cache: bool = False) -> Dict[str, TimeSeriesGroup]:
        """
        :param nocache
        :param repopulate_cache
        :return:
        """
        full_range = TimeRange(self.parser.start_epoch,
                               self.parser.end_epoch,
                               self.parser.interval)

        Telemetry.inst.registry.histogram(
            'ReadApi.sql_api.points_requested',
            tag_key_values=[f"User={self.user}"]
        ).add(full_range.points())
        if QueryCache.inst is None or nocache:
            return SqlApi._wait(self._execute_for_range(full_range))
        cache_key = self.parser.key()

        # Lookup the query cache first
        cache_entries = None
        if repopulate_cache:
            # User has requested clearing of any previously existing cache
            # entries for the query, and asked us to repopulate with the fresh
            # set of results
            QueryCache.inst.clear(cache_key)
            cache_entries = None
        else:
            cache_entries = QueryCache.inst.get(cache_key, self.user)

        if cache_entries is None:
            # we got a total miss
            result = SqlApi._wait(self._execute_for_range(full_range))
            QueryCache.inst.set(cache_key,
                                [QueryCacheEntry(full_range, result)],
                                self.user, trace=self.trace,
                                trace_id=self._query_id)
            return result

        # Prune the results of the cached results
        query_cache_result = QueryCache.inst.prune(cache_entries, full_range,
                                                   user=self.user)
        if not query_cache_result.missed_ranges:
            # No missing ranges. Total hit
            return query_cache_result.cached_results

        # We have a partial hit, fetch the missing ranges and merge
        logger.debug('Cache miss: %s', ','.join(
            [str(r) for r in query_cache_result.missed_ranges]))
        points_to_fetch = 0

        for missing_range in query_cache_result.missed_ranges:
            points_to_fetch += missing_range.points()
            range_result = SqlApi._wait(self._execute_for_range(missing_range))
            new_entry = QueryCacheEntry(missing_range, range_result)
            # Merge missing range with previously cached results
            cache_entries = QueryCache.inst.merge(cache_entries, new_entry,
                                                  self.user)

        query_cache_result = QueryCache.inst.prune(cache_entries, full_range,
                                                   user=self.user)
        QueryCache.inst.set(cache_key, cache_entries, self.user,
                            trace=self.trace, trace_id=self._query_id)

        Telemetry.inst.registry.histogram(
            'ReadApi.sql_api.points_to_fetch',
            tag_key_values=[f"User={self.user}"]
        ).add(points_to_fetch)

        return query_cache_result.cached_results

    @time_calls
    @meter_calls
    def _execute_for_range(self, time_range: TimeRange) -> Dict:
        """
        execute the SQL Query
        :param time_range
        :return: Dictionary of field_name to TimeSeriesGroup
        """
        results = {}
        parallelize = len(self.parser.expressions) > 1
        for expression in self.parser.expressions:
            results.update(self._evaluate_future(expression,
                                                 parallelize,
                                                 time_range
                                                 ))
        return results

    @staticmethod
    @time_calls
    @meter_calls
    def _wait(results) -> Dict[str, TimeSeriesGroup]:
        """

        :param results:
        :return:
        """
        # Wait for the futures
        for result_name, result_value in results.items():
            if isinstance(result_value, Future):
                results[result_name] = result_value.result()

        return results

    def _traverse_and_extract_field_and_funcs(
            self, expression, field: str,
            funcs_list: List[str]) -> (str, List[str]):
        """
        Recursively traverse the expression tree and extract the first field &
        the functions used.

        We then use the field & list of functions to construct the series name
        in the Influx response.
        """
        if isinstance(expression, str):
            # String expression means a field
            if not field:
                # We use the first field found as the return value field
                field = expression
            return field, funcs_list

        if not isinstance(expression, dict):
            # Literal integer most likely
            return field, funcs_list

        for func in expression:
            args = expression[func]
            if func not in ['literal', '+', '-', '/', '*']:
                # InfluxQL responses do not consider arithmetic operators as
                # functions
                funcs_list += [func]

            if isinstance(args, str) and not field:
                # We use the first field found as the return value field
                field = arg
            elif isinstance(args, list):
                # Args is a list
                for arg in args:
                    field, funcs_list = \
                        self._traverse_and_extract_field_and_funcs(
                            arg, field, funcs_list)

        return field, funcs_list

    def _evaluate_future(self,
                         expression: str,
                         parallelize: bool,
                         time_range: TimeRange) -> \
            Dict[str, TimeSeriesGroup]:
        """
        Evaluates the result field expression
        expression is of form:
        {
          name: 'result_name' , // This is the as clause
          value: string or dictionary
        }
        When value is a string, then it refers to some constant or field
        When value is dictionary, then its a function and takes the form
        value : {
            "func_name" : string or list
        }
        When the func_name refers to a string, then its a single argument to
        func
        When the func_name refers to a list, then its a list of arguments to
        func.

        Note the arguments can be nested functions themselves

        Examples:
        # Simple no name example, refers to a single string literal
        {
            "value": "user"
        }
        # A named single argument function
        {
            "name" : "f1",
            "value" : { "mean": "cpu" }
        }
        # A named multi-argument function
        {
            "name": "f2",
            "value": {"div": ["cpu", 10]}
        }
        Complex nested expression
        {
            "name": "f1",
            "value": {
              "add": [ {
                         "div": [{
                                    "add": [{"mean": "cpu"}, 10]
                                },
                                {
                                    "sub": [{"mul": [{"sum": "user"}, 100]},
                                            20]
                                }]
                        },
                        10]
            }
        }

        Assumptions for parsing:
            1) Standard arithmetic functions supported are
               add, sub, mul, div
            2) Aggregator functions supported
                mean, sum, ... see Aggregator factory for a complete list
            3) String literals are assumed to be fields
        :param expression:
        :param parallelize: if true the call is non blocking
        :param time_range:
        :return: dictionary with single entry for the
         expression name to TimeSeriesGroup
        """
        # Traverse the expression creating the futures chain
        value = expression['value']
        if 'name' in expression:
            result_name = expression['name']
        else:
            # We traverse the expression to determine the appropriate series
            # name to be returned in our InfluxQL response
            field, funcs_list = self._traverse_and_extract_field_and_funcs(
                expression['value'], '', [])
            if funcs_list:
                result_name = '%s_%s' % (field, '_'.join(funcs_list))
            else:
                result_name = field

        if isinstance(value, dict):
            for func in value:
                args = value[func]
                # special case for literal
                if func == 'literal':
                    tsg = TimeSeriesGroup.create_default_group_single_series(
                        args, time_range)
                    return {result_name: tsg}
                tsg = self._evaluate_func_future(func,
                                                 args,
                                                 parallelize,
                                                 time_range)
                return {result_name: tsg}

        # Check if value is not a string
        if not isinstance(value, str):
            tsg = TimeSeriesGroup.create_default_group_single_series(
                value, time_range)
            return {result_name: tsg}

        # value is a field and we return the down-sampled values of the field
        tsg = self._get_field_future(value, parallelize, time_range)
        return {result_name: tsg}

    def _evaluate_func_future(self, func_name: str,
                              args: list,
                              parallelize: bool,
                              time_range: TimeRange) -> Future:
        """
        First evaluates the arguments, collects their futures
        Then calls get_func_future with argument futures
        :param func_name:
        :param args:
        :param parallelize:
        :return:
        """
        if isinstance(args, dict):
            # The arguments represent a sub function, recursively evaluate
            # the sub function
            sub_func = next(iter(args))
            arg_future = self._evaluate_func_future(sub_func,
                                                    args[sub_func],
                                                    parallelize,
                                                    time_range)
            if parallelize:
                return self._get_func_future(func_name, [arg_future])
            return self._get_func_result(func_name, [arg_future])

        if not isinstance(args, list):
            args = [args]

        # we have to evaluate each argument first
        arg_futures = []
        for arg in args:
            arg_futures.append(self._get_arg_future(arg,
                                                    parallelize,
                                                    time_range))

        if parallelize:
            return self._get_func_future(func_name, arg_futures)
        return self._get_func_result(func_name, arg_futures)

    def _get_arg_future(self,
                        arg,
                        parallelize: bool,
                        time_range: TimeRange):
        """
        Evaluate the argument and return a future of its result
        :param arg: argument, can be a method, a constant or a field
        :param parallelize:
        :param time_range:
        :return: Future of TimeSeriesGroup or TimeSeriesGroup
        """
        if isinstance(arg, dict):
            # Arg represents a function
            func_name = next(iter(arg))
            if func_name == 'literal':
                # literal values are just values
                return TimeSeriesGroup.create_default_group_single_series(
                    arg[func_name], time_range)
            return self._evaluate_func_future(func_name,
                                              arg[func_name],
                                              parallelize,
                                              time_range)
        if isinstance(arg, str):
            # special string None
            if arg == 'none':
                return TimeSeriesGroup.create_default_group_single_series(
                    None, time_range)
            # We assume this is a field, and we need to fetch the time series
            # corresponding to this field
            return self._get_field_future(arg, parallelize, time_range)

        # Its just a regular argument, no work needs to be done on this.
        # We should wrap this in a future just to keep return
        # type and downstream code consistent
        # In an ideal world we should just be able to create a Future and
        # set its result, but concurrent.futures doesnt provide a way to do
        # that
        return TimeSeriesGroup.create_default_group_single_series(arg,
                                                                  time_range)

    def _get_func_future(self, func_name: str, arg_futures) -> Future:
        """
        Gets a future for the execution of the func_name with arg_futures
        :param func_name:
        :param arg_futures:
        :return: future for the func_name applied to arg_futures
        """
        return ThreadPools.inst.sql_api_pool.submit(self._get_func_result,
                                                    func_name, arg_futures)

    def _get_func_result(self, func_name: str,
                         arg_futures: List) -> TimeSeriesGroup:
        """
        Waits for all the argument futures to be resolved
        then computes the function on the arguments
        Note this is a blocking method and doesn't return a future
        but computes the result
        :param func_name:
        :param arg_futures:
        :return: result of the function applied to the arg futures
        """
        arg_results: List[TimeSeriesGroup] = list()
        for arg_future in arg_futures:
            if isinstance(arg_future, Future):
                # Note Blocking Wait on the future here
                arg_results.append(arg_future.result())
            else:
                arg_results.append(arg_future)

        logger.debug("Computing %s over results: %s", func_name, arg_results)
        return DenseFunctions.execute(func_name, arg_results,
                                      self.parser.fill_func,
                                      self.parser.fill_value)

    @time_calls
    @meter_calls
    def _get_field_future(self,
                          field: str,
                          parallelize: bool,
                          time_range: TimeRange):
        """
        Get the data associated with the field
        :param field:
        :param parallelize:
        :param time_range:
        :return: future for epochs , list of values
        Where list of values is for each series that matched the filter
        condition
        """
        series_reader: SparseSeriesReaderInterface = DruidReader.inst

        # obtain the = clause tags , so we can look for cluster id
        # and match against dashboard queries
        tags = dict()
        SqlParser.extract_tag_conditions_from_filters(self.parser.filters,
                                                      tags)
        cluster_id = tags[CLUSTER_TAG_KEY] if CLUSTER_TAG_KEY in tags else None

        # Check if this particular field and tag combination has been
        # duplicated to a dashboards only datasource
        # if so we prefer this because it offers better performance
        tags[FIELD] = field
        # Assuming that only a single measurement will be specified
        measurement = list(self.parser.measurement.values())[0]
        datasource = series_reader.get_datasource(self.schema,
                                                  measurement,
                                                  tags,
                                                  cluster_id,
                                                  time_range.interval,
                                                  protocol='influx')
        sparseness_disabled = self.schema.is_sparseness_disabled(tags)

        if parallelize:
            return ThreadPools.inst.sql_api_pool.submit(
                series_reader.get_field,
                self.schema,
                datasource,
                field,
                self.parser.filters,
                time_range,
                self.parser.groupby,
                self._query_id,
                self.trace,
                self.parser.fill_func,
                self.parser.fill_value,
                False,
                sparseness_disabled,
                user=self.user
            )
        return series_reader.get_field(self.schema,
                                       datasource,
                                       field,
                                       self.parser.filters,
                                       time_range,
                                       self.parser.groupby,
                                       self._query_id,
                                       self.trace,
                                       self.parser.fill_func,
                                       self.parser.fill_value,
                                       False,
                                       sparseness_disabled,
                                       user=self.user
                                       )
