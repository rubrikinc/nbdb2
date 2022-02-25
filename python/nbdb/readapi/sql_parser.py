"""
SqlAPi and related exceptions
"""
import json
import re
import time
from datetime import datetime
from enum import Enum
from typing import List, Tuple, Dict

# pylint: disable=no-name-in-module
from nbdb.readapi.time_series_response import TimeRange
from pglast.parser import parse_sql, ParseError

from nbdb.config.settings import Settings


class SqlParserError(Exception):
    """
    Parsing exception should be thrown for incorrect SQL statements
    """
    # pylint: disable=too-few-public-methods

    def __init__(self, message: str):
        """
        constructor
        :param message:
        """
        Exception.__init__(self)
        self.message = message

    def __str__(self, *args, **kwargs) -> str:
        """ Return str(self). """
        return 'SqlParserError: ' + self.message


class UnsupportedSqlError(Exception):
    """
    Should be through for valid but not yet supported SQL statements
    """
    # pylint: disable=too-few-public-methods

    def __init__(self, message: str):
        """
        constructor
        :param message:
        """
        Exception.__init__(self)
        self.message = message

    def __str__(self, *args, **kwargs) -> str:
        """ Return str(self). """
        return 'UnsupportedSqlError: ' + self.message


# The pattern for extracting left hand side value of time filter
TIME_LH_PATTERN = re.compile('(?:where|and|or)+\\s+'
                             '(?P<value>(now\\(\\)|[\\+\\-\\w\\d\\s])+)\\s*'
                             '[><=]+\\s*'
                             'time', flags=re.IGNORECASE)

# The pattern for extracting right hand side value of time filter
TIME_RH_PATTERN = re.compile('time\\s*'
                             '[<>=]+\\s*'
                             '(?P<value>(now\\(\\)|[\\+\\-\\w\\d\\s])+)\\s+'
                             '(?:group|and|or)', flags=re.IGNORECASE)

TIME_GROUP_BY_PATTERN = re.compile('.* group by time\\((?P<value>.*)\\)',
                                   flags=re.IGNORECASE)


class FillFunc(Enum):
    """
    Various fill functions supported
    """
    NULL = 0
    PREVIOUS = 1
    LINEAR = 2
    CONSTANT = 3


class SqlParser:
    """
    Parses and executes a SQL statement
    Note this uses multiple threads to compute various expressions
    independently in parallel
    """
    # pylint: disable=too-many-instance-attributes
    # pylint: disable=too-few-public-methods
    # Might refactor this class to swap out the group by and filters
    def __init__(self, query: str):
        """
        Parses the SQL query and raises a SyntaxError for any parsing related
        failure
        Validates that only supported queries are parsed
        :return: parsed AST
        """
        # Store the original query for reference and logging
        self._query = query
        self.interval = Settings.inst.sql_api.default_group_by_interval

        # Describes how to deal with no data
        self.fill_func = FillFunc.CONSTANT
        self.fill_value = 0

        # preprocess the query to make it compatible with SQL parsing
        self.preprocessed_query = self._preprocess(query)

        # ready for parsing now
        try:
            parsed = parse_sql(self.preprocessed_query)
        except ParseError as e:
            raise SqlParserError(','.join(e.args) +
                                 ' processed query: ' +
                                 self.preprocessed_query)
        if len(parsed) <= 0:
            raise ValueError('Failed to find any valid sql')
        if len(parsed) > 1:
            raise UnsupportedSqlError('Detected {} sql statements. '
                                      'Batching is not supported'.
                                      format(len(parsed)))
        stmt = parsed[0]['RawStmt']['stmt']
        # Check it has a select, where and group by clause
        if 'SelectStmt' not in stmt:
            raise UnsupportedSqlError('Select clause is required')
        selectStmt = stmt['SelectStmt']
        if 'whereClause' not in selectStmt:
            raise UnsupportedSqlError('Where clause is required')
        if 'fromClause' not in selectStmt:
            raise UnsupportedSqlError('from clause is required')
        if 'targetList' not in selectStmt:
            raise UnsupportedSqlError('target list is required')

        self.measurement = SqlParser._parse_measurement(
            selectStmt['fromClause'])
        self.expressions = SqlParser._parse_expression(
            selectStmt['targetList'])
        self.groupby = SqlParser._parse_group_by(selectStmt)
        parsed_filters = selectStmt['whereClause']

        time_filters = list()
        filters_sans_time = SqlParser._parse_time_filters(
            parsed_filters, time_filters)
        self.start_epoch = SqlParser._parse_start_epoch(time_filters)
        self.end_epoch = SqlParser._parse_end_epoch(time_filters)
        if self.end_epoch is None:
            # We default to now when end epoch is not specified
            self.end_epoch = int(time.time())

        self._align_time_range()

        self.time_range = TimeRange(self.start_epoch,
                                    self.end_epoch,
                                    self.interval)

        self.filters: Dict = filters_sans_time

        # Validate the query now
        if self.start_epoch is None:
            raise UnsupportedSqlError('start epoch must be specified using'
                                      ' time filter')

    def __str__(self):
        """
        override the default to str
        :return: string representation
        """
        return json.dumps({
            'measurement': self.measurement,
            'expressions': self.expressions,
            'group_by': self.groupby,
            'time_range': str(self.time_range),
            'filters': self.filters,
            'fill_func': str(self.fill_func),
            'fill_value': self.fill_value
        })

    def _align_time_range(self) -> None:
        """
        Expands The time range and aligns to a multiple of the interval
        """
        if self.start_epoch is None or \
            self.end_epoch is None or \
            self.interval is None:
            # We need the full time range to align
            return
        self.start_epoch = int(self.start_epoch/self.interval)*self.interval
        end_epoch = int(self.end_epoch/self.interval)*self.interval
        if end_epoch < self.end_epoch:
            end_epoch = end_epoch + self.interval
        self.end_epoch = end_epoch

    def preprocess_child(self, query):
        """
        PGLast doesn't like function in group by clause
        We will remove the time interval
        :param query:
        :return:
        """
        pattern = TIME_GROUP_BY_PATTERN
        value_iterator = re.finditer(pattern, query)
        if value_iterator is not None:
            values = [(m.group('value'), m.span('value'))
                      for m in value_iterator]
            if len(values) > 0:
                self.interval = SqlParser._parse_known_time_prefixes(
                    values[0][0])
                time_arg_start = values[0][1][0] - 1
                time_arg_end = values[0][1][1] + 1
                # strip the arg list from the time() and
                # convert time(xx) to time
                return query[:time_arg_start] + query[time_arg_end:]

        return query

    @staticmethod
    def _parse_expression(targetList: dict) -> List[str]:
        """
        Parses the expression list from the parsed result of SQL query
        :param targetList:
        :return: list of expressions
        """
        expressions = list()
        for target in targetList:
            if 'ResTarget' not in target:
                raise UnsupportedSqlError('{} target type not supported'.
                                          format(json.dumps(target)))
            res_target = target['ResTarget']
            res = dict()
            if 'name' in res_target:
                res['name'] = res_target['name']
            res['value'] = SqlParser._parse_value(res_target['val'])
            expressions.append(res)
        return expressions

    @staticmethod
    def _parse_measurement(fromClause: dict) -> str:
        """
        Parse the measurement from the fromClause
        :param fromClause:
        :return: measurement name
        """
        measurements = dict()
        for clause in fromClause:
            rangeVar = clause['RangeVar']
            name = rangeVar['relname']
            if 'alias' in rangeVar:
                alias = rangeVar['alias']['Alias']['aliasname']
            else:
                alias = name
            measurements[alias] = name
        return measurements

    @staticmethod
    def _parse_time_filters(filters: dict,
                            time_filters: list) -> dict:
        if len(filters) != 1:
            raise UnsupportedSqlError('Only one top level filter '
                                      'supported {}'.
                                      format(json.dumps(filters)))
        for exp_type, args in filters.items():
            if exp_type == 'BoolExpr':
                operator = SqlParser._parse_bool_op(args['boolop'])
                args_sans_time = list()
                for arg in args['args']:
                    arg_sans_time = \
                        SqlParser._parse_time_filters(arg, time_filters)
                    if arg_sans_time is not None:
                        args_sans_time.append(arg_sans_time)
                # check if only one argument is left then the operator
                # is not needed anymore and we can just return the argument
                if len(args_sans_time) == 1:
                    return args_sans_time[0]
                return {operator: args_sans_time}
            if exp_type == 'A_Expr':
                exp = SqlParser._parse_a_expr(args)
                for values in exp.values():
                    if 'time' in values:
                        time_filters.append(exp)
                        return None
                return exp
            raise UnsupportedSqlError('unsupported type for filter: {}'.
                                      format(exp_type))

    @staticmethod
    def _parse_func(args):
        """

        :param args:
        :return:
        """
        funcname = SqlParser._parse_multi_string(args['funcname'])
        args = [SqlParser._parse_value(arg) for arg in args['args']]
        return {funcname: args}

    @staticmethod
    def _parse_a_expr(args):
        """
        :return:
        """
        operator = SqlParser._parse_string(args['name'][0])
        l_val = SqlParser._parse_value(args['lexpr'])
        r_val = SqlParser._parse_value(args['rexpr'])
        return {operator: [l_val, r_val]}

    @staticmethod
    def _parse_bool_op(op):
        if op == 0:
            return 'and'
        if op == 1:
            return 'or'
        return op

    @staticmethod
    def _parse_value(value_ref):
        """

        :param value_ref:
        :return:
        """
        if 'ColumnRef' in value_ref:
            return SqlParser._parse_column_ref(value_ref['ColumnRef'])
        if 'A_Const' in value_ref:
            # constants have to be distinguished from columns
            return {'literal':
                        SqlParser._parse_const_ref(value_ref['A_Const'])
                    }
        if 'A_Expr' in value_ref:
            return SqlParser._parse_a_expr(value_ref['A_Expr'])
        if 'FuncCall' in value_ref:
            return SqlParser._parse_func(value_ref['FuncCall'])

        raise UnsupportedSqlError('value type={} is not supported'.
                                  format(json.dumps(value_ref)))

    @staticmethod
    def _parse_column_ref(column_ref):
        """
        :param column_ref:
        :return:
        """
        if 'fields' not in column_ref:
            return None
        return SqlParser._parse_multi_string(column_ref['fields'])

    @staticmethod
    def _parse_const_ref(const_ref):
        """
        :param column_ref:
        :return:
        """
        if 'val' not in const_ref:
            return None
        val = const_ref['val']
        if 'String' in val:
            return SqlParser._parse_string(const_ref['val'])
        if 'Integer' in val:
            return val['Integer']['ival']
        if 'Float' in val:
            return float(val['Float']['str'])
        raise UnsupportedSqlError('unsupported const_ref: {}'.
                                  format(json.dumps(const_ref)))

    @staticmethod
    def _parse_multi_string(fields):
        """

        :param fields:
        :return:
        """
        return '.'.join([
            SqlParser._parse_string(field)
            for field in fields
        ])

    @staticmethod
    def _parse_string(string_ref):
        """
        parses the string value
        :param string_ref:
        :return:
        """
        if 'String' not in string_ref or 'str' not in string_ref['String']:
            return None
        return string_ref['String']['str']

    @staticmethod
    def _parse_start_epoch(time_filters: list) -> int:
        """

        :param time_filters:
        :return:
        """
        for time_filter in time_filters:
            for operator, (lhs, rhs) in time_filter.items():
                if lhs == 'time':
                    if operator == '>':
                        # start epoch = value[1] + 1
                        return SqlParser.time_str_parser(rhs) + 1
                    if operator == '>=':
                        return SqlParser.time_str_parser(rhs)
                if rhs == 'time':
                    if operator == '<':
                        return SqlParser.time_str_parser(lhs) + 1
                    if operator == '<=':
                        return SqlParser.time_str_parser(lhs)
        return None

    @staticmethod
    def _parse_end_epoch(time_filters: list) -> int:
        """

        :param time_filters:
        :return:
        """
        for time_filter in time_filters:
            for operator, (lhs, rhs) in time_filter.items():
                if lhs == 'time':
                    if operator == '<':
                        # end epoch = value[1] + 1
                        return SqlParser.time_str_parser(rhs)
                    if operator == '<=':
                        return SqlParser.time_str_parser(rhs) + 1
                if rhs == 'time':
                    if operator == '>':
                        return SqlParser.time_str_parser(lhs)
                    if operator == '>=':
                        return SqlParser.time_str_parser(lhs) + 1
        return None

    @staticmethod
    def _parse_group_by_interval(parsed: dict) -> int:
        group_bys = parsed['groupby'] if 'groupby' in parsed else list()
        if not isinstance(group_bys, list):
            group_bys = [group_bys]

        for group_by in group_bys:
            value = group_by['value']
            if isinstance(value, dict):
                if 'time' in value:
                    interval = value['time']
                    if isinstance(interval, dict):
                        interval = interval['literal']
                    if isinstance(interval, int):
                        return interval
                    if isinstance(interval, str):
                        return SqlParser._parse_known_time_prefixes(interval)
                    raise ValueError('unsupported type of interval {}'
                                     .format(interval))

        return Settings.inst.sql_api.default_group_by_interval

    @staticmethod
    def _parse_group_by(selectStmnt: dict) -> List[str]:
        """

        :param selectStmnt:
        :return:
        """
        if 'groupClause' not in selectStmnt:
            return list()
        group_bys = selectStmnt['groupClause']
        group_by_tags = [SqlParser._parse_value(field)
                         for field in group_bys]
        try:
            group_by_tags.remove('time')
        except ValueError:
            pass
        # validate that we only have fields in the groupby clause
        for tag in group_by_tags:
            if not isinstance(tag, str):
                raise UnsupportedSqlError('Group by tag: {} not allowed'.
                                          format(tag))
        return group_by_tags

    def key(self):
        """
        Generate a consistent hash of the query without the time range
        :return:
        """
        return '{expressions}.{measurement}.{filters}.{groupby}.' \
               '{interval}.{fill_func}'.\
            format(expressions=json.dumps(self.expressions),
                   measurement=self.measurement,
                   filters=json.dumps(self.filters),
                   groupby=json.dumps(self.groupby),
                   interval=self.interval,
                   fill_func=self.fill_func
                   )

    def _preprocess(self, query: str) -> str:
        """
        InfluxQL is not strictly SQL compatible, this method
        extracts additional information passed and makes the query
        string compatible with SQL parsing
        :param query: query string
        :return: SQL compatible query string
        """
        # Extract the fill function
        index = query.find('fill(')
        if index >= 0:
            end_index = query.find(')', index)
            if end_index < len(query) - 1:
                raise ValueError('Expected fill func at the end of query')
            fill_func = query[index + len('fill('):end_index]
            if fill_func == 'null':
                self.fill_func = FillFunc.NULL
            elif fill_func == 'previous':
                self.fill_func = FillFunc.PREVIOUS
            elif fill_func == 'linear':
                self.fill_func = FillFunc.LINEAR
            else:
                # see if its a float value
                try:
                    self.fill_value = float(fill_func)
                    self.fill_func = FillFunc.CONSTANT
                except ValueError:
                    # not a floating value
                    raise ValueError('Unsupported fill_func: ' + fill_func)

            query = query[:index]

        preprocessed = SqlParser._preprocess_time_filter(query)

        return self.preprocess_child(preprocessed)

    @staticmethod
    def _preprocess_time_filter(query: str) -> str:
        """
        The influx QL can pass time filter in a non-sql compliant way
        this will break SQL parsing
        we have to preprocess the time filter to make it SQL compatible
        This preprocessor adds quotes around all values associated with
        time filters or time group by
        """
        if 'time' not in query:
            return query
        query = query.strip()
        values = list()
        for pattern in [TIME_LH_PATTERN,
                        TIME_RH_PATTERN,
                        TIME_GROUP_BY_PATTERN]:
            value_iterator = re.finditer(pattern, query)
            if value_iterator is not None:
                values.extend([(m.group('value'), m.span('value'))
                               for m in value_iterator])

        # Add single quotes around the values
        # we need to process them in increasing order of span
        # in order to accurately compute the shift in spans
        values.sort(key=lambda value: value[1][0])
        shift = 0
        for value in values:
            mod_query = SqlParser._safely_quote_value(query, value, shift)
            shift = shift + len(mod_query) - len(query)
            query = mod_query
        return query

    @staticmethod
    def _safely_quote_value(query: str,
                            value: Tuple[str, Tuple[int, int]],
                            shift: int) \
            -> str:
        """
        Replace all occurrences of value with 'value' in query
        if value is not already under quotes in query
        :param query: query string to modify
        :param value: Tuple of value and its position
        :param shift: shift in the position variable
        :return: replaced query
        """
        # only replace value if its not already under quotes
        if value[0].startswith('\''):
            return query

        # Double check that value in query is not under quotes
        span = value[1]
        span_start = span[0] + shift
        span_end = span[1] + shift
        if span_start > 0 and query[span_start - 1] == '\'':
            return query
        if span_end < len(query) - 1 and query[span_end + 1] == '\'':
            return query

        return query[:span_start] + '\'' + value[0] + '\'' + query[span_end:]

    @staticmethod
    def time_str_parser(time_str) -> int:
        """
        Parses the time_str and returns the epoch representation
        :param time_str: can be an integer or string
        :return:
        """
        if isinstance(time_str, dict):
            time_str = time_str['literal']

        if isinstance(time_str, int):
            # We are already provided an epoch, integers are assumed epochs
            return time_str

        if isinstance(time_str, str) and '_' in time_str:
            # If underscore is present, it has to be in the format
            # 18:16_20210601
            try:
                date = datetime.strptime(time_str, "%H:%M_%Y%m%d")
                return int((date - datetime(1970, 1, 1)).total_seconds())
            except:
                raise ValueError(
                    f"Unsupported format {time_str} in time range filter")

        parts = re.split('(\\+|\\-| )', time_str)
        epoch = 0
        operand = None
        for part in parts:
            part = part.strip()
            if part == '':
                continue
            if part in ['+', '-']:
                operand = part
            else:
                part_value = SqlParser._parse_known_time_prefixes(part)
                if operand is None:
                    epoch = part_value
                elif operand == '+':
                    epoch = epoch + part_value
                elif operand == '-':
                    epoch = epoch - part_value
                else:
                    raise ValueError('unsupported operand: {} '
                                     'in time filter: {}'
                                     .format(operand, time_str))
        if epoch < 0:
            # in this case its assumed relative to current time
            epoch = time.time() + epoch
        return int(epoch)

    @staticmethod
    def _parse_known_time_prefixes(str_val: str) -> int:
        """
        We support some well known prefixes for the time values
        :param str_val: now(), 10ms, 1d, 1h
        :return:
        """
        # strip any quotes
        str_val = str_val.strip("'")
        if str_val in ('now()', 'now'):
            return time.time()

        modifiers = [('ms', 1/1000),
                     ('s', 1),
                     ('m', 60),
                     ('min', 60),
                     ('h', 3600),
                     ('d', 3600*24),
                     ('mon', 3600*24*30),
                     ('M', 3600*24*30),
                     ('y', 3600*24*365)]
        for mod, mul in modifiers:
            if str_val.endswith(mod):
                return int(str_val.replace(mod, ''))*mul

        # assume this is just an integer then
        return int(str_val)

    @staticmethod
    def lookup_filter_condition(filters: Dict, variable: str, op: str) -> str:
        """
        Checks if there is a filter condition with the specified variable
        :param filters:
        :param variable:
        :param op:
        :return: the corresponding value associated or None if no condition
         is found
        """
        if filters is None:
            return None
        for key, values in filters.items():
            if key != op:
                if isinstance(values, list):
                    for value in values:
                        result = SqlParser.lookup_filter_condition(value,
                                                                   variable,
                                                                   op)
                        if result is not None:
                            return result
            else:
                if isinstance(values, list):
                    if variable in values:
                        # return the literal in the values
                        for value in values:
                            if isinstance(value, dict):
                                return value['literal']
        return None

    @staticmethod
    def extract_tag_conditions_from_filters(filters: Dict,
                                            tags: Dict[str, str]) -> None:
        """
        Extract the tag key=value dictionary from filters
        :param filters:
        :param tags: dictionary of tag key = value
        """
        if filters is None:
            return
        for key, values in filters.items():
            if key != '=':
                if isinstance(values, list):
                    for value in values:
                        SqlParser.extract_tag_conditions_from_filters(value,
                                                                      tags)
            else:
                if isinstance(values, list):
                    # return the literal in the values
                    for value in values:
                        if isinstance(value, str):
                            tag_key = value
                        if isinstance(value, dict):
                            tag_value = value['literal']
                    tags[tag_key] = tag_value
