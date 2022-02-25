"""
GraphiteParser Module
"""
import json
import logging
import uuid
from typing import List, Dict, Tuple

logger = logging.getLogger(__name__)


class GraphiteParser:
    """
    Parses the graphite target string
    """

    def __init__(self, target):
        """
        Parse the graphite target string
        :param target:
        # Sample:
        # aliasSub(
            aliasSub(
                sortByName(
                    groupByNode(
                        aliasSub(
                            sumSeriesWithWildcards(
                                clusters.$Cluster.$Node.Diamond.version.*.*.*,
                                2),
                            '(\\d+)\\.(\\d+)\\.(.*)$',
                            '\\1!\\2!\\3'),
                        4,
                        'sum'
                    )
                ),
                '#(.*)#',
                ''
            ),
            '!',
            '.'
          )"

        target is nested set of functions
        """
        params = target.split('||')
        self.target = GraphiteParser.convert_set_patterns(params[0])
        self.trace = False
        self.disable_cache = False
        self.repopulate_cache = False
        self.sparseness_disabled = -1
        self.lookback_sec = -1
        self.optimize_lookback = -1
        # parse the additional params
        if len(params) > 1:
            additional_params = params[1].split(',')
            for param in additional_params:
                key, value = param.split('=')
                if key == 'trace':
                    self.trace = value == 'true'
                elif key == 'nocache':
                    self.disable_cache = value == 'true'
                elif key == 'repopulate_cache':
                    self.repopulate_cache = value == 'true'
                elif key == 'sparseness_disabled':
                    if value not in ['false', 'true']:
                        logger.error('sparseness_disabled must be true|false')
                        continue
                    self.sparseness_disabled = 0 if value == 'false' else 1
                elif key == 'lookback_sec':
                    if not value.isdigit() or int(value) > 86400:
                        logger.error('lookback_sec must be in [0, 86400]')
                        continue
                    self.lookback_sec = int(value)
                elif key == 'optimize_lookback':
                    if value not in ['false', 'true']:
                        logger.error('optimize_lookback must be true|false')
                        continue
                    self.optimize_lookback = 0 if value == 'false' else 1

        self.query_id = str(uuid.uuid1()) if self.trace else None

        self.args = GraphiteParser.parse_args(self.target)
        if self.trace:
            logger.info('[Trace: %s]: GraphiteParser: parsed_query: %s',
                        self.query_id, str(self))

    def __str__(self):
        """
        dumps a json representation of the parsed target
        :return:
        """
        return json.dumps({'target': self.target, 'parsed': self.args})

    @staticmethod
    def convert_set_patterns(target: str) -> str:
        """
        Convert patterns like {a,b,c} in the metric query to {a;b;c}

        We use commas in our parsing logic to demarcate argument boundaries.
        The set pattern {a,b,c} confuses our logic into thinking they are
        different arguments when they are a part of the same metric in reality.

        :param target:
        :return: Converted string
        """
        # Generate list of tuples where each tuple stores the parenthesis start
        # and end indices
        set_indices = list()
        last_char = None
        last_char_idx = None
        for idx, char in enumerate(target):
            if char not in ['{', '}']:
                # Only interested in set patterns
                continue

            if char == '{':
                if last_char == '{':
                    raise ValueError('Nested {} patterns not supported')

            elif char == '}':
                if last_char == '}':
                    raise ValueError('Nested {} patterns not supported')

                # Last character has to be {
                if last_char is None:
                    raise ValueError('Unterminated {} pattern seen')

                set_indices += [(last_char_idx, idx)]

            last_char = char
            last_char_idx = idx

        if last_char == '{':
            raise ValueError('Unterminated {} pattern seen')

        for set_start_idx, set_end_idx in set_indices:
            # Replace the commas that lie within the set index boundaries, but
            # make sure the other parts of the target string are not touched
            target = (target[:set_start_idx] +
                      target[set_start_idx:set_end_idx].replace(",", ";") +
                      target[set_end_idx:])

        return target

    @staticmethod
    def parse_args(target: str) -> List:
        """
        parse the argument strings as list
        :param target:
        :return:
        """
        args = list()
        _target = target
        while _target is not None and _target != '':
            arg, _target = GraphiteParser.next_arg(_target)
            if arg is not None:
                # arg can be None because of comma after string literal
                # we don't try to parse the comman after string literal
                args.append(arg)
        return args

    @staticmethod
    def next_func(target: str) -> Tuple[Dict, str]:
        """
        Looks ahead and tries to recursively parse a function
        if the next argument is a function
        Returns None if the next arg is not a function
        :param target:
        :return:
        """
        # Check the index where the function arguments begin
        arg_begin = GraphiteParser.index(target, '(')
        if arg_begin < 0:
            # no function here
            return None, target

        # check in target where the current argument will end
        current_arg_end = GraphiteParser.index(target, ',')
        if 0 <= current_arg_end < arg_begin:
            # The current argument ends before the function, so premature
            # to parse the function
            return None, target

        func_arg_begin = arg_begin
        arg_begin = arg_begin + 1
        arg_end = arg_begin + 1
        unmatched_parenthesis = 1
        while unmatched_parenthesis > 0:
            arg_next_begin = GraphiteParser.index(target, '(', arg_begin)
            arg_end = GraphiteParser.index(target, ')', arg_end)
            if 0 < arg_next_begin < arg_end:
                unmatched_parenthesis = unmatched_parenthesis + 1
                arg_begin = arg_next_begin + 1
            else:
                unmatched_parenthesis = unmatched_parenthesis - 1
                arg_end = arg_end + 1

        # Recursively parse the args
        args_string = target[func_arg_begin+1:arg_end-1]
        args = GraphiteParser.parse_args(args_string)
        return {'func': target[:func_arg_begin],
                'args': args}, target[arg_end+1:]

    @staticmethod
    def next_arg(target: str) -> Dict:
        """
        Example: a1, f(f1,g(g1,h(h1,h2),g2),f2), a2
        Return: [{literal: 'a1'},
                 {func:'f',
                   arg:[{literal: 'f1'},
                       {func:'g',
                        arg: [{literal: 'g1'},
                              {func: 'h', arg: [{literal: 'h1'},
                                                {literal: 'h2'}]}
                              {literal: 'g2'}
                             ]},
                       {literal: 'f2'}]},
                 {literal: 'a2'}
        :param target:
        :return:
        """
        # Look for string
        _target = target.strip()
        if _target.startswith("'") or _target.startswith('"'):
            # parse the string literal
            delimiter = _target[0]
            index = 1
            while index < len(_target):
                index = GraphiteParser.index(_target, delimiter, start=index)
                # check if its not terminating
                if _target[index-1] != '\\':
                    break
                index = index + 1
            if index >= len(_target):
                raise ValueError('unterminated delimiter %s in %s' %
                                 (delimiter, _target))
            return {'literal': _target[1:index]}, _target[index+1:]

        # Look for function
        arg, _target = GraphiteParser.next_func(_target)
        if arg is not None:
            return arg, _target

        # not a string or a function, so safe to split by comma
        index = GraphiteParser.index(_target, ',')
        if index < 0:
            return GraphiteParser.parse_arg(_target), None
        return GraphiteParser.parse_arg(_target[:index]), _target[index+1:]

    @staticmethod
    def parse_arg(target: str) -> Dict:
        """
        parse the argument
        :param target:
        :return:
        """
        # Check if it's an integer
        try:
            return {'literal': int(target)}
        except ValueError:
            pass

        # Check if it's a float
        try:
            return {'literal': float(target)}
        except ValueError:
            pass

        # Check if it's a boolean
        if target in ['true', 'false']:
            return {'literal': target == 'true'}

        # Just a string
        if target is not None and target != '':
            return {'field': target}
        return None

    @staticmethod
    def index(s: str, sub: str, start: int = None, end: int = None) -> int:
        """
        Finds the index of sub string in the string s
        :param s:
        :param sub:
        :param start:
        :param end:
        :return: -1 if not found, or the actual index
        """
        try:
            return s.index(sub, start, end)
        except ValueError:
            return -1
