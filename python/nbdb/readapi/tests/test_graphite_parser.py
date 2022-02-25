"""
unittests for sqlapi
"""
import os
from unittest import TestCase
from nbdb.readapi.GraphiteParser import GraphiteParser


class TestGraphiteParser(TestCase):
    """
    Unit test for SQLApi, mocks sparse_series_reader
    """

    def test_simple(self) -> None:
        """

        :return:
        """
        parser = GraphiteParser("a('1\\'2', 3.1)")
        self.assertEqual([{"func": "a", "args": [{"literal": "1\\'2"},
                                                 {"literal": 3.1}]
                           }], parser.args)

    def test_complex(self) -> None:
        """
        Test for invalid SQL statements
        """
        parser = GraphiteParser('a(1,2,b(3,4,c(3),5),6),7,8,d(1)')
        self.assertEqual([{"func": "a",
                           "args": [{"literal": 1},
                                    {"literal": 2},
                                    {"func": "b",
                                     "args": [{"literal": 3},
                                              {"literal": 4},
                                              {"func": "c",
                                               "args": [{"literal": 3}]},
                                              {"literal": 5}]},
                                    {"literal": 6}]},
                          {"literal": 7},
                          {"literal": 8},
                          {"func": "d", "args": [{"literal": 1}]}
                          ],
                         parser.args)

    def test_set_patterns(self) -> None:
        """
        Test Graphite queries with {a,b,c} patterns
        """
        # TEST1: Verify nested set patterns are rejected
        self.assertRaises(ValueError, GraphiteParser.convert_set_patterns,
                          "maxSeries(a.b.{c,{de, ef}})")

        # TEST2: Verify unterminated set patterns are rejected
        self.assertRaises(ValueError, GraphiteParser.convert_set_patterns,
                          "maxSeries(a.b.{c,d.count)")
        self.assertRaises(ValueError, GraphiteParser.convert_set_patterns,
                          "maxSeries(a.b.{c,d}.count})")

        # TEST3: Verify end to end handling
        parser = GraphiteParser('maxSeries(a.b.c.{InfluxDB,Telegraf}.count)')
        self.assertEqual([{"func": "maxSeries",
                           "args": [{"field":
                                     "a.b.c.{InfluxDB;Telegraf}.count"}]},
                          ], parser.args)

    def test_string_literal_parsing(self) -> None:
        """
        Test Graphite queries which use single / double quotes for certain
        arguments.
        Eg. summarize(a, "1h", "sum")
        """
        # TEST1: Double quotes
        parser = GraphiteParser('summarize(a.b.c, "1h", "sum")')
        exp_args = [
            {"func": "summarize",
             "args": [{"field": "a.b.c"},
                      {"literal": "1h"},
                      {"literal": "sum"}]}
        ]
        self.assertEqual(exp_args, parser.args)

        # TEST2: Single quotes
        parser = GraphiteParser("summarize(a.b.c, '1h', 'sum')")
        self.assertEqual(exp_args, parser.args)

    def test_pipe_character_parsing(self) -> None:
        """
        We use || to indicate special query configuration. Make sure it doesn't
        clash with Graphite queries containing '|'
        """
        parser = GraphiteParser("exclude(a.b.*, 'c|d|e')||nocache=true")
        exp_args = [
            {"func": "exclude",
             "args": [{"field": "a.b.*"},
                      {"literal": "c|d|e"}]}
        ]
        self.assertEqual(exp_args, parser.args)
        self.assertTrue(parser.disable_cache)

    def test_cluster_health_dashboard_queries(self) -> None:
        """

        :return:
        """
        _ = self
        ch_queries_file = os.path.dirname(__file__) +\
                          '/data/cluster_health_dashboard_targets.txt'
        funcs = dict()
        fields = dict()
        with open(ch_queries_file) as file:
            targets = file.readlines()
            for target in targets:
                parser = GraphiteParser(target)
                self.find_funcs(parser.args, funcs, fields)

        print('------FUNCS---------: {}', len(funcs))
        for func, count in funcs.items():
            print("{},{}".format(func, count))
        print('------FIELDS--------: {}', len(fields))
        for field, count in fields.items():
            print("{},{}".format(field, count))

    def find_funcs(self, args, funcs, fields):
        """
        Find the functions and fields in the args and add them to the list
        :param args:
        :param funcs:
        :param fields:
        :return:
        """
        for arg in args:
            for key, value in arg.items():
                if key == 'func':
                    if value not in funcs:
                        funcs[value] = 0
                    funcs[value] = funcs[value] + 1
                if key == 'args':
                    self.find_funcs(value, funcs, fields)
                if key == 'field':
                    if value not in fields:
                        fields[value] = 0
                    fields[value] = fields[value] + 1
