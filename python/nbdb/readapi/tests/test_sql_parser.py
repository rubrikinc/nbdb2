"""
unittests for sqlapi
"""
import os
import time
from unittest import TestCase

from nbdb.common.thread_pools import ThreadPools
from nbdb.config.settings import Settings
from nbdb.readapi.sql_parser import SqlParserError, UnsupportedSqlError, \
    SqlParser, FillFunc


class TestSqlParser(TestCase):
    """
    Unit test for SQLApi, mocks sparse_series_reader
    """

    def setUp(self) -> None:
        """
        Setup the test suite, loads the test_settings.yaml and instantiates
        ThreadPools
        """
        Settings.load_yaml_settings(os.path.dirname(__file__) +
                                    '/test_settings.yaml')
        ThreadPools.instantiate()

    def tearDown(self) -> None:
        """
        Stop all the thread pools, blocking wait
        """
        ThreadPools.inst.stop()

    def test_invalid_sql(self) -> None:
        """
        Test for invalid SQL statements
        """
        self.assertRaises(SqlParserError, SqlParser, 'foo')
        self.assertRaises(SqlParserError, SqlParser, 'select * from')
        # only select statements can be parsed
        self.assertRaises(UnsupportedSqlError, SqlParser,
                          'insert into t (c1) values (1)')

    def test_unsupported_sql(self) -> None:
        """
        Test for unsupported SQL statements
        """
        # No where clause or group by clause
        self.assertRaises(UnsupportedSqlError, SqlParser,
                          'select f from m')

    def test_unsupported_group_by(self) -> None:
        """
        Test for unsupported group by clauses, no methods allowed
        """
        # Group by clause cannot have functions, other than time(interval)
        self.assertRaises(UnsupportedSqlError, SqlParser,
                          'select f from m where time>0 group by mean(c)')

    def test_group_by_interval(self) -> None:
        """
        Test for unsupported group by clauses, no methods allowed
        """
        # Group by clause cannot have functions, other than time(interval)
        sql_parser = SqlParser('select f from m where time>0'
                                     ' group by time(10)')
        self.assertEqual(10, sql_parser.interval)

        # By default we return the interval configured in the yaml
        sql_parser = SqlParser('select f from m where time>0')
        self.assertEqual(Settings.inst.sql_api.default_group_by_interval,
                         sql_parser.interval)

    def test_start_end_epoch(self) -> None:
        """
        Test parsing of start and end epoch from the query
        :return:
        """
        sql_parser = SqlParser('select f from m where '
                                     'n=1 and '
                                     '(a=0 or a= 1) and '
                                     'time > 10 and '
                                     '20 > time '
                                     'group by time(1)')

        self.assertEqual(11, sql_parser.start_epoch)
        self.assertEqual(20, sql_parser.end_epoch)

        sql_parser = SqlParser('select f from m where time > 1 and '
                                     'time < 3 group by time(1)')
        self.assertEqual(2, sql_parser.start_epoch)
        self.assertEqual(3, sql_parser.end_epoch)

        sql_parser = SqlParser('select f from m where time >= 1 and '
                                     'time < 3 group by time(1)')
        self.assertEqual(1, sql_parser.start_epoch)
        self.assertEqual(3, sql_parser.end_epoch)

        sql_parser = SqlParser('select f from m where time >= 1 and '
                                     'time <= 3 group by time(1)')
        self.assertEqual(1, sql_parser.start_epoch)
        self.assertEqual(4, sql_parser.end_epoch)

    def test_influx_ql_timefilters(self) -> None:
        """
        test the influx timefilter preprocessing
        :return:
        """
        sql_parse = SqlParser('select f from m where time >= 10 and'
                              ' time < 20 group by time(1)')
        self.assertEqual(10, sql_parse.start_epoch)
        self.assertEqual(20, sql_parse.end_epoch)

        sql_parse = SqlParser('select f from m where time >= 1000'
                              ' group by time(1)')
        self.assertEqual(1000, sql_parse.start_epoch)

        sql_parse = SqlParser('select f from m where time >= 1ms'
                              ' group by time(1)')
        self.assertEqual(0, sql_parse.start_epoch)

        sql_parse = SqlParser('select f from m where time >= now()- 1h'
                              ' group by time(1)')
        self.assertEqual(int(time.time()) - 3600, sql_parse.start_epoch)

        sql_parse = SqlParser('select f from m where time >= now()- 1d'
                              ' group by time(1)')
        self.assertEqual(int(time.time()) - 3600*24, sql_parse.start_epoch)

        sql_parse = SqlParser('select f from m where now()- 1h > time'
                              ' and now()-2h <= time group by time(1)')
        self.assertEqual(int(time.time()) - 7200, sql_parse.start_epoch)
        self.assertEqual(int(time.time()) - 3600, sql_parse.end_epoch)

        sql_parse = SqlParser('select f from m where 153d > time '
                              'and 1<=time group by time(1)')
        self.assertEqual(1, sql_parse.start_epoch)
        self.assertEqual(153 * 24 * 3600, sql_parse.end_epoch)

        sql_parse = SqlParser('select f from m where now() + 153d -3h >'
                              ' time and 1<= time group by time(1)')
        self.assertEqual(1, sql_parse.start_epoch)
        self.assertEqual(int(time.time()) + 153 * 24 * 3600 - 3*3600,
                         sql_parse.end_epoch)

        sql_parse = SqlParser('select f from m where now()+153h <= time'
                              ' and time<now()+1M group by time(1)')
        self.assertEqual(int(time.time()) + 153*3600, sql_parse.start_epoch)
        self.assertEqual(int(time.time()) + 1*3600*24*30, sql_parse.end_epoch)

        sql_parse = SqlParser('select f from m where now()+153h <= time'
                              ' and time<now()+1mon group by time(1)')
        self.assertEqual(int(time.time()) + 153*3600, sql_parse.start_epoch)
        self.assertEqual(int(time.time()) + 1*3600*24*30, sql_parse.end_epoch)

        sql_parse = SqlParser(
            ' select f from m where node=\'n\' and'
            ' now()+153h <= time'
            ' and time<now()+1M and cluster=\'c1\' '
            'group by time(1)')
        self.assertEqual(int(time.time()) + 153*3600, sql_parse.start_epoch)
        self.assertEqual(int(time.time()) + 1*3600*24*30, sql_parse.end_epoch)

    def test_influx_group_by(self):
        """
        Test the group by preprocessing
        :return:
        """
        sql_parse = SqlParser(
            'select f from m where time>10 group by time(1h)')
        self.assertEqual(3600, sql_parse.interval)

        sql_parse = SqlParser(
            'select f from m where time>10 group by time(\'1\')')
        self.assertEqual(1, sql_parse.interval)

        sql_parse = SqlParser(
            'select f from m where time>10 group by time(1)')
        self.assertEqual(1, sql_parse.interval)

        sql_parse = SqlParser(
            'select f from m where time>10 group by time(\'1d\')')
        self.assertEqual(86400, sql_parse.interval)

    def test_influx_fill_func(self) -> None:
        """
        Tests the influx fill function parsing
        :return:
        """
        sql_parse = SqlParser('select f from m where time>10'
                              ' group by time(\'1d\') fill(null)')
        self.assertEqual(FillFunc.NULL, sql_parse.fill_func)

        sql_parse = SqlParser('select f from m where time>10'
                              ' group by time(\'1d\')')
        self.assertEqual(FillFunc.CONSTANT, sql_parse.fill_func)
        self.assertEqual(0, sql_parse.fill_value)

        sql_parse = SqlParser('select f from m where time>10'
                              ' group by time(\'1d\') fill(10.1)')
        self.assertEqual(FillFunc.CONSTANT, sql_parse.fill_func)
        self.assertEqual(10.1, sql_parse.fill_value)

        sql_parse = SqlParser('select f from m where time>10'
                              ' group by time(\'1d\') fill(previous)')
        self.assertEqual(FillFunc.PREVIOUS, sql_parse.fill_func)

        sql_parse = SqlParser('select f from m where time>10'
                              ' group by time(\'1d\') fill(linear)')
        self.assertEqual(FillFunc.LINEAR, sql_parse.fill_func)

    def test_invalid_influx_ql(self) -> None:
        """
        Unsupported but possible influx ql
        :return:
        """
        # fill function in the middle of select statement
        self.assertRaises(ValueError,
                          SqlParser,
                          'select f from m where 10>time group by time(1)'
                          ' fill(null) anything_here_will_be_ignored')
        # Multiplier is not captured
        self.assertRaises(SqlParserError,
                          SqlParser,
                          'select f from m where now()*153d > time'
                          ' group by time(1)')

        # Unknown modifier
        self.assertRaises(ValueError,
                          SqlParser,
                          'select f from m where 153K > time'
                          ' group by time(1)')

    def test_grafana_generated_influx_queries(self) -> None:
        """
        Actual queries generated by influx datasource in grafana
        :return:
        """
        sql_parse = SqlParser(
            'SELECT mean("diamond.process.test_field_1")'
            ' FROM "m"'
            ' WHERE time >= 1582162150842ms and'
            ' time <= 1582162250842ms'
            ' GROUP BY time(1y) fill(null)')
        # note the start,end epoch are aligned to the interval boundary
        self.assertEqual(1576800000, sql_parse.start_epoch)
        self.assertEqual(1608336000, sql_parse.end_epoch)
        self.assertEqual(3600*24*365, sql_parse.interval)
        self.assertEqual(FillFunc.NULL, sql_parse.fill_func)

    def test_complete_query(self) -> None:
        """
        Test a query that captures all the different supported usecases
        """
        sql_parse = SqlParser(
            'SELECT mean(diamond.tf1)/sum(tf2) + 10*np.percentile(tf4, 0.95)'
            ' as f1, tf3 from m '
            'where (node=\'1\' or node=\'2\') and process=\'p1\' and '
            'time < now() - 1h and 1582247379s < time '
            'group by time(\'1h\'),node,process fill(null)'
        )
        end_epoch = int(time.time() - 3600)
        aligned_epoch = int(end_epoch/3600)*3600
        aligned_epoch = aligned_epoch if aligned_epoch >= end_epoch else \
            aligned_epoch + 3600
        self.assertEqual(aligned_epoch, sql_parse.end_epoch)
        self.assertEqual(1582246800, sql_parse.start_epoch)
        self.assertEqual({
            'and': [
                {
                    'or': [
                        {'=': ['node', {'literal': '1'}]},
                        {'=': ['node', {'literal': '2'}]}
                    ]
                },
                {'=': ['process', {'literal': 'p1'}]}
            ]
        }, sql_parse.filters)
        self.assertEqual(['node', 'process'], sql_parse.groupby)
        self.assertEqual({'m':'m'}, sql_parse.measurement)
        self.assertEqual(3600, sql_parse.interval)
        self.assertEqual(
            [
                {
                    'name': 'f1',
                    'value': {
                        '+': [
                            {'/': [{'mean': ['diamond.tf1']},
                                   {'sum': ['tf2']}]
                             },
                            {'*': [{'literal': 10},
                                   {'np.percentile': ['tf4',
                                                      {'literal': 0.95}]}]
                             }]
                    }
                },
                {'value': 'tf3'}
            ],
            sql_parse.expressions)

    def test_grafana_generated_influx_queries_with_filters(self) -> None:
        """
        Tests the grafana generated queries that are not strictly SQL
        compatible and require some preprocessing
        """
        sql_parse = SqlParser(
            'SELECT diamond.process.test_field_1 FROM m '
            'WHERE node = \'1\' AND time >= 1582225779s AND '
            'time < 1582247379s Group by time(10),node')
        self.assertEqual(1582225770, sql_parse.start_epoch)
        self.assertEqual(1582247380, sql_parse.end_epoch)
        self.assertEqual({'=': ['node', {'literal': '1'}]}, sql_parse.filters)

        sql_parse = SqlParser(
            'SELECT diamond.process.test_field_1 FROM m '
            'WHERE (node = \'1\' or node=\'2\') AND '
            'cluster=\'c1\' AND '
            'time >= 1582225779s AND '
            'time < 1582247379s Group by time(10)')
        self.assertEqual(1582225770, sql_parse.start_epoch)
        self.assertEqual(1582247380, sql_parse.end_epoch)
        self.assertEqual(
            {
                'and': [
                    {'or': [
                        {'=': ['node', {'literal': '1'}]},
                        {'=': ['node', {'literal': '2'}]},
                        ]},
                    {'=': ['cluster', {'literal': 'c1'}]}
                ]
            }, sql_parse.filters)

    def test_filter_lookup(self) -> None:
        """
        Tests looking up a specific filter value, its used by the
        SqlApi to look up the cluster tag value if specified in the
        filters
        :return:
        """
        sql_parse = SqlParser(
            'SELECT diamond.process.test_field_1 FROM m '
            'WHERE node = \'1\' AND time >= 1582225779s AND '
            'cluster = \'cluster-uuid-x-y-z\' AND '
            'time < 1582247379s Group by time(10),node')
        self.assertEqual(
            'cluster-uuid-x-y-z',
            SqlParser.lookup_filter_condition(sql_parse.filters,
                                              'cluster',
                                              '='))
        self.assertEqual(
            None,
            SqlParser.lookup_filter_condition(sql_parse.filters,
                                              'process',
                                              '='))

    def test_extract_tags_from_filters(self) -> None:
        """
        Tests looking up a specific filter value, its used by the
        SqlApi to look up the cluster tag value if specified in the
        filters
        :return:
        """
        sql_parse = SqlParser(
            'SELECT diamond.process.test_field_1 FROM m '
            'WHERE node = \'1\' AND time >= 1582225779s AND '
            'cluster = \'cluster-uuid-x-y-z\' AND '
            'time < 1582247379s Group by time(10),node')
        tags = dict()
        SqlParser.extract_tag_conditions_from_filters(sql_parse.filters,
                                                      tags)
        self.assertEqual(
            'cluster-uuid-x-y-z',
            tags['cluster'])
        self.assertEqual(
            '1',
            tags['node'])
