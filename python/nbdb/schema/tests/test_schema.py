"""
Unit tests for schema.py
"""
import unittest
import os

from nbdb.common.data_point import DataPoint
from nbdb.config.settings import Settings
from nbdb.schema.schema import BATCH_DATASOURCE_PREFIX
from nbdb.schema.schema import Schema, DATASOURCE_SHARD_SEP
from nbdb.schema.schema import ROLLUP_LAST, ROLLUP_SUM
from nbdb.schema.schema import SHARD_BY_CLUSTER, SHARD_BY_METRIC


class TestSchema(unittest.TestCase):
    """
    Basic test for EsIndexInterface
    """

    def setUp(self):
        """
        Initialize the Schema
        :return:
        """
        self.schema = Schema.load_from_file(
            os.path.dirname(__file__) + '/test_schema.yaml')
        Settings.load_yaml_settings(os.path.dirname(__file__) +
                                    '/test_settings.yaml')

    def test_schema(self):
        """
        Checks that create_es_index_template() works as expected
        :return:
        """
        self.assertEqual('max', self.schema.get_down_sampling_function('m2',
                                                                       'f2'))
        self.assertEqual('mean', self.schema.get_down_sampling_function('m4',
                                                                        'f5'))

        self.assertEqual('mean', self.schema.get_down_sampling_function('m5',
                                                                        'f3'))

    def test_field_level_attributes_with_sharded_datasource(self):
        """
        Tests that we can extract field level attributes for sharded
        datasources
        :return:
        """
        # unsharded data source
        self.assertEqual('mean', self.schema.get_down_sampling_function(
            'm5', 'f3'))
        # sharded data source
        self.assertEqual('max', self.schema.get_down_sampling_function(
            'm2{}8'.format(DATASOURCE_SHARD_SEP), 'f2'))

    def test_sparse_algos(self):
        """
        Check the sparse algos authoring
        :return:
        """
        self.assertEqual(4, len(self.schema.sparse_algos()))

    def test_measurement_to_datasource(self):
        """
        Check measurement to datasource mapping
        :return:
        """
        tags = {'m1_shard_tag': '0', 'm2_shard_tag': 'b'}
        self.assertEqual('graphite_flat',
                         self.schema.get_datasource('random', tags,
                                                    'graphite'))
        self.assertEqual('m4', self.schema.get_datasource('a.1.2.3', tags,
                                                          'graphite'))
        self.assertEqual('m4',
                         self.schema.get_datasource('a.b.c.d.e.1.2.3', tags,
                                                    'graphite'))
        # Test longest prefix match to sharded datasource
        self.assertEqual('m2{}1'.format(DATASOURCE_SHARD_SEP),
                         self.schema.get_datasource('a.b.1.2.3', tags,
                                                    'graphite'))
        self.assertEqual('m1{}3'.format(DATASOURCE_SHARD_SEP),
                         self.schema.get_datasource('a.b.c.1.2.3', tags,
                                                    'graphite'))

    def test_measurement_to_datasource_cc(self):
        """
        Check measurement to datasource mapping, cross cluster
        :return:
        """
        tags = {'tkn3': 'service_1', 'tkn4': 'Fileset'}
        self.assertEqual('cc_t_1',
                         self.schema.get_datasource('random', tags, 'graphite',
                                                    SHARD_BY_METRIC))
        self.assertEqual('cc_t_1',
                         self.schema.get_datasource('a.1.2.3', tags,
                                                    'graphite',
                                                    SHARD_BY_METRIC))
        self.assertEqual('cc_t_1',
                         self.schema.get_datasource('a.b.c.d.e.1.2.3', tags,
                                                    'graphite',
                                                    SHARD_BY_METRIC))
        # Test longest prefix match to sharded datasource
        tags = {'tkn3': 'Diamond', 'tkn4': 'process_1',
                'tkn5': 'Node', 'tkn6': 'Status'}
        self.assertEqual('cc_t_0',
                         self.schema.get_datasource('a.b.1.2.3', tags,
                                                    'graphite',
                                                    SHARD_BY_METRIC))
        self.assertEqual('cc_t_0',
                         self.schema.get_datasource('a.b.c.1.2.3', tags,
                                                    'graphite',
                                                    SHARD_BY_METRIC))
        
        tags = {'tkn3': 'Diamond', 'tkn4': 'process_1',
                'tkn5': 'Node'}
        with self.assertRaises(ValueError):
            self.schema.get_datasource('a.b.1.2.3', tags, 'graphite',
                                       SHARD_BY_METRIC)

        with self.assertRaises(ValueError):
            self.schema.get_datasource('a.b.c.1.2.3', tags, 'graphite',
                                       SHARD_BY_METRIC)
        
        tags = {'tkn3': 'Diamond', 'tkn4': 'process_1',
                'tkn5': 'Node', 'tkn6': 'Status', 'tkn7': 'random'}
        self.assertEqual('cc_t_0',
                         self.schema.get_datasource('a.b.1.2.3', tags,
                                                    'graphite',
                                                    SHARD_BY_METRIC))
        self.assertEqual('cc_t_0',
                         self.schema.get_datasource('a.b.c.1.2.3', tags,
                                                    'graphite',
                                                    SHARD_BY_METRIC))

    def test_sharded_datasources(self) -> None:
        """
        Test the datasources that are sharded by cluster
        :return:
        """
        self.assertEqual(
            'cc{S}0:600:DAY,cc{S}1:600:DAY,'
            'm1{S}0:600:DAY,m1{S}1:600:DAY,m1{S}2:600:DAY,'
            'm1{S}3:600:DAY,m1{S}4:600:DAY,m2{S}0:600:DAY,'
            'm2{S}1:600:DAY,m4:600:DAY,m5:600:DAY,'
            'rollup_clusters_sharded_ds{S}0:600:DAY,'
            'rollup_clusters_sharded_ds{S}1:600:DAY,'
            'rollup_non_sharded_ds:600:DAY,'
            'rollup_Diamond_sharded_ds{S}0:600:DAY,'
            'rollup_Diamond_sharded_ds{S}1:600:DAY,'
            'rollup_catchall:600:DAY,'
            'graphite_flat:600:DAY'.format(S=DATASOURCE_SHARD_SEP),
            ','.join([':'.join([str(i) for i in datasource_details])
                      for datasource_details
                      in self.schema.datasources()]))

        # m4 is not sharded so all clusters go to same datasource
        tags1 = {'m1_shard_tag': '0123'}
        tags2 = {'m1_shard_tag': 'f123'}
        self.assertEqual(self.schema.get_datasource('a.1.2.3', tags1,
                                                    'graphite'),
                         self.schema.get_datasource('a.1.2.3', tags2,
                                                    'graphite'))

    def test_rollup_datasource_match_rules(self) -> None:
        """
        Test the match rules used to select rollup datasources
        :return:
        """
        sep = DATASOURCE_SHARD_SEP
        # Use hardcoded cluster ID and node IDs and store their expected shard
        # suffix values
        cluster = 'ABC'
        cluster_shard = '1'
        node_id = 'RVM123'
        node_shard = '0'

        # Flat Diamond metric should go to Diamond rollup datasource
        diamond_metric = {'tkn0': 'clusters',
                          'tkn1': cluster,
                          'tkn2': node_id,
                          'tkn3': 'Diamond',
                          'tkn4': 'uptime'}
        self.assertEqual(
            self.schema.get_rollup_datasource(None, diamond_metric),
            f'rollup_Diamond_sharded_ds{sep}{node_shard}')

        # Flat non-Diamond metric should go to cluster-sharded rollup
        # datasource
        non_diamond_metric = {'tkn0': 'clusters',
                              'tkn1': cluster,
                              'tkn2': node_id,
                              'tkn3': 'InfluxDB',
                              'tkn4': 'uptime'}
        self.assertEqual(
            self.schema.get_rollup_datasource(None, non_diamond_metric),
            f'rollup_clusters_sharded_ds{sep}{cluster_shard}')

        # Flat non-CDM metric (i.e. not beginning with clusters) should go to
        # non-sharded rollup datasource
        non_cdm_metric = {'tkn0': 'internal',
                          'tkn1': 'turbo',
                          'tkn2': 'bhanu',
                          'tkn3': 'InfluxDB',
                          'tkn4': 'uptime'}
        self.assertEqual(
            self.schema.get_rollup_datasource(None, non_cdm_metric),
            'rollup_non_sharded_ds')

        # Non graphite metrics should go to catchall rollup datasource
        influx_metric = {'cluster': cluster,
                         'node': node_id,
                         'field': 'Diamond.uptime.seconds'}
        self.assertEqual(
            self.schema.get_rollup_datasource(None, influx_metric),
            'rollup_catchall')

    def test_prod_rollup_function_regex_rules(self) -> None:
        """
        Test the regex rules used in the prod schema to select rollup functions
        :return:
        """
        schema = Schema.load_from_file(
            os.path.dirname(__file__) + '/../../config/schema_prod.yaml')
        # Use hardcoded cluster ID and node IDs
        cluster = 'ABC'
        node_id = 'RVM123'

        # Diamond.service.Diamond.Run.count should get ROLLUP_LAST
        tags = {'tkn0': 'clusters',
                'tkn1': cluster,
                'tkn2': node_id,
                'tkn3': 'Diamond',
                'tkn4': 'service',
                'tkn5': 'Diamond',
                'tkn6': 'Run',
                'tkns': '7'}
        datapoint = DataPoint('all_metrics_t_0', 'count', tags, 0, 0, 0)
        func = schema.get_rollup_function(datapoint.series_id)
        self.assertEqual(func, ROLLUP_LAST)

        # Diamond.schedulerstats.* should get ROLLUP_LAST
        tags = {'tkn0': 'clusters',
                'tkn1': cluster,
                'tkn2': node_id,
                'tkn3': 'Diamond',
                'tkn4': 'schedulerstats',
                'tkn5': 'cpu',
                'tkn6': 'cpu0',
                'tkns': '7'}
        datapoint = DataPoint('all_metrics_t_0', 'waittime', tags, 0, 0, 0)
        func = schema.get_rollup_function(datapoint.series_id)
        self.assertEqual(func, ROLLUP_LAST)

        # JobFetcherLoop.CPU.* should get ROLLUP_LAST
        tags = {'tkn0': 'clusters',
                'tkn1': cluster,
                'tkn2': node_id,
                'tkn3': 'JobFetcherLoop',
                'tkn4': 'CPU',
                'tkns': '5'}
        datapoint = DataPoint('all_metrics_t_0', 'JOB_FOO', tags, 0, 0, 0)
        func = schema.get_rollup_function(datapoint.series_id)
        self.assertEqual(func, ROLLUP_LAST)

        # Diamond.nfacct.* should get ROLLUP_SUM
        tags = {'tkn0': 'clusters',
                'tkn1': cluster,
                'tkn2': node_id,
                'tkn3': 'Diamond',
                'tkn4': 'nfacct',
                'tkn5': 'Input',
                'tkn6': 'IPv4',
                'tkn7': 'Tcp',
                'tkns': '8'}
        datapoint = DataPoint('all_metrics_t_0', 'bytes', tags, 0, 0, 0)
        func = schema.get_rollup_function(datapoint.series_id)
        self.assertEqual(func, ROLLUP_SUM)

    def test_validate_authored_schema_files(self) -> None:
        """
        Scans the config directory for all authored schema files
        and validates them
        """
        _ = self
        # List all files in a directory using scandir()
        basepath = os.path.dirname(__file__) + '/../../config'
        # look for all schema files
        schema_files = []
        with os.scandir(basepath) as entries:
            for entry in entries:
                if entry.is_file():
                    if ('schema' in entry.name and
                            entry.name.endswith('.yaml')):
                        schema_files.append(basepath + '/' + entry.name)

        for schema_file in schema_files:
            print('validating: {} '.format(schema_file))
            try:
                Schema.load_from_file(schema_file)
            except Exception as exc:
                raise Exception("Failed to validate %s. Saw error: %s" %
                                (schema_file, exc)) from exc

    def test_batch_mode(self):
        """Test datasource renaming in batch mode"""
        # Reload schema with batch_mode=True
        self.schema = Schema.load_from_file(
            os.path.dirname(__file__) + '/test_schema.yaml',
            batch_mode=True)
        for (ds_name, _, _) in self.schema.datasources():
            self.assertTrue(ds_name.startswith(BATCH_DATASOURCE_PREFIX))
        # Test datasource lookups for field to datasource mapping
        tags = {'m1_shard_tag': '0', 'm2_shard_tag': 'b'}
        self.assertEqual('batch_graphite_flat',
                         self.schema.get_datasource('random', tags,
                                                    'graphite'))
        self.assertEqual('batch_m4',
                         self.schema.get_datasource('a.1.2.3', tags,
                                                    'graphite'))
        self.assertEqual('batch_m4',
                         self.schema.get_datasource('a.b.c.d.e.1.2.3', tags,
                                                    'graphite'))
        self.assertEqual('batch_m2{}1'.format(DATASOURCE_SHARD_SEP),
                         self.schema.get_datasource('a.b.1.2.3', tags,
                                                    'graphite'))
        self.assertEqual('batch_m1{}3'.format(DATASOURCE_SHARD_SEP),
                         self.schema.get_datasource('a.b.c.1.2.3', tags,
                                                    'graphite'))
        # Test datasource lookups for rollup
        self.assertEqual('batch_rollup_catchall_w_3600',
                         self.schema.get_rollup_datasource(3600, None))
        self.assertEqual(
            'batch_rollup_clusters_sharded_ds_t_0_w_3600',
            self.schema.get_rollup_datasource(
                3600, {'tkn0': 'clusters', 'tkn1': '1'}))
        self.assertEqual(
            'batch_rollup_clusters_sharded_ds_t_1_w_3600',
            self.schema.get_rollup_datasource(
                3600, {'tkn0': 'clusters', 'tkn1': '2'}))

    def test_batch_mode_cc(self):
        """Test datasource renaming in batch mode, cross cluster"""
        # Reload schema with batch_mode=True
        self.schema = Schema.load_from_file(
            os.path.dirname(__file__) + '/test_schema.yaml',
            batch_mode=True)

        tags = {'tkn3': 'service_1', 'tkn4': 'Fileset'}
        self.assertEqual('batch_cc_t_1',
                         self.schema.get_datasource('random', tags, 'graphite',
                                                    SHARD_BY_METRIC))
        self.assertEqual('batch_cc_t_1',
                         self.schema.get_datasource('a.1.2.3', tags,
                                                    'graphite',
                                                    SHARD_BY_METRIC))
        self.assertEqual('batch_cc_t_1',
                         self.schema.get_datasource('a.b.c.d.e.1.2.3', tags,
                                                    'graphite',
                                                    SHARD_BY_METRIC))
        self.assertEqual('batch_rollup_cc_t_1_w_3600',
                         self.schema.get_rollup_datasource(3600, tags,
                                                           SHARD_BY_METRIC))
        
        tags = {'tkn3': 'Diamond', 'tkn4': 'process_1',
                'tkn5': 'Node', 'tkn6': 'Status'}
        self.assertEqual('batch_cc_t_0',
                         self.schema.get_datasource('a.b.1.2.3', tags,
                                                    'graphite',
                                                    SHARD_BY_METRIC))
        self.assertEqual('batch_cc_t_0',
                         self.schema.get_datasource('a.b.c.1.2.3', tags,
                                                    'graphite',
                                                    SHARD_BY_METRIC))
        self.assertEqual('batch_rollup_cc_t_0_w_3600',
                         self.schema.get_rollup_datasource(3600, tags,
                                                           SHARD_BY_METRIC))


        tags = {'tkn3': 'Diamond', 'tkn4': 'process_1',
                'tkn5': 'random'}
        with self.assertRaises(ValueError):
            self.schema.get_datasource('a.b.1.2.3', tags, 'graphite',
                                       SHARD_BY_METRIC)

        with self.assertRaises(ValueError):
            self.schema.get_datasource('a.b.c.1.2.3', tags, 'graphite',
                                       SHARD_BY_METRIC)

        with self.assertRaises(ValueError):
            self.schema.get_rollup_datasource(3600, tags, SHARD_BY_METRIC)

        
        tags = {'tkn3': 'Diamond', 'tkn4': 'process_1',
                'tkn5': 'Node', 'tkn6': 'Status', 'tkn7': 'random'}
        self.assertEqual('batch_cc_t_0',
                         self.schema.get_datasource('a.b.1.2.3', tags,
                                                    'graphite', SHARD_BY_METRIC))
        self.assertEqual('batch_cc_t_0',
                         self.schema.get_datasource('a.b.c.1.2.3', tags,
                                                    'graphite', SHARD_BY_METRIC))
        self.assertEqual('batch_rollup_cc_t_0_w_3600',
                         self.schema.get_rollup_datasource(3600, tags,
                                                           SHARD_BY_METRIC))

    def test_get_crosscluster_shard(self):
        # Test against a cross-cluster pattern with no wildcards
        tags = {
            'tkn0': 'clusters', 
            'tkn1': '*', 
            'tkn2': '*', 
            'tkn3': 'service_3',
            'tkn4': 'process_2',
            'tkn5': 'task_1',
            'tkn6': 'Errors', 
            'tkns': '8', 
            'field': 'count'
        }
        # pylint: disable-msg=W0212  # Protected Access
        shard_suffix = Schema._get_crosscluster_shard(
            self.schema.cross_cluster_patterns, tags)
        self.assertEqual('1', shard_suffix)

        # Test against a cross-cluster pattern which has characters + wildcards
        # (eg. x*) for certain tokens
        tags = {
            'tkn0': 'clusters',
            'tkn1': '*',
            'tkn2': '*',
            'tkn3': 'service_3',
            'tkn4': 'process_2',
            'tkn5': 'task_1',
            'tkn6': 'Errors',
            'tkns': '8',
            'field': 'xyz'
        }
        # pylint: disable-msg=W0212  # Protected Access
        shard_suffix = Schema._get_crosscluster_shard(
            self.schema.cross_cluster_patterns, tags)
        self.assertEqual('1', shard_suffix)

        # Verify that if no cross-cluster patterns are matched, we return NULL
        # datasource
        tags = {
            'tkn0': 'clusters',
            'tkn1': '*',
            'tkn2': '*',
            'tkn3': 'service_3',
            'tkn4': 'process_2',
            'tkn5': 'task_1',
            'tkn6': 'Errors',
            'tkns': '8',
            'field': 'abc'
        }
        # pylint: disable-msg=W0212  # Protected Access
        shard_suffix = Schema._get_crosscluster_shard(
            self.schema.cross_cluster_patterns, tags)
        self.assertEqual(None, shard_suffix)

        # Test for a find query against cross-cluster patterns. Usually we
        # return a non-match if there are certain tokens present in a
        # cross-cluster pattern but which are missing in the query metric. But
        # for find queries, we ignore missing tokens.
        tags = {
            'tkn0': 'clusters',
            'tkn1': '*',
            'tkn2': '*',
            'tkn3': 'service_3',
            'tkn4': 'process_2',
            'tkn5': 'task_1',
            'tkns': '7'
        }
        # pylint: disable-msg=W0212  # Protected Access
        shard_suffix = Schema._get_crosscluster_shard(
            self.schema.cross_cluster_patterns, tags, find_query=True)
        self.assertEqual('1', shard_suffix)

    def test_get_sparseness_disabled_interval(self):
        """
        Check measurement to datasource mapping, cross cluster
        :return:
        """
        # Test against a pattern with no wildcards fourth token onwards
        # non-node
        tags = {
            'tkn0': 'clusters',
            'tkn1': '*',
            'tkn2': '*',
            'tkn3': 'Diamond',
            'tkn4': 'm_1',
            'tkns': '6',
            'field': 'seconds'
        }
        interval = self.schema.get_sparseness_disabled_interval(tags)
        self.assertEqual(600, interval)

        # Test against a pattern which has characters + wildcards
        # (eg. x*) for certain tokens
        tags = {
            'tkn0': 'clusters',
            'tkn1': '*',
            'tkn2': '*',
            'tkn3': 'Diamond',
            'tkn4': 'service',
            'tkn5': 'testDB',
            'tkns': '7',
            'field': 'm_2'
        }
        interval = self.schema.get_sparseness_disabled_interval(tags)
        self.assertEqual(1200, interval)

        # Test against a pattern which has a wildcard for a certain token
        tags = {
            'tkn0': 'clusters',
            'tkn1': '*',
            'tkn2': '*',
            'tkn3': 'service_1',
            'tkn4': 'process_1',
            'tkns': '6',
            'field': 'xyz'
        }
        interval = self.schema.get_sparseness_disabled_interval(tags)
        self.assertEqual(60, interval)

        # Query containing wildcard should only work if the pattern also has a
        # wildcard for the same token
        tags = {
            'tkn0': 'clusters',
            'tkn1': '*',
            'tkn2': '*',
            'tkn3': 'service_1',
            'tkn4': 'process_1',
            'tkns': '6',
            'field': '*'
        }
        interval = self.schema.get_sparseness_disabled_interval(tags)
        self.assertEqual(60, interval)

        tags = {
            'tkn0': 'clusters',
            'tkn1': '*',
            'tkn2': '*',
            'tkn3': 'Diamond',
            'tkn4': 'service',
            'tkn5': '*',
            'tkns': '7',
            'field': 'm_2'
        }
        interval = self.schema.get_sparseness_disabled_interval(tags)
        self.assertEqual(None, interval)

        # Verify that if no sparseness disabled patterns are matched, we return
        # NULL interval
        tags = {
            'tkn0': 'clusters',
            'tkn1': '*',
            'tkn2': '*',
            'tkn3': 'service_x',
            'tkn4': 'process_x',
            'tkn5': 'process_y',
            'tkn6': 'Errors',
            'tkns': '8',
            'field': 'abc'
        }
        interval = self.schema.get_sparseness_disabled_interval(tags)
        self.assertEqual(None, interval)
