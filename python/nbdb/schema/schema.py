"""
Schema module
"""

from __future__ import annotations

import json
import logging
import re
import zlib
from re import Pattern
from dataclasses import dataclass
from typing import List, Dict, Tuple
import fnmatch

import yaml
from pytrie import StringTrie

from nbdb.common.data_point import FIELD
from nbdb.common.druid_admin import GRANULARITY_STRING_TO_SECS
from nbdb.common.druid_admin import SUPPORTED_GRANULARY_STRINGS

from nbdb.common.data_point import FIELD, TOKEN_TAG_PREFIX

logger = logging.getLogger()

DATASOURCES = 'datasources'
MEASUREMENTS = 'measurements'
EXPLORATION_DATASOURCES = 'exploration_datasources'
MEASUREMENT_TO_DATASOURCE_MAPPING = 'measurement_to_datasource_mapping'
CATCH_ALL_DATASOURCE = 'catch_all_datasource'
TRANSFORMS = 'transforms'
TRANSFORM_TYPE = 'transform_type'
EXCLUDE_REGEX = 'exclude_regex'
INCLUDE_REGEX = 'include_regex'
SUFFIX = 'suffix'
NUM_SHARDS = 'num_shards'
WINDOWS = 'windows'
GRANULARITIES = 'granularities'
QUERY = 'query'
DEFAULT_SEGMENT_GRANULARITY = 'default_segment'
SEGMENT_GRANULARITY = 'segment_granularity'
QUERY_GRANULARITY = 'query_granularity'
DATASOURCE_SHARD_SEP = '_t_'
DATASOURCE_WINDOW_SEP = '_w_'
CROSSCLUSTER_TEMPLATE = 'cc'

# ROLLUP keys
ROLLUPS = 'rollups'
ROLLUP_SHARDED_DATASOURCE = 'sharded_datasource'
ROLLUP_CATCH_ALL_DATASOURCE = 'catch_all_datasource'
ROLLUP_MATCH_RULES = 'rollup_datasource_match_rules'
ROLLUP_ALGORITHMS = 'algos'
ROLLUP_DEFAULT_ALGORITHM = 'default_algo'
ROLLUP_WINDOWS = 'windows'

CROSS_CLUSTER_PATTERNS = 'cross_cluster_patterns'
CROSS_CLUSTER_PATTERNS_PATTERN = 'pattern'
CROSS_CLUSTER_PATTERNS_HASHKEY = 'hashkey'

SPARSENESS_DISABLED_PATTERNS = 'sparseness_disabled_patterns'
SPARSENESS_DISABLED_RULE_PATTERN = 'pattern'
SPARSENESS_DISABLED_RULE_INTERVAL = 'interval'

# Batch ingest keys
BATCH_DATASOURCE_PREFIX = 'batch_'

ROLLUP_DATASOURCE_PREFIX = 'rollup_'

# Collocated queries
DASHBOARD_QUERIES = 'dashboard_queries'
CLUSTERS = 'clusters'

# Rollup functions
ROLLUP_NONE = 0
ROLLUP_MEAN = 1
ROLLUP_MAX = 2
ROLLUP_LAST = 3
ROLLUP_SUM = 4


SUPPORTED_TRANSFORMS = ['non_negative_derivative']

# Shard by cluster ID to optimize query with respect to a cluster
SHARD_BY_CLUSTER = 'cluster'
# Shard by metric name to optimize query for a given metric cross
# cluster
SHARD_BY_METRIC = 'metric'

@dataclass
class SparseAlgoSetting: #pylint: disable=too-many-instance-attributes
    """
    User configurable settings for an outlier metric
    """
    # The regex pattern to match against the metric series_id
    patterns: List[str]
    # Name of the sparse algorithm
    algo: str
    # Setting for last value delta algorithm
    min_delta: float
    # Setting for percentile window algorithm
    quantile: float
    # Setting for percentile window algorithm
    window_size: int

    # Settings for outlier type
    # Number of bins to use to estimate the distribution
    bin_count: int
    # Number of data points received after which bands are recomputed
    band_computation_points: int
    # Maximum allowable band size (high-low)
    max_band_width: float
    # Minimum probability allowed for data occurrence in a band
    min_band_probability: float
    # Minimum change in band probability required to keep expanding the band
    min_band_probability_change: float
    # Drop the data points before the outlier metric type is ready
    drop_before_ready: bool
    # IF true, then bands are reported in aggregate only
    # If false, then bands are reported per series
    report_aggregated_stats: bool
    # IF aggregated stats is true, then aggregated stats are generated
    # at this interval
    stats_interval: int
    # These tag keys are dropped from the data point metadata for generating
    # aggregated stats
    drop_tags: List[str]


class Schema: #pylint: disable=too-many-instance-attributes
    """
    Wrapper for class schema yaml
    """

    def __init__(self, schema_yaml: dict, batch_mode: bool):
        """
        Constructor for schema
        :param schema_yaml:
        :param batch_mode: If True, we use the batch version of datasources
                           defined in the schema yaml. The mapping of a
                           realtime datasource to a batch datasource is simple.
                           For a reatime datasource named `$ds`, the
                           corresponding batch datasource is called
                           `batch_$ds`.
        """
        self.batch_mode = batch_mode

        if self.batch_mode:
            self.cross_cluster_template_ds = BATCH_DATASOURCE_PREFIX + CROSSCLUSTER_TEMPLATE
            self.cross_cluster_rollup_template_ds = BATCH_DATASOURCE_PREFIX + ROLLUP_DATASOURCE_PREFIX + CROSSCLUSTER_TEMPLATE
        else:
            self.cross_cluster_template_ds = CROSSCLUSTER_TEMPLATE
            self.cross_cluster_rollup_template_ds = ROLLUP_DATASOURCE_PREFIX + CROSSCLUSTER_TEMPLATE

        self._yaml = schema_yaml
        if self.batch_mode:
            Schema._add_batch_prefix_to_datasources(schema_yaml)
        
        self.default_query_granularity = self._yaml[GRANULARITIES][QUERY]
        self.default_segment_granularity = \
            self._yaml[GRANULARITIES][DEFAULT_SEGMENT_GRANULARITY]

        # Exploration datasources
        self.exploration_datasources: Dict[str, str] = dict()
        self._process_exploration_datasources()

        # Field to Datasource mapping
        self.default_datasource: Dict[str, str] = dict()
        self.measurement_prefix_trie: Dict[str, StringTrie] = dict()
        self._process_measurement_prefixes()

        # Stream transform rules
        self.include_transform_regex: Dict[Pattern, Dict] = dict()
        self.transform_types = dict()
        self._process_transform_regex()

        # rollup algorithm rules
        self.rollup_algorithms: Dict[Pattern, str] = dict()
        self.default_rollup_algorithm = ROLLUP_MEAN
        self._process_rollup_algorithms()

        # Rollup windows
        self.rollup_windows: List[int] = self._yaml.get(ROLLUPS, {}).get(
            ROLLUP_WINDOWS)

        # minimum interval at which data points are stored
        self.MIN_STORAGE_INTERVAL: int = 1 # pylint: disable=invalid-name
        if 'MIN_STORAGE_INTERVAL' in self._yaml:
            self.MIN_STORAGE_INTERVAL = self._yaml['MIN_STORAGE_INTERVAL']

        # In memory map of datasource name to their properties
        self._datasource_properties = dict()
        self._create_datasource_properties()

        # Build the cross-cluster patterns and the sparseness disabled patterns
        self.cross_cluster_patterns: List[Tuple[Dict, str]] = list()
        self.sparseness_disabled_patterns: List[Tuple[Dict, int]] = list()
        self._process_cross_cluster_patterns()
        self._process_sparseness_disabled_patterns()

        # Validate the schema to make sure it matches expected rules
        self._validate()

    @staticmethod
    def is_crosscluster_datasource(data_source: str) -> bool:
        return CROSSCLUSTER_TEMPLATE in data_source.split('_')

    @staticmethod
    def _add_batch_prefix_to_datasources(schema_yaml: dict) -> None:
        """
        Prefix all datasources in the schema yaml dict with
        BATCH_DATASOURCE_PREFIX.
        The dict is updated in-place
        """
        updated_datasources = {}
        # Update top level DATASOURCES map
        if DATASOURCES in schema_yaml and schema_yaml[DATASOURCES] is not None:
            for (key, value) in schema_yaml[DATASOURCES].items():
                if key.startswith(BATCH_DATASOURCE_PREFIX):
                    raise ValueError(f"Datasource {key} uses reserved prefix"
                                     f" {BATCH_DATASOURCE_PREFIX}")
                updated_datasources[BATCH_DATASOURCE_PREFIX + key] = value
            schema_yaml[DATASOURCES] = updated_datasources
        # Update exploration rules
        if EXPLORATION_DATASOURCES in schema_yaml and \
                schema_yaml[EXPLORATION_DATASOURCES] is not None:
            updated_rules = {}
            for protocol, datasource in \
                    schema_yaml[EXPLORATION_DATASOURCES].items():
                # Update the exploration datasources
                updated_datasource = BATCH_DATASOURCE_PREFIX + datasource
                updated_rules[protocol] = updated_datasource

            schema_yaml[EXPLORATION_DATASOURCES] = updated_rules

        # Update MEASUREMENT_TO_DATASOURCE_MAPPING & catch all datasources
        if MEASUREMENT_TO_DATASOURCE_MAPPING in schema_yaml and \
            schema_yaml[MEASUREMENT_TO_DATASOURCE_MAPPING] is not None:
            for protocol, conf in \
                    schema_yaml[MEASUREMENT_TO_DATASOURCE_MAPPING].items():
                # Update catch all datasource
                updated_datasource = \
                    BATCH_DATASOURCE_PREFIX + conf[CATCH_ALL_DATASOURCE]
                conf[CATCH_ALL_DATASOURCE] = updated_datasource

                # Update measurement prefix rules
                updated_measurement_mapping = {}
                for (key, value) in conf['rules'].items():
                    updated_measurement_mapping[key] = \
                        BATCH_DATASOURCE_PREFIX + value
                conf['rules'] = updated_measurement_mapping

        # Update rollup datasources
        if ROLLUPS in schema_yaml:
            # Update rollup match rules with new datasources
            for rule in schema_yaml[ROLLUPS].get(ROLLUP_MATCH_RULES, []):
                rule['datasource'] = BATCH_DATASOURCE_PREFIX + \
                    rule['datasource']

            if ROLLUP_CATCH_ALL_DATASOURCE in schema_yaml[ROLLUPS]:
                updated_datasource = \
                    BATCH_DATASOURCE_PREFIX + \
                        schema_yaml[ROLLUPS][ROLLUP_CATCH_ALL_DATASOURCE]
                schema_yaml[ROLLUPS][ROLLUP_CATCH_ALL_DATASOURCE] = \
                    updated_datasource

    def _process_exploration_datasources(self) -> None:
        """
        Parse exploration datasource rules
        """
        exploration_rules = self._yaml.get(EXPLORATION_DATASOURCES)
        if exploration_rules is None:
            return

        for protocol, datasource in exploration_rules.items():
            if datasource not in self._yaml[DATASOURCES]:
                raise ValueError(f"Exploration datasource {datasource} "
                                 f"missing in Datasources section")

            self.exploration_datasources[protocol] = datasource

    def _process_measurement_prefixes(self) -> None:
        """
        Build a trie from measurement prefixes to datasource mapping
        """
        measurement_to_datasource_mapping = self._yaml[
            MEASUREMENT_TO_DATASOURCE_MAPPING]
        if measurement_to_datasource_mapping is None:
            return

        for protocol, conf in measurement_to_datasource_mapping.items():
            # Set default datasource for the protocol
            if conf[CATCH_ALL_DATASOURCE] not in self._yaml[DATASOURCES]:
                raise ValueError(f"Catch all datasource: {datasource} is "
                                 f"missing in Datasources section")
            self.default_datasource[protocol] = conf[CATCH_ALL_DATASOURCE]

            # Build string trie for the rules in this protocol
            self.measurement_prefix_trie[protocol] = StringTrie()
            for measurement_prefix, datasource in conf['rules'].items():
                if datasource not in self._yaml[DATASOURCES]:
                    raise ValueError(
                        f"Datasource: {datasource} in measurement mapping "
                        f"for: {measurement_prefix} is missing in "
                        f"Datasources section")
                self.measurement_prefix_trie[protocol][
                    measurement_prefix] = datasource

    def _process_transform_regex(self) -> None:
        """
        Compile the transform regexes and map each regex to corresponding
        transform
        :return:
        """
        if TRANSFORMS in self._yaml:
            for transform_type, settings in self._yaml[TRANSFORMS].items():
                if transform_type not in SUPPORTED_TRANSFORMS:
                    raise ValueError('Unsupported transform: {}'.
                                     format(transform_type))
                transform_settings = {
                    EXCLUDE_REGEX: [],
                    SUFFIX: settings[SUFFIX],
                    TRANSFORM_TYPE: transform_type
                }
                for regex in settings[EXCLUDE_REGEX]:
                    transform_settings[EXCLUDE_REGEX].append(re.compile(regex))

                for regex in settings[INCLUDE_REGEX]:
                    self.include_transform_regex[re.compile(regex)] = \
                        transform_settings

    def _process_rollup_algorithms(self) -> None:
        """
        compile regex rules and storage the pattern to algorithm map
        """
        if ROLLUPS in self._yaml and ROLLUP_ALGORITHMS in self._yaml[ROLLUPS]:
            self.default_rollup_algorithm = \
                Schema._rollup_function_name_to_id(
                    self._yaml[ROLLUPS][ROLLUP_DEFAULT_ALGORITHM])

            algos = self._yaml[ROLLUPS][ROLLUP_ALGORITHMS]
            for algo, rules in algos.items():
                algo_id = Schema._rollup_function_name_to_id(algo)
                for rule in rules:
                    self.rollup_algorithms[re.compile(rule)] = algo_id

    def _process_cross_cluster_patterns(self) -> None:
        """
        load the cross cluster patterns
        """
        if DATASOURCES in self._yaml \
            and self.cross_cluster_template_ds in self._yaml[DATASOURCES] \
            and NUM_SHARDS in self._yaml[DATASOURCES][self.cross_cluster_template_ds] \
            and CROSS_CLUSTER_PATTERNS in self._yaml:
            num_shards = self._yaml[DATASOURCES][
                self.cross_cluster_template_ds][NUM_SHARDS]
            for pattern_data in self._yaml[CROSS_CLUSTER_PATTERNS]:
                pattern_text = pattern_data[CROSS_CLUSTER_PATTERNS_PATTERN]
                hash_key = pattern_data[CROSS_CLUSTER_PATTERNS_HASHKEY]
                shard = zlib.adler32(hash_key.encode()) % num_shards
                pattern = {}
                token_list = pattern_text.split('.')
                for i, item in enumerate(token_list):
                    if item == '*':
                        continue
                    if i == len(token_list) - 1:
                        key = FIELD
                    else:
                        key = '%s%d' % (TOKEN_TAG_PREFIX, 3 + i)
                    pattern[key] = item
                self.cross_cluster_patterns.append((pattern, str(shard)))

    def _process_sparseness_disabled_patterns(self) -> None:
        """
        load the sparseness disabled patterns
        """
        if self.batch_mode:
            # We will never disable sparseness in batch ingest
            return

        if SPARSENESS_DISABLED_PATTERNS not in self._yaml:
            # No patterns defined
            return

        for rule in self._yaml[SPARSENESS_DISABLED_PATTERNS]:
            pattern_text = rule[SPARSENESS_DISABLED_RULE_PATTERN]
            interval = rule[SPARSENESS_DISABLED_RULE_INTERVAL]
            pattern = {}
            for tag_key, value_pattern in pattern_text.items():
                if pattern == '*':
                    continue
                pattern[tag_key] = value_pattern
            self.sparseness_disabled_patterns.append((pattern, interval))

    @staticmethod
    def _rollup_function_name_to_id(func_name: str) -> int:
        """
        maps the string function to integer id
        :param func_name:
        :return: id
        """
        if func_name == 'max':
            return ROLLUP_MAX
        if func_name == 'last':
            return ROLLUP_LAST
        if func_name == 'mean':
            return ROLLUP_MEAN
        if func_name == 'sum':
            return ROLLUP_SUM
        raise ValueError('unknown ROLLUP function: ' + func_name)

    def _create_datasource_properties(self) -> None:
        """
        Creates a mapping of datasource name to its properties
        """
        datasources = self.datasources()
        for datasource, query_granularity, segment_granularity in datasources:
            self._datasource_properties[datasource] = (query_granularity,
                                                       segment_granularity)

    def _validate(self) -> None:
        """
        Validates the schema is properly authored
        """
        # Validate datasource authoring. Check both the default datasource and
        # each individual datasource spec
        if len(self.default_datasource) == 0:
            raise ValueError('Default Datasource must be defined')

        for datasource, spec in self._yaml[DATASOURCES].items():
            if spec is None:
                continue
            if NUM_SHARDS in spec:
                if not isinstance(spec[NUM_SHARDS], int):
                    raise ValueError('Expected integer num_shards for '
                                     'datasource %s' % datasource)
                if 'shard_tag_key' not in spec:
                    raise ValueError('Missing shard_tag_key for sharded '
                                     'datasource %s' % datasource)

        # validate rollups if defined (optional)
        if ROLLUPS in self._yaml:
            if ROLLUP_WINDOWS not in self._yaml[ROLLUPS]:
                raise ValueError('Rollup windows must be defined')
            if not isinstance(self._yaml[ROLLUPS][ROLLUP_WINDOWS], list):
                raise ValueError('Rollup windows must be a list')

            for window in self._yaml[ROLLUPS][ROLLUP_WINDOWS]:
                if not isinstance(window, int):
                    raise ValueError('Rollup window must be an integer: %s' %
                                     window)

            if ROLLUP_CATCH_ALL_DATASOURCE not in self._yaml[ROLLUPS]:
                raise ValueError('Rollup catch all datasource must be defined'
                                 'under rollups')

            # Validate the presence of fallback rollup datasource
            catch_all_ds = self.get_rollup_datasource(None, {})
            if catch_all_ds not in self._yaml[DATASOURCES]:
                raise ValueError('Rollup catch all datasource {} not '
                                 'defined in datasources list'.
                                 format(catch_all_ds))

            # Validate rollup datasource match rules
            for rule in self._yaml[ROLLUPS][ROLLUP_MATCH_RULES]:
                if not isinstance(rule, dict):
                    raise ValueError('Rollup match rule %s needs to be a '
                                     'dict with attributes' % rule)
                if 'match_tag_key' not in rule:
                    raise ValueError('Rollup match rule %s needs to have '
                                     'a match_tag_key attribute' % rule)
                if 'datasource' not in rule:
                    raise ValueError('Rollup match rule %s needs to have '
                                     'a datasource attribute' % rule)

                # Validate that the datasource specified exists in the
                # datasources spec
                sharded_ds = rule['datasource']
                if sharded_ds not in self._yaml[DATASOURCES]:
                    raise ValueError('Rollup sharded datasource {} not '
                                     'defined in datasources list'.
                                     format(sharded_ds))

        # Validate cross-cluster rules if defined
        if CROSS_CLUSTER_PATTERNS in self._yaml:
            for rule in self._yaml[CROSS_CLUSTER_PATTERNS]:
                if not isinstance(rule, dict):
                    raise ValueError('Cross-cluster pattern rule %s needs '
                                     'to be a dict with attributes' % rule)
                if CROSS_CLUSTER_PATTERNS_PATTERN not in rule:
                    raise ValueError('Cross-cluster pattern rule %s needs '
                                     'to have an entry for "%s"' %
                                     (rule, CROSS_CLUSTER_PATTERNS_PATTERN))
                if CROSS_CLUSTER_PATTERNS_HASHKEY not in rule:
                    raise ValueError('Cross-cluster pattern rule %s needs '
                                     'to have an entry for "%s"' %
                                     (rule, CROSS_CLUSTER_PATTERNS_HASHKEY))

        # Validate sparseness disabled patterns if defined
        if SPARSENESS_DISABLED_PATTERNS in self._yaml:
            for rule in self._yaml[SPARSENESS_DISABLED_PATTERNS]:
                if not isinstance(rule, dict):
                    raise ValueError('Sparseness disabled pattern rule %s needs '
                                     'to be a dict with attributes' % rule)
                if SPARSENESS_DISABLED_RULE_PATTERN not in rule:
                    raise ValueError('Cross-cluster pattern rule %s needs '
                                     'to have an entry for "%s"' %
                                     (rule, SPARSENESS_DISABLED_RULE_PATTERN))
                if SPARSENESS_DISABLED_RULE_INTERVAL not in rule:
                    raise ValueError('Cross-cluster pattern rule %s needs '
                                     'to have an entry for "%s"' %
                                     (rule, SPARSENESS_DISABLED_RULE_INTERVAL))
                if not isinstance(rule[SPARSENESS_DISABLED_RULE_INTERVAL],
                                  int):
                    raise ValueError(
                        'Cross-cluster pattern rule %s needs to '
                        'have an integer interval instead of %s' %
                        (rule, rule[SPARSENESS_DISABLED_RULE_INTERVAL]))

        if not self.batch_mode:
            for (datasource, _) in self._yaml[DATASOURCES].items():
                if datasource.startswith(BATCH_DATASOURCE_PREFIX):
                    raise ValueError(f"Datasource {datasource} uses reserved "
                                     f"prefix {BATCH_DATASOURCE_PREFIX}")

    @staticmethod
    def load_from_file(schema_file_path: str,
                       batch_mode: bool = False) -> Schema:
        """
        Load the schema from file
        :param schema_file_path: Path of the schema yaml file
        :param batch_mode: Whether to use batch mode while loading schema
        :return:
        """
        logger.info('Schema.load_from_file: %s', schema_file_path)
        with open(schema_file_path) as filename:
            schema_yaml = yaml.safe_load(filename)
            return Schema(schema_yaml, batch_mode)

    def get_measurements(self) -> List[str]:
        """
        Return the measurements loaded from the YAML
        """
        return self._yaml[MEASUREMENTS]

    def sparse_algos(self) -> List[SparseAlgoSetting]:
        """
        Get a dictionary of sparse algorithms to list of metric regexes
        the regex match the metrics for which the sparse algorithm will
        apply
        :return:
        """
        sparse_algo_settings: List[SparseAlgoSetting] = list()
        for s in self._yaml['sparse_algos']:
            sparse_algo_settings.append(
                SparseAlgoSetting(
                    s['pattern'] if 'pattern' in s else list(),
                    s['algo'],
                    s['min_delta'] if 'min_delta' in s else None,
                    s['quantile'] if 'quantile' in s else None,
                    s['window_size'] if 'window_size' in s else None,
                    s['bin_count'] if 'bin_count' in s else None,
                    s['band_computation_points']
                    if 'band_computation_points' in s else None,
                    s['max_band_width'] if 'max_band_width' in s else None,
                    s['min_band_probability']
                    if 'min_band_probability' in s else None,
                    s['min_band_probability_change']
                    if 'min_band_probability_change' in s else None,
                    s['drop_before_ready']
                    if 'drop_before_ready' in s else None,
                    s['report_aggregated_stats']
                    if 'report_aggregated_stats' in s else None,
                    s['stats_interval'] if 'stats_interval' in s else None,
                    s['drop_tags'] if 'drop_tags' in s else list()
                ))
        return sparse_algo_settings

    def datasources(self) -> List[Tuple[str, int, int]]:
        """
        Get list of datasource names
        :return: List of datasources along with query granularity and segment
        granularity
        """
        # check if any of the data sources are sharded
        templates = self._yaml[DATASOURCES]
        datasources = list()
        for template in templates.keys():
            settings = templates[template]
            if settings is not None:
                shard_suffixes = [None]
                window_suffixes = [None]
                if NUM_SHARDS in settings:
                    shards = settings[NUM_SHARDS]
                    shard_suffixes = Schema._shard_suffixes(shards)
                if WINDOWS in settings:
                    window_suffixes = settings[WINDOWS]
                for shard_suffix in shard_suffixes:
                    for window_suffix in window_suffixes:
                        query_granularity, segment_granularity = \
                            self._get_datasource_granularities(settings,
                                                               window_suffix)
                        datasource_name = Schema.template_to_datasource(
                            template, shard_suffix, window_suffix)
                        datasources.append((datasource_name,
                                            query_granularity,
                                            segment_granularity))
            else:
                query_granularity, segment_granularity = \
                    self._get_datasource_granularities()
                datasources.append((template,
                                    self.default_query_granularity,
                                    segment_granularity))
        return datasources

    def _get_datasource_granularities(self,
                                      settings: Dict = None,
                                      window_suffix: int = None):
        """
        Get datasource query and segment granularities
        :param settings:
        :param window_suffix:
        :return: query_granularity, segment_granularity
        """
        if window_suffix is not None:
            query_granularity = window_suffix
        elif settings is not None and QUERY_GRANULARITY in settings:
            query_granularity = settings[QUERY_GRANULARITY]
        else:
            query_granularity = self.default_query_granularity

        if settings is not None and SEGMENT_GRANULARITY in settings:
            segment_granularity = settings[SEGMENT_GRANULARITY]
        else:
            segment_granularity = self.default_segment_granularity
        return query_granularity, segment_granularity

    def get_crosscluster_shard(self, tags: dict) -> str:
        """
        Find the shard value to be used while accessing the crosscluster
        datasource for the given metric.

        If metric doesn't match any of the crosscluster patterns, we return
        None.
        """
        # Should not be called from a find query
        return Schema._get_crosscluster_shard(self.cross_cluster_patterns,
                                              tags, find_query=False)

    def compose_crosscluster_datasource(self, shard_suffix: str,
                                        window: int = None) -> str:
        """
        Generate the exact crosscluster datasource name to be used using the
        provided shard suffix and window value
        """
        if window is not None:
            template = self.cross_cluster_rollup_template_ds
        else:
            template = self.cross_cluster_template_ds
        return Schema.template_to_datasource(
                                        template, 
                                        shard_suffix=shard_suffix,
                                        window_suffix=window)

    def get_rollup_datasource(self, window: int, tags: dict,
                              mode: str = SHARD_BY_CLUSTER) -> str:
        """
        Get the rollup datasource
        :param window:
        :param cluster_id:
        :return:
        """
        if mode == SHARD_BY_METRIC:
            shard_suffix = Schema._get_crosscluster_shard(
                self.cross_cluster_patterns, tags)
            if shard_suffix is None:
                raise ValueError('Cross-cluster query does not match any '
                                 'of the whitelisted patterns. Please '
                                 'make sure the metrics needed by your query '
                                 'have been added to the cross-cluster '
                                 'whitelist on AnomalyDB')
            return self.compose_crosscluster_datasource(shard_suffix, window)
        
        template = None
        shard_tag_key = None
        if tags:
            # No point iterating over match rules if no metric tags were
            # provided
            for rule in self._yaml[ROLLUPS][ROLLUP_MATCH_RULES]:
                match_tag_key = rule['match_tag_key']
                if match_tag_key not in tags:
                    # Metric doesn't contain necessary tag
                    continue
                if ('match_tag_value' in rule and
                        tags.get(match_tag_key) != rule['match_tag_value']):
                    # Metric contains tag but doesn't match the necessary value
                    continue

                # If we reached here, rule matches. We want to find the first
                # rule that matches, so we can break the loop safely
                template = rule['datasource']
                break

        if template is None:
            # No matching rule was found. Use fallback rollup datasource
            template = self._yaml[ROLLUPS][ROLLUP_CATCH_ALL_DATASOURCE]
            return Schema.template_to_datasource(template,
                                                 window_suffix=window)

        settings = self._yaml[DATASOURCES][template]
        if settings is None or NUM_SHARDS not in settings:
            # Seems to be a non-sharded datasource
            return Schema.template_to_datasource(template,
                                                 window_suffix=window)

        shard_tag_key = settings['shard_tag_key']
        shard_tag_value = tags.get(shard_tag_key)
        if shard_tag_value is None:
            raise ValueError('Rollup datasource %s expects to use tag '
                             'key %s for sharding but found value=%s' %
                             (template, shard_tag_key, shard_tag_value))
        shard_suffix = Schema._get_shard(settings[NUM_SHARDS], shard_tag_value)
        return Schema.template_to_datasource(template,
                                             shard_suffix=shard_suffix,
                                             window_suffix=window)

    @staticmethod
    def _get_crosscluster_shard(
            cross_cluster_patterns: List[(Dict[str, str], str)],
            tags: Dict[str, str],
            find_query: bool = False) -> str:
        """
        Check if given metric matches any of the cross-cluster whitelist
        patterns. If found, return the cross-cluster datasource shard value to
        be used. Otherwise, return None

        :param cross_cluster_patterns: Cross-cluster whitelist rules
        :param tags: Tags corresponding to metric
        :param find_query: Boolean indicating whether a find query is
        requesting a match.

        Normally if a whitelist rule uses something like "clusters.*.*.X.Y.Z"
        and the metric only has "clusters.*.*.X.Y", we return None. But for
        find queries, we will return True
        """
        for pattern, shard in cross_cluster_patterns:
            found = True
            for key in pattern:
                pattern_value = pattern[key]
                tag_value = tags.get(key)
                if tag_value is None:
                    if find_query:
                        # Tag pattern is present in whitelist rule but not in
                        # the query metric. This is expected for find queries
                        continue

                    # This is not a find query. Therefore a missing token
                    # means no match.
                    found = False
                    break
                if not fnmatch.fnmatch(tag_value, pattern_value):
                    # Cross-cluster whitelist rule contains a filter rule
                    # for this token but input metric doesn't contain any
                    # such token
                    found = False
                    break
            if found:
                return shard
        return None

    def get_sparseness_disabled_interval(self, tags: Dict[str, str]) -> int:
        """
        Check if given metric matches any of the patterns for sparseness
        disabled metrics, and return the expected reporting interval for the
        series.

        If match found, return the expected reporting interval.
        Otherwise, return None

        :param tags: Tags corresponding to metric
        """
        for pattern, interval in self.sparseness_disabled_patterns:
            found = True
            for key in pattern:
                pattern_value = pattern[key]
                tag_value = tags.get(key)
                if tag_value is None:
                    # A missing token means no match.
                    found = False
                    break
                if not fnmatch.fnmatch(tag_value, pattern_value):
                    # Metric token value doesn't match the expected pattern for
                    # the token
                    found = False
                    break
            if found:
                return interval
        return None

    def is_sparseness_disabled(self, tags: Dict[str, str]) -> bool:
        """
        Return True if given metric matches any of the patterns for sparseness
        disabled metrics. Otherwise return False
        """
        interval = self.get_sparseness_disabled_interval(tags)
        return interval is not None

    def get_datasource(self, measurement: str, tags: Dict[str, str],
                       protocol: str,
                       mode: str = SHARD_BY_CLUSTER,
                       find_query: bool = False) -> List[str]:
        """
        Get the measurement to use for the measurement.
        Matches the longest sequence
        :param measurement:
        :param tags:
        :param protocol: Either 'graphite' or 'influx'.
        :param mode: Indicating the strategy to be used for selecting a
        datasource. Normally we select the sharded datasource based on cluster
        ID, but for cross-cluster metrics will be selected on the basis of
        metric name.
        :param find_query: Boolean indicating whether we are trying to find the
        datasource for a find query or not.
        :return: datasource name
        """
        if mode == SHARD_BY_METRIC:
            shard_suffix = Schema._get_crosscluster_shard(
                self.cross_cluster_patterns, tags, find_query=find_query)
            if shard_suffix is None:
                raise ValueError('Cross-cluster query does not match any '
                                 'of the whitelisted patterns. Please '
                                 'make sure the metrics needed by your query '
                                 'have been added to the cross-cluster '
                                 'whitelist on AnomalyDB')
            return self.compose_crosscluster_datasource(shard_suffix)

        # Make sure protocol is supported
        assert protocol in self.measurement_prefix_trie, \
            ("Protocol %s not found in "
             "measurement_to_datasource_mapping" % protocol)

        # Determine longest matching prefix based on the rules defined for the
        # given protocol
        template = self.measurement_prefix_trie[protocol].longest_prefix_value(
            measurement, self.default_datasource[protocol])
        return self._get_sharded_data_source(measurement, template, tags)

    def get_shard_tag_value(self, measurement: str, tags: Dict[str, str], \
                            protocol: str) -> str:
        """
        Retrieve the tag value for cluster sharding from the given tags
        Typical the tag value is cluster ID, which can be '*'
        """
        template = self.measurement_prefix_trie[protocol].longest_prefix_value(
            measurement, self.default_datasource[protocol])
        settings = self._yaml[DATASOURCES][template]
        if settings is None:
            return None
        shard_tag_key = settings['shard_tag_key']
        shard_tag_value = tags.get(shard_tag_key)
        return shard_tag_value

    def _get_sharded_data_source(self,
                                 measurement: str,
                                 template: str,
                                 tags: Dict[str, str]) -> str:
        """
        Checks if the template datasource is sharded if so returns the
        appropriate shard for the cluster
        :param measurement:
        :param template:
        :param cluster_id:
        :return:
        """
        # check if this datasource is further sharded
        settings = self._yaml[DATASOURCES][template]
        if settings is None or NUM_SHARDS not in settings:
            return template

        shard_tag_key = settings['shard_tag_key']
        shard_tag_value = tags.get(shard_tag_key)

        if (shard_tag_value is None or '*' in shard_tag_value or
                shard_tag_value == ''):
            raise ValueError('Measurement:{} matched Datasource: {} '
                            'which is sharded '
                            'by tag {} but no specific tag_value :{} is '
                            'defined. Cross cluster queries for datasources '
                            'sharded by cluster are not supported yet'.
                            format(measurement, template, shard_tag_key,
                                    shard_tag_value))
        shard_suffix = Schema._get_shard(settings[NUM_SHARDS], shard_tag_value)

        return Schema.template_to_datasource(template, shard_suffix=shard_suffix)

    def get_dashboard_datasource(self,
                                 tags: Dict[str, str],
                                 cluster_id: str) -> str:
        """
        Checks if this data point belongs to a dashboard datasource
        :param tags:
        :param cluster_id:
        :returns: None if there is no match or the dashboard datasource
        """
        if DASHBOARD_QUERIES not in self._yaml:
            logger.debug('get_dashboard_datasource: '
                         'tags=%s cluster_id=%s '
                         'DASHBOARD_QUERIES DISABLED',
                         json.dumps(tags), cluster_id)
            return None

        # check if this cluster_id belongs to clusters optimized for
        # dashboard queries
        if cluster_id not in self._yaml[DASHBOARD_QUERIES][CLUSTERS]:
            return None

        for datasource in self._yaml[DASHBOARD_QUERIES]:
            tag_sets = self._yaml[DASHBOARD_QUERIES][datasource]
            for required_tags in tag_sets:
                # check if the required_tags match the provided tags
                # tags provided must have the required tags
                valid = True
                for tag_key in required_tags:
                    if tag_key not in tags:
                        valid = False
                        break
                    # check if the tag value is among the expected values
                    if tags[tag_key] not in required_tags[tag_key]:
                        valid = False
                        break
                if valid:
                    logger.debug('get_dashboard_datasource: '
                                'tags=%s cluster_id=%s. FOUND',
                                json.dumps(tags), cluster_id)
                                
                    return self._get_sharded_data_source(
                        tags[FIELD], datasource, tags)
                                                
        logger.debug('get_dashboard_datasource: '
                     'tags=%s cluster_id=%s. NO TAGSET MATCHED',
                     json.dumps(tags), cluster_id)
        # No match found
        return None

    def get_forced_rewrite_interval(self, datasource: str) -> int:
        """
        Forced rewrite is half of segment granularity.
        we force a write at half the segment granularity to ensure
        we don't have to query last value across segments
        and every segment is safe to purge
        :param datasource:
        :return:
        """
        segment_granularity = self.get_segment_granularity(datasource)
        if isinstance(segment_granularity, int):
            return segment_granularity / 2

        # Need to convert string to integer
        assert isinstance(segment_granularity, str)
        assert segment_granularity in SUPPORTED_GRANULARY_STRINGS
        return GRANULARITY_STRING_TO_SECS[segment_granularity] / 2

    def get_segment_granularity(self, datasource: str):
        """
        Get the segment granularity for the datasource
        :param datasource:
        :return:
        """
        if datasource not in self._datasource_properties:
            raise ValueError('Unknown datasource: {} known datasources:{}'
                             .format(datasource,
                                     json.dumps(self._datasource_properties)))
        return self._datasource_properties[datasource][1]

    def get_down_sampling_function(self, datasource: str, field: str) -> str:
        """
        Get the down sampling function for the measurement field from the
        schema definition
        :param datasource:
        :param field:
        :return: down sampling function
        """
        return self._get_field_level_attribute(datasource,
                                               field,
                                               'down_sampling',
                                               'mean')

    def get_rollup_function(self, series_id: str) -> int:
        """
        :param series_id:
        :return: Id of the rollup function
        """
        # For counters we return last
        for pattern, algo in self.rollup_algorithms.items():
            if pattern.match(series_id):
                return algo
        return self.default_rollup_algorithm

    def _get_field_level_attribute(self,
                                   datasource: str,
                                   field: str,
                                   attr: str,
                                   default_value):
        """
        Get the attribute if defined for the measurement and field
        otherwise return the provided default_value
        :param datasource:
        :param field:
        :param attr:
        :param default_value: type not defined, because it depends on type of
        attribute
        :return: value of the attribute (type unknown)
        """
        _datasource = Schema._datasource_to_template(datasource)
        if _datasource not in self._yaml[DATASOURCES]:
            raise ValueError('datasource={} not defined in schema'.
                             format(_datasource))

        datasource_def = self._yaml[DATASOURCES][_datasource]
        if datasource_def is not None and field in datasource_def:
            field_def = datasource_def[field]
            if field_def is not None and attr in field_def:
                return field_def[attr]
        return default_value

    def check_transform_type(self, field: str) -> dict:
        """
        Checks which transform applies
        :param field:
        :return: transform_settings if there is a match
        """
        for pattern in self.include_transform_regex:
            if pattern.match(field):
                transform_settings = self.include_transform_regex[pattern]
                for exclude_pattern in transform_settings[EXCLUDE_REGEX]:
                    if exclude_pattern.match(field):
                        continue
                return transform_settings
        return None

    @staticmethod
    def _shard_suffixes(num_shards: int) -> List[str]:
        """
        Generates a list of cluster shard prefixes based on num shards
        the cluster_id can be matched against the shard prefix to pick
        the datasource
        :param num_shards:
        :return:
        """
        return [str(x) for x in range(0, num_shards)]

    @staticmethod
    def _get_shard(num_shards: int, shard_tag_value: str) -> str:
        """
        Pick the appropriate shard for the tag value given the number of shards
        :param num_shards:
        :param shard_tag_value:
        :return: shard prefix
        """
        return str(zlib.adler32(shard_tag_value.encode()) % num_shards)

    @staticmethod
    def template_to_datasource(template: str,
                               shard_suffix: str = None,
                               window_suffix: int = None) -> str:
        """
        Appends the shard_suffix to the data source template
        :param template:
        :param shard_suffix:
        :param window_suffix:
        :return: sharded datasource name
        """
        data_source = template
        if shard_suffix is not None:
            data_source += DATASOURCE_SHARD_SEP + shard_suffix
        if window_suffix is not None:
            data_source += DATASOURCE_WINDOW_SEP + str(window_suffix)
        return data_source

    @staticmethod
    def _datasource_to_template(datasource: str) -> str:
        """
        Checks if the datasource is sharded if so extracts the template name
        :param datasource:
        :return: template
        """
        if DATASOURCE_SHARD_SEP in datasource:
            return datasource.split(DATASOURCE_SHARD_SEP)[0]
        if DATASOURCE_WINDOW_SEP in datasource:
            return datasource.split(DATASOURCE_WINDOW_SEP)[0]
        return datasource

