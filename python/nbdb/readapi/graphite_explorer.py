"""
Graphite explorer module
"""
from __future__ import annotations
import logging
import time
from typing import List

# Its there not sure why pylint is unable to find it
# pylint: disable-msg=E0611  # No Name In Module
from lru import LRU

from nbdb.common.data_point import TOKEN_TAG_PREFIX, TOKEN_COUNT, FIELD
from nbdb.common.telemetry import Telemetry
from nbdb.config.settings import Settings
from nbdb.readapi.druid_reader import DruidReader, DruidCursor
from nbdb.schema.schema import Schema
from nbdb.schema.schema import SHARD_BY_CLUSTER, SHARD_BY_METRIC

from pyformance import time_calls

logger = logging.getLogger(__name__)


class GraphiteExplorer:
    """
    Explorer for the data based graphite.
    This is limited support only.
    Main limitation is in discovering the schema scattered across
    data sources. Once the datasource is identified
    the functionality matches the graphite functionality
    """

    # Global singleton
    inst: GraphiteExplorer = None

    def __init__(self,
                 druid_reader: DruidReader,
                 sparse_algos_settings: Settings,
                 graphite_cache_size: int = 100000) -> None:
        """

        :param druid_reader:
        """
        self.druid_reader: DruidReader = druid_reader
        self.sparse_algos_settings = sparse_algos_settings
        self._data = LRU(graphite_cache_size)

    @staticmethod
    def instantiate(druid_reader: DruidReader,
                    sparse_algos_settings: Settings) -> None:
        """

        :param druid_reader:
        :return:
        """
        if GraphiteExplorer.inst is None:
            GraphiteExplorer.inst = GraphiteExplorer(druid_reader,
                                                     sparse_algos_settings)

    @staticmethod
    def _initial_discovery():
        """
        This is a limited scope implementation for flat schema.

        Starting discovery is intended to avoid scanning too many series for
        the first token
        :return:
        """
        # We only have two types of Graphite series in AnomalyDB right now:
        # clusters.* and internal.*
        return [
            {
                "allowChildren": 1,
                "expandable": 1,
                "leaf": 0,
                "id": 'clusters',
                "text": 'clusters',
                "context": {}
            },
            {
                "allowChildren": 1,
                "expandable": 1,
                "leaf": 0,
                "id": 'internal',
                "text": 'internal',
                "context": {}
            }
        ]

    # pylint: disable-msg=R0914  # Too Many Locals
    # pylint: disable-msg=R0913  # Too Many Arguments
    def _fetch_metadata(self,
                        datasource: str,
                        is_non_leaf: bool,
                        tokens: List[str],
                        start_epoch: int,
                        end_epoch: int,
                        user: str) -> DruidCursor:
        """
        Construct the query to look up metadata and return the cursor
        plus the list of tokens to lookup
        :param datasource: data source to query the metadata for
        :param is_non_leaf: is this known to be a non-leaf lookup
        :param tokens: list of ordered token values of the prefix
        :param start_epoch: Start of time range for metadata lookup
        :param end_epoch: End of time range for metadata lookup
        :param user: User running the query
        :return: cursor
        """
        filters = []
        columns = []
        for i, token in enumerate(tokens):
            if not is_non_leaf and i == len(tokens) - 1:
                # If this is the last token and we want to fetch leaf nodes,
                # then we want to use the "field" column
                column_name = FIELD
            else:
                # Either this is not the last token or we only want to fetch
                # non-leaf nodes. Column name is the usual "tkn%d" format
                column_name = f'{TOKEN_TAG_PREFIX}{i}'
            columns += [column_name]

            if token == '*':
                # Wildcard token. No need to add a filter clause for this token
                continue

            # Token can contain set pattern. Eg. {4_1,5_2*}
            word_filters = []
            for word in token.strip('{').strip('}').split(','):
                if '*' in word:
                    # Sometimes the pattern can be a like a partial wildcard
                    # pattern. Eg. SDFS Disk ID = '*5000*' or Node = 'RVM*'
                    word_pattern = word.replace('*', '%')
                    word_filters.append(
                        f'{column_name} LIKE \'{word_pattern}\'')
                else:
                    word_filters.append(f'{column_name} = \'{word}\'')

            # We need all word filter clauses to be separated by an OR clause
            filters += ['({})'.format(' OR '.join(word_filters))]

        # Add the token count filter based on whether we are requesting leaf or
        # non-leaf series
        num_tokens = len(tokens)
        if is_non_leaf:
            # We want to fetch non-leaf series ie. series which have more
            # tokens than the requested pattern
            filters += [f'{TOKEN_COUNT} > {num_tokens}']
        else:
            # We want to fetch leaf series ie. series which have exactly the
            # same number of tokens as in the requested pattern
            filters += [f'{TOKEN_COUNT} = {num_tokens}']

        filter_clause = ' AND '.join(filters)
        filter_clause += " AND " \
                         "MILLIS_TO_TIMESTAMP({start_epoch}) <= __time  " \
                         "AND __time < MILLIS_TO_TIMESTAMP({end_epoch})"\
            .format(start_epoch=start_epoch*1000, end_epoch=end_epoch*1000)

        # We have seen ~100 node clusters at max, so pick 200 rows to ensure
        # most tokens get loaded during exploration
        limit_rows = 200

        tokens_clause = ','.join(columns)
        query = "SELECT " \
                " {tokens_clause}" \
                " FROM \"{datasource}\"" \
                " WHERE {filter_clause}" \
                " GROUP BY {tokens_clause}"\
                " LIMIT {limit_rows}".\
            format(tokens_clause=tokens_clause,
                   datasource=datasource,
                   filter_clause=filter_clause,
                   limit_rows=limit_rows)
        logger.info('DruidReader.browse_dot_schema: %s', query)
        cursor = self.druid_reader.execute(query, datasource, user)
        return cursor

    # pylint: disable-msg=R0914  # Too Many Locals
    @time_calls
    def browse_dot_schema(self,
                          schema: Schema,
                          metric_name: str,
                          start_epoch: int,
                          end_epoch: int,
                          user: str) -> List:
        """
        Browse the schema
        :param metric_name: flat metric name
        :param start_epoch:
        :param end_epoch:
        :param user:
        :return:
        """
        if end_epoch is None:
            if schema.batch_mode:
                # For batch mode, we expect a delay of 24h before metrics
                # appear because bundles are uploaded at that freq
                timestamp = time.time() - 24 * 60 * 60
            else:
                # Default to now
                timestamp = time.time()
            # Default to now but aligned on hourly boundaries. This ensures
            # that we get a better cache hit rate, but it also means new series
            # will not be discovered for atleast an hour
            end_epoch = int(timestamp / 3600) * 3600
        if start_epoch is None:
            # Default to last 12 hours. We are guaranteed to have atleast one
            # datapoint
            start_epoch = (end_epoch -
                           self.sparse_algos_settings.forced_write_interval)

        cache_key = '{}.{}.{}'.format(metric_name, start_epoch, end_epoch)
        result = None

        # Check if cache has key
        timer = Telemetry.inst.registry.timer(
            "ReadApi.GraphiteExplorer.lru_cache.get_calls",
            tag_key_values=[f"User={user}"])
        with timer.time():
            result = self._data.get(cache_key)


        # Record hit rate stats
        hits, misses = self._data.get_stats()
        if hits + misses > 0:
            Telemetry.inst.registry.gauge(
                "ReadApi.GraphiteExplorer.lru_cache.hit_rate",
                tag_key_values=[f"User={user}"]
            ).set_value(float(hits) / (hits + misses))

        if result:
            return result

        # identify datasource
        tokens = metric_name.split('.')
        shard_tags = {}
        if len(tokens) == 1:
            return self._initial_discovery()
        else:
            for idx, token in enumerate(tokens):
                shard_tags[TOKEN_TAG_PREFIX + str(idx)] = token

        # For flat Graphite metrics, we treat the metric name without the
        # "clusters.<CID>.<NID>" prefix as the measurement
        measurement_prefix = '.'.join(tokens[3:])

        is_non_leaf = False
        if not metric_name.startswith('clusters'):
            datasource = schema.get_datasource(measurement_prefix, None,
                                               protocol="graphite")
        else:
            # First token is 'clusters'
            if schema.batch_mode:
                # Batch mode does not have raw datapoints. It only has rollup
                # datapoints
                datasource = schema.get_rollup_datasource(window=600,
                                                          tags=shard_tags)
            else:
                shard_tag_value = schema.get_shard_tag_value('', shard_tags,
                                                             protocol="graphite")
                if shard_tag_value == '*':
                    # Seems like a clusters.* query. We need to select
                    # datasources sharded by metric name
                    select_mode = SHARD_BY_METRIC
                else:
                    # Seems like the cluster ID is not a wildcard entry. We
                    # need to pick the datasource sharded based on cluster ID
                    select_mode = SHARD_BY_CLUSTER
                datasource = schema.get_datasource('', shard_tags,
                                                   protocol="graphite",
                                                   mode=select_mode,
                                                   find_query=True)

        prefixes = dict()
        start_time = time.perf_counter()
        # Fetch non-leaf and leaf nodes one by one
        for is_non_leaf in [True, False]:
            cursor = self._fetch_metadata(datasource, is_non_leaf, tokens,
                                          start_epoch, end_epoch, user)
            row = cursor.fetchone()
            while row is not None:
                # skip empty values
                logger.debug('DruidReader.browse_dot_schema: |%s|', row)
                _id = '.'.join(row)
                if not is_non_leaf:
                    # If this is a non leaf node prefix, we want to use the
                    # field column
                    prefix = row[FIELD]
                else:
                    # For non-leaf nodes, column name is the usual "tkn%d"
                    # format
                    column_name = f"{TOKEN_TAG_PREFIX}{len(tokens)-1}"
                    prefix = row[column_name]

                prefixes[prefix] = {"allowChildren": 1 if is_non_leaf else 0,
                                    "expandable": 1 if is_non_leaf else 0,
                                    "leaf": 0 if is_non_leaf else 1,
                                    "id": _id,
                                    "text": prefix,
                                    "context": {}
                                    }
                row = cursor.fetchone()

        druid_fetch_time = time.perf_counter() - start_time
        Telemetry.inst.registry.histogram(
            "ReadApi.GraphiteExplorer.DruidReader.fetch_time",
            tag_key_values=[f"User={user}"]
        ).add(druid_fetch_time)
        response = sorted(prefixes.values(), key=lambda x: x['text'])

        # Store entry in LRU cache
        timer = Telemetry.inst.registry.timer(
            "ReadApi.GraphiteExplorer.lru_cache.set_calls",
            tag_key_values=[f"User={user}"])
        with timer.time():
            self._data[cache_key] = response

        # Record size of cache
        Telemetry.inst.registry.gauge(
            "ReadApi.GraphiteExplorer.lru_cache.size",
            tag_key_values=[f"User={user}"]
        ).set_value(len(self._data))

        # Return response
        return response
