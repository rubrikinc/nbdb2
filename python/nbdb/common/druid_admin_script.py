"""
Command line tool to perform administrative tasks on druid
"""

import logging
import traceback
from datetime import datetime

from absl import flags, app

from nbdb.common.druid_admin import DruidAdmin, REINGEST_PERIODS_SUPPORTED
from nbdb.common.druid_admin import SUPPORTED_GRANULARY_STRINGS
from nbdb.config.settings import Settings

FLAGS = flags.FLAGS
flags.DEFINE_boolean('debug', False, 'Enable debug level logs')
flags.DEFINE_string('command', None,
                    'supported commands:'
                    '\n list_datasources: '
                    '\n segments: Load or drop segments in an interval'
                    '\n data_retention: Create or delete a retention rule'
                    '\n update_ingest: Update Kafka ingest spec for datasources'
                    '\n reingest: Reingest data for datasource'
                    '\n compact: specify a compaction policy'
                    '\n mm_worker_strategy: Specify MM worker strategy')
flags.DEFINE_string('start_date', None,
                    'start date string in ISO format – yyyy-mm-dd '
                    'example: 2020-06-08')
flags.DEFINE_string('end_date', None,
                    'end date string in ISO format – yyyy-mm-dd '
                    'example: 2020-06-09')
flags.DEFINE_enum('reingest_period', None, REINGEST_PERIODS_SUPPORTED,
                  'Interval of data to be handled by one reingest task')
flags.DEFINE_integer('num_reingest_periods', None,
                     'Number of periods to reingest')
flags.DEFINE_string('data_source_pattern', None,
                    'Regex pattern for matching data sources')
flags.DEFINE_enum('filter_by_interval', "NONE", SUPPORTED_GRANULARY_STRINGS,
                  'Filter segments based on given interval length')
flags.DEFINE_integer('max_rows_per_segment', None, 'Maximum rows per segment')
flags.DEFINE_string('skip_offset_from_latest',
                    None,
                    'P1D, P1W or P1M (https://en.wikipedia.org/wiki/ISO_8601)')
flags.DEFINE_string('retention_interval',
                    None,
                    'P1D, P1W, P1M (https://en.wikipedia.org/wiki/ISO_8601)')
flags.DEFINE_integer('num_replicas', None, 'Number of replicas')
flags.DEFINE_enum('query_granularity', None, SUPPORTED_GRANULARY_STRINGS,
                  'Query granularity period')
flags.DEFINE_enum('segment_granularity', None, SUPPORTED_GRANULARY_STRINGS,
                  'Segment granularity period')
flags.DEFINE_bool('drop',
                  False,
                  'Used with compact, purge and set_replicas. '
                  'True: drops the segments or rule. '
                  'False: means create or load')
flags.DEFINE_bool('test', True, 'If True just logs, but doesnt act')
flags.DEFINE_string('router_ip', None, 'router private IP (enable VPN)')
flags.DEFINE_integer('router_port', 80, 'router port')

logger = logging.getLogger()


def init_logging(debug: bool):
    """
    Initialize the logger
    :param debug: If true enable debug logging
    """
    if debug:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)
    logger.info('Logging Initialized: Console Only')


# pylint: disable-msg=R0912  # Too Many Branches
def run(argv) -> None:
    """
    Create the indexing_consumers and start all the consumers
    :param argv:
    :return:
    """
    del argv
    init_logging(FLAGS.debug)
    try:
        if FLAGS.router_ip is None:
            raise ValueError('overlord_ip is required')
        if FLAGS.router_port is None:
            raise ValueError('overlord_ip is required')
        if FLAGS.command is None:
            raise ValueError('Must specify some command')

        # DruidAdmin talks to overlord IP and port
        # However router is a proxy for overlord too , so we can send
        # the overlord requests to router and router will proxy them
        # appropriately. From command line (own machine) router is exposed
        # through web security group using VPN, whereas overlord is secured
        connection_string = Settings({
            "overlord_ip": FLAGS.router_ip,
            "overlord_port": FLAGS.router_port,
            "router_ip": FLAGS.router_ip,
            "router_port": FLAGS.router_port,
        })
        druid_admin = DruidAdmin(connection_string)

        if FLAGS.command == 'list_datasources':
            for datasource in sorted(druid_admin.fetch_data_sources()):
                logger.info('%s', datasource)
        elif FLAGS.command == 'segments':
            if FLAGS.data_source_pattern is None:
                raise ValueError('manage_segments require data_source_pattern')
            if FLAGS.start_date is None:
                raise ValueError('manage_segments require start_date')
            if FLAGS.end_date is None:
                raise ValueError('manage_segments require end_date')

            start_date: datetime = datetime.fromisoformat(FLAGS.start_date)
            end_date: datetime = datetime.fromisoformat(FLAGS.end_date)
            if FLAGS.filter_by_interval == 'NONE':
                druid_admin.manage_all_segments(FLAGS.data_source_pattern,
                                                start_date,
                                                end_date,
                                                FLAGS.drop,
                                                FLAGS.test)
            else:
                if not FLAGS.drop:
                    raise ValueError(
                        "load only supported with --filter_by_interval=NONE")

                druid_admin.drop_matching_segments(
                    FLAGS.data_source_pattern, start_date, end_date,
                    FLAGS.filter_by_interval, FLAGS.test)

        elif FLAGS.command == 'delete_datasource':
            if FLAGS.data_source_pattern is None:
                raise ValueError('delete_datasource require '
                                 'data_source_pattern')
            druid_admin.delete_data_sources(FLAGS.data_source_pattern,
                                            FLAGS.test)
        elif FLAGS.command == 'delete_supervisor':
            if FLAGS.data_source_pattern is None:
                raise ValueError('delete_supervisor require'
                                 ' data_source_pattern')
            druid_admin.delete_supervisors(FLAGS.data_source_pattern,
                                           FLAGS.test)
        elif FLAGS.command == 'hard_reset_supervisor':
            if FLAGS.data_source_pattern is None:
                raise ValueError('hard_reset_supervisor require'
                                 ' data_source_pattern')
            druid_admin.hard_reset_supervisors(FLAGS.data_source_pattern,
                                               FLAGS.test)
        elif FLAGS.command == 'compact':
            if FLAGS.data_source_pattern is None:
                raise ValueError('compact require data_source_pattern')
            if FLAGS.max_rows_per_segment is None:
                raise ValueError('compact requires max_rows_per_segment')
            if FLAGS.skip_offset_from_latest is None:
                raise ValueError('compact requires skip_offset_from_latest')
            druid_admin.compact(FLAGS.data_source_pattern,
                                FLAGS.max_rows_per_segment,
                                FLAGS.skip_offset_from_latest,
                                FLAGS.test)
        elif FLAGS.command == 'data_retention':
            if FLAGS.data_source_pattern is None:
                raise ValueError('data_retention require data_source_pattern')
            if FLAGS.retention_interval is None:
                raise ValueError('data_retention requires retention_interval')
            if FLAGS.num_replicas is None:
                raise ValueError('data_retention requires num_replicas')
            druid_admin.manage_retention_rules(FLAGS.data_source_pattern,
                                               FLAGS.retention_interval,
                                               FLAGS.num_replicas,
                                               FLAGS.drop,
                                               FLAGS.test)
        elif FLAGS.command == 'update_ingest':
            if FLAGS.query_granularity is None:
                raise ValueError('update_ingest requires query_granularity')
            if FLAGS.segment_granularity is None:
                raise ValueError('update_ingest requires segment_granularity')
            Settings.load_yaml_settings(
                "../config/prod_no_agg_graphite_consumer.yaml")
            druid_admin.update_data_sources(
                FLAGS.data_source_pattern,
                Settings.inst.metric_consumer.kafka_brokers,
                Settings.inst.Druid.datasource,
                FLAGS.query_granularity,
                FLAGS.segment_granularity,
                FLAGS.test)

        elif FLAGS.command == 'reingest':
            if FLAGS.query_granularity is None:
                raise ValueError('reingest requires query_granularity')
            if FLAGS.segment_granularity is None:
                raise ValueError('reingest requires segment_granularity')
            if FLAGS.start_date is None:
                raise ValueError('reingest requires start_date')
            if FLAGS.reingest_period is None:
                raise ValueError('reingest requires reingest_period')
            if FLAGS.num_reingest_periods is None:
                raise ValueError('reingest requires num_reingest_periods')
            # We assume the same settings as used in our prod environment
            Settings.load_yaml_settings(
                "../config/prod_no_agg_graphite_consumer.yaml")
            druid_admin.reingest_data(
                FLAGS.data_source_pattern,
                Settings.inst.Druid.datasource,
                FLAGS.query_granularity,
                FLAGS.segment_granularity,
                FLAGS.start_date,
                FLAGS.reingest_period,
                FLAGS.num_reingest_periods,
                FLAGS.test)
        elif FLAGS.command == 'mm_worker_strategy':
            if FLAGS.data_source_pattern is None:
                raise ValueError('mm_worker_strategy require '
                                 'data_source_pattern')
            druid_admin.update_crosscluster_mm_worker_strategy(
                FLAGS.data_source_pattern, FLAGS.test)
        else:
            raise ValueError('Unsupported command: {}'.format(FLAGS.command))

    # pylint: disable-msg=W0703  # Broad Except
    except Exception as e:
        logger.error('%s\n ST: %s', str(e), traceback.format_exc())
        logger.info('Usage: %s', FLAGS.get_help())


if __name__ == "__main__":
    app.run(run)
