"""
SchemaCreator Module
Provides a command line tool to create the schema
"""
import logging
import time
import traceback
from typing import List, Tuple

from absl import app, flags
import kafka
from kafka import KafkaAdminClient
from kafka.admin import NewTopic

from nbdb.common.context import Context
from nbdb.common.thread_pools import ThreadPools
from nbdb.common.druid_admin import DruidAdmin
from nbdb.config.settings import Settings
from nbdb.common.entry_point import EntryPoint
from nbdb.schema.schema import Schema

logger = logging.getLogger()

FLAGS = flags.FLAGS
flags.DEFINE_boolean('clean', False, 'Deletes and recreates')
flags.DEFINE_boolean('druid', True, 'Creates druid schema')
flags.DEFINE_boolean('kafka', False, 'Creates kafka schema')


class SchemaCreator:
    """
    Creates the schema
    """
    @staticmethod
    def start_sync(context: Context) -> None:
        """
        Schedules a periodic job that ensures that the schema is appropriately
        created.
        """
        logger.info("SchemaCreator.start_sync")
        if Settings.inst.schema_creator.cleanup_on_boot:
            logger.info("SchemaCreator.cleanup_on_boot."
                        " Deleting and recreating the topics")
            SchemaCreator.create(context=context,
                                 clean=True,
                                 operate_on_druid=True,
                                 operate_on_kafka=True)

        logger.info('Starting Schema creator background task')
        ThreadPools.inst.schema_creator_pool.submit(
            SchemaCreator.ensure_schema, context)

    @staticmethod
    def ensure_schema(context: Context) -> None:
        """
        Periodically wakes up and checks if the schema is upto date
        For now only creates druid datasources if it they are missing
        """
        logger.info('SchemaCreator.ensure_schema: enter')
        druid_admin = DruidAdmin(Settings.inst.Druid.connection_string)
        kafka_bootstrap_servers = Settings.inst.sparse_kafka.\
            connection_string.bootstrap.servers

        while not ThreadPools.inst.is_app_exiting():
            try:
                logger.info('SchemaCreator.ensure_schema: Check for '
                            'missing datasources')
                existing_data_sources: List[str] = \
                    druid_admin.fetch_supervisors()
                required_data_sources = context.schema.datasources()
                missing_data_sources = [(d, qg, sg)
                                        for (d, qg, sg) in required_data_sources
                                        if d not in existing_data_sources]
                logger.info('SchemaCreator.ensure_schema: %d missing '
                            'datasources', len(missing_data_sources))
                if len(missing_data_sources) > 0:
                    logger.info('SchemaCreator.ensure_schema:'
                                ' Missing Datasources %s',
                                ','.join(['{}:{}:{}'.format(d, qg, sg)
                                          for (d, qg, sg) in
                                          missing_data_sources]))

                    for data_source, query_gran, seg_gran in \
                            missing_data_sources:
                        druid_admin.create_data_source(
                            kafka_bootstrap_servers, data_source,
                            Settings.inst.Druid.datasource, query_gran,
                            seg_gran)

                    # Ensure the kafka topic also exists for the missing
                    # datasources
                    SchemaCreator.create_topics(kafka_bootstrap_servers,
                                                missing_data_sources)
            except Exception as exc:
                logger.error("SchemaCreator.ensure_schema: Saw error %s", exc)
                logger.error("SchemaCreator.ensure_schema: %s",
                             traceback.format_exc())

            time.sleep(30)
        logger.info('SchemaCreator.ensure_schema: app shutting down')

    @staticmethod
    def create(context: Context,
               clean: bool = False,
               operate_on_druid: bool = True,
               operate_on_kafka: bool = True) -> None:
        """
        Create the kafka topics and druid kafka specs
        :param clean: If true deletes all the topics in kafka and data sources
        :param operate_on_druid: If true operates on druid datasources
        :param operate_on_kafka: If true operates on kafka topics
        in druid
        """
        druid_connection_string = Settings.inst.Druid.connection_string
        kafka_bootstrap_servers = Settings.inst.sparse_kafka.\
            connection_string.bootstrap.servers
        datasources: List[Tuple[str, int, int]] = context.schema.datasources()
        if clean:
            logger.info('SchemaCreator.create: cleaning up')
            if operate_on_druid:
                SchemaCreator.delete_data_source(druid_connection_string,
                                                 datasources)
            if operate_on_kafka:
                SchemaCreator.delete_topics(kafka_bootstrap_servers,
                                            datasources)

        logger.info('SchemaCreator.create: creating schema')
        if operate_on_druid:
            logger.info('SchemaCreator.create: creating datasources')
            SchemaCreator.create_data_source(druid_connection_string,
                                             kafka_bootstrap_servers,
                                             context.schema)
        if operate_on_kafka:
            logger.info('SchemaCreator.create: creating topics')
            SchemaCreator.create_topics(kafka_bootstrap_servers,
                                        datasources)

    @staticmethod
    def delete_topics(brokers,
                      datasources: List[Tuple[str, int, int]]) -> None:
        """
        Delete the provided topics
        :param brokers:
        :param datasources:
        :return:
        """
        logger.info('SchemaCreator.create: deleting topics: %s',
                    ','.join([d for d, _, _ in datasources]))
        admin = KafkaAdminClient(bootstrap_servers=brokers)
        schema_settings = Settings.inst.schema_creator
        for datasource, _, _ in datasources:
            topic_name = "%s_%s" % (schema_settings.druid_topic_prefix,
                                    datasource)
            try:
                admin.delete_topics([topic_name])
            except kafka.errors.UnknownTopicOrPartitionError as exc:
                logger.info('IGNORE: Failed to delete topic: %s Exception: %s.',
                            topic_name, str(exc))

    @staticmethod
    def create_topics(brokers,
                      datasources: List[Tuple[str, int, int]]) -> None:
        """
        Create topics per measurement
        :param brokers: kafka brokers
        :param datasources:
        """
        logger.info('SchemaCreator.create: creating topics: %s',
                    ','.join([d for d, _, _ in datasources]))
        admin = KafkaAdminClient(bootstrap_servers=brokers)
        schema_settings = Settings.inst.schema_creator
        # NOTE: Have to create topics one by one. We have seen issues where if
        # multiple topic creation requests are clubbed together in one API
        # call, some of them can silently fail.
        for datasource, _, _ in datasources:
            topic_name = "%s_%s" % (schema_settings.druid_topic_prefix,
                                    datasource)
            topic = NewTopic(
                name=topic_name,
                num_partitions=Settings.inst.sparse_kafka.partitions,
                replication_factor=Settings.inst.sparse_kafka.replicas,
                topic_configs={
                    'retention.ms': Settings.inst.sparse_kafka.retention,
                    'segment.ms': Settings.inst.sparse_kafka.segment}
            )
            try:
                admin.create_topics([topic])
                logger.info('SchemaCreator.create: success created topic: %s',
                            topic_name)
            except kafka.errors.TopicAlreadyExistsError as exc:
                logger.info('IGNORE: Failed to create topic: %s Exception: %s',
                            topic_name, str(exc))

    @staticmethod
    def delete_data_source(connection_string,
                           datasources: List[Tuple[str, int, int]]) -> None:
        """
        Delete the data sources
        :param connection_string:
        :param datasources: list of Druid datasources, each one is configured
        as a Kafka data source
        :return:
        """
        logger.info('SchemaCreator.create: deleting data sources: %s',
                    ','.join([d for d, _, _ in datasources]))
        druid_admin = DruidAdmin(connection_string)
        for datasource, _, _ in datasources:
            druid_admin.delete_supervisor(datasource)

    @staticmethod
    def create_data_source(connection_string,
                           kafka_brokers,
                           schema: Schema,
                           ) -> None:
        """
        For each measurement create a kafka topic spec
        :param connection_string:
        :param kafka_brokers:
        :param schema:
        :return:
        """
        logger.info('SchemaCreator.create: creating data souces: %s',
                    ','.join([':'.join([str(i) for i in datasource_details])
                              for datasource_details in schema.datasources()]))
        druid_admin = DruidAdmin(connection_string)
        for datasource, query_gran, segment_gran in schema.datasources():
            druid_admin.create_data_source(kafka_brokers,
                                           datasource,
                                           Settings.inst.Druid.datasource,
                                           query_gran,
                                           segment_gran)


class SchemaCreatorApp(EntryPoint):
    """
    Entry point class for the Schema Creator App
    """

    def __init__(self):
        """
        Initialize the entry point
        """
        EntryPoint.__init__(self)

    def start(self):
        """
        Initialize the write path and start the consumers
        :return:
        """
        logger.info("Creating the Schema")
        SchemaCreator.create(context=self.context,
                             clean=True,
                             operate_on_druid=False,
                             operate_on_kafka=True)
        logger.info("Done creating the Schema")


if __name__ == "__main__":
    app.run(SchemaCreatorApp().run)
