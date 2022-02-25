""""
Entry point for the Indexer
"""
import logging
import socket
import signal
import time
import os
from logging.handlers import TimedRotatingFileHandler
from typing import List

from absl import flags
from nbdb.common.context import Context
from nbdb.common.telemetry import Telemetry
from nbdb.common.thread_pools import ThreadPools
from nbdb.config.settings import Settings
from nbdb.schema.schema import Schema


FLAGS = flags.FLAGS
flags.DEFINE_boolean('debug', False, 'Enable debug level logs')
flags.DEFINE_string('setting_file', '/app/nbdb/config/settings.yaml',
                    'absolute path to the yaml settings file')
flags.DEFINE_multi_string('schema_mapping',
                          'default:/app/nbdb/config/schema.yaml',
                          'Database-to-schema file mapping. '
                          'Eg. default:nbdb/config/schema_prod.yaml')
flags.DEFINE_string('wait_for_conn', '',
                    'Wait for <IP:port> pair to be up before proceeding')

logger = logging.getLogger()


class EntryPoint:
    """
    Base Entry point for all executables, does the common initialization
    and provides signal handling for graceful shutdown
    """

    def __init__(self):
        """
        Initialize
        """
        self.name = self.__class__.__name__
        self.context: Context = None

    def init_logging(self):
        """
        Initialize the logger
        """
        if Settings.inst.logging.debug:
            logger.setLevel(logging.DEBUG)
        else:
            logger.setLevel(logging.INFO)

        if not Settings.inst.logging.log_to_console:
            if not os.path.exists(Settings.inst.logging.log_dir):
                os.makedirs(Settings.inst.logging.log_dir)

            log_path = '{}/{}.log'.format(Settings.inst.logging.log_dir,
                                          self.name)
            file_handler = TimedRotatingFileHandler(log_path,
                                                    when='D',
                                                    backupCount=30)
            log_formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            file_handler.setFormatter(log_formatter)
            logger.handlers = []
            logger.addHandler(file_handler)
            logger.info('Logging Initialized: %s', log_path)
        else:
            logger.info('Logging Initialized: Console Only')

    def signal_handler(self, signum, frame) -> None:
        """
        Handle the signal
        :param signum:
        :param frame:
        :return:
        """
        _ = frame
        logger.info('EntryPoint: %s.signal_handler: called %d',
                    self.name, signum)
        logger.info('EntryPoint: %s.signal_handler: starting graceful shutdown'
                    , self.name)
        logger.info('EntryPoint: %s.signal_handler, stopping thread pools')
        ThreadPools.inst.stop()
        logger.info('EntryPoint: %s.signal_handler, stopping telemetry')
        Telemetry.inst.reporter.stop()
        logger.info('EntryPoint: %s.signal_handler, shutdown complete')

    def register_signal_handlers(self) -> None:
        """
        Register the signal handlers to catch keyboard interrupts
        :return:
        """
        signal.signal(signal.SIGALRM, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)
        signal.signal(signal.SIGINT, self.signal_handler)

    @staticmethod
    def wait_for_conn_up(ip_addr, port, timeout_secs=120):
        """
        Check if connection is being accepted on given <ip_addr:port> pair
        """
        logger.info("Waiting for %s:%s to be up", ip_addr, port)
        while timeout_secs > 0:
            sock = socket.socket()
            sock.settimeout(5)
            try:
                sock.connect((ip_addr, port))
                sock.close()
                logger.info("Successfully connected to %s:%s", ip_addr, port)
                return
            # pylint: disable-msg=W0703  # Catching too general exception
            except Exception as exc:
                # Try connecting again
                logger.info("Failed to connect to %s:%s due to %s. Retrying",
                            ip_addr, port, exc)
                timeout_secs -= 10
                time.sleep(10)
                sock.close()

        # If we reached here, we were unable to connect
        assert False, "Unable to connect to %s:%s" % (ip_addr, port)

    @staticmethod
    def wait_for_all_conns_up() -> None:
        """
        Check if connections are being accepted on all <ip_addr:port> pairs
        """
        if not FLAGS.wait_for_conn:
            return

        ip_port_pairs = FLAGS.wait_for_conn.split(',')
        for pair in ip_port_pairs:
            ip_addr, port = pair.split(':')
            port = int(port)
            EntryPoint.wait_for_conn_up(ip_addr, port)

    def wait_for_service_ready(self) -> None:
        """Wait for several checks to pass before starting service.

        Override this method if you want to add more checks for your particular
        service.
        """
        _ = self
        # Wait for connections to be up
        EntryPoint.wait_for_all_conns_up()

    def load_schema(self, schema_mapping: List) -> Schema:
        """
        Instantiate Schema from a yaml file.
        Override this to customize schema load.
        """
        # Schema mapping consists of multiple database to schema path mappings
        # Eg. --schema_mapping default:config/schema.yaml
        #     --schema_mapping batch:config/batch_schema.yaml
        _ = self

        # Default behaviour is to load the schema from the first file specified
        # in the list
        _, schema_file = schema_mapping[0].split(":")
        return Schema.load_from_file(schema_file)

    def run(self, argv) -> None:
        """
        Create the indexing_consumers and start all the consumers
        :param argv:
        :return:
        """
        del argv
        logger.info('Loading settings from %s', FLAGS.setting_file)
        Settings.load_yaml_settings(FLAGS.setting_file)

        logger.info('Loading schema from %s', FLAGS.schema_mapping)
        schema = self.load_schema(FLAGS.schema_mapping)
        self.context = Context(schema=schema)

        self.init_logging()
        self.wait_for_service_ready()

        logger.info('Initialize ThreadPools')
        ThreadPools.instantiate()

        logger.info('Registering signal handlers')
        self.register_signal_handlers()

        logger.info('Initializing Telemetry')
        Telemetry.initialize()

        logger.info('Calling custom initializer')
        self.start()

    def start(self) -> None:
        """
        Override this method to do any custom initialization and
        start the main loop of app
        """
        raise NotImplementedError('EntryPoint.start must be implemented')
