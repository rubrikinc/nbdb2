"""
TestBatchWriter
"""
import logging
import os
from concurrent.futures import Future
from unittest import TestCase
from unittest.mock import Mock

from nbdb.common.thread_pools import ThreadPools

from nbdb.config.settings import Settings
from nbdb.common.telemetry import Telemetry
from nbdb.common.batch_writer import BatchWriter

logger = logging.getLogger()


class DummyBatchWriter(BatchWriter):
    """
    Dummy batch writer for testing the batchwriter
    Just prints to console the items provided
    """
    def __init__(self):
        """
        Initialize the dummybatchwriter with default params
        """
        super(DummyBatchWriter, self).__init__(
            queue_size=10,
            rate_limit=10,
            batch_size=2,
            sleep_time=1,
            reset_time=2,
            max_pending_futures=4
        )
        self.store = Mock()

    def process(self, batch: list) -> Future:
        """
        Process the batch, by creating a future
        :param batch:
        :return:
        """
        _ = self
        print('process: {}'.format(','.join([str(item) for item in batch])))
        return ThreadPools.inst.metric_consumer_pool.submit(
            self.async_process,
            batch
        )

    def async_process(self, batch: list) -> None:
        """
        Actual processing of the batch, just log it
        :param batch:
        :return:
        """
        _ = self
        batch_str = ','.join([str(item) for item in batch])
        print('async_process: start: {}'.format(batch_str))
        print('async_process: done-: {}'.format(batch_str))


class TestBatchWriter(TestCase):
    """
    TestMetricKey Unittests
    """

    def setUp(self) -> None:
        """
        Setup the string table and generate test keys
        """
        Settings.load_yaml_settings(os.path.dirname(__file__) +
                                    '/test_settings.yaml')
        ThreadPools.instantiate()
        if Telemetry.inst is None:
            Telemetry.initialize()
        self.batch_writer = DummyBatchWriter()

    def tearDown(self) -> None:
        """
        Stop the threadpools
        """
        ThreadPools.inst.stop()

    def test_batch_writer_basic(self) -> None:
        """
        Test the batch_writer by generating some load
        and verifying that each of the item is processed
        """
        ThreadPools.inst.batch_writer_pool.submit(
            self.batch_writer.start_drain)

        item_count = 10
        for item in range(item_count):
            self.batch_writer.write(item)

        meter_exceptions = Telemetry.inst.registry.meter(
            'DummyBatchWriter.start_drain.exceptions')
        histogram_future_time_taken = Telemetry.inst.registry.histogram(
            'DummyBatchWriter.future_time_taken')
        meter_done_future = Telemetry.inst.registry.meter(
            'DummyBatchWriter.start_drain.done_future')

        # Wait while the batch writer processes the
        num_batches = item_count/self.batch_writer.batch_size
        ThreadPools.inst.exit_app = True
        ThreadPools.inst.batch_writer_pool.shutdown(wait=True)
        ThreadPools.inst.stop(wait=True)

        self.assertEqual(0, meter_exceptions.get_count())
        self.assertEqual(num_batches, histogram_future_time_taken.get_count())
        self.assertEqual(num_batches, meter_done_future.get_count())
