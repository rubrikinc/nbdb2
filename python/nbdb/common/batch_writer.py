"""
BatchWriter
"""

import logging
import time
import traceback
from concurrent.futures import Future, wait, FIRST_COMPLETED, ALL_COMPLETED

from queue import Queue, Empty
from time import sleep
from typing import Tuple

from nbdb.common.telemetry import Telemetry
from nbdb.common.thread_pools import ThreadPools
from nbdb.common.simple_throttle import SimpleThrottle

logger = logging.getLogger()


class BatchWriter:
    """
    We have multiple threads that process data points to be written,
     each data point is considered for sparseness filter
    and most data points get dropped. The ones that are to be written
    are collected by this class and drained
    as batched writes to the store.
    This class also provides an ability to throttle the writes to store
    and creates a back-pressure.
    """
    def __init__(self,
                 queue_size: int,
                 rate_limit: int,
                 batch_size: int,
                 sleep_time: float,
                 reset_time: float,
                 max_pending_futures: int) -> None:
        """
        Initialize the blocking Queue and the throttle for the DB writes
        :param store: DB client
        :param queue_size: Size of the queue for items in the queue
        :param rate_limit: items/second rate limit that is enforced while
        writing items to the DB
        :param batch_size: Maximum size of the batch that must be prepared,
        note if the queue doesn't have enough items
        smaller batches are prepared and written to the DB. So if incoming
        rate is very low the batch writer will write
        each item as a separate batch.
        :param sleep_time: Time to sleep when rate limit is exceeded, smaller
               values ensure that rate is uniform but overhead is higher
        :param rate_reset_time: The computed rate is reset after this time
        """
        self.name = self.__class__.__name__
        self.queue = Queue(maxsize=queue_size)
        self.simple_throttle = SimpleThrottle(rate_limit, sleep_time,
                                              reset_time)
        self.batch_size = batch_size
        self.max_pending_futures = max_pending_futures

    def write(self, item) -> None:
        """
        Write the item to the queue, this thread may block if the queue is full
        :param item:
        """
        self.queue.put(item)

    def initialize_pending_futures(self):
        """
        Initialize the pending futures, note the child class can
        inherit and change the data type of pending future if needed
        :return:
        """
        _ = self
        return set()

    def add_to_pending_futures(self, pending_futures, future):
        """
        Pending futures data type can be altered by child class
        :param pending_futures:
        :param future:
        :return:
        """
        _ = self
        pending_futures.add(future)

    def start_drain(self) -> None:
        """
        Drains the queue and generate batches to be written to the store
        This can be executed on a single thread because it uses async write
        method so doesn't block at all
        :return:
        """
        logger.info('batch_writer.start_drain: %s', self.name)
        pending_futures = self.initialize_pending_futures()
        while True:
            try:
                pending_futures, app_exiting = \
                    self.process_single_batch(pending_futures)
                if app_exiting:
                    # app is exiting
                    break
            # pylint: disable-msg=W0703 # Broad Except
            except Exception as exception:
                if Telemetry.inst is not None:
                    Telemetry.inst.registry.meter(
                        self.name + '.start_drain.exceptions').mark()
                logger.error('batch_writer.start_drain: %s failed: %s ST: %s',
                             self.name,
                             str(exception),
                             traceback.format_exc()
                             )
        # Wait for all pending futures
        logger.info('batch_writer.start_drain: %s waiting for %d futures',
                    self.name, len(pending_futures))
        self.wait_for_pending_futures(pending_futures, ALL_COMPLETED)

        logger.info('batch_writer.start_drain: %s exiting',
                    self.name)

    def process_single_batch(self, pending_futures) -> Tuple:
        """
        Process a single batch by deque-ing and writing asynchronously
        to the DB
        If the pending futures exceed the max pending futures, this blocks
        If the write rate exceeds the throttled value, this blocks
        :param pending_futures: set or list of futures that are pending
        :return: PendingFutures (can be set or list), AppExiting (true, false)
        """
        batch = self._get_next_batch()
        if Telemetry.inst is not None:
            Telemetry.inst.registry.gauge(self.name + '.queue_size') \
                .set_value(self.queue.qsize())
            Telemetry.inst.registry.gauge(self.name + '.pending_futures') \
                .set_value(len(pending_futures))

        if len(pending_futures) >= self.max_pending_futures:
            # Wait for the oldest future to complete
            pending_futures = self.wait_for_pending_futures(
                pending_futures, FIRST_COMPLETED)

        if len(batch) > 0:
            future = self.process(batch)
            if future is not None:
                self.add_to_pending_futures(pending_futures, future)
                self.add_timing_callback(future)

            items_written = len(batch)
            if Telemetry.inst is not None:
                Telemetry.inst.registry.histogram(
                    self.name + '.batch_size').add(items_written)

            if self.simple_throttle.throttle(items_written):
                # call was throttled, report the instant rate
                if Telemetry.inst is not None:
                    Telemetry.inst.registry.gauge(
                        self.name + '.throttle_rate') \
                        .set_value(self.simple_throttle.instant_rate)
                logger.info('batch_writer.process_single_batch: %s '
                            'batch:%d QPS:%d',
                            self.name,
                            len(batch),
                            int(self.simple_throttle.instant_rate))
        else:
            if ThreadPools.inst.is_app_exiting():
                logger.info('batch_writer.process_single_batch: %s queue is'
                            ' empty and app is exiting, '
                            'safe to stop drain now', self.name)
                return pending_futures, True
            # sleep for a second to let the queue build up
            logger.debug('batch_writer.process_single_batch: %s '
                         'waiting for data', self.name)
            sleep(1)
        return pending_futures, False

    def process(self, batch: list) -> Future:
        """
        To be implemented by the child class, provide any filtering or
        processing on the batch and writing to store
        :param batch:
        :return: number of items written to store
        """
        raise NotImplementedError('BatchedWrites.process {} not implemented, '
                                  'child class must provide an implementation'
                                  .format(self.name))

    def _get_next_batch(self) -> list:
        """
        Dequeues batch_size items from the given queue, method is non-blocking,
         if the queue
        doesn't have batch_size items then returns fewer items
        :param batch_size: 0 means get all available items in the queue
        :return: batch of items , upto batch_size
        """
        batch = []
        while len(batch) < self.batch_size or self.batch_size <= 0:
            try:
                item = self.queue.get_nowait()
                batch.append(item)
            except Empty:
                # no more data in queue
                break
        return batch

    def record_time_taken(self, future, start_time):
        """Record time taken by future"""
        if future.exception() is not None:
            # Don't bother recording completion times for failed futures
            return
        if Telemetry.inst is not None:
            metric_name = self.name + '.future_time_taken'
            value = time.time() - start_time
            Telemetry.inst.registry.histogram(metric_name).add(value)

    def add_timing_callback(self, future):
        """Add a callback to the future to record its completion time"""
        _ = self
        if Telemetry.inst is not None:
            start_time = time.time()
            future.add_done_callback(
                lambda future: self.record_time_taken(future, start_time))

    def wait_for_pending_futures(self, pending_futures, flag):
        """Wait for futures to finish and update metrics."""
        done_futures, pending_futures = wait(pending_futures, return_when=flag)
        self._report_success_fail(done_futures)
        return pending_futures

    def _report_success_fail(self, futures) -> None:
        """
        Report the futures that succeeded vs failed
        :param futures: all completed futures
        """
        for done_future in futures:
            if done_future.exception() is None:
                if Telemetry.inst is not None:
                    Telemetry.inst.registry.meter(
                        self.name + '.start_drain.done_future').mark()
            else:
                if Telemetry.inst is not None:
                    Telemetry.inst.registry.meter(
                        self.name + '.start_drain.fail_future').mark()
