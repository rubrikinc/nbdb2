"""
ThreadPools
"""

from __future__ import annotations

import logging
from concurrent.futures.thread import ThreadPoolExecutor
from nbdb.config.settings import Settings

logger = logging.getLogger()


class ThreadPools:
    """
    All the threadpools used by the nbdb are instantiated and managed here
    This allows us to easily monitor and understand the places where threads
    are created. Alternately we can allow threadpool creation where needed
    by the owner of the relevant component. However that can lead to
    proliferation of thread pools and possibility of leaking threads.
    By having this shared class we also make it possible to reuse more of the
    threads
    """

    # Global instance variable for thread-pool
    # must call instantiate method before inst can be referenced
    inst: ThreadPools = None

    def __init__(self) -> None:
        """
        Create the threadpools based on the configuration
        Assumes config is already initialized
        """
        self.pools = dict()
        for service_name, pool_name, worker_name in [
                ('sql_api', 'sql_api_pool', 'workers'),
                ('sql_api', 'series_query_pool', 'store_read_workers'),
                ('metric_consumer', 'metric_consumer_pool', 'num_consumers'),
                ('recovery_consumer', 'recovery_consumer_pool', 'num_consumers'),
                ('realtime_metric_consumer', 'realtime_metric_consumer_pool', 'num_consumers'),
                ('rollup_metric_consumer', 'rollup_metric_consumer_pool', 'num_consumers'),
                ('sparse_batch_consumer', 'sparse_batch_consumer_pool',
                 'num_consumers'),
                ('dense_batch_consumer', 'dense_batch_consumer_pool',
                 'num_consumers'),
                ('indexing_consumer', 'indexing_consumer_pool', 'num_consumers'),
                ('sql_api', 'index_query_pool', 'index_search_workers'),
                ('batch_writers', 'batch_writer_pool', 'workers'),
                ('redisdb', 'redis_write_pool', 'write_workers'),
                ('scylladb', 'scylladb_write_pool', 'write_workers'),
                ('sparse_kafka', 'sparse_kafka_write_pool', 'write_workers'),
                ('test_metric_reader', 'test_reader_pool', 'num_workers'),
                ('schema_creator', 'schema_creator_pool', 'num_workers')

        ]:
            self._create_thread_pool(service_name, pool_name, worker_name)
        self.exit_app = False

    def _create_thread_pool(self,
                            service_name: str,
                            thread_pool_name: str,
                            workers_name: str) -> None:
        """
        Create a threadpool for the given service with given workers
        :param service_name: Attribute name in Settings
        :param thread_pool_name: Attribute name for threadpool
        :param workers_name: Sub field in Settings Attribute
        :return:
        """
        thread_pool = None
        if hasattr(Settings.inst, service_name):
            service = getattr(Settings.inst, service_name)
            if hasattr(service, workers_name):
                workers = getattr(service, workers_name)
                if workers > 0:
                    thread_pool = ThreadPoolExecutor(max_workers=workers)

        setattr(self, thread_pool_name, thread_pool)
        if thread_pool is not None:
            self.pools[thread_pool_name] = thread_pool

    def is_app_exiting(self):
        """
        Checks if the app is exiting
        :return: True if app is exiting and all background activity should
        stop
        """
        return self.exit_app

    def stop(self, wait=False) -> None:
        """
        Stop all threadpools and blocks until all futures are completed
        :param wait: If true (default is false) then blocks until the scheduled
        tasks on the threadpools complete
        For unittests where background activity results need to be confirmed
        set the wait to true to block the unittest thread until background
        activity is completed before verifying such activity
        """
        logger.info('ThreadPools.stop: setting exitapp')
        self.exit_app = True
        for pool_name, thread_pool in self.pools.items():
            logger.info('ThreadPools.stop: waiting for %s', pool_name)
            thread_pool.shutdown(wait)
        logger.info('ThreadPools.stop: all pools shutdown')

    @staticmethod
    def instantiate() -> None:
        """
        Instantiate the ThreadPool global instance
        """
        ThreadPools.inst = ThreadPools()
