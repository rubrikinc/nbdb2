"""
RedisCache
"""

import logging
from typing import List

import pickle
import lz4.frame
from redis import Redis

from nbdb.common.telemetry import Telemetry, user_time_calls
from nbdb.readapi.cache_provider import CacheProviderInterface
from nbdb.readapi.query_cache_entry import QueryCacheEntry

logger = logging.getLogger()


class RedisCache(CacheProviderInterface):
    """
    Redis backed cache provider that uses TTL per key to manage the
    cache size. This is not LRU cache and there is no bound on the cache
    A better implementation might be to use redis-lru
    https://pypi.org/project/redis-lru/
    (these are beta projects, so not sure about stability)

    Keep in mind the redis-cache is used by the QueryCache. The QueryCache
    in normal usage will create an update for a key that was recently used
    because it updates the previous set with a new data for the recent time.

    This means the QueryCache turns a get into a set after some interval.
    Effectively this makes the current implementation of using expire similar
    to LRU.
    The keys that are not accessed will get expired, because they wont be set
    The keys that are accessed with moving window of time will be set
    frequently and that will update the TTL.
    """

    def __init__(self, host: str, port: int, db_id: int, ttl_seconds: int):
        """
        Create the redis client.
        :param host:
        :param port:
        :param db_id:
        :param ttl_seconds: TTL in seconds every time a key is updated its
         ttl is updated
        """
        logger.info('Connecting to Redis %s: %d', host, port)
        self.ttl_seconds = ttl_seconds
        self.redis_client = Redis(host=host, port=port, db=db_id)

    @staticmethod
    def _normalize_key(key: str) -> str:
        """
        Normalize the key to escape special wild card characters that may exist
        :param key:
        :return: normalized key
        """
        return key.replace('*', '\\*').replace('?', '\\?')

    def has(self, key: str) -> bool:
        """
        Checks if the key exists in cache
        :param key:
        :return: True if exists False otherwise
        """
        # keys() method returns all keys that match the pattern specified
        # because we are looking for a single key (no pattern)
        # so we expect to get exact match or no key
        #
        # NOTE: We do not use _normalize_key() because KEYS does not expect
        # special characters to be escaped and will return zero results
        # otherwise.
        keys: List[str] = self.redis_client.keys(key)
        if keys is None or len(keys) == 0:
            return False

        # Redis returns bytes not strings and the results contain escaped
        # characters. Before comparing, convert bytes to string and use
        # normalized key with special characters escaped.
        normalized_key = RedisCache._normalize_key(key)
        return normalized_key in [k.decode("utf-8") for k in keys]

    @user_time_calls
    def get(self, key: str,
            # NOTE: 'user' must remain a keyword argument for the
            # user_time_calls() decorator to work
            user: str = None) -> List[QueryCacheEntry]:
        """
        Get the key from redis
        :param key:
        :param user:
        :return: list of QueryCacheEntry or None
        """
        key = RedisCache._normalize_key(key)
        tags = [f"User={user}"]

        # Get data from Redis
        timer = Telemetry.inst.registry.timer(
            "RedisCache.redis_client.get_calls", tag_key_values=tags)
        with timer.time():
            output = self.redis_client.get(key)

        if output is None:
            return None
        # We store query cache results in compressed pickled form.
        # Uncompress and then load as pickle object
        timer = Telemetry.inst.registry.timer(
            "RedisCache.redis_client.lz4_extraction", tag_key_values=tags)
        with timer.time():
            uncompressed_str = lz4.frame.decompress(output)

        # Data has been uncompressed. Now load pickled object
        timer = Telemetry.inst.registry.timer(
            "RedisCache.redis_client.pickle_extraction", tag_key_values=tags)
        with timer.time():
            output = pickle.loads(uncompressed_str)
        return output

    @user_time_calls
    def set(self, key: str, entries: List[QueryCacheEntry],
            # NOTE: 'user' must remain a keyword argument for the
            # user_time_calls() decorator to work
            user: str = None) -> None:
        """
        Set the key value in redis with a TTL
        :param key:
        :param entries:
        :param user:
        """
        key = RedisCache._normalize_key(key)
        tags = [f"User={user}"]
        # We store query cache results in compressed pickled form.
        # First pickle data
        timer = Telemetry.inst.registry.timer(
            "RedisCache.redis_client.pickle_compression", tag_key_values=tags)
        with timer.time():
            pickle_str = pickle.dumps(entries)

        # Data has been pickled. Now compress. Compressing helps reduce the
        # binary string size especially for large cross-cluster queries. Redis
        # GET performance is quite slow for large strings, so compressing
        # ensures fast GET performance
        timer = Telemetry.inst.registry.timer(
            "RedisCache.redis_client.lz4_compression", tag_key_values=tags)
        with timer.time():
            compressed_str = lz4.frame.compress(pickle_str)

        # Record entry size
        Telemetry.inst.registry.histogram(
            'ReadApi.RedisCache.cache_entry_size', tag_key_values=tags
        ).add(len(compressed_str))

        # Store in Redis with an expiry period
        timer = Telemetry.inst.registry.timer(
            "RedisCache.redis_client.set_calls", tag_key_values=tags)
        with timer.time():
            self.redis_client.set(key, compressed_str)
            self.redis_client.expire(key, self.ttl_seconds)

    def clear(self, key: str) -> None:
        """
        Clear the key from cache
        :param key:
        """
        key = RedisCache._normalize_key(key)
        # Removes the specified key. Key is ignored if it does not exist.
        self.redis_client.delete(key)

    def size(self) -> int:
        """
        Number of entries in cache
        Not supported, please use the redis reported metrics
        :return: count of entries
        """
        return 0
