"""
InMemoryCache module
"""

import logging
from typing import List

# Its there not sure why pylint is unable to find it
# pylint: disable-msg=E0611  # No Name In Module
from lru import LRU

from nbdb.readapi.cache_provider import CacheProviderInterface
from nbdb.readapi.query_cache_entry import QueryCacheEntry

logger = logging.getLogger()


class InMemoryCache(CacheProviderInterface):
    """
    In memopry Cache provider for the query cache
    """

    def __init__(self, cache_size: int):
        """
        Initialize the LRU cache
        :param cache_size
        """
        logger.info('Creating LRU cache: max_entries: %d', cache_size)
        self._data = LRU(cache_size)

    def has(self, key: str) -> bool:
        """
        Checks if the key exists in cache
        :param key:
        :return: True if exists False otherwise
        """
        return key in self._data

    def get(self, key: str, user: str = None) -> List[QueryCacheEntry]:
        """
        Get the key from redis
        :param key:
        :return: list of QueryCacheEntry or None
        """
        _ = user
        if key not in self._data:
            return None
        cache_entries = self._data[key]
        return cache_entries

    def set(self, key: str, entries: List[QueryCacheEntry],
            user: str = None) -> None:
        """
        Set the key value in redis
        :param key:
        :param entries:
        """
        _ = user
        self._data[key] = entries

    def clear(self, key: str) -> None:
        """
        Clear the key from cache
        :param key:
        """
        if key in self._data:
            del self._data[key]

    def size(self) -> int:
        """
        Number of entries in cache
        :return: count of entries
        """
        return len(self._data)
