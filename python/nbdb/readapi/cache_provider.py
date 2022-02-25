"""
CacheProviderInterface
"""

from typing import List
from nbdb.readapi.query_cache_entry import QueryCacheEntry


class CacheProviderInterface:
    """
    Cache provider for the query cache must implement this interface
    """

    def has(self, key: str) -> bool:
        """

        :param key:
        :return:
        """
        raise NotImplementedError('Child class must provide definition of'
                                  ' CacheProviderInterface.has method')

    def get(self, key: str,
            # NOTE: 'user' must remain a keyword argument for the
            # user_time_calls() decorator to work
            user: str = None) -> List[QueryCacheEntry]:
        """
        Establish a connection to the underlying db
        """
        raise NotImplementedError('Child class must provide definition of'
                                  ' CacheProviderInterface.get method')

    def set(self, key: str, entries: List[QueryCacheEntry],
            # NOTE: 'user' must remain a keyword argument for the
            # user_time_calls() decorator to work
            user: str = None) -> None:
        """
        Shutdown the connection to the underlying db
        :return:
        """
        raise NotImplementedError('Child class must provide definition of'
                                  ' CacheProviderInterface.set method')

    def clear(self, key: str) -> None:
        """
        Clears the key from the cache
        :param key:
        :return:
        """
        raise NotImplementedError('Child class must provide definition of '
                                  'CacheProviderInterface.clear method')

    def size(self) -> int:
        """
        Size of cache in terms of entries
        :return: number of entries in the cache
        """
        raise NotImplementedError('Child class must provide definition of '
                                  'CacheProviderInterface.clear method')
