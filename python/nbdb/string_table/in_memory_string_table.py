"""
InMemoryStringTable
"""
import threading

from nbdb.string_table.string_table_interface import StringTableInterface


class InMemoryStringTable(StringTableInterface):
    """
    This is meant for testing only and not actual production use
    Production usage will require a Redis based String table that can
    guarantee synchronization across processes and threads and also provide
    persistence
    """

    def __init__(self):
        """
        Initialize the string table
        """
        self._str_to_int = dict()
        self._int_to_str = dict()
        self._counter = 0
        self._lock = threading.Lock()

    def get_token_value(self, str_value: str) -> int:
        """
        Get the integer value corresponding to the string
        :param str_value:
        :return:
        """
        if str_value not in self._str_to_int:
            with self._lock:
                self._str_to_int[str_value] = self._counter
                self._int_to_str[self._counter] = str_value
                self._counter = self._counter + 1

        return self._str_to_int[str_value]

    def get_str_value(self, token_value: int) -> str:
        """
        Returns the string value associated with the int_value token
        :param token_value:
        :return:
        """
        if token_value not in self._int_to_str:
            raise RuntimeError('token value {} not present in string table'
                               .format(token_value))
        return self._int_to_str[token_value]
