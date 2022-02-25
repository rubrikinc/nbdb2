"""
IdentityStringTable
"""
from nbdb.string_table.string_table_interface import StringTableInterface


class IdentityStringTable(StringTableInterface):
    """
    This is meant for testing only and not actual production use
    Production usage will require a Redis based String table that can
    guarantee synchronization across processes and threads and also provide
    persistence
    IdentityStringTable doesn't do any mapping token is same as original string
    """

    def get_token_value(self, str_value: str) -> str:
        """
        Get the integer value corresponding to the string
        :param str_value:
        :return:
        """
        return str_value

    def get_str_value(self, token_value: str) -> str:
        """
        Returns the string value associated with the int_value token
        :param token_value:
        :return:
        """
        return token_value
