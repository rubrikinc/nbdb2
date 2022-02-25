"""
StringTableInterface
"""


class StringTableInterface:
    """
    StringTableInterface provides a way to compress a known set of strings
    into compact token representation where a token is of type int
    """

    def get_token_value(self, str_value: str) -> str:
        """
        Returns the token (integer) value corresponding to the str in the
        string table
        if the str doesnt exist the string table should be updated with a new
        entry
        """
        raise NotImplementedError('Child class must provide definition '
                                  'of StringTableInterface.get_token_value '
                                  'method')

    def get_str_value(self, token_value: str) -> str:
        """
        Returns the string value corresponding to the int_value
        in the string table. If the int value doesn't exist it raises an error
        """
        raise NotImplementedError('Child class must provide definition '
                                  'of StringTableInterface.get_str_value '
                                  'method')

    def tokenize_dictionary(self, str_dict: dict) -> dict:
        """
        Tokenizes the key,value pairs in the dictionary and returns a new
        dictionary containing token_key=token_value
        This is a convenience method for frequently used data types
        :param str_dict:
        :return: tokenized dictionary
        """
        token_dict = dict()
        for key, value in str_dict.items():
            token_dict[self.get_token_value(key)] = self.get_token_value(value)
        return token_dict
