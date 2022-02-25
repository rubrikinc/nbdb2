"""
TestStringTable
"""

from unittest import TestCase

from nbdb.string_table.identity_string_table import IdentityStringTable
from nbdb.string_table.in_memory_string_table import InMemoryStringTable


class TestStringTable(TestCase):
    """
    Basic Single Threaded test for String Table.
    This does not test for concurrency or persistence

    All StringTable implementations must pass this test.

    But this test is not sufficient to test the StringTable implementations
    required for production use
    """

    def setUp(self):
        """
        Initialize the InMemoryString table and creates some token entries
        :return:
        """
        self.string_table = InMemoryStringTable()
        self.v_abc = self.string_table.get_token_value('abc')
        self.v_def = self.string_table.get_token_value('def')

    def test_tokenize_string(self):
        """
        Checks that the strings can be tokenized and detokenized back to
        original value
        :return:
        """
        self.assertEqual(self.v_abc, self.string_table.get_token_value('abc'))
        self.assertEqual(self.string_table.get_str_value(self.v_abc), 'abc')

        self.assertEqual(self.v_def, self.string_table.get_token_value('def'))
        self.assertEqual(self.string_table.get_str_value(self.v_def), 'def')

    def test_unknown_token(self):
        """
        Unknown tokens are expected to throw an error when trying to detokenize
        this test checks for that
        :return:
        """
        self.assertEqual(self.string_table.get_str_value(self.v_abc), 'abc')
        self.assertRaises(RuntimeError, self.string_table.get_str_value,
                          100000)

    def test_tokenize_dictionary(self):
        """
        Tests the tokenization of the dictionary by verifying that we
        reuse the tokens already present in the string table
        and that all keys and values of the dictionary are correctly tokenized
        :return:
        """
        dic = dict()
        dic['abc'] = 'ghi'
        dic['def'] = 'jkl'
        v_dic = self.string_table.tokenize_dictionary(dic)

        self.assertTrue(self.v_abc in v_dic)
        self.assertTrue(self.v_def in v_dic)

        self.assertEqual(v_dic[self.v_abc],
                         self.string_table.get_token_value('ghi'))

        self.assertEqual(v_dic[self.v_def],
                         self.string_table.get_token_value('jkl'))

    def test_identity_string_table(self) -> None:
        """
        Tests the identity string table
        """
        identity_string_table = IdentityStringTable()
        self.assertEqual('abc', identity_string_table.get_token_value('abc'))
        self.assertEqual('abc', identity_string_table.get_str_value('abc'))
