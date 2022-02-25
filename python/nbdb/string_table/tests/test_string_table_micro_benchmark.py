"""
TestStringTable
"""
import os
from unittest import TestCase

from nbdb.string_table.in_memory_string_table import InMemoryStringTable


def to_base(n, b=1000000):
    """
    represent the number n in base B string
    :param n:
    :param b:
    :return:
    """
    return "0" if not n else to_base(n // b, b).lstrip("0") + chr(n % b)


class TestStringTable(TestCase):
    """
    Basic Single Threaded test for String Table.
    This does not test for concurrency or persistence

    All StringTable implementations must pass this test.

    But this test is not sufficient to test the StringTable implementations
    required for production use
    """
    @staticmethod
    def fields(line):
        """
        extract words from line
        :param line:
        :return:
        """
        parts = line.split('.')
        if len(parts) > 6:
            measurement = parts[0] + '.' + parts[3] + '.' + parts[4] + '.' + parts[5]
            return list((measurement, parts[1], parts[2])) + parts[6:]
        return parts

    def test_string_table_perf(self) -> None:
        """
        Test an offline tokenizer by using in memory string table
        """
        string_table = InMemoryStringTable()
        hulk_file_path = os.path.join(os.path.dirname(__file__),
                                      '../../utils/hulk.txt')
        with open(hulk_file_path) as f:
            lines = f.readlines()
            word_freq = dict()
            # compute the word freq
            for line in lines:
                parts = TestStringTable.fields(line)
                for part in parts:
                    if part not in word_freq:
                        word_freq[part] = 0
                    word_freq[part] = word_freq[part] + 1
            word_freq_items = list()
            for word, freq in word_freq.items():
                word_freq_items.append((freq, word))
            # sort words by highest freq first
            word_freq_items.sort(key=lambda x: x[0], reverse=True)
            # assign the smallest token value to highest freq token first
            for freq, word in word_freq_items:
                string_table.get_token_value(word)

            # now tokenize
            tokenized_line_map = list()
            tokenized_lines = list()
            osize = 0
            tsize = 0
            for line in lines:
                token_parts = list()
                parts = self.fields(line)
                for part in parts:
                    token = string_table.get_token_value(part)
                    token_parts.append(to_base(token))
                tokenized_line = '.'.join(token_parts)
                tsize = tsize + len(tokenized_line)
                osize = osize + len(line)
                tokenized_lines.append(tokenized_line)
                tokenized_line_map.append((line, tokenized_line))

            print('original: {} tokenized: {} Ratio: {}'.
                  format(osize, tsize, tsize/osize))

            self.assertGreater(0.14, tsize/osize)

