"""
MetricKey
"""
# Required for specifying self type before the self is defined
# Some methods return MetricKey objects
from __future__ import annotations
from nbdb.string_table.string_table_interface import StringTableInterface


class MetricKey:
    """
    Represents the metadata associated with a metric
    A metric is represented using Influx schema of measurement, field and tags
    There are two distinct forms supported
    1) Human readable text based. Here metadata is of string type
    2) Machine optimized compressed. Here metadata is of integer type
    The same object can support both and provides generator methods
    to convert human friendly to machine optimized
    """

    def __init__(self, field: str, tags: dict) -> None:
        """
        Initialize the Metric Key object using the metadata provided
        :param field: <String or integer>
        :param tags: Dictionary where key,values can be string or integer
        """
        self.field = field
        self.tags = tags
        # Series id is a generated field and can be generated only for machine
        # optimized metric key
        self.series_id = None

    def is_tokenized(self) -> bool:
        """
        Checks if its a compressed or uncompressed form
        :return:
        """
        return self.series_id is not None

    def generate_tokenized_metric_key(self,
                                      string_table: StringTableInterface)\
            -> MetricKey:
        """
        Generates a new metric key using the provided string table
        :param string_table: String table object to use to create the
        compressed key
        :return: compressed metric key with series id generated
        """
        metric_key = MetricKey(string_table.get_token_value(self.field),
                               string_table.tokenize_dictionary(self.tags))
        metric_key.series_id = metric_key.generate_series_id()
        return metric_key

    def generate_series_id(self) -> str:
        """
        Generate the string representation of the metric by concatenating
        the tokenized values of measurement, field and sorted tags
        The MetricKey must be a tokenized version
        :return: string
        """
        series_id = [str(self.field)]
        # NOTE: We must iterate over the tag keys in a sorted order to make
        # sure we generate the same series ID everytime.
        for tag_key in sorted(self.tags.keys()):
            series_id.append('{}={}'.format(tag_key, self.tags[tag_key]))

        return '.'.join(series_id)
