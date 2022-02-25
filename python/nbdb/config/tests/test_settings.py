"""
TestSettings
"""

import os

from unittest import TestCase
from nbdb.config.settings import Settings


class TestSettings(TestCase):
    """
    Test the yaml settings parser
    """

    def setUp(self) -> None:
        """
        Setup the test by loading the test_settings yaml
        :return:
        """
        Settings.load_yaml_settings(os.path.dirname(__file__) +
                                    '/test_settings.yaml')

    def test_few_configured_yaml_settings(self):
        """
        Tests that we are able to load the yaml and parse the structure
        as a python object
        :return:
        """
        self.assertEqual(1000, Settings.inst.sparse_store.bucket_size)
        self.assertEqual(0.01, Settings.inst.sparse_store.min_delta)
        self.assertEqual(1000000,
                         Settings.inst.sparse_store.data_points.queue_size)
        self.assertEqual('scylla',
                         Settings.inst.scylladb.connection_string.username)
        self.assertEqual(3,
                         len(Settings.inst.scylladb.connection_string.
                             contact_points))
