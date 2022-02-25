"""
TracingConfig module
"""

from __future__ import annotations

import logging
import os
from datetime import datetime
from typing import Dict

import boto3
import botocore
import yaml

logger = logging.getLogger()
client = boto3.client("s3")


# This class should be turned into a data class
# pylint: disable-msg=R0903 # Too Few Public Methods
class TracingConfig:
    """Helper class to store tracing configuration."""
    inst: TracingConfig = None
    # Global trace (assumes single threaded operation for the write path)
    TRACE_ACTIVE: bool = False

    def __init__(self, tracing_settings, is_local=False):
        """
        Initialize the tracing config rules
        """
        self.tracing_settings = tracing_settings
        self.last_tracing_config_download_time = None
        self.tracing_tag_sets = []
        if not is_local:
            self.download_tracing_config_from_s3()
        else:
            tracing_config_path = os.path.join(
                os.path.dirname(__file__), "..", "config", "tracing_config.yaml")
            with open(tracing_config_path) as text:
                self.tracing_tag_sets = yaml.safe_load(text)

    @staticmethod
    def initialize(tracing_config, is_local=False):
        """
        Initialize the tracing config helper
        :return:
        """
        if TracingConfig.inst is None:
            TracingConfig.inst = TracingConfig(tracing_config, is_local)

    def download_tracing_config_from_s3(self) -> None:
        """
        Check if a new tracing config was uploaded to S3 and if so, download &
        initialize tracing config
        :return:
        """
        bucket = self.tracing_settings.s3_bucket
        key = self.tracing_settings.s3_key
        if not bucket or not key:
            logger.error("Bucket %s or key %s empty. Skipping tracing config "
                         "download", bucket, key)
            return

        if_modified_since = self.last_tracing_config_download_time
        try:
            if if_modified_since is not None:
                resp = client.get_object(Bucket=bucket, Key=key,
                                         IfModifiedSince=if_modified_since)
            else:
                resp = client.get_object(Bucket=bucket, Key=key)
        except botocore.exceptions.ClientError:
            logger.info("Tracing config file did not change. Return")
            return
        except client.exceptions.NoSuchKey:
            logger.error("Tracing config file %s not found in bucket %s", key,
                        bucket)
            return
        else:
            text = resp['Body'].read()
            logger.info('TracingConfig: New traces loaded:\n %s', text)
            self.last_tracing_config_download_time = datetime.utcnow()

        # Update the global tracing config
        self.load_from_text(text)

    def load_from_text(self, text: str) -> None:
        """
        Load the tracing config tag set rules from the given text
        """
        self.tracing_tag_sets = yaml.safe_load(text)

    def match(self, metric_tags: Dict[str, str]):
        """
        Check if all the tag keys & tag values in the metric matches the
        tracing config
        """
        for trace_tags in self.tracing_tag_sets:
            matched = True
            for tag_key in trace_tags:
                # Check if the trace tag key is present in the metric tags
                if tag_key not in metric_tags:
                    matched = False
                    break

                # Check if the metric tag value is among the trace tag values
                if metric_tags[tag_key] not in trace_tags[tag_key]:
                    matched = False
                    break

            # If we reached this point, it means the metric has all the
            # necessary trace tag keys & tag values
            if matched:
                return True

        # If we reached this point, none of the trace rules matched
        return False
