"""
Settings parsing and type checking module
"""
from __future__ import annotations

from dataclasses import dataclass
import time
from typing import List
import yaml


@dataclass
class EsConnectionSettings:
    """ES connection settings"""

    def __init__(self, conn_settings: dict) -> None:
        """Init.

        :param conn_settings: ES connection settings dict
        """
        self.host: str = conn_settings["host"]
        self.port: int = conn_settings["port"]
        self.region: str = conn_settings["region"]
        self.aws_access_key_id: str = conn_settings["aws_access_key_id"]
        self.aws_secret_access_key: str = conn_settings["aws_secret_access_key"]


@dataclass
class EsIndexSettings:
    """ES index settings"""

    def __init__(self, index_settings: dict) -> None:
        """Init.

        :param index_settings: ES index settings dict
        """
        self.index_prefix: str = index_settings["index_prefix"]
        self.num_replicas: int = index_settings["num_replicas"]
        self.num_shards: int = index_settings["num_shards"]
        self.recreate_index_template: bool = index_settings[
            "recreate_index_template"]


@dataclass
class EsWritesSettings:
    """ES write operation settings"""

    def __init__(self, write_settings: dict) -> None:
        """Init.

        :param write_settings: ES write settings dict
        """
        self.queue_size: int = write_settings["queue_size"]
        self.rate_limit: int = write_settings["rate_limit"]
        self.batch_size: int = write_settings["batch_size"]
        self.batch_write_timeout_secs: int = write_settings[
            "batch_write_timeout_secs"]
        self.ratelimit_sleep_time: float = write_settings[
            "ratelimit_sleep_time"]
        self.ratelimit_reset_time: float = write_settings[
            "ratelimit_reset_time"]


@dataclass
class EsSettings:
    """ES settings"""

    def __init__(self, settings: dict) -> None:
        """Init.

        :param settings: Dictionary representing ES settings
        """
        self.connection = EsConnectionSettings(settings["connection"])
        self.index = EsIndexSettings(settings["index"])
        self.writes = EsWritesSettings(settings["writes"])


@dataclass
class SparseBatchConsumerSettings:
    """sparse batch consumer settings"""

    def __init__(self, settings: dict) -> None:
        """
        Init.

        :param settings: Dictionary representing batch consumer settings
        """
        self.kafka_brokers: str = settings["kafka_brokers"]
        self.topic: str = settings["topic"]
        self.group: str = settings["group"]
        self.metric_protocol: str = settings["metric_protocol"]
        self.num_consumers: int = settings["num_consumers"]
        self.max_inflight_tasks: int = settings["max_inflight_tasks"]
        self.max_messages_to_combine: int = settings["max_messages_to_combine"]
        self.max_inflight_messages: int = settings["max_inflight_messages"]
        self.num_days_to_fetch_on_assign: int = \
            settings["num_days_to_fetch_on_assign"]
        self.s3_bucket = settings["s3_bucket"]


@dataclass
class DenseBatchConsumerSettings:
    """dense batch consumer settings"""

    def __init__(self, settings: dict) -> None:
        self.kafka_brokers: str = settings["kafka_brokers"]
        self.topic: str = settings["topic"]
        self.group: str = settings["group"]
        self.num_consumers: int = settings["num_consumers"]
        self.s3_bucket: str = settings["s3_bucket"]
        self.past_message_lag: int = settings["past_message_lag"]
        self.future_message_lag: int = settings["future_message_lag"]
        self.conversion_whitelist: List[str] = settings["conversion_whitelist"]

@dataclass
class DruidConnectionStringSettings:
    """druid connection string settings"""

    def __init__(self, settings: dict) -> None:
        self.router_ip: str = settings["router_ip"]
        self.router_port: int = settings["router_port"]
        self.cc_router_ip: str = settings["cc_router_ip"]
        self.cc_router_port: int = settings["cc_router_port"]
        self.overlord_ip: str = settings["overlord_ip"]
        self.overlord_port: int = settings["overlord_port"]
        self.read_path: str = settings["read_path"]
        self.scheme: str = settings["scheme"]
        self.schema_refresh_time: int = settings["schema_refresh_time"]

@dataclass
class DruidDatasourceSettings:
    """druid data source settings"""

    def __init__(self, settings: dict) -> None:
        self.maxRowsInMemory: int = settings["maxRowsInMemory"]
        self.maxRowsPerSegment: int = settings["maxRowsPerSegment"]
        self.lateMessageRejectionPeriod: str = settings[
            "lateMessageRejectionPeriod"]
        self.earlyMessageRejectionPeriod: str = settings[
            "earlyMessageRejectionPeriod"]


@dataclass
class DruidBatchIngestSettings:
    """druid batch ingest settings"""

    def __init__(self, settings: dict) -> None:
        self.maxNumConcurrentSubTasks: int = \
            settings["maxNumConcurrentSubTasks"]

@dataclass
class DruidSettings:
    """druid settings"""

    def __init__(self, settings: dict) -> None:
        self.connection_string = DruidConnectionStringSettings(
            settings["connection_string"])
        self.datasource = DruidDatasourceSettings(settings["datasource"])
        self.batch_ingest = DruidBatchIngestSettings(settings["batch_ingest"])

@dataclass
class Settings:
    """
    The settings object represent the yaml settings
    The yaml structure is parsed from the file and corresponding
    python objects are initialized.
    We should be able to query the following yaml snippet
        sparse_store:
          bucket_size: 1000
    as
        Settings.sparse_store.bucket_size

    """
    # Global singleton for settings
    inst: Settings = None

    def __init__(self, setting: dict) -> None:
        """
        Initialize the object using the provided structure
        :param entries: should be loaded from the yaml
        """
        for a, b in setting.items():
            if isinstance(b, (list, tuple)):
                setattr(self, a,
                        [Settings(x) if isinstance(x, dict) else x for x in b])
            elif a == "elasticsearch":
                setattr(self, a, EsSettings(b))
            elif a == "sparse_batch_consumer":
                self.sparse_batch_consumer = SparseBatchConsumerSettings(b)
            elif a == "dense_batch_consumer":
                self.dense_batch_consumer = DenseBatchConsumerSettings(b)
            elif a == "Druid":
                self.Druid = DruidSettings(b)
            else:
                if isinstance(b, dict):
                    a_value = Settings(b)
                    if a == "test_metric_producer":
                        Settings.parse_test_metric_producer_settings(a_value)
                    setattr(self, a, a_value)
                else:
                    setattr(self, a, b)

    @staticmethod
    def load_yaml_settings(yaml_setting_file_path: str) -> 'Settings':
        """
        Use this method to obtain the settings object from the yaml file
        :param yaml_setting_file_path:
        :return: Settings object representing the yaml file content
        """
        with open(yaml_setting_file_path) as filename:
            settings = yaml.safe_load(filename)
            Settings.inst = Settings(settings)
            return Settings.inst

    @staticmethod
    def parse_test_metric_producer_settings(
            producer_settings: Settings) -> None:
        """
        Generate start, end epoch based on the different settings
        :param producer_settings:
        :return:
        """
        # check what time range is specified
        if not hasattr(producer_settings, 'epoch_start'):
            if not hasattr(producer_settings, 'last_hours'):
                raise ValueError('TestMetricProducer producer_settings must '
                                 'specify epoch_start/end or '
                                 'last_hours/interval_seconds')
            producer_settings.epoch_end = int(time.time())
            producer_settings.epoch_start = \
                producer_settings.epoch_end - \
                producer_settings.last_hours * 3600
