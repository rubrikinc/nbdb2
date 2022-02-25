"""
SchemaManager
"""
import json
from typing import List
from datetime import datetime

import kafka
from pyformance import meter_calls, time_calls

from nbdb.common.metric_key import MetricKey


class SchemaManager:
    """
    Manages writes and reads from the Elastic
    """

    def __init__(self, kafka_brokers: List[str], topic_name: str,
                 index_prefix: str) -> None:
        """
        Creates a kafka producer
        :param kafka_brokers: Kafka brokers to which docs will be sent
        :param topic_name: Kafka topic to use while sending docs
        :param index_prefix: ES index prefix to be used for docs
        """
        self.kafka_brokers = kafka_brokers
        self.topic_name = topic_name
        self.index_prefix = index_prefix
        self.producer = None

    @time_calls
    @meter_calls
    def connect(self):
        """
        Creates a kafka producer
        :return:
        """
        self.producer = kafka.KafkaProducer(bootstrap_servers=
                                            self.kafka_brokers)

    @time_calls
    @meter_calls
    def new_metric(self,
                   metric_key: MetricKey,
                   start_epoch: int,
                   ttl_epoch: int) -> None:
        """
        Submits a new schema document to the elasticsearch
        :param metric_key: Tokenized metric key
        :param start_epoch: start of the metric
        :param ttl_epoch: Future expiry time
        """
        doc = self._generate_elastic_doc(metric_key, start_epoch, ttl_epoch)
        self.producer.send(self.topic_name, value=doc.encode('utf-8'))

    def _generate_elastic_doc(self, metric_key: MetricKey,
                              start_epoch: int,
                              ttl_epoch: int) -> str:
        """
        Generates the elastic json document assuming this is a compressed form
        :return: json document representing the metric key
        """
        assert metric_key.is_tokenized()
        doc = dict()
        # TODO: Use different indices for ephemeral series and non-ephemeral
        # series
        today = datetime.strftime(datetime.utcnow(), "%Y-%m-%d")
        doc['_index'] = '{}_{}'.format(self.index_prefix, today)

        doc['_type'] = '_doc'
        doc['Field'] = metric_key.field
        doc['Tags'] = ['%s=%s' % (k, v) for (k, v) in metric_key.tags.items()]
        doc['StartEpoch'] = start_epoch
        doc['EndEpoch'] = ttl_epoch

        return json.dumps(doc)
