"""KafkaUtils"""
import logging

import kafka
from retrying import retry

logger = logging.getLogger()


class KafkaUtils:
    """Common kafka utils"""
    @staticmethod
    @retry(stop_max_attempt_number=8, wait_fixed=15000)
    def wait_for_topic_status(bootstrap_servers, topic_name,
                                exists=True) -> None:
        """Wait for topic to exist or to be deleted."""
        client = kafka.KafkaClient(bootstrap_servers)
        logger.info("Check for topic %s status exists=%s client topics=%s",
                    topic_name, exists, str(client.topics))
        if exists:
            assert topic_name in client.topics
        else:
            assert topic_name not in client.topics
