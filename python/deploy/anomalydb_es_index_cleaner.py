"""Lambda which cleans up oldest ES indices for AnomalyDB."""

import logging
import os
from datetime import datetime, timedelta

from elasticsearch import Elasticsearch, RequestsHttpConnection
from aws_requests_auth.aws_auth import AWSRequestsAuth


ELASTIC_SEARCH_HOST = os.environ.get("ELASTIC_SEARCH_HOST")
INDEX_PREFIX = os.environ.get("INDEX_PREFIX")
DELETE_DELTA_DAYS = int(os.environ.get("DELETE_DELTA_DAYS"))

auth = AWSRequestsAuth(
    aws_access_key=os.environ['AWS_ACCESS_KEY_ID'],
    aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'],
    aws_token=os.environ['AWS_SESSION_TOKEN'],
    aws_host=ELASTIC_SEARCH_HOST,
    aws_region='us-east-1',
    aws_service='es')

es_client = Elasticsearch(
    host=ELASTIC_SEARCH_HOST,
    port=443,
    http_auth=auth,
    use_ssl=True,
    connection_class=RequestsHttpConnection
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def delete_old_index():
    """Delete oldest index in ES."""
    oldest_day = datetime.now() - timedelta(days=DELETE_DELTA_DAYS)
    index_name = "{}_{}".format(INDEX_PREFIX, oldest_day.strftime("%Y-%m-%d"))
    logger.info("Checking for index %s", index_name)

    # Delete index if it exists
    if es_client.indices.exists(index=index_name):
        logger.info("Found index %s to delete", index_name)
        es_client.indices.delete(index=index_name)


def lambda_handler(event, context):
    """Main handler function which is invoked."""
    del event, context
    delete_old_index()


if __name__ == '__main__':
    lambda_handler(None, None)
