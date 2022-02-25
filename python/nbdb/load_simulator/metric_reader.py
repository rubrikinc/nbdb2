"""
TestMetricProducer
"""

import json
import logging
import time

import requests
from nbdb.common.telemetry import Telemetry

logger = logging.getLogger()


#pylint: disable=too-few-public-methods
class MetricReader:
    """
    This is a test class for reading metrics from Flask
    """
    def __init__(self, reader_settings):
        """
        Initialize the Test metric reader
        :param reader_settings: Test reader settings
        """
        self.reader_settings = reader_settings

    def run_query(self, query: str):
        """Check that query is successful and output is not empty."""
        params = {'q': query, 'epoch': 's'}
        url = "https://{}/query".format(self.reader_settings.flask_addr)
        dur = time.time()
        req = requests.get(url, params=params)
        Telemetry.inst.registry.meter('MetricReader.requests').mark()
        if not req.ok:
            Telemetry.inst.registry.meter('MetricReader.failed').mark()
            logger.info('FAILED: %s: %s %s',
                        str(req.status_code), query, req.text)
            req.raise_for_status()
        Telemetry.inst.registry.histogram('MetricReader.resp_size').add(
            len(req.text))
        dur = time.time() - dur
        Telemetry.inst.registry.histogram('MetricReader.resp_dur').add(dur)
        return json.loads(req.text)
