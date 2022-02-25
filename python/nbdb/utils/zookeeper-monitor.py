#! /usr/bin/env python3
"""
Utility to fetch zookeeper metrics and send them to timestream

This script modifies a python script on Zookeeper repo to work on a single container
https://github.com/apache/zookeeper/blob/12efc9f88705796cb905b46159e1d519323b1d9c/src/contrib/monitoring/check_zookeeper.py

The following are the differences
1. Moved to python3
2. Changed the Main class to give node level stats
"""

import argparse
import logging
import socket
import sys
from io import StringIO
from time import time, sleep

import boto3

# Hardcoded values
TIMESTREAM_DB = "anomalydb-metrics"
TIMESTREAM_TABLE = "anomalydb-druid-metrics"
TS_FMT = "%Y-%m-%dT%H:%M:%S"
MAX_TIMESTREAM_RECORDS_PER_CALL = 100



log = logging.getLogger()
logging.basicConfig(level=logging.ERROR)

# pylint: disable-msg=R0903 # Too Few Public Methods
class ZookeeperMetricsGenerator():
    """Connect to Zookeeper Server and generate metrics"""
    def __init__(self, host='localhost', port='2181', timeout=5):
        self._address = (host, int(port))
        self._timeout = timeout

    def get_stats(self):
        """ Get ZooKeeper server stats as a map """
        data = self._send_cmd('mntr')
        if data:
            return self._parse_mntr(data)

        log.error("No data returned from the mntr command")
        return {}

    def get_node_stats(self):
        """ Get stats for a node """
        try:
            stats = self.get_stats()

        except socket.error as e:
            log.error('%s \n Unable to connect to zookeeper server',
                      repr(e))

        return stats

    def _send_cmd(self, cmd: str):
        """ Send a 4letter word command to the server """
        s = socket.socket()
        s.settimeout(self._timeout)

        s.connect(self._address)
        s.send(cmd.encode())

        data = s.recv(2048)
        s.close()

        return data

    def _parse_mntr(self, data: bytes):
        """
        Parse the output from the 'mntr' 4letter word command

        Sample output of the mntr command, all metrics except zk_version
        and zk_server_state are of type BIGINT

        zk_version 3.4.14-4c25d480e66aadd371de8bd2fd8da255ac140bcf, built on 03/06/2019 16:18 GMT
        zk_avg_latency 14
        zk_max_latency 1684
        zk_min_latency 0
        zk_packets_received 18306044030
        zk_packets_sent 28105840678
        zk_num_alive_connections 316
        zk_outstanding_requests 0
        zk_server_state follower
        zk_znode_count 20454
        zk_watch_count 616964
        zk_ephemerals_count 14877
        zk_approximate_data_size 18393251
        zk_open_file_descriptor_count 523
        zk_max_file_descriptor_count 16384
        zk_fsync_threshold_exceed_count 15
        """
        h = StringIO(data.decode())

        result = {}
        for line in h.readlines():
            try:
                key, value = self._parse_line(line)
                result[key] = value
            except ValueError:
                pass # ignore broken lines

        return result


    @classmethod
    def _parse_line(cls, line: str):
        try:
            key, value = list(map(str.strip, line.split('\t')))
        except ValueError as invalid_line:
            raise ValueError('Found invalid line: %s' % line) from invalid_line

        if not key:
            raise ValueError('The key is mandatory and should not be empty')

        try:
            value = int(value)
        except (TypeError, ValueError):
            pass

        return key, value


def parse_args(args):
    """Parse command line arguments."""
    argparser = argparse.ArgumentParser(
        description='Get Zookeeper metrics from all hosts and publish them to Timestream')

    argparser.add_argument("--nodes", type=str,
                           default='',
                           help="Comma separated list of nodes")

    argparser.add_argument("--debug", type=str,
                           default=False,
                           required=False,
                           help="Debug mode, print metrics generated")

    return argparser.parse_args(args)


def main(nodes: list):
    """
    Main function which polls every 10 minutes and publishes the metrics
    """
    stats_interval = 10 * 60
    zk_metrics_generator_dict = {}
    for node in nodes:
        zk_metrics_generator_dict[node] = ZookeeperMetricsGenerator(node)

    while True:
        sleep(stats_interval - time() % stats_interval)
        timestamp = time()
        for node in nodes:
            node_stats = zk_metrics_generator_dict[node].get_node_stats()
            report_metrics(node_stats, node, timestamp)
            dump_stats(node_stats)


def report_metrics(node_stats: dict, node_ip: str, timestamp: int):
    """
    Report metrics to timestream
    """
    timestream_write_client = boto3.client("timestream-write",
                                           region_name="us-east-1")
    common_attributes = {
        'Dimensions': [
            {'Name': 'hostname',
             'Value': node_ip,
             'DimensionValueType': 'VARCHAR'},
        ]
    }

    records = []
    for key, value in list(node_stats.items()):
        if key in ('zk_version', 'zk_server_state'):
            continue
        records.append(
            {
                'Dimensions': [
                ],
                'MeasureName': key,
                'MeasureValue': str(value),
                'MeasureValueType': 'BIGINT',
                'Time': str(int(timestamp)),
                'TimeUnit': 'SECONDS'
            }
        )
    report_all_timestream_metrics(timestream_write_client, TIMESTREAM_DB,
                                  TIMESTREAM_TABLE, records, common_attributes)


# pylint: disable=R0913  # Too Many Arguments
def report_all_timestream_metrics(timestream_write_client, database_name,
                                  table_name, metrics, common_attributes):
    """Send out all metrics to TSDB."""
    assert isinstance(metrics, list)
    assert TIMESTREAM_DB is not None
    try:
        for idx in range(0, len(metrics), MAX_TIMESTREAM_RECORDS_PER_CALL):
            records = metrics[idx:idx + MAX_TIMESTREAM_RECORDS_PER_CALL]
            timestream_write_client.write_records(
                DatabaseName=database_name,
                TableName=table_name,
                CommonAttributes=common_attributes,
                Records=records)
    except timestream_write_client.exceptions.RejectedRecordsException as exc:
        log.error('Failed to report TSDB metrics because '
                  'some records were rejected')
        for rej_info in exc.response['RejectedRecords']:
            idx = rej_info['RecordIndex']
            reason = rej_info['Reason']
            log.error('Rejected record: %s, common: %s for reason: %s',
                      records[idx], common_attributes, reason)
        raise exc
    except Exception as exc:
        log.warning('Failed to report TSDB metrics (Eg. %s) to table %s',
                 metrics[0], TIMESTREAM_TABLE, exc_info=True)
        log.error('Error: %s', exc)
        raise exc


def dump_stats(node_stats: dict):
    """ Dump stats into stdout """
    for key, value in list(node_stats.items()):
        log.debug(("%s %s", (key, value)))



if __name__ == '__main__':
    # Parse & validate command line arguments
    input_args = parse_args(sys.argv[1:])

    # Convert nodes to a list
    zkpr_nodes = input_args.nodes.split(',')

    if input_args.debug:
        logging.getLogger().setLevel(logging.DEBUG)

    sys.exit(main(zkpr_nodes))
