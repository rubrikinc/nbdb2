"""
Utility to check the hydration status for a given cluster
"""

import cProfile
import argparse
import atexit
import json
import math
import logging
import os
import socket
import sys
import time
from typing import Dict, Tuple

from sshtunnel import SSHTunnelForwarder
from nbdb.common.telemetry import Telemetry
from nbdb.common.thread_pools import ThreadPools
from nbdb.config.settings import Settings
from nbdb.readapi.druid_reader import DruidReader
from nbdb.readapi.graphite_api import GraphiteApi
from nbdb.readapi.graphite_explorer import GraphiteExplorer
from nbdb.readapi.query_cache import QueryCache
from nbdb.readapi.sql_parser import SqlParser
from nbdb.schema.schema import Schema
from werkzeug.datastructures import ImmutableMultiDict

logger = logging.getLogger(__name__)

"""
ENTER YOUR JUMPBOX CRED HERE
Jumpbox - server/instance which can connect to all the services or nodes
          deployed in ECS
"""
JUMPBOX_IP = "X.X.X.X"
JUMPBOX_USER = "ec2-user"


# Epilog message covering how to use script
epilog = """
Examples
--------
1. Run a query over the last 6 hours
python benchmark_read_query.py --render="clusters.*.*.Diamond.uptime.seconds"
--from=now-6h --until=now --maxDataPoints=36 --router_ip=X.X.X.X

2. Run a query using absolute timestamps
python benchmark_read_query.py --render="clusters.*.*.Diamond.uptime.seconds"
--from=12:16_20210601 --until=18:16_20210601 --router_ip=X.X.X.X

3. Run queries from targets file
python3.7 benchmark_read_query.py --targets_file="/tmp/targets.json"
--from=now-60m --until=now --router_ip=X.X.X.X

An example for /tmp/targets.json:
{
  "targets": [
    ["render", "clusters.cluster-uuid-x-y-z.cluster_name.svc.process.job.req_time.*||nocache=true"],
    ["render", "clusters.cluster-uuid-x-y-z.cluster_name.svc.process.job.req_time.*||nocache=true"]
  ]
}

Note that a 1 sec sleep is performed between queries. Also, the time range from
the command applies to all queries in the file (could be refined in the future).

"""


def parse_args():
    """
    Parse arguments
    """
    argparser = argparse.ArgumentParser(
        description='Benchmark read query locally',
        epilog=epilog,
        # We do not want line-wrapped formatting for description & epilog
        formatter_class=argparse.RawDescriptionHelpFormatter)

    argparser.add_argument('--render', type=str, default='',
                           help='Graphite render query target')
    argparser.add_argument('--find', type=str, default='',
                           help='Graphite find query target')
    argparser.add_argument('--targets_file', type=str, default='',
                           help='Read also targets from file')
    argparser.add_argument('--iterations', type=int, default=1,
                           help='Repeat with specified num of iterations')
    argparser.add_argument('--from', dest='from_str', type=str, required=True,
                           help='Query start time')
    argparser.add_argument('--until', type=str, required=True,
                           help='Query end time')
    argparser.add_argument('--router_ip', type=str, required=True,
                           help='AnomalyDB router private IP')
    argparser.add_argument('--maxDataPoints', type=int, default=2000,
                           help='Max number of datapoints. '
                           'Default is very high')
    argparser.add_argument('--user', type=str, default='anomalydb_grafana',
                           help='Run query as user')
    argparser.add_argument('--enable_redis_cache', action='store_true',
                           help='Enable use of Redis cache')
    argparser.add_argument('--enable_profiling', action='store_true',
                           help='Enable cProfile')
    argparser.add_argument('--print_response', action='store_true',
                           help='Print query response')
    argparser.add_argument('--truncate_response', type=int, default=10000,
                           help='Truncate query response printed')

    args = argparser.parse_args()

    # validate args
    assert args.router_ip.startswith('172.31'), \
        "%s does not seem like a private IP" % args.router_ip
    assert args.render or args.find or args.targets_file, \
        "Must use at least one of: --render, --find, --targets_file"

    return args


def get_targets(render_target, find_target, targets_file):
    """
    Return a list of targets to execute, e.g.:
    [
      ["render", "TARGET_1"],
      ["render", "TARGET_2"],
      ["find", "TARGET_k"]
    ]
    """
    targets = []

    # add --render and -find targets, if specified in args
    if render_target:
        targets.append(['render', render_target])
    if find_target:
        targets.append(['find', find_target])

    # add targets from --targets_file, if specified in args
    if os.path.exists(targets_file):
        with open(targets_file, 'r', encoding='utf-8') as f:
            targets += json.loads(f.read())["targets"]
    return targets


class Benchmark:
    """
    Run benchmark locally against prod data
    """
    def __init__(self, args):
        self.args = args
        self.router_forwarder = None
        self.redis_forwarder = None
        self.schema = None
        self.null_logger = logging.getLogger('null')
        self.null_logger.addHandler(logging.NullHandler())
        self.null_logger.propagate = False

        atexit.register(self.cleanup)

    def setup_router_port_forwarding(self) -> None:
        """
        Start SSHTunnelForwarder to Druid router via Jumpbox.
        """
        self.router_forwarder = SSHTunnelForwarder(
            JUMPBOX_IP, ssh_username=JUMPBOX_USER, logger=self.null_logger,
            remote_bind_address=(self.args.router_ip, 80)
        )
        self.router_forwarder.start()

    def setup_redis_port_forwarding(self) -> None:
        """
        Start SSHTunnelForwarder to Redis cache via Jumpbox.
        """
        redis_addr = Settings.inst.sql_api.host
        redis_port = Settings.inst.sql_api.port
        self.redis_forwarder = SSHTunnelForwarder(
            JUMPBOX_IP, ssh_username=JUMPBOX_USER, logger=self.null_logger,
            remote_bind_address=(redis_addr, redis_port),
        )
        self.redis_forwarder.start()

    def cleanup(self):
        """
        Stop SSHTunnelForwarder at exit.
        """
        if self.router_forwarder:
            self.router_forwarder.stop()
        if self.redis_forwarder:
            self.redis_forwarder.stop()

    def setup_prereqs(self):
        """
        Load prod configuration, schema, telemetry and then start DruidReader.
        """
        # Load prod config & schema
        curr_file_path = os.path.abspath(__file__)
        Settings.load_yaml_settings(
            os.path.dirname(curr_file_path) + '/../config/settings_prod.yaml')
        self.schema = Schema.load_from_file(
            os.path.dirname(curr_file_path) + '/../config/schema_prod.yaml')

        # Modify Druid connection settings to talk to the local port of our
        # port forwarding session
        assert (self.router_forwarder and
                self.router_forwarder.local_bind_port), \
            "Port forwarding failed. Were your SSH keys added on Jumpbox?"
        Settings.inst.Druid.connection_string.router_ip = '127.0.0.1'
        Settings.inst.Druid.connection_string.router_port = \
            self.router_forwarder.local_bind_port
        Settings.inst.Druid.connection_string.cc_router_ip = '127.0.0.1'
        Settings.inst.Druid.connection_string.cc_router_port = \
            self.router_forwarder.local_bind_port

        # Instantiate Telemetry, but disable reporters
        Telemetry.inst = None
        Telemetry.initialize(start_reporter=False)

        DruidReader.inst = None
        DruidReader.instantiate(Settings.inst.Druid.connection_string)
        ThreadPools.instantiate()

        if (self.args.enable_redis_cache and
                Settings.inst.sql_api.cache_provider == 'redis'):
            # Setup port forwarding to Redis, update config settings & start
            # query cache
            self.setup_redis_port_forwarding()
            Settings.inst.sql_api.host = '127.0.0.1'
            Settings.inst.sql_api.port = self.redis_forwarder.local_bind_port
            QueryCache.initialize()

    @staticmethod
    def get_metric_value(metrics: Dict, req_metric: Tuple):
        """
        Get value for the given metric from the generated metrics dump

        :param metrics: Generated metrics dump
        :param req_metric: Tuple containing the measurement name, tag key-value
        pairs and the metric type
        """
        measurement, tag_kv_pairs, metric_type = req_metric
        # Add Identifier & Environment tag
        tag_kv_pairs = [
            f"Identifier={socket.gethostname()}",
            f"Environment={Settings.inst.telemetry.environment}"
        ] + tag_kv_pairs

        key = measurement + "," + ",".join(tag_kv_pairs)

        # We only print one value. Since there was only one query run, it makes
        # sense to print the avg / median
        if metric_type == 'histogram':
            field = 'avg'
        elif metric_type == 'timer':
            field = 'sum'
        elif metric_type == 'meter':
            field = 'count'
        else:
            assert False, "Unknown metric type: %s" % metric_type
        if key not in metrics:
            return 0
        return metrics[key].get(field, 0)

    @staticmethod
    def dump_metric(metrics: Dict, req_metric: Tuple):
        """
        Print value for the given metric from the generated metrics dump.

        :param metrics: Generated metrics dump
        :param req_metric: Tuple containing the measurement name, tag key-value
        pairs and the metric type
        """
        value = Benchmark.get_metric_value(metrics, req_metric)
        measurement = req_metric[0]
        if isinstance(value, float):
            print("%-65s: %.6f" % (measurement, value), file=sys.stderr)
        else:
            print("%-65s: %10d" % (measurement, value), file=sys.stderr)

    def dump_read_metrics(self, metrics):
        """
        Print metrics related to the Read API.

        :param metrics: Generated metrics dump
        """
        # Each entry must have a tuple containing
        # (measurement, exp_tag_kv_pairs, metric_type)
        user_tag = [f"User={self.args.user}"]
        if self.args.render:
            # Render query
            req_metrics = [
                ("RedisCache.redis_client.get_calls", user_tag, "timer"),
                ("RedisCache.redis_client.lz4_extraction", user_tag, "timer"),
                ("RedisCache.redis_client.pickle_extraction", user_tag,
                 "timer"),
                ("RedisCache.get_calls", user_tag, "timer"),
                ("QueryCache._get_calls", user_tag, "timer"),
                ("RedisCache.redis_client.set_calls", user_tag, "timer"),
                ("RedisCache.redis_client.lz4_compression", user_tag, "timer"),
                ("RedisCache.redis_client.pickle_compression", user_tag,
                 "timer"),
                ("RedisCache.set_calls", user_tag, "timer"),
                ("QueryCache._set_calls", user_tag, "timer"),
                ("ReadApi.RedisCache.cache_entry_size", user_tag, "histogram"),
                ("ReadApi.DruidReader.get_field_last_val_marker_fetch_time",
                 user_tag, "histogram"),
                ("ReadApi.DruidReader."
                 "get_field_last_val_non_marker_fetch_time",
                 user_tag, "histogram"),
                ("ReadApi.DruidReader.get_field_sparse_data_fetch_time",
                 user_tag, "histogram"),
                ("ReadApi.DruidReader.get_field_process_time",
                 user_tag, "histogram"),
                ("ReadApi.DruidReader.get_field_complete_time",
                 user_tag, "histogram"),
                ("ReadApi.DruidReader.last_val_marker_samples_processed",
                 user_tag, "meter"),
                ("ReadApi.DruidReader.last_val_non_marker_samples_processed",
                 user_tag, "meter"),
                ("ReadApi.DruidReader.sparse_data_samples_processed",
                 user_tag, "meter"),
            ]
        else:
            # Find query
            req_metrics = [
                ("ReadApi.GraphiteExplorer.lru_cache.get_calls",
                 user_tag, "timer"),
                ("ReadApi.GraphiteExplorer.lru_cache.set_calls",
                 user_tag, "timer"),
                ("ReadApi.GraphiteExplorer.DruidReader.fetch_time",
                 user_tag, "histogram"),
            ]
        print("\nREAD METRICS", file=sys.stderr)
        print("--------------", file=sys.stderr)
        for req_metric in req_metrics:
            Benchmark.dump_metric(metrics, req_metric)

    def dump_other_metrics(self, resp, metrics: Dict, total_time: float):
        """
        Print other metrics related to time taken & sparseness

        :param resp: Response from GraphiteApi / GraphiteExplorer
        :param metrics: Generated metrics dump
        :param total_time: Total time taken for the query response
        """
        start_epoch = SqlParser.time_str_parser(self.args.from_str)
        end_epoch = SqlParser.time_str_parser(self.args.until)
        if self.args.render:
            num_series = len(resp.response)
        else:
            num_series = len(resp)
        exp_points_per_series = math.ceil(float(end_epoch - start_epoch) /
                                          600)
        total_dps = num_series * exp_points_per_series

        stored_dps = self.get_metric_value(
            metrics,
            ("ReadApi.DruidReader.sparse_data_samples_processed",
             [f"User={self.args.user}"], "meter"))
        if total_dps > 0:
            drop_rate = float(total_dps - stored_dps) * 100.0 / total_dps
        else:
            drop_rate = 0

        print("\nOTHER METRICS", file=sys.stderr)
        print("---------------", file=sys.stderr)
        print("%-65s: %.6f" % ("Total Time", total_time), file=sys.stderr)
        print("%-65s: %d" % ("Number of series", num_series), file=sys.stderr)
        print("%-65s: %.6f" % ("Approx Drop Rate (+- 2%)", drop_rate),
              file=sys.stderr)

    def _run(self):
        pr = cProfile.Profile()
        if self.args.enable_profiling:
            pr.enable()

        start_epoch = SqlParser.time_str_parser(self.args.from_str)
        end_epoch = SqlParser.time_str_parser(self.args.until)

        start_time = time.perf_counter()
        if self.args.render:
            graphite_api = GraphiteApi(
                self.schema, ImmutableMultiDict({
                    'target': self.args.render,
                    'from': start_epoch,
                    'until': end_epoch,
                    'maxDataPoints': self.args.maxDataPoints}),
                min_interval=600, user=self.args.user)
            resp = graphite_api.execute_graphite()
        else:
            graphite_explorer = GraphiteExplorer(
                DruidReader.inst, Settings.inst.sparse_store.sparse_algos)
            resp = graphite_explorer.browse_dot_schema(self.schema,
                                                       self.args.find,
                                                       start_epoch, end_epoch,
                                                       user=self.args.user)

        # Print time range - useful for re-runs while debugging
        print("\nQUERY INFO", file=sys.stderr)
        print("--------------", file=sys.stderr)
        print("%-11s: %s" % ("Target", (self.args.render if self.args.render
                                        else self.args.find)), file=sys.stderr)
        print("%-11s: --from=%d --until=%d" % ("Time Range", start_epoch,
                                               end_epoch), file=sys.stderr)

        if self.args.print_response:
            # print the response (truncate to configured length)
            rsp = json.dumps(resp.response, skipkeys=True)
            sz = len(rsp)
            print(f"\nRESPONSE (size={sz})", file=sys.stderr)
            print("--------------", file=sys.stderr)
            print("%s%s\n" % (rsp[:self.args.truncate_response],
                              "" if sz <= self.args.truncate_response else
                              ("\n<!---%d bytes truncated---!>" %
                               (sz - self.args.truncate_response))),
                  file=sys.stderr)

        end_time = time.perf_counter()
        if self.args.enable_profiling:
            pr.disable()
        metrics = Telemetry.inst.registry.dump_metrics()

        # Print read metrics
        self.dump_read_metrics(metrics)

        # Print total time & sparseness metrics
        self.dump_other_metrics(resp, metrics, end_time - start_time)
        if self.args.enable_profiling:
            pr.print_stats(sort="tottime")

        print(file=sys.stderr)

    def run(self):
        """
        Run benchmark
        """
        self.setup_router_port_forwarding()
        self.setup_prereqs()

        targets = get_targets(self.args.render, self.args.find,
                              self.args.targets_file)
        for i in range(self.args.iterations):
            target_i = 0
            for target in targets:
                target_i += 1
                print(f'\n======> [Iteration={i + 1}/{self.args.iterations} '
                      f'Target={target_i}/{len(targets)}] '
                      f'{target[0].upper()}: {target[1]}\n',
                      file=sys.stderr)
                if target[0] == 'render':
                    self.args.render = target[1]
                elif target[0] == 'find':
                    self.args.find = target[1]
                self._run()
                time.sleep(1)  # throttle to avoid over-load production brokers


if __name__ == '__main__':
    Benchmark(parse_args()).run()
