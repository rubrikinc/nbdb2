"""AnomalyDB deploy script."""

import argparse
import logging
import os
import pathlib
import subprocess
import sys
import tempfile
import time
from shutil import copy, copytree, ignore_patterns, rmtree

import boto3
import jinja2
import yaml
from retrying import retry

from ecs_cli_lib import launch_service_on_ecs, scale_service_on_ecs, \
    validate_ecs_cli_version
from ecs_cli_lib import push_docker_image_to_ecr_repo  # noqa
from nbdb.common import deployment_safety


log = logging.getLogger(__name__)

as_client = boto3.client("autoscaling")
ec2_client = boto3.client("ec2")

"""
Make changes in the deployment details below as needed
Searching for "EDIT" might be helpful
"""

# EDIT - Required ECS CLI version
ECS_CLI_VERSION = "1.20.0"
# EDIT - Required Terraform version
TERRAFORM_VERSION = "0.11.14"

# EDIT - ECR repo hosting AnomalyDB docker images
ECR_REPO = '0123456789.your.ecr.com'

# EDIT - Default number of instances that are created
NUM_INSTANCES = 1

# Base path
SRC_PATH = os.path.dirname(os.path.abspath(__file__))


#################
# Druid Configs
#################

# EDIT - below with your druid-prod deployment details
DRUID_REGION = 'us-east-1'


############
# Read API
############

# EDIT - below with your prod deployment details
READ_API_CLUSTER = 'anomalydb-druid-query'
READ_API_PROFILE = 'default'
READ_API_REGION = 'us-east-1'
READ_API_SVC_NAME = 'anomalydb-read-api'
READ_API_TG = ("arn:aws:<id>:"
               "targetgroup/anomalydb-prod-flask/<hash>")
READ_API_NBDB_SECRET_BUCKET = 'your.anomalydb-bucket.com'
READ_API_NBDB_SECRET_OBJECT = 'private/your/secrets/anomalydb/creds.txt'

# Compose file to use
READ_API_COMPOSE_FILE = os.path.join(
    SRC_PATH, 'compose', 'read_api.yml')
READ_API_COMPOSE_TEMPLATE = os.path.join(
    SRC_PATH, 'compose', 'read_api.yml.j2')


##################
# Metric Consumer
##################

# EDIT - below with your prod deployment details
METRIC_CONSUMER_CLUSTER = 'anomalydb-metric-consumers'
METRIC_CONSUMER_PROFILE = 'default'
METRIC_CONSUMER_REGION = 'us-east-1'

INFLUX_METRIC_CONSUMER_SVC_NAME = 'influx-metric-consumer'
MASTER_GRAPHITE_CONSUMER_SVC_NAME = 'master-graphite-consumer'
PROD_GRAPHITE_CONSUMER_SVC_NAME = 'prod-graphite-consumer'
PROD_NO_AGG_GRAPHITE_CONSUMER_SVC_NAME = 'prod_no_agg-graphite-consumer'
V4_LIVE_INFLUX_CONSUMER_SVC_NAME = 'v4_live-influx-consumer'
INTERNAL_GRAPHITE_CONSUMER_SVC_NAME = 'internal-graphite-consumer'
PROD_PICKLE_GRAPHITE_CONSUMER_SVC_NAME = 'prod_pickle-graphite-consumer'
V2_LIVE_PICKLE_GRAPHITE_CONSUMER_SVC_NAME = \
    'v2_live_pickle-graphite-consumer'

# Compose file to use
TURBO_CONSUMER_COMPOSE_FILE = os.path.join(
    SRC_PATH, 'compose', 'turbo_consumer.yml')
TURBO_CONSUMER_COMPOSE_TEMPLATE = os.path.join(
    SRC_PATH, 'compose', "turbo_consumer.yml.j2")

PROD_CONSUMER_COMPOSE_FILE = os.path.join(
    SRC_PATH, 'compose', 'prod_consumer.yml')
PROD_CONSUMER_COMPOSE_TEMPLATE = os.path.join(
    SRC_PATH, 'compose', "prod_consumer.yml.j2")

##################
# Sparse Batch Consumer
##################

# EDIT - below with your prod deployment details
SPARSE_BATCH_CONSUMER_CLUSTER = 'anomalydb-metric-consumers'
SPARSE_BATCH_CONSUMER_PROFILE = 'default'
SPARSE_BATCH_CONSUMER_REGION = 'us-east-1'
SPARSE_BATCH_CONSUMER_SVC_NAME = "sparse-batch-consumer"

# Compose file to use
SPARSE_BATCH_CONSUMER_COMPOSE_FILE = os.path.join(SRC_PATH, 'compose',
                                                  'sparse_batch_consumer.yml')
SPARSE_BATCH_CONSUMER_COMPOSE_TEMPLATE = os.path.join(
    SRC_PATH, 'compose', "sparse_batch_consumer.yml.j2")

##################
# Dense Batch Consumer
##################

# EDIT - below with your prod deployment details
DENSE_BATCH_CONSUMER_CLUSTER = 'anomalydb-metric-consumers'
DENSE_BATCH_CONSUMER_PROFILE = 'default'
DENSE_BATCH_CONSUMER_REGION = 'us-east-1'
DENSE_BATCH_CONSUMER_SVC_NAME = "dense-batch-consumer"

# Compose file to use
DENSE_BATCH_CONSUMER_COMPOSE_FILE = os.path.join(SRC_PATH, 'compose',
                                                 'dense_batch_consumer.yml')
DENSE_BATCH_CONSUMER_COMPOSE_TEMPLATE = os.path.join(
    SRC_PATH, 'compose', "dense_batch_consumer.yml.j2")

##################
# Indexing Consumer
##################

# EDIT - below with your prod deployment details
PROD_INDEXING_CONSUMER_CLUSTER = 'anomalydb-metric-consumers'
PROD_INDEXING_CONSUMER_PROFILE = 'default'
PROD_INDEXING_CONSUMER_SVC_NAME = 'indexing-consumer'
PROD_INDEXING_CONSUMER_REGION = 'us-east-1'

# Compose file to use
INDEXING_CONSUMER_COMPOSE_FILE = os.path.join(SRC_PATH, 'compose',
                                              'indexing_consumer.yml')
INDEXING_CONSUMER_COMPOSE_TEMPLATE = os.path.join(SRC_PATH, 'compose',
                                                  "indexing_consumer.yml.j2")

# ES index cleaner settings. Need to load prod settings first
with open(os.path.join(SRC_PATH, "../nbdb/config/settings_prod.yaml")) as file:
    settings = yaml.safe_load(file)
    DRUID_MODE = settings['sparse_store']['druid_mode']
    if not DRUID_MODE:
        ELASTIC_SEARCH_HOST = settings['elasticsearch']['connection']['host']
        INDEX_PREFIX = settings['elasticsearch']['index']['index_prefix']

# Delete any indices older than 2 days
DELETE_DELTA_DAYS = 2


#######################
# Test Metric Reader
#######################

# EDIT - below with your prod deployment details
TEST_METRIC_READER_CLUSTER = 'anomalydb-metric-consumers'
TEST_METRIC_READER_PROFILE = 'default'
TEST_METRIC_READER_SVC_NAME = 'test-metric-reader'
TEST_METRIC_READER_REGION = 'us-east-1'

# Compose file to use
TEST_METRIC_READER_COMPOSE_FILE = os.path.join(
    SRC_PATH, 'compose', 'test_metric_reader.yml')
TEST_METRIC_READER_COMPOSE_TEMPLATE = os.path.join(
    SRC_PATH, 'compose', "test_metric_reader.yml.j2")


#######################
# Test Metric Producer
#######################

# EDIT - below with your prod deployment details
TEST_METRIC_PRODUCER_CLUSTER = 'anomalydb-metric-consumers'
TEST_METRIC_PRODUCER_PROFILE = 'default'
TEST_METRIC_PRODUCER_SVC_NAME = 'test-metric-producer'
TEST_METRIC_PRODUCER_REGION = 'us-east-1'

# Compose file to use
TEST_METRIC_PRODUCER_COMPOSE_FILE = os.path.join(
    SRC_PATH, 'compose', 'test_metric_producer.yml')
TEST_METRIC_PRODUCER_COMPOSE_TEMPLATE = os.path.join(
    SRC_PATH, 'compose', "test_metric_producer.yml.j2")

#######################
# Druid Deployment
#######################

# Config files to be generated at runtime
COMMON_DRUID_CONF = os.path.join(
    SRC_PATH, "druid_conf", "druid", "_common", "common.runtime.properties")
COMMON_DRUID_CONF_TEMPLATE = os.path.join(
    SRC_PATH, "druid_conf", "druid", "_common", "common.runtime.properties.j2")

DRUID_ZOOKEEPER_CONF = os.path.join(
    SRC_PATH, 'druid_conf/zk', 'zoo.cfg')
DRUID_ZOOKEEPER_CONF_TEMPLATE = os.path.join(
    SRC_PATH, 'druid_conf/zk', "zoo.cfg.j2")

DRUID_MM_CONF = os.path.join(
    SRC_PATH, 'druid_conf/druid/middleManager', 'runtime.properties')
DRUID_MM_CONF_TEMPLATE = os.path.join(
    SRC_PATH, 'druid_conf/druid/middleManager', 'runtime.properties.j2')

DRUID_QUERY_CONF = os.path.join(
    SRC_PATH, 'druid_conf/druid/broker', 'runtime.properties')
DRUID_QUERY_CONF_TEMPLATE = os.path.join(
    SRC_PATH, 'druid_conf/druid/broker', 'runtime.properties.j2')

# Task definition templates
DRUID_ZOOKEEPER_TASK_DEF = os.path.join(
    SRC_PATH, 'compose', "druid_zookeeper.json")
DRUID_ZOOKEEPER_TASK_DEF_TEMPLATE = os.path.join(
    SRC_PATH, 'compose', "druid_zookeeper.json.j2")

DRUID_ZOOKEEPER_MONITOR_TASK_DEF = os.path.join(
    SRC_PATH, 'compose', "druid_zookeeper_monitor.json")
DRUID_ZOOKEEPER_MONITOR_TASK_DEF_TEMPLATE = os.path.join(
    SRC_PATH, 'compose', "druid_zookeeper_monitor.json.j2")

DRUID_COORDINATOR_TASK_DEF = os.path.join(
    SRC_PATH, 'compose', 'druid_coordinator.json')
DRUID_COORDINATOR_TASK_DEF_TEMPLATE = os.path.join(
    SRC_PATH, 'compose', "druid_coordinator.json.j2")

DRUID_OVERLORD_TASK_DEF = os.path.join(
    SRC_PATH, 'compose', 'druid_overlord.json')
DRUID_OVERLORD_TASK_DEF_TEMPLATE = os.path.join(
    SRC_PATH, 'compose', "druid_overlord.json.j2")

DRUID_HISTORICAL_TASK_DEF = os.path.join(
    SRC_PATH, 'compose', 'druid_historical.json')
DRUID_HISTORICAL_TASK_DEF_TEMPLATE = os.path.join(
    SRC_PATH, 'compose', "druid_historical.json.j2")

DRUID_MIDDLEMANAGER_TASK_DEF = os.path.join(
    SRC_PATH, 'compose', 'druid_middlemanager.json')
DRUID_MIDDLEMANAGER_TASK_DEF_TEMPLATE = os.path.join(
    SRC_PATH, 'compose', "druid_middlemanager.json.j2")

DRUID_QUERY_TASK_DEF = os.path.join(
    SRC_PATH, 'compose', 'druid_query.json')
DRUID_QUERY_TASK_DEF_TEMPLATE = os.path.join(
    SRC_PATH, 'compose', "druid_query.json.j2")

# Prod Druid deployment
DRUID_MASTER_AUTOSCALING_GROUP = "anomalydb-druid-master-new"
DRUID_ZOOKEEPER_AUTOSCALING_GROUP = "anomalydb-druid-zookeeper"
DRUID_HISTORICAL_AUTOSCALING_GROUP = "anomalydb-druid-historical-node"
DRUID_MIDDLEMANAGER_AUTOSCALING_GROUP = "anomalydb-druid-middlemanager"
DRUID_QUERY_AUTOSCALING_GROUP = "anomalydb-druid-query-nodes"

# Turbo Druid deployment
TURBO_DRUID_MASTER_AUTOSCALING_GROUP = "turbo-anomalydb-druid-master"
TURBO_DRUID_HISTORICAL_AUTOSCALING_GROUP = "turbo-anomalydb-druid-historical"
TURBO_DRUID_MIDDLEMANAGER_AUTOSCALING_GROUP = \
    "turbo-anomalydb-druid-middlemanager"
TURBO_DRUID_QUERY_AUTOSCALING_GROUP = "turbo-anomalydb-druid-query-node"


def parse_args(args):
    """Parse command line arguments."""
    argparser = argparse.ArgumentParser(description='Deploy anomalydb on aws')

    argparser.add_argument("--ecs_cli_path", type=str, default='ecs-cli',
                         help="Path to ecs-cli binary.")

    target_choices = ['influx_metric_consumer',
                      'master_graphite_consumer',
                      'prod_graphite_consumer',
                      'prod_no_agg_graphite_consumer',
                      'v4_live_influx_consumer',
                      'internal_graphite_consumer',
                      'prod_pickle_graphite_consumer',
                      'v2_live_pickle_graphite_consumer',
                      'sparse_batch_consumer', 'dense_batch_consumer',
                      'test_metric_reader', 'test_metric_producer', 'read_api',
                      'druid_deployment', 'druid_master', 'druid_zookeeper',
                      'druid_zookeeper_monitor',
                      'druid_middlemanager_regular',
                      'druid_middlemanager_crosscluster',
                      'druid_historical', 'druid_query', 'druid_query_cc']
    argparser.add_argument('--target', type=str, choices=target_choices,
                           help='Service to deploy in AWS')

    deploy_modes = ['prod', 'turbo']
    argparser.add_argument('--mode', type=str, choices=deploy_modes,
                           default='prod', help='Deploy mode in AWS',
                           required=False)

    actions = ['up', 'down', 'build']
    argparser.add_argument('--state', type=str, default='up', choices=actions,
                           help="Specify the requested AWS state. "
                                "Default: up")
    argparser.add_argument('--num_instances', type=int, default=NUM_INSTANCES,
                           help='Number of containers to deploy')

    argparser.add_argument('--start_cluster_id', type=str, default='n0',
                           help='start_cluster_id id to use for TMP')

    argparser.add_argument('--setup', type=int, default=0,
                           help='If 1 then TMP recreates schema')

    argparser.add_argument('--master_desired_count', type=int,
                           help='Number of Druid masters to deploy')
    argparser.add_argument('--zookeeper_desired_count', type=int,
                           help='Number of zookeeper nodes')
    argparser.add_argument('--middlemanager_regular_desired_count', type=int,
                           help='Number of Druid regular MMs to deploy')
    argparser.add_argument('--middlemanager_crosscluster_desired_count',
                           type=int,
                           help='Number of Druid crosscluster MMs to deploy')
    argparser.add_argument('--historical_desired_count', type=int,
                           help='Number of Druid historical nodes to deploy')
    argparser.add_argument('--query_desired_count', type=int,
                           help='Number of Druid query nodes to deploy')
    argparser.add_argument('--query_cc_desired_count', type=int,
                help='Number of Druid query nodes for crosscluster to deploy')

    consumer_modes = ['realtime', 'rollup', 'both']
    argparser.add_argument('--consumer_mode', type=str, choices=consumer_modes,
                           default=None, required=False, help='Consumer mode')

    return argparser.parse_args(args)


def validate_args(args):
    """Validate command line arguments."""
    if args.target == 'druid_deployment':
        assert args.master_desired_count is not None, \
            "Druid master desired count must be specified"
        assert args.zookeeper_desired_count is not None, \
            "Druid zookeeper desired count must be specified"
        assert args.middlemanager_regular_desired_count is not None, \
            "Druid regular MM node desired count must be specified"
        assert args.middlemanager_crosscluster_desired_count is not None, \
            "Druid crosscluster MM node desired count must be specified"
        assert args.historical_desired_count is not None, \
            "Druid historical node desired count must be specified"
        assert args.query_desired_count is not None, \
            "Druid query node desired count must be specified"
        assert args.query_cc_desired_count is not None, \
            "Druid query node for crosscluster desired count must be specified"

    if args.target == 'influx_metric_consumer':
        assert args.mode == 'turbo', \
            "Currently we support Influx consumer for only Turbo metrics"

    if args.target in ['influx_metric_consumer',
                       'master_graphite_consumer',
                       'prod_graphite_consumer',
                       'prod_no_agg_graphite_consumer',
                       'v4_live_influx_consumer',
                       'internal_graphite_consumer',
                       'prod_pickle_graphite_consumer',
                       'v2_live_pickle_graphite_consumer']:
        assert args.consumer_mode is not None, \
            "Must specify consumer_mode for metric consumers"


def validate_dependencies(ecs_cli_path: str):
    """Validate external CLI dependencies."""
    try:
        validate_ecs_cli_version(ecs_cli_path)
    except Exception as e:
        log.error("Could not validate ecs-cli. Please provide a "
                  "custom path with --ecs_cli_path and try again."
                  " Exception: \n%s", e)
        exit(1)
    # Sample output:
    #   Terraform v0.11.14\n...
    #
    # For some reason, pylint keeps complaining about the use of the keyword
    # argument text=True in subprocess.check_output(). Silence this error
    # pylint: disable-msg=E1123  # Unexpected Keyword Arg
    terraform_version_str = subprocess.check_output(["terraform", "--version"],
                                                    text=True)
    if f"v{TERRAFORM_VERSION}" not in terraform_version_str:
        raise RuntimeError(f"Need version {TERRAFORM_VERSION} of terraform")


class AnomalyDBDeploy:
    """AnomalyDB deploy class."""

    def __init__(self, args):
        """Init def."""
        self.args = args

    def stage_files(self):
        """Stage the AnomalyDB container files."""
        staging_dir = os.path.join(tempfile.mkdtemp(), 'anomalydb')

        # First copy all AnomalyDB files
        copytree(SRC_PATH, staging_dir,
                 ignore=ignore_patterns('*.pyc', '*.sh', 'test', 'compose',
                                        '*.log.txt', '.terraform'))

        # Copy extra directories for important libraries
        dirs_to_copy = [os.path.join(SRC_PATH, '..', 'nbdb')]
        for src_dir in dirs_to_copy:
            src_path = os.path.join(SRC_PATH, src_dir)
            dst_path = os.path.join(staging_dir, os.path.basename(src_dir))
            copytree(src_path, dst_path,
                     ignore=ignore_patterns('*.pyc', '*.sh', 'test', '*.txt'))

        # Copy necessary extra files
        extra_files = [os.path.join(SRC_PATH, '..', 'setup.py'),
                       os.path.join(SRC_PATH, '..', 'setup_pypy.py'),
                       os.path.join(SRC_PATH, '..', 'requirements.txt')]
        for f in extra_files:
            src_path = os.path.join(SRC_PATH, f)
            copy(src_path, staging_dir)

        return staging_dir

    @staticmethod
    def docker_build_anomalydb(staging_dir, image_tag):
        """Helper to build AnomalyDB Docker image."""
        # Save the current working dir
        cwd = os.getcwd()
        os.chdir(staging_dir)

        build_cmd = ['sudo', 'docker', 'build', '-t',
                     'anomalydb:%s' % image_tag, '-f', 'Dockerfile', '.']
        print(build_cmd)
        try:
            subprocess.check_call(build_cmd, stderr=subprocess.STDOUT)
        finally:
            # Revert back to old working dir and remove the staged files
            os.chdir(cwd)
            rmtree(staging_dir)

    @staticmethod
    def get_zookeeper_ec2_private_ips(deploy_mode):
        """
        Return private IP addresses of the EC2 instances belonging to the ZK
        deployment
        """
        # Determine the EC2 instance IDs
        group_name = AnomalyDBDeploy.get_autoscaling_group_name(
            deploy_mode, "druid_zookeeper")
        output = as_client.describe_auto_scaling_groups(
            AutoScalingGroupNames=[group_name])
        instances = output['AutoScalingGroups'][0]['Instances']
        instance_ids = [i['InstanceId'] for i in instances]

        # Determine the private IP addresses of the instance IDs
        output = ec2_client.describe_instances(InstanceIds=instance_ids)
        private_ips = []
        for reservation in output['Reservations']:
            for instance in reservation['Instances']:
                private_ips += [instance['PrivateIpAddress']]

        assert len(private_ips) == 3, \
            "Unexpected number of ZK IPs for %s" % deploy_mode
        # Return a sorted list of private IPs so that we get the same
        # deterministic output. A deterministic output will ensure the ZK IDs
        # in the ensemble don't change.
        return private_ips

    @staticmethod
    def docker_build_druid(image_tag, deploy_mode, target):
        """Helper to build Druid Docker image."""
        # Render common Druid configuration based on deployment being used
        zookeeper_ips = AnomalyDBDeploy.get_zookeeper_ec2_private_ips(
            deploy_mode)

        params = {}
        if deploy_mode == 'prod':
            params['druid_deployment_name'] = 'prod'
            params['druid_s3_bucket'] = 'anomalydb-prod-druid'
            params['druid_zookeeper_ips'] = zookeeper_ips
            params['druid_mysql_host'] = (
                "anomalydb-druid-prod-metadata-store."
                "test.mysql.com")
        else:
            params['druid_deployment_name'] = 'turbo_druid'
            params['druid_s3_bucket'] = 'anomalydb-turbo-druid'
            params['druid_zookeeper_ips'] = zookeeper_ips
            params['druid_mysql_host'] = (
                "turbo-anomalydb-druid-metadata."
                "test.mysql.com")

        AnomalyDBDeploy.render_jinja_file(COMMON_DRUID_CONF_TEMPLATE,
                                          COMMON_DRUID_CONF, params)

        # Render Druid zookeeper config containing the IPs
        params = {}
        params['druid_zookeeper_1'] = zookeeper_ips[0]
        params['druid_zookeeper_2'] = zookeeper_ips[1]
        params['druid_zookeeper_3'] = zookeeper_ips[2]
        AnomalyDBDeploy.render_jinja_file(DRUID_ZOOKEEPER_CONF_TEMPLATE,
                                          DRUID_ZOOKEEPER_CONF, params)

        # Render Druid MM config based on which MM category (regular /
        # crosscluster) is being requested
        params = {}
        if target == 'druid_middlemanager_regular':
            # For regular MMs, we use 1 GB max heap + 700 MB direct memory per
            # slot. Thus we end up using 1.7 GB per slot & reserve 24 slots
            params["mm_category"] = "regular"
            params["slots_capacity"] = 24
            params["heap_max_size"] = "1024m"
            params["max_direct_mem"] = "700m"
            params["processing_buffer_size_bytes"] = 100000000
            params["num_server_threads"] = 40
            params["processing_threads"] = 2
            # Disable caching for regular MM nodes
            params["use_cache"] = "false"
            params["populate_cache"] = "false"
            params["cache_size_in_bytes"] = 0
        elif target == 'druid_middlemanager_crosscluster':
            # For crosscluster MMs, we use 10 GB max heap + 3.5 GB direct memory
            # per slot. We need a bigger heap because crosscluster datasources
            # receive far more metrics than any regular cluster-sharded
            # datasource, and we are also enabling a 1 GB cache. Thus we end up
            # using 13.5 GB per slot & reserve 2 slots.
            params["mm_category"] = "crosscluster"
            params["slots_capacity"] = 2
            params["heap_max_size"] = "9216m"
            params["max_direct_mem"] = "3500m"
            # Cross-cluster groupBy queries require a bigger processing buffer
            # size
            params["processing_buffer_size_bytes"] = 500000000
            # Increase server threads & processing threads because
            # cross-cluster MM nodes will see a much higher query load
            params["num_server_threads"] = 60
            params["processing_threads"] = 4
            # Enable caching for cross-cluster MM nodes
            params["use_cache"] = "true"
            params["populate_cache"] = "true"
            # Use 1 GB cache
            params["cache_size_in_bytes"] = 1000000000
        else:
            # Non middlemanager target
            pass

        if params:
            AnomalyDBDeploy.render_jinja_file(DRUID_MM_CONF_TEMPLATE,
                                              DRUID_MM_CONF, params)

        # Render Druid query config based on which category (regular /
        # crosscluster) is being requested
        params = {}
        if target == 'druid_query':
            params["query_category"] = "regular"
            params["num_threads"] = 60
            params["num_connections"] = 10
            params["max_queued_bytes"] = 50000000
            params["processing_buffer_size_bytes"] = 536870912
            params["processing_num_merge_buffers"] = 8
            params["processing_num_threads"] = 2
            params["use_cache"] = "false"
            params["populate_cache"] = "false"
            params["sql_enable"] = "true"
        elif target == 'druid_query_cc':
            params["query_category"] = "crosscluster"
            params["num_threads"] = 60
            params["num_connections"] = 10
            params["max_queued_bytes"] = 50000000
            params["processing_buffer_size_bytes"] = 536870912
            params["processing_num_merge_buffers"] = 8
            params["processing_num_threads"] = 2
            params["use_cache"] = "false"
            params["populate_cache"] = "false"
            params["sql_enable"] = "true"
        else:
            # Non query target
            pass

        if params:
            AnomalyDBDeploy.render_jinja_file(DRUID_QUERY_CONF_TEMPLATE,
                                              DRUID_QUERY_CONF, params)

        build_cmd = ['sudo', 'docker', 'build', '-t',
                     'druid:%s' % image_tag, '-f', 'DruidDockerfile', '.']
        print(build_cmd)
        subprocess.check_call(build_cmd, stderr=subprocess.STDOUT)

    @staticmethod
    def render_jinja_file(input_file, output_file, render_kwargs=None,
                          undefined=None):
        """Helper to render Jinja templatized vars in a file."""
        render_kwargs = render_kwargs or {}
        path, filename = os.path.split(input_file)
        if undefined:
            # The caller expects to render undefined variables a certain way.
            # Fulfill their expectations
            env = jinja2.Environment(loader=jinja2.FileSystemLoader(path),
                                     undefined=undefined)
        else:
            env = jinja2.Environment(loader=jinja2.FileSystemLoader(path))

        text = env.get_template(filename).render(env=os.environ,
                                                 **render_kwargs)
        with open(output_file, 'w') as f:
            f.write(text)

    @staticmethod
    def build_lamba_zipfile():
        """Create a zipfile for the ES index cleaner Lambda."""
        # Create temporary directory
        staging_dir = tempfile.mkdtemp()

        # Copy cleaner script
        copy(os.path.join(SRC_PATH, "anomalydb_es_index_cleaner.py"),
             staging_dir)

        # Install pip dependencies
        for pip_pkg in ("aws-requests-auth==0.4.2", "elasticsearch==7.1.0"):
            subprocess.check_call(["python3.7", "-m", "pip", "install",
                                   pip_pkg, "-t", staging_dir])

        # Create zipfile & copy over to current directory
        cwd = os.getcwd()
        os.chdir(staging_dir)
        subprocess.check_call(["zip", "-r", "function.zip", "."])
        copy(os.path.join(staging_dir, "function.zip"), SRC_PATH)

        # Revert back to old working directory
        os.chdir(cwd)
        rmtree(staging_dir)

    @staticmethod
    def deploy_es_index_cleaner(state):
        """Deploy a Lambda for deleting old ES indices."""
        # Need to run init to pull the latest info
        subprocess.check_call(['terraform', 'init', SRC_PATH])

        if state in ["up", "restart"]:
            # Need to build zipfile if creating a Lambda
            AnomalyDBDeploy.build_lamba_zipfile()
            cmd = ["terraform", "apply", "-auto-approve"]
        else:
            cmd = ["terraform", "destroy", "-auto-approve"]

        cmd += [
            "-var", "elastic_search_host=%s" % ELASTIC_SEARCH_HOST,
            "-var", "index_prefix=%s" % INDEX_PREFIX,
            "-var", "delete_delta_days=%s" % DELETE_DELTA_DAYS,
            SRC_PATH
        ]
        subprocess.check_call(cmd)

    @staticmethod
    def docker_push(repo_name, image_tag):
        """Push common Docker image to ECR repo."""
        push_docker_image_to_ecr_repo(repo_name, image_tag, ECR_REPO)

    # Have a timeout for 180 secs
    @staticmethod
    @retry(stop_max_attempt_number=18, wait_fixed=10000)
    def wait_autoscaling_num_instances(group_name, desired_instances):
        """
        Wait till requested number of EC2 instances have been instantiated.
        """
        output = as_client.describe_auto_scaling_groups(
            AutoScalingGroupNames=[group_name])
        running_instances = len(output['AutoScalingGroups'][0]['Instances'])
        try:
            assert running_instances == desired_instances
        except AssertionError as exc:
            log.warning("Autoscaling %s instances: Desired %s, Running: %s",
                        group_name, desired_instances, running_instances)
            raise exc
        else:
            log.info("Autoscaling %s instances: Desired %s, Running: %s",
                     group_name, desired_instances, running_instances)

    @staticmethod
    def get_autoscaling_group_name(deploy_mode, target):
        """
        Return the autoscaling group name corresponding to deployment & target
        """
        group_name = None
        if deploy_mode == 'prod':
            if target == 'druid_master':
                group_name = DRUID_MASTER_AUTOSCALING_GROUP
            elif target == 'druid_zookeeper':
                group_name = DRUID_ZOOKEEPER_AUTOSCALING_GROUP
            elif target == 'druid_middlemanager':
                group_name = DRUID_MIDDLEMANAGER_AUTOSCALING_GROUP
            elif target == 'druid_historical':
                group_name = DRUID_HISTORICAL_AUTOSCALING_GROUP
            else:
                group_name = DRUID_QUERY_AUTOSCALING_GROUP
        else:
            if target == 'druid_master':
                group_name = TURBO_DRUID_MASTER_AUTOSCALING_GROUP
            elif target == 'druid_middlemanager':
                group_name = TURBO_DRUID_MIDDLEMANAGER_AUTOSCALING_GROUP
            elif target == 'druid_historical':
                group_name = TURBO_DRUID_HISTORICAL_AUTOSCALING_GROUP
            else:
                group_name = TURBO_DRUID_QUERY_AUTOSCALING_GROUP
        return group_name

    @staticmethod
    def set_autoscaling_desired_capacity(target, deploy_mode, capacity):
        """
        Set desired autoscaling group capacity.

        Sets the desired capacity for the given autoscaling group & waits till
        requested number of EC2 instances have been instantiated.
        """
        if capacity <= 0:
            # We don't reduce autoscaling group capacity to avoid EC2 instance
            # churning, in case user is just restarting the deployment
            return

        group_name = AnomalyDBDeploy.get_autoscaling_group_name(deploy_mode,
                                                                target)
        as_client.set_desired_capacity(AutoScalingGroupName=group_name,
                                       DesiredCapacity=capacity)

        AnomalyDBDeploy.wait_autoscaling_num_instances(
            group_name, desired_instances=capacity)

    def configure_service(self, num_instances, image_tag):
        """Create requested number of instances for the service."""
        if self.args.target == "druid_deployment":
            # Druid deployment requires containers with network_mode=host.
            # ecs-cli v0.6.0 does not support that, so using Terraform
            self.configure_service_terraform(image_tag)
        else:
            self.configure_service_ecs_cli(num_instances, image_tag)

    def configure_service_terraform(self, image_tag):
        """Create requested number of instances using ecs-cli"""
        # We configure the Druid processes such that only a single container
        # runs in each VM. We may need to adjust the autoscaling group size
        AnomalyDBDeploy.set_autoscaling_desired_capacity(
            "druid_master", self.args.mode,
            self.args.master_desired_count)
        AnomalyDBDeploy.set_autoscaling_desired_capacity(
            "druid_zookeeper", self.args.mode,
            self.args.zookeeper_desired_count)
        # We use the same ASG for regular MMs and crosscluster MMs
        AnomalyDBDeploy.set_autoscaling_desired_capacity(
            "druid_middlemanager", self.args.mode,
            self.args.middlemanager_regular_desired_count +
            self.args.middlemanager_crosscluster_desired_count)
        AnomalyDBDeploy.set_autoscaling_desired_capacity(
            "druid_historical", self.args.mode,
            self.args.historical_desired_count)
        AnomalyDBDeploy.set_autoscaling_desired_capacity(
            "druid_query", self.args.mode,
            self.args.query_desired_count +
            self.args.query_cc_desired_count)

        # Generate task definition JSON files
        self.generate_druid_master_task_definition(image_tag)
        self.generate_druid_zookeeper_task_definition(image_tag)
        self.generate_druid_middlemanager_task_definition(image_tag)
        self.generate_druid_historical_task_definition(image_tag)
        self.generate_druid_query_task_definition(image_tag)

        cmd = ["terraform", "apply", "-auto-approve"]
        cmd += [
            "-var", "master_desired_count=%s" %
            self.args.master_desired_count,
            "-var", "zookeeper_desired_count=%s" %
            self.args.zookeeper_desired_count,
            "-var", "middlemanager_regular_desired_count=%s" %
            self.args.middlemanager_regular_desired_count,
            "-var", "middlemanager_crosscluster_desired_count=%s" %
            self.args.middlemanager_crosscluster_desired_count,
            "-var", "historical_desired_count=%s" %
            self.args.historical_desired_count,
            "-var", "query_desired_count=%s" %
            self.args.query_desired_count,
            "-var", "query_cc_desired_count=%s" %
            self.args.query_cc_desired_count,
        ]
        if self.args.mode == 'prod':
            cmd += ['-target', 'module.prod_druid_deployment']
        else:
            cmd += ['-target', 'module.turbo_druid_deployment']
        subprocess.check_call(cmd)

    def configure_service_ecs_cli(self, num_instances, image_tag):
        """Create requested number of instances using ecs-cli"""
        if num_instances == 0:
            service_state = "down"
        else:
            service_state = "up"

        if self.args.target == 'indexing_consumer':
            # We also need to deploy a Lambda for cleaning up older AnomalyDB
            # ES indices
            AnomalyDBDeploy.deploy_es_index_cleaner(service_state)

        # Render the compose file with any Jinja variables that are required
        compose_file = self.generate_compose_or_task_def_file(image_tag)

        # Get service deployment details
        (svc_name, region, cluster, profile, elb_arn, container_name,
         container_port, service_role) = \
            self.get_service_deploy_details()

        # Launch services using the ecs cli wrapper
        launch_service_on_ecs(
            ecs_cli_path=self.args.ecs_cli_path,
            compose_file=compose_file,
            project_name=svc_name,
            service_state=service_state,
            region=region,
            cluster=cluster,
            profile=profile,
            elb_arn=elb_arn,
            container_name=container_name,
            container_port=container_port,
            service_role=service_role)

        if num_instances > 1:
            # ecs-cli service up only brings up 1 instance. Also need to call
            # scale command
            scale_service_on_ecs(
                ecs_cli_path=self.args.ecs_cli_path,
                compose_file=compose_file,
                project_name=svc_name,
                num_instances=num_instances,
                region=region,
                cluster=cluster,
                profile=profile)

    @staticmethod
    def get_git_hash():
        """Generate hash of last git commit."""
        return subprocess.getoutput("git rev-parse HEAD")[:-1]

    def get_image_tag(self):
        """Generate Docker image tag based on last git commit ID."""
        return '%s-%s-%s' % (self.args.mode, AnomalyDBDeploy.get_git_hash(),
                             int(time.time()))

    def get_service_deploy_details(self):
        """Generate deployment details for requested target

        When adding any new component to be deployed, add your
        AWS deployment details by creating a new `get_foo_deploy_details()`
        method and add the function call here.
        """
        if self.args.target in ['influx_metric_consumer',
                                'master_graphite_consumer',
                                'prod_graphite_consumer',
                                'prod_no_agg_graphite_consumer',
                                'v4_live_influx_consumer',
                                'internal_graphite_consumer',
                                'prod_pickle_graphite_consumer',
                                'v2_live_pickle_graphite_consumer']:
            return self.get_metric_consumer_deploy_details()
        if self.args.target == "sparse_batch_consumer":
            return self.get_sparse_batch_consumer_deploy_details()
        if self.args.target == "dense_batch_consumer":
            return self.get_dense_batch_consumer_deploy_details()
        if self.args.target == 'indexing_consumer':
            return self.get_indexing_consumer_deploy_details()
        if self.args.target == 'test_metric_reader':
            return self.get_test_metric_reader_deploy_details()
        if self.args.target == 'test_metric_producer':
            return self.get_test_metric_producer_deploy_details()
        return self.get_read_api_deploy_details()

    def get_read_api_deploy_details(self):
        """Return AWS deployment details for the Flask app."""
        # Global variables independent of deployment
        container_name = "read_api"
        container_port = 5000
        service_role = 'ecsServiceRole'

        # Set variables based on deploy mode
        if self.args.mode == "prod":
            cluster = READ_API_CLUSTER
            profile = READ_API_PROFILE
            svc_name = READ_API_SVC_NAME
            elb_arn = READ_API_TG
            region = READ_API_REGION
        else:
            assert False, "Sandbox not supported"

        return (svc_name, region, cluster, profile, elb_arn,
                container_name, container_port, service_role)

    def get_metric_consumer_deploy_details(self):
        """Return AWS deployment details for the metric consumer."""
        # Global variables independent of deployment
        container_name = self.args.target
        container_port = None
        service_role = 'ecsServiceRole'
        elb_arn = None

        cluster = METRIC_CONSUMER_CLUSTER
        profile = METRIC_CONSUMER_PROFILE
        region = METRIC_CONSUMER_REGION
        if self.args.target == 'influx_metric_consumer':
            svc_name = INFLUX_METRIC_CONSUMER_SVC_NAME
        elif self.args.target == 'master_graphite_consumer':
            svc_name = MASTER_GRAPHITE_CONSUMER_SVC_NAME
        elif self.args.target == 'prod_graphite_consumer':
            svc_name = PROD_GRAPHITE_CONSUMER_SVC_NAME
        elif self.args.target == 'prod_no_agg_graphite_consumer':
            svc_name = PROD_NO_AGG_GRAPHITE_CONSUMER_SVC_NAME
        elif self.args.target == 'internal_graphite_consumer':
            svc_name = INTERNAL_GRAPHITE_CONSUMER_SVC_NAME        
        elif self.args.target == 'prod_pickle_graphite_consumer':
            svc_name = PROD_PICKLE_GRAPHITE_CONSUMER_SVC_NAME
        elif self.args.target == 'v2_live_pickle_graphite_consumer':
            svc_name = V2_LIVE_PICKLE_GRAPHITE_CONSUMER_SVC_NAME
        else:
            svc_name = V4_LIVE_INFLUX_CONSUMER_SVC_NAME

        svc_name = "%s-%s" % (svc_name, self.args.consumer_mode)
        return (svc_name, region, cluster, profile, elb_arn,
                container_name, container_port, service_role)

    def get_sparse_batch_consumer_deploy_details(self):
        """Return AWS deployment details for the sparse batch consumer."""
        # Global variables independent of deployment
        container_name = self.args.target
        container_port = None
        service_role = 'ecsServiceRole'
        elb_arn = None

        cluster = SPARSE_BATCH_CONSUMER_CLUSTER
        profile = SPARSE_BATCH_CONSUMER_PROFILE
        region = SPARSE_BATCH_CONSUMER_REGION
        svc_name = SPARSE_BATCH_CONSUMER_SVC_NAME

        return (svc_name, region, cluster, profile, elb_arn,
                container_name, container_port, service_role)

    def get_dense_batch_consumer_deploy_details(self):
        """Return AWS deployment details for the sparse batch consumer."""
        # Global variables independent of deployment
        container_name = self.args.target
        container_port = None
        service_role = 'ecsServiceRole'
        elb_arn = None

        cluster = DENSE_BATCH_CONSUMER_CLUSTER
        profile = DENSE_BATCH_CONSUMER_PROFILE
        region = DENSE_BATCH_CONSUMER_REGION
        svc_name = DENSE_BATCH_CONSUMER_SVC_NAME

        return (svc_name, region, cluster, profile, elb_arn,
                container_name, container_port, service_role)

    def get_indexing_consumer_deploy_details(self):
        """Return AWS deployment details for the indexing consumer."""
        _ = self
        container_name = "indexing_consumer"
        container_port = None
        service_role = 'ecsServiceRole'
        elb_arn = None

        cluster = PROD_INDEXING_CONSUMER_CLUSTER
        profile = PROD_INDEXING_CONSUMER_PROFILE
        svc_name = PROD_INDEXING_CONSUMER_SVC_NAME
        region = PROD_INDEXING_CONSUMER_REGION

        return (svc_name, region, cluster, profile, elb_arn,
                container_name, container_port, service_role)

    @staticmethod
    def get_test_metric_reader_deploy_details():
        """Return AWS deployment details for the test metric reader."""
        cluster = TEST_METRIC_READER_CLUSTER
        profile = TEST_METRIC_READER_PROFILE
        svc_name = TEST_METRIC_READER_SVC_NAME
        region = TEST_METRIC_READER_REGION

        container_name = "test_metric_reader"
        container_port = None
        service_role = 'ecsServiceRole'
        elb_arn = None

        return (svc_name, region, cluster, profile, elb_arn,
                container_name, container_port, service_role)

    def get_test_metric_producer_deploy_details(self):
        """Return AWS deployment details for the test metric producer."""
        cluster = TEST_METRIC_PRODUCER_CLUSTER
        profile = TEST_METRIC_PRODUCER_PROFILE
        svc_name = TEST_METRIC_PRODUCER_SVC_NAME
        region = TEST_METRIC_PRODUCER_REGION

        svc_name = '{}-{}'.format(svc_name, self.args.start_cluster_id)
        container_name = "test_metric_producer"
        container_port = None
        service_role = 'ecsServiceRole'
        elb_arn = None

        return (svc_name, region, cluster, profile, elb_arn,
                container_name, container_port, service_role)

    def generate_compose_or_task_def_file(self, image_tag):
        """Generate compose file based on target requested.

        When adding any new component to be deployed, define your YAML / JSON
        generating function and call it here.
        """
        # pylint: disable-msg=R0911  # Too many return statements
        if self.args.target in ['master_graphite_consumer',
                                'prod_graphite_consumer',
                                'prod_no_agg_graphite_consumer',
                                'v4_live_influx_consumer',
                                'internal_graphite_consumer',
                                'prod_pickle_graphite_consumer',
                                'v2_live_pickle_graphite_consumer']:
            return self.generate_metric_consumer_compose_file(image_tag)
        if self.args.target == "sparse_batch_consumer":
            return self.generate_sparse_batch_consumer_compose_file(image_tag)
        if self.args.target == "dense_batch_consumer":
            return self.generate_dense_batch_consumer_compose_file(image_tag)
        if self.args.target == 'indexing_consumer':
            return self.generate_indexing_consumer_compose_file(image_tag)
        if self.args.target == 'test_metric_reader':
            return self.generate_test_metric_reader_compose_file(image_tag)
        if self.args.target == 'test_metric_producer':
            return self.generate_test_metric_producer_compose_file(image_tag)
        if self.args.target == 'read_api':
            return self.generate_read_api_compose_file(image_tag)
        if self.args.target == 'druid_deployment':
            # We need to generate all Druid task definitions
            self.generate_druid_master_task_definition(image_tag)
            self.generate_druid_zookeeper_task_definition(image_tag)
            self.generate_druid_middlemanager_task_definition(image_tag)
            self.generate_druid_historical_task_definition(image_tag)
            # Arbitrarily return the Druid query task def file. We don't need
            # to actually return anything since this should only be called for
            # --state=build
            return self.generate_druid_query_task_definition(image_tag)
        if self.args.target == 'druid_master':
            return self.generate_druid_master_task_definition(image_tag)
        if self.args.target == 'druid_zookeeper':
            return self.generate_druid_zookeeper_task_definition(image_tag)
        if self.args.target == 'druid_zookeeper_monitor':
            return self.generate_druid_zookeeper_monitor_task_definition(image_tag)
        if self.args.target in ['druid_middlemanager_regular',
                                'druid_middlemanager_crosscluster']:
            return self.generate_druid_middlemanager_task_definition(image_tag)
        if self.args.target == 'druid_historical':
            return self.generate_druid_historical_task_definition(image_tag)
        return self.generate_druid_query_task_definition(image_tag)

    def generate_metric_consumer_compose_file(self, image_tag):
        """Generate compose file for metric consumer."""
        compose_params = {
            'image_repo': os.path.join(ECR_REPO, 'anomalydb'),
            'image_tag': image_tag,
            'mode': self.args.mode,
            'region': METRIC_CONSUMER_REGION,
        }
        if self.args.target == 'influx_metric_consumer':
            compose_template = TURBO_CONSUMER_COMPOSE_TEMPLATE
            compose_file = TURBO_CONSUMER_COMPOSE_FILE
            config_filename = 'settings_turbo.yaml'
            schema_filename = 'schema_turbo.yaml'
        else:
            compose_template = PROD_CONSUMER_COMPOSE_TEMPLATE
            compose_file = PROD_CONSUMER_COMPOSE_FILE
            schema_filename = 'schema_prod.yaml'
            if self.args.target == 'master_graphite_consumer':
                config_filename = 'master_graphite_consumer.yaml'
            elif self.args.target == 'prod_graphite_consumer':
                config_filename = 'prod_graphite_consumer.yaml'
            elif self.args.target == 'prod_no_agg_graphite_consumer':
                config_filename = 'prod_no_agg_graphite_consumer.yaml'
            elif self.args.target == 'v4_live_influx_consumer':
                config_filename = 'v4_live_influx_consumer.yaml'
            elif self.args.target == 'internal_graphite_consumer':
                config_filename = 'internal_graphite_consumer.yaml'
            elif self.args.target == 'prod_pickle_graphite_consumer':
                config_filename = 'prod_pickle_graphite_consumer.yaml'
            elif self.args.target == 'v2_live_pickle_graphite_consumer':
                config_filename = 'v2_live_pickle_graphite_consumer.yaml'

        if self.args.target == 'prod_no_agg_graphite_consumer':
            # prod_no_agg gets the maximum number of series (120M out of a
            # total 130M as of July 2021). We end up needing to run more
            # consumers than the remaining to keep up with the speed. The
            # memory usage also changes depending on how many containers are
            # running.
            #
            # These memory reservation & limit numbers have been chosen based
            # on the assumption that 50 realtime & 50 rollup containers are
            # running
            if self.args.consumer_mode == 'realtime':
                mem_reservation = int(3.5 * 1024**3)  # 3.5 GB
                mem_limit = int(3.75 * 1024**3)       # 3.5 GB
            elif self.args.consumer_mode == 'rollup':
                mem_reservation = int(5.5 * 1024**3)  # 5.5 GB
                mem_limit = int(5.75 * 1024**3)       # 5.5 GB
            else:
                # Dual consumer
                mem_reservation = int(7 * 1024**3)    # 7.0 GB
                mem_limit = int(7.5 * 1024**3)        # 7.5 GB
        else:
            # Default numbers for all other deployments
            if self.args.consumer_mode == 'realtime':
                # Reserving 3.5 GB of memory per realtime container. Realtime
                # consumer uses about 45% memory as a dual consumer which needs
                # 7 GB
                mem_reservation = int(3.5 * 1024**3)  # 3.5 GB
                mem_limit = int(3.75 * 1024**3)       # 3.75 GB
            elif self.args.consumer_mode == 'rollup':
                # Reserving 5.5 GB of memory per rollup container. Rollup
                # consumer uses about 85% memory as a dual container
                mem_reservation = int(6.3 * 1024**3)  # 6.3 GB
                mem_limit = int(6.5 * 1024**3)        # 6.75 GB
            else:
                # Dual consumer
                mem_reservation = int(7 * 1024**3)    # 7.0 GB
                mem_limit = int(7.5 * 1024**3)        # 7.5 GB

        compose_params['config_file'] = config_filename
        compose_params['schema_file'] = schema_filename
        compose_params['mem_reservation'] = mem_reservation
        compose_params['mem_limit'] = mem_limit
        compose_params["consumer_mode"] = self.args.consumer_mode

        # Render compose file after plugging in variables
        AnomalyDBDeploy.render_jinja_file(compose_template, compose_file,
                                          compose_params)
        return compose_file

    def generate_sparse_batch_consumer_compose_file(self, image_tag):
        """Generate compose file for batch consumer."""
        compose_params = {
            'image_repo': os.path.join(ECR_REPO, 'anomalydb'),
            'image_tag': image_tag,
            'mode': self.args.mode,
            'region': SPARSE_BATCH_CONSUMER_REGION,
            "schema_file": "schema_batch_prod.yaml",
            "config_file": "settings_batch_consumer.yaml"
        }

        # Render compose file after plugging in variables
        AnomalyDBDeploy.render_jinja_file(SPARSE_BATCH_CONSUMER_COMPOSE_TEMPLATE,
                                          SPARSE_BATCH_CONSUMER_COMPOSE_FILE,
                                          compose_params)
        return SPARSE_BATCH_CONSUMER_COMPOSE_FILE

    def generate_dense_batch_consumer_compose_file(self, image_tag):
        """Generate compose file for batch consumer."""
        compose_params = {
            'image_repo': os.path.join(ECR_REPO, 'anomalydb'),
            'image_tag': image_tag,
            'mode': self.args.mode,
            'region': DENSE_BATCH_CONSUMER_REGION,
            "schema_file": "schema_batch_prod.yaml",
            "config_file": "settings_batch_consumer.yaml",
            "filter_file": "batch_metrics_filter_prod.yaml"
        }

        # Render compose file after plugging in variables
        AnomalyDBDeploy.render_jinja_file(DENSE_BATCH_CONSUMER_COMPOSE_TEMPLATE,
                                          DENSE_BATCH_CONSUMER_COMPOSE_FILE,
                                          compose_params)
        return DENSE_BATCH_CONSUMER_COMPOSE_FILE

    def generate_indexing_consumer_compose_file(self, image_tag):
        """Generate compose file for indexing consumer."""
        compose_params = {
            'image_repo': os.path.join(ECR_REPO, 'anomalydb'),
            'image_tag': image_tag,
            'mode': self.args.mode,
            'region': PROD_INDEXING_CONSUMER_REGION,
        }

        # Render compose file after plugging in variables
        AnomalyDBDeploy.render_jinja_file(INDEXING_CONSUMER_COMPOSE_TEMPLATE,
                                          INDEXING_CONSUMER_COMPOSE_FILE,
                                          compose_params)
        return INDEXING_CONSUMER_COMPOSE_FILE

    def generate_test_metric_reader_compose_file(self, image_tag):
        """Generate compose file for test metric reader."""
        compose_params = {
            'image_repo': os.path.join(ECR_REPO, 'anomalydb'),
            'image_tag': image_tag,
            'mode': self.args.mode,
            'region': TEST_METRIC_READER_REGION,
        }

        # Render compose file after plugging in variables
        AnomalyDBDeploy.render_jinja_file(TEST_METRIC_READER_COMPOSE_TEMPLATE,
                                          TEST_METRIC_READER_COMPOSE_FILE,
                                          compose_params)
        return TEST_METRIC_READER_COMPOSE_FILE

    def generate_test_metric_producer_compose_file(self, image_tag):
        """Generate compose file for test metric producer."""
        compose_params = {
            'image_repo': os.path.join(ECR_REPO, 'anomalydb'),
            'image_tag': image_tag,
            'mode': self.args.mode,
            'region': TEST_METRIC_PRODUCER_REGION,
            'start_cluster_id': self.args.start_cluster_id,
            'setup': self.args.setup
        }

        # Render compose file after plugging in variables
        AnomalyDBDeploy.render_jinja_file(TEST_METRIC_PRODUCER_COMPOSE_TEMPLATE,
                                          TEST_METRIC_PRODUCER_COMPOSE_FILE,
                                          compose_params)
        return TEST_METRIC_PRODUCER_COMPOSE_FILE

    def generate_read_api_compose_file(self, image_tag):
        """Generate the task def JSON for the Read API."""
        compose_params = {
            'anomalydb_image_repo': os.path.join(ECR_REPO, 'anomalydb'),
            'druid_image_repo': os.path.join(ECR_REPO, 'druid'),
            'image_tag': image_tag,
            'mode': self.args.mode,
            'region': READ_API_REGION,
            'anomalydb_secret_bucket': READ_API_NBDB_SECRET_BUCKET,
            'anomalydb_secret_object': READ_API_NBDB_SECRET_OBJECT
        }
        if self.args.mode == 'prod':
            compose_params['config_file'] = 'settings_prod.yaml'
            compose_params['schema_file'] = 'schema_prod.yaml'
            compose_params['batch_schema_file'] = 'schema_batch_prod.yaml'
        else:
            assert False, "Unsupported mode: %s" % self.args.mode

        # Render compose file after plugging in variables
        AnomalyDBDeploy.render_jinja_file(READ_API_COMPOSE_TEMPLATE,
                                          READ_API_COMPOSE_FILE,
                                          compose_params)
        return READ_API_COMPOSE_FILE

    def generate_druid_master_task_definition(self, image_tag):
        """Generate the task def JSON for the Druid master."""
        task_def_params = {
            'druid_image_repo': os.path.join(ECR_REPO, 'druid'),
            'image_tag': image_tag,
            'mode': self.args.mode,
            'region': DRUID_REGION,
        }
        # Render coordinator & overlord JSON files after plugging in variables
        AnomalyDBDeploy.render_jinja_file(DRUID_COORDINATOR_TASK_DEF_TEMPLATE,
                                          DRUID_COORDINATOR_TASK_DEF,
                                          task_def_params)

        AnomalyDBDeploy.render_jinja_file(DRUID_OVERLORD_TASK_DEF_TEMPLATE,
                                          DRUID_OVERLORD_TASK_DEF,
                                          task_def_params)

    def generate_druid_zookeeper_task_definition(self, image_tag):
        """Generate the task def JSON for the Druid zookeeper."""
        zookeeper_ips = AnomalyDBDeploy.get_zookeeper_ec2_private_ips(
            self.args.mode)

        task_def_params = {
            'druid_image_repo': os.path.join(ECR_REPO, 'druid'),
            'image_tag': image_tag,
            'mode': self.args.mode,
            'region': DRUID_REGION,
            'zookeeper_ips': ','.join(zookeeper_ips),
        }

        AnomalyDBDeploy.render_jinja_file(DRUID_ZOOKEEPER_TASK_DEF_TEMPLATE,
                                          DRUID_ZOOKEEPER_TASK_DEF,
                                          task_def_params)

    def generate_druid_zookeeper_monitor_task_definition(self, image_tag):
        """Generate the task def JSON for the Druid zookeeper."""
        zookeeper_ips = AnomalyDBDeploy.get_zookeeper_ec2_private_ips(
            self.args.mode)

        task_def_params = {
            'anomalydb_image_repo': os.path.join(ECR_REPO, 'anomalydb'),
            'image_tag': image_tag,
            'mode': self.args.mode,
            'region': DRUID_REGION,
            'zookeeper_ips': ','.join(zookeeper_ips),
        }

        AnomalyDBDeploy.render_jinja_file(DRUID_ZOOKEEPER_MONITOR_TASK_DEF_TEMPLATE,
                                          DRUID_ZOOKEEPER_MONITOR_TASK_DEF,
                                          task_def_params)

    def generate_druid_historical_task_definition(self, image_tag):
        """Generate the task def JSON for the Druid historical nodes."""
        task_def_params = {
            'druid_image_repo': os.path.join(ECR_REPO, 'druid'),
            'image_tag': image_tag,
            'mode': self.args.mode,
            'region': DRUID_REGION,
        }

        # Render JSON file after plugging in variables
        AnomalyDBDeploy.render_jinja_file(DRUID_HISTORICAL_TASK_DEF_TEMPLATE,
                                          DRUID_HISTORICAL_TASK_DEF,
                                          task_def_params)

    def generate_druid_middlemanager_task_definition(self, image_tag):
        """Generate the task def JSON for the Druid Middlemanager."""
        task_def_params = {
            'druid_image_repo': os.path.join(ECR_REPO, 'druid'),
            'image_tag': image_tag,
            'mode': self.args.mode,
            'region': DRUID_REGION,
        }

        # Render JSON file after plugging in variables
        AnomalyDBDeploy.render_jinja_file(DRUID_MIDDLEMANAGER_TASK_DEF_TEMPLATE,
                                          DRUID_MIDDLEMANAGER_TASK_DEF,
                                          task_def_params)

    def generate_druid_query_task_definition(self, image_tag):
        """Generate the task def JSON for Druid query node."""
        task_def_params = {
            'anomalydb_image_repo': os.path.join(ECR_REPO, 'anomalydb'),
            'druid_image_repo': os.path.join(ECR_REPO, 'druid'),
            'image_tag': image_tag,
            'mode': self.args.mode,
            'region': DRUID_REGION,
        }
        if self.args.mode == 'prod':
            task_def_params['config_file'] = 'settings_prod.yaml'
            task_def_params['schema_file'] = 'schema_prod.yaml'
            task_def_params['batch_schema_file'] = 'schema_batch_prod.yaml'
        else:
            task_def_params['config_file'] = 'settings_turbo.yaml'
            task_def_params['schema_file'] = 'schema_turbo.yaml'
            task_def_params['batch_schema_file'] = 'schema_turbo.yaml'

        # Render JSON file after plugging in variables
        AnomalyDBDeploy.render_jinja_file(DRUID_QUERY_TASK_DEF_TEMPLATE,
                                          DRUID_QUERY_TASK_DEF,
                                          task_def_params)

    def run(self):
        image_tag = self.get_image_tag()

        # No need to build docker image if req state is down
        if self.args.state != 'down':
            # Build Druid Docker image & push to ECR
            AnomalyDBDeploy.docker_build_druid(image_tag, self.args.mode,
                                               self.args.target)
            AnomalyDBDeploy.docker_push("druid", image_tag)

            # Build AnomalyDB Docker image & push to ECR
            AnomalyDBDeploy.docker_build_anomalydb(self.stage_files(), image_tag)
            AnomalyDBDeploy.docker_push("anomalydb", image_tag)

        if self.args.state == 'build':
            # We only had to build images. Return after generating the compose
            # / task def JSON files
            self.generate_compose_or_task_def_file(image_tag)
            return

        if self.args.state == "up":
            num_instances = self.args.num_instances
        else:
            num_instances = 0

        # Launch services on AWS
        self.configure_service(num_instances, image_tag)


def _get_systest_pass_hash():
    systest_pass_file = '../.deployment_safety_systest_passed'
    if not os.path.exists(systest_pass_file):
        return 'NONE'
    with open(systest_pass_file, 'r', encoding='utf-8') as f:
        return str(f.read()).strip()


if __name__ == '__main__':
    # Perform safety checks before deployment
    deployment_safety.assert_cwd_eq_prog_parent_path(
        str(pathlib.Path(__file__).parent.resolve()))
    deployment_safety.assert_git_repo_state(_get_systest_pass_hash())

    # Parse & validate command line arguments
    input_args = parse_args(sys.argv[1:])
    validate_args(input_args)
    validate_dependencies(input_args.ecs_cli_path)

    # Run deployment code
    AnomalyDBDeploy(input_args).run()
