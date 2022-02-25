"""Script to test AnomalyDB code locally"""

import atexit
import logging
import os
import pdb
import platform
import subprocess
import socket
import tempfile
import traceback
from pathlib import Path
from shutil import copytree, ignore_patterns, rmtree

import jinja2
import netifaces
import requests

from retrying import retry


from nbdb.common.context import Context
from nbdb.common.graphite import MetricServer
from nbdb.common.thread_pools import ThreadPools
from nbdb.config.settings import Settings
from nbdb.schema.schema import Schema

LOG_FORMAT = '%(asctime)s %(levelname)s ' + \
    '<%(process)d.%(threadName)s> [%(name)s] %(funcName)s:%(lineno)d %(' \
    'message)s'
logging.basicConfig(format=LOG_FORMAT, level=logging.INFO)
log = logging.getLogger(__name__)

# Try to disable requests errors because we expect the initial requests to fail
# when services are booting up
# pylint: disable-msg=E1101  # No member
# For some weird reason, linter thinks the following statement will fail
requests.packages.urllib3.disable_warnings()

SRC_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..')


class AnomalyDbTestLib:
    """Library to make it easier to run AnomalyDB testcases."""

    def __init__(self):
        # Load the appropriate platform-specific test settings
        system = platform.system()
        log.info("Detected platform: %s", system)
        if system == 'Linux':
            Settings.load_yaml_settings(
                os.path.join(os.path.dirname(__file__),
                             '..', 'config', 'settings_linux.yaml'))
        elif system == 'Darwin':
            # Docker does not support host_mode for containers on OSX. We need
            # to use different compose files & settings to work around this.
            Settings.load_yaml_settings(
                os.path.join(os.path.dirname(__file__),
                             '..', 'config', 'settings_darwin.yaml'))

            # Need to figure out en0 interface IP and plug it in for the router
            # and overlord URL
            en0_ip = AnomalyDbTestLib.get_ifaddress('en0')
            Settings.inst.Druid.connection_string.router_ip = en0_ip
            Settings.inst.Druid.connection_string.overlord_ip = en0_ip
        else:
            assert False, "Unsupported platform: %s" % platform

        schema = Schema.load_from_file(
            os.path.join(
                os.path.dirname(__file__), '..', 'config', 'schema.yaml'))
        self.context = Context(schema=schema)
        if not ThreadPools.inst:
            ThreadPools.instantiate()

        atexit.register(self.cleanup)

        # Interface for querying Graphite
        self.ms = None
        # Compose file location. Can be used by tests to restart certain
        # containers
        self.compose_file = None
        # Credentials needed for talking to Flask app
        self.http_auth = ("test", "test")

    @staticmethod
    def get_ifaddress(intf_name):
        """
        Determine the IP address of an interface.
        :param intf_name: Interface name. Eg. eth0
        :return:
        """
        output = netifaces.ifaddresses(intf_name)
        assert output, "No interface named %s found" % intf_name
        return output[netifaces.AF_INET][0]['addr']

    @staticmethod
    def stage_files():
        """Stage the alert container files."""
        staging_dir = os.path.join(tempfile.mkdtemp(), 'anomalydb')

        # Copy all AnomalyDB files
        copytree(SRC_PATH, staging_dir,
                 ignore=ignore_patterns('*.pyc', '*.sh', 'test', 'dist'))

        return staging_dir

    @staticmethod
    def docker_build_anomalydb():
        """Build a local Docker image for AnomalyDB"""
        # Stage all source files in a temporary directory
        staging_dir = AnomalyDbTestLib.stage_files()

        # Save the current working dir
        cwd = os.getcwd()
        os.chdir(staging_dir)

        # Run the build command
        build_cmd = ['sudo', 'docker', 'build', '-t',
                     'anomalydb:local', '-f', 'deploy/Dockerfile', '.']
        try:
            subprocess.check_call(build_cmd, stderr=subprocess.STDOUT)
        finally:
            # Revert back to old working dir and remove the staged files
            os.chdir(cwd)
            rmtree(staging_dir)

    @staticmethod
    def docker_build_druid():
        """Build a local Docker image for Druid"""
        druid_dockerfile = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), 'DruidDockerfile')
        build_cmd = ['sudo', 'docker', 'build', '-t',
                     'druid:local', '-f', druid_dockerfile, '.']
        subprocess.check_call(build_cmd, stderr=subprocess.STDOUT)

    @staticmethod
    def render_jinja_file(input_file, output_file, render_kwargs=None):
        """Render j2 file with the required variables."""
        render_kwargs = render_kwargs or {}
        path, filename = os.path.split(input_file)
        env = jinja2.Environment(loader=jinja2.FileSystemLoader(path))

        text = env.get_template(filename).render(env=os.environ,
                                                 **render_kwargs)
        with open(output_file, 'w') as f:
            f.write(text)

    def launch_service(self):
        """Launch all services necessary for AnomalyDB using docker-compose."""
        # Generate the platform-specific Docker compose file with necessary
        # variables
        system = platform.system()
        if system == 'Linux':
            compose_file_j2 = os.path.join(os.path.dirname(__file__),
                                           "docker-compose-linux.yml.j2")
        elif system == 'Darwin':
            # Docker does not support host_mode for containers on OSX. We need
            # to use different compose files & settings to work around this.
            compose_file_j2 = os.path.join(os.path.dirname(__file__),
                                           "docker-compose-darwin.yml.j2")
        else:
            assert False, "Unsupported platform: %s" % platform

        # We need to find the home directory to pass on AWS credentials
        home_dir = str(Path.home())
        # aws_credentials_file = os.path.join(home_dir, ".aws/credentials")
        # assert os.path.exists(aws_credentials_file), \
        #     "Could not find AWS credentials in ~/.aws/credentials"
        dev_home = os.environ.get('DEVBOX_HOST_USER_HOME')

        render_kwargs = {"home_dir": home_dir if dev_home is None else dev_home}

        self.compose_file, _, _ = compose_file_j2.partition(".j2")
        AnomalyDbTestLib.render_jinja_file(compose_file_j2, self.compose_file,
                                           render_kwargs=render_kwargs)

        launch_cmd = ['sudo', '-E', 'docker-compose', '-p', 'anomalydb', '-f',
                      self.compose_file, "up", "-d"]
        subprocess.check_output(launch_cmd)
        log.info("Launched all services")

    @staticmethod
    def check_conn_listening(ip_addr, port):
        """Check if connections are being accepted on <ip_addr:port>"""
        sock = socket.socket()
        sock.settimeout(5)
        sock.connect((ip_addr, port))
        sock.close()

    # Takes some time for the ES container to be up
    @staticmethod
    @retry(stop_max_attempt_number=180, wait_fixed=1000)
    def wait_for_es_up():
        """Check if ES container is up and accepting connections."""
        try:
            AnomalyDbTestLib.check_conn_listening('127.0.0.1', 9200)
            log.info("ES container is up")
        except Exception as e:
            log.exception("ES container is not up. Retrying")
            raise e

    # Takes some time for the Kafka container to be up
    @staticmethod
    @retry(stop_max_attempt_number=180, wait_fixed=1000)
    def wait_for_kafka_up():
        """Check if Kafka container is up and accepting connections."""
        try:
            AnomalyDbTestLib.check_conn_listening('127.0.0.1', 9092)
            log.info("Kafka container is up")
        except Exception as e:
            log.exception("Kafka container is not up. Retrying")
            raise e

    # Takes some time for the ScyllaDB container to be up
    @staticmethod
    @retry(stop_max_attempt_number=180, wait_fixed=1000)
    def wait_for_scylladb_up():
        """Check if ScyllaDB container is up and accepting connections."""
        try:
            AnomalyDbTestLib.check_conn_listening('127.0.0.1', 9042)
            log.info("ScyllaDB container is up")
        except Exception as e:
            log.exception("ScyllaDB container is not up. Retrying")
            raise e

    # Takes some time for the Flask container to be up
    @staticmethod
    @retry(stop_max_attempt_number=30, wait_fixed=1000)
    def wait_for_flask_up():
        """Check if Flask container is up and accepting requests."""
        try:
            req = requests.get("http://127.0.0.1:5000/health")
            assert req.ok
            log.info("Flask container is up")
        except Exception as e:
            log.info("Flask container is not up %s. Retrying", str(e))
            raise e

    # Takes some time for the Graphite container to be up
    @staticmethod
    @retry(stop_max_attempt_number=180, wait_fixed=1000)
    def wait_for_graphite_up():
        """Check if Graphite container is up and accepting queries."""
        try:
            req = requests.get("http://127.0.0.1:4080/")
            assert req.ok
            log.info("Graphite container is up")
        except Exception as e:
            log.exception("Graphite container is not up. Retrying")
            raise e

    def setup_graphite_telemetry(self):
        """Create Graphite interface to make telemetry querying easier."""
        self.ms = MetricServer("localhost", port=4080, use_ssl=False,
                               url_prefix="/")

    def setup(self):
        """Setup necessary stuff for running testcases."""
        # Build local Docker image
        AnomalyDbTestLib.docker_build_anomalydb()
        # TODO: docker is running natively
        #AnomalyDbTestLib.docker_build_druid()

        # Launch all services needed by AnomalyDB
        self.launch_service()

        # Wait for critical services to be up
        # TODO: Make this configurable based on druid or not
        # AnomalyDbTestLib.wait_for_es_up()
        AnomalyDbTestLib.wait_for_flask_up()
        # TODO: Restore this
        # AnomalyDbTestLib.wait_for_graphite_up()

        # Setup Graphite interface to make it easier to query telemetry
        # TODO Restore this
        # self.setup_graphite_telemetry()

    def run(self, do_setup=True):
        """Run testcases"""
        if do_setup:
            self.setup()

        # Run testcases
        self.run_testcases()

        # If we reached here, all testcases passed
        log.info("\n\n\033[32m PASS \033[0m: All testcases passed\n")

    def run_testcases(self):
        """Override this method and add your testcases here."""
        raise NotImplementedError

    def cleanup(self):
        """Shutdown all services before ending test."""
        #_, _, _ = self, exc_type, exc_type, traceback
        _ = self

        system = platform.system()
        compose_file = None
        if system == 'Linux':
            compose_file = os.path.join(os.path.dirname(__file__),
                                        "docker-compose-linux.yml")
        elif system == 'Darwin':
            compose_file = os.path.join(os.path.dirname(__file__),
                                        "docker-compose-darwin.yml")
        # Shutdown services launched via docker-compose
        subprocess.check_output(["sudo", "-E", "docker-compose", "-p", "anomalydb",
                                 "-f", compose_file, "down"])

        # Delete all containers. Without this, we end up with stale DAGs, jobs
        # state & stats.
        subprocess.check_output(["sudo", "-E", "docker-compose", "-p", "anomalydb",
                                 "-f", compose_file, "rm", "-f"])

        # Shutdown thread pool
        ThreadPools.inst.stop()


def run_tests(test_class, do_setup=True):
    """Run test classes derived from AnomalyDbTestLib with error handling."""
    try:
        test_class.run(do_setup)
    except Exception as e:
        log.error("\033[31m FAIL \033[0m: Saw error while running %s: %s",
                  test_class.__class__.__name__, e)
        log.error(traceback.format_exc())
        # Add breakpoint so that user can debug for now
        pdb.set_trace()
        raise e
