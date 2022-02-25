"""
Entry point for the AnomalyDB API web-service
"""

import json
import logging
import os
import time
import traceback
from typing import List, Dict

from absl import app
import boto3
from flask import Flask, request, jsonify, Response
from flask_compress import Compress
from flask_httpauth import HTTPBasicAuth
from flask_injector import FlaskInjector
from injector import inject
import orjson
from nbdb.api.database_schema_factory import DatabaseSchemaFactory
from nbdb.api.explorer import Explorer
from nbdb.readapi.druid_reader import DruidReader
from nbdb.common.context import Context
from nbdb.common.entry_point import EntryPoint
from nbdb.common.telemetry import Telemetry
from nbdb.common.telemetry import user_meter_calls_with_flask_auth
from nbdb.common.telemetry import user_time_calls_with_flask_auth
from nbdb.config.settings import Settings
from nbdb.readapi.anomaly_api import AnomalyApi
from nbdb.readapi.graphite_api import GraphiteApi
from nbdb.readapi.graphite_explorer import GraphiteExplorer
from nbdb.readapi.graphite_response import NumpyEncoder
from nbdb.readapi.query_cache import QueryCache
from nbdb.readapi.sql_api import SqlApi
from nbdb.readapi.sql_parser import SqlParser
from nbdb.schema.schema import Schema
from werkzeug.security import generate_password_hash, check_password_hash
from pyformance import meter_calls, time_calls

import tracemalloc

logger = logging.getLogger()
flask = Flask(__name__)
# Enable gzip compression by default on all APIs. This can help improve
# performance for multi-panel dashboards on very large clusters
flask.config["COMPRESS_ALGORITHM"] = "gzip"
Compress(flask)
flask_auth = HTTPBasicAuth()

GRAPHITE_DATABASE_HEADER_KEY = "AnomalyDB-Graphite-Database"
GRAPHITE_DEFAULT_DATABASE = "default"

# Constants related to AnomalyDB HTTP authentication
ANOMALYDB_SECRETS_BUCKET = os.environ.get("ANOMALYDB_SECRETS_BUCKET")
ANOMALYDB_SECRETS_OBJECT = os.environ.get("ANOMALYDB_SECRETS_OBJECT")
# Dictionary storing hashed password indexed by username. Entries are added
# later in ApiApp
users = {}


LOCAL_SIGNAL_FILE = '/app/signal.dat'
LOCAL_LOG_FILE = '/app/log.dat'

def is_signal_on():
    return os.path.exists(LOCAL_SIGNAL_FILE)

def generate_profile_stats(snapshot):
    top_stats = snapshot.statistics('lineno')
    stats_content = ''
    for stat in top_stats[:10]:
        stats_content += str(stat) + '|'
        logger.info('memory_profile_stats: %s', stats_content)
    with open(LOCAL_LOG_FILE, 'a') as f:
        f.write(stats_content + '\n')

@flask_auth.verify_password
def verify_password(username, password) -> str:
    """Check if credentials provided are valid"""
    if (username in users and
            check_password_hash(users.get(username), password)):
        return username

    # Invalid credentials
    return None


class InvalidUsage(Exception):
    """
    Invalid Usage exception, always raise this exception when an api is called
    with an incorrect usage.
    """
    status_code = 400

    @meter_calls
    def __init__(self, message, status_code=None, payload=None):
        """
        Create the invalid usage object
        :param message: Text message describing the incorrect usage,
        ideally should contain help for correct usage
        :param status_code: HTTP status code to use
        :param payload: Dictionary for details
        """
        Exception.__init__(self)
        self.message = message
        if status_code is not None:
            self.status_code = status_code
        self.payload = payload

    def to_dict(self) -> Dict:
        """
        Converts the exception attributes into a dictionary
        :return:
        """
        rv = dict(self.payload or ())
        rv['message'] = self.message
        return rv


class InvalidFindUsage(InvalidUsage):
    """
    Invalid Usage exception but for find queries.
    Need a child class so that we can create a different errorhandler and
    update different metrics
    """



@flask.errorhandler(InvalidUsage)
@meter_calls
def handle_invalid_usage(error):
    """
    Error handler for the invalid usage exception
    :param error:
    :return:
    """
    response = jsonify(error.to_dict())
    response.status_code = error.status_code
    return response


@flask.errorhandler(InvalidFindUsage)
@meter_calls
def handle_invalid_find_usage(error):
    """
    Error handler for the invalid find usage exception
    :param error:
    :return:
    """
    response = jsonify(error.to_dict())
    response.status_code = error.status_code
    return response


@flask.route("/health", methods=('get',))
@time_calls
@meter_calls
def health():
    """AWS target group calls this to detect unhealthy instances."""
    return Response(status=200)


def parse_csv_params(csv_params: str) -> dict:
    """
    Parses parameters encoded as comma separated key=value pairs
    :param csv_params:
    :return: dictionary
    """
    params = dict()
    arg_parts = csv_params.strip().split(',')
    for arg in arg_parts:
        key_value = arg.split('=')
        if len(key_value) == 2:
            params[key_value[0]] = key_value[1]
        else:
            logger.error("Ignoring malformed param: %s it must be of form "
                         "key=value", arg)
    return params


def _parse_graphite_database(headers,
                            schema_factory: DatabaseSchemaFactory) -> str:
    """
    Parse database name from custom header

    :param headers: HTTP headers in request
    """
    database = headers.get(
        key=GRAPHITE_DATABASE_HEADER_KEY,
        default=GRAPHITE_DEFAULT_DATABASE)
    if not schema_factory.validate_database(database):
        raise ValueError(f"Unknown database {database}")
    return database

@flask.route("/tags/autoComplete/tags", methods=('get',))
def tags():
    """
    TODO: stats.rubrik.com makes this call after find api
    Unclear what this is for
    :return:
    """
    # return an empty response for now
    return Response(response='',
                    status=200,
                    mimetype="application/json")


@inject
@flask.route("/metrics/find", methods=('get', 'post'))
@flask_auth.login_required
@user_time_calls_with_flask_auth(flask_auth)
@user_meter_calls_with_flask_auth(flask_auth)
def find(schema_factory: DatabaseSchemaFactory):
    """
    Graphite API find
    :return:
    """
    find_start_time = time.time()
    try:
        _from = request.args.get('from')
        _until = request.args.get('until')
        database = _parse_graphite_database(request.headers, schema_factory)
        # Pick the right schema based on the database param
        schema = schema_factory.get(database)
        logger.debug("Graphite database is %s", database)
        if request.method == 'POST':
            _query = request.form['query']
        else:
            _query = request.args.get('query')
        logger.info('home.find: from=%s until=%s query=%s',
                    _from, _until, _query)
        start_epoch = None if _from is None \
            else SqlParser.time_str_parser(_from)
        end_epoch = None if _until is None \
            else SqlParser.time_str_parser(_until)
        field_prefix = _query
        response = GraphiteExplorer.inst.browse_dot_schema(
            schema, field_prefix, start_epoch, end_epoch,
            flask_auth.current_user())
        return Response(response=orjson.dumps(response),
                        status=200,
                        mimetype="application/json")
    except Exception as e:
        logger.error('Failed to execute: find=%s Exception=%s StackTrace=%s',
                     json.dumps(request.args), str(e), traceback.format_exc())
        raise InvalidFindUsage('Failed to execute query',
                               status_code=400,
                               payload={'query': json.dumps(request.args),
                                        'Exception': str(e),
                                        'StackTrace': traceback.format_exc()})
    finally:
        time_taken = time.time() - find_start_time
        if time_taken >= 200:
            logger.info(
                "Time taken by find query: %s is %d seconds by user: %s",
                 _query, time_taken, flask_auth.current_user())


@inject
@flask.route("/render", methods=('post', 'get'))
@flask_auth.login_required
@user_time_calls_with_flask_auth(flask_auth)
@user_meter_calls_with_flask_auth(flask_auth)
def render(schema_factory: DatabaseSchemaFactory):
    """
    Graphite DataSource Render API
    :return:
    """
    if not tracemalloc.is_tracing():
        if is_signal_on():
            tracemalloc.start()
    else:
        if not is_signal_on():
            tracemalloc.stop()

    render_start_time = time.time()
    try:
        if request.method == 'POST':
            _query = request.form
        else:
            _query = request.args
        database = _parse_graphite_database(request.headers, schema_factory)
        logger.info("Graphite database is %s", database)
        # Pick the right schema based on the database param
        schema = schema_factory.get(database)
        graphite = GraphiteApi(
            schema, _query, schema.MIN_STORAGE_INTERVAL,
            flask_auth.current_user())
        graphite_response = graphite.execute_graphite()
        return Response(response=orjson.dumps(graphite_response.response,
                                              option=orjson.OPT_SERIALIZE_NUMPY),
                        status=200,
                        mimetype="application/json")
    except Exception as e:
        logger.error('Failed to execute: render=%s Exception=%s StackTrace=%s',
                     json.dumps(_query), str(e),
                     traceback.format_exc())
        raise InvalidUsage('Failed to execute query',
                           status_code=400,
                           payload={'query': json.dumps(_query),
                                    'Exception': str(e),
                                    'StackTrace': traceback.format_exc()
                                    }
                           )
    finally:
        time_taken = time.time() - render_start_time
        if time_taken >= 300:
            logger.info(
                "Time taken by render query: %s is %d seconds by user: %s",
                 json.dumps(_query), time_taken, flask_auth.current_user())

    if tracemalloc.is_tracing():
        generate_profile_stats(tracemalloc.take_snapshot())

@inject
@flask.route("/query", methods=('get',))
@flask_auth.login_required
@user_time_calls_with_flask_auth(flask_auth)
@user_meter_calls_with_flask_auth(flask_auth)
def query(context: Context):
    """
    Get API for running the SQL queries and returning results as a json
    Required parameters: query = SQL statement
    :return: JSON result, for now just the parsed result
    """
    query = request.args.get('q')
    logger.info('query=%s', query)
    query_parts = query.split('||')
    query = query_parts[0]
    trace = False
    nocache = False
    repopulate_cache = False
    if len(query_parts) > 1:
        additional_params = parse_csv_params(query_parts[1])
        logger.info('additional_params: %s', json.dumps(additional_params))
        trace = 'trace' in additional_params and \
                additional_params['trace'] == 'true'
        nocache = 'nocache' in additional_params and \
                additional_params['nocache'] == 'true'
        repopulate_cache = 'repopulate_cache' in additional_params and \
                additional_params['repopulate_cache'] == 'true'
        if trace:
            logger.info('warning: verbose tracing enabled,'
                        ' this will generate a lot of logs')

    if query.startswith('SHOW'):
        return Explorer.inst.query(context.schema, query)

    # ignored by us for now, we have only one database at this point
    _ = request.args.get('db')
    # we only support epoch precision of seconds, the response is however
    # generated at the desired precision
    epoch_precision = request.args.get('epoch')

    if not query:
        raise InvalidUsage('query is a required parameter', status_code=400)
    try:
        sql_api = SqlApi(context.schema, query, flask_auth.current_user(),
                         trace=trace)
    except Exception as e:
        logger.error('Failed to parse: query=%s Exception=%s StackTrace=%s',
                     query, str(e), traceback.format_exc())
        raise InvalidUsage(' query',
                           status_code=400,
                           payload={'query': query,
                                    'Exception': str(e),
                                    'StackTrace': traceback.format_exc()
                                    }
                           )
    try:
        influx_series_response = sql_api.execute_influx(epoch_precision,
                                                        nocache,
                                                        repopulate_cache)
        json_result = json.dumps(influx_series_response.response,
                                 indent=2,
                                 sort_keys=True)
        resp = Response(response=json_result,
                        status=200,
                        mimetype="application/json")

        Telemetry.inst.registry.meter(
            'ReadApi.query_calls.response_size').mark(len(json_result))
        return resp
    except Exception as e:
        logger.error('Failed to execute: query=%s Exception=%s StackTrace=%s',
                     query, str(e), traceback.format_exc())
        raise InvalidUsage('Failed to execute query',
                           status_code=400,
                           payload={'query': query,
                                    'Exception': str(e),
                                    'StackTrace': traceback.format_exc()
                                    }
                           )


@inject
@flask.route("/anomaly", methods=('post',))
@flask_auth.login_required
@user_time_calls_with_flask_auth(flask_auth)
@user_meter_calls_with_flask_auth(flask_auth)
def anomaly(schema_factory: DatabaseSchemaFactory):
    """
    Anomaly API

    Does the following:
    1. Fetch data by calling the graphite/sql functions
    2. Compute baseline
    3. Flag anomalies and return response

    TODO: Currently only the graphite format is supported since we
    TODO: do not have proper influx tagged data yet.
    :return:
    """
    try:
        form = request.form
        database = _parse_graphite_database(request.headers, schema_factory)
        logger.debug("Graphite database is %s", database)
        # Pick the right schema based on the database param
        schema = schema_factory.get(database)
        graphite = GraphiteApi(
            schema, form, schema.MIN_STORAGE_INTERVAL,
            flask_auth.current_user())
        graphite_response = graphite.execute_graphite()
        anomalyapi = AnomalyApi(form)
        anomaly_response = anomalyapi.execute_graphite(graphite_response)
        return Response(response=json.dumps(anomaly_response.__dict__),
                        status=200,
                        mimetype="application/json")
    except Exception as e:
        logger.error('Failed to execute: anomaly=%s Exception=%s StackTrace=%s',
                     json.dumps(form), str(e), traceback.format_exc())
        raise InvalidUsage('Failed to execute query',
                           status_code=400,
                           payload={'query': json.dumps(form),
                                    'Exception': str(e),
                                    'StackTrace': traceback.format_exc()
                                    }
                           )


class ApiApp(EntryPoint):
    """
    Entry point class for the Flask API Application
    """

    def __init__(self):
        EntryPoint.__init__(self)
        self.database_schema_factory = None

    @staticmethod
    def load_user_credentials():
        """
        Download user credentials from S3
        """
        s3_client = boto3.client("s3")
        file_path = "/tmp/creds.txt"
        assert ANOMALYDB_SECRETS_BUCKET and ANOMALYDB_SECRETS_OBJECT, \
            "Secrets env variables not set"
        s3_client.download_file(ANOMALYDB_SECRETS_BUCKET,
                                ANOMALYDB_SECRETS_OBJECT, file_path)
        with open(file_path) as creds_file:
            for line in creds_file.readlines():
                username, password = line.split()
                users[username] = generate_password_hash(password)

        os.remove(file_path)

    def load_schema(self, schema_mapping: List) -> Schema:
        database_schema_factory = DatabaseSchemaFactory()
        for mapping in schema_mapping:
            database, schema_file = mapping.split(":")
            batch_mode = database == DatabaseSchemaFactory.BATCH_DATABASE
            database_schema_factory.add(
                database,
                Schema.load_from_file(schema_file, batch_mode=batch_mode))

        self.database_schema_factory = database_schema_factory
        # Use default schema for EntryPoint
        return database_schema_factory.get(
            DatabaseSchemaFactory.DEFAULT_DATABASE)

    def start(self):
        """
        Initialize the read path and start the flask web server
        :return:
        """
        assert self.context is not None
        logger.info("Creating the DruidReader")
        DruidReader.instantiate(Settings.inst.Druid.connection_string)
        Explorer.instantiate(DruidReader.inst.druid_schema)
        GraphiteExplorer.instantiate(DruidReader.inst,
                                     Settings.inst.sparse_store.sparse_algos)

        # instantiate the query cache
        if Settings.inst.flask.enable_cache:
            logger.info("Initializing QueryCache")
            QueryCache.initialize()
        else:
            logger.info('QueryCache disabled in settings.'
                        ' See flask.enable_cache')

        ApiApp.load_user_credentials()

        # Inject request scoped dependencies
        def configure(binder):
            binder.bind(
                Context,
                to=self.context,
                scope=request,
            )
            binder.bind(
                DatabaseSchemaFactory,
                to=self.database_schema_factory,
                scope=request
            )

        FlaskInjector(app=flask, modules=[configure])

        logger.info("Starting the Flask server")
        # Ready to start the flask server now
        flask.run(threaded=True,
                  host=Settings.inst.flask.host,
                  port=Settings.inst.flask.port)


if __name__ == "__main__":
    app.run(ApiApp().run)

