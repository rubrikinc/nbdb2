"""
Explorer module
"""
from __future__ import annotations

import json
import logging
from typing import Dict

from flask import Response

from nbdb.common.druid_schema import DruidSchema
from nbdb.schema.schema import Schema

logger = logging.getLogger()


class Explorer:
    """
    Provides ability to browse and explore the metrics
    """

    # Global singleton
    inst: Explorer = None

    def __init__(self,
                 druid_schema: DruidSchema):
        """
        Initializer
        """
        self.druid_schema = druid_schema

    @staticmethod
    def instantiate(druid_schema: DruidSchema):
        """
        Instantiate the global singleton
        :param druid_schema:
        :param measurements: List of authored measurements in schema.yaml
        :return:
        """
        if Explorer.inst is None:
            Explorer.inst = Explorer(druid_schema)

    def query(self, schema: Schema, query: str) -> Response:
        """

        :param query:
        :return:
        """
        if not query.startswith('SHOW'):
            raise ValueError('Unsupported query by Explorer: {}'.format(query))

        sub_query = query.replace('SHOW ', '')

        if sub_query.startswith('RETENTION'):
            result = Explorer._show_retention()
        elif sub_query.startswith('MEASUREMENTS'):
            result = self._show_measurements(schema, sub_query)
        elif sub_query.startswith('FIELD'):
            result = self._show_fields(schema, sub_query)
        elif sub_query.startswith('TAG KEYS'):
            result = self._show_tag_keys(schema, sub_query)
        elif sub_query.startswith('TAG VALUES'):
            result = self._show_tag_values(schema, sub_query)
        else:
            raise ValueError('Unsupported query by Explorer: {}'.format(query))

        return Explorer._response(query, result)

    @staticmethod
    def _show_retention() -> Dict:
        """
        Return retention policies
        :param sub_query:
        :return:
        """
        # AnomalyDB doesn't support the concept of retention policies. Return
        # some default value
        return {
            "results": [
                {
                    "statement_id": 0,
                    "series": [
                        {
                            "columns": ["name"],
                            "values": [["default"]],
                        }
                    ]
                }
            ],
        }

    def _show_measurements(self, schema: Schema, sub_query: str) -> Dict:
        """
        Return measurement names.
        :param sub_query:
        :return:
        """
        exploration_ds = schema.exploration_datasources["influx"]
        measurements = self.druid_schema.get_measurements(exploration_ds)

        # Sub query looks like:
        #  MEASUREMENTS WITH MEASUREMENT =~ /k/ LIMIT 100
        if "WITH MEASUREMENT" in sub_query:
            tokens = sub_query.split()
            # Pattern should exist in the third from last token
            pattern = tokens[-3].strip('/')
            measurements = list(filter(lambda name: pattern in name,
                                       measurements))
        return {
            "results": [
                {
                    "statement_id": 0,
                    "series": [
                        {
                            "name": "measurements",
                            "columns": ["name"],
                            "values": [[m] for m in measurements],
                        }
                    ]
                }
            ],
        }

    def _show_fields(self, schema: Schema, sub_query: str) -> Dict:
        """
        FIELD KEYS FROM <measurement>
        :param sub_query
        :return:
        """
        # Sub query looks like:
        #  FIELD KEYS FROM "tsdb"
        expected_pattern = "FIELD KEYS FROM"
        if not sub_query.startswith(expected_pattern):
            raise ValueError("Expected query to contain: '%s'" %
                             expected_pattern)

        measurement_name = sub_query.split()[3].strip('"')
        exploration_ds = schema.exploration_datasources["influx"]
        fields = self.druid_schema.get_fields(exploration_ds, measurement_name)

        return {
            "results": [
                {
                    "statement_id": 0,
                    "series": [
                        {
                            "name": measurement_name,
                            "columns": ["fieldKey"],
                            "values": [[f] for f in fields],
                        }
                    ]
                }
            ],
        }

    def _show_tag_keys(self, schema: Schema, sub_query: str) -> Dict:
        """
        TAG KEYS FROM <measurement>
        :param sub_query:
        :return:
        """
        # Sub query looks like:
        #  TAG KEYS FROM "tsdb"
        expected_pattern = "TAG KEYS FROM"
        if not sub_query.startswith(expected_pattern):
            raise ValueError("Expected query to contain: '%s'" %
                             expected_pattern)

        measurement_name = sub_query.split()[3].strip('"')
        exploration_ds = schema.exploration_datasources["influx"]
        tags = self.druid_schema.get_dimensions(exploration_ds)

        return {
            "results": [
                {
                    "statement_id": 0,
                    "series": [
                        {
                            "name": measurement_name,
                            "columns": ["tagKey"],
                            "values": [[t] for t in tags],
                        }
                    ]
                }
            ],
        }

    def _show_tag_values(self, schema: Schema, sub_query: str) -> Dict:
        """
        TAG VALUES FROM <measurement>
        :param sub_query:
        :return:
        """
        # Sub query looks like:
        #  TAG VALUES FROM "tsdb" WITH KEY = "DiskLabel"
        expected_pattern = "TAG VALUES FROM"
        if not sub_query.startswith(expected_pattern):
            raise ValueError("Expected query to contain: '%s'" %
                             expected_pattern)
        tokens = sub_query.split()
        # The tag key should be the eighth token and the measurement name should
        # be the fourth token
        tag_key = tokens[7].strip('"').strip("'")
        measurement_name = sub_query.split()[3].strip('"')
        exploration_ds = schema.exploration_datasources["influx"]

        # Check if we need to add additional filter clauses
        if len(tokens) > 8:
            # Sub query looks like:
            # TAG VALUES FROM "tsdb" WITH KEY = "DiskLabel" AND tag_key =
            # 'tag_value'
            #
            # This type of query is not supported in InfluxQL but we allow it
            # so that we can allow tag relationships. Relationships can be
            # used to determine tag values under certain conditions.
            #
            # For example, if you wanted to find out the Node tag values for a
            # given cluster UUID, you could use the following query:
            # SHOW TAG VALUES FROM "t" WITH KEY = "Node" AND Cluster = "ABC"
            if (len(tokens) - 8) % 4 != 0:
                # Each filter clause must have 4 tokens
                # TODO: In the future, we should replace this with some SQL
                # parsing library
                raise ValueError("Unsupported query: '%s'" % sub_query)

            # TODO: In the future, we should replace this with some SQL
            # parsing library
            extra_and_clauses = {}
            for idx in range(8, len(tokens), 4):
                operator = tokens[idx]
                if operator.upper() != "AND":
                    raise ValueError("Only AND is supported in "
                                     "SHOW TAG VALUES: %s" % sub_query)
                filter_tag_key = tokens[idx + 1]
                filter_tag_value = tokens[idx+3].strip("'").strip('"')
                extra_and_clauses[filter_tag_key] = filter_tag_value

        tag_values = self.druid_schema.get_dimension_values(exploration_ds,
                                                            tag_key,
                                                            extra_and_clauses)
        return {
            "results": [
                {
                    "statement_id": 0,
                    "series": [
                        {
                            "name": measurement_name,
                            "columns": ["key", "value"],
                            "values": [[tag_key, v] for v in tag_values],
                        }
                    ]
                }
            ],
        }

    @staticmethod
    def _response(query: str, result: Dict) -> Response:
        """
        Create the response object
        :param query:
        :param result:
        :return:
        """
        json_result = json.dumps(result)
        logger.info('%s = %s', query, json_result)
        return Response(response=json_result,
                        status=200,
                        mimetype="application/json")
