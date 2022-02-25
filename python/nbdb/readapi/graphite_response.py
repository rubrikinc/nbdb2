"""
TimeSeriesResponse
"""
from __future__ import annotations

import json
import logging
import math
from dataclasses import dataclass
from typing import List, Dict

from nbdb.readapi.time_series_response import TimeRange, TimeSeries
from nbdb.readapi.time_series_response import TimeSeriesGroup
import numpy as np

logger = logging.getLogger(__name__)


class NumpyEncoder(json.JSONEncoder):
    """Custom encoder for certain numpy data types that can't be handled """
    # Pylint keeps complaining about these errors. It doesn't know what it's
    # talking about. Ignore
    #
    # pylint: disable-msg=W0221  # Arguments Differ
    # pylint: disable-msg=E0202  # Method Hidden
    def default(self, obj):
        if isinstance(obj, (np.int_, np.intc, np.intp, np.int8,
                            np.int16, np.int32, np.int64, np.uint8,
                            np.uint16, np.uint32, np.uint64)):
            return int(obj)
        return json.JSONEncoder.default(self, obj)


@dataclass
class GraphiteResponse:
    """
        TimeSeriesResponse captures the result of a query against the sparse
        metric store. The response object captures the result as a dense matrix
        Grafana expects from a Graphite Datasource
        [
            {
                "datapoints": [ [value, epoch_s]..],
                "target": "series_name_1",
                tags: {optional}
            },
            ...
            {
                "datapoints": [ [value, epoch_s]..],
                "target": "series_name_n",
                tags: {optional}
            }
        ]
    """
    response: list

    @staticmethod
    def create_response(result: Dict[str, TimeSeriesGroup]) \
            -> GraphiteResponse:
        """
        Converts the TimeSeriesGroup to TimeSeriesResponse object
        :param result: TimeSeriesGroup
        """
        if '_' not in result or '_' not in result['_'].groups:
            raise ValueError('Unexpected result for GraphiteResponse: {}'
                             .format(json.dumps(result, cls=NumpyEncoder)))
        return GraphiteResponse._create_response(result['_'].groups['_'])

    @staticmethod
    def _create_response(result: List[TimeSeries]) -> GraphiteResponse:
        """
        :param result:
        :return:
        """
        response = list()
        for ts in result:
            graphite_series = dict()
            graphite_series["datapoints"] = [
                [None if p[1] is not None and math.isnan(p[1]) else p[1],
                 p[0]]
                for p in ts.points]
            graphite_series["target"] = ts.series_id
            response.append(graphite_series)

        return GraphiteResponse(response)

@dataclass
class Anomaly:
    """
    Data class to hold anomaly objects
    """
    timewindow: TimeRange
    score: float


@dataclass
class AnomalyResponse:
    """
        AnomalyResponse captures the result of running anomaly detection
        on the given set of series specified by the graphite query
        {
            "target1": {
                "data": [ [value, epoch_s]..],
                "anomalies": [[start1, end1], [start2, end2]...]
            },
            ...
            "targetn": {
                "data": [ [value, epoch_s]..],
                "anomalies": [[start1, end1], [start2, end2]...]
            }
        ]
    """
    response: Dict[str, Dict[str, List]]

    @staticmethod
    def create_response_from_graphite(anomalies: Dict[str, List[Anomaly]],
                                      data: GraphiteResponse) -> \
            AnomalyResponse:
        """
        Converts the TimeSeriesGroup to TimeSeriesResponse object
        :param result: TimeSeriesGroup
        """
        response = dict()
        for series in data.response:
            target = series['target']
            response[target] = {
                'anomalies': anomalies[target],
                'data': series['datapoints']
            }

        return AnomalyResponse(response)
