"""
Anomaly API module
"""

import importlib
import logging
from typing import Dict, List

from nbdb.readapi.graphite_response import AnomalyResponse, GraphiteResponse
from pyformance import meter_calls, time_calls
from werkzeug.datastructures import ImmutableMultiDict
import numpy as np
import pandas as pd


logger = logging.getLogger(__name__)


class AnomalyApi:  # pylint: disable=too-few-public-methods
    """
    Parses and executes a graphite api
    """

    def __init__(self, query: ImmutableMultiDict):
        self.query = query

    @staticmethod
    def __series_from_datapoints(datapoints: List) -> pd.Series:
        epochs = [x[1] for x in datapoints]
        values = [x[0] for x in datapoints]
        return pd.Series(values, index=epochs)

    @time_calls
    @meter_calls
    def execute_graphite(self, graphite: GraphiteResponse) -> AnomalyResponse:
        """
        :param graphite: Graphite response object containing raw datapoints
        :return:
        """
        anomaly_algorithm = self.query.get('anomaly_algorithm',
                                           'nbdb.anomaly.gaussian.Gaussian')
        module, cls = anomaly_algorithm.rsplit('.', 1)
        anomaly_module = importlib.import_module(module)
        anomaly_finder = getattr(anomaly_module, cls)(self.query)
        groupby_mappings = dict()
        baselines = self._compute_baselines(graphite, groupby_mappings)

        # Compute anomalies per series
        anomalies = dict()
        for timeseries in graphite.response:
            raw_data = self.__series_from_datapoints(timeseries['datapoints'])
            groupby_key = groupby_mappings.get(timeseries['target'])
            if not groupby_key:
                baseline = raw_data.values
            else:
                baseline = np.array(baselines[groupby_key])
            anomalies[timeseries['target']] = anomaly_finder.find_anomalies(
                baseline, raw_data)

        return AnomalyResponse.create_response_from_graphite(anomalies, graphite)

    def _compute_baselines(self, graphite: GraphiteResponse, groupby_mappings: Dict) -> Dict:
        baselines = dict()
        groupby_tokens = self.query.get('groupby_tokens', [])
        # Build baselines
        if groupby_tokens:
            for timeseries in graphite.response:
                tokens = timeseries['target'].split('.')
                groupby_key = ','.join([tokens[token_idx]
                                        for token_idx in groupby_tokens])
                if groupby_key not in baselines:
                    baselines[groupby_key] = list()
                baselines[groupby_key].extend(timeseries['datapoints'])
                groupby_mappings[timeseries['target']] = groupby_key
        return baselines
