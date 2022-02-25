"""
Gaussian based anomaly detection
"""
from typing import List, Tuple
import logging

from scipy.stats import norm
import numpy as np
import pandas as pd

from nbdb.anomaly.anomaly_interface import AnomalyInterface
from nbdb.readapi.graphite_response import Anomaly
from nbdb.readapi.time_series_response import TimeRange

logger = logging.getLogger(__name__)


class Gaussian(AnomalyInterface):  # pylint: disable=too-few-public-methods
    """
    Implementation of anomaly detection for series that follow a
    gaussian distribution.
    """
    def find_anomalies(self,
                       baseline: np.ndarray,
                       raw_data: pd.Series) -> List[Tuple]:
        anomalies = []
        # Preprocess the data - remove NaNs etc
        baseline_values = self._preprocess(baseline)
        raw_data.dropna(inplace=True)
        mean, std = baseline_values.mean(), baseline_values.std()
        raw_values = raw_data.values

        # In case the data only has nans or there is no change, then
        # return
        if std == 0 or len(raw_data) == 0:
            return anomalies

        # Compute the probability of occurence of the individual points
        probabilities = norm.pdf((raw_values - mean) / std)

        # Take the complement of the probabilities. Thus anything above
        # a specified threshold is anomalous.
        # TODO: Determine this threshold based on an expected number of
        # TODO: anomalies setting
        probabilities_comp = 1 - probabilities
        probability_threshold = self.config.get('probability_threshold', 0.99)
        anomalous_indices = \
            np.argwhere(probabilities_comp > probability_threshold).flatten()

        # No anomalous points found. Return early
        if len(anomalous_indices) == 0:
            return anomalies

        anomalies = self._mark_anomalies(raw_data, probabilities,
                                         anomalous_indices)
        return anomalies

    @staticmethod
    def _mark_anomalies(raw_data: pd.Series,
                        probabilities: np.ndarray,
                        anomalous_indices: np.ndarray) -> List:
        """
        Iterate through the anomalous points and create anomaly objects
        :param raw_data:
        :param probabilities:
        :param anomalous_indices:
        :return:
        """
        anomalies = []
        anomalous_points = raw_data.iloc[anomalous_indices]
        sampling_interval = np.diff(raw_data.index).min()
        previous_epoch = anomalous_points.index[0]
        anomaly_start = anomalous_points.index[0]
        anomaly_score = 1.0
        epoch = None
        for epoch, _ in anomalous_points.iteritems():
            if (epoch - previous_epoch) / sampling_interval > 1:
                # Mark the current anomaly as ended and start a new one
                anomaly_window = TimeRange(anomaly_start, previous_epoch,
                                           sampling_interval)
                anomalies.append(Anomaly(anomaly_window, anomaly_score))
                anomaly_score = 1.0
                anomaly_start = epoch
            else:
                previous_epoch = epoch
            anomaly_score *= probabilities[epoch]

        # append the final anomaly
        if epoch is not None:
            anomaly_window = TimeRange(anomaly_start, epoch,
                                       sampling_interval)
            anomalies.append(Anomaly(anomaly_window, anomaly_score))
        return anomalies
