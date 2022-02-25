"""
Static threshold based anomaly detection
"""
from typing import List, Tuple
import logging

import numpy as np
import pandas as pd

from nbdb.anomaly.anomaly_interface import AnomalyInterface
from nbdb.readapi.graphite_response import Anomaly
from nbdb.readapi.time_series_response import TimeRange

logger = logging.getLogger(__name__)


class Static(AnomalyInterface):  # pylint: disable=too-few-public-methods
    """
    Simple algorithm to do threshold based anomaly detection.
    Currently supports two functions (lt, gt).
    """

    def find_anomalies(self,
                       baseline: np.ndarray,
                       raw_data: pd.Series) -> List[Tuple]:
        """
        Use static threshold to determine anomalies in the
        raw data. Supports the lt, gt functions to compare
        against the threshold
        :param baseline:
        :param raw_data:
        :return:
        """
        comparator_fn = self.config.get('comparator_fn', 'gt')
        threshold = self.config.get('threshold')
        raw_data.dropna(inplace=True)
        if comparator_fn == 'gt':
            anomalous_points = raw_data[raw_data > threshold]
        elif comparator_fn == 'lt':
            anomalous_points = raw_data[raw_data < threshold]
        else:
            raise NotImplementedError('Unknown comparator fn: {}'.format(
                comparator_fn))

        anomalies = []
        # No anomalous points found. Return early
        if len(anomalous_points) == 0:
            return anomalies

        previous_epoch = anomalous_points.index[0]
        anomaly_start = anomalous_points.index[0]
        sampling_interval = np.diff(raw_data.index).min()
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
            anomaly_score += 1

        # append the final anomaly
        if epoch is not None:
            anomaly_window = TimeRange(anomaly_start, epoch,
                                       sampling_interval)
            anomalies.append(Anomaly(anomaly_window, anomaly_score))
        return anomalies
