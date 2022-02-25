"""
Basic anomaly interface class
"""
from typing import List, Tuple

import numpy as np
import pandas as pd


class AnomalyInterface:  # pylint: disable=too-few-public-methods
    """
    Base interface for the anomaly algorithm objects

    Needs to be extended by the actual algorithm classes
    """
    def __init__(self, config):
        self.config = config

    @staticmethod
    def _preprocess(values: np.array) -> np.array:
        """
        Clean up the data.

        Currently we only remove nan values from the numpy array
        :param values:
        :return:
        """
        return values[~np.isnan(values)]

    def find_anomalies(self,
                       baseline: np.array,
                       raw_data: pd.Series) -> List[Tuple]:
        """
        Abstract method to be implemented by child classes.

        Takes the baseline along with the raw data and returns
        a list of anomalies in the raw data
        :param baseline:
        :param raw_data:
        :return:
        """
        raise NotImplementedError('Child classes should implement this '
                                  'function')
