"""
MetricValue
"""
from dataclasses import dataclass

from nbdb.common.metric_key import MetricKey


@dataclass
class MetricValue:
    """
    Stores a single metric data point along with the tokenized metric key
    """
    key: MetricKey
    epoch: int
    value: float

    def __post_init__(self):
        """
        Validates the members are correctly initialized
        """
        assert isinstance(self.value, float)
        assert self.key.is_tokenized()
        assert self.epoch > 0
