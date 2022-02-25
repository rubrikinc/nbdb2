"""
SparseSeriesReaderInterface
"""

from abc import ABC, abstractmethod
from typing import List, Dict

from nbdb.schema.schema import Schema
from nbdb.readapi.down_sampler_sparse_mean import DownSamplerSparseMean
from nbdb.readapi.down_sampler_dense_mean import DownSamplerDenseMean
from nbdb.readapi.sql_parser import FillFunc
from nbdb.readapi.time_series_response import TimeSeriesGroup, TimeRange


class SparseSeriesReaderInterface(ABC):
    """
    Interface fo providing read access to the sparse data stored in backend
    This interface is required to map the sparse data back to dense form
    The readapi uses the dense form data to provide further aggregations
    """

    @abstractmethod
    def get_datasource(self,
                       schema: Schema,
                       field_prefix: str,
                       tags: Dict[str, str],
                       cluster_id: str,
                       interval: int,
                       protocol: str) -> str:
        """

        :param field_prefix:
        :param cluster_id:
        :param interval:
        :param protocol:
        :return:
        """
        raise NotImplementedError('Child class must provide definition of '
                                  'SparseSeriesReaderInterface.get_datasource '
                                  'method')

    @abstractmethod
    def get_field(self, # pylint: disable-msg=R0913  # Too many arguments
                  schema: Schema,
                  datasource: str,
                  field: str,
                  filters: dict,
                  time_range: TimeRange,
                  groupby: List[str],
                  query_id: str,
                  trace: bool,
                  fill_func: FillFunc,
                  fill_value: float,
                  create_flat_series_id: bool
                  ) -> TimeSeriesGroup:
        """
        Run the query and return the timeseries
        :param datasource: Datasource name
        :param field:
        :param filters:
        :param time_range:
        :param groupby: list of fields to group the results by
        :param query_id: unique string assigned to the specific query
        :param trace: if true log detailed execution trace
        :param fill_func: Function to use to fill in None values
        :param fill_value: Fill value to use if fill_func is constant
        :param create_flat_series_id: If true series_id generated follow
        graphite flat dot schema
        :return: list of epochs & list of list of values corresponding to
                 epochs
        The field depending on the filters may map to multiple series
        we return the result of each series as list of values
        Where len(epochs) = (end_epoch - start_epoch)/interval
        """
        raise NotImplementedError('Child class must provide definition of '
                                  'SparseSeriesReaderInterface.get_field '
                                  'method')

    @staticmethod
    def instantiate_aggregator(down_sampling_function: str,
                               time_range: TimeRange,
                               fill_func: FillFunc,
                               fill_value: float,
                               default_fill_value: float,
                               sparseness_disabled: bool,
                               query_id: str = None,
                               trace: bool = False):
        """
        Instatiate the aggregator given the name
        :param down_sampling_function:
        :param time_range:
        :param fill_value:
        :param query_id:
        :param trace:
        :return:
        """
        if down_sampling_function != 'mean':
            raise ValueError('DownSamplingAlgorithm: {} not supported'
                             .format(down_sampling_function))

        if sparseness_disabled:
            # Sparseness is disabled. Instantiate aggregators which treat data
            # as non-sparse
            return DownSamplerDenseMean(time_range, query_id, trace)

        # Sparseness is enabled. Instantiate aggregators which can treat
        # data as sparse and extrapolate
        return DownSamplerSparseMean(time_range, fill_func, fill_value,
                                     default_fill_value, query_id, trace)

