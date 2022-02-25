from abc import ABC, abstractmethod

from nbdb.common.data_point import DataPoint

# Special value reserved for missing datapoints

MISSING_POINT_VALUE = -1
# Special value reserved for zeros
ZERO_VALUE = -2
# Special value reserved for series marked dead
TOMBSTONE_VALUE = -3

class SparseStore(ABC):

    @staticmethod
    def normalize_value(value: float) -> float:
        """
        # NOTE: druid doesn't know how to distinguish null from 0
        # since sparse metrics generate lot of nulls, its important
        # we map valid 0 values to another number so that we have
        # an unambiguous representation of 0=null in druid
        :param value: original value that should be normalized
        :return: normalied value that can be safely written to druid
        """
        if value == 0:
            # special mapping of 0 (which is reserved by
            # druid for null)
            # read time the reverse mapping of -2 -> 0 have
            # to be applied
            return ZERO_VALUE
        return value

    @staticmethod
    def denormalize_value(value: float) -> float:
        """
        See the comments in normalize, this does the reverse mapping
        :param value:
        :return:
        """
        if value == ZERO_VALUE:
            return 0
        return value

    @abstractmethod
    def write(self, data_point: DataPoint) -> None:
        """
        Write the datapoint to the sparse store.
        """
        pass

    @abstractmethod
    def flush(self) -> None:
        """
        Flush outstanding datapoints to the sparse store.
        """
        pass


class NoopSparseStore(SparseStore):

    def write(self, data_point: DataPoint) -> None:
        return
    
    def flush(self) -> None:
        return
