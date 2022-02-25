"""Math utilities"""
import math


def quantile(data, qtile):
    """
    For small arrays, pure python implementation is faster than numpy.quantile.

    Note that unlike numpy.quantile, we do not interpolate percentile.
    """
    size = len(data)
    return sorted(data)[int(math.ceil((size * qtile))) - 1]
