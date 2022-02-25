"""
SeriesStatsBase
"""
from __future__ import annotations

import json

SERVER_RX_TIME = 'server_rx_time'
REFRESH_EPOCH = 'refresh_epoch'
CROSSCLUSTER_SHARD = 'cc_shard'
SPARSENESS_DISABLED = 'sparseness_disabled'


class SeriesStatsBase:
    """
    The stats object maintains basic stats for a given series.
    """

    def __str__(self):
        """
        Override the default to string method to provide a more human
        readable value
        :return:
        """
        d = dict(self.__dict__)
        return json.dumps(d, sort_keys=True)

    def __eq__(self, other: SeriesStatsBase) -> bool:
        """
        Comparator for the stats object
        :param other:
        :return: True if the other object has same attributes as this
        """
        if len(other.__dict__) != len(self.__dict__):
            # They have different set of attributes
            return False

        # same number of attributes, so we need to check just one against the
        # the other, if they have a different set but same number
        # then one of the keys in others will be missing in self
        for key in other.__dict__:
            if key not in self.__dict__:
                return False
            if other.__dict__[key] != self.__dict__[key]:
                return False
        return True

    def set_refresh_epoch(self, epoch: int) -> SeriesStatsBase:
        """
        epoch of the last point received
        used to identify missing data points
        :param epoch:
        """
        setattr(self, REFRESH_EPOCH, epoch)
        return self

    def get_refresh_epoch(self) -> int:
        """
        epoch of the last point received
        used to identify missing data points
        :return:
        """
        return getattr(self, REFRESH_EPOCH, 0)

    def set_server_rx_time(self, epoch: int) -> SeriesStatsBase:
        """
        server_rx_time of latest  point received
        :param epoch:
        """
        setattr(self, SERVER_RX_TIME, epoch)
        return self

    def get_server_rx_time(self) -> int:
        """
        server_rx_time of latest  point received
        :return:
        """
        return getattr(self, SERVER_RX_TIME, 0)

    def get_crosscluster_shard(self) -> str:
        """
        Indicates the shard to be used for accessing the metric via
        the cross-cluster datasource.

        Return value of None means this series isn't whitelisted for the
        cross-cluster datasource
        """
        return getattr(self, CROSSCLUSTER_SHARD, None)

    def set_crosscluster_shard(self, shard: str) -> SeriesStatsBase:
        """
        Set the shard value of the crosscluster datasource to be used when
        generating a crosscluster datapoint
        """
        setattr(self, CROSSCLUSTER_SHARD, shard)
        return self

    def is_sparseness_disabled(self) -> str:
        """
        Indicates whether we have disabled sparseness for this series
        """
        return getattr(self, SPARSENESS_DISABLED, False)

    def set_sparseness_disabled(self) -> SeriesStatsBase:
        """
        Disable sparseness for this series

        NOTE: We only store this attribute for series which have sparseness
        disabled. For series which don't have sparseness disabled, we don't set
        this attribute. We will thus save memory because sparseness will be
        enabled for almost all series
        """
        setattr(self, SPARSENESS_DISABLED, True)
        return self
