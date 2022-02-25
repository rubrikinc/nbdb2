"""
SparseAlgoLossLess
"""
from nbdb.schema.schema import Schema

from nbdb.common.data_point import DataPoint, MODE_REALTIME
from nbdb.common.data_point import DATA_DROP, DATA_WRITE, DATA_FORCE_WRITE
from nbdb.store.sparse_series_stats import SparseSeriesStats


class SparseAlgoLossLess:
    """
    Performs an equality check (loss less)
    """

    def __init__(self, schema: Schema, consumer_mode: str):
        """
        Initialize
        :param schema:
        """
        self.schema = schema
        self.consumer_mode = consumer_mode

    def process(self, data_point: DataPoint,
                stats: SparseSeriesStats,
                window: int,
                replay_mode: bool) -> int:
        """
        Checks if it has changed
        :param data_point:
        :param stats:
        :param window: rollup window
        :param replay_mode: Indicates whether this datapoint received is being
        processed in rewind phase or live phase.
        :return: Integer enum indicating whether point should be dropped,
        written or force written.
        """
        result = DATA_DROP
        if self._should_force_write(data_point, stats, window, replay_mode):
            # DATA_FORCE_WRITE check takes precedence over DATA_WRITE.
            # If we get a datapoint beyond the forced write interval with a
            # value significantly different, we should return DATA_FORCE_WRITE
            # instead of DATA_WRITE so that exploration datapoints get written.
            result = DATA_FORCE_WRITE
        elif self.has_changed(data_point, stats, window):
            result = DATA_WRITE

        self.update_received_point(data_point, stats, window, replay_mode)
        return result

    def has_changed(self,
                    data_point: DataPoint,
                    stats: SparseSeriesStats,
                    window: int) -> bool:
        """
        Loss-less sparseness,
        :param data_point:
        :param stats:
        :param window:
        :return:
        """
        # not relevant here but child classes benefit from self reference
        _ = self
        return data_point.value != stats.get_window_value(window)

    def _should_force_write(self,
                            data_point: DataPoint,
                            stats: SparseSeriesStats,
                            window: int,
                            new_replay_mode: bool) -> bool:
        """
        Checks if the last data point written to store is past the
        forced_write_interval in which case even if the value hasn't
        changed we are forced to write it

        Additionally we also force write if the consumer is transitioning from
        replay_mode=True to replay_mode=False (ie. going from "rewind" phase to
        "live" phase)

        :param data_point:
        :param stats:
        :param window:
        :param new_replay_mode: Indicates whether the latest data_point is
        being processed in rewind phase or live phase.
        :return:
        """
        if (self.consumer_mode != MODE_REALTIME and
                not new_replay_mode and
                stats.get_replay_mode()):
            # This is the first datapoint we have received since our rewind
            # phase was over. Force write this datapoint since any of the
            # writes generated during the rewind phase are not actually
            # recorded.
            #
            # Doing this is necessary to maintain our forced_write_interval
            # invariant: i.e. ensure max gap of 12h (forced_write_interval)
            # between all datapoints.
            #
            # Consider this example:
            # 12:00 AM: Value=1 (written)
            # 12:10 AM: Value=1 (dropped)
            # ...
            # 06:30 AM: Value=1 (dropped)
            # ...
            # 11:00 AM: Value=1 (dropped)
            #
            # [Consumer crashes, resets offsets to 06:30 AM]
            # 06:30 AM: Value=1 (replay_mode=True, dummy write)
            # ...
            # 11:10 AM: Value=1 (replay_mode=False, write=?)
            #
            # If we do not write the 11:10 AM datapoint, then the next
            # datapoint that will be written is the 06:30 PM one. This is
            # because during the rewind phase, we set the last written
            # datapoint to be 06:30 AM one.
            #
            # This is why we must write the 11:10 AM to maintain the invariant
            # of max 12h gap between datapoints
            return True

        # Get the forced write interval for the datasource

        # TODO the commented line below is the correct one - the write path
        #  should use the setting that specifies forced write interval. But,
        #  since the read path currently uses this as the value for lookback,
        #  we can't simply change the setting until we deploy readapi change to
        #  do a "smart" lookback, i.e. use 12hr for "older-than-change"
        #  window-start-time of queries, or 30 min for "after-the-change".
        #  Once readapi "smart" lookback is deployed - use the commented
        #  line instead, and update the setting in the config file to
        #  officially be 30m.
        #
        # forced_write_interval = self.schema.get_forced_rewrite_interval(
        #    data_point.datasource)
        forced_write_interval = min(
            self.schema.get_forced_rewrite_interval(data_point.datasource),
            30 * 60)  # 30 minutes

        return (data_point.epoch - stats.get_window_epoch(window) >
                forced_write_interval)

    def update_written_point(self,
                             data_point: DataPoint,
                             stats: SparseSeriesStats,
                             window: int) -> None:
        """
        Update the stats object when the data point is written
        :param data_point:
        :param stats:
        :param window:
        """
        # this method will be overridden
        _ = self
        stats.set_window_value(window, data_point.value)
        stats.set_window_epoch(window, data_point.epoch)
        if data_point.pre_transform_value is not None:
            # store the pre-transform-value in the data dictionary
            stats.set_pre_transform_value(data_point.pre_transform_value)
        # TODO: Disabled for windows
        # stats.count = stats.count + 1

    def update_received_point(self,
                              data_point: DataPoint,
                              stats: SparseSeriesStats,
                              window: int,
                              replay_mode: bool) -> None:
        """
        Update the stats object for points received
        :param data_point:
        :param stats:
        :param window:
        :return:
        """
        # not used here, but is relevant to some algorithms
        _ = self
        # TODO: disabled
        # stats.incoming_count = stats.incoming_count + 1
        if window == 0:
            # only relevant for the default window (no rollups)
            stats.set_refresh_epoch(data_point.epoch)
            stats.set_server_rx_time(data_point.server_rx_epoch)
            if self.consumer_mode != MODE_REALTIME:
                # We do not need to update replay mode state if we are
                # generating just realtime metrics. We forego replay mode
                # processing for realtime metrics to reduce write lag.
                stats.set_replay_mode(replay_mode)
