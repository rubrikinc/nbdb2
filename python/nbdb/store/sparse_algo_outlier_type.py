"""
SparseAlgoLastValueDelta
"""
import logging
from typing import Tuple

from nbdb.common.tracing_config import TracingConfig
from nbdb.config.settings import Settings
from nbdb.store.dist_hist import DistHist
from nbdb.store.sparse_algo_last_value_delta import SparseAlgoLastValueDelta

from nbdb.schema.schema import Schema, SparseAlgoSetting
from nbdb.common.data_point import DataPoint, BAND_LOW, BAND_HIGH, BAND_PROB
from nbdb.common.data_point import MEASUREMENT
from nbdb.store.sparse_store import SparseStore
from nbdb.store.sparse_series_stats import SparseSeriesStats

logger = logging.getLogger()


class SparseAlgoOutlierType(SparseAlgoLastValueDelta):
    """
    Learns distribution of data across multiple dimensions.
    Then computes bands of data ranges with high density of data
    A new data point is replaced by the band if it falls within the band
    The original data point is dropped and the band value is written to
    druid assuming the last value was not the band value already.
    A data point outside the band is reported as is.
    The forced rewrite interval and tombstones are still computed as before
    """

    def __init__(self,
                 schema: Schema,
                 consumer_mode: str,
                 sparse_store: SparseStore,
                 setting: SparseAlgoSetting):
        """
        Initialize
        :param sparse_algo_settings:
        """
        SparseAlgoLastValueDelta.__init__(self, schema, consumer_mode,
                                          setting.min_delta)
        self.sparse_store = sparse_store
        self.dist_hist: DistHist = DistHist(
            pattern=','.join(setting.patterns),
            bin_count=setting.bin_count,
            band_computation_points=setting.band_computation_points,
            max_band_width=setting.max_band_width,
            min_band_probability=setting.min_band_probability,
            min_band_probability_change=setting.min_band_probability_change
        )
        self.setting: SparseAlgoSetting = setting
        self._last_stats_epoch: int = 0

    def has_changed(self,
                    data_point: DataPoint,
                    stats: SparseSeriesStats,
                    window: int
                    ) -> bool:
        """
        Returns true if the point is significant and should be written
        :param data_point:
        :param stats:
        :param window:
        :return: True if the point should be written
        """
        # check if the data point belongs to an outlier type
        # First fit the data point
        if self.dist_hist.fit(data_point.value):
            # New set of bands got computed
            # check if we should be reporting aggregated stats
            if self.setting.report_aggregated_stats:
                self.write_stats(data_point)

        if TracingConfig.TRACE_ACTIVE:
            logger.info('data_point: %s dist_hist has %d points',
                        data_point.series_id,
                        self.dist_hist.get_count())

        # Check if the bands are ready (has had enough data points)
        if not self.dist_hist.is_ready():
            if self.setting.drop_before_ready:
                if TracingConfig.TRACE_ACTIVE:
                    logger.info('data_point: %s dropped histogram not ready.'
                                ' points in histogram %d',
                                data_point.series_id,
                                self.dist_hist.get_count())
                return False
            return SparseAlgoLastValueDelta.has_changed(self,
                                                        data_point,
                                                        stats,
                                                        window)

        # get the band if any
        band: Tuple[float, float, float] = self.dist_hist.bands.get_band(
            data_point.value)
        if not band:
            if TracingConfig.TRACE_ACTIVE:
                logger.info('data_point: %s %f not in band',
                            data_point.series_id, data_point.value)
            return SparseAlgoLastValueDelta.has_changed(self,
                                                        data_point,
                                                        stats,
                                                        window)

        if self.setting.report_aggregated_stats:
            if TracingConfig.TRACE_ACTIVE:
                logger.info('data_point: %s %f in band [%f-%f, %f].'
                            ' Dropped because only aggregated stats requested',
                            data_point.series_id,
                            data_point.value,
                            band[0], band[1], band[2])

            # In this case we only report aggregated stats
            # and outlier points only.
            # Band values are not reported per series
            return False

        band_mean_value = (band[0]+band[1])/2

        if TracingConfig.TRACE_ACTIVE:
            logger.info('data_point: %s %f in band [%f-%f, %f].'
                        ' reporting band mean value',
                        data_point.series_id,
                        data_point.value,
                        band[0], band[1], band[2])

        # check if the last value stored in stats matches the band_mean_value
        if stats.get_window_value(window) != band_mean_value:
            self._write_band(data_point, band)
            stats.set_window_value(window, band_mean_value)
            stats.set_window_epoch(window, data_point.epoch)

        # Data point can be dropped we have already written the band for it
        return False

    def _write_band(self,
                    data_point: DataPoint,
                    band: Tuple[float, float, float]) -> None:
        """
        Write the band value to druid in lieu of the specified data point
        The original data point is replaced by a band data point.
        The band data point value is the mean value of band (center line
        through it), additionally to preserve the band information I create
        three new dimensions band_low, band_high and band_probability.

        So a band unaware API call will still function, because it will ignore
        the "band dimensions" and will see a flat center line of the band,
        instead of the original data points.

        However a band aware API call, can query for the additional band
        dimensions as well, and return the result as expicit points or area
        bands where the points were covered by a band.

        Both options can be supported by this schema
        :param data_point:
        :param band:
        """
        band_tags = dict(data_point.tags)
        band_tags[BAND_LOW] = str(int(band[0]*100)/100)
        band_tags[BAND_HIGH] = str(int(band[1]*100)/100)
        band_tags[BAND_PROB] = str(int(band[2]*100)/100)
        data_point_band = DataPoint(data_point.datasource,
                                    data_point.field,
                                    band_tags,
                                    data_point.epoch,
                                    data_point.server_rx_epoch,
                                    int((band[0] + band[1])*100)/200)

        if Settings.inst.logging.log_metrics:
            logger.info(str(data_point_band))
        self.sparse_store.write(data_point_band)

    def write_stats(self, dp: DataPoint) -> None:
        """
        Write the aggregated stats
        """
        # check if interval since last report has expired
        if self._last_stats_epoch != 0 and \
                dp.epoch - self._last_stats_epoch < self.setting.stats_interval:
            return

        band_tags = dict(dp.tags)
        # drop the tags that across which distributions are built
        for tag_key in self.setting.drop_tags:
            if tag_key in band_tags:
                del band_tags[tag_key]

        # Its likely that we have dropped cluster id, so get the default
        # datasource which is independent of the clusters
        protocol = 'influx' if MEASUREMENT in dp.tags else 'graphite'
        band_datasource = self.schema.default_datasource[protocol]

        for band in self.dist_hist.bands.bands:
            band_tags[BAND_LOW] = str(int(band[0] * 100) / 100)
            band_tags[BAND_HIGH] = str(int(band[1] * 100) / 100)
            band_tags[BAND_PROB] = str(int(band[2] * 100) / 100)
            data_point_band = DataPoint(band_datasource,
                                        dp.field,
                                        band_tags,
                                        dp.epoch,
                                        dp.server_rx_epoch,
                                        int((band[0] + band[1]) * 100) / 200)
            if Settings.inst.logging.log_metrics:
                logger.info(str(data_point_band))

            self.sparse_store.write(data_point_band)

        self._last_stats_epoch = dp.epoch
