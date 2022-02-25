"""
SparseTelemetry
"""
import logging

from nbdb.common.data_point import DataPoint, DATA_DROP
from nbdb.common.telemetry import Telemetry
from nbdb.config.settings import Settings
from nbdb.store.sparse_series_stats import SparseSeriesStats

logger = logging.getLogger()


class SparseTelemetry:
    """
    Reports telemetry about sparseness
    """

    def __init__(self,
                 telemetry_settings: Settings,
                 source_id: str,
                 consumer_mode: str):
        """
        Initialize
        :param telemetry_settings:
        :param source_id:
        """
        self.telemetry_settings = telemetry_settings
        self.source_id = source_id
        self.consumer_mode = consumer_mode

    def report(self,
               data_point: DataPoint,
               stats: SparseSeriesStats,
               algo_name: str,
               result: int) -> None:
        """
        Report telemetry
        :param data_point: Datapoint
        :param stats: Stats object
        :param algo_name: Name of sparse algo used
        :param result: Integer enum indicating whether point is written to
        store or not.
        :return:
        """
        written = result != DATA_DROP
        if self.telemetry_settings.gen_sparse_data_distribution:
            self.update_sparse_drop_counters(
                data_point.flat_field(), written)

        Telemetry.inst.registry.meter(
            measurement='SparseSeriesWriter.algo.total',
            tag_key_values=[f"Topic={self.source_id}",
                            f"ConsumerMode={self.consumer_mode}",
                            f"Algo={algo_name}"]).mark()
        if not written:
            Telemetry.inst.registry.meter(
                measurement='SparseSeriesWriter.algo.drop',
                tag_key_values=[f"Topic={self.source_id}",
                                f"ConsumerMode={self.consumer_mode}",
                                f"Algo={algo_name}"]).mark()

        if self.telemetry_settings.drop_ratio_log_threshold > 0:
            # XXX: Disable for production. Log series which are doing horribly
            # TODO : Reenable the count/incoming count with windows first
            #  drop_ratio = 1.0 - (float(stats.count) / stats.incoming_count)
            drop_ratio = 0
            # Wait for atleast 10 datapoints to appear before making a decision
            # about logging
            if (stats.incoming_count >= 10 and drop_ratio <=
                    self.telemetry_settings.drop_ratio_log_threshold):
                logger.info("LOW DROP RATIO: %s %s %s",
                            data_point.series_id, 0, 0)

    @staticmethod
    def update_sparse_drop_counters(field_name: str, changed: bool) -> None:
        """
        Update aggregate & per-metric-type sparseness counters.
        """
        metric_type = SparseTelemetry.get_metric_type(field_name)
        component_name = SparseTelemetry.get_component_name(field_name)
        # For histograms, record an additional level of counters
        hist_category = None
        if metric_type == "histograms":
            hist_category = SparseTelemetry.get_histogram_category(
                field_name)

        # Record total count per metric type
        Telemetry.inst.registry.meter(
            measurement="SparseSeriesWriter.total",
            tag_key_values=[f"MetricType={metric_type}"]).mark()
        if hist_category:
            Telemetry.inst.registry.meter(
                measurement="SparseSeriesWriter.total",
                tag_key_values=[f"MetricType={metric_type}",
                                f"HistCategory={hist_category}"]).mark()

        # Record total count per component, per metric type
        if component_name:
            Telemetry.inst.registry.meter(
                measurement="SparseSeriesWriter.total",
                tag_key_values=[f"Component={component_name}",
                                f"MetricType={metric_type}"]).mark()

        if not changed:
            # Record global drop count
            base_metric_name = "SparseSeriesWriter.sparse_drop"
            Telemetry.inst.registry.meter(base_metric_name).mark()

            # Record drop count per metric type
            Telemetry.inst.registry.meter(
                measurement=base_metric_name,
                tag_key_values=[f"MetricType={metric_type}"]).mark()
            if hist_category:
                Telemetry.inst.registry.meter(
                    measurement=base_metric_name,
                    tag_key_values=[f"MetricType={metric_type}",
                                    f"HistCategory={hist_category}"]).mark()

            # Record drop rate per component per metric type
            if component_name:
                Telemetry.inst.registry.meter(
                    measurement=base_metric_name,
                    tag_key_values=[f"Component={component_name}",
                                    f"MetricType={metric_type}"]).mark()

    @staticmethod
    def get_metric_type(metric_name: str) -> str:
        """Determine the type (gauge, counter etc.) of the incoming metric."""
        histogram_tokens = [".p50", ".p50", ".p75", ".p9", ".mean", ".max",
                            ".min", ".total", ".stddev"]
        meter_tokens = ["_rate"]
        counter_tokens = [".count"]

        check_fn = lambda t: t in metric_name
        if any(map(check_fn, meter_tokens)):
            return "meters"

        if any(map(check_fn, histogram_tokens)):
            # NOTE: We have to check for histograms later because otherwise we
            # can misclassify ".mean_rate" as a histogram
            return "histograms"

        if any(map(check_fn, counter_tokens)):
            return "counters"

        # Assume gauge by default
        return "gauges"

    @staticmethod
    def get_component_name(metric_name: str) -> str:
        """Return the 2-level component name often used in Graphite metrics"""
        tokens = metric_name.split(".")
        component_name = None
        if len(tokens) > 2:
            # Eg. clusters.SDFSMain.AgentServer
            component_name = "{}.{}".format(tokens[1], tokens[2])
        elif len(tokens) == 2:
            # Eg. clusters.Diamond.
            component_name = "{}.{}".format(tokens[0], tokens[1])
        elif len(tokens) == 1:
            component_name = tokens[0]
        return component_name

    @staticmethod
    def get_histogram_category(metric_name: str) -> str:
        """Return the individual category of the histogram metric.

        Eg. For the metric "JobFetcherLoop.JOB1.p_95", it will return 'p_95'
        """
        # NOTE: We are assuming the caller has already determined that this is
        # a histogram. We will not perform any extra validation and simply
        # return the last token
        tokens = metric_name.split(".")
        return tokens[-1] if tokens else None

    def record_distributions(self,
                             stats: SparseSeriesStats,
                             value: float,
                             delta: float) -> None:
        """
        Record distribution of deltas and values.
        """

        if not self.telemetry_settings.gen_sparse_data_distribution:
            # Skip recording the distribution
            return
        # Record global distributions
        if delta is not None:
            Telemetry.inst.registry.histogram(
                'SparseSeriesWriter.metric_delta').add(delta)
        Telemetry.inst.registry.histogram(
            'SparseSeriesWriter.metric_values').add(value)

        # Record distributions per metric type
        metric_type = SparseTelemetry.get_metric_type(stats.key)
        if delta is not None:
            Telemetry.inst.registry.histogram(
                measurement='SparseSeriesWriter.metric_delta',
                tag_key_values=[f"MetricType={metric_type}"]).add(delta)
        Telemetry.inst.registry.histogram(
            measurement='SparseSeriesWriter.metric_values',
            tag_key_values=[f"MetricType={metric_type}"]).add(value)

        # Record distributions per component, per metric type
        component_name = SparseTelemetry.get_component_name(stats.key)
        if component_name:
            if delta:
                Telemetry.inst.registry.histogram(
                    measurement='SparseSeriesWriter.metric_delta',
                    tag_key_values=[f"Component={component_name}",
                                    f"MetricType={metric_type}"]).add(delta)

            Telemetry.inst.registry.histogram(
                measurement='SparseSeriesWriter.metric_values',
                tag_key_values=[f"Component={component_name}",
                                f"MetricType={metric_type}"]).add(value)
