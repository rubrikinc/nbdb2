"""
DenseBatchConsumer
"""

import logging
import os
import random
import re
import shutil
import string
import subprocess
import time
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Callable

import boto3
from confluent_kafka import Producer, TopicPartition, KafkaException, \
    KafkaError
from pyformance import time_calls

from nbdb.batch_consumer.dense_consumer_telemetry import \
    DenseConsumerTelmetryHelper
from nbdb.batch_consumer.utils import SparseBatchMessage, \
    DenseBatchMessage
from nbdb.batch_converter.sparse_converter import SparseConverter
from nbdb.common.consumer_base import ConsumerBase
from nbdb.common.context import Context
from nbdb.common.telemetry import Telemetry, meter_failures
from nbdb.config.settings import DenseBatchConsumerSettings, \
    SparseBatchConsumerSettings, Settings
from nbdb.store.file_backed_sparse_store import FileBackedSparseStore, \
    FilestoreManifest

logger = logging.getLogger()
s3_resource = boto3.resource("s3")
s3_client = boto3.client("s3")

SKIP_PREVIOUSLY_PROCESSED = (
    int(os.environ.get("SKIP_PREVIOUSLY_PROCESSED", "1")) == 1)

@dataclass
class UploadedSparseFileInfo:
    bucket: str
    key: str
    datasource: str


class DenseBatchConsumer(ConsumerBase):
    """
    DenseBatchConsumer processes messages from the dense batch
    kafka topic, applies sparseness on the dense batch metrics,
    uploads them to S3, and sends messages to the sparse kafka
    topic on successful processing.
    """

    TMP_ROOT_DIR = "/tmp/dense-consumer-root/"
    BATCH_JSON_LOCAL_NAME = "batch.json.gz"
    FILESTORE_DIR_NAME = "filestore"
    # Sparse conversion can take a while. Set poll interval limit to 60 mins
    # to be safe
    MAX_POLL_INTERVAL_MS = 3600000

    def __init__(self,
                 context: Context,
                 sparse_batch_settings: SparseBatchConsumerSettings,
                 dense_batch_settings: DenseBatchConsumerSettings,
                 setting_file: str,
                 schema_mapping: str,
                 batch_filter_file: str,
                 consumer_mode: str):
        """
        Initialize the consumer
        :param consumer_settings:
        """
        if not context.schema.batch_mode:
            raise RuntimeError("Schema must be loaded in batch mode.")

        self.telemetry_helper = DenseConsumerTelmetryHelper(Telemetry.inst)
        self.settings = Settings.inst
        self.context = context
        self.dense_batch_settings = dense_batch_settings
        self.sparse_batch_settings = sparse_batch_settings
        self.conversion_whitelist = dense_batch_settings.conversion_whitelist

        # Useful for passing to command line executions
        self.setting_file = setting_file
        self.schema_mapping = schema_mapping
        self.batch_filter_file = batch_filter_file
        self.consumer_mode = consumer_mode

        self.items_received = 0
        self.partition_to_producer_map: Dict[int, Producer] = {}
        self.group_metadata = None
        self.sparse_converter = SparseConverter(self.context,
                                                self.settings,
                                                min_storage_interval=60)
        # Create root tmp dir
        self.root_tmp_dir = Path(DenseBatchConsumer.TMP_ROOT_DIR)
        if self.root_tmp_dir.exists():
            shutil.rmtree(str(self.root_tmp_dir))
        self.root_tmp_dir.mkdir()

        ConsumerBase.__init__(self,
                              dense_batch_settings.topic,
                              dense_batch_settings.kafka_brokers,
                              dense_batch_settings.group,
                              lambda x: x.decode('utf-8'),
                              max_poll_interval_ms=\
                                  DenseBatchConsumer.MAX_POLL_INTERVAL_MS)

    @staticmethod
    def _check_if_file_exists_in_s3(bucket, key) -> bool:
        """Return True if the file already exists in S3, else False."""
        response = s3_client.list_objects_v2(Bucket=bucket, Prefix=key)
        for obj in response.get('Contents', []):
            if obj['Key'] == key:
                return True
        return False

    @staticmethod
    def _parse_hostname(s3_key: str) -> str:
        """
        Parse hostname from the S3 key.

        :param s3_key: S3 key to be downloaded
        :return: Returns parsed hostname string
        """
        # S3 key looks like the following:
        # s3://<bucket>/<date>/<CID>/<NID>/2021-03-18T01-53-03.batch.json.gz
        pattern = (r"\/([a-zA-Z0-9_-]+)\/"
                   r"\d{4}\-\d{2}\-\d{2}T\d{2}\-\d{2}\-\d{2}.batch.json")
        match = re.search(pattern, s3_key)
        assert match, "S3 key does not follow expected pattern: %s" % s3_key
        return match.group(1)

    @staticmethod
    def _parse_bundle_creation_timestamp(s3_key: str) -> datetime:
        """
        Parse bundle creation timestamp from the S3 key.

        :param s3_key: S3 key to be downloaded
        :return: Returns parsed datetime object
        """
        # S3 key looks like the following:
        # s3://<bucket>/<date>/<CID>/<NID>/2021-03-18T01-53-03.batch.json.gz
        pattern = r"\/(\d{4}\-\d{2}\-\d{2}T\d{2}\-\d{2}\-\d{2}).batch.json"
        match = re.search(pattern, s3_key)
        assert match, "S3 key does not follow expected pattern: %s" % s3_key
        timestamp_str = match.group(1)
        timestamp = datetime.strptime(timestamp_str, "%Y-%m-%dT%H-%M-%S")
        return timestamp

    @staticmethod
    def seconds_since_epoch(timestamp: datetime) -> int:
        """
        Convert datetime to seconds since epoch.
        """
        return int((timestamp - datetime(1970, 1, 1)).total_seconds())

    # TODO: Right now, if there's a failure in processing a message, ConsumerBase
    # skips the message. Consider a notion of retries or a strict mode where each
    # message needs to be processed
    def process_item(self, message: str, partition: int, offset: int) -> None:
        """
        Process the metric message

        :param message: Metric message
        :param partition: Partition from which the Kafka message was fetched
        :param offset: Offset of the message within the Kafka partition
        """
        message = DenseBatchMessage.from_serialized_json(message)
        logger.info("Message is %s", message)
        if (self.conversion_whitelist and
                message.cluster_id not in self.conversion_whitelist):
            # Conversion whitelist is not empty and cluster is not present in
            # the whitelist
            logger.info("Skipping bundle from %s", message.cluster_id)
            return

        self.items_received += 1
        bundle_creation_ts = \
            DenseBatchConsumer._parse_bundle_creation_timestamp(
                message.batch_json_s3_key)
        hostname = DenseBatchConsumer._parse_hostname(
            message.batch_json_s3_key)

        # Check if sparse JSON files already exist
        sparse_json_output_s3_key = "%s/%s/%s/%s.batch.json.gz/MANIFEST" % (
            bundle_creation_ts.strftime("%Y-%m-%d"), message.cluster_id,
            hostname, bundle_creation_ts.strftime("%Y-%m-%dT%H-%M-%S"))

        if self._check_if_file_exists_in_s3(
                self.sparse_batch_settings.s3_bucket,
                sparse_json_output_s3_key):
            logger.info("Sparse JSON already exists for message: %s", message)
            if SKIP_PREVIOUSLY_PROCESSED:
                logger.info("Skipping message because already processed: %s",
                            message)
                return

        # Create a temp dir for this message
        with self._create_dir_for_message(message) as tmp_dir:
            tmp_dir: Path
            # Download file from S3 locally
            batch_json_local_path = DenseBatchConsumer._download_json_from_s3(
                bucket=self.dense_batch_settings.s3_bucket,
                key=message.batch_json_s3_key,
                tmp_dir=tmp_dir
            )

            # Parse bundle creation timestamp from S3 key. This will be used
            # for versioning later
            bundle_creation_time = DenseBatchConsumer.seconds_since_epoch(
                bundle_creation_ts)

            start_time = time.time()

            # Apply sparseness to dense JSON
            (file_store_path, manifest) = self._convert(
                message, batch_json_local_path, tmp_dir, bundle_creation_time)

            logger.info("SparseConversionStats- time: %ds, "
                        "size: %0.4f MB, datapoints: %0.2f mil, "
                        "cluster_id: %s",
                        (time.time() - start_time),
                        message.size_bytes/(1024 * 1024),
                        manifest.total_points_processed/1000000,
                        message.cluster_id)

            # Upload sparse files to S3
            uploaded = self._upload_filestore(
                key_prefix=message.batch_json_s3_key,
                filestore_dir=file_store_path,
                manifest=manifest)
            # Send messages to sparse batch consumer topic
            # Note that this method also commits the producer and consumer
            # streams, so we don't need to explicitly call
            # `ConsumerBase.mark_commit_req()`
            self._send_messages_to_sparse_consumer(partition,
                                                   offset,
                                                   uploaded)
            # Emit metrics
            self.telemetry_helper.update_on_successful_conversion(
                message=message, manifest=manifest)
        logger.info("Successfully processed message %s", message)

    @meter_failures
    def _convert(self,
                 message: DenseBatchMessage,
                 batch_json_local_path: Path,
                 tmp_dir: Path,
                 bundle_creation_time: int,
                 ) -> (Path, FilestoreManifest):
        """
        Convert dense batch json to sparse file store.

        :param message:
        :param batch_json_local_path:
        :param tmp_dir:
        :param bundle_creation_time: Seconds since epoch when bundle was
        created.
        """
        file_store_path = tmp_dir.joinpath(
            DenseBatchConsumer.FILESTORE_DIR_NAME)

        # We enable pypy conversion by default because it is much faster
        is_cluster_pypy_whitelisted = True
        if is_cluster_pypy_whitelisted:
            # Run sparse_converter in pypy
            cmd = ["pypy3",
                   "nbdb/batch_converter/sparse_converter_pypy.py",
                   "--setting_file", self.setting_file,
                   "--batch_mode", "True",
                   "--cluster_id", message.cluster_id,
                   "--batch_json_path", str(batch_json_local_path),
                   "--output_dir_path", str(file_store_path),
                   "--batch_filter_file", self.batch_filter_file,
                   "--bundle_creation_time", str(bundle_creation_time),
                   "--consumer_mode", self.consumer_mode,
                   "--compress", "True"]

            # schema_mapping is a list and we have to pass each of the
            # files in it separately
            for schema_mapping_val in self.schema_mapping:
                cmd.append("--schema_mapping")
                cmd.append(schema_mapping_val)

            subprocess.check_call(cmd, stderr=subprocess.STDOUT)
        else:
            self.sparse_converter.convert(
                cluster_id=message.cluster_id,
                batch_json_path=str(batch_json_local_path),
                output_dir_path=str(file_store_path),
                batch_filter_file=str(self.batch_filter_file),
                bundle_creation_time=bundle_creation_time,
                consumer_mode=self.consumer_mode,
                compress=True,
                progress_callback=self.telemetry_helper.update_conversion_rate
            )

        manifest = FileBackedSparseStore.parse_manifest(str(file_store_path))

        if is_cluster_pypy_whitelisted:
            # Since we don't send metrics in the pypy sparse converter,
            # send the total points processed, note that the first
            # argument is irrelevant
            self.telemetry_helper.update_conversion_rate(0,
                manifest.total_points_processed)

        return (file_store_path, manifest)

    @meter_failures
    @time_calls
    def _send_messages_to_sparse_consumer(
        self,
        partition: int,
        offset: int,
        sparse_files: List[UploadedSparseFileInfo]) -> None:

        # Transaction will be aborted if there's an exception
        with self._get_transactional_producer(partition) as producer:
            for sparse_file in sparse_files:
                s3_uri = f"s3://{self.sparse_batch_settings.s3_bucket}/{sparse_file.key}"
                sparse_message = SparseBatchMessage(
                    location_type="s3",
                    path=s3_uri,
                    datasource=sparse_file.datasource,
                    # Unused right now
                    start_ts_epoch=-1,
                    end_ts_epoch=-1
                )
                sparse_topic = self.sparse_batch_settings.topic
                # We use key=datasource so that all messages for the same
                # datasource go to the same partition in the sparse consumer.
                #
                # This is needed since Druid doesn't like multiple concurrent batch
                # ingest tasks for the same datasource. Sparse consumer fetches
                # and combines multiple messages for the same datasource into one
                # batch ingest task to maintain the invariant of at most one
                # concurrent batch ingest task per datasource
                producer.produce(topic=sparse_topic,
                                value=sparse_message.to_serialized_json(),
                                key=sparse_file.datasource)
                logger.info("Sent message %s to sparse topic %s",
                            sparse_message, sparse_topic)
                # Offset is that of the next message to be consumed
                producer.send_offsets_to_transaction(
                    [TopicPartition(self.topic, partition, offset + 1)],
                    self.consumer.consumer_group_metadata())

    @meter_failures
    @time_calls
    def _upload_filestore(self,
                          key_prefix: str,
                          filestore_dir: Path,
                          manifest: FilestoreManifest
                          ) -> List[UploadedSparseFileInfo]:
        uploaded = []
        bucket = self.sparse_batch_settings.s3_bucket

        # Upload manifest file
        manifest_key = f"{key_prefix}/MANIFEST"
        manifest_file_path = filestore_dir.joinpath("MANIFEST")
        obj = s3_resource.Object(bucket, manifest_key)
        obj.upload_file(str(manifest_file_path))
        logger.info("Uploaded file %s to s3 bucket %s key %s",
                    manifest_file_path, bucket, manifest_key)

        for (datasource, file_info) in manifest.datasource_info.items():
            key = f"{key_prefix}/{datasource}/{file_info.filename}"
            file_path = filestore_dir.joinpath(file_info.filename)
            obj = s3_resource.Object(bucket, key)
            obj.upload_file(str(file_path))
            logger.info("Uploaded file %s to s3 bucket %s key %s",
                        file_path, bucket, key)
            uploaded.append(UploadedSparseFileInfo(bucket=bucket,
                                                   key=key,
                                                   datasource=datasource))
        return uploaded

    @staticmethod
    @meter_failures
    @time_calls
    def _download_json_from_s3(bucket: str, key: str, tmp_dir: Path) -> Path:
        batch_json_local_path = tmp_dir.joinpath(
            DenseBatchConsumer.BATCH_JSON_LOCAL_NAME)
        obj = s3_resource.Object(bucket, key)
        obj.download_file(str(batch_json_local_path))
        logger.info("Downloaded s3 key %s from bucket %s to %s",
                    key, bucket, batch_json_local_path)
        return batch_json_local_path

    @staticmethod
    @meter_failures
    @contextmanager
    def _create_dir_for_message(message: DenseBatchMessage) -> Path:
        random_suffix = ''.join(random.choices(
            string.ascii_letters + string.digits, k=16))
        dir_name = f"{message.cluster_id}_{random_suffix}"
        tmp_dir = Path(DenseBatchConsumer.TMP_ROOT_DIR, dir_name)
        tmp_dir.mkdir()
        logger.info("Created dir %s for message %s", tmp_dir, message)
        try:
            yield tmp_dir
        finally:
            shutil.rmtree(str(tmp_dir), ignore_errors=False)

    @contextmanager
    def _get_transactional_producer(self, partition: int) -> Producer:
        """
        We create one producer per partition. This is required when using
        transactions.

        Prior to KIP-447 being supported each input partition requires
        its own transactional producer.
        """
        # Each producer is identified by its `transactional.id`. Only one
        # producer with a given transactional.id can be active at a time.
        producer = Producer({
            "bootstrap.servers": self.sparse_batch_settings.kafka_brokers,
            "transactional.id": \
                f"dense-consumer-sparse-producer-{partition}",
            # We don't keep producer around for so long, so
            "transaction.timeout.ms": 60000
        })
        # If there are ongoing transactions for this transactional.id, this
        # function waits till those transactions finish
        self._run_kafka_fn_with_retries(
            fn=producer.init_transactions, num_retries=3)
        try:
            self._run_kafka_fn_with_retries(
                fn=producer.begin_transaction, num_retries=3)
            yield producer
            self._run_kafka_fn_with_retries(
                fn=producer.commit_transaction, num_retries=3)
        except KafkaException as e:
            logger.error("Producer transaction failed for partition %d.",
                         partition, exc_info=e)
            kafka_error: KafkaError = e.args[0]
            if kafka_error.txn_requires_abort():
                producer.abort_transaction()
                logger.info("Aborting transaction for partition %d", partition)
            raise e
        finally:
            producer.flush()

    @staticmethod
    def _run_kafka_fn_with_retries(fn: Callable[[], None],
                                   num_retries: int) -> None:
        num_attempt = 0
        last_kafka_exception = None
        while num_attempt < num_retries:
            num_attempt += 1
            logger.info("Running %s. Retry attempt %d of %d",
                        fn.__name__, num_attempt, num_retries)
            try:
                return fn()
            except KafkaException as e:
                last_kafka_exception = e
                kafka_error: KafkaError = e.args[0]
                if kafka_error.retriable(): # pylint: disable=no-else-continue
                    logger.warning("Got retriable error...", exc_info=e)
                    time.sleep(60)
                    continue
                else:
                    raise e
        # If we exhaust all our retries on retriable errors, throw exception
        logger.error("Exhausted retry attempts for %s. Failing ...",
                     fn.__name__, exc_info=last_kafka_exception)
        return last_kafka_exception
