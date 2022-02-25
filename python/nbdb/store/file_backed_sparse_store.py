"""
FileBackedSparseStore
"""
import json
import logging
import pathlib
import os
from dataclasses import dataclass, asdict
from typing import TextIO, Mapping
import gzip

from nbdb.common.data_point import DataPoint
from nbdb.store.sparse_store import SparseStore

logger = logging.getLogger()

MANIFEST_VERSION = 1
MANIFEST_FILENAME = "MANIFEST"
MANIFEST_DS_MAP_KEY = "datasource_map"
MANIFEST_VERSION_MAP_KEY = "version"
MANIFEST_TOTAL_POINTS_PROCESSED = "total_points_processed"

@dataclass
class DatasourceFileInfo:
    """
    Information about a datasource file.

    Since this dataclass is persisted, changes to it must be backward
    compatible.
    """
    filename: str
    points_written: int
    size_bytes: int

    @staticmethod
    def from_dict(info_dict: dict) -> 'DatasourceFileInfo':
        """
        Construct instance from a dictionary.
        """
        return DatasourceFileInfo(
            filename=info_dict["filename"],
            points_written=info_dict["points_written"],
            size_bytes=info_dict["size_bytes"]
        )


@dataclass
class FilestoreManifest:
    """Parsed file store manifest."""
    datasource_info: Mapping[str, DatasourceFileInfo]
    total_points_processed: int

    @property
    def total_points_written(self) -> int:
        """Total points written to the filestore across datasources."""
        return sum([self.datasource_info[ds].points_written for ds in
                    self.datasource_info])

    @property
    def total_size_bytes(self) -> int:
        """Total bytes written to the filestore across datasources."""
        return sum([self.datasource_info[ds].size_bytes for ds in
                    self.datasource_info])


@dataclass
class DatasourceFile:
    """
    Encapsulates a JSON file which contains datapoints for a datasource.
    """
    file: TextIO
    filename: str
    points_written: int

    def to_info(self, filestore_dir: str) -> DatasourceFileInfo:
        """
        Convert instance to DatasourceFileInfo.
        """
        file_size_bytes = \
            pathlib.Path(filestore_dir, self.filename).stat().st_size
        return DatasourceFileInfo(
            filename=self.filename, points_written=self.points_written,
            size_bytes=file_size_bytes)


class FileBackedSparseStore(SparseStore):
    """
    This class implements a file backed SparseStore.

    For each unique datasource, a JSON file is created which contains all
    serialized datapoints for that datasource.
    A MANIFEST file is generated on calling `store.flush()` or `store.close()`,
    which contains a mapping of datasource names to JSON filenames.

    This class is not thread safe, and `write` calls must be made serially.
    """
    def __init__(self, output_dir: str, compress: bool):
        """
        Initialize the object
        :param output_dir: Path of root directory which will be created to
                           store JSON files and the manifest.
        :param compress: If True, compress files using gzip
        """
        FileBackedSparseStore._create_and_validate_directory(output_dir)
        self.total_points_processed = -1
        self.output_dir = output_dir
        self.compress = compress
        """
        Maintain a mapping of unique datasources to the files that contain
        datapoints for that datasource. Note that the filename for a datasource
        need not be the same as the datasource. It must be looked up from the
        MANIFEST file.
        """
        self.datasource_to_file = {}
        self.closed = False

    def set_total_points_processed(self, num: int) -> None:
        """
        Update the total number of points processed to generate this file store.
        """
        self.total_points_processed = num

    def write(self, data_point: DataPoint) -> None:
        """
        Write the datapoint to the sparse store.
        """
        if self.closed:
            raise ValueError("Store is already closed.")
        ds_file = self._get_or_create_file(data_point)
        json_str = data_point.to_druid_json_str()
        ds_file.file.write(json_str + "\n")
        ds_file.points_written += 1

    def flush(self) -> None:
        """
        Flush outstanding datapoints to the sparse store.
        """
        if self.closed:
            raise ValueError("Store is already closed.")
        for ds_file in self.datasource_to_file.values():
            ds_file.file.flush()
        self._write_and_flush_manifest()

    def close(self) -> None:
        """
        Close the store. Once closed, no new datapoints can be written to
        the store.
        """
        self._write_and_flush_manifest()
        for ds_file in self.datasource_to_file.values():
            ds_file.file.close()
        self.closed = True

    @property
    def points_written(self) -> int:
        """
        Get the total number of data points written so far to the store.
        """
        return sum(
            [df.points_written for df in self.datasource_to_file.values()])

    def points_written_for_datasource(self, datasource: str) -> int:
        """
        Get the number of data points written for the specified datasource
        so far.
        """
        if not datasource in self.datasource_to_file:
            return 0
        return self.datasource_to_file[datasource].points_written

    def get_manifest(self) -> FilestoreManifest:
        """
        :return: a map of datasource names to DatasourceFileInfo.
        """
        return FileBackedSparseStore.parse_manifest(self.output_dir)

    @staticmethod
    def parse_manifest(directory: str) -> FilestoreManifest:
        """
        Parses the manifest file and returns FilestoreManifest.

        :param directory: Root directory for the store
        """
        if not os.path.isdir(directory):
            raise ValueError("Directory {} doesn't exist.".format(directory))

        manifest_path = os.path.join(directory, MANIFEST_FILENAME)
        if not os.path.isfile(manifest_path):
            raise ValueError("Could not find manifest at \
                             {}".format(manifest_path))

        with open(manifest_path, 'r') as manifest:
            manifest_dict = json.loads(manifest.read())
            ds_map = manifest_dict[MANIFEST_DS_MAP_KEY]
            ds_info = {}
            for ds_name, info_dict in ds_map.items():
                ds_info[ds_name] = DatasourceFileInfo.from_dict(info_dict)
            total_points_processed = \
                manifest_dict[MANIFEST_TOTAL_POINTS_PROCESSED]
        return FilestoreManifest(
            datasource_info=ds_info,
            total_points_processed=total_points_processed)

    @staticmethod
    def parse_datasources_from_manifest(directory: str
                                        ) -> Mapping[str, DatasourceFileInfo]:
        """
        Parses the manifest file and returns a map of datasource name
        to DatasourceFileInfo.
        :param directory: Root directory for the store
        """

        return FileBackedSparseStore.parse_manifest(directory).datasource_info

    def _get_or_create_file(self, data_point: DataPoint) -> DatasourceFile:
        """
        Get an existing file handle where this data point should go based
        on datasource. If it doesn't exist, create one and add it to the map.
        """
        datasource = data_point.datasource
        if not self.datasource_to_file.get(datasource):
            # Using filename = <datasource>.json for simplicity
            filename = datasource + ".json"
            if self.compress:
                filename = filename + ".gz"
                path = os.path.join(self.output_dir, filename)
                file = gzip.open(path, 'wt', compresslevel=4)
            else:
                path = os.path.join(self.output_dir, filename)
                file = open(path, 'w')
            logger.info("Created JSON file %s", path)
            ds_file = DatasourceFile(file=file, filename=filename, points_written=0)
            self.datasource_to_file[datasource] = ds_file
        return self.datasource_to_file[datasource]

    def _write_and_flush_manifest(self):
        """
        Generate and persist a manifest file containing information
        about all files being tracked by the store. The manifest
        file is written out at <root_store_dir>/MANIFEST.

        This function should be called after all file handles are flushed,
        otherwise, the information about files may not be correct.
        """
        manifest_path = os.path.join(self.output_dir, MANIFEST_FILENAME)
        manifest_dict = {
            MANIFEST_VERSION_MAP_KEY: MANIFEST_VERSION,
            MANIFEST_DS_MAP_KEY: {
                 k: asdict(v.to_info(filestore_dir=self.output_dir)) for k,v in
                 self.datasource_to_file.items()},
            MANIFEST_TOTAL_POINTS_PROCESSED: self.total_points_processed
        }
        with open(manifest_path, 'w') as file:
            manifest_serialized = json.dumps(manifest_dict)
            file.write(manifest_serialized)

    @staticmethod
    def _create_and_validate_directory(directory: str) -> None:
        """
        Create the root directory for the store.
        """
        dir_path = pathlib.Path(directory)
        if dir_path.exists():
            raise ValueError(f"Directory {dir_path} already exists")
        dir_path.mkdir()
