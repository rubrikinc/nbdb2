"""
Unit tests for FileBackedSparseStore.
"""
import json
import os
import random
import shutil
import string
import time
import tempfile
from typing import List, IO
from unittest import TestCase
from contextlib import contextmanager
import gzip
import pytest

from nbdb.common.data_point import DataPoint
from nbdb.store.file_backed_sparse_store import FileBackedSparseStore

def _random_string() -> str:
    length = random.randint(4, 32)
    return ''.join(random.choices(
        string.ascii_letters + string.digits, k=length))

def _generate_data_points(datasource: str, num: int) -> List[DataPoint]:
    results = []
    field = _random_string()
    start_time = time.time_ns()
    for i in range(num):
        data_point = DataPoint(
            datasource=datasource,
            field=field,
            tags={},
            epoch=start_time + i*10**3,
            server_rx_epoch=start_time + i**10**3,
            value=random.randint(0, 2**32))
        results.append(data_point)
    return results

class UncompressedDeps:
    """
    Test FileBackedSparseStore without compression.
    """

    @staticmethod
    def create_store(directory: str) -> FileBackedSparseStore:
        """Create an uncompressed filestore."""
        return FileBackedSparseStore(directory, compress=False)

    @staticmethod
    @contextmanager
    def open_sparse_file(path) -> IO:
        """Open an uncompressed sparse file."""
        file = open(path, 'r')
        try:
            yield file
        finally:
            file.close()

class CompressedDeps:
    """
    Test FileBackedSparseStore with compression.
    """

    @staticmethod
    def create_store(directory: str) -> FileBackedSparseStore:
        """Create a compressed filestore."""
        return FileBackedSparseStore(directory, compress=True)

    @staticmethod
    @contextmanager
    def open_sparse_file(path) -> IO:
        """Open a compressed
        sparse file."""
        file = gzip.open(path, 'rt')
        try:
            yield file
        finally:
            file.close()

# pylint: disable=redefined-outer-name
@pytest.fixture()
def test_dir():
    """Temporary directory resource for tests."""
    test_dir = os.path.join(tempfile.mkdtemp(), "filestore")
    yield test_dir
    shutil.rmtree(test_dir)

@pytest.fixture(params=[CompressedDeps(), UncompressedDeps()])
def deps(request):
    """Dependencies for tests."""
    return request.param

class TestFileBackedSparseStore:
    """
    Base test class for FileBackedSparseStore.
    """

    @staticmethod
    def _verify_sparse_store_json(deps, test_dir, filename: str, num_dps: int) -> None:
        filepath = os.path.join(test_dir, filename)
        with deps.open_sparse_file(filepath) as file:
            lines = file.readlines()
            parsed = [json.loads(line) for line in lines]
            assert len(parsed) == num_dps

    @staticmethod
    def test_empty_store(deps, test_dir):
        """
        Verify operations on empty store.
        """
        sparse_store = deps.create_store(test_dir)
        sparse_store.flush()
        sparse_store.close()

        # Manifest file must exist
        assert os.path.isfile(os.path.join(test_dir, "MANIFEST"))
        assert len(
            FileBackedSparseStore.parse_datasources_from_manifest(
                test_dir)
            ) == 0

        assert sparse_store.points_written == 0

        # Close is idempotent
        sparse_store.close()
        # Flush will fail for closed stores
        with pytest.raises(ValueError):
            sparse_store.flush()
        # Write will fail for closed stores
        data_point, *_ = _generate_data_points("ds", 1)
        with pytest.raises(ValueError):
            sparse_store.write(data_point)
        # Creating another store in the same directory will fail
        with pytest.raises(ValueError):
            FileBackedSparseStore(test_dir, compress=False)

    @staticmethod
    def test_writes(deps, test_dir):
        """
        Verify write operations on file backed sparse store.
        """
        sparse_store = deps.create_store(test_dir)
        ds1_dps = _generate_data_points("ds1", 101)
        ds2_dps = _generate_data_points("ds2", 99)

        for data_point in ds1_dps:
            sparse_store.write(data_point)
        sparse_store.flush()
        ds_map = \
            FileBackedSparseStore.parse_datasources_from_manifest(test_dir)
        assert len(ds_map) == 1
        ds1_info = ds_map["ds1"]
        assert ds1_info.points_written == 101

        # Write ds2_dps
        for data_point in ds2_dps:
            sparse_store.write(data_point)
        sparse_store.flush()
        ds_map = \
            FileBackedSparseStore.parse_datasources_from_manifest(test_dir)
        assert len(ds_map) == 2
        ds2_info = ds_map["ds2"]
        assert ds2_info.points_written == 99
        assert sparse_store.points_written == 200
        # Directory should have a total of 3 files
        assert len(os.listdir(test_dir)) == 3

        sparse_store.close()
        # All files must be closed
        for ds_file in sparse_store.datasource_to_file.values():
            assert ds_file.file.closed
        # Close is idempotent
        sparse_store.close()

        TestFileBackedSparseStore._verify_sparse_store_json(
            deps, test_dir, ds1_info.filename, 101)
        TestFileBackedSparseStore._verify_sparse_store_json(
            deps, test_dir, ds2_info.filename, 99)

        # Manifest can be read after close
        ds_map = \
            FileBackedSparseStore.parse_datasources_from_manifest(test_dir)
        assert len(ds_map) == 2
        # Flush will fail for closed stores
        with pytest.raises(ValueError):
            sparse_store.flush()
        # Write will fail for closed stores
        with pytest.raises(ValueError):
            sparse_store.write(ds1_dps[0])
