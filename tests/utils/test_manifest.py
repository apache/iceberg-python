# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
# pylint: disable=redefined-outer-name,arguments-renamed,fixme
import importlib
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

import fastavro
import pytest

import pyiceberg.manifest as manifest_module
from pyiceberg.avro.codecs import AvroCompressionCodec
from pyiceberg.io import load_file_io
from pyiceberg.io.pyarrow import PyArrowFileIO
from pyiceberg.manifest import (
    DataFile,
    DataFileContent,
    FileFormat,
    ManifestContent,
    ManifestEntry,
    ManifestEntryStatus,
    ManifestFile,
    PartitionFieldSummary,
    _inherit_from_manifest,
    _layout_version_from_field_count,
    _manifests,
    clear_manifest_cache,
    read_manifest_list,
    write_manifest,
    write_manifest_list,
)
from pyiceberg.partitioning import UNPARTITIONED_PARTITION_SPEC, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table.snapshots import Operation, Snapshot, Summary
from pyiceberg.typedef import Record, TableVersion
from pyiceberg.types import IntegerType, NestedField


@pytest.fixture(autouse=True)
def reset_global_manifests_cache() -> None:
    clear_manifest_cache()


def _verify_metadata_with_fastavro(avro_file: str, expected_metadata: dict[str, str]) -> None:
    with open(avro_file, "rb") as f:
        reader = fastavro.reader(f)
        metadata = reader.metadata
        for k, v in expected_metadata.items():
            assert k in metadata
            assert metadata[k] == v


def test_read_manifest_entry(generated_manifest_entry_file: str) -> None:
    manifest = ManifestFile.from_args(
        manifest_path=generated_manifest_entry_file,
        manifest_length=0,
        partition_spec_id=0,
        added_snapshot_id=0,
        sequence_number=0,
        partitions=[],
    )
    manifest_entries = manifest.fetch_manifest_entry(PyArrowFileIO())
    manifest_entry = manifest_entries[0]

    assert manifest_entry.status == ManifestEntryStatus.ADDED
    assert manifest_entry.snapshot_id == 8744736658442914487
    assert manifest_entry.sequence_number == 0
    assert isinstance(manifest_entry.data_file, DataFile)

    data_file = manifest_entry.data_file

    assert data_file.content == DataFileContent.DATA
    assert data_file.file_path == (
        "/home/iceberg/warehouse/nyc/taxis_partitioned/data/VendorID=null/"
        "00000-633-d8a4223e-dc97-45a1-86e1-adaba6e8abd7-00001.parquet"
    )
    assert data_file.file_format == FileFormat.PARQUET
    assert repr(data_file.partition) == "Record[1, 1925]"
    assert data_file.record_count == 19513
    assert data_file.file_size_in_bytes == 388872
    assert data_file.column_sizes == {
        1: 53,
        2: 98153,
        3: 98693,
        4: 53,
        5: 53,
        6: 53,
        7: 17425,
        8: 18528,
        9: 53,
        10: 44788,
        11: 35571,
        12: 53,
        13: 1243,
        14: 2355,
        15: 12750,
        16: 4029,
        17: 110,
        18: 47194,
        19: 2948,
    }
    assert data_file.value_counts == {
        1: 19513,
        2: 19513,
        3: 19513,
        4: 19513,
        5: 19513,
        6: 19513,
        7: 19513,
        8: 19513,
        9: 19513,
        10: 19513,
        11: 19513,
        12: 19513,
        13: 19513,
        14: 19513,
        15: 19513,
        16: 19513,
        17: 19513,
        18: 19513,
        19: 19513,
    }
    assert data_file.null_value_counts == {
        1: 19513,
        2: 0,
        3: 0,
        4: 19513,
        5: 19513,
        6: 19513,
        7: 0,
        8: 0,
        9: 19513,
        10: 0,
        11: 0,
        12: 19513,
        13: 0,
        14: 0,
        15: 0,
        16: 0,
        17: 0,
        18: 0,
        19: 0,
    }
    assert data_file.nan_value_counts == {16: 0, 17: 0, 18: 0, 19: 0, 10: 0, 11: 0, 12: 0, 13: 0, 14: 0, 15: 0}
    assert data_file.lower_bounds == {
        2: b"\x01\x00\x00\x00\x00\x00\x00\x00",
        3: b"\x01\x00\x00\x00\x00\x00\x00\x00",
        7: b"\x03\x00\x00\x00",
        8: b"\x01\x00\x00\x00",
        10: b"\xf6(\\\x8f\xc2\x05S\xc0",
        11: b"\x00\x00\x00\x00\x00\x00\x00\x00",
        13: b"\x00\x00\x00\x00\x00\x00\x00\x00",
        14: b"\x00\x00\x00\x00\x00\x00\xe0\xbf",
        15: b")\\\x8f\xc2\xf5(\x08\xc0",
        16: b"\x00\x00\x00\x00\x00\x00\x00\x00",
        17: b"\x00\x00\x00\x00\x00\x00\x00\x00",
        18: b"\xf6(\\\x8f\xc2\xc5S\xc0",
        19: b"\x00\x00\x00\x00\x00\x00\x04\xc0",
    }
    assert data_file.upper_bounds == {
        2: b"\x06\x00\x00\x00\x00\x00\x00\x00",
        3: b"\x06\x00\x00\x00\x00\x00\x00\x00",
        7: b"\t\x01\x00\x00",
        8: b"\t\x01\x00\x00",
        10: b"\xcd\xcc\xcc\xcc\xcc,_@",
        11: b"\x1f\x85\xebQ\\\xe2\xfe@",
        13: b"\x00\x00\x00\x00\x00\x00\x12@",
        14: b"\x00\x00\x00\x00\x00\x00\xe0?",
        15: b"q=\n\xd7\xa3\xf01@",
        16: b"\x00\x00\x00\x00\x00`B@",
        17: b"333333\xd3?",
        18: b"\x00\x00\x00\x00\x00\x18b@",
        19: b"\x00\x00\x00\x00\x00\x00\x04@",
    }
    assert data_file.key_metadata is None
    assert data_file.split_offsets == [4]
    assert data_file.equality_ids is None
    assert data_file.sort_order_id == 0


def test_read_manifest_list(generated_manifest_file_file_v1: str) -> None:
    input_file = PyArrowFileIO().new_input(generated_manifest_file_file_v1)
    manifest_list = list(read_manifest_list(input_file))[0]

    assert manifest_list.manifest_length == 7989
    assert manifest_list.partition_spec_id == 0
    assert manifest_list.added_snapshot_id == 9182715666859759686
    assert manifest_list.added_files_count == 3
    assert manifest_list.existing_files_count == 0
    assert manifest_list.deleted_files_count == 0

    assert isinstance(manifest_list.partitions, list)

    partitions_summary = manifest_list.partitions[0]
    assert isinstance(partitions_summary, PartitionFieldSummary)

    assert partitions_summary.contains_null is True
    assert partitions_summary.contains_nan is False
    assert partitions_summary.lower_bound == b"\x01\x00\x00\x00"
    assert partitions_summary.upper_bound == b"\x02\x00\x00\x00"

    assert manifest_list.added_rows_count == 237993
    assert manifest_list.existing_rows_count == 0
    assert manifest_list.deleted_rows_count == 0


def test_read_manifest_v1(generated_manifest_file_file_v1: str) -> None:
    io = load_file_io()

    snapshot = Snapshot(
        snapshot_id=25,
        parent_snapshot_id=19,
        timestamp_ms=1602638573590,
        manifest_list=generated_manifest_file_file_v1,
        summary=Summary(Operation.APPEND),
        schema_id=3,
    )
    manifest_list = snapshot.manifests(io)[0]

    assert manifest_list.manifest_length == 7989
    assert manifest_list.partition_spec_id == 0
    assert manifest_list.content == ManifestContent.DATA
    assert manifest_list.sequence_number == 0
    assert manifest_list.min_sequence_number == 0
    assert manifest_list.added_snapshot_id == 9182715666859759686
    assert manifest_list.added_files_count == 3
    assert manifest_list.existing_files_count == 0
    assert manifest_list.deleted_files_count == 0
    assert manifest_list.added_rows_count == 237993
    assert manifest_list.existing_rows_count == 0
    assert manifest_list.deleted_rows_count == 0
    assert manifest_list.key_metadata is None

    assert isinstance(manifest_list.partitions, list)

    partition = manifest_list.partitions[0]

    assert isinstance(partition, PartitionFieldSummary)

    assert partition.contains_null is True
    assert partition.contains_nan is False
    assert partition.lower_bound == b"\x01\x00\x00\x00"
    assert partition.upper_bound == b"\x02\x00\x00\x00"

    entries = manifest_list.fetch_manifest_entry(io)

    assert isinstance(entries, list)

    entry = entries[0]

    assert entry.sequence_number == 0
    assert entry.file_sequence_number == 0
    assert entry.snapshot_id == 8744736658442914487
    assert entry.status == ManifestEntryStatus.ADDED


def test_read_manifest_v2(generated_manifest_file_file_v2: str) -> None:
    io = load_file_io()

    snapshot = Snapshot(
        snapshot_id=25,
        parent_snapshot_id=19,
        timestamp_ms=1602638573590,
        manifest_list=generated_manifest_file_file_v2,
        summary=Summary(Operation.APPEND),
        schema_id=3,
    )
    manifest_list = snapshot.manifests(io)[0]

    assert manifest_list.manifest_length == 7989
    assert manifest_list.partition_spec_id == 0
    assert manifest_list.content == ManifestContent.DELETES
    assert manifest_list.sequence_number == 3
    assert manifest_list.min_sequence_number == 3
    assert manifest_list.added_snapshot_id == 9182715666859759686
    assert manifest_list.added_files_count == 3
    assert manifest_list.existing_files_count == 0
    assert manifest_list.deleted_files_count == 0
    assert manifest_list.added_rows_count == 237993
    assert manifest_list.existing_rows_count == 0
    assert manifest_list.deleted_rows_count == 0
    assert manifest_list.key_metadata is None

    assert isinstance(manifest_list.partitions, list)

    partition = manifest_list.partitions[0]

    assert isinstance(partition, PartitionFieldSummary)

    assert partition.contains_null is True
    assert partition.contains_nan is False
    assert partition.lower_bound == b"\x01\x00\x00\x00"
    assert partition.upper_bound == b"\x02\x00\x00\x00"

    entries = manifest_list.fetch_manifest_entry(io)

    assert isinstance(entries, list)

    entry = entries[0]

    assert entry.sequence_number == 3
    assert entry.file_sequence_number == 3
    assert entry.snapshot_id == 8744736658442914487
    assert entry.status == ManifestEntryStatus.ADDED


def test_read_manifest_cache(generated_manifest_file_file_v2: str) -> None:
    """Test that ManifestFile objects are cached and reused across multiple reads.

    The cache now stores individual ManifestFile objects by their manifest_path,
    rather than caching entire manifest list tuples. This is more memory-efficient
    when multiple manifest lists share overlapping ManifestFile objects.
    """
    io = load_file_io()

    snapshot = Snapshot(
        snapshot_id=25,
        parent_snapshot_id=19,
        timestamp_ms=1602638573590,
        manifest_list=generated_manifest_file_file_v2,
        summary=Summary(Operation.APPEND),
        schema_id=3,
    )

    # Access the manifests property multiple times
    manifests_first_call = snapshot.manifests(io)
    manifests_second_call = snapshot.manifests(io)

    # Ensure that the same manifest list content is returned
    assert manifests_first_call == manifests_second_call

    # Verify that ManifestFile objects are the same instances (cached)
    for mf1, mf2 in zip(manifests_first_call, manifests_second_call, strict=True):
        assert mf1 is mf2, "ManifestFile objects should be the same cached instance"


def test_write_empty_manifest() -> None:
    io = load_file_io()
    test_schema = Schema(NestedField(1, "foo", IntegerType(), False))
    with TemporaryDirectory(ignore_cleanup_errors=True) as tmpdir:
        tmp_avro_file = tmpdir + "/test_write_manifest.avro"

        with pytest.raises(ValueError, match="An empty manifest file has been written"):
            with write_manifest(
                format_version=1,
                spec=UNPARTITIONED_PARTITION_SPEC,
                schema=test_schema,
                output_file=io.new_output(tmp_avro_file),
                snapshot_id=8744736658442914487,
                avro_compression="deflate",
            ) as _:
                pass


@pytest.mark.parametrize("format_version", [1, 2])
@pytest.mark.parametrize("compression", ["null", "deflate", "zstd"])
def test_write_manifest(
    generated_manifest_file_file_v1: str,
    generated_manifest_file_file_v2: str,
    format_version: TableVersion,
    test_schema: Schema,
    test_partition_spec: PartitionSpec,
    compression: AvroCompressionCodec,
) -> None:
    io = load_file_io()
    snapshot = Snapshot(
        snapshot_id=25,
        parent_snapshot_id=19,
        timestamp_ms=1602638573590,
        manifest_list=generated_manifest_file_file_v1 if format_version == 1 else generated_manifest_file_file_v2,
        summary=Summary(Operation.APPEND),
        schema_id=3,
    )
    demo_manifest_file = snapshot.manifests(io)[0]
    manifest_entries = demo_manifest_file.fetch_manifest_entry(io)
    with TemporaryDirectory() as tmpdir:
        tmp_avro_file = tmpdir + "/test_write_manifest.avro"
        output = io.new_output(tmp_avro_file)
        with write_manifest(
            format_version=format_version,
            spec=test_partition_spec,
            schema=test_schema,
            output_file=output,
            snapshot_id=8744736658442914487,
            avro_compression=compression,
        ) as writer:
            for entry in manifest_entries:
                writer.add_entry(entry)
            new_manifest = writer.to_manifest_file()
            with pytest.raises(RuntimeError):
                # It is already closed
                writer.add_entry(manifest_entries[0])

        expected_metadata = {
            "schema": test_schema.model_dump_json(),
            "partition-spec": (
                '[{"source-id":1,"field-id":1000,"transform":"identity","name":"VendorID"},'
                '{"source-id":2,"field-id":1001,"transform":"day","name":"tpep_pickup_day"}]'
            ),
            "partition-spec-id": str(demo_manifest_file.partition_spec_id),
            "format-version": str(format_version),
        }
        _verify_metadata_with_fastavro(
            tmp_avro_file,
            expected_metadata,
        )
        new_manifest_entries = new_manifest.fetch_manifest_entry(io)

        manifest_entry = new_manifest_entries[0]

        assert manifest_entry.status == ManifestEntryStatus.ADDED
        assert manifest_entry.snapshot_id == 8744736658442914487
        assert manifest_entry.sequence_number == (-1 if format_version == 1 else 3)
        assert isinstance(manifest_entry.data_file, DataFile)

        data_file = manifest_entry.data_file

        assert data_file.content == DataFileContent.DATA
        assert data_file.file_path == (
            "/home/iceberg/warehouse/nyc/taxis_partitioned/data/VendorID=null/"
            "00000-633-d8a4223e-dc97-45a1-86e1-adaba6e8abd7-00001.parquet"
        )
        assert data_file.file_format == FileFormat.PARQUET
        assert data_file.partition == Record(1, 1925)
        assert data_file.record_count == 19513
        assert data_file.file_size_in_bytes == 388872
        assert data_file.column_sizes == {
            1: 53,
            2: 98153,
            3: 98693,
            4: 53,
            5: 53,
            6: 53,
            7: 17425,
            8: 18528,
            9: 53,
            10: 44788,
            11: 35571,
            12: 53,
            13: 1243,
            14: 2355,
            15: 12750,
            16: 4029,
            17: 110,
            18: 47194,
            19: 2948,
        }
        assert data_file.value_counts == {
            1: 19513,
            2: 19513,
            3: 19513,
            4: 19513,
            5: 19513,
            6: 19513,
            7: 19513,
            8: 19513,
            9: 19513,
            10: 19513,
            11: 19513,
            12: 19513,
            13: 19513,
            14: 19513,
            15: 19513,
            16: 19513,
            17: 19513,
            18: 19513,
            19: 19513,
        }
        assert data_file.null_value_counts == {
            1: 19513,
            2: 0,
            3: 0,
            4: 19513,
            5: 19513,
            6: 19513,
            7: 0,
            8: 0,
            9: 19513,
            10: 0,
            11: 0,
            12: 19513,
            13: 0,
            14: 0,
            15: 0,
            16: 0,
            17: 0,
            18: 0,
            19: 0,
        }
        assert data_file.nan_value_counts == {16: 0, 17: 0, 18: 0, 19: 0, 10: 0, 11: 0, 12: 0, 13: 0, 14: 0, 15: 0}
        assert data_file.lower_bounds == {
            2: b"\x01\x00\x00\x00\x00\x00\x00\x00",
            3: b"\x01\x00\x00\x00\x00\x00\x00\x00",
            7: b"\x03\x00\x00\x00",
            8: b"\x01\x00\x00\x00",
            10: b"\xf6(\\\x8f\xc2\x05S\xc0",
            11: b"\x00\x00\x00\x00\x00\x00\x00\x00",
            13: b"\x00\x00\x00\x00\x00\x00\x00\x00",
            14: b"\x00\x00\x00\x00\x00\x00\xe0\xbf",
            15: b")\\\x8f\xc2\xf5(\x08\xc0",
            16: b"\x00\x00\x00\x00\x00\x00\x00\x00",
            17: b"\x00\x00\x00\x00\x00\x00\x00\x00",
            18: b"\xf6(\\\x8f\xc2\xc5S\xc0",
            19: b"\x00\x00\x00\x00\x00\x00\x04\xc0",
        }
        assert data_file.upper_bounds == {
            2: b"\x06\x00\x00\x00\x00\x00\x00\x00",
            3: b"\x06\x00\x00\x00\x00\x00\x00\x00",
            7: b"\t\x01\x00\x00",
            8: b"\t\x01\x00\x00",
            10: b"\xcd\xcc\xcc\xcc\xcc,_@",
            11: b"\x1f\x85\xebQ\\\xe2\xfe@",
            13: b"\x00\x00\x00\x00\x00\x00\x12@",
            14: b"\x00\x00\x00\x00\x00\x00\xe0?",
            15: b"q=\n\xd7\xa3\xf01@",
            16: b"\x00\x00\x00\x00\x00`B@",
            17: b"333333\xd3?",
            18: b"\x00\x00\x00\x00\x00\x18b@",
            19: b"\x00\x00\x00\x00\x00\x00\x04@",
        }
        assert data_file.key_metadata is None
        assert data_file.split_offsets == [4]
        assert data_file.equality_ids is None
        assert data_file.sort_order_id == 0


@pytest.mark.parametrize("format_version", [1, 2])
@pytest.mark.parametrize("parent_snapshot_id", [19, None])
@pytest.mark.parametrize("compression", ["null", "deflate"])
def test_write_manifest_list(
    generated_manifest_file_file_v1: str,
    generated_manifest_file_file_v2: str,
    format_version: TableVersion,
    parent_snapshot_id: int | None,
    compression: AvroCompressionCodec,
) -> None:
    io = load_file_io()

    snapshot = Snapshot(
        snapshot_id=25,
        parent_snapshot_id=parent_snapshot_id,
        timestamp_ms=1602638573590,
        manifest_list=generated_manifest_file_file_v1 if format_version == 1 else generated_manifest_file_file_v2,
        summary=Summary(Operation.APPEND),
        schema_id=3,
    )

    demo_manifest_list = snapshot.manifests(io)
    with TemporaryDirectory() as tmp_dir:
        path = tmp_dir + "/manifest-list.avro"
        output = io.new_output(path)
        with write_manifest_list(
            format_version=format_version,
            output_file=output,
            snapshot_id=25,
            parent_snapshot_id=parent_snapshot_id,
            sequence_number=0,
            avro_compression=compression,
        ) as writer:
            writer.add_manifests(demo_manifest_list)
        new_manifest_list = list(read_manifest_list(io.new_input(path)))

        if parent_snapshot_id:
            expected_metadata = {"snapshot-id": "25", "parent-snapshot-id": "19", "format-version": str(format_version)}
        else:
            expected_metadata = {"snapshot-id": "25", "parent-snapshot-id": "null", "format-version": str(format_version)}

        if format_version == 2:
            expected_metadata["sequence-number"] = "0"
        _verify_metadata_with_fastavro(path, expected_metadata)

        manifest_file = new_manifest_list[0]

        assert manifest_file.manifest_length == 7989
        assert manifest_file.partition_spec_id == 0
        assert manifest_file.content == (ManifestContent.DATA if format_version == 1 else ManifestContent.DELETES)
        assert manifest_file.sequence_number == (0 if format_version == 1 else 3)
        assert manifest_file.min_sequence_number == (0 if format_version == 1 else 3)
        assert manifest_file.added_snapshot_id == 9182715666859759686
        assert manifest_file.added_files_count == 3
        assert manifest_file.existing_files_count == 0
        assert manifest_file.deleted_files_count == 0
        assert manifest_file.added_rows_count == 237993
        assert manifest_file.existing_rows_count == 0
        assert manifest_file.deleted_rows_count == 0
        assert manifest_file.key_metadata is None

        assert isinstance(manifest_file.partitions, list)

        partition = manifest_file.partitions[0]

        assert isinstance(partition, PartitionFieldSummary)

        assert partition.contains_null is True
        assert partition.contains_nan is False
        assert partition.lower_bound == b"\x01\x00\x00\x00"
        assert partition.upper_bound == b"\x02\x00\x00\x00"

        entries = manifest_file.fetch_manifest_entry(io)

        assert isinstance(entries, list)

        entry = entries[0]

        assert entry.sequence_number == (0 if format_version == 1 else 3)
        assert entry.file_sequence_number == (0 if format_version == 1 else 3)
        assert entry.snapshot_id == 8744736658442914487
        assert entry.status == ManifestEntryStatus.ADDED


@pytest.mark.parametrize(
    "raw_file_format,expected_file_format",
    [
        ("avro", FileFormat("AVRO")),
        ("AVRO", FileFormat("AVRO")),
        ("parquet", FileFormat("PARQUET")),
        ("PARQUET", FileFormat("PARQUET")),
        ("orc", FileFormat("ORC")),
        ("ORC", FileFormat("ORC")),
        ("NOT_EXISTS", None),
    ],
)
def test_file_format_case_insensitive(raw_file_format: str, expected_file_format: FileFormat) -> None:
    if expected_file_format:
        parsed_file_format = FileFormat(raw_file_format)
        assert parsed_file_format == expected_file_format, (
            f"File format {raw_file_format}: {parsed_file_format} != {expected_file_format}"
        )
    else:
        with pytest.raises(ValueError):
            _ = FileFormat(raw_file_format)


def test_manifest_cache_deduplicates_manifest_files() -> None:
    """Test that the manifest cache deduplicates ManifestFile objects across manifest lists.

    This test verifies the fix for https://github.com/apache/iceberg-python/issues/2325

    The issue was that when caching manifest lists by their path, overlapping ManifestFile
    objects were duplicated. For example:
    - ManifestList1: (ManifestFile1)
    - ManifestList2: (ManifestFile1, ManifestFile2)
    - ManifestList3: (ManifestFile1, ManifestFile2, ManifestFile3)

    With the old approach, ManifestFile1 was stored 3 times in the cache.
    With the new approach, ManifestFile objects are cached individually by their
    manifest_path, so ManifestFile1 is stored only once and reused.
    """
    io = PyArrowFileIO()

    with TemporaryDirectory() as tmp_dir:
        # Create three manifest files to simulate manifests created during appends
        manifest1_path = f"{tmp_dir}/manifest1.avro"
        manifest2_path = f"{tmp_dir}/manifest2.avro"
        manifest3_path = f"{tmp_dir}/manifest3.avro"

        schema = Schema(NestedField(field_id=1, name="id", field_type=IntegerType(), required=True))
        spec = UNPARTITIONED_PARTITION_SPEC

        # Create manifest file 1
        with write_manifest(
            format_version=2,
            spec=spec,
            schema=schema,
            output_file=io.new_output(manifest1_path),
            snapshot_id=1,
            avro_compression="zstandard",
        ) as writer:
            data_file1 = DataFile.from_args(
                content=DataFileContent.DATA,
                file_path=f"{tmp_dir}/data1.parquet",
                file_format=FileFormat.PARQUET,
                partition=Record(),
                record_count=100,
                file_size_in_bytes=1000,
            )
            writer.add_entry(
                ManifestEntry.from_args(
                    status=ManifestEntryStatus.ADDED,
                    snapshot_id=1,
                    data_file=data_file1,
                )
            )
        manifest_file1 = writer.to_manifest_file()

        # Create manifest file 2
        with write_manifest(
            format_version=2,
            spec=spec,
            schema=schema,
            output_file=io.new_output(manifest2_path),
            snapshot_id=2,
            avro_compression="zstandard",
        ) as writer:
            data_file2 = DataFile.from_args(
                content=DataFileContent.DATA,
                file_path=f"{tmp_dir}/data2.parquet",
                file_format=FileFormat.PARQUET,
                partition=Record(),
                record_count=200,
                file_size_in_bytes=2000,
            )
            writer.add_entry(
                ManifestEntry.from_args(
                    status=ManifestEntryStatus.ADDED,
                    snapshot_id=2,
                    data_file=data_file2,
                )
            )
        manifest_file2 = writer.to_manifest_file()

        # Create manifest file 3
        with write_manifest(
            format_version=2,
            spec=spec,
            schema=schema,
            output_file=io.new_output(manifest3_path),
            snapshot_id=3,
            avro_compression="zstandard",
        ) as writer:
            data_file3 = DataFile.from_args(
                content=DataFileContent.DATA,
                file_path=f"{tmp_dir}/data3.parquet",
                file_format=FileFormat.PARQUET,
                partition=Record(),
                record_count=300,
                file_size_in_bytes=3000,
            )
            writer.add_entry(
                ManifestEntry.from_args(
                    status=ManifestEntryStatus.ADDED,
                    snapshot_id=3,
                    data_file=data_file3,
                )
            )
        manifest_file3 = writer.to_manifest_file()

        # Create manifest list 1: contains only manifest1
        manifest_list1_path = f"{tmp_dir}/manifest-list1.avro"
        with write_manifest_list(
            format_version=2,
            output_file=io.new_output(manifest_list1_path),
            snapshot_id=1,
            parent_snapshot_id=None,
            sequence_number=1,
            avro_compression="zstandard",
        ) as list_writer:
            list_writer.add_manifests([manifest_file1])

        # Create manifest list 2: contains manifest1 and manifest2 (overlapping manifest1)
        manifest_list2_path = f"{tmp_dir}/manifest-list2.avro"
        with write_manifest_list(
            format_version=2,
            output_file=io.new_output(manifest_list2_path),
            snapshot_id=2,
            parent_snapshot_id=1,
            sequence_number=2,
            avro_compression="zstandard",
        ) as list_writer:
            list_writer.add_manifests([manifest_file1, manifest_file2])

        # Create manifest list 3: contains all three manifests (overlapping manifest1 and manifest2)
        manifest_list3_path = f"{tmp_dir}/manifest-list3.avro"
        with write_manifest_list(
            format_version=2,
            output_file=io.new_output(manifest_list3_path),
            snapshot_id=3,
            parent_snapshot_id=2,
            sequence_number=3,
            avro_compression="zstandard",
        ) as list_writer:
            list_writer.add_manifests([manifest_file1, manifest_file2, manifest_file3])

        # Read all three manifest lists
        manifests1 = _manifests(io, manifest_list1_path)
        manifests2 = _manifests(io, manifest_list2_path)
        manifests3 = _manifests(io, manifest_list3_path)

        # Verify the manifest files have the expected paths
        assert len(manifests1) == 1
        assert len(manifests2) == 2
        assert len(manifests3) == 3

        # Verify that ManifestFile objects with the same manifest_path are the same object (identity)
        # This is the key assertion - if caching works correctly, the same ManifestFile
        # object should be reused instead of creating duplicates

        # manifest_file1 appears in all three lists - should be the same object
        assert manifests1[0] is manifests2[0], "ManifestFile1 should be the same object instance across manifest lists"
        assert manifests2[0] is manifests3[0], "ManifestFile1 should be the same object instance across manifest lists"

        # manifest_file2 appears in lists 2 and 3 - should be the same object
        assert manifests2[1] is manifests3[1], "ManifestFile2 should be the same object instance across manifest lists"

        # Verify cache size - should only have 3 unique ManifestFile objects
        # instead of 1 + 2 + 3 = 6 objects as with the old approach
        assert len(manifest_module._manifest_cache) == 3, (
            f"Cache should contain exactly 3 unique ManifestFile objects, but has {len(manifest_module._manifest_cache)}"
        )


def test_manifest_cache_efficiency_with_many_overlapping_lists() -> None:
    """Test that the manifest cache remains efficient with many overlapping manifest lists.

    This simulates the scenario from GitHub issue #2325 where many appends create
    manifest lists that increasingly overlap.
    """
    io = PyArrowFileIO()

    with TemporaryDirectory() as tmp_dir:
        schema = Schema(NestedField(field_id=1, name="id", field_type=IntegerType(), required=True))
        spec = UNPARTITIONED_PARTITION_SPEC

        num_manifests = 10
        manifest_files = []

        # Create N manifest files
        for i in range(num_manifests):
            manifest_path = f"{tmp_dir}/manifest{i}.avro"
            with write_manifest(
                format_version=2,
                spec=spec,
                schema=schema,
                output_file=io.new_output(manifest_path),
                snapshot_id=i + 1,
                avro_compression="zstandard",
            ) as writer:
                data_file = DataFile.from_args(
                    content=DataFileContent.DATA,
                    file_path=f"{tmp_dir}/data{i}.parquet",
                    file_format=FileFormat.PARQUET,
                    partition=Record(),
                    record_count=100 * (i + 1),
                    file_size_in_bytes=1000 * (i + 1),
                )
                writer.add_entry(
                    ManifestEntry.from_args(
                        status=ManifestEntryStatus.ADDED,
                        snapshot_id=i + 1,
                        data_file=data_file,
                    )
                )
            manifest_files.append(writer.to_manifest_file())

        # Create N manifest lists, each containing an increasing number of manifests
        # list[i] contains manifests[0:i+1]
        manifest_list_paths = []
        for i in range(num_manifests):
            list_path = f"{tmp_dir}/manifest-list{i}.avro"
            with write_manifest_list(
                format_version=2,
                output_file=io.new_output(list_path),
                snapshot_id=i + 1,
                parent_snapshot_id=i if i > 0 else None,
                sequence_number=i + 1,
                avro_compression="zstandard",
            ) as list_writer:
                list_writer.add_manifests(manifest_files[: i + 1])
            manifest_list_paths.append(list_path)

        # Read all manifest lists
        all_results = []
        for path in manifest_list_paths:
            result = _manifests(io, path)
            all_results.append(result)

        # With the old cache approach, we would have:
        # 1 + 2 + 3 + ... + N = N*(N+1)/2 ManifestFile objects in memory
        # With the new approach, we should have exactly N objects

        # Verify cache has exactly N unique entries
        assert len(manifest_module._manifest_cache) == num_manifests, (
            f"Cache should contain exactly {num_manifests} ManifestFile objects, "
            f"but has {len(manifest_module._manifest_cache)}. "
            f"Old approach would have {num_manifests * (num_manifests + 1) // 2} objects."
        )

        # Verify object identity - all references to the same manifest should be the same object
        for i in range(num_manifests):
            # Find all references to this manifest across all results
            references = []
            for j, result in enumerate(all_results):
                if j >= i:  # This manifest should be in lists from i onwards
                    references.append(result[i])

            # All references should be the same object
            if len(references) > 1:
                for ref in references[1:]:
                    assert ref is references[0], f"All references to manifest {i} should be the same object instance"


@pytest.mark.parametrize("format_version", [1, 2])
def test_manifest_writer_tell(format_version: TableVersion) -> None:
    io = load_file_io()
    test_schema = Schema(NestedField(1, "foo", IntegerType(), False))

    with TemporaryDirectory() as tmpdir:
        output_file = io.new_output(f"{tmpdir}/test-manifest.avro")
        with write_manifest(
            format_version=format_version,
            spec=UNPARTITIONED_PARTITION_SPEC,
            schema=test_schema,
            output_file=output_file,
            snapshot_id=1,
            avro_compression="null",
        ) as writer:
            initial_bytes = writer.tell()
            data_file = DataFile.from_args(
                content=DataFileContent.DATA,
                file_path=f"{tmpdir}/data.parquet",
                file_format=FileFormat.PARQUET,
                partition=Record(),
                record_count=100,
                file_size_in_bytes=1000,
            )
            entry = ManifestEntry.from_args(
                status=ManifestEntryStatus.ADDED,
                snapshot_id=1,
                data_file=data_file,
            )
            writer.add_entry(entry)
            after_entry_bytes = writer.tell()

            assert after_entry_bytes > initial_bytes, "Bytes should increase after adding entry"


@pytest.mark.parametrize("format_version", [1, 2])
def test_write_manifest_min_sequence_number_zero(format_version: TableVersion) -> None:
    # A data sequence number of 0 is a legitimate min for a live file (e.g. files from a
    # v1 table or the initial commit of a v2 table). It must be preserved in the manifest,
    # not collapsed to UNASSIGNED_SEQ (-1), which would let a merge/compaction silently
    # raise the manifest's min data sequence number. This mirrors the Java reference, which
    # only falls back to UNASSIGNED_SEQ when the min is unset (null).
    io = load_file_io()
    test_schema = Schema(NestedField(1, "foo", IntegerType(), False))

    with TemporaryDirectory() as tmpdir:
        output_file = io.new_output(f"{tmpdir}/test-manifest.avro")
        with write_manifest(
            format_version=format_version,
            spec=UNPARTITIONED_PARTITION_SPEC,
            schema=test_schema,
            output_file=output_file,
            snapshot_id=1,
            avro_compression="null",
        ) as writer:
            data_file = DataFile.from_args(
                content=DataFileContent.DATA,
                file_path=f"{tmpdir}/data.parquet",
                file_format=FileFormat.PARQUET,
                partition=Record(),
                record_count=100,
                file_size_in_bytes=1000,
            )
            writer.existing(
                ManifestEntry.from_args(
                    status=ManifestEntryStatus.EXISTING,
                    snapshot_id=1,
                    sequence_number=0,
                    file_sequence_number=0,
                    data_file=data_file,
                )
            )
            manifest_file = writer.to_manifest_file()

    assert manifest_file.min_sequence_number == 0


def test_inherit_from_manifest_snapshot_id() -> None:
    entry = ManifestEntry.from_args(
        status=ManifestEntryStatus.ADDED,
        snapshot_id=None,
        sequence_number=None,
        file_sequence_number=None,
        data_file=DataFile.from_args(
            content=DataFileContent.DATA,
            file_path="s3://bucket/data/file.parquet",
            file_format=FileFormat.PARQUET,
            partition=Record(),
            record_count=100,
            file_size_in_bytes=1024,
        ),
    )

    manifest = ManifestFile.from_args(
        manifest_path="s3://bucket/metadata/manifest.avro",
        manifest_length=1000,
        partition_spec_id=0,
        content=ManifestContent.DATA,
        sequence_number=1,
        min_sequence_number=1,
        added_snapshot_id=3051729675574597004,
        added_files_count=1,
        existing_files_count=0,
        deleted_files_count=0,
        added_rows_count=100,
        existing_rows_count=0,
        deleted_rows_count=0,
    )

    result = _inherit_from_manifest(entry, manifest)

    assert result.status == ManifestEntryStatus.ADDED
    assert result.snapshot_id == 3051729675574597004
    assert result.sequence_number == 1
    assert result.file_sequence_number == 1


def _create_test_manifest_list(module: Any, io: PyArrowFileIO, tmp_dir: str, name: str, snapshot_id: int) -> str:
    schema = Schema(NestedField(field_id=1, name="id", field_type=IntegerType(), required=True))
    spec = UNPARTITIONED_PARTITION_SPEC

    manifest_path = f"{tmp_dir}/manifest-{name}.avro"
    with module.write_manifest(
        format_version=2,
        spec=spec,
        schema=schema,
        output_file=io.new_output(manifest_path),
        snapshot_id=snapshot_id,
        avro_compression="zstandard",
    ) as writer:
        data_file = module.DataFile.from_args(
            content=module.DataFileContent.DATA,
            file_path=f"{tmp_dir}/data-{name}.parquet",
            file_format=module.FileFormat.PARQUET,
            partition=Record(),
            record_count=100,
            file_size_in_bytes=1000,
        )
        writer.add_entry(
            module.ManifestEntry.from_args(
                status=module.ManifestEntryStatus.ADDED,
                snapshot_id=snapshot_id,
                data_file=data_file,
            )
        )
    manifest_file = writer.to_manifest_file()

    list_path = f"{tmp_dir}/manifest-list-{name}.avro"
    with module.write_manifest_list(
        format_version=2,
        output_file=io.new_output(list_path),
        snapshot_id=snapshot_id,
        parent_snapshot_id=snapshot_id - 1 if snapshot_id > 1 else None,
        sequence_number=snapshot_id,
        avro_compression="zstandard",
    ) as list_writer:
        list_writer.add_manifests([manifest_file])

    return list_path


def test_clear_manifest_cache() -> None:
    """Test that clear_manifest_cache() clears cache entries while keeping cache enabled."""
    io = PyArrowFileIO()

    with TemporaryDirectory() as tmp_dir:
        list_path = _create_test_manifest_list(manifest_module, io, tmp_dir, name="clear", snapshot_id=1)

        # Populate the cache
        _manifests(io, list_path)

        # Verify cache has entries
        assert len(manifest_module._manifest_cache) > 0, "Cache should have entries after reading manifests"

        # Clear the cache
        clear_manifest_cache()

        # Verify cache is empty but still enabled
        assert len(manifest_module._manifest_cache) == 0, "Cache should be empty after clear"


def test_manifest_cache_can_be_disabled_with_size_zero(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test that manifest-cache-size=0 disables caching."""
    monkeypatch.setenv("PYICEBERG_MANIFEST_CACHE_SIZE", "0")
    importlib.reload(manifest_module)

    try:
        assert manifest_module._manifest_cache.maxsize == 0
        assert len(manifest_module._manifest_cache) == 0

        io = PyArrowFileIO()

        with TemporaryDirectory() as tmp_dir:
            list_path = _create_test_manifest_list(manifest_module, io, tmp_dir, name="disabled", snapshot_id=1)

            manifests_first_call = manifest_module._manifests(io, list_path)
            manifests_second_call = manifest_module._manifests(io, list_path)

            assert len(manifest_module._manifest_cache) == 0
            assert manifests_first_call[0] is not manifests_second_call[0]
    finally:
        monkeypatch.delenv("PYICEBERG_MANIFEST_CACHE_SIZE", raising=False)
        importlib.reload(manifest_module)


def test_manifest_cache_respects_positive_env_size(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test that a positive manifest-cache-size enables a bounded cache."""
    monkeypatch.setenv("PYICEBERG_MANIFEST_CACHE_SIZE", "1")
    importlib.reload(manifest_module)

    try:
        assert manifest_module._manifest_cache.maxsize == 1

        io = PyArrowFileIO()

        with TemporaryDirectory() as tmp_dir:
            first_list_path = _create_test_manifest_list(manifest_module, io, tmp_dir, name="first", snapshot_id=1)
            second_list_path = _create_test_manifest_list(manifest_module, io, tmp_dir, name="second", snapshot_id=2)

            manifests_first_call = manifest_module._manifests(io, first_list_path)
            manifests_second_call = manifest_module._manifests(io, first_list_path)

            assert manifests_first_call[0] is manifests_second_call[0]
            assert len(manifest_module._manifest_cache) == 1

            manifest_module._manifests(io, second_list_path)

            assert len(manifest_module._manifest_cache) == 1
    finally:
        monkeypatch.delenv("PYICEBERG_MANIFEST_CACHE_SIZE", raising=False)
        importlib.reload(manifest_module)


def test_manifest_cache_reads_size_from_configuration_file(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Test that manifest-cache-size can be loaded from .pyiceberg.yaml."""
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    (config_dir / ".pyiceberg.yaml").write_text("manifest-cache-size: 2\n", encoding="utf-8")

    monkeypatch.delenv("PYICEBERG_MANIFEST_CACHE_SIZE", raising=False)
    monkeypatch.setenv("PYICEBERG_HOME", str(config_dir))
    importlib.reload(manifest_module)

    try:
        assert manifest_module._manifest_cache.maxsize == 2

        io = PyArrowFileIO()

        with TemporaryDirectory() as tmp_dir:
            first_list_path = _create_test_manifest_list(manifest_module, io, tmp_dir, name="first", snapshot_id=1)
            second_list_path = _create_test_manifest_list(manifest_module, io, tmp_dir, name="second", snapshot_id=2)
            third_list_path = _create_test_manifest_list(manifest_module, io, tmp_dir, name="third", snapshot_id=3)

            manifest_module._manifests(io, first_list_path)
            manifest_module._manifests(io, second_list_path)
            manifest_module._manifests(io, third_list_path)

            assert len(manifest_module._manifest_cache) == 2
    finally:
        monkeypatch.delenv("PYICEBERG_HOME", raising=False)
        importlib.reload(manifest_module)


def test_invalid_manifest_cache_size_raises_value_error(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test that invalid manifest-cache-size values raise a helpful error."""
    monkeypatch.setenv("PYICEBERG_MANIFEST_CACHE_SIZE", "not-an-int")

    try:
        with pytest.raises(ValueError, match="manifest-cache-size should be an integer or left unset"):
            importlib.reload(manifest_module)
    finally:
        monkeypatch.delenv("PYICEBERG_MANIFEST_CACHE_SIZE", raising=False)
        importlib.reload(manifest_module)


def test_negative_manifest_cache_size_raises_value_error(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test that negative manifest-cache-size values raise a helpful error."""
    monkeypatch.setenv("PYICEBERG_MANIFEST_CACHE_SIZE", "-1")

    try:
        with pytest.raises(ValueError, match="manifest-cache-size should be a non-negative integer or left unset"):
            importlib.reload(manifest_module)
    finally:
        monkeypatch.delenv("PYICEBERG_MANIFEST_CACHE_SIZE", raising=False)
        importlib.reload(manifest_module)


@pytest.mark.parametrize("compression", ["null", "deflate"])
def test_write_manifest_v3(compression: AvroCompressionCodec) -> None:
    io = load_file_io()
    test_schema = Schema(NestedField(1, "foo", IntegerType(), False))

    v3_data_file = DataFile.from_args(
        _table_format_version=3,
        content=DataFileContent.DATA,
        file_path="/data/file-v3.parquet",
        file_format=FileFormat.PARQUET,
        partition=Record(),
        record_count=100,
        file_size_in_bytes=1024,
        column_sizes={1: 10},
        value_counts={1: 100},
        null_value_counts={1: 0},
        split_offsets=[4],
        sort_order_id=1,
        first_row_id=1000,
    )
    v2_data_file = DataFile.from_args(
        content=DataFileContent.DATA,
        file_path="/data/file-v2.parquet",
        file_format=FileFormat.PARQUET,
        partition=Record(),
        record_count=50,
        file_size_in_bytes=512,
    )

    with TemporaryDirectory() as tmp_dir:
        path = tmp_dir + "/manifest-v3.avro"
        with write_manifest(
            format_version=3,
            spec=UNPARTITIONED_PARTITION_SPEC,
            schema=test_schema,
            output_file=io.new_output(path),
            snapshot_id=25,
            avro_compression=compression,
        ) as writer:
            writer.add(ManifestEntry.from_args(status=ManifestEntryStatus.ADDED, snapshot_id=25, data_file=v3_data_file))
            # a data file bound to the default (V2) layout is rebound to the V3 layout
            writer.add(ManifestEntry.from_args(status=ManifestEntryStatus.ADDED, snapshot_id=25, data_file=v2_data_file))

        _verify_metadata_with_fastavro(path, {"format-version": "3", "content": "data"})

        with open(path, "rb") as f:
            entries = list(fastavro.reader(f))

        assert len(entries) == 2
        assert entries[0]["data_file"]["first_row_id"] == 1000
        assert entries[1]["data_file"]["first_row_id"] is None
        for entry in entries:
            for v3_field in ("first_row_id", "referenced_data_file", "content_offset", "content_size_in_bytes"):
                assert v3_field in entry["data_file"]
        assert entries[0]["data_file"]["sort_order_id"] == 1
        assert entries[0]["data_file"]["split_offsets"] == [4]

        # the V3 manifest must remain readable by the current reader
        read_entries = writer.to_manifest_file().fetch_manifest_entry(io, discard_deleted=False)
        assert len(read_entries) == 2
        assert read_entries[0].status == ManifestEntryStatus.ADDED
        assert read_entries[0].data_file.file_path == "/data/file-v3.parquet"
        assert read_entries[0].data_file.record_count == 100
        assert read_entries[0].data_file.column_sizes == {1: 10}
        assert read_entries[0].data_file.value_counts == {1: 100}
        assert read_entries[0].data_file.sort_order_id == 1
        assert read_entries[1].data_file.file_path == "/data/file-v2.parquet"
        # the reader must expose the V3-only data file fields rather than dropping them, otherwise a
        # V3 manifest cannot round-trip through the reader
        assert read_entries[0].data_file.first_row_id == 1000
        assert read_entries[1].data_file.first_row_id is None
        for read_entry in read_entries:
            for v3_field in ("first_row_id", "referenced_data_file", "content_offset", "content_size_in_bytes"):
                assert hasattr(read_entry.data_file, v3_field)


def test_write_manifest_v3_round_trips_deletion_vector_fields() -> None:
    """The V3-only referenced_data_file/content_offset/content_size_in_bytes fields must survive a
    write/read round trip with their actual values, not just be present-but-empty on the reader.
    """
    io = load_file_io()
    test_schema = Schema(NestedField(1, "foo", IntegerType(), False))

    dv_data_file = DataFile.from_args(
        _table_format_version=3,
        content=DataFileContent.POSITION_DELETES,
        file_path="/data/dv-1.puffin",
        file_format=FileFormat.PUFFIN,
        partition=Record(),
        record_count=5,
        file_size_in_bytes=256,
        referenced_data_file="/data/file-v3.parquet",
        content_offset=128,
        content_size_in_bytes=64,
    )

    with TemporaryDirectory() as tmp_dir:
        path = tmp_dir + "/manifest-v3-dv.avro"
        with write_manifest(
            format_version=3,
            spec=UNPARTITIONED_PARTITION_SPEC,
            schema=test_schema,
            output_file=io.new_output(path),
            snapshot_id=25,
            avro_compression="null",
        ) as writer:
            writer.add(ManifestEntry.from_args(status=ManifestEntryStatus.ADDED, snapshot_id=25, data_file=dv_data_file))

        read_entries = writer.to_manifest_file().fetch_manifest_entry(io, discard_deleted=False)
        assert len(read_entries) == 1
        read_data_file = read_entries[0].data_file
        assert read_data_file.referenced_data_file == "/data/file-v3.parquet"
        assert read_data_file.content_offset == 128
        assert read_data_file.content_size_in_bytes == 64


def test_layout_version_from_field_count() -> None:
    layouts = {
        1: Schema(NestedField(1, "a", IntegerType(), False)),
        2: Schema(NestedField(1, "a", IntegerType(), False), NestedField(2, "b", IntegerType(), False)),
    }
    assert _layout_version_from_field_count(layouts, 1) == 1
    assert _layout_version_from_field_count(layouts, 2) == 2

    with pytest.raises(ValueError, match="Cannot determine layout version"):
        _layout_version_from_field_count(layouts, 3)


def test_layout_version_from_field_count_rejects_ambiguous_layouts() -> None:
    # two versions with the same field count cannot be told apart by field count alone
    layouts = {
        1: Schema(NestedField(1, "a", IntegerType(), False)),
        2: Schema(NestedField(1, "a", IntegerType(), False)),
    }
    with pytest.raises(ValueError, match="Ambiguous layout"):
        _layout_version_from_field_count(layouts, 1)


def test_write_manifest_v3_rebinds_v1_data_file() -> None:
    """A V1-bound data file (fewer fields than V2) must be rebindable to the V3 layout.

    The re-wrap logic recovers the source layout from the field count, so a V1 data file is zipped
    against the V1 fields rather than a fixed V2 layout, which would raise a confusing strict-zip
    length mismatch.
    """
    io = load_file_io()
    test_schema = Schema(NestedField(1, "foo", IntegerType(), False))

    v1_data_file = DataFile.from_args(
        _table_format_version=1,
        file_path="/data/file-v1.parquet",
        file_format=FileFormat.PARQUET,
        partition=Record(),
        record_count=10,
        file_size_in_bytes=128,
    )

    with TemporaryDirectory() as tmp_dir:
        path = tmp_dir + "/manifest-v3-from-v1.avro"
        with write_manifest(
            format_version=3,
            spec=UNPARTITIONED_PARTITION_SPEC,
            schema=test_schema,
            output_file=io.new_output(path),
            snapshot_id=25,
            avro_compression="null",
        ) as writer:
            writer.add(ManifestEntry.from_args(status=ManifestEntryStatus.ADDED, snapshot_id=25, data_file=v1_data_file))

        read_entries = writer.to_manifest_file().fetch_manifest_entry(io, discard_deleted=False)
        assert len(read_entries) == 1
        assert read_entries[0].data_file.file_path == "/data/file-v1.parquet"
        assert read_entries[0].data_file.record_count == 10
        # V3-only field is filled with its default when rebinding a V1 data file
        assert read_entries[0].data_file.first_row_id is None


@pytest.mark.parametrize("compression", ["null", "deflate"])
def test_write_manifest_list_v3_assigns_first_row_id(compression: AvroCompressionCodec) -> None:
    io = load_file_io()

    def manifest(path: str, content: ManifestContent, first_row_id: int | None = None, **counts: int) -> ManifestFile:
        args: dict[str, Any] = {
            "manifest_path": path,
            "manifest_length": 100,
            "partition_spec_id": 0,
            "content": content,
            "sequence_number": 1,
            "min_sequence_number": 1,
            "added_snapshot_id": 25,
            "added_files_count": 1,
            "existing_files_count": 1,
            "deleted_files_count": 0,
            "added_rows_count": counts.get("added", 0),
            "existing_rows_count": counts.get("existing", 0),
            "deleted_rows_count": 0,
        }
        if first_row_id is not None:
            return ManifestFile.from_args(_table_format_version=3, first_row_id=first_row_id, **args)
        # bound to the default (V2) layout to exercise rebinding in the writer
        return ManifestFile.from_args(**args)

    unassigned_data = manifest("/m1.avro", ManifestContent.DATA, added=100, existing=25)
    preserved_data = manifest("/m2.avro", ManifestContent.DATA, first_row_id=77, added=10)
    deletes = manifest("/m3.avro", ManifestContent.DELETES, added=10)
    second_unassigned_data = manifest("/m4.avro", ManifestContent.DATA, added=5)

    with TemporaryDirectory() as tmp_dir:
        path = tmp_dir + "/manifest-list-v3.avro"
        with write_manifest_list(
            format_version=3,
            output_file=io.new_output(path),
            snapshot_id=25,
            parent_snapshot_id=19,
            sequence_number=2,
            avro_compression=compression,
            first_row_id=1000,
        ) as writer:
            writer.add_manifests([unassigned_data, preserved_data, deletes, second_unassigned_data])

        # 1000 + (100 + 25) + (5): assigned manifests advance by added + existing rows
        assert writer.next_row_id == 1130  # type: ignore[attr-defined]

        _verify_metadata_with_fastavro(
            path,
            {
                "snapshot-id": "25",
                "parent-snapshot-id": "19",
                "sequence-number": "2",
                "first-row-id": "1000",
                "format-version": "3",
            },
        )

        with open(path, "rb") as f:
            records = list(fastavro.reader(f))

        assert [r["first_row_id"] for r in records] == [1000, 77, None, 1125]

        # the V3 manifest list must remain readable by the current reader
        read_back = list(read_manifest_list(io.new_input(path)))
        assert [m.manifest_path for m in read_back] == ["/m1.avro", "/m2.avro", "/m3.avro", "/m4.avro"]
        assert read_back[0].added_rows_count == 100
        assert read_back[0].existing_rows_count == 25
        assert read_back[2].content == ManifestContent.DELETES
        # the reader must expose the assigned first_row_id values rather than dropping them, otherwise a
        # V3 manifest list cannot round-trip through the reader
        assert [m.first_row_id for m in read_back] == [1000, 77, None, 1125]

        # re-writing the manifests read back from the V3 list must preserve their already-assigned
        # first_row_id values instead of reassigning them
        rewritten_path = tmp_dir + "/manifest-list-v3-rewritten.avro"
        with write_manifest_list(
            format_version=3,
            output_file=io.new_output(rewritten_path),
            snapshot_id=25,
            parent_snapshot_id=19,
            sequence_number=2,
            avro_compression=compression,
            first_row_id=2000,
        ) as rewriter:
            rewriter.add_manifests(read_back)

        rewritten = list(read_manifest_list(io.new_input(rewritten_path)))
        # data manifests keep their prior first_row_id; the delete manifest stays None
        assert [m.first_row_id for m in rewritten] == [1000, 77, None, 1125]


def test_write_manifest_list_v3_requires_first_row_id() -> None:
    io = load_file_io()
    with TemporaryDirectory() as tmp_dir:
        with pytest.raises(ValueError, match="First-row-id is required for V3 tables"):
            write_manifest_list(
                format_version=3,
                output_file=io.new_output(tmp_dir + "/manifest-list.avro"),
                snapshot_id=25,
                parent_snapshot_id=19,
                sequence_number=2,
                avro_compression="null",
                first_row_id=None,
            )


@pytest.mark.parametrize("compression", ["null", "deflate"])
def test_write_manifest_v3_carries_first_row_id(compression: AvroCompressionCodec) -> None:
    io = load_file_io()
    test_schema = Schema(NestedField(1, "foo", IntegerType(), False))
    data_file = DataFile.from_args(
        content=DataFileContent.DATA,
        file_path="/data/file.parquet",
        file_format=FileFormat.PARQUET,
        partition=Record(),
        record_count=100,
        file_size_in_bytes=1024,
    )

    with TemporaryDirectory() as tmp_dir:
        with write_manifest(
            format_version=3,
            spec=UNPARTITIONED_PARTITION_SPEC,
            schema=test_schema,
            output_file=io.new_output(tmp_dir + "/manifest-rewrite.avro"),
            snapshot_id=25,
            avro_compression=compression,
            first_row_id=100,
        ) as writer:
            writer.add(ManifestEntry.from_args(status=ManifestEntryStatus.ADDED, snapshot_id=25, data_file=data_file))

        manifest_file = writer.to_manifest_file()
        assert manifest_file.first_row_id == 100

        # a manifest list writer must preserve the carried first_row_id and not advance next_row_id for it
        path = tmp_dir + "/manifest-list.avro"
        with write_manifest_list(
            format_version=3,
            output_file=io.new_output(path),
            snapshot_id=25,
            parent_snapshot_id=19,
            sequence_number=2,
            avro_compression=compression,
            first_row_id=1000,
        ) as list_writer:
            list_writer.add_manifests([manifest_file])
        assert list_writer.next_row_id == 1000  # type: ignore[attr-defined]

        with open(path, "rb") as f:
            records = list(fastavro.reader(f))
        assert [r["first_row_id"] for r in records] == [100]


def test_write_manifest_first_row_id_requires_v3() -> None:
    io = load_file_io()
    test_schema = Schema(NestedField(1, "foo", IntegerType(), False))
    with TemporaryDirectory() as tmp_dir:
        with pytest.raises(ValueError, match="First-row-id is only supported for V3 tables"):
            write_manifest(
                format_version=2,
                spec=UNPARTITIONED_PARTITION_SPEC,
                schema=test_schema,
                output_file=io.new_output(tmp_dir + "/manifest.avro"),
                snapshot_id=25,
                avro_compression="null",
                first_row_id=100,
            )


def test_read_v2_manifest_list_with_v3_layout() -> None:
    from pyiceberg.avro.file import AvroFile
    from pyiceberg.manifest import MANIFEST_LIST_FILE_SCHEMAS

    io = load_file_io()
    manifest = ManifestFile.from_args(
        manifest_path="/m1.avro",
        manifest_length=100,
        partition_spec_id=0,
        content=ManifestContent.DATA,
        sequence_number=1,
        min_sequence_number=1,
        added_snapshot_id=25,
        added_files_count=1,
        existing_files_count=0,
        deleted_files_count=0,
        added_rows_count=100,
        existing_rows_count=0,
        deleted_rows_count=0,
    )

    with TemporaryDirectory() as tmp_dir:
        path = tmp_dir + "/manifest-list-v2.avro"
        with write_manifest_list(
            format_version=2,
            output_file=io.new_output(path),
            snapshot_id=25,
            parent_snapshot_id=19,
            sequence_number=2,
            avro_compression="null",
        ) as writer:
            writer.add_manifests([manifest])

        # reading a V2 manifest list with the V3 layout yields a null first_row_id
        with AvroFile[ManifestFile](
            io.new_input(path),
            MANIFEST_LIST_FILE_SCHEMAS[3],
            read_types={-1: ManifestFile},
            read_enums={517: ManifestContent},
        ) as reader:
            entries = list(reader)
        assert len(entries) == 1
        assert entries[0].first_row_id is None
        assert entries[0].manifest_path == "/m1.avro"


def test_write_manifest_list_v3_rejects_unknown_row_counts() -> None:
    io = load_file_io()
    manifest = ManifestFile.from_args(
        manifest_path="/m1.avro",
        manifest_length=100,
        partition_spec_id=0,
        content=ManifestContent.DATA,
        sequence_number=1,
        min_sequence_number=1,
        added_snapshot_id=25,
        added_rows_count=None,
        existing_rows_count=None,
    )

    with TemporaryDirectory() as tmp_dir:
        with pytest.raises(ValueError, match="unknown row counts"):
            with write_manifest_list(
                format_version=3,
                output_file=io.new_output(tmp_dir + "/manifest-list.avro"),
                snapshot_id=25,
                parent_snapshot_id=19,
                sequence_number=2,
                avro_compression="null",
                first_row_id=1000,
            ) as writer:
                writer.add_manifests([manifest])
