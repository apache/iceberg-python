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
from datetime import date, datetime
from decimal import Decimal
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from pyiceberg.catalog import Catalog
from pyiceberg.manifest import DataFile, DataFileContent, FileFormat, ManifestEntryStatus
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.table.snapshots import Snapshot
from pyiceberg.table.statistics import BlobMetadata, PartitionStatisticsFile, StatisticsFile
from pyiceberg.transforms import BucketTransform, IdentityTransform, YearTransform
from pyiceberg.typedef import Record
from pyiceberg.types import (
    BinaryType,
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    LongType,
    NestedField,
    StringType,
    TimestampType,
)


def test_partition_statistics_file() -> None:
    partition_statistics_file_json = (
        """{"snapshot-id":123,"statistics-path":"s3://bucket/statistics.parquet","file-size-in-bytes":345}"""
    )
    partition_statistics_file = PartitionStatisticsFile.model_validate_json(partition_statistics_file_json)

    assert partition_statistics_file == PartitionStatisticsFile(
        snapshot_id=123, statistics_path="s3://bucket/statistics.parquet", file_size_in_bytes=345
    )

    assert partition_statistics_file.model_dump_json() == partition_statistics_file_json


def test_statistics_file() -> None:
    statistics_file_json = """{"snapshot-id":123,"statistics-path":"s3://bucket/statistics.parquet","file-size-in-bytes":345,"file-footer-size-in-bytes":456,"blob-metadata":[{"type":"apache-datasketches-theta-v1","snapshot-id":567,"sequence-number":22,"fields":[1,2,3],"properties":{"foo":"bar"}}]}"""
    statistics_file = StatisticsFile.model_validate_json(statistics_file_json)

    assert statistics_file == StatisticsFile(
        snapshot_id=123,
        statistics_path="s3://bucket/statistics.parquet",
        file_size_in_bytes=345,
        file_footer_size_in_bytes=456,
        key_metadata=None,
        blob_metadata=[
            BlobMetadata(
                type="apache-datasketches-theta-v1",
                snapshot_id=567,
                sequence_number=22,
                fields=[1, 2, 3],
                properties={"foo": "bar"},
            )
        ],
    )

    assert statistics_file.model_dump_json() == statistics_file_json


def _create_table_with_two_snapshots(catalog: Catalog) -> tuple[Table, int, int]:
    arrow_table = pa.table({"id": [1, 2, 3]})
    catalog.create_namespace("default")
    table = catalog.create_table("default.test_partition_statistics", schema=arrow_table.schema)
    table.append(arrow_table)
    table.append(arrow_table)
    snapshot_id_1, snapshot_id_2 = [snapshot.snapshot_id for snapshot in table.snapshots()]
    return table, snapshot_id_1, snapshot_id_2


def _partition_statistics_file(snapshot_id: int, statistics_path: str) -> PartitionStatisticsFile:
    return PartitionStatisticsFile(snapshot_id=snapshot_id, statistics_path=statistics_path, file_size_in_bytes=124)


def test_update_partition_statistics(catalog: Catalog) -> None:
    table, snapshot_id_1, snapshot_id_2 = _create_table_with_two_snapshots(catalog)
    stats_1 = _partition_statistics_file(snapshot_id_1, "s3://bucket/partition-stats-1.parquet")
    stats_1_replaced = _partition_statistics_file(snapshot_id_1, "s3://bucket/partition-stats-1-v2.parquet")
    stats_2 = _partition_statistics_file(snapshot_id_2, "s3://bucket/partition-stats-2.parquet")

    table.update_partition_statistics().set_partition_statistics(**stats_1.model_dump(by_alias=False)).commit()
    assert table.metadata.partition_statistics == [stats_1]

    # A file set for a snapshot that already has one replaces it
    table.update_partition_statistics().set_partition_statistics(**stats_1_replaced.model_dump(by_alias=False)).commit()
    assert table.metadata.partition_statistics == [stats_1_replaced]

    with table.update_partition_statistics() as update:
        update.set_partition_statistics(**stats_2.model_dump(by_alias=False))
    assert table.metadata.partition_statistics == [stats_1_replaced, stats_2]

    table.update_partition_statistics().remove_partition_statistics(snapshot_id_1).commit()
    assert table.metadata.partition_statistics == [stats_2]
    assert catalog.load_table(table.name()).metadata.partition_statistics == [stats_2]


def test_update_partition_statistics_set_and_remove_in_one_update(catalog: Catalog) -> None:
    table, snapshot_id_1, snapshot_id_2 = _create_table_with_two_snapshots(catalog)
    stats_1 = _partition_statistics_file(snapshot_id_1, "s3://bucket/partition-stats-1.parquet")
    stats_1_replaced = _partition_statistics_file(snapshot_id_1, "s3://bucket/partition-stats-1-v2.parquet")
    stats_2 = _partition_statistics_file(snapshot_id_2, "s3://bucket/partition-stats-2.parquet")

    # Operations on different snapshots are both applied, in either call order
    table.update_partition_statistics().set_partition_statistics(**stats_1.model_dump(by_alias=False)).commit()
    table.update_partition_statistics().set_partition_statistics(
        **stats_2.model_dump(by_alias=False)
    ).remove_partition_statistics(snapshot_id_1).commit()
    assert table.metadata.partition_statistics == [stats_2]

    with table.update_partition_statistics() as update:
        update.set_partition_statistics(**stats_1.model_dump(by_alias=False))
        update.remove_partition_statistics(snapshot_id_2)
    assert table.metadata.partition_statistics == [stats_1]

    # On the same snapshot, the removal wins in either call order, with or without an existing file
    for has_existing_file in (False, True):
        for remove_first in (False, True):
            if has_existing_file:
                table.update_partition_statistics().set_partition_statistics(**stats_1.model_dump(by_alias=False)).commit()
            else:
                table.update_partition_statistics().remove_partition_statistics(snapshot_id_1).commit()
            with table.update_partition_statistics() as update:
                if remove_first:
                    update.remove_partition_statistics(snapshot_id_1).set_partition_statistics(
                        **stats_1_replaced.model_dump(by_alias=False)
                    )
                else:
                    update.set_partition_statistics(**stats_1_replaced.model_dump(by_alias=False)).remove_partition_statistics(
                        snapshot_id_1
                    )
            assert table.metadata.partition_statistics == []

    # Repeated operations: the last file set wins, and repeated removals are no-ops
    with table.update_partition_statistics() as update:
        update.set_partition_statistics(**stats_1.model_dump(by_alias=False)).set_partition_statistics(
            **stats_1_replaced.model_dump(by_alias=False)
        )
    assert table.metadata.partition_statistics == [stats_1_replaced]

    with table.update_partition_statistics() as update:
        update.remove_partition_statistics(snapshot_id_1)
        update.remove_partition_statistics(snapshot_id_1)
    assert table.metadata.partition_statistics == []


def test_update_partition_statistics_in_transaction(catalog: Catalog) -> None:
    table, snapshot_id_1, snapshot_id_2 = _create_table_with_two_snapshots(catalog)
    stats_1 = _partition_statistics_file(snapshot_id_1, "s3://bucket/partition-stats-1.parquet")
    stats_2 = _partition_statistics_file(snapshot_id_2, "s3://bucket/partition-stats-2.parquet")

    with table.transaction() as transaction:
        transaction.update_partition_statistics().set_partition_statistics(**stats_1.model_dump(by_alias=False)).commit()
        with transaction.update_partition_statistics() as update:
            update.set_partition_statistics(**stats_2.model_dump(by_alias=False))

        # The pending changes are visible in the transaction only, until it commits
        assert transaction.table_metadata.partition_statistics == [stats_1, stats_2]
        assert table.metadata.partition_statistics == []
        assert catalog.load_table(table.name()).metadata.partition_statistics == []

    assert table.metadata.partition_statistics == [stats_1, stats_2]
    assert catalog.load_table(table.name()).metadata.partition_statistics == [stats_1, stats_2]

    # An exception raised inside the transaction rolls back its pending changes
    def abort_transaction() -> None:
        with table.transaction() as transaction:
            transaction.update_partition_statistics().remove_partition_statistics(snapshot_id_1).commit()
            raise RuntimeError("abort")

    with pytest.raises(RuntimeError, match="abort"):
        abort_transaction()

    assert table.metadata.partition_statistics == [stats_1, stats_2]
    assert catalog.load_table(table.name()).metadata.partition_statistics == [stats_1, stats_2]


def test_remove_partition_statistics_without_file_is_noop(catalog: Catalog) -> None:
    table, snapshot_id_1, snapshot_id_2 = _create_table_with_two_snapshots(catalog)
    stats_2 = _partition_statistics_file(snapshot_id_2, "s3://bucket/partition-stats-2.parquet")
    table.update_partition_statistics().set_partition_statistics(**stats_2.model_dump(by_alias=False)).commit()
    metadata_before = table.metadata

    # Removing a snapshot without a file commits without error, once or repeated, and leaves the metadata unchanged
    table.update_partition_statistics().remove_partition_statistics(snapshot_id_1).commit()
    with table.update_partition_statistics() as update:
        update.remove_partition_statistics(snapshot_id_1)
        update.remove_partition_statistics(snapshot_id_1)

    assert table.metadata == metadata_before
    assert catalog.load_table(table.name()).metadata.partition_statistics == [stats_2]


def _create_category_table(catalog: Catalog, name: str) -> Table:
    schema = Schema(NestedField(1, "category", StringType()), NestedField(2, "id", LongType()))
    spec = PartitionSpec(PartitionField(source_id=1, field_id=1000, transform=IdentityTransform(), name="category"))
    return catalog.create_table(name, schema=schema, partition_spec=spec)


def _read_partition_statistics(table: Table, statistics_file: PartitionStatisticsFile) -> list[dict[str, Any]]:
    with table.io.new_input(statistics_file.statistics_path).open() as stream:
        return pq.read_table(stream).to_pylist()


def _expected_partition_statistics_row(spec_id: int, partition: dict[str, Any], **values: Any) -> dict[str, Any]:
    row: dict[str, Any] = {
        "partition": partition,
        "spec_id": spec_id,
        "data_record_count": 0,
        "data_file_count": 0,
        "total_data_file_size_in_bytes": 0,
        "position_delete_record_count": 0,
        "position_delete_file_count": 0,
        "equality_delete_record_count": 0,
        "equality_delete_file_count": 0,
        "total_record_count": None,
        "last_updated_at": None,
        "last_updated_snapshot_id": None,
    }
    return {**row, **values}


def test_compute_partition_statistics(catalog: Catalog) -> None:
    catalog.create_namespace("default")
    table = _create_category_table(catalog, "default.test_compute_partition_statistics")
    arrow_schema = table.schema().as_arrow()
    table.append(pa.Table.from_pylist([{"category": "a", "id": 1}, {"category": None, "id": 2}], schema=arrow_schema))
    snapshot_1 = table.current_snapshot()
    assert snapshot_1 is not None
    table.append(pa.Table.from_pylist([{"category": "a", "id": 3}], schema=arrow_schema))
    snapshot_2 = table.current_snapshot()
    assert snapshot_2 is not None
    # The overwrite deletes both files of partition "a" and adds one file
    table.overwrite(pa.Table.from_pylist([{"category": "a", "id": 5}], schema=arrow_schema), overwrite_filter="category = 'a'")
    snapshot_3 = table.current_snapshot()
    assert snapshot_3 is not None
    table.append(pa.Table.from_pylist([{"category": "b", "id": 4}], schema=arrow_schema))
    table.delete("category = 'b'")
    snapshot_5 = table.current_snapshot()
    assert snapshot_5 is not None

    statistics_file = table.maintenance.compute_partition_statistics()

    assert statistics_file is not None
    assert statistics_file.snapshot_id == snapshot_5.snapshot_id
    assert statistics_file.statistics_path.startswith(f"{table.metadata.location}/metadata/partition-stats-")
    assert statistics_file.file_size_in_bytes == len(table.io.new_input(statistics_file.statistics_path))
    assert table.metadata.partition_statistics == [statistics_file]
    assert catalog.load_table(table.name()).metadata.partition_statistics == [statistics_file]

    # Partitions are sorted with NULL first. The deleted partition "b" keeps a row with zero counts, as in Java.
    size_of = {row["partition"]["category"]: row["file_size_in_bytes"] for row in table.inspect.data_files().to_pylist()}
    assert _read_partition_statistics(table, statistics_file) == [
        _expected_partition_statistics_row(
            0,
            {"category": None},
            data_record_count=1,
            data_file_count=1,
            total_data_file_size_in_bytes=size_of[None],
            last_updated_at=snapshot_1.timestamp_ms,
            last_updated_snapshot_id=snapshot_1.snapshot_id,
        ),
        _expected_partition_statistics_row(
            0,
            {"category": "a"},
            data_record_count=1,
            data_file_count=1,
            total_data_file_size_in_bytes=size_of["a"],
            last_updated_at=snapshot_3.timestamp_ms,
            last_updated_snapshot_id=snapshot_3.snapshot_id,
        ),
        _expected_partition_statistics_row(
            0,
            {"category": "b"},
            last_updated_at=snapshot_5.timestamp_ms,
            last_updated_snapshot_id=snapshot_5.snapshot_id,
        ),
    ]

    # Statistics of an older snapshot are registered for that snapshot, next to the current one
    statistics_file_2 = table.maintenance.compute_partition_statistics(snapshot_id=snapshot_2.snapshot_id)
    assert statistics_file_2 is not None
    assert [row["partition"]["category"] for row in _read_partition_statistics(table, statistics_file_2)] == [None, "a"]
    assert table.metadata.partition_statistics == [statistics_file, statistics_file_2]


def test_compute_partition_statistics_file_layout(catalog: Catalog) -> None:
    catalog.create_namespace("default")
    table = _create_category_table(catalog, "default.test_compute_partition_statistics_file_layout")
    table.append(pa.Table.from_pylist([{"category": "a", "id": 1}], schema=table.schema().as_arrow()))

    statistics_file = table.maintenance.compute_partition_statistics()
    assert statistics_file is not None

    with table.io.new_input(statistics_file.statistics_path).open() as stream:
        arrow_table = pq.read_table(stream)
    # The version 2 layout, with the field IDs of the spec and the required fields of the spec as not nullable
    assert [(field.name, int(field.metadata[b"PARQUET:field_id"]), not field.nullable) for field in arrow_table.schema] == [
        ("partition", 1, True),
        ("spec_id", 2, True),
        ("data_record_count", 3, True),
        ("data_file_count", 4, True),
        ("total_data_file_size_in_bytes", 5, True),
        ("position_delete_record_count", 6, False),
        ("position_delete_file_count", 7, False),
        ("equality_delete_record_count", 8, False),
        ("equality_delete_file_count", 9, False),
        ("total_record_count", 10, False),
        ("last_updated_at", 11, False),
        ("last_updated_snapshot_id", 12, False),
    ]
    partition_field = arrow_table.schema.field("partition").type.field("category")
    assert partition_field.metadata == {b"PARQUET:field_id": b"1000"}


def test_compute_partition_statistics_across_partition_specs(catalog: Catalog) -> None:
    catalog.create_namespace("default")
    schema = Schema(NestedField(1, "dt", DateType()), NestedField(2, "category", StringType()), NestedField(3, "id", LongType()))
    spec = PartitionSpec(PartitionField(source_id=1, field_id=1000, transform=IdentityTransform(), name="dt"))
    table = catalog.create_table("default.test_compute_partition_statistics_specs", schema=schema, partition_spec=spec)
    arrow_schema = table.schema().as_arrow()
    table.append(pa.Table.from_pylist([{"dt": date(2025, 1, 2), "category": None, "id": 1}], schema=arrow_schema))
    table.update_spec().add_identity("category").commit()
    table.append(pa.Table.from_pylist([{"dt": date(2025, 1, 1), "category": "x", "id": 2}], schema=arrow_schema))

    statistics_file = table.maintenance.compute_partition_statistics()
    assert statistics_file is not None

    # The unified partition type holds the fields of both specs, and rows of the first spec have NULL for the new field
    rows = _read_partition_statistics(table, statistics_file)
    assert [(row["spec_id"], row["partition"], row["data_record_count"]) for row in rows] == [
        (1, {"dt": date(2025, 1, 1), "category": "x"}, 1),
        (0, {"dt": date(2025, 1, 2), "category": None}, 1),
    ]


def test_compute_partition_statistics_skips_dropped_source_columns(catalog: Catalog) -> None:
    catalog.create_namespace("default")
    schema = Schema(NestedField(1, "a", LongType()), NestedField(2, "b", StringType()), NestedField(3, "id", LongType()))
    spec = PartitionSpec(PartitionField(source_id=1, field_id=1000, transform=IdentityTransform(), name="a"))
    table = catalog.create_table("default.test_compute_partition_statistics_dropped", schema=schema, partition_spec=spec)
    table.append(
        pa.Table.from_pylist([{"a": 1, "b": "x", "id": 1}, {"a": 2, "b": "x", "id": 2}], schema=table.schema().as_arrow())
    )
    table.update_spec().remove_field("a").add_identity("b").commit()
    table.update_schema().delete_column("a").commit()
    table.append(pa.Table.from_pylist([{"b": "y", "id": 3}], schema=table.schema().as_arrow()))

    statistics_file = table.maintenance.compute_partition_statistics()
    assert statistics_file is not None

    # The field of the dropped source column is left out of the partition type, so the two partitions of the first spec
    # that differ only in "a" share one row with NULL for "b". Java keeps them in separate rows; this is a known difference.
    rows = _read_partition_statistics(table, statistics_file)
    assert [(row["spec_id"], row["partition"], row["data_record_count"], row["data_file_count"]) for row in rows] == [
        (0, {"b": None}, 2, 2),
        (1, {"b": "y"}, 1, 1),
    ]


def test_compute_partition_statistics_partition_value_types(catalog: Catalog) -> None:
    catalog.create_namespace("default")
    cases: list[tuple[Any, Any]] = [
        (LongType(), 1),
        (StringType(), "a"),
        (DateType(), date(2025, 1, 1)),
        (TimestampType(), datetime(2025, 1, 1, 12, 30)),
        (BooleanType(), True),
        (DecimalType(10, 2), Decimal("1.50")),
        (BinaryType(), b"ab"),
    ]
    for index, (field_type, value) in enumerate(cases):
        schema = Schema(NestedField(1, "p", field_type), NestedField(2, "id", LongType()))
        spec = PartitionSpec(PartitionField(source_id=1, field_id=1000, transform=IdentityTransform(), name="p"))
        table = catalog.create_table(
            f"default.test_compute_partition_statistics_types_{index}", schema=schema, partition_spec=spec
        )
        table.append(pa.Table.from_pylist([{"p": value, "id": 1}, {"p": None, "id": 2}], schema=table.schema().as_arrow()))

        statistics_file = table.maintenance.compute_partition_statistics()
        assert statistics_file is not None
        assert [row["partition"]["p"] for row in _read_partition_statistics(table, statistics_file)] == [None, value], field_type


def test_compute_partition_statistics_returns_registered_file(catalog: Catalog) -> None:
    catalog.create_namespace("default")
    table = _create_category_table(catalog, "default.test_compute_partition_statistics_registered")
    table.append(pa.Table.from_pylist([{"category": "a", "id": 1}], schema=table.schema().as_arrow()))
    statistics_file = table.maintenance.compute_partition_statistics()
    metadata_location = table.metadata_location

    # The file of the snapshot is returned as is, without writing a file or committing the table again
    assert table.maintenance.compute_partition_statistics() == statistics_file
    assert table.metadata_location == metadata_location
    assert table.metadata.partition_statistics == [statistics_file]


def test_compute_partition_statistics_without_statistics(catalog: Catalog) -> None:
    catalog.create_namespace("default")
    table = _create_category_table(catalog, "default.test_compute_partition_statistics_empty")
    # A table without snapshots has nothing to compute
    assert table.maintenance.compute_partition_statistics() is None
    assert table.metadata.partition_statistics == []


def test_compute_partition_statistics_errors(catalog: Catalog) -> None:
    catalog.create_namespace("default")
    unpartitioned = catalog.create_table(
        "default.test_compute_partition_statistics_unpartitioned", schema=Schema(NestedField(1, "id", LongType()))
    )
    unpartitioned.append(pa.Table.from_pylist([{"id": 1}], schema=unpartitioned.schema().as_arrow()))
    with pytest.raises(ValueError, match="Table must be partitioned"):
        unpartitioned.maintenance.compute_partition_statistics()

    table = _create_category_table(catalog, "default.test_compute_partition_statistics_errors")
    table.append(pa.Table.from_pylist([{"category": "a", "id": 1}], schema=table.schema().as_arrow()))
    with pytest.raises(ValueError, match="Snapshot not found: 42"):
        table.maintenance.compute_partition_statistics(snapshot_id=42)

    # The file is written in the default file format of the table, and PyIceberg only has a Parquet writer
    table.transaction().set_properties({"write.format.default": "orc"}).commit_transaction()
    with pytest.raises(ValueError, match="No writer registered"):
        table.maintenance.compute_partition_statistics()
    assert table.metadata.partition_statistics == []

    # PyIceberg cannot create format version 3 tables, so the format version of a loaded v2 table is changed for this check
    table.metadata = table.metadata.model_copy(update={"format_version": 3})
    with pytest.raises(NotImplementedError, match="format version 3"):
        table.maintenance.compute_partition_statistics()


def test_compute_partition_statistics_float_partition_identity_and_order(catalog: Catalog) -> None:
    catalog.create_namespace("default")
    # Each value is appended on its own, so each value is one data file
    values: list[float | None] = [float("nan"), float("nan"), -0.0, 0.0, float("inf"), float("-inf"), None]
    for index, field_type in enumerate([FloatType(), DoubleType()]):
        schema = Schema(NestedField(1, "p", field_type), NestedField(2, "id", LongType()))
        spec = PartitionSpec(PartitionField(source_id=1, field_id=1000, transform=IdentityTransform(), name="p"))
        table = catalog.create_table(
            f"default.test_compute_partition_statistics_float_{index}", schema=schema, partition_spec=spec
        )
        for value in values:
            table.append(pa.Table.from_pylist([{"p": value, "id": 1}], schema=table.schema().as_arrow()))

        statistics_file = table.maintenance.compute_partition_statistics()
        assert statistics_file is not None
        # NULL first. NaN values form one partition that sorts after the other floats, and -0.0 and 0.0 are two partitions.
        rows = _read_partition_statistics(table, statistics_file)
        assert [(repr(row["partition"]["p"]), row["data_record_count"], row["data_file_count"]) for row in rows] == [
            ("None", 1, 1),
            ("-inf", 1, 1),
            ("-0.0", 1, 1),
            ("0.0", 1, 1),
            ("inf", 1, 1),
            ("nan", 2, 2),
        ]


def test_compute_partition_statistics_keeps_output_type_of_removed_v1_partition_field(catalog: Catalog) -> None:
    catalog.create_namespace("default")
    cases: list[tuple[str, Any, Any, Any]] = [
        ("bucket", BucketTransform(8), StringType(), "abc"),
        ("year", YearTransform(), DateType(), date(2025, 1, 1)),
    ]
    for name, transform, source_type, value in cases:
        schema = Schema(NestedField(1, "a", source_type), NestedField(2, "id", LongType()))
        spec = PartitionSpec(PartitionField(source_id=1, field_id=1000, transform=transform, name="p"))
        table = catalog.create_table(
            f"default.test_compute_partition_statistics_v1_{name}",
            schema=schema,
            partition_spec=spec,
            properties={"format-version": "1"},
        )
        table.append(pa.Table.from_pylist([{"a": value, "id": 1}], schema=table.schema().as_arrow()))
        # In v1, removing a partition field replaces its transform with void, under the same field ID
        table.update_spec().remove_field("p").commit()

        statistics_file = table.maintenance.compute_partition_statistics()
        assert statistics_file is not None
        with table.io.new_input(statistics_file.statistics_path).open() as stream:
            arrow_table = pq.read_table(stream)
        # The value keeps the output type of the transform in the manifest, not the type of the source column
        assert arrow_table.schema.field("partition").type.field("p").type == pa.int32()
        assert arrow_table.column("partition").to_pylist() == [{"p": transform.transform(source_type)(value)}]


def test_compute_partition_statistics_delete_files(catalog: Catalog) -> None:
    catalog.create_namespace("default")
    table = _create_category_table(catalog, "default.test_compute_partition_statistics_deletes")
    table.append(pa.Table.from_pylist([{"category": "a", "id": 1}], schema=table.schema().as_arrow()))
    snapshot = _commit_delete_manifest(
        table,
        [
            (_delete_file(table, "a", DataFileContent.POSITION_DELETES, record_count=2), ManifestEntryStatus.ADDED),
            (_delete_file(table, "a", DataFileContent.EQUALITY_DELETES, record_count=3), ManifestEntryStatus.ADDED),
            # A deleted delete file is not counted, but its partition still gets a row
            (_delete_file(table, "b", DataFileContent.POSITION_DELETES, record_count=9), ManifestEntryStatus.DELETED),
        ],
    )

    statistics_file = table.maintenance.compute_partition_statistics()
    assert statistics_file is not None
    size_of = _data_file_sizes(table)
    assert _read_partition_statistics(table, statistics_file) == [
        _expected_partition_statistics_row(
            0,
            {"category": "a"},
            data_record_count=1,
            data_file_count=1,
            total_data_file_size_in_bytes=size_of["a"],
            position_delete_record_count=2,
            position_delete_file_count=1,
            equality_delete_record_count=3,
            equality_delete_file_count=1,
            last_updated_at=snapshot.timestamp_ms,
            last_updated_snapshot_id=snapshot.snapshot_id,
        ),
        _expected_partition_statistics_row(
            0,
            {"category": "b"},
            last_updated_at=snapshot.timestamp_ms,
            last_updated_snapshot_id=snapshot.snapshot_id,
        ),
    ]


def test_compute_partition_statistics_empty_snapshot(catalog: Catalog) -> None:
    catalog.create_namespace("default")
    table = _create_category_table(catalog, "default.test_compute_partition_statistics_empty_snapshot")
    with table.transaction() as transaction:
        with transaction.update_snapshot().fast_append():
            pass
    assert table.current_snapshot() is not None

    # A snapshot without manifest entries has no statistics to write
    assert table.maintenance.compute_partition_statistics() is None
    assert table.metadata.partition_statistics == []


def test_compute_partition_statistics_entries_of_expired_snapshot(catalog: Catalog) -> None:
    catalog.create_namespace("default")
    table = _create_category_table(catalog, "default.test_compute_partition_statistics_expired_entries")
    table.append(pa.Table.from_pylist([{"category": "a", "id": 1}], schema=table.schema().as_arrow()))
    expired = table.current_snapshot()
    assert expired is not None
    table.append(pa.Table.from_pylist([{"category": "b", "id": 2}], schema=table.schema().as_arrow()))
    current = table.current_snapshot()
    assert current is not None
    table.maintenance.expire_snapshots().by_id(expired.snapshot_id).commit()

    statistics_file = table.maintenance.compute_partition_statistics()
    assert statistics_file is not None
    size_of = _data_file_sizes(table)
    # The entries of partition "a" come from the expired snapshot, so they set no last update, as in Java
    assert _read_partition_statistics(table, statistics_file) == [
        _expected_partition_statistics_row(
            0, {"category": "a"}, data_record_count=1, data_file_count=1, total_data_file_size_in_bytes=size_of["a"]
        ),
        _expected_partition_statistics_row(
            0,
            {"category": "b"},
            data_record_count=1,
            data_file_count=1,
            total_data_file_size_in_bytes=size_of["b"],
            last_updated_at=current.timestamp_ms,
            last_updated_snapshot_id=current.snapshot_id,
        ),
    ]


def test_compute_partition_statistics_rejects_registered_file_of_expired_snapshot(catalog: Catalog) -> None:
    catalog.create_namespace("default")
    table = _create_category_table(catalog, "default.test_compute_partition_statistics_stale")
    table.append(pa.Table.from_pylist([{"category": "a", "id": 1}], schema=table.schema().as_arrow()))
    statistics_file = table.maintenance.compute_partition_statistics()
    assert statistics_file is not None
    table.append(pa.Table.from_pylist([{"category": "b", "id": 2}], schema=table.schema().as_arrow()))
    table.maintenance.expire_snapshots().by_id(statistics_file.snapshot_id).commit()

    # Simulate a stale descriptor retained by an older writer (see #4364).
    table.metadata = table.metadata.model_copy(update={"partition_statistics": [statistics_file]})
    with pytest.raises(ValueError, match=f"Snapshot not found: {statistics_file.snapshot_id}"):
        table.maintenance.compute_partition_statistics(snapshot_id=statistics_file.snapshot_id)


def test_compute_partition_statistics_same_partition_under_different_specs(catalog: Catalog) -> None:
    catalog.create_namespace("default")
    schema = Schema(NestedField(1, "dt", DateType()), NestedField(2, "category", StringType()), NestedField(3, "id", LongType()))
    spec = PartitionSpec(PartitionField(source_id=1, field_id=1000, transform=IdentityTransform(), name="dt"))
    table = catalog.create_table("default.test_compute_partition_statistics_same_tuple", schema=schema, partition_spec=spec)
    arrow_schema = table.schema().as_arrow()
    table.append(pa.Table.from_pylist([{"dt": date(2025, 1, 1), "category": None, "id": 1}], schema=arrow_schema))
    table.update_spec().add_identity("category").commit()
    table.append(pa.Table.from_pylist([{"dt": date(2025, 1, 1), "category": None, "id": 2}], schema=arrow_schema))

    statistics_file = table.maintenance.compute_partition_statistics()
    assert statistics_file is not None
    # Both specs have the unified partition (2025-01-01, NULL), and each spec keeps its own row
    rows = _read_partition_statistics(table, statistics_file)
    assert sorted((row["spec_id"], row["partition"]["dt"], row["partition"]["category"]) for row in rows) == [
        (0, date(2025, 1, 1), None),
        (1, date(2025, 1, 1), None),
    ]


def _data_file_sizes(table: Table) -> dict[str, int]:
    return {row["partition"]["category"]: row["file_size_in_bytes"] for row in table.inspect.data_files().to_pylist()}


def _delete_file(table: Table, category: str, content: DataFileContent, record_count: int) -> DataFile:
    import uuid

    return DataFile.from_args(
        content=content,
        file_path=table.location_provider().new_data_location(f"{content.name.lower()}-{uuid.uuid4()}.parquet"),
        file_format=FileFormat.PARQUET,
        partition=Record(category),
        record_count=record_count,
        file_size_in_bytes=1,
        equality_ids=[2] if content == DataFileContent.EQUALITY_DELETES else None,
    )


def _commit_delete_manifest(table: Table, entries: list[tuple[DataFile, ManifestEntryStatus]]) -> Snapshot:
    """Commit a snapshot that adds a manifest of delete files, written as other engines write them.

    PyIceberg cannot write delete files. The snapshot keeps the manifests of the current snapshot and adds this one.
    """
    import uuid

    from pyiceberg.manifest import ManifestContent, ManifestEntry, ManifestWriterV2, write_manifest_list
    from pyiceberg.table.refs import SnapshotRefType
    from pyiceberg.table.snapshots import Operation, Summary
    from pyiceberg.table.update import AddSnapshotUpdate, SetSnapshotRefUpdate

    class DeleteManifestWriter(ManifestWriterV2):
        def content(self) -> ManifestContent:
            return ManifestContent.DELETES

        @property
        def _meta(self) -> dict[str, str]:
            return {**super()._meta, "content": "deletes"}

    parent = table.current_snapshot()
    assert parent is not None
    snapshot_id = parent.snapshot_id + 1
    sequence_number = table.last_sequence_number + 1
    manifest_path = table.location_provider().new_metadata_location(f"delete-manifest-{uuid.uuid4()}.avro")
    with DeleteManifestWriter(table.spec(), table.schema(), table.io.new_output(manifest_path), snapshot_id, "null") as writer:
        for data_file, status in entries:
            writer.add_entry(
                ManifestEntry.from_args(
                    status=status,
                    snapshot_id=snapshot_id,
                    sequence_number=sequence_number,
                    file_sequence_number=sequence_number,
                    data_file=data_file,
                )
            )

    list_path = table.location_provider().new_metadata_location(f"snap-{snapshot_id}-{uuid.uuid4()}.avro")
    with write_manifest_list(
        2, table.io.new_output(list_path), snapshot_id, parent.snapshot_id, sequence_number, "null"
    ) as manifests:
        manifests.add_manifests([*parent.manifests(table.io), writer.to_manifest_file()])

    snapshot = Snapshot(
        snapshot_id=snapshot_id,
        parent_snapshot_id=parent.snapshot_id,
        sequence_number=sequence_number,
        timestamp_ms=parent.timestamp_ms + 1,
        manifest_list=list_path,
        summary=Summary(Operation.OVERWRITE),
        schema_id=table.schema().schema_id,
    )
    with table.transaction() as transaction:
        transaction._apply(
            (
                AddSnapshotUpdate(snapshot=snapshot),
                SetSnapshotRefUpdate(ref_name="main", type=SnapshotRefType.BRANCH, snapshot_id=snapshot_id),
            )
        )
    return snapshot
