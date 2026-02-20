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
# pylint:disable=redefined-outer-name

import multiprocessing
import os
import re
import threading
from collections.abc import Iterator
from datetime import date
from unittest import mock

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pyspark.sql import SparkSession
from pytest_mock.plugin import MockerFixture

from pyiceberg.catalog import Catalog
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.io import FileIO
from pyiceberg.io.pyarrow import UnsupportedPyArrowTypeException, schema_to_pyarrow
from pyiceberg.manifest import DataFile
from pyiceberg.partitioning import UNPARTITIONED_PARTITION_SPEC, PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.table.metadata import TableMetadata
from pyiceberg.transforms import BucketTransform, HourTransform, IdentityTransform, MonthTransform
from pyiceberg.types import (
    BooleanType,
    DateType,
    IntegerType,
    LongType,
    NestedField,
    StringType,
    TimestampType,
    TimestamptzType,
)

TABLE_SCHEMA = Schema(
    NestedField(field_id=1, name="foo", field_type=BooleanType(), required=False),
    NestedField(field_id=2, name="bar", field_type=StringType(), required=False),
    NestedField(field_id=4, name="baz", field_type=IntegerType(), required=False),
    NestedField(field_id=10, name="qux", field_type=DateType(), required=False),
)

ARROW_SCHEMA = pa.schema(
    [
        ("foo", pa.bool_()),
        ("bar", pa.string()),
        ("baz", pa.int32()),
        ("qux", pa.date32()),
    ]
)

ARROW_TABLE = pa.Table.from_pylist(
    [
        {
            "foo": True,
            "bar": "bar_string",
            "baz": 123,
            "qux": date(2024, 3, 7),
        }
    ],
    schema=ARROW_SCHEMA,
)

ARROW_SCHEMA_WITH_IDS = pa.schema(
    [
        pa.field("foo", pa.bool_(), nullable=False, metadata={"PARQUET:field_id": "1"}),
        pa.field("bar", pa.string(), nullable=False, metadata={"PARQUET:field_id": "2"}),
        pa.field("baz", pa.int32(), nullable=False, metadata={"PARQUET:field_id": "3"}),
        pa.field("qux", pa.date32(), nullable=False, metadata={"PARQUET:field_id": "4"}),
    ]
)


ARROW_TABLE_WITH_IDS = pa.Table.from_pylist(
    [
        {
            "foo": True,
            "bar": "bar_string",
            "baz": 123,
            "qux": date(2024, 3, 7),
        }
    ],
    schema=ARROW_SCHEMA_WITH_IDS,
)

ARROW_SCHEMA_UPDATED = pa.schema(
    [
        ("foo", pa.bool_()),
        ("baz", pa.int32()),
        ("qux", pa.date32()),
        ("quux", pa.int32()),
    ]
)

ARROW_TABLE_UPDATED = pa.Table.from_pylist(
    [
        {
            "foo": True,
            "baz": 123,
            "qux": date(2024, 3, 7),
            "quux": 234,
        }
    ],
    schema=ARROW_SCHEMA_UPDATED,
)


def _write_parquet(io: FileIO, file_path: str, arrow_schema: pa.Schema, arrow_table: pa.Table) -> None:
    fo = io.new_output(file_path)
    with fo.create(overwrite=True) as fos:
        with pq.ParquetWriter(fos, schema=arrow_schema) as writer:
            writer.write_table(arrow_table)


def _create_table(
    session_catalog: Catalog,
    identifier: str,
    format_version: int,
    partition_spec: PartitionSpec = UNPARTITIONED_PARTITION_SPEC,
    schema: Schema = TABLE_SCHEMA,
) -> Table:
    try:
        session_catalog.drop_table(identifier=identifier)
    except NoSuchTableError:
        pass

    return session_catalog.create_table(
        identifier=identifier,
        schema=schema,
        properties={"format-version": str(format_version)},
        partition_spec=partition_spec,
    )


@pytest.fixture(name="format_version", params=[pytest.param(1, id="format_version=1"), pytest.param(2, id="format_version=2")])
def format_version_fixure(request: pytest.FixtureRequest) -> Iterator[int]:
    """Fixture to run tests with different table format versions."""
    yield request.param


@pytest.mark.integration
def test_add_files_to_unpartitioned_table(spark: SparkSession, session_catalog: Catalog, format_version: int) -> None:
    identifier = f"default.unpartitioned_table_v{format_version}"
    tbl = _create_table(session_catalog, identifier, format_version)

    file_paths = [f"s3://warehouse/default/unpartitioned/v{format_version}/test-{i}.parquet" for i in range(5)]
    # write parquet files
    for file_path in file_paths:
        fo = tbl.io.new_output(file_path)
        with fo.create(overwrite=True) as fos:
            with pq.ParquetWriter(fos, schema=ARROW_SCHEMA) as writer:
                writer.write_table(ARROW_TABLE)

    # add the parquet files as data files
    tbl.add_files(file_paths=file_paths)

    # NameMapping must have been set to enable reads
    assert tbl.name_mapping() is not None

    rows = spark.sql(
        f"""
        SELECT added_data_files_count, existing_data_files_count, deleted_data_files_count
        FROM {identifier}.all_manifests
    """
    ).collect()

    assert [row.added_data_files_count for row in rows] == [5]
    assert [row.existing_data_files_count for row in rows] == [0]
    assert [row.deleted_data_files_count for row in rows] == [0]

    df = spark.table(identifier)
    assert df.count() == 5, "Expected 5 rows"
    for col in df.columns:
        assert df.filter(df[col].isNotNull()).count() == 5, "Expected all 5 rows to be non-null"

    # check that the table can be read by pyiceberg
    assert len(tbl.scan().to_arrow()) == 5, "Expected 5 rows"


@pytest.mark.integration
def test_add_files_to_unpartitioned_table_raises_file_not_found(
    spark: SparkSession, session_catalog: Catalog, format_version: int
) -> None:
    identifier = f"default.unpartitioned_raises_not_found_v{format_version}"
    tbl = _create_table(session_catalog, identifier, format_version)

    file_paths = [f"s3://warehouse/default/unpartitioned_raises_not_found/v{format_version}/test-{i}.parquet" for i in range(5)]
    # write parquet files
    for file_path in file_paths:
        fo = tbl.io.new_output(file_path)
        with fo.create(overwrite=True) as fos:
            with pq.ParquetWriter(fos, schema=ARROW_SCHEMA) as writer:
                writer.write_table(ARROW_TABLE)

    # add the parquet files as data files
    with pytest.raises(FileNotFoundError):
        tbl.add_files(file_paths=file_paths + ["s3://warehouse/default/unpartitioned_raises_not_found/unknown.parquet"])


@pytest.mark.integration
def test_add_files_to_unpartitioned_table_with_field_ids(
    spark: SparkSession, session_catalog: Catalog, format_version: int
) -> None:
    identifier = f"default.unpartitioned_with_field_ids_v{format_version}"
    tbl = _create_table(session_catalog, identifier, format_version)

    file_paths = [f"s3://warehouse/default/unpartitioned_with_field_ids/v{format_version}/test-{i}.parquet" for i in range(5)]
    # write parquet files with field IDs matching the table schema
    for file_path in file_paths:
        fo = tbl.io.new_output(file_path)
        with fo.create(overwrite=True) as fos:
            with pq.ParquetWriter(fos, schema=ARROW_SCHEMA_WITH_IDS) as writer:
                writer.write_table(ARROW_TABLE_WITH_IDS)

    # add the parquet files as data files
    tbl.add_files(file_paths=file_paths)

    # NameMapping should still be set even though files have field IDs
    assert tbl.name_mapping() is not None

    # Verify files were added successfully
    rows = spark.sql(
        f"""
        SELECT added_data_files_count, existing_data_files_count, deleted_data_files_count
        FROM {identifier}.all_manifests
    """
    ).collect()

    assert [row.added_data_files_count for row in rows] == [5]
    assert [row.existing_data_files_count for row in rows] == [0]
    assert [row.deleted_data_files_count for row in rows] == [0]

    # Verify data can be read back correctly
    df = spark.table(identifier).toPandas()
    assert len(df) == 5
    assert all(df["foo"] == True)  # noqa: E712
    assert all(df["bar"] == "bar_string")
    assert all(df["baz"] == 123)
    assert all(df["qux"] == date(2024, 3, 7))


@pytest.mark.integration
def test_add_files_parallelized(spark: SparkSession, session_catalog: Catalog, format_version: int) -> None:
    from pyiceberg.io.pyarrow import parquet_file_to_data_file

    real_parquet_file_to_data_file = parquet_file_to_data_file

    lock = threading.Lock()
    unique_threads_seen = set()
    cpu_count = multiprocessing.cpu_count()

    # patch the function _parquet_file_to_data_file to we can track how many unique thread IDs
    # it was executed from
    with mock.patch("pyiceberg.io.pyarrow.parquet_file_to_data_file") as patch_func:

        def mock_parquet_file_to_data_file(io: FileIO, table_metadata: TableMetadata, file_path: str) -> DataFile:
            lock.acquire()
            thread_id = threading.get_ident()  # the current thread ID
            unique_threads_seen.add(thread_id)
            lock.release()
            return real_parquet_file_to_data_file(io=io, table_metadata=table_metadata, file_path=file_path)

        patch_func.side_effect = mock_parquet_file_to_data_file

        identifier = f"default.unpartitioned_table_schema_updates_v{format_version}"
        tbl = _create_table(session_catalog, identifier, format_version)

        file_paths = [
            f"s3://warehouse/default/add_files_parallel/v{format_version}/test-{i}.parquet" for i in range(cpu_count * 2)
        ]
        # write parquet files
        for file_path in file_paths:
            fo = tbl.io.new_output(file_path)
            with fo.create(overwrite=True) as fos:
                with pq.ParquetWriter(fos, schema=ARROW_SCHEMA) as writer:
                    writer.write_table(ARROW_TABLE)

        tbl.add_files(file_paths=file_paths)

    # duration creation of threadpool processor, when max_workers is not
    # specified, python will add cpu_count + 4 as the number of threads in the
    # pool in this case
    # https://github.com/python/cpython/blob/e06bebb87e1b33f7251196e1ddb566f528c3fc98/Lib/concurrent/futures/thread.py#L173-L181
    # we check that we have at least seen the number of threads. we don't
    # specify the workers in the thread pool and we can't check without
    # accessing private attributes of ThreadPoolExecutor
    assert len(unique_threads_seen) >= cpu_count


@pytest.mark.integration
def test_add_files_to_unpartitioned_table_with_schema_updates(
    spark: SparkSession, session_catalog: Catalog, format_version: int
) -> None:
    identifier = f"default.unpartitioned_table_schema_updates_v{format_version}"
    tbl = _create_table(session_catalog, identifier, format_version)

    file_paths = [f"s3://warehouse/default/unpartitioned_schema_updates/v{format_version}/test-{i}.parquet" for i in range(5)]
    # write parquet files
    for file_path in file_paths:
        fo = tbl.io.new_output(file_path)
        with fo.create(overwrite=True) as fos:
            with pq.ParquetWriter(fos, schema=ARROW_SCHEMA) as writer:
                writer.write_table(ARROW_TABLE)

    # add the parquet files as data files
    tbl.add_files(file_paths=file_paths)

    # NameMapping must have been set to enable reads
    assert tbl.name_mapping() is not None

    with tbl.update_schema() as update:
        update.add_column("quux", IntegerType())
        update.delete_column("bar")

    file_path = f"s3://warehouse/default/unpartitioned_schema_updates/v{format_version}/test-6.parquet"
    # write parquet files
    fo = tbl.io.new_output(file_path)
    with fo.create(overwrite=True) as fos:
        with pq.ParquetWriter(fos, schema=ARROW_SCHEMA_UPDATED) as writer:
            writer.write_table(ARROW_TABLE_UPDATED)

    # add the parquet files as data files
    tbl.add_files(file_paths=[file_path])
    rows = spark.sql(
        f"""
        SELECT added_data_files_count, existing_data_files_count, deleted_data_files_count
        FROM {identifier}.all_manifests
    """
    ).collect()

    assert [row.added_data_files_count for row in rows] == [5, 1, 5]
    assert [row.existing_data_files_count for row in rows] == [0, 0, 0]
    assert [row.deleted_data_files_count for row in rows] == [0, 0, 0]

    df = spark.table(identifier)
    assert df.count() == 6, "Expected 6 rows"
    assert len(df.columns) == 4, "Expected 4 columns"

    for col in df.columns:
        value_count = 1 if col == "quux" else 6
        assert df.filter(df[col].isNotNull()).count() == value_count, f"Expected {value_count} rows to be non-null"

    # check that the table can be read by pyiceberg
    assert len(tbl.scan().to_arrow()) == 6, "Expected 6 rows"


@pytest.mark.integration
def test_add_files_to_partitioned_table(spark: SparkSession, session_catalog: Catalog, format_version: int) -> None:
    identifier = f"default.partitioned_table_v{format_version}"

    partition_spec = PartitionSpec(
        PartitionField(source_id=4, field_id=1000, transform=IdentityTransform(), name="baz"),
        PartitionField(source_id=10, field_id=1001, transform=MonthTransform(), name="qux_month"),
        spec_id=0,
    )

    tbl = _create_table(session_catalog, identifier, format_version, partition_spec)

    date_iter = iter([date(2024, 3, 7), date(2024, 3, 8), date(2024, 3, 16), date(2024, 3, 18), date(2024, 3, 19)])

    file_paths = [f"s3://warehouse/default/partitioned/v{format_version}/test-{i}.parquet" for i in range(5)]
    # write parquet files
    for file_path in file_paths:
        fo = tbl.io.new_output(file_path)
        with fo.create(overwrite=True) as fos:
            with pq.ParquetWriter(fos, schema=ARROW_SCHEMA) as writer:
                writer.write_table(
                    pa.Table.from_pylist(
                        [
                            {
                                "foo": True,
                                "bar": "bar_string",
                                "baz": 123,
                                "qux": next(date_iter),
                            }
                        ],
                        schema=ARROW_SCHEMA,
                    )
                )

    # add the parquet files as data files
    tbl.add_files(file_paths=file_paths)

    # NameMapping must have been set to enable reads
    assert tbl.name_mapping() is not None

    rows = spark.sql(
        f"""
        SELECT added_data_files_count, existing_data_files_count, deleted_data_files_count
        FROM {identifier}.all_manifests
    """
    ).collect()

    assert [row.added_data_files_count for row in rows] == [5]
    assert [row.existing_data_files_count for row in rows] == [0]
    assert [row.deleted_data_files_count for row in rows] == [0]

    df = spark.table(identifier)
    assert df.count() == 5, "Expected 5 rows"
    for col in df.columns:
        assert df.filter(df[col].isNotNull()).count() == 5, "Expected all 5 rows to be non-null"

    partition_rows = spark.sql(
        f"""
        SELECT partition, record_count, file_count
        FROM {identifier}.partitions
    """
    ).collect()

    assert [row.record_count for row in partition_rows] == [5]
    assert [row.file_count for row in partition_rows] == [5]
    assert [(row.partition.baz, row.partition.qux_month) for row in partition_rows] == [(123, 650)]

    # check that the table can be read by pyiceberg
    assert len(tbl.scan().to_arrow()) == 5, "Expected 5 rows"


@pytest.mark.integration
def test_add_files_to_bucket_partitioned_table_fails(spark: SparkSession, session_catalog: Catalog, format_version: int) -> None:
    identifier = f"default.partitioned_table_bucket_fails_v{format_version}"

    partition_spec = PartitionSpec(
        PartitionField(source_id=4, field_id=1000, transform=BucketTransform(num_buckets=3), name="baz_bucket_3"),
        spec_id=0,
    )

    tbl = _create_table(session_catalog, identifier, format_version, partition_spec)

    int_iter = iter(range(5))

    file_paths = [f"s3://warehouse/default/partitioned_table_bucket_fails/v{format_version}/test-{i}.parquet" for i in range(5)]
    # write parquet files
    for file_path in file_paths:
        fo = tbl.io.new_output(file_path)
        with fo.create(overwrite=True) as fos:
            with pq.ParquetWriter(fos, schema=ARROW_SCHEMA) as writer:
                writer.write_table(
                    pa.Table.from_pylist(
                        [
                            {
                                "foo": True,
                                "bar": "bar_string",
                                "baz": next(int_iter),
                                "qux": date(2024, 3, 7),
                            }
                        ],
                        schema=ARROW_SCHEMA,
                    )
                )

    # add the parquet files as data files
    with pytest.raises(ValueError) as exc_info:
        tbl.add_files(file_paths=file_paths)
    assert (
        "Cannot infer partition value from parquet metadata for a non-linear Partition Field: "
        "baz_bucket_3 with transform bucket[3]" in str(exc_info.value)
    )


@pytest.mark.integration
def test_add_files_to_partitioned_table_fails_with_lower_and_upper_mismatch(
    spark: SparkSession, session_catalog: Catalog, format_version: int
) -> None:
    identifier = f"default.partitioned_table_mismatch_fails_v{format_version}"

    partition_spec = PartitionSpec(
        PartitionField(source_id=4, field_id=1000, transform=IdentityTransform(), name="baz"),
        spec_id=0,
    )

    tbl = _create_table(session_catalog, identifier, format_version, partition_spec)

    file_paths = [f"s3://warehouse/default/partitioned_table_mismatch_fails/v{format_version}/test-{i}.parquet" for i in range(5)]
    # write parquet files
    for file_path in file_paths:
        fo = tbl.io.new_output(file_path)
        with fo.create(overwrite=True) as fos:
            with pq.ParquetWriter(fos, schema=ARROW_SCHEMA) as writer:
                writer.write_table(
                    pa.Table.from_pylist(
                        [
                            {
                                "foo": True,
                                "bar": "bar_string",
                                "baz": 123,
                                "qux": date(2024, 3, 7),
                            },
                            {
                                "foo": True,
                                "bar": "bar_string",
                                "baz": 124,
                                "qux": date(2024, 3, 7),
                            },
                        ],
                        schema=ARROW_SCHEMA,
                    )
                )

    # add the parquet files as data files
    with pytest.raises(ValueError) as exc_info:
        tbl.add_files(file_paths=file_paths)
    assert (
        "Cannot infer partition value from parquet metadata as there are more than one partition values "
        "for Partition Field: baz. lower_value=123, upper_value=124" in str(exc_info.value)
    )


@pytest.mark.integration
def test_add_files_snapshot_properties(spark: SparkSession, session_catalog: Catalog, format_version: int) -> None:
    identifier = f"default.test_add_files_snapshot_properties_v{format_version}"
    tbl = _create_table(session_catalog, identifier, format_version)

    file_paths = [f"s3://warehouse/default/unpartitioned/v{format_version}/test-{i}.parquet" for i in range(5)]
    # write parquet files
    for file_path in file_paths:
        fo = tbl.io.new_output(file_path)
        with fo.create(overwrite=True) as fos:
            with pq.ParquetWriter(fos, schema=ARROW_SCHEMA) as writer:
                writer.write_table(ARROW_TABLE)

    # add the parquet files as data files
    tbl.add_files(file_paths=file_paths, snapshot_properties={"snapshot_prop_a": "test_prop_a"})

    # NameMapping must have been set to enable reads
    assert tbl.name_mapping() is not None

    summary = spark.sql(f"SELECT * FROM {identifier}.snapshots;").collect()[0].summary

    assert "snapshot_prop_a" in summary
    assert summary["snapshot_prop_a"] == "test_prop_a"


@pytest.mark.integration
def test_add_files_fails_on_schema_mismatch(spark: SparkSession, session_catalog: Catalog, format_version: int) -> None:
    identifier = f"default.table_schema_mismatch_fails_v{format_version}"

    tbl = _create_table(session_catalog, identifier, format_version)
    WRONG_SCHEMA = pa.schema(
        [
            ("foo", pa.bool_()),
            ("bar", pa.string()),
            ("baz", pa.string()),  # should be integer
            ("qux", pa.date32()),
        ]
    )
    file_path = f"s3://warehouse/default/table_schema_mismatch_fails/v{format_version}/test.parquet"
    # write parquet files
    fo = tbl.io.new_output(file_path)
    with fo.create(overwrite=True) as fos:
        with pq.ParquetWriter(fos, schema=WRONG_SCHEMA) as writer:
            writer.write_table(
                pa.Table.from_pylist(
                    [
                        {
                            "foo": True,
                            "bar": "bar_string",
                            "baz": "123",
                            "qux": date(2024, 3, 7),
                        },
                        {
                            "foo": True,
                            "bar": "bar_string",
                            "baz": "124",
                            "qux": date(2024, 3, 7),
                        },
                    ],
                    schema=WRONG_SCHEMA,
                )
            )

    expected = """Mismatch in fields:
┏━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━┓
┃    ┃ Table field              ┃ Dataframe field          ┃
┡━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━┩
│ ✅ │ 1: foo: optional boolean │ 1: foo: optional boolean │
│ ✅ │ 2: bar: optional string  │ 2: bar: optional string  │
│ ❌ │ 3: baz: optional int     │ 3: baz: optional string  │
│ ✅ │ 4: qux: optional date    │ 4: qux: optional date    │
└────┴──────────────────────────┴──────────────────────────┘
"""

    with pytest.raises(ValueError, match=expected):
        tbl.add_files(file_paths=[file_path])


@pytest.mark.integration
def test_add_files_with_field_ids_fails_on_schema_mismatch(
    spark: SparkSession, session_catalog: Catalog, format_version: int
) -> None:
    """Test that files with mismatched field types (when field IDs match) are rejected."""
    identifier = f"default.table_schema_mismatch_based_on_field_ids__fails_v{format_version}"

    tbl = _create_table(session_catalog, identifier, format_version)

    # All fields are renamed and reordered but have matching field IDs, so they should be compatible
    # except for 'baz' which has the wrong type
    WRONG_SCHEMA = pa.schema(
        [
            pa.field("qux_", pa.date32(), metadata={"PARQUET:field_id": "4"}),
            pa.field("baz_", pa.string(), metadata={"PARQUET:field_id": "3"}),  # Wrong type: should be int32
            pa.field("bar_", pa.string(), metadata={"PARQUET:field_id": "2"}),
            pa.field("foo_", pa.bool_(), metadata={"PARQUET:field_id": "1"}),
        ]
    )
    file_path = f"s3://warehouse/default/table_with_field_ids_schema_mismatch_fails/v{format_version}/test.parquet"
    # write parquet files
    fo = tbl.io.new_output(file_path)
    with fo.create(overwrite=True) as fos:
        with pq.ParquetWriter(fos, schema=WRONG_SCHEMA) as writer:
            writer.write_table(
                pa.Table.from_pylist(
                    [
                        {
                            "qux_": date(2024, 3, 7),
                            "baz_": "123",
                            "bar_": "bar_string",
                            "foo_": True,
                        },
                        {
                            "qux_": date(2024, 3, 7),
                            "baz_": "124",
                            "bar_": "bar_string",
                            "foo_": True,
                        },
                    ],
                    schema=WRONG_SCHEMA,
                )
            )

    expected = """Mismatch in fields:
┏━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
┃    ┃ Table field              ┃ Dataframe field           ┃
┡━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━┩
│ ✅ │ 1: foo: optional boolean │ 1: foo_: optional boolean │
│ ✅ │ 2: bar: optional string  │ 2: bar_: optional string  │
│ ❌ │ 3: baz: optional int     │ 3: baz_: optional string  │
│ ✅ │ 4: qux: optional date    │ 4: qux_: optional date    │
└────┴──────────────────────────┴───────────────────────────┘
"""

    with pytest.raises(ValueError, match=expected):
        tbl.add_files(file_paths=[file_path])


@pytest.mark.integration
def test_add_files_with_large_and_regular_schema(spark: SparkSession, session_catalog: Catalog, format_version: int) -> None:
    identifier = f"default.unpartitioned_with_large_types{format_version}"

    iceberg_schema = Schema(NestedField(1, "foo", StringType(), required=True))
    arrow_schema = pa.schema(
        [
            pa.field("foo", pa.string(), nullable=False),
        ]
    )

    tbl = _create_table(session_catalog, identifier, format_version, schema=iceberg_schema)

    file_path = f"s3://warehouse/default/unpartitioned_with_large_types/v{format_version}/test-0.parquet"
    _write_parquet(
        tbl.io,
        file_path,
        arrow_schema,
        pa.Table.from_pylist(
            [
                {
                    "foo": "normal",
                }
            ],
            schema=arrow_schema,
        ),
    )

    tbl.add_files([file_path])

    table_schema = tbl.scan().to_arrow().schema
    assert table_schema == arrow_schema

    file_path_large = f"s3://warehouse/default/unpartitioned_with_large_types/v{format_version}/test-1.parquet"
    _write_parquet(
        tbl.io,
        file_path_large,
        arrow_schema,
        pa.Table.from_pylist(
            [
                {
                    "foo": "normal",
                }
            ],
            schema=arrow_schema,
        ),
    )

    tbl.add_files([file_path_large])

    table_schema = tbl.scan().to_arrow().schema
    assert table_schema == arrow_schema


@pytest.mark.integration
def test_add_files_with_timestamp_tz_ns_fails(session_catalog: Catalog, format_version: int, mocker: MockerFixture) -> None:
    nanoseconds_schema_iceberg = Schema(NestedField(1, "quux", TimestamptzType()))

    nanoseconds_schema = pa.schema(
        [
            ("quux", pa.timestamp("ns", tz="UTC")),
        ]
    )

    arrow_table = pa.Table.from_pylist(
        [
            {
                "quux": 1615967687249846175,  # 2021-03-17 07:54:47.249846159
            }
        ],
        schema=nanoseconds_schema,
    )
    mocker.patch.dict(os.environ, values={"PYICEBERG_DOWNCAST_NS_TIMESTAMP_TO_US_ON_WRITE": "True"})

    identifier = f"default.timestamptz_ns_added{format_version}"
    tbl = _create_table(session_catalog, identifier, format_version, schema=nanoseconds_schema_iceberg)

    file_path = f"s3://warehouse/default/test_timestamp_tz/v{format_version}/test.parquet"
    # write parquet files
    fo = tbl.io.new_output(file_path)
    with fo.create(overwrite=True) as fos:
        with pq.ParquetWriter(fos, schema=nanoseconds_schema) as writer:
            writer.write_table(arrow_table)

    # add the parquet files as data files
    with pytest.raises(
        UnsupportedPyArrowTypeException,
        match=re.escape("Column 'quux' has an unsupported type: timestamp[ns, tz=UTC]"),
    ) as exc_info:
        tbl.add_files(file_paths=[file_path])

    exception_cause = exc_info.value.__cause__
    assert isinstance(exception_cause, TypeError)
    assert (
        "Iceberg does not yet support 'ns' timestamp precision. Use 'downcast-ns-timestamp-to-us-on-write' "
        "configuration property to automatically downcast 'ns' to 'us' on write." in exception_cause.args[0]
    )


@pytest.mark.integration
@pytest.mark.parametrize("format_version", [1, 2])
def test_add_file_with_valid_nullability_diff(spark: SparkSession, session_catalog: Catalog, format_version: int) -> None:
    identifier = f"default.test_table_with_valid_nullability_diff{format_version}"
    table_schema = Schema(
        NestedField(field_id=1, name="long", field_type=LongType(), required=False),
    )
    other_schema = pa.schema(
        (
            pa.field("long", pa.int64(), nullable=False),  # can support writing required pyarrow field to optional Iceberg field
        )
    )
    arrow_table = pa.Table.from_pydict(
        {
            "long": [1, 9],
        },
        schema=other_schema,
    )
    tbl = _create_table(session_catalog, identifier, format_version, schema=table_schema)

    file_path = f"s3://warehouse/default/test_add_file_with_valid_nullability_diff/v{format_version}/test.parquet"
    # write parquet files
    fo = tbl.io.new_output(file_path)
    with fo.create(overwrite=True) as fos:
        with pq.ParquetWriter(fos, schema=other_schema) as writer:
            writer.write_table(arrow_table)

    tbl.add_files(file_paths=[file_path])
    # table's long field should cast to be optional on read
    written_arrow_table = tbl.scan().to_arrow()
    assert written_arrow_table == arrow_table.cast(pa.schema((pa.field("long", pa.int64(), nullable=True),)))
    lhs = spark.table(f"{identifier}").toPandas()
    rhs = written_arrow_table.to_pandas()

    for column in written_arrow_table.column_names:
        for left, right in zip(lhs[column].to_list(), rhs[column].to_list(), strict=True):
            assert left == right


@pytest.mark.integration
@pytest.mark.parametrize("format_version", [1, 2])
def test_add_files_with_valid_upcast(
    spark: SparkSession,
    session_catalog: Catalog,
    format_version: int,
    table_schema_with_promoted_types: Schema,
    pyarrow_schema_with_promoted_types: pa.Schema,
    pyarrow_table_with_promoted_types: pa.Table,
) -> None:
    identifier = f"default.test_table_with_valid_upcast{format_version}"
    tbl = _create_table(session_catalog, identifier, format_version, schema=table_schema_with_promoted_types)

    file_path = f"s3://warehouse/default/test_add_files_with_valid_upcast/v{format_version}/test.parquet"
    # write parquet files
    fo = tbl.io.new_output(file_path)
    with fo.create(overwrite=True) as fos:
        with pq.ParquetWriter(fos, schema=pyarrow_schema_with_promoted_types) as writer:
            writer.write_table(pyarrow_table_with_promoted_types)

    tbl.add_files(file_paths=[file_path], check_duplicate_files=False)
    # table's long field should cast to long on read
    written_arrow_table = tbl.scan().to_arrow()
    assert written_arrow_table == pyarrow_table_with_promoted_types.cast(
        pa.schema(
            (
                pa.field("long", pa.int64(), nullable=True),
                pa.field("list", pa.list_(pa.int64()), nullable=False),
                pa.field("map", pa.map_(pa.string(), pa.int64()), nullable=False),
                pa.field("double", pa.float64(), nullable=True),
                pa.field("uuid", pa.uuid(), nullable=True),
            )
        )
    )
    lhs = spark.table(f"{identifier}").toPandas()
    rhs = written_arrow_table.to_pandas()

    for column in written_arrow_table.column_names:
        for left, right in zip(lhs[column].to_list(), rhs[column].to_list(), strict=True):
            if column == "map":
                # Arrow returns a list of tuples, instead of a dict
                right = dict(right)
            if column == "list":
                # Arrow returns an array, convert to list for equality check
                left, right = list(left), list(right)
            if column == "uuid":
                # Spark Iceberg represents UUID as hex string like '715a78ef-4e53-4089-9bf9-3ad0ee9bf545'
                # whereas PyIceberg represents UUID as bytes on read
                left, right = left.replace("-", ""), right.hex()
            assert left == right


@pytest.mark.integration
@pytest.mark.parametrize("format_version", [1, 2])
def test_add_files_subset_of_schema(spark: SparkSession, session_catalog: Catalog, format_version: int) -> None:
    identifier = f"default.test_table_subset_of_schema{format_version}"
    tbl = _create_table(session_catalog, identifier, format_version)

    file_path = f"s3://warehouse/default/test_add_files_subset_of_schema/v{format_version}/test.parquet"
    arrow_table_without_some_columns = ARROW_TABLE.combine_chunks().drop(ARROW_TABLE.column_names[0])

    # write parquet files
    fo = tbl.io.new_output(file_path)
    with fo.create(overwrite=True) as fos:
        with pq.ParquetWriter(fos, schema=arrow_table_without_some_columns.schema) as writer:
            writer.write_table(arrow_table_without_some_columns)

    tbl.add_files(file_paths=[file_path])
    written_arrow_table = tbl.scan().to_arrow()
    assert tbl.scan().to_arrow() == pa.Table.from_pylist(
        [
            {
                "foo": None,  # Missing column is read as None on read
                "bar": "bar_string",
                "baz": 123,
                "qux": date(2024, 3, 7),
            }
        ],
        schema=ARROW_SCHEMA,
    )

    lhs = spark.table(f"{identifier}").toPandas()
    rhs = written_arrow_table.to_pandas()

    for column in written_arrow_table.column_names:
        for left, right in zip(lhs[column].to_list(), rhs[column].to_list(), strict=True):
            assert left == right


@pytest.mark.integration
@pytest.mark.parametrize("format_version", [1, 2])
def test_add_files_with_duplicate_files_in_file_paths(spark: SparkSession, session_catalog: Catalog, format_version: int) -> None:
    identifier = f"default.test_table_duplicate_add_files_v{format_version}"
    tbl = _create_table(session_catalog, identifier, format_version)
    file_path = "s3://warehouse/default/unpartitioned/v{format_version}/test-1.parquet"
    file_paths = [file_path, file_path]

    # add the parquet files as data files
    with pytest.raises(ValueError) as exc_info:
        tbl.add_files(file_paths=file_paths)
    assert "File paths must be unique" in str(exc_info.value)


@pytest.mark.integration
@pytest.mark.parametrize("format_version", [1, 2])
def test_add_files_that_referenced_by_current_snapshot(
    spark: SparkSession, session_catalog: Catalog, format_version: int
) -> None:
    identifier = f"default.test_table_add_referenced_file_v{format_version}"
    tbl = _create_table(session_catalog, identifier, format_version)

    file_paths = [f"s3://warehouse/default/unpartitioned/v{format_version}/test-{i}.parquet" for i in range(5)]

    # write parquet files
    for file_path in file_paths:
        fo = tbl.io.new_output(file_path)
        with fo.create(overwrite=True) as fos:
            with pq.ParquetWriter(fos, schema=ARROW_SCHEMA) as writer:
                writer.write_table(ARROW_TABLE)

    # add the parquet files as data files
    tbl.add_files(file_paths=file_paths)
    existing_files_in_table = tbl.inspect.files().to_pylist().pop()["file_path"]

    with pytest.raises(ValueError) as exc_info:
        tbl.add_files(file_paths=[existing_files_in_table])
    assert f"Cannot add files that are already referenced by table, files: {existing_files_in_table}" in str(exc_info.value)


@pytest.mark.integration
@pytest.mark.parametrize("format_version", [1, 2])
def test_add_files_that_referenced_by_current_snapshot_with_check_duplicate_files_false(
    spark: SparkSession, session_catalog: Catalog, format_version: int
) -> None:
    identifier = f"default.test_table_add_referenced_file_v{format_version}"
    tbl = _create_table(session_catalog, identifier, format_version)

    file_paths = [f"s3://warehouse/default/unpartitioned/v{format_version}/test-{i}.parquet" for i in range(5)]
    # write parquet files
    for file_path in file_paths:
        fo = tbl.io.new_output(file_path)
        with fo.create(overwrite=True) as fos:
            with pq.ParquetWriter(fos, schema=ARROW_SCHEMA) as writer:
                writer.write_table(ARROW_TABLE)

    # add the parquet files as data files
    tbl.add_files(file_paths=file_paths)
    existing_files_in_table = tbl.inspect.files().to_pylist().pop()["file_path"]
    tbl.add_files(file_paths=[existing_files_in_table], check_duplicate_files=False)
    rows = spark.sql(
        f"""
        SELECT added_data_files_count, existing_data_files_count, deleted_data_files_count
        FROM {identifier}.all_manifests
    """
    ).collect()
    assert [row.added_data_files_count for row in rows] == [5, 1, 5]
    assert [row.existing_data_files_count for row in rows] == [0, 0, 0]
    assert [row.deleted_data_files_count for row in rows] == [0, 0, 0]


@pytest.mark.integration
@pytest.mark.parametrize("format_version", [1, 2])
def test_add_files_that_referenced_by_current_snapshot_with_check_duplicate_files_true(
    spark: SparkSession, session_catalog: Catalog, format_version: int
) -> None:
    identifier = f"default.test_table_add_referenced_file_v{format_version}"
    tbl = _create_table(session_catalog, identifier, format_version)

    file_paths = [f"s3://warehouse/default/unpartitioned/v{format_version}/test-{i}.parquet" for i in range(5)]
    # write parquet files
    for file_path in file_paths:
        fo = tbl.io.new_output(file_path)
        with fo.create(overwrite=True) as fos:
            with pq.ParquetWriter(fos, schema=ARROW_SCHEMA) as writer:
                writer.write_table(ARROW_TABLE)

    # add the parquet files as data files
    tbl.add_files(file_paths=file_paths)
    existing_files_in_table = tbl.inspect.files().to_pylist().pop()["file_path"]
    with pytest.raises(ValueError) as exc_info:
        tbl.add_files(file_paths=[existing_files_in_table], check_duplicate_files=True)
    assert f"Cannot add files that are already referenced by table, files: {existing_files_in_table}" in str(exc_info.value)


@pytest.mark.integration
def test_add_files_hour_transform(session_catalog: Catalog) -> None:
    identifier = "default.test_add_files_hour_transform"

    schema = Schema(NestedField(1, "hourly", TimestampType()))
    schema_arrow = schema_to_pyarrow(schema, include_field_ids=False)
    spec = PartitionSpec(PartitionField(source_id=1, field_id=1000, transform=HourTransform(), name="spec_hour"))

    tbl = _create_table(session_catalog, identifier, format_version=1, schema=schema, partition_spec=spec)

    file_path = "s3://warehouse/default/test_add_files_hour_transform/test.parquet"

    from pyiceberg.utils.datetime import micros_to_timestamp

    arrow_table = pa.Table.from_pylist(
        [{"hourly": micros_to_timestamp(1743465600155254)}, {"hourly": micros_to_timestamp(1743469198047855)}],
        schema=schema_arrow,
    )

    fo = tbl.io.new_output(file_path)
    with fo.create(overwrite=True) as fos:
        with pq.ParquetWriter(fos, schema=schema_arrow) as writer:
            writer.write_table(arrow_table)

    tbl.add_files(file_paths=[file_path])


@pytest.mark.integration
def test_add_files_to_branch(spark: SparkSession, session_catalog: Catalog, format_version: int) -> None:
    identifier = f"default.test_add_files_branch_v{format_version}"
    branch = "branch1"

    tbl = _create_table(session_catalog, identifier, format_version)

    file_paths = [f"s3://warehouse/default/addfile/v{format_version}/test-{i}.parquet" for i in range(5)]
    # write parquet files
    for file_path in file_paths:
        fo = tbl.io.new_output(file_path)
        with fo.create(overwrite=True) as fos:
            with pq.ParquetWriter(fos, schema=ARROW_SCHEMA) as writer:
                writer.write_table(ARROW_TABLE)

    # Dummy write to avoid failures on creating branch in empty table
    tbl.append(ARROW_TABLE)
    assert tbl.metadata.current_snapshot_id is not None
    tbl.manage_snapshots().create_branch(snapshot_id=tbl.metadata.current_snapshot_id, branch_name=branch).commit()

    # add the parquet files as data files
    tbl.add_files(file_paths=file_paths, branch=branch)

    df = spark.table(identifier)
    assert df.count() == 1, "Expected 1 row in Main table"

    branch_df = spark.table(f"{identifier}.branch_{branch}")
    assert branch_df.count() == 6, "Expected 5 rows in branch"

    for col in branch_df.columns:
        assert branch_df.filter(branch_df[col].isNotNull()).count() == 6, "Expected all 6 rows to be non-null"
