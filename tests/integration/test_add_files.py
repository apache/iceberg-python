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

from datetime import date
from typing import Optional

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pyspark.sql import SparkSession

from pyiceberg.catalog import Catalog
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.partitioning import PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.types import (
    BooleanType,
    DateType,
    IntegerType,
    NestedField,
    StringType,
)

TABLE_SCHEMA = Schema(
    NestedField(field_id=1, name="foo", field_type=BooleanType(), required=False),
    NestedField(field_id=2, name="bar", field_type=StringType(), required=False),
    NestedField(field_id=4, name="baz", field_type=IntegerType(), required=False),
    NestedField(field_id=10, name="qux", field_type=DateType(), required=False),
)

ARROW_SCHEMA = pa.schema([
    ("foo", pa.bool_()),
    ("bar", pa.string()),
    ("baz", pa.int32()),
    ("qux", pa.date32()),
])

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

ARROW_SCHEMA_WITH_IDS = pa.schema([
    pa.field('foo', pa.bool_(), nullable=False, metadata={"PARQUET:field_id": "1"}),
    pa.field('bar', pa.string(), nullable=False, metadata={"PARQUET:field_id": "2"}),
    pa.field('baz', pa.int32(), nullable=False, metadata={"PARQUET:field_id": "3"}),
    pa.field('qux', pa.date32(), nullable=False, metadata={"PARQUET:field_id": "4"}),
])


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

ARROW_SCHEMA_UPDATED = pa.schema([
    ("foo", pa.bool_()),
    ("baz", pa.int32()),
    ("qux", pa.date32()),
    ("quux", pa.int32()),
])

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


def _create_table(session_catalog: Catalog, identifier: str, partition_spec: Optional[PartitionSpec] = None) -> Table:
    try:
        session_catalog.drop_table(identifier=identifier)
    except NoSuchTableError:
        pass

    tbl = session_catalog.create_table(
        identifier=identifier, schema=TABLE_SCHEMA, partition_spec=partition_spec if partition_spec else PartitionSpec()
    )

    return tbl


@pytest.mark.integration
def test_add_files_to_unpartitioned_table(spark: SparkSession, session_catalog: Catalog) -> None:
    identifier = "default.unpartitioned_table"
    tbl = _create_table(session_catalog, identifier)

    file_paths = [f"s3://warehouse/default/unpartitioned/test-{i}.parquet" for i in range(5)]
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


@pytest.mark.integration
def test_add_files_to_unpartitioned_table_raises_file_not_found(spark: SparkSession, session_catalog: Catalog) -> None:
    identifier = "default.unpartitioned_raises_not_found"
    tbl = _create_table(session_catalog, identifier)

    file_paths = [f"s3://warehouse/default/unpartitioned_raises_not_found/test-{i}.parquet" for i in range(5)]
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
def test_add_files_to_unpartitioned_table_raises_has_field_ids(spark: SparkSession, session_catalog: Catalog) -> None:
    identifier = "default.unpartitioned_raises_field_ids"
    tbl = _create_table(session_catalog, identifier)

    file_paths = [f"s3://warehouse/default/unpartitioned_raises_field_ids/test-{i}.parquet" for i in range(5)]
    # write parquet files
    for file_path in file_paths:
        fo = tbl.io.new_output(file_path)
        with fo.create(overwrite=True) as fos:
            with pq.ParquetWriter(fos, schema=ARROW_SCHEMA_WITH_IDS) as writer:
                writer.write_table(ARROW_TABLE_WITH_IDS)

    # add the parquet files as data files
    with pytest.raises(NotImplementedError):
        tbl.add_files(file_paths=file_paths)


@pytest.mark.integration
def test_add_files_to_unpartitioned_table_with_schema_updates(spark: SparkSession, session_catalog: Catalog) -> None:
    identifier = "default.unpartitioned_table_2"
    tbl = _create_table(session_catalog, identifier)

    file_paths = [f"s3://warehouse/default/unpartitioned_2/test-{i}.parquet" for i in range(5)]
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

    file_path = "s3://warehouse/default/unpartitioned_2/test-6.parquet"
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
