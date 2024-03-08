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

from pyiceberg.catalog import Catalog, Table
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import IdentityTransform
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

PARTITION_SPEC = PartitionSpec(
    PartitionField(source_id=4, field_id=1000, transform=IdentityTransform(), name="baz"),
    PartitionField(source_id=10, field_id=1001, transform=IdentityTransform(), name="qux"),
    spec_id=0,
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
def test_add_files_to_partitioned_table(spark: SparkSession, session_catalog: Catalog) -> None:
    identifier = "default.partitioned_table"
    tbl = _create_table(session_catalog, identifier, PARTITION_SPEC)

    file_paths = [f"s3://warehouse/default/baz=123/qux=2024-03-07/test-{i}.parquet" for i in range(5)]
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
