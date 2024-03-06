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
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pathlib import Path

from pyiceberg.catalog import Catalog, Properties, Table
from pyiceberg.io.pyarrow import schema_to_pyarrow
from pyiceberg.schema import Schema
from pyiceberg.types import (
    BooleanType,
    IntegerType,
    NestedField,
    StringType,
)
from pyiceberg.exceptions import NoSuchTableError
from pyspark.sql import SparkSession

TABLE_SCHEMA = Schema(
    NestedField(field_id=1, name="foo", field_type=BooleanType(), required=False),
    NestedField(field_id=2, name="bar", field_type=StringType(), required=False),
    NestedField(field_id=4, name="baz", field_type=IntegerType(), required=False),
)

ARROW_TABLE = pa.Table.from_pylist(
            [
                {
                    "foo": True,
                    "bar": "bar_string",
                    "baz": 123,
                }
            ],
            schema=schema_to_pyarrow(TABLE_SCHEMA),
        )

def _create_table(session_catalog: Catalog, identifier: str) -> Table:
    try:
        session_catalog.drop_table(identifier=identifier)
    except NoSuchTableError:
        pass

    tbl = session_catalog.create_table(identifier=identifier, schema=TABLE_SCHEMA)

    return tbl

@pytest.mark.integration
def test_add_files_to_unpartitioned_table(spark: SparkSession, session_catalog: Catalog, warehouse: Path) -> None:
    identifier = "default.unpartitioned_table"
    tbl = _create_table(session_catalog, identifier)
    # rows = spark.sql(
    #     f"""
    #     SELECT added_data_files_count, existing_data_files_count, deleted_data_files_count
    #     FROM {identifier}.all_manifests
    # """
    # ).collect()

    # assert [row.added_data_files_count for row in rows] == []
    # assert [row.existing_data_files_count for row in rows] == []
    # assert [row.deleted_data_files_count for row in rows] == []

    file_paths = [f"/{warehouse}/test-{i}.parquet" for i in range(5)]
    # write parquet files
    for file_path in file_paths:
        fo = tbl.io.new_output(file_path)
        with fo.create(overwrite=True) as fos:
            with pq.ParquetWriter(fos, schema=ARROW_TABLE.schema) as writer:
                writer.write_table(ARROW_TABLE)

    # add the parquet files as data files
    tbl.add_files(file_paths)

    rows = spark.sql(
        f"""
        SELECT added_data_files_count, existing_data_files_count, deleted_data_files_count
        FROM {identifier}.all_manifests
    """
    ).collect()

    assert [row.added_data_files_count for row in rows] == [5]
    assert [row.existing_data_files_count for row in rows] == [0]
    assert [row.deleted_data_files_count for row in rows] == [0]