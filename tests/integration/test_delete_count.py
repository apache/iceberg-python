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
from datetime import datetime
from typing import Generator, List

import pyarrow as pa
import pytest
from pyspark.sql import SparkSession

from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.expressions import AlwaysTrue, EqualTo
from pyiceberg.manifest import ManifestEntryStatus
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.table.snapshots import Operation, Summary
from pyiceberg.transforms import IdentityTransform
from pyiceberg.types import FloatType, IntegerType, LongType, NestedField, StringType, TimestampType


def run_spark_commands(spark: SparkSession, sqls: List[str]) -> None:
    for sql in sqls:
        spark.sql(sql)


@pytest.fixture()
def test_table(session_catalog: RestCatalog) -> Generator[Table, None, None]:
    identifier = "default.__test_table"
    arrow_table = pa.Table.from_arrays([pa.array([1, 2, 3, 4, 5]), pa.array(["a", "b", "c", "d", "e"])], names=["idx", "value"])
    test_table = session_catalog.create_table(
        identifier,
        schema=Schema(
            NestedField(1, "idx", LongType()),
            NestedField(2, "value", StringType()),
        ),
    )
    test_table.append(arrow_table)

    yield test_table

    session_catalog.drop_table(identifier)


@pytest.mark.integration
@pytest.mark.parametrize("format_version", [1, 2])
def test_partitioned_table_delete_full_file(spark: SparkSession, session_catalog: RestCatalog, format_version: int) -> None:
    identifier = "default.table_partitioned_delete"
    run_spark_commands(
        spark,
        [
            f"DROP TABLE IF EXISTS {identifier}",
            f"""
            CREATE TABLE {identifier} (
                number_partitioned  int,
                number              int
            )
            USING iceberg
            PARTITIONED BY (number_partitioned)
            TBLPROPERTIES('format-version' = {format_version})
        """,
            f"""
            INSERT INTO {identifier} VALUES (10, 20), (10, 30)
        """,
            f"""
            INSERT INTO {identifier} VALUES (11, 20), (11, 30)
        """,
        ],
    )

    tbl = session_catalog.load_table(identifier)
    tbl.delete(EqualTo("number_partitioned", 10))

    # No overwrite operation
    assert [snapshot.summary.operation.value for snapshot in tbl.snapshots()] == ["append", "append", "delete"]
    assert tbl.scan().to_arrow().to_pydict() == {"number_partitioned": [11, 11], "number": [20, 30]}

    assert tbl.scan().count() == len(tbl.scan().to_arrow())


@pytest.mark.integration
@pytest.mark.filterwarnings("ignore:Merge on read is not yet supported, falling back to copy-on-write")
def test_delete_partitioned_table_positional_deletes(spark: SparkSession, session_catalog: RestCatalog) -> None:
    identifier = "default.table_partitioned_delete"

    run_spark_commands(
        spark,
        [
            f"DROP TABLE IF EXISTS {identifier}",
            f"""
            CREATE TABLE {identifier} (
                number_partitioned  int,
                number              int
            )
            USING iceberg
            PARTITIONED BY (number_partitioned)
            TBLPROPERTIES(
                'format-version' = 2,
                'write.delete.mode'='merge-on-read',
                'write.update.mode'='merge-on-read',
                'write.merge.mode'='merge-on-read'
            )
        """,
            f"""
            INSERT INTO {identifier} VALUES (10, 20), (10, 30), (10, 40)
        """,
            # Generate a positional delete
            f"""
            DELETE FROM {identifier} WHERE number = 30
        """,
        ],
    )

    tbl = session_catalog.load_table(identifier)

    assert tbl.scan().count() == len(tbl.scan().to_arrow())

    # Will rewrite a data file without the positional delete
    tbl.delete(EqualTo("number", 40))

    assert [snapshot.summary.operation.value for snapshot in tbl.snapshots()] == ["append", "overwrite", "overwrite"]
    assert tbl.scan().to_arrow().to_pydict() == {"number_partitioned": [10], "number": [20]}

    assert tbl.scan().count() == len(tbl.scan().to_arrow())

    run_spark_commands(
        spark,
        [
            f"""
            INSERT INTO {identifier} VALUES (10, 20), (10, 30), (10, 40), (20, 30)
        """,
            # Generate a positional delete
            f"""
            DELETE FROM {identifier} WHERE number = 30
        """,
        ],
    )

    tbl = session_catalog.load_table(identifier)

    assert tbl.scan().count() == len(tbl.scan().to_arrow())


    run_spark_commands(
        spark,
        [
            # Generate a positional delete
            f"""
            DELETE FROM {identifier} WHERE number = 30
        """,
            f"""
            INSERT INTO {identifier} VALUES (10, 20), (10, 30), (10, 40), (20, 30)
        """,
            # Generate a positional delete
            f"""
                DELETE FROM {identifier} WHERE number = 20
            """,
        ],
    )

    tbl = session_catalog.load_table(identifier)

    assert tbl.scan().count() == len(tbl.scan().to_arrow())


    filter_on_partition = "number_partitioned = 10"
    scan_on_partition = tbl.scan(row_filter=filter_on_partition)
    assert  scan_on_partition.count() == len(scan_on_partition.to_arrow())


    filter = "number = 10"
    scan = tbl.scan(row_filter=filter)
    assert  scan.count() == len(scan.to_arrow())

