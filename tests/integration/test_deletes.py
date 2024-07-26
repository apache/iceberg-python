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
from typing import List

import pyarrow as pa
import pytest
from pyspark.sql import SparkSession

from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.expressions import AlwaysTrue, EqualTo
from pyiceberg.manifest import ManifestEntryStatus
from pyiceberg.schema import Schema
from pyiceberg.table.snapshots import Operation, Summary
from pyiceberg.types import FloatType, IntegerType, NestedField


def run_spark_commands(spark: SparkSession, sqls: List[str]) -> None:
    for sql in sqls:
        spark.sql(sql)


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


@pytest.mark.integration
@pytest.mark.parametrize("format_version", [1, 2])
def test_partitioned_table_rewrite(spark: SparkSession, session_catalog: RestCatalog, format_version: int) -> None:
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
    tbl.delete(EqualTo("number", 20))

    # We don't delete a whole partition, so there is only a overwrite
    assert [snapshot.summary.operation.value for snapshot in tbl.snapshots()] == ["append", "append", "overwrite"]
    assert tbl.scan().to_arrow().to_pydict() == {"number_partitioned": [11, 10], "number": [30, 30]}


@pytest.mark.integration
@pytest.mark.parametrize("format_version", [1, 2])
def test_rewrite_partitioned_table_with_null(spark: SparkSession, session_catalog: RestCatalog, format_version: int) -> None:
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
            INSERT INTO {identifier} VALUES (11, 20), (11, NULL)
        """,
        ],
    )

    tbl = session_catalog.load_table(identifier)
    tbl.delete(EqualTo("number", 20))

    # We don't delete a whole partition, so there is only a overwrite
    assert [snapshot.summary.operation.value for snapshot in tbl.snapshots()] == ["append", "append", "overwrite"]
    assert tbl.scan().to_arrow().to_pydict() == {"number_partitioned": [11, 10], "number": [None, 30]}


@pytest.mark.integration
@pytest.mark.parametrize("format_version", [1, 2])
def test_partitioned_table_no_match(spark: SparkSession, session_catalog: RestCatalog, format_version: int) -> None:
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
        ],
    )

    tbl = session_catalog.load_table(identifier)
    tbl.delete(EqualTo("number_partitioned", 22))  # Does not affect any data

    assert [snapshot.summary.operation.value for snapshot in tbl.snapshots()] == ["append"]
    assert tbl.scan().to_arrow().to_pydict() == {"number_partitioned": [10, 10], "number": [20, 30]}


@pytest.mark.integration
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

    # Assert that there is just a single Parquet file, that has one merge on read file
    files = list(tbl.scan().plan_files())
    assert len(files) == 1
    assert len(files[0].delete_files) == 1

    # Will rewrite a data file without the positional delete
    tbl.delete(EqualTo("number", 40))

    # One positional delete has been added, but an OVERWRITE status is set
    # https://github.com/apache/iceberg/issues/10122
    assert [snapshot.summary.operation.value for snapshot in tbl.snapshots()] == ["append", "overwrite", "overwrite"]
    assert tbl.scan().to_arrow().to_pydict() == {"number_partitioned": [10], "number": [20]}


@pytest.mark.integration
def test_overwrite_partitioned_table(spark: SparkSession, session_catalog: RestCatalog) -> None:
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
            INSERT INTO {identifier} VALUES (10, 1), (10, 2), (20, 3)
        """,
        ],
    )

    tbl = session_catalog.load_table(identifier)

    files = list(tbl.scan().plan_files())
    assert len(files) == 2

    arrow_schema = pa.schema([pa.field("number_partitioned", pa.int32()), pa.field("number", pa.int32())])
    arrow_tbl = pa.Table.from_pylist(
        [
            {"number_partitioned": 10, "number": 4},
            {"number_partitioned": 10, "number": 5},
        ],
        schema=arrow_schema,
    )

    # Will rewrite a data file without the positional delete
    tbl.overwrite(arrow_tbl, "number_partitioned == 10")

    # One positional delete has been added, but an OVERWRITE status is set
    # https://github.com/apache/iceberg/issues/10122
    assert [snapshot.summary.operation.value for snapshot in tbl.snapshots()] == ["append", "delete", "append"]
    assert tbl.scan().to_arrow().to_pydict() == {"number_partitioned": [10, 10, 20], "number": [4, 5, 3]}


@pytest.mark.integration
def test_partitioned_table_positional_deletes_sequence_number(spark: SparkSession, session_catalog: RestCatalog) -> None:
    identifier = "default.table_partitioned_delete_sequence_number"

    # This test case is a bit more complex. Here we run a MoR delete on a file, we make sure that
    # the manifest gets rewritten (but not the data file with a MoR), and check if the delete is still there
    # to assure that the sequence numbers are maintained

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
            INSERT INTO {identifier} VALUES (10, 100), (10, 101), (20, 200), (20, 201), (20, 202)
        """,
            # Generate a positional delete
            f"""
            DELETE FROM {identifier} WHERE number = 101
        """,
        ],
    )

    tbl = session_catalog.load_table(identifier)

    files = list(tbl.scan().plan_files())
    assert len(files) == 2

    # Will rewrite a data file without a positional delete
    tbl.delete(EqualTo("number", 201))

    # One positional delete has been added, but an OVERWRITE status is set
    # https://github.com/apache/iceberg/issues/10122
    snapshots = tbl.snapshots()
    assert len(snapshots) == 3

    # Snapshots produced by Spark
    assert [snapshot.summary.operation.value for snapshot in tbl.snapshots()[0:2]] == ["append", "overwrite"]

    # Will rewrite one parquet file
    assert snapshots[2].summary == Summary(
        Operation.OVERWRITE,
        **{
            "added-files-size": snapshots[2].summary["total-files-size"],
            "added-data-files": "1",
            "added-records": "2",
            "changed-partition-count": "1",
            "total-files-size": snapshots[2].summary["total-files-size"],
            "total-delete-files": "0",
            "total-data-files": "1",
            "total-position-deletes": "0",
            "total-records": "2",
            "total-equality-deletes": "0",
            "deleted-data-files": "2",
            "removed-delete-files": "1",
            "deleted-records": "5",
            "removed-files-size": snapshots[2].summary["removed-files-size"],
            "removed-position-deletes": "1",
        },
    )

    assert tbl.scan().to_arrow().to_pydict() == {"number_partitioned": [20, 20, 10], "number": [200, 202, 100]}


@pytest.mark.integration
def test_delete_no_match(session_catalog: RestCatalog) -> None:
    arrow_schema = pa.schema([pa.field("ints", pa.int32())])
    arrow_tbl = pa.Table.from_pylist(
        [
            {"ints": 1},
            {"ints": 3},
        ],
        schema=arrow_schema,
    )

    iceberg_schema = Schema(NestedField(1, "ints", IntegerType()))

    tbl_identifier = "default.test_delete_no_match"

    try:
        session_catalog.drop_table(tbl_identifier)
    except NoSuchTableError:
        pass

    tbl = session_catalog.create_table(tbl_identifier, iceberg_schema)
    tbl.append(arrow_tbl)

    assert [snapshot.summary.operation for snapshot in tbl.snapshots()] == [Operation.APPEND]

    tbl.delete("ints == 2")  # Only 1 and 3 in the file, but is between the lower and upper bound

    assert [snapshot.summary.operation for snapshot in tbl.snapshots()] == [Operation.APPEND]


@pytest.mark.integration
def test_delete_overwrite(session_catalog: RestCatalog) -> None:
    arrow_schema = pa.schema([pa.field("ints", pa.int32())])
    arrow_tbl = pa.Table.from_pylist(
        [
            {"ints": 1},
            {"ints": 2},
        ],
        schema=arrow_schema,
    )

    iceberg_schema = Schema(NestedField(1, "ints", IntegerType()))

    tbl_identifier = "default.test_delete_overwrite"

    try:
        session_catalog.drop_table(tbl_identifier)
    except NoSuchTableError:
        pass

    tbl = session_catalog.create_table(tbl_identifier, iceberg_schema)
    tbl.append(arrow_tbl)

    assert [snapshot.summary.operation for snapshot in tbl.snapshots()] == [Operation.APPEND]

    arrow_tbl_overwrite = pa.Table.from_pylist(
        [
            {"ints": 3},
            {"ints": 4},
        ],
        schema=arrow_schema,
    )
    tbl.overwrite(arrow_tbl_overwrite, "ints == 2")  # Should rewrite one file

    assert [snapshot.summary.operation for snapshot in tbl.snapshots()] == [
        Operation.APPEND,
        Operation.OVERWRITE,
        Operation.APPEND,
    ]

    assert tbl.scan().to_arrow()["ints"].to_pylist() == [3, 4, 1]


@pytest.mark.integration
def test_delete_truncate(session_catalog: RestCatalog) -> None:
    arrow_schema = pa.schema([pa.field("ints", pa.int32())])
    arrow_tbl = pa.Table.from_pylist(
        [
            {"ints": 1},
        ],
        schema=arrow_schema,
    )

    iceberg_schema = Schema(NestedField(1, "ints", IntegerType()))

    tbl_identifier = "default.test_delete_overwrite"

    try:
        session_catalog.drop_table(tbl_identifier)
    except NoSuchTableError:
        pass

    tbl = session_catalog.create_table(tbl_identifier, iceberg_schema)
    tbl.append(arrow_tbl)

    # Effectively a truncate
    tbl.delete(delete_filter=AlwaysTrue())

    manifests = tbl.current_snapshot().manifests(tbl.io)
    assert len(manifests) == 1

    entries = manifests[0].fetch_manifest_entry(tbl.io, discard_deleted=False)
    assert len(entries) == 1

    assert entries[0].status == ManifestEntryStatus.DELETED


@pytest.mark.integration
def test_delete_overwrite_table_with_null(session_catalog: RestCatalog) -> None:
    arrow_schema = pa.schema([pa.field("ints", pa.int32())])
    arrow_tbl = pa.Table.from_pylist(
        [{"ints": 1}, {"ints": 2}, {"ints": None}],
        schema=arrow_schema,
    )

    iceberg_schema = Schema(NestedField(1, "ints", IntegerType()))

    tbl_identifier = "default.test_delete_overwrite_with_null"

    try:
        session_catalog.drop_table(tbl_identifier)
    except NoSuchTableError:
        pass

    tbl = session_catalog.create_table(tbl_identifier, iceberg_schema)
    tbl.append(arrow_tbl)

    assert [snapshot.summary.operation for snapshot in tbl.snapshots()] == [Operation.APPEND]

    arrow_tbl_overwrite = pa.Table.from_pylist(
        [
            {"ints": 3},
            {"ints": 4},
        ],
        schema=arrow_schema,
    )
    tbl.overwrite(arrow_tbl_overwrite, "ints == 2")  # Should rewrite one file

    assert [snapshot.summary.operation for snapshot in tbl.snapshots()] == [
        Operation.APPEND,
        Operation.OVERWRITE,
        Operation.APPEND,
    ]

    assert tbl.scan().to_arrow()["ints"].to_pylist() == [3, 4, 1, None]


@pytest.mark.integration
def test_delete_overwrite_table_with_nan(session_catalog: RestCatalog) -> None:
    arrow_schema = pa.schema([pa.field("floats", pa.float32())])

    # Create Arrow Table with NaN values
    data = [pa.array([1.0, float("nan"), 2.0], type=pa.float32())]
    arrow_tbl = pa.Table.from_arrays(
        data,
        schema=arrow_schema,
    )

    iceberg_schema = Schema(NestedField(1, "floats", FloatType()))

    tbl_identifier = "default.test_delete_overwrite_with_nan"

    try:
        session_catalog.drop_table(tbl_identifier)
    except NoSuchTableError:
        pass

    tbl = session_catalog.create_table(tbl_identifier, iceberg_schema)
    tbl.append(arrow_tbl)

    assert [snapshot.summary.operation for snapshot in tbl.snapshots()] == [Operation.APPEND]

    arrow_tbl_overwrite = pa.Table.from_pylist(
        [
            {"floats": 3.0},
            {"floats": 4.0},
        ],
        schema=arrow_schema,
    )
    """
    We want to test the _expression_to_complementary_pyarrow function can generate a correct complimentary filter
    for selecting records to remain in the new overwritten file.
    Compared with test_delete_overwrite_table_with_null which tests rows with null cells,
    nan testing is faced with a more tricky issue:
    A filter of (field == value) will not include cells of nan but col != val will.
    (Interestingly, neither == or != will include null)

    This means if we set the test case as floats == 2.0 (equal predicate as in test_delete_overwrite_table_with_null),
    test will pass even without the logic under test
    in _NullNaNUnmentionedTermsCollector (a helper of _expression_to_complementary_pyarrow
    to handle revert of iceberg expression of is_null/not_null/is_nan/not_nan).
    Instead, we test the filter of !=, so that the revert is == which exposes the issue.
    """
    tbl.overwrite(arrow_tbl_overwrite, "floats != 2.0")  # Should rewrite one file

    assert [snapshot.summary.operation for snapshot in tbl.snapshots()] == [
        Operation.APPEND,
        Operation.OVERWRITE,
        Operation.APPEND,
    ]

    result = tbl.scan().to_arrow()["floats"].to_pylist()

    from math import isnan

    assert any(isnan(e) for e in result)
    assert 2.0 in result
    assert 3.0 in result
    assert 4.0 in result
