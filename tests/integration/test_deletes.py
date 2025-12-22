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
from collections.abc import Generator
from datetime import datetime

import pyarrow as pa
import pytest
from pyspark.sql import SparkSession

from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.expressions import AlwaysTrue, EqualTo, LessThanOrEqual
from pyiceberg.manifest import ManifestEntryStatus
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.table.snapshots import Operation, Summary
from pyiceberg.transforms import IdentityTransform
from pyiceberg.types import FloatType, IntegerType, LongType, NestedField, StringType, TimestampType


def run_spark_commands(spark: SparkSession, sqls: list[str]) -> None:
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
@pytest.mark.filterwarnings("ignore:Delete operation did not match any records")
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

    # Assert that there is just a single Parquet file, that has one merge on read file
    files = list(tbl.scan().plan_files())
    assert len(files) == 1
    assert len(files[0].delete_files) == 1

    # Will rewrite a data file without the positional delete
    tbl.delete(EqualTo("number", 40))

    assert [snapshot.summary.operation.value for snapshot in tbl.snapshots()] == ["append", "delete", "overwrite"]
    assert tbl.scan().to_arrow().to_pydict() == {"number_partitioned": [10], "number": [20]}


@pytest.mark.integration
@pytest.mark.filterwarnings("ignore:Merge on read is not yet supported, falling back to copy-on-write")
def test_delete_partitioned_table_positional_deletes_empty_batch(spark: SparkSession, session_catalog: RestCatalog) -> None:
    identifier = "default.test_delete_partitioned_table_positional_deletes_empty_batch"

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
                'write.merge.mode'='merge-on-read',
                'write.parquet.row-group-limit'=1
            )
        """,
        ],
    )

    tbl = session_catalog.load_table(identifier)

    arrow_table = pa.Table.from_arrays(
        [
            pa.array([10, 10, 10]),
            pa.array([1, 2, 3]),
        ],
        schema=pa.schema([pa.field("number_partitioned", pa.int32()), pa.field("number", pa.int32())]),
    )

    tbl.append(arrow_table)

    assert len(tbl.scan().to_arrow()) == 3

    run_spark_commands(
        spark,
        [
            # Generate a positional delete
            f"""
            DELETE FROM {identifier} WHERE number = 1
        """,
        ],
    )
    # Assert that there is just a single Parquet file, that has one merge on read file
    tbl = tbl.refresh()

    files = list(tbl.scan().plan_files())
    assert len(files) == 1
    assert len(files[0].delete_files) == 1

    assert len(tbl.scan().to_arrow()) == 2

    assert len(tbl.scan(row_filter="number_partitioned == 10").to_arrow()) == 2

    assert len(tbl.scan(row_filter="number_partitioned == 1").to_arrow()) == 0

    reader = tbl.scan(row_filter="number_partitioned == 1").to_arrow_batch_reader()
    assert isinstance(reader, pa.RecordBatchReader)
    assert len(reader.read_all()) == 0


@pytest.mark.integration
@pytest.mark.filterwarnings("ignore:Merge on read is not yet supported, falling back to copy-on-write")
def test_read_multiple_batches_in_task_with_position_deletes(spark: SparkSession, session_catalog: RestCatalog) -> None:
    identifier = "default.test_read_multiple_batches_in_task_with_position_deletes"

    run_spark_commands(
        spark,
        [
            f"DROP TABLE IF EXISTS {identifier}",
            f"""
            CREATE TABLE {identifier} (
                number              int
            )
            USING iceberg
            TBLPROPERTIES(
                'format-version' = 2,
                'write.delete.mode'='merge-on-read',
                'write.update.mode'='merge-on-read',
                'write.merge.mode'='merge-on-read'
            )
        """,
        ],
    )

    tbl = session_catalog.load_table(identifier)

    arrow_table = pa.Table.from_arrays(
        [
            pa.array(list(range(1, 1001)) * 100),
        ],
        schema=pa.schema([pa.field("number", pa.int32())]),
    )

    tbl.append(arrow_table)

    run_spark_commands(
        spark,
        [
            f"""
            DELETE FROM {identifier} WHERE number in (1, 2, 3, 4)
        """,
        ],
    )

    tbl.refresh()

    reader = tbl.scan(row_filter="number <= 50").to_arrow_batch_reader()
    assert isinstance(reader, pa.RecordBatchReader)
    pyiceberg_count = len(reader.read_all())
    expected_count = 46 * 100
    assert pyiceberg_count == expected_count, f"Failing check. {pyiceberg_count} != {expected_count}"


@pytest.mark.integration
@pytest.mark.filterwarnings("ignore:Merge on read is not yet supported, falling back to copy-on-write")
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

    assert [snapshot.summary.operation.value for snapshot in tbl.snapshots()] == ["append", "delete", "append"]
    assert tbl.scan().to_arrow().to_pydict() == {"number_partitioned": [10, 10, 20], "number": [4, 5, 3]}


@pytest.mark.integration
@pytest.mark.filterwarnings("ignore:Merge on read is not yet supported, falling back to copy-on-write")
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

    snapshots = tbl.snapshots()
    assert len(snapshots) == 3

    # Snapshots produced by Spark
    assert [snapshot.summary.operation.value for snapshot in tbl.snapshots()[0:2]] == ["append", "delete"]

    # Will rewrite one parquet file
    assert snapshots[2].summary == Summary(
        Operation.OVERWRITE,
        **{
            "added-data-files": "1",
            "added-files-size": snapshots[2].summary["added-files-size"],
            "added-records": "2",
            "changed-partition-count": "1",
            "deleted-data-files": "1",
            "deleted-records": "3",
            "removed-files-size": snapshots[2].summary["removed-files-size"],
            "total-data-files": "2",
            "total-delete-files": "1",
            "total-equality-deletes": "0",
            "total-files-size": snapshots[2].summary["total-files-size"],
            "total-position-deletes": "1",
            "total-records": "4",
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


@pytest.mark.integration
def test_delete_after_partition_evolution_from_unpartitioned(session_catalog: RestCatalog) -> None:
    identifier = "default.test_delete_after_partition_evolution_from_unpartitioned"

    arrow_table = pa.Table.from_arrays(
        [
            pa.array([2, 3, 4, 5, 6]),
        ],
        names=["idx"],
    )

    try:
        session_catalog.drop_table(identifier)
    except NoSuchTableError:
        pass

    tbl = session_catalog.create_table(
        identifier,
        schema=Schema(
            NestedField(1, "idx", LongType()),
        ),
    )

    tbl.append(arrow_table)

    with tbl.transaction() as tx:
        with tx.update_schema() as schema:
            schema.rename_column("idx", "id")
        with tx.update_spec() as spec:
            spec.add_field("id", IdentityTransform())

    # Append one more time to create data files with two partition specs
    tbl.append(arrow_table.rename_columns(["id"]))

    tbl.delete("id == 4")

    # Expect 8 records: 10 records - 2
    assert len(tbl.scan().to_arrow()) == 8


@pytest.mark.integration
def test_delete_after_partition_evolution_from_partitioned(session_catalog: RestCatalog) -> None:
    identifier = "default.test_delete_after_partition_evolution_from_partitioned"

    arrow_table = pa.Table.from_arrays(
        [
            pa.array([2, 3, 4, 5, 6]),
            pa.array(
                [
                    datetime(2021, 5, 19),
                    datetime(2022, 7, 25),
                    datetime(2023, 3, 22),
                    datetime(2024, 7, 17),
                    datetime(2025, 2, 22),
                ]
            ),
        ],
        names=["idx", "ts"],
    )

    try:
        session_catalog.drop_table(identifier)
    except NoSuchTableError:
        pass

    tbl = session_catalog.create_table(
        identifier,
        schema=Schema(NestedField(1, "idx", LongType()), NestedField(2, "ts", TimestampType())),
        partition_spec=PartitionSpec(PartitionField(source_id=2, field_id=1000, transform=IdentityTransform(), name="ts")),
    )

    tbl.append(arrow_table)

    with tbl.transaction() as tx:
        with tx.update_schema() as schema:
            schema.rename_column("idx", "id")
        with tx.update_spec() as spec:
            spec.add_field("id", IdentityTransform())

    # Append one more time to create data files with two partition specs
    tbl.append(arrow_table.rename_columns(["id", "ts"]))

    tbl.delete("id == 4")

    # Expect 8 records: 10 records - 2
    assert len(tbl.scan().to_arrow()) == 8


@pytest.mark.integration
def test_delete_with_filter_case_sensitive_by_default(test_table: Table) -> None:
    record_to_delete = {"idx": 2, "value": "b"}
    assert record_to_delete in test_table.scan().to_arrow().to_pylist()

    with pytest.raises(ValueError) as e:
        test_table.delete(f"Idx == {record_to_delete['idx']}")
    assert "Could not find field with name Idx" in str(e.value)
    assert record_to_delete in test_table.scan().to_arrow().to_pylist()

    test_table.delete(f"idx == {record_to_delete['idx']}")
    assert record_to_delete not in test_table.scan().to_arrow().to_pylist()


@pytest.mark.integration
def test_delete_with_filter_case_sensitive(test_table: Table) -> None:
    record_to_delete = {"idx": 2, "value": "b"}
    assert record_to_delete in test_table.scan().to_arrow().to_pylist()

    with pytest.raises(ValueError) as e:
        test_table.delete(f"Idx == {record_to_delete['idx']}", case_sensitive=True)
    assert "Could not find field with name Idx" in str(e.value)
    assert record_to_delete in test_table.scan().to_arrow().to_pylist()

    test_table.delete(f"idx == {record_to_delete['idx']}", case_sensitive=True)
    assert record_to_delete not in test_table.scan().to_arrow().to_pylist()


@pytest.mark.integration
def test_delete_with_filter_case_insensitive(test_table: Table) -> None:
    record_to_delete_1 = {"idx": 2, "value": "b"}
    record_to_delete_2 = {"idx": 3, "value": "c"}
    assert record_to_delete_1 in test_table.scan().to_arrow().to_pylist()
    assert record_to_delete_2 in test_table.scan().to_arrow().to_pylist()

    test_table.delete(f"Idx == {record_to_delete_1['idx']}", case_sensitive=False)
    assert record_to_delete_1 not in test_table.scan().to_arrow().to_pylist()

    test_table.delete(f"idx == {record_to_delete_2['idx']}", case_sensitive=False)
    assert record_to_delete_2 not in test_table.scan().to_arrow().to_pylist()


@pytest.mark.integration
def test_overwrite_with_filter_case_sensitive_by_default(test_table: Table) -> None:
    record_to_overwrite = {"idx": 2, "value": "b"}
    assert record_to_overwrite in test_table.scan().to_arrow().to_pylist()

    new_record_to_insert = {"idx": 10, "value": "x"}
    new_table = pa.Table.from_arrays(
        [
            pa.array([new_record_to_insert["idx"]]),
            pa.array([new_record_to_insert["value"]]),
        ],
        names=["idx", "value"],
    )

    with pytest.raises(ValueError) as e:
        test_table.overwrite(df=new_table, overwrite_filter=f"Idx == {record_to_overwrite['idx']}")
    assert "Could not find field with name Idx" in str(e.value)
    assert record_to_overwrite in test_table.scan().to_arrow().to_pylist()
    assert new_record_to_insert not in test_table.scan().to_arrow().to_pylist()

    test_table.overwrite(df=new_table, overwrite_filter=f"idx == {record_to_overwrite['idx']}")
    assert record_to_overwrite not in test_table.scan().to_arrow().to_pylist()
    assert new_record_to_insert in test_table.scan().to_arrow().to_pylist()


@pytest.mark.integration
def test_overwrite_with_filter_case_sensitive(test_table: Table) -> None:
    record_to_overwrite = {"idx": 2, "value": "b"}
    assert record_to_overwrite in test_table.scan().to_arrow().to_pylist()

    new_record_to_insert = {"idx": 10, "value": "x"}
    new_table = pa.Table.from_arrays(
        [
            pa.array([new_record_to_insert["idx"]]),
            pa.array([new_record_to_insert["value"]]),
        ],
        names=["idx", "value"],
    )

    with pytest.raises(ValueError) as e:
        test_table.overwrite(df=new_table, overwrite_filter=f"Idx == {record_to_overwrite['idx']}", case_sensitive=True)
    assert "Could not find field with name Idx" in str(e.value)
    assert record_to_overwrite in test_table.scan().to_arrow().to_pylist()
    assert new_record_to_insert not in test_table.scan().to_arrow().to_pylist()

    test_table.overwrite(df=new_table, overwrite_filter=f"idx == {record_to_overwrite['idx']}", case_sensitive=True)
    assert record_to_overwrite not in test_table.scan().to_arrow().to_pylist()
    assert new_record_to_insert in test_table.scan().to_arrow().to_pylist()


@pytest.mark.integration
def test_overwrite_with_filter_case_insensitive(test_table: Table) -> None:
    record_to_overwrite = {"idx": 2, "value": "b"}
    assert record_to_overwrite in test_table.scan().to_arrow().to_pylist()

    new_record_to_insert = {"idx": 10, "value": "x"}
    new_table = pa.Table.from_arrays(
        [
            pa.array([new_record_to_insert["idx"]]),
            pa.array([new_record_to_insert["value"]]),
        ],
        names=["idx", "value"],
    )

    test_table.overwrite(df=new_table, overwrite_filter=f"Idx == {record_to_overwrite['idx']}", case_sensitive=False)
    assert record_to_overwrite not in test_table.scan().to_arrow().to_pylist()
    assert new_record_to_insert in test_table.scan().to_arrow().to_pylist()


@pytest.mark.integration
@pytest.mark.parametrize("format_version", [1, 2])
@pytest.mark.filterwarnings("ignore:Delete operation did not match any records")
def test_delete_on_empty_table(spark: SparkSession, session_catalog: RestCatalog, format_version: int) -> None:
    identifier = f"default.test_delete_on_empty_table_{format_version}"

    run_spark_commands(
        spark,
        [
            f"DROP TABLE IF EXISTS {identifier}",
            f"""
            CREATE TABLE {identifier} (
                volume              int
            )
            USING iceberg
            TBLPROPERTIES('format-version' = {format_version})
        """,
        ],
    )

    tbl = session_catalog.load_table(identifier)

    # Perform a delete operation on the empty table
    tbl.delete(AlwaysTrue())

    # Assert that no new snapshot was created because no rows were deleted
    assert len(tbl.snapshots()) == 0


@pytest.mark.integration
def test_manifest_entry_after_deletes(session_catalog: RestCatalog) -> None:
    identifier = "default.test_manifest_entry_after_deletes"
    try:
        session_catalog.drop_table(identifier)
    except NoSuchTableError:
        pass

    schema = pa.schema(
        [
            ("id", pa.int32()),
            ("name", pa.string()),
        ]
    )

    table = session_catalog.create_table(identifier, schema)
    data = pa.Table.from_pylist(
        [
            {"id": 1, "name": "foo"},
            {"id": 2, "name": "bar"},
            {"id": 3, "name": "bar"},
            {"id": 4, "name": "bar"},
        ],
        schema=schema,
    )
    table.append(data)

    def assert_manifest_entry(expected_status: ManifestEntryStatus, expected_snapshot_id: int) -> None:
        current_snapshot = table.refresh().current_snapshot()
        assert current_snapshot is not None

        manifest_files = current_snapshot.manifests(table.io)
        assert len(manifest_files) == 1

        entries = manifest_files[0].fetch_manifest_entry(table.io, discard_deleted=False)
        assert len(entries) == 1
        entry = entries[0]
        assert entry.status == expected_status
        assert entry.snapshot_id == expected_snapshot_id

    before_delete_snapshot = table.current_snapshot()
    assert before_delete_snapshot is not None

    assert_manifest_entry(ManifestEntryStatus.ADDED, before_delete_snapshot.snapshot_id)

    table.delete(LessThanOrEqual("id", 4))
    after_delete_snapshot = table.refresh().current_snapshot()
    assert after_delete_snapshot is not None

    assert_manifest_entry(ManifestEntryStatus.DELETED, after_delete_snapshot.snapshot_id)
