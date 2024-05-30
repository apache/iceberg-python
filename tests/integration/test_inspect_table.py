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

import math
from datetime import date, datetime

import pyarrow as pa
import pytest
import pytz
from pyspark.sql import DataFrame, SparkSession

from pyiceberg.catalog import Catalog
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.typedef import Properties
from pyiceberg.types import (
    BinaryType,
    BooleanType,
    DateType,
    DoubleType,
    FixedType,
    FloatType,
    IntegerType,
    LongType,
    NestedField,
    StringType,
    TimestampType,
    TimestamptzType,
)

TABLE_SCHEMA = Schema(
    NestedField(field_id=1, name="bool", field_type=BooleanType(), required=False),
    NestedField(field_id=2, name="string", field_type=StringType(), required=False),
    NestedField(field_id=3, name="string_long", field_type=StringType(), required=False),
    NestedField(field_id=4, name="int", field_type=IntegerType(), required=False),
    NestedField(field_id=5, name="long", field_type=LongType(), required=False),
    NestedField(field_id=6, name="float", field_type=FloatType(), required=False),
    NestedField(field_id=7, name="double", field_type=DoubleType(), required=False),
    NestedField(field_id=8, name="timestamp", field_type=TimestampType(), required=False),
    NestedField(field_id=9, name="timestamptz", field_type=TimestamptzType(), required=False),
    NestedField(field_id=10, name="date", field_type=DateType(), required=False),
    # NestedField(field_id=11, name="time", field_type=TimeType(), required=False),
    # NestedField(field_id=12, name="uuid", field_type=UuidType(), required=False),
    NestedField(field_id=12, name="binary", field_type=BinaryType(), required=False),
    NestedField(field_id=13, name="fixed", field_type=FixedType(16), required=False),
)


def _create_table(session_catalog: Catalog, identifier: str, properties: Properties) -> Table:
    try:
        session_catalog.drop_table(identifier=identifier)
    except NoSuchTableError:
        pass

    return session_catalog.create_table(identifier=identifier, schema=TABLE_SCHEMA, properties=properties)


@pytest.mark.integration
@pytest.mark.parametrize("format_version", [1, 2])
def test_inspect_snapshots(
    spark: SparkSession, session_catalog: Catalog, arrow_table_with_null: pa.Table, format_version: int
) -> None:
    identifier = "default.table_metadata_snapshots"
    tbl = _create_table(session_catalog, identifier, properties={"format-version": format_version})

    tbl.overwrite(arrow_table_with_null)
    # should produce a DELETE entry
    tbl.overwrite(arrow_table_with_null)
    # Since we don't rewrite, this should produce a new manifest with an ADDED entry
    tbl.append(arrow_table_with_null)

    df = tbl.inspect.snapshots()

    assert df.column_names == [
        "committed_at",
        "snapshot_id",
        "parent_id",
        "operation",
        "manifest_list",
        "summary",
    ]

    for committed_at in df["committed_at"]:
        assert isinstance(committed_at.as_py(), datetime)

    for snapshot_id in df["snapshot_id"]:
        assert isinstance(snapshot_id.as_py(), int)

    assert df["parent_id"][0].as_py() is None
    assert df["parent_id"][1:] == df["snapshot_id"][:2]

    assert [operation.as_py() for operation in df["operation"]] == ["append", "overwrite", "append"]

    for manifest_list in df["manifest_list"]:
        assert manifest_list.as_py().startswith("s3://")

    assert df["summary"][0].as_py() == [
        ("added-files-size", "5459"),
        ("added-data-files", "1"),
        ("added-records", "3"),
        ("total-data-files", "1"),
        ("total-delete-files", "0"),
        ("total-records", "3"),
        ("total-files-size", "5459"),
        ("total-position-deletes", "0"),
        ("total-equality-deletes", "0"),
    ]

    lhs = spark.table(f"{identifier}.snapshots").toPandas()
    rhs = df.to_pandas()
    for column in df.column_names:
        for left, right in zip(lhs[column].to_list(), rhs[column].to_list()):
            if column == "summary":
                # Arrow returns a list of tuples, instead of a dict
                right = dict(right)

            if isinstance(left, float) and math.isnan(left) and isinstance(right, float) and math.isnan(right):
                # NaN != NaN in Python
                continue

            assert left == right, f"Difference in column {column}: {left} != {right}"


@pytest.mark.integration
@pytest.mark.parametrize("format_version", [1, 2])
def test_inspect_entries(
    spark: SparkSession, session_catalog: Catalog, arrow_table_with_null: pa.Table, format_version: int
) -> None:
    identifier = "default.table_metadata_entries"
    tbl = _create_table(session_catalog, identifier, properties={"format-version": format_version})

    # Write some data
    tbl.append(arrow_table_with_null)

    def check_pyiceberg_df_equals_spark_df(df: pa.Table, spark_df: DataFrame) -> None:
        assert df.column_names == [
            "status",
            "snapshot_id",
            "sequence_number",
            "file_sequence_number",
            "data_file",
            "readable_metrics",
        ]

        # Make sure that they are filled properly
        for int_column in ["status", "snapshot_id", "sequence_number", "file_sequence_number"]:
            for value in df[int_column]:
                assert isinstance(value.as_py(), int)

        for snapshot_id in df["snapshot_id"]:
            assert isinstance(snapshot_id.as_py(), int)

        lhs = df.to_pandas()
        rhs = spark_df.toPandas()
        for column in df.column_names:
            for left, right in zip(lhs[column].to_list(), rhs[column].to_list()):
                if column == "data_file":
                    for df_column in left.keys():
                        if df_column == "partition":
                            # Spark leaves out the partition if the table is unpartitioned
                            continue

                        df_lhs = left[df_column]
                        df_rhs = right[df_column]
                        if isinstance(df_rhs, dict):
                            # Arrow turns dicts into lists of tuple
                            df_lhs = dict(df_lhs)

                        assert df_lhs == df_rhs, f"Difference in data_file column {df_column}: {df_lhs} != {df_rhs}"
                elif column == "readable_metrics":
                    assert list(left.keys()) == [
                        "bool",
                        "string",
                        "string_long",
                        "int",
                        "long",
                        "float",
                        "double",
                        "timestamp",
                        "timestamptz",
                        "date",
                        "binary",
                        "fixed",
                    ]

                    assert left.keys() == right.keys()

                    for rm_column in left.keys():
                        rm_lhs = left[rm_column]
                        rm_rhs = right[rm_column]

                        assert rm_lhs["column_size"] == rm_rhs["column_size"]
                        assert rm_lhs["value_count"] == rm_rhs["value_count"]
                        assert rm_lhs["null_value_count"] == rm_rhs["null_value_count"]
                        assert rm_lhs["nan_value_count"] == rm_rhs["nan_value_count"]

                        if rm_column == "timestamptz":
                            # PySpark does not correctly set the timstamptz
                            rm_rhs["lower_bound"] = rm_rhs["lower_bound"].replace(tzinfo=pytz.utc)
                            rm_rhs["upper_bound"] = rm_rhs["upper_bound"].replace(tzinfo=pytz.utc)

                        assert rm_lhs["lower_bound"] == rm_rhs["lower_bound"]
                        assert rm_lhs["upper_bound"] == rm_rhs["upper_bound"]
                else:
                    assert left == right, f"Difference in column {column}: {left} != {right}"

    for snapshot in tbl.metadata.snapshots:
        df = tbl.inspect.entries(snapshot_id=snapshot.snapshot_id)
        spark_df = spark.sql(f"SELECT * FROM {identifier}.entries VERSION AS OF {snapshot.snapshot_id}")
        check_pyiceberg_df_equals_spark_df(df, spark_df)


@pytest.mark.integration
@pytest.mark.parametrize("format_version", [1, 2])
def test_inspect_entries_partitioned(spark: SparkSession, session_catalog: Catalog, format_version: int) -> None:
    identifier = "default.table_metadata_entries_partitioned"
    try:
        session_catalog.drop_table(identifier=identifier)
    except NoSuchTableError:
        pass

    spark.sql(
        f"""
        CREATE TABLE {identifier} (
            dt date
        )
        PARTITIONED BY (months(dt))
    """
    )

    spark.sql(
        f"""
        INSERT INTO {identifier} VALUES (CAST('2021-01-01' AS date))
    """
    )

    spark.sql(
        f"""
        ALTER TABLE {identifier}
        REPLACE PARTITION FIELD dt_month WITH days(dt)
    """
    )

    spark.sql(
        f"""
        INSERT INTO {identifier} VALUES (CAST('2021-02-01' AS date))
    """
    )

    df = session_catalog.load_table(identifier).inspect.entries()

    assert df.to_pydict()["data_file"][0]["partition"] == {"dt_day": date(2021, 2, 1), "dt_month": None}
    assert df.to_pydict()["data_file"][1]["partition"] == {"dt_day": None, "dt_month": 612}


@pytest.mark.integration
@pytest.mark.parametrize("format_version", [1, 2])
def test_inspect_refs(
    spark: SparkSession, session_catalog: Catalog, arrow_table_with_null: pa.Table, format_version: int
) -> None:
    identifier = "default.table_metadata_refs"
    tbl = _create_table(session_catalog, identifier, properties={"format-version": format_version})

    # write data to create snapshot
    tbl.overwrite(arrow_table_with_null)

    # create a test branch
    spark.sql(
        f"""
    ALTER TABLE {identifier} CREATE BRANCH IF NOT EXISTS testBranch RETAIN 7 DAYS WITH SNAPSHOT RETENTION 2 SNAPSHOTS
        """
    )

    # create a test tag against current snapshot
    current_snapshot = tbl.current_snapshot()
    assert current_snapshot is not None
    current_snapshot_id = current_snapshot.snapshot_id

    spark.sql(
        f"""
    ALTER TABLE {identifier} CREATE TAG testTag AS OF VERSION {current_snapshot_id} RETAIN 180 DAYS
        """
    )

    df = tbl.refresh().inspect.refs()

    assert df.column_names == [
        "name",
        "type",
        "snapshot_id",
        "max_reference_age_in_ms",
        "min_snapshots_to_keep",
        "max_snapshot_age_in_ms",
    ]

    assert [name.as_py() for name in df["name"]] == ["testBranch", "main", "testTag"]
    assert [ref_type.as_py() for ref_type in df["type"]] == ["BRANCH", "BRANCH", "TAG"]

    for snapshot_id in df["snapshot_id"]:
        assert isinstance(snapshot_id.as_py(), int)

    for int_column in ["max_reference_age_in_ms", "min_snapshots_to_keep", "max_snapshot_age_in_ms"]:
        for value in df[int_column]:
            assert isinstance(value.as_py(), int) or not value.as_py()

    lhs = spark.table(f"{identifier}.refs").toPandas()
    rhs = df.to_pandas()
    for column in df.column_names:
        for left, right in zip(lhs[column].to_list(), rhs[column].to_list()):
            if isinstance(left, float) and math.isnan(left) and isinstance(right, float) and math.isnan(right):
                # NaN != NaN in Python
                continue
            assert left == right, f"Difference in column {column}: {left} != {right}"


@pytest.mark.integration
@pytest.mark.parametrize("format_version", [1, 2])
def test_inspect_partitions_unpartitioned(
    spark: SparkSession, session_catalog: Catalog, arrow_table_with_null: pa.Table, format_version: int
) -> None:
    identifier = "default.table_metadata_partitions_unpartitioned"
    tbl = _create_table(session_catalog, identifier, properties={"format-version": format_version})

    # Write some data through multiple commits
    tbl.append(arrow_table_with_null)
    tbl.append(arrow_table_with_null)

    df = tbl.inspect.partitions()
    assert df.column_names == [
        "record_count",
        "file_count",
        "total_data_file_size_in_bytes",
        "position_delete_record_count",
        "position_delete_file_count",
        "equality_delete_record_count",
        "equality_delete_file_count",
        "last_updated_at",
        "last_updated_snapshot_id",
    ]
    for last_updated_at in df["last_updated_at"]:
        assert isinstance(last_updated_at.as_py(), datetime)

    int_cols = [
        "record_count",
        "file_count",
        "total_data_file_size_in_bytes",
        "position_delete_record_count",
        "position_delete_file_count",
        "equality_delete_record_count",
        "equality_delete_file_count",
        "last_updated_snapshot_id",
    ]
    for column in int_cols:
        for value in df[column]:
            assert isinstance(value.as_py(), int)
    lhs = df.to_pandas()
    rhs = spark.table(f"{identifier}.partitions").toPandas()
    for column in df.column_names:
        for left, right in zip(lhs[column].to_list(), rhs[column].to_list()):
            assert left == right, f"Difference in column {column}: {left} != {right}"


@pytest.mark.integration
@pytest.mark.parametrize("format_version", [1, 2])
def test_inspect_partitions_partitioned(spark: SparkSession, session_catalog: Catalog, format_version: int) -> None:
    identifier = "default.table_metadata_partitions_partitioned"
    try:
        session_catalog.drop_table(identifier=identifier)
    except NoSuchTableError:
        pass

    spark.sql(
        f"""
        CREATE TABLE {identifier} (
            name string,
            dt date
        )
        PARTITIONED BY (months(dt))
    """
    )

    spark.sql(
        f"""
        INSERT INTO {identifier} VALUES ('John', CAST('2021-01-01' AS date))
    """
    )

    spark.sql(
        f"""
        INSERT INTO {identifier} VALUES ('Doe', CAST('2021-01-05' AS date))
    """
    )

    spark.sql(
        f"""
        ALTER TABLE {identifier}
        REPLACE PARTITION FIELD dt_month WITH days(dt)
    """
    )

    spark.sql(
        f"""
        INSERT INTO {identifier} VALUES ('Jenny', CAST('2021-02-01' AS date))
    """
    )

    spark.sql(
        f"""
        ALTER TABLE {identifier}
        DROP PARTITION FIELD dt_day
    """
    )

    spark.sql(
        f"""
        INSERT INTO {identifier} VALUES ('James', CAST('2021-02-01' AS date))
    """
    )

    def check_pyiceberg_df_equals_spark_df(df: pa.Table, spark_df: DataFrame) -> None:
        lhs = df.to_pandas().sort_values("spec_id")
        rhs = spark_df.toPandas().sort_values("spec_id")
        for column in df.column_names:
            for left, right in zip(lhs[column].to_list(), rhs[column].to_list()):
                assert left == right, f"Difference in column {column}: {left} != {right}"

    tbl = session_catalog.load_table(identifier)
    for snapshot in tbl.metadata.snapshots:
        df = tbl.inspect.partitions(snapshot_id=snapshot.snapshot_id)
        spark_df = spark.sql(f"SELECT * FROM {identifier}.partitions VERSION AS OF {snapshot.snapshot_id}")
        check_pyiceberg_df_equals_spark_df(df, spark_df)


@pytest.mark.integration
@pytest.mark.parametrize("format_version", [1, 2])
def test_inspect_manifests(spark: SparkSession, session_catalog: Catalog, format_version: int) -> None:
    identifier = "default.table_metadata_manifests"
    try:
        session_catalog.drop_table(identifier=identifier)
    except NoSuchTableError:
        pass

    spark.sql(
        f"""
        CREATE TABLE {identifier} (
            id int,
            data string
        )
        PARTITIONED BY (data)
    """
    )

    spark.sql(
        f"""
        INSERT INTO {identifier} VALUES (1, "a")
    """
    )

    spark.sql(
        f"""
        INSERT INTO {identifier} VALUES (2, "b")
    """
    )

    df = session_catalog.load_table(identifier).inspect.manifests()

    assert df.column_names == [
        "content",
        "path",
        "length",
        "partition_spec_id",
        "added_snapshot_id",
        "added_data_files_count",
        "existing_data_files_count",
        "deleted_data_files_count",
        "added_delete_files_count",
        "existing_delete_files_count",
        "deleted_delete_files_count",
        "partition_summaries",
    ]

    int_cols = [
        "content",
        "length",
        "partition_spec_id",
        "added_snapshot_id",
        "added_data_files_count",
        "existing_data_files_count",
        "deleted_data_files_count",
        "added_delete_files_count",
        "existing_delete_files_count",
        "deleted_delete_files_count",
    ]

    for column in int_cols:
        for value in df[column]:
            assert isinstance(value.as_py(), int)

    for value in df["path"]:
        assert isinstance(value.as_py(), str)

    for value in df["partition_summaries"]:
        assert isinstance(value.as_py(), list)
        for row in value:
            assert isinstance(row["contains_null"].as_py(), bool)
            assert isinstance(row["contains_nan"].as_py(), (bool, type(None)))
            assert isinstance(row["lower_bound"].as_py(), (str, type(None)))
            assert isinstance(row["upper_bound"].as_py(), (str, type(None)))

    lhs = spark.table(f"{identifier}.manifests").toPandas()
    rhs = df.to_pandas()
    for column in df.column_names:
        for left, right in zip(lhs[column].to_list(), rhs[column].to_list()):
            assert left == right, f"Difference in column {column}: {left} != {right}"
