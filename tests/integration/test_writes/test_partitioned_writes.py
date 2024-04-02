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
import pytest
from pyspark.sql import SparkSession

from pyiceberg.catalog import Catalog
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.transforms import (
    BucketTransform,
    DayTransform,
    HourTransform,
    IdentityTransform,
    MonthTransform,
    TruncateTransform,
    YearTransform,
)
from tests.conftest import TEST_DATA_WITH_NULL
from utils import TABLE_SCHEMA, _create_table


@pytest.mark.integration
@pytest.mark.parametrize(
    "part_col", ['int', 'bool', 'string', "string_long", "long", "float", "double", "date", 'timestamp', 'timestamptz', 'binary']
)
@pytest.mark.parametrize("format_version", [1, 2])
def test_query_filter_null_partitioned(
    session_catalog: Catalog, spark: SparkSession, arrow_table_with_null: pa.Table, part_col: str, format_version: int
) -> None:
    # Given
    identifier = f"default.arrow_table_v{format_version}_with_null_partitioned_on_col_{part_col}"
    nested_field = TABLE_SCHEMA.find_field(part_col)
    partition_spec = PartitionSpec(
        PartitionField(source_id=nested_field.field_id, field_id=1001, transform=IdentityTransform(), name=part_col)
    )

    # When
    tbl = _create_table(
        session_catalog=session_catalog,
        identifier=identifier,
        properties={"format-version": str(format_version)},
        data=[arrow_table_with_null],
        partition_spec=partition_spec,
    )

    # Then
    assert tbl.format_version == format_version, f"Expected v{format_version}, got: v{tbl.format_version}"
    df = spark.table(identifier)
    assert df.count() == 3, f"Expected 3 total rows for {identifier}"
    for col in TEST_DATA_WITH_NULL.keys():
        assert df.where(f"{col} is not null").count() == 2, f"Expected 2 non-null rows for {col}"
        assert df.where(f"{col} is null").count() == 1, f"Expected 1 null row for {col} is null"


@pytest.mark.integration
@pytest.mark.parametrize(
    "part_col", ['int', 'bool', 'string', "string_long", "long", "float", "double", "date", 'timestamp', 'timestamptz', 'binary']
)
@pytest.mark.parametrize("format_version", [1, 2])
def test_query_filter_without_data_partitioned(
    session_catalog: Catalog, spark: SparkSession, arrow_table_without_data: pa.Table, part_col: str, format_version: int
) -> None:
    # Given
    identifier = f"default.arrow_table_v{format_version}_without_data_partitioned_on_col_{part_col}"
    nested_field = TABLE_SCHEMA.find_field(part_col)
    partition_spec = PartitionSpec(
        PartitionField(source_id=nested_field.field_id, field_id=1001, transform=IdentityTransform(), name=part_col)
    )

    # When
    tbl = _create_table(
        session_catalog=session_catalog,
        identifier=identifier,
        properties={"format-version": str(format_version)},
        data=[arrow_table_without_data],
        partition_spec=partition_spec,
    )

    # Then
    assert tbl.format_version == format_version, f"Expected v{format_version}, got: v{tbl.format_version}"
    df = spark.table(identifier)
    for col in TEST_DATA_WITH_NULL.keys():
        assert df.where(f"{col} is null").count() == 0, f"Expected 0 row for {col}"
        assert df.where(f"{col} is not null").count() == 0, f"Expected 0 row for {col}"


@pytest.mark.integration
@pytest.mark.parametrize(
    "part_col", ['int', 'bool', 'string', "string_long", "long", "float", "double", "date", 'timestamp', 'timestamptz', 'binary']
)
@pytest.mark.parametrize("format_version", [1, 2])
def test_query_filter_only_nulls_partitioned(
    session_catalog: Catalog, spark: SparkSession, arrow_table_with_only_nulls: pa.Table, part_col: str, format_version: int
) -> None:
    # Given
    identifier = f"default.arrow_table_v{format_version}_with_only_nulls_partitioned_on_col_{part_col}"
    nested_field = TABLE_SCHEMA.find_field(part_col)
    partition_spec = PartitionSpec(
        PartitionField(source_id=nested_field.field_id, field_id=1001, transform=IdentityTransform(), name=part_col)
    )

    # When
    tbl = _create_table(
        session_catalog=session_catalog,
        identifier=identifier,
        properties={"format-version": str(format_version)},
        data=[arrow_table_with_only_nulls],
        partition_spec=partition_spec,
    )

    # Then
    assert tbl.format_version == format_version, f"Expected v{format_version}, got: v{tbl.format_version}"
    df = spark.table(identifier)
    for col in TEST_DATA_WITH_NULL.keys():
        assert df.where(f"{col} is null").count() == 2, f"Expected 2 row for {col}"
        assert df.where(f"{col} is not null").count() == 0, f"Expected 0 rows for {col}"


@pytest.mark.integration
@pytest.mark.parametrize(
    "part_col", ['int', 'bool', 'string', "string_long", "long", "float", "double", "date", "timestamptz", "timestamp", "binary"]
)
@pytest.mark.parametrize("format_version", [1, 2])
def test_query_filter_appended_null_partitioned(
    session_catalog: Catalog, spark: SparkSession, arrow_table_with_null: pa.Table, part_col: str, format_version: int
) -> None:
    # Given
    identifier = f"default.arrow_table_v{format_version}_appended_with_null_partitioned_on_col_{part_col}"
    nested_field = TABLE_SCHEMA.find_field(part_col)
    partition_spec = PartitionSpec(
        PartitionField(source_id=nested_field.field_id, field_id=1001, transform=IdentityTransform(), name=part_col)
    )

    # When
    tbl = _create_table(
        session_catalog=session_catalog,
        identifier=identifier,
        properties={"format-version": str(format_version)},
        data=[],
        partition_spec=partition_spec,
    )
    # Append with arrow_table_1 with lines [A,B,C] and then arrow_table_2 with lines[A,B,C,A,B,C]
    tbl.append(arrow_table_with_null)
    tbl.append(pa.concat_tables([arrow_table_with_null, arrow_table_with_null]))

    # Then
    assert tbl.format_version == format_version, f"Expected v{format_version}, got: v{tbl.format_version}"
    df = spark.table(identifier)
    for col in TEST_DATA_WITH_NULL.keys():
        df = spark.table(identifier)
        assert df.where(f"{col} is not null").count() == 6, f"Expected 6 non-null rows for {col}"
        assert df.where(f"{col} is null").count() == 3, f"Expected 3 null rows for {col}"
    # expecting 6 files: first append with [A], [B], [C],  second append with [A, A], [B, B], [C, C]
    rows = spark.sql(f"select partition from {identifier}.files").collect()
    assert len(rows) == 6


@pytest.mark.integration
@pytest.mark.parametrize(
    "part_col", ['int', 'bool', 'string', "string_long", "long", "float", "double", "date", "timestamptz", "timestamp", "binary"]
)
def test_query_filter_v1_v2_append_null(
    session_catalog: Catalog, spark: SparkSession, arrow_table_with_null: pa.Table, part_col: str
) -> None:
    # Given
    identifier = f"default.arrow_table_v1_v2_appended_with_null_partitioned_on_col_{part_col}"
    nested_field = TABLE_SCHEMA.find_field(part_col)
    partition_spec = PartitionSpec(
        PartitionField(source_id=nested_field.field_id, field_id=1001, transform=IdentityTransform(), name=part_col)
    )

    # When
    tbl = _create_table(
        session_catalog=session_catalog,
        identifier=identifier,
        properties={"format-version": "1"},
        data=[],
        partition_spec=partition_spec,
    )
    tbl.append(arrow_table_with_null)

    # Then
    assert tbl.format_version == 1, f"Expected v1, got: v{tbl.format_version}"

    # When
    with tbl.transaction() as tx:
        tx.upgrade_table_version(format_version=2)

    tbl.append(arrow_table_with_null)

    # Then
    assert tbl.format_version == 2, f"Expected v2, got: v{tbl.format_version}"
    for col in TEST_DATA_WITH_NULL.keys():  # type: ignore
        df = spark.table(identifier)
        assert df.where(f"{col} is not null").count() == 4, f"Expected 4 non-null rows for {col}"
        assert df.where(f"{col} is null").count() == 2, f"Expected 2 null rows for {col}"


@pytest.mark.integration
def test_summaries_with_null(spark: SparkSession, session_catalog: Catalog, arrow_table_with_null: pa.Table) -> None:
    identifier = "default.arrow_table_summaries"

    try:
        session_catalog.drop_table(identifier=identifier)
    except NoSuchTableError:
        pass
    tbl = session_catalog.create_table(
        identifier=identifier,
        schema=TABLE_SCHEMA,
        partition_spec=PartitionSpec(PartitionField(source_id=4, field_id=1001, transform=IdentityTransform(), name="int")),
        properties={'format-version': '2'},
    )

    tbl.append(arrow_table_with_null)
    tbl.append(arrow_table_with_null)

    rows = spark.sql(
        f"""
        SELECT operation, summary
        FROM {identifier}.snapshots
        ORDER BY committed_at ASC
    """
    ).collect()

    operations = [row.operation for row in rows]
    assert operations == ['append', 'append']

    summaries = [row.summary for row in rows]
    assert summaries[0] == {
        'changed-partition-count': '3',
        'added-data-files': '3',
        'added-files-size': '15029',
        'added-records': '3',
        'total-data-files': '3',
        'total-delete-files': '0',
        'total-equality-deletes': '0',
        'total-files-size': '15029',
        'total-position-deletes': '0',
        'total-records': '3',
    }

    assert summaries[1] == {
        'changed-partition-count': '3',
        'added-data-files': '3',
        'added-files-size': '15029',
        'added-records': '3',
        'total-data-files': '6',
        'total-delete-files': '0',
        'total-equality-deletes': '0',
        'total-files-size': '30058',
        'total-position-deletes': '0',
        'total-records': '6',
    }


@pytest.mark.integration
def test_data_files_with_table_partitioned_with_null(
    spark: SparkSession, session_catalog: Catalog, arrow_table_with_null: pa.Table
) -> None:
    identifier = "default.arrow_data_files"

    try:
        session_catalog.drop_table(identifier=identifier)
    except NoSuchTableError:
        pass
    tbl = session_catalog.create_table(
        identifier=identifier,
        schema=TABLE_SCHEMA,
        partition_spec=PartitionSpec(PartitionField(source_id=4, field_id=1001, transform=IdentityTransform(), name="int")),
        properties={'format-version': '1'},
    )

    tbl.append(arrow_table_with_null)
    tbl.append(arrow_table_with_null)

    # added_data_files_count, existing_data_files_count, deleted_data_files_count
    rows = spark.sql(
        f"""
        SELECT added_data_files_count, existing_data_files_count, deleted_data_files_count
        FROM {identifier}.all_manifests
    """
    ).collect()

    assert [row.added_data_files_count for row in rows] == [3, 3, 3]
    assert [row.existing_data_files_count for row in rows] == [
        0,
        0,
        0,
    ]
    assert [row.deleted_data_files_count for row in rows] == [0, 0, 0]


@pytest.mark.integration
def test_invalid_arguments(spark: SparkSession, session_catalog: Catalog) -> None:
    identifier = "default.arrow_data_files"

    try:
        session_catalog.drop_table(identifier=identifier)
    except NoSuchTableError:
        pass

    tbl = session_catalog.create_table(
        identifier=identifier,
        schema=TABLE_SCHEMA,
        partition_spec=PartitionSpec(PartitionField(source_id=4, field_id=1001, transform=IdentityTransform(), name="int")),
        properties={'format-version': '1'},
    )

    with pytest.raises(ValueError, match="Expected PyArrow table, got: not a df"):
        tbl.append("not a df")


@pytest.mark.integration
@pytest.mark.parametrize(
    "spec",
    [
        # mixed with non-identity is not supported
        (
            PartitionSpec(
                PartitionField(source_id=4, field_id=1001, transform=BucketTransform(2), name="int_bucket"),
                PartitionField(source_id=1, field_id=1002, transform=IdentityTransform(), name="bool"),
            )
        ),
        # none of non-identity is supported
        (PartitionSpec(PartitionField(source_id=4, field_id=1001, transform=BucketTransform(2), name="int_bucket"))),
        (PartitionSpec(PartitionField(source_id=5, field_id=1001, transform=BucketTransform(2), name="long_bucket"))),
        (PartitionSpec(PartitionField(source_id=10, field_id=1001, transform=BucketTransform(2), name="date_bucket"))),
        (PartitionSpec(PartitionField(source_id=8, field_id=1001, transform=BucketTransform(2), name="timestamp_bucket"))),
        (PartitionSpec(PartitionField(source_id=9, field_id=1001, transform=BucketTransform(2), name="timestamptz_bucket"))),
        (PartitionSpec(PartitionField(source_id=2, field_id=1001, transform=BucketTransform(2), name="string_bucket"))),
        (PartitionSpec(PartitionField(source_id=12, field_id=1001, transform=BucketTransform(2), name="fixed_bucket"))),
        (PartitionSpec(PartitionField(source_id=11, field_id=1001, transform=BucketTransform(2), name="binary_bucket"))),
        (PartitionSpec(PartitionField(source_id=4, field_id=1001, transform=TruncateTransform(2), name="int_trunc"))),
        (PartitionSpec(PartitionField(source_id=5, field_id=1001, transform=TruncateTransform(2), name="long_trunc"))),
        (PartitionSpec(PartitionField(source_id=2, field_id=1001, transform=TruncateTransform(2), name="string_trunc"))),
        (PartitionSpec(PartitionField(source_id=11, field_id=1001, transform=TruncateTransform(2), name="binary_trunc"))),
        (PartitionSpec(PartitionField(source_id=8, field_id=1001, transform=YearTransform(), name="timestamp_year"))),
        (PartitionSpec(PartitionField(source_id=9, field_id=1001, transform=YearTransform(), name="timestamptz_year"))),
        (PartitionSpec(PartitionField(source_id=10, field_id=1001, transform=YearTransform(), name="date_year"))),
        (PartitionSpec(PartitionField(source_id=8, field_id=1001, transform=MonthTransform(), name="timestamp_month"))),
        (PartitionSpec(PartitionField(source_id=9, field_id=1001, transform=MonthTransform(), name="timestamptz_month"))),
        (PartitionSpec(PartitionField(source_id=10, field_id=1001, transform=MonthTransform(), name="date_month"))),
        (PartitionSpec(PartitionField(source_id=8, field_id=1001, transform=DayTransform(), name="timestamp_day"))),
        (PartitionSpec(PartitionField(source_id=9, field_id=1001, transform=DayTransform(), name="timestamptz_day"))),
        (PartitionSpec(PartitionField(source_id=10, field_id=1001, transform=DayTransform(), name="date_day"))),
        (PartitionSpec(PartitionField(source_id=8, field_id=1001, transform=HourTransform(), name="timestamp_hour"))),
        (PartitionSpec(PartitionField(source_id=9, field_id=1001, transform=HourTransform(), name="timestamptz_hour"))),
        (PartitionSpec(PartitionField(source_id=10, field_id=1001, transform=HourTransform(), name="date_hour"))),
    ],
)
def test_unsupported_transform(
    spec: PartitionSpec, spark: SparkSession, session_catalog: Catalog, arrow_table_with_null: pa.Table
) -> None:
    identifier = "default.unsupported_transform"

    try:
        session_catalog.drop_table(identifier=identifier)
    except NoSuchTableError:
        pass

    tbl = session_catalog.create_table(
        identifier=identifier,
        schema=TABLE_SCHEMA,
        partition_spec=spec,
        properties={'format-version': '1'},
    )

    with pytest.raises(ValueError, match="All transforms are not supported.*"):
        tbl.append(arrow_table_with_null)
