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
from typing import Any

import pyarrow as pa
import pytest
from pyspark.sql import SparkSession

from pyiceberg.catalog import Catalog
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table import TableProperties
from pyiceberg.transforms import (
    BucketTransform,
    DayTransform,
    HourTransform,
    IdentityTransform,
    MonthTransform,
    Transform,
    TruncateTransform,
    YearTransform,
)
from pyiceberg.types import (
    StringType,
)
from utils import TABLE_SCHEMA, _create_table


@pytest.mark.integration
@pytest.mark.parametrize(
    "part_col", ["int", "bool", "string", "string_long", "long", "float", "double", "date", "timestamp", "timestamptz", "binary"]
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
    for col in arrow_table_with_null.column_names:
        assert df.where(f"{col} is not null").count() == 2, f"Expected 2 non-null rows for {col}"
        assert df.where(f"{col} is null").count() == 1, f"Expected 1 null row for {col} is null"


@pytest.mark.integration
@pytest.mark.parametrize(
    "part_col", ["int", "bool", "string", "string_long", "long", "float", "double", "date", "timestamp", "timestamptz", "binary"]
)
@pytest.mark.parametrize("format_version", [1, 2])
def test_query_filter_without_data_partitioned(
    session_catalog: Catalog,
    spark: SparkSession,
    arrow_table_without_data: pa.Table,
    part_col: str,
    arrow_table_with_null: pa.Table,
    format_version: int,
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
    for col in arrow_table_with_null.column_names:
        assert df.where(f"{col} is null").count() == 0, f"Expected 0 row for {col}"
        assert df.where(f"{col} is not null").count() == 0, f"Expected 0 row for {col}"


@pytest.mark.integration
@pytest.mark.parametrize(
    "part_col", ["int", "bool", "string", "string_long", "long", "float", "double", "date", "timestamp", "timestamptz", "binary"]
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
    for col in arrow_table_with_only_nulls.column_names:
        assert df.where(f"{col} is null").count() == 2, f"Expected 2 row for {col}"
        assert df.where(f"{col} is not null").count() == 0, f"Expected 0 rows for {col}"


@pytest.mark.integration
@pytest.mark.parametrize(
    "part_col", ["int", "bool", "string", "string_long", "long", "float", "double", "date", "timestamptz", "timestamp", "binary"]
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
    for col in arrow_table_with_null.column_names:
        assert df.where(f"{col} is not null").count() == 6, f"Expected 6 non-null rows for {col}"
        assert df.where(f"{col} is null").count() == 3, f"Expected 3 null rows for {col}"
    # expecting 6 files: first append with [A], [B], [C],  second append with [A, A], [B, B], [C, C]
    rows = spark.sql(f"select partition from {identifier}.files").collect()
    assert len(rows) == 6


@pytest.mark.integration
@pytest.mark.parametrize(
    "part_col",
    [
        "int",
        "bool",
        "string",
        "string_long",
        "long",
        "float",
        "double",
        "date",
        "timestamp",
        "binary",
        "timestamptz",
    ],
)
@pytest.mark.parametrize(
    "format_version",
    [1, 2],
)
def test_query_filter_dynamic_partition_overwrite_null_partitioned(
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
    tbl.dynamic_partition_overwrite(arrow_table_with_null)
    tbl.dynamic_partition_overwrite(arrow_table_with_null.slice(0, 2))
    # Then
    assert tbl.format_version == format_version, f"Expected v{format_version}, got: v{tbl.format_version}"
    df = spark.table(identifier)
    for col in arrow_table_with_null.column_names:
        assert df.where(f"{col} is not null").count() == 2, f"Expected 2 non-null rows for {col},"
        assert df.where(f"{col} is null").count() == 1, f"Expected 1 null rows for {col},"
    # expecting 3 files:
    rows = spark.sql(f"select partition from {identifier}.files").collect()
    assert len(rows) == 3


@pytest.mark.integration
@pytest.mark.parametrize(
    "part_col", ["int", "bool", "string", "string_long", "long", "float", "double", "date", "timestamptz", "timestamp", "binary"]
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
    for col in arrow_table_with_null.column_names:  # type: ignore
        df = spark.table(identifier)
        assert df.where(f"{col} is not null").count() == 4, f"Expected 4 non-null rows for {col}"
        assert df.where(f"{col} is null").count() == 2, f"Expected 2 null rows for {col}"


@pytest.mark.integration
@pytest.mark.parametrize(
    "part_col", ["int", "bool", "string", "string_long", "long", "float", "double", "date", "timestamp", "timestamptz", "binary"]
)
@pytest.mark.parametrize("format_version", [1, 2])
def test_object_storage_location_provider_excludes_partition_path(
    session_catalog: Catalog, spark: SparkSession, arrow_table_with_null: pa.Table, part_col: str, format_version: int
) -> None:
    nested_field = TABLE_SCHEMA.find_field(part_col)
    partition_spec = PartitionSpec(
        PartitionField(source_id=nested_field.field_id, field_id=1001, transform=IdentityTransform(), name=part_col)
    )

    # Enable `write.object-storage.enabled` which is False by default
    # `write.object-storage.partitioned-paths` is True by default
    assert TableProperties.OBJECT_STORE_ENABLED_DEFAULT is False
    assert TableProperties.WRITE_OBJECT_STORE_PARTITIONED_PATHS_DEFAULT is True
    tbl = _create_table(
        session_catalog=session_catalog,
        identifier=f"default.arrow_table_v{format_version}_with_null_partitioned_on_col_{part_col}",
        properties={"format-version": str(format_version), TableProperties.OBJECT_STORE_ENABLED: True},
        data=[arrow_table_with_null],
        partition_spec=partition_spec,
    )

    original_paths = tbl.inspect.data_files().to_pydict()["file_path"]
    assert len(original_paths) == 3

    # Update props to exclude partitioned paths and append data
    with tbl.transaction() as tx:
        tx.set_properties({TableProperties.WRITE_OBJECT_STORE_PARTITIONED_PATHS: False})
    tbl.append(arrow_table_with_null)

    added_paths = set(tbl.inspect.data_files().to_pydict()["file_path"]) - set(original_paths)
    assert len(added_paths) == 3

    # All paths before the props update should contain the partition, while all paths after should not
    assert all(f"{part_col}=" in path for path in original_paths)
    assert all(f"{part_col}=" not in path for path in added_paths)


@pytest.mark.integration
@pytest.mark.parametrize(
    "spec",
    [
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
    ],
)
@pytest.mark.parametrize(
    "format_version",
    [1, 2],
)
def test_dynamic_partition_overwrite_non_identity_transform(
    session_catalog: Catalog, arrow_table_with_null: pa.Table, spec: PartitionSpec, format_version: int
) -> None:
    identifier = "default.dynamic_partition_overwrite_non_identity_transform"
    try:
        session_catalog.drop_table(identifier=identifier)
    except NoSuchTableError:
        pass

    tbl = session_catalog.create_table(
        identifier=identifier,
        schema=TABLE_SCHEMA,
        properties={"format-version": format_version},
        partition_spec=spec,
    )
    with pytest.raises(
        ValueError,
        match=(
            "For now dynamic overwrite does not support a table with non-identity-transform field in the latest partition spec: *"
        ),
    ):
        tbl.dynamic_partition_overwrite(arrow_table_with_null.slice(0, 1))


@pytest.mark.integration
def test_dynamic_partition_overwrite_invalid_on_unpartitioned_table(
    session_catalog: Catalog, arrow_table_with_null: pa.Table
) -> None:
    identifier = "default.arrow_data_files"
    tbl = _create_table(session_catalog, identifier, {"format-version": "1"}, [])

    with pytest.raises(ValueError, match="Cannot apply dynamic overwrite on an unpartitioned table."):
        tbl.dynamic_partition_overwrite(arrow_table_with_null)


@pytest.mark.integration
@pytest.mark.parametrize(
    "part_col",
    [
        "int",
        "bool",
        "string",
        "string_long",
        "long",
        "float",
        "double",
        "date",
        "timestamp",
        "binary",
        "timestamptz",
    ],
)
@pytest.mark.parametrize(
    "format_version",
    [1, 2],
)
def test_dynamic_partition_overwrite_unpartitioned_evolve_to_identity_transform(
    spark: SparkSession, session_catalog: Catalog, arrow_table_with_null: pa.Table, part_col: str, format_version: int
) -> None:
    identifier = f"default.unpartitioned_table_v{format_version}_evolve_into_identity_transformed_partition_field_{part_col}"

    try:
        session_catalog.drop_table(identifier=identifier)
    except NoSuchTableError:
        pass

    tbl = session_catalog.create_table(
        identifier=identifier,
        schema=TABLE_SCHEMA,
        properties={"format-version": format_version},
    )
    tbl.append(arrow_table_with_null)
    tbl.update_spec().add_field(part_col, IdentityTransform(), f"{part_col}_identity").commit()
    tbl.append(arrow_table_with_null)
    # each column should be [a, null, b, a, null, b]
    # dynamic overwrite a non-null row a, resulting in [null, b, null, b, a]
    tbl.dynamic_partition_overwrite(arrow_table_with_null.slice(0, 1))
    df = spark.table(identifier)
    assert df.where(f"{part_col} is not null").count() == 3, f"Expected 3 non-null rows for {part_col},"
    assert df.where(f"{part_col} is null").count() == 2, f"Expected 2 null rows for {part_col},"

    # The first 2 appends come from 2 calls of the append API, while the dynamic partition overwrite API
    # firstly overwrites of the unpartitioned file from first append,
    # then it deletes one of the 3 partition files generated by the second append,
    # finally it appends with new data.
    expected_operations = ["append", "append", "delete", "overwrite", "append"]

    # For a long string, the lower bound and upper bound  is truncated
    # e.g. aaaaaaaaaaaaaaaaaaaaaa has lower bound of aaaaaaaaaaaaaaaa and upper bound of aaaaaaaaaaaaaaab
    # this makes strict metric evaluator determine the file evaluate as ROWS_MIGHT_NOT_MATCH
    # this further causes the partitioned data file to be overwritten rather than deleted
    if part_col == "string_long":
        expected_operations = ["append", "append", "overwrite", "append"]
    assert tbl.inspect.snapshots().to_pydict()["operation"] == expected_operations


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
        properties={"format-version": "2"},
    )

    tbl.append(arrow_table_with_null)
    tbl.append(arrow_table_with_null)
    tbl.dynamic_partition_overwrite(arrow_table_with_null)
    tbl.append(arrow_table_with_null)
    tbl.dynamic_partition_overwrite(arrow_table_with_null.slice(0, 2))

    rows = spark.sql(
        f"""
        SELECT operation, summary
        FROM {identifier}.snapshots
        ORDER BY committed_at ASC
    """
    ).collect()

    operations = [row.operation for row in rows]
    assert operations == ["append", "append", "delete", "append", "append", "delete", "append"]

    summaries = [row.summary for row in rows]
    file_size = int(summaries[0]["added-files-size"])
    assert file_size > 0

    assert summaries[0] == {
        "changed-partition-count": "3",
        "added-data-files": "3",
        "added-files-size": str(file_size),
        "added-records": "3",
        "total-data-files": "3",
        "total-delete-files": "0",
        "total-equality-deletes": "0",
        "total-files-size": str(file_size),
        "total-position-deletes": "0",
        "total-records": "3",
    }

    assert summaries[1] == {
        "changed-partition-count": "3",
        "added-data-files": "3",
        "added-files-size": str(file_size),
        "added-records": "3",
        "total-data-files": "6",
        "total-delete-files": "0",
        "total-equality-deletes": "0",
        "total-files-size": str(file_size * 2),
        "total-position-deletes": "0",
        "total-records": "6",
    }
    assert summaries[2] == {
        "removed-files-size": str(file_size * 2),
        "changed-partition-count": "3",
        "total-equality-deletes": "0",
        "deleted-data-files": "6",
        "total-position-deletes": "0",
        "total-delete-files": "0",
        "deleted-records": "6",
        "total-files-size": "0",
        "total-data-files": "0",
        "total-records": "0",
    }
    assert summaries[3] == {
        "changed-partition-count": "3",
        "added-data-files": "3",
        "total-equality-deletes": "0",
        "added-records": "3",
        "total-position-deletes": "0",
        "added-files-size": str(file_size),
        "total-delete-files": "0",
        "total-files-size": str(file_size),
        "total-data-files": "3",
        "total-records": "3",
    }
    assert summaries[4] == {
        "changed-partition-count": "3",
        "added-data-files": "3",
        "total-equality-deletes": "0",
        "added-records": "3",
        "total-position-deletes": "0",
        "added-files-size": str(file_size),
        "total-delete-files": "0",
        "total-files-size": str(file_size * 2),
        "total-data-files": "6",
        "total-records": "6",
    }
    assert "removed-files-size" in summaries[5]
    assert "total-files-size" in summaries[5]
    assert summaries[5] == {
        "removed-files-size": summaries[5]["removed-files-size"],
        "changed-partition-count": "2",
        "total-equality-deletes": "0",
        "deleted-data-files": "4",
        "total-position-deletes": "0",
        "total-delete-files": "0",
        "deleted-records": "4",
        "total-files-size": summaries[5]["total-files-size"],
        "total-data-files": "2",
        "total-records": "2",
    }
    assert "added-files-size" in summaries[6]
    assert "total-files-size" in summaries[6]
    assert summaries[6] == {
        "changed-partition-count": "2",
        "added-data-files": "2",
        "total-equality-deletes": "0",
        "added-records": "2",
        "total-position-deletes": "0",
        "added-files-size": summaries[6]["added-files-size"],
        "total-delete-files": "0",
        "total-files-size": summaries[6]["total-files-size"],
        "total-data-files": "4",
        "total-records": "4",
    }


@pytest.mark.integration
def test_data_files_with_table_partitioned_with_null(
    spark: SparkSession, session_catalog: Catalog, arrow_table_with_null: pa.Table
) -> None:
    # Append           : First append has manifestlist file linking to 1 manifest file.
    #                    ML1 = [M1]
    #
    # Append           : Second append's manifestlist links to 2 manifest files.
    #                    ML2 = [M1, M2]
    #
    # Dynamic Overwrite: Dynamic overwrite on all partitions of the table delete all data and append new data
    #                    it has 2 snapshots of delete and append and thus 2 snapshots
    #                    the first snapshot generates M3 with 6 delete data entries collected from M1 and M2.
    #                    ML3 = [M3]
    #
    #                    The second snapshot generates M4 with 3 appended data entries and since M3 (previous manifests)
    #                    only has delete entries it does not lint to it.
    #                    ML4 = [M4]

    # Append           : Append generates M5 with new data entries and links to all previous manifests which is M4 .
    #                    ML5 = [M5, M4]

    # Dynamic Overwrite: Dynamic overwrite on partial partitions of the table delete partial data and append new data
    #                    it has 2 snapshots of delete and append and thus 2 snapshots
    #                    the first snapshot generates M6 with 4 delete data entries collected from M1 and M2,
    #                    then it generates M7 as remaining existing entries from M1 and M8 from M2
    #                    ML6 = [M6, M7, M8]
    #
    #                    The second snapshot generates M9 with 3 appended data entries and it also looks at manifests
    #                    in ML6 (previous manifests) it ignores M6 since it only has delete entries but it links to M7 and M8.
    #                    ML7 = [M9, M7, M8]

    # tldr:
    # APPEND               ML1 = [M1]
    # APPEND               ML2 = [M1, M2]
    # DYNAMIC_PARTITION_OVERWRITE    ML3 = [M3]
    #                      ML4 = [M4]
    # APPEND               ML5 = [M5, M4]
    # DYNAMIC_PARTITION_OVERWRITE    ML6 = [M6, M7, M8]
    #                      ML7 = [M9, M7, M8]

    identifier = "default.arrow_data_files"

    try:
        session_catalog.drop_table(identifier=identifier)
    except NoSuchTableError:
        pass
    tbl = session_catalog.create_table(
        identifier=identifier,
        schema=TABLE_SCHEMA,
        partition_spec=PartitionSpec(
            PartitionField(source_id=1, field_id=1001, transform=IdentityTransform(), name="bool"),
            PartitionField(source_id=4, field_id=1002, transform=IdentityTransform(), name="int"),
        ),
        properties={"format-version": "1"},
    )

    tbl.append(arrow_table_with_null)
    tbl.append(arrow_table_with_null)
    tbl.dynamic_partition_overwrite(arrow_table_with_null)
    tbl.append(arrow_table_with_null)
    tbl.dynamic_partition_overwrite(arrow_table_with_null.slice(0, 2))
    rows = spark.sql(
        f"""
        SELECT *
        FROM {identifier}.all_manifests
    """
    ).collect()

    assert [row.added_data_files_count for row in rows] == [3, 3, 3, 0, 3, 3, 3, 0, 0, 0, 2, 0, 0]
    assert [row.existing_data_files_count for row in rows] == [0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 0, 1, 1]
    assert [row.deleted_data_files_count for row in rows] == [0, 0, 0, 6, 0, 0, 0, 4, 0, 0, 0, 0, 0]


@pytest.mark.integration
@pytest.mark.parametrize(
    "format_version",
    [1, 2],
)
def test_dynamic_partition_overwrite_rename_column(spark: SparkSession, session_catalog: Catalog, format_version: int) -> None:
    arrow_table = pa.Table.from_pydict(
        {
            "place": ["Amsterdam", "Drachten"],
            "inhabitants": [921402, 44940],
        },
    )

    identifier = f"default.partitioned_{format_version}_dynamic_partition_overwrite_rename_column"
    with pytest.raises(NoSuchTableError):
        session_catalog.drop_table(identifier)

    tbl = session_catalog.create_table(
        identifier=identifier,
        schema=arrow_table.schema,
        properties={"format-version": str(format_version)},
    )

    with tbl.transaction() as tx:
        with tx.update_spec() as schema:
            schema.add_identity("place")

    tbl.append(arrow_table)

    with tbl.transaction() as tx:
        with tx.update_schema() as schema:
            schema.rename_column("place", "city")

    arrow_table = pa.Table.from_pydict(
        {
            "city": ["Drachten"],
            "inhabitants": [44941],  # A new baby was born!
        },
    )

    tbl.dynamic_partition_overwrite(arrow_table)
    result = tbl.scan().to_arrow()

    assert result["city"].to_pylist() == ["Drachten", "Amsterdam"]
    assert result["inhabitants"].to_pylist() == [44941, 921402]


@pytest.mark.integration
@pytest.mark.parametrize(
    "format_version",
    [1, 2],
)
@pytest.mark.filterwarnings("ignore")
def test_dynamic_partition_overwrite_evolve_partition(spark: SparkSession, session_catalog: Catalog, format_version: int) -> None:
    arrow_table = pa.Table.from_pydict(
        {
            "place": ["Amsterdam", "Drachten"],
            "inhabitants": [921402, 44940],
        },
    )

    identifier = f"default.partitioned_{format_version}_test_dynamic_partition_overwrite_evolve_partition"
    try:
        session_catalog.drop_table(identifier)
    except NoSuchTableError:
        pass

    tbl = session_catalog.create_table(
        identifier=identifier,
        schema=arrow_table.schema,
        properties={"format-version": str(format_version)},
    )

    with tbl.transaction() as tx:
        with tx.update_spec() as schema:
            schema.add_identity("place")

    tbl.append(arrow_table)

    with tbl.transaction() as tx:
        with tx.update_schema() as schema:
            schema.add_column("country", StringType())
        with tx.update_spec() as schema:
            schema.add_identity("country")

    arrow_table = pa.Table.from_pydict(
        {
            "place": ["Groningen"],
            "country": ["Netherlands"],
            "inhabitants": [238147],
        },
    )

    tbl.dynamic_partition_overwrite(arrow_table)
    result = tbl.scan().to_arrow()

    assert result["place"].to_pylist() == ["Groningen", "Amsterdam", "Drachten"]
    assert result["inhabitants"].to_pylist() == [238147, 921402, 44940]


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
        properties={"format-version": "1"},
    )

    with pytest.raises(ValueError, match="Expected PyArrow table, got: not a df"):
        tbl.append("not a df")


@pytest.mark.integration
@pytest.mark.parametrize(
    "spec",
    [
        (PartitionSpec(PartitionField(source_id=4, field_id=1001, transform=TruncateTransform(2), name="int_trunc"))),
        (PartitionSpec(PartitionField(source_id=5, field_id=1001, transform=TruncateTransform(2), name="long_trunc"))),
        (PartitionSpec(PartitionField(source_id=2, field_id=1001, transform=TruncateTransform(2), name="string_trunc"))),
    ],
)
@pytest.mark.parametrize("format_version", [1, 2])
def test_truncate_transform(
    spec: PartitionSpec,
    spark: SparkSession,
    session_catalog: Catalog,
    arrow_table_with_null: pa.Table,
    format_version: int,
) -> None:
    identifier = "default.truncate_transform"

    try:
        session_catalog.drop_table(identifier=identifier)
    except NoSuchTableError:
        pass

    tbl = _create_table(
        session_catalog=session_catalog,
        identifier=identifier,
        properties={"format-version": str(format_version)},
        data=[arrow_table_with_null],
        partition_spec=spec,
    )

    assert tbl.format_version == format_version, f"Expected v{format_version}, got: v{tbl.format_version}"
    df = spark.table(identifier)
    assert df.count() == 3, f"Expected 3 total rows for {identifier}"
    for col in arrow_table_with_null.column_names:
        assert df.where(f"{col} is not null").count() == 2, f"Expected 2 non-null rows for {col}"
        assert df.where(f"{col} is null").count() == 1, f"Expected 1 null row for {col} is null"

    assert tbl.inspect.partitions().num_rows == 3
    files_df = spark.sql(
        f"""
            SELECT *
            FROM {identifier}.files
        """
    )
    assert files_df.count() == 3


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
    ],
)
@pytest.mark.parametrize("format_version", [1, 2])
def test_identity_and_bucket_transform_spec(
    spec: PartitionSpec,
    spark: SparkSession,
    session_catalog: Catalog,
    arrow_table_with_null: pa.Table,
    format_version: int,
) -> None:
    identifier = "default.identity_and_bucket_transform"

    try:
        session_catalog.drop_table(identifier=identifier)
    except NoSuchTableError:
        pass

    tbl = _create_table(
        session_catalog=session_catalog,
        identifier=identifier,
        properties={"format-version": str(format_version)},
        data=[arrow_table_with_null],
        partition_spec=spec,
    )

    assert tbl.format_version == format_version, f"Expected v{format_version}, got: v{tbl.format_version}"
    df = spark.table(identifier)
    assert df.count() == 3, f"Expected 3 total rows for {identifier}"
    for col in arrow_table_with_null.column_names:
        assert df.where(f"{col} is not null").count() == 2, f"Expected 2 non-null rows for {col}"
        assert df.where(f"{col} is null").count() == 1, f"Expected 1 null row for {col} is null"

    assert tbl.inspect.partitions().num_rows == 3
    files_df = spark.sql(
        f"""
            SELECT *
            FROM {identifier}.files
        """
    )
    assert files_df.count() == 3


@pytest.mark.integration
@pytest.mark.parametrize(
    "spec",
    [
        (PartitionSpec(PartitionField(source_id=11, field_id=1001, transform=TruncateTransform(2), name="binary_trunc"))),
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
        properties={"format-version": "1"},
    )

    with pytest.raises(
        ValueError,
        match="FeatureUnsupported => Unsupported data type for truncate transform: LargeBinary",
    ):
        tbl.append(arrow_table_with_null)


@pytest.mark.integration
@pytest.mark.parametrize(
    "spec, expected_rows",
    [
        (PartitionSpec(PartitionField(source_id=4, field_id=1001, transform=BucketTransform(2), name="int_bucket")), 3),
        (PartitionSpec(PartitionField(source_id=5, field_id=1001, transform=BucketTransform(2), name="long_bucket")), 3),
        (PartitionSpec(PartitionField(source_id=10, field_id=1001, transform=BucketTransform(2), name="date_bucket")), 3),
        (PartitionSpec(PartitionField(source_id=8, field_id=1001, transform=BucketTransform(2), name="timestamp_bucket")), 3),
        (PartitionSpec(PartitionField(source_id=9, field_id=1001, transform=BucketTransform(2), name="timestamptz_bucket")), 3),
        (PartitionSpec(PartitionField(source_id=2, field_id=1001, transform=BucketTransform(2), name="string_bucket")), 3),
        (PartitionSpec(PartitionField(source_id=12, field_id=1001, transform=BucketTransform(2), name="fixed_bucket")), 2),
        (PartitionSpec(PartitionField(source_id=11, field_id=1001, transform=BucketTransform(2), name="binary_bucket")), 2),
    ],
)
@pytest.mark.parametrize("format_version", [1, 2])
def test_bucket_transform(
    spark: SparkSession,
    session_catalog: Catalog,
    arrow_table_with_null: pa.Table,
    spec: PartitionSpec,
    expected_rows: int,
    format_version: int,
) -> None:
    identifier = "default.bucket_transform"

    try:
        session_catalog.drop_table(identifier=identifier)
    except NoSuchTableError:
        pass

    tbl = _create_table(
        session_catalog=session_catalog,
        identifier=identifier,
        properties={"format-version": str(format_version)},
        data=[arrow_table_with_null],
        partition_spec=spec,
    )

    assert tbl.format_version == format_version, f"Expected v{format_version}, got: v{tbl.format_version}"
    df = spark.table(identifier)
    assert df.count() == 3, f"Expected 3 total rows for {identifier}"
    for col in arrow_table_with_null.column_names:
        assert df.where(f"{col} is not null").count() == 2, f"Expected 2 non-null rows for {col}"
        assert df.where(f"{col} is null").count() == 1, f"Expected 1 null row for {col} is null"

    assert tbl.inspect.partitions().num_rows == expected_rows
    files_df = spark.sql(
        f"""
            SELECT *
            FROM {identifier}.files
        """
    )
    assert files_df.count() == expected_rows


@pytest.mark.integration
@pytest.mark.parametrize(
    "transform,expected_rows",
    [
        pytest.param(YearTransform(), 2, id="year_transform"),
        pytest.param(MonthTransform(), 3, id="month_transform"),
        pytest.param(DayTransform(), 3, id="day_transform"),
    ],
)
@pytest.mark.parametrize("part_col", ["date", "timestamp", "timestamptz"])
@pytest.mark.parametrize("format_version", [1, 2])
def test_append_ymd_transform_partitioned(
    session_catalog: Catalog,
    spark: SparkSession,
    arrow_table_with_null: pa.Table,
    transform: Transform[Any, Any],
    expected_rows: int,
    part_col: str,
    format_version: int,
) -> None:
    # Given
    identifier = f"default.arrow_table_v{format_version}_with_{str(transform)}_partition_on_col_{part_col}"
    nested_field = TABLE_SCHEMA.find_field(part_col)

    if isinstance(transform, YearTransform):
        partition_name = f"{part_col}_year"
    elif isinstance(transform, MonthTransform):
        partition_name = f"{part_col}_month"
    elif isinstance(transform, DayTransform):
        partition_name = f"{part_col}_day"

    partition_spec = PartitionSpec(
        PartitionField(source_id=nested_field.field_id, field_id=1001, transform=transform, name=partition_name)
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
    for col in arrow_table_with_null.column_names:
        assert df.where(f"{col} is not null").count() == 2, f"Expected 2 non-null rows for {col}"
        assert df.where(f"{col} is null").count() == 1, f"Expected 1 null row for {col} is null"

    assert tbl.inspect.partitions().num_rows == expected_rows
    files_df = spark.sql(
        f"""
            SELECT *
            FROM {identifier}.files
        """
    )
    assert files_df.count() == expected_rows


@pytest.mark.integration
@pytest.mark.parametrize(
    "transform,expected_partitions",
    [
        pytest.param(YearTransform(), {53, 54, None}, id="year_transform"),
        pytest.param(MonthTransform(), {647, 648, 649, None}, id="month_transform"),
        pytest.param(
            DayTransform(), {date(2023, 12, 31), date(2024, 1, 1), date(2024, 1, 31), date(2024, 2, 1), None}, id="day_transform"
        ),
        pytest.param(HourTransform(), {473328, 473352, 474072, 474096, 474102, None}, id="hour_transform"),
    ],
)
@pytest.mark.parametrize("format_version", [1, 2])
def test_append_transform_partition_verify_partitions_count(
    session_catalog: Catalog,
    spark: SparkSession,
    arrow_table_date_timestamps: pa.Table,
    table_date_timestamps_schema: Schema,
    transform: Transform[Any, Any],
    expected_partitions: set[Any],
    format_version: int,
) -> None:
    # Given
    part_col = "timestamptz"
    identifier = f"default.arrow_table_v{format_version}_with_{str(transform)}_transform_partitioned_on_col_{part_col}"
    nested_field = table_date_timestamps_schema.find_field(part_col)

    if isinstance(transform, YearTransform):
        partition_name = f"{part_col}_year"
    elif isinstance(transform, MonthTransform):
        partition_name = f"{part_col}_month"
    elif isinstance(transform, DayTransform):
        partition_name = f"{part_col}_day"
    elif isinstance(transform, HourTransform):
        partition_name = f"{part_col}_hour"

    partition_spec = PartitionSpec(
        PartitionField(source_id=nested_field.field_id, field_id=1001, transform=transform, name=partition_name),
    )

    # When
    tbl = _create_table(
        session_catalog=session_catalog,
        identifier=identifier,
        properties={"format-version": str(format_version)},
        data=[arrow_table_date_timestamps],
        partition_spec=partition_spec,
        schema=table_date_timestamps_schema,
    )

    # Then
    assert tbl.format_version == format_version, f"Expected v{format_version}, got: v{tbl.format_version}"
    df = spark.table(identifier)
    assert df.count() == 6, f"Expected 6 total rows for {identifier}"
    for col in arrow_table_date_timestamps.column_names:
        assert df.where(f"{col} is not null").count() == 5, f"Expected 2 non-null rows for {col}"
        assert df.where(f"{col} is null").count() == 1, f"Expected 1 null row for {col} is null"

    partitions_table = tbl.inspect.partitions()
    assert partitions_table.num_rows == len(expected_partitions)
    assert {part[partition_name] for part in partitions_table["partition"].to_pylist()} == expected_partitions
    files_df = spark.sql(
        f"""
            SELECT *
            FROM {identifier}.files
        """
    )
    assert files_df.count() == len(expected_partitions)


@pytest.mark.integration
@pytest.mark.parametrize("format_version", [1, 2])
def test_append_multiple_partitions(
    session_catalog: Catalog,
    spark: SparkSession,
    arrow_table_date_timestamps: pa.Table,
    table_date_timestamps_schema: Schema,
    format_version: int,
) -> None:
    # Given
    identifier = f"default.arrow_table_v{format_version}_with_multiple_partitions"
    partition_spec = PartitionSpec(
        PartitionField(
            source_id=table_date_timestamps_schema.find_field("date").field_id,
            field_id=1001,
            transform=YearTransform(),
            name="date_year",
        ),
        PartitionField(
            source_id=table_date_timestamps_schema.find_field("timestamptz").field_id,
            field_id=1000,
            transform=HourTransform(),
            name="timestamptz_hour",
        ),
    )

    # When
    tbl = _create_table(
        session_catalog=session_catalog,
        identifier=identifier,
        properties={"format-version": str(format_version)},
        data=[arrow_table_date_timestamps],
        partition_spec=partition_spec,
        schema=table_date_timestamps_schema,
    )

    # Then
    assert tbl.format_version == format_version, f"Expected v{format_version}, got: v{tbl.format_version}"
    df = spark.table(identifier)
    assert df.count() == 6, f"Expected 6 total rows for {identifier}"
    for col in arrow_table_date_timestamps.column_names:
        assert df.where(f"{col} is not null").count() == 5, f"Expected 2 non-null rows for {col}"
        assert df.where(f"{col} is null").count() == 1, f"Expected 1 null row for {col} is null"

    partitions_table = tbl.inspect.partitions()
    assert partitions_table.num_rows == 6
    partitions = partitions_table["partition"].to_pylist()
    assert {(part["date_year"], part["timestamptz_hour"]) for part in partitions} == {
        (53, 473328),
        (54, 473352),
        (54, 474072),
        (54, 474096),
        (54, 474102),
        (None, None),
    }
    files_df = spark.sql(
        f"""
            SELECT *
            FROM {identifier}.files
        """
    )
    assert files_df.count() == 6


@pytest.mark.integration
@pytest.mark.parametrize("format_version", [1, 2])
def test_stage_only_dynamic_partition_overwrite_files(
    spark: SparkSession, session_catalog: Catalog, arrow_table_with_null: pa.Table, format_version: int
) -> None:
    identifier = f"default.test_stage_only_dynamic_partition_overwrite_files_v{format_version}"
    try:
        session_catalog.drop_table(identifier=identifier)
    except NoSuchTableError:
        pass
    tbl = session_catalog.create_table(
        identifier=identifier,
        schema=TABLE_SCHEMA,
        partition_spec=PartitionSpec(
            PartitionField(source_id=1, field_id=1001, transform=IdentityTransform(), name="bool"),
            PartitionField(source_id=4, field_id=1002, transform=IdentityTransform(), name="int"),
        ),
        properties={"format-version": str(format_version)},
    )

    tbl.append(arrow_table_with_null)
    current_snapshot = tbl.metadata.current_snapshot_id
    assert current_snapshot is not None

    original_count = len(tbl.scan().to_arrow())
    assert original_count == 3

    # write to staging snapshot
    tbl.dynamic_partition_overwrite(arrow_table_with_null.slice(0, 1), branch=None)

    assert current_snapshot == tbl.metadata.current_snapshot_id
    assert len(tbl.scan().to_arrow()) == original_count
    snapshots = tbl.snapshots()
    # dynamic partition overwrite will create 2 snapshots, one delete and another append
    assert len(snapshots) == 3

    # Write to main branch
    tbl.append(arrow_table_with_null)

    # Main ref has changed
    assert current_snapshot != tbl.metadata.current_snapshot_id
    assert len(tbl.scan().to_arrow()) == 6
    snapshots = tbl.snapshots()
    assert len(snapshots) == 4

    rows = spark.sql(
        f"""
                    SELECT operation, parent_id, snapshot_id
                    FROM {identifier}.snapshots
                    ORDER BY committed_at ASC
                """
    ).collect()
    operations = [row.operation for row in rows]
    parent_snapshot_id = [row.parent_id for row in rows]
    assert operations == ["append", "delete", "append", "append"]
    assert parent_snapshot_id == [None, current_snapshot, current_snapshot, current_snapshot]
