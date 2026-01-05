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
import os
import random
import re
import time
import uuid
from datetime import date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import fastavro
import pandas as pd
import pandas.testing
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import pytest
import pytz
from pyarrow.fs import S3FileSystem
from pydantic_core import ValidationError
from pyspark.sql import SparkSession
from pytest_mock.plugin import MockerFixture

from pyiceberg.catalog import Catalog, load_catalog
from pyiceberg.catalog.hive import HiveCatalog
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.exceptions import CommitFailedException, NoSuchTableError
from pyiceberg.expressions import And, EqualTo, GreaterThanOrEqual, In, LessThan, Not
from pyiceberg.io.pyarrow import UnsupportedPyArrowTypeException, _dataframe_to_data_files
from pyiceberg.manifest import FileFormat
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table import TableProperties
from pyiceberg.table.refs import MAIN_BRANCH
from pyiceberg.table.sorting import SortDirection, SortField, SortOrder
from pyiceberg.transforms import DayTransform, HourTransform, IdentityTransform, Transform
from pyiceberg.types import (
    DateType,
    DecimalType,
    DoubleType,
    IntegerType,
    ListType,
    LongType,
    NestedField,
    StringType,
    UUIDType,
)
from utils import TABLE_SCHEMA, _create_table


@pytest.fixture(scope="session", autouse=True)
def table_v1_with_null(session_catalog: Catalog, arrow_table_with_null: pa.Table) -> None:
    identifier = "default.arrow_table_v1_with_null"
    tbl = _create_table(session_catalog, identifier, {"format-version": "1"}, [arrow_table_with_null])
    assert tbl.format_version == 1, f"Expected v1, got: v{tbl.format_version}"


@pytest.fixture(scope="session", autouse=True)
def table_v1_without_data(session_catalog: Catalog, arrow_table_without_data: pa.Table) -> None:
    identifier = "default.arrow_table_v1_without_data"
    tbl = _create_table(session_catalog, identifier, {"format-version": "1"}, [arrow_table_without_data])
    assert tbl.format_version == 1, f"Expected v1, got: v{tbl.format_version}"


@pytest.fixture(scope="session", autouse=True)
def table_v1_with_only_nulls(session_catalog: Catalog, arrow_table_with_only_nulls: pa.Table) -> None:
    identifier = "default.arrow_table_v1_with_only_nulls"
    tbl = _create_table(session_catalog, identifier, {"format-version": "1"}, [arrow_table_with_only_nulls])
    assert tbl.format_version == 1, f"Expected v1, got: v{tbl.format_version}"


@pytest.fixture(scope="session", autouse=True)
def table_v1_appended_with_null(session_catalog: Catalog, arrow_table_with_null: pa.Table) -> None:
    identifier = "default.arrow_table_v1_appended_with_null"
    tbl = _create_table(session_catalog, identifier, {"format-version": "1"}, 2 * [arrow_table_with_null])
    assert tbl.format_version == 1, f"Expected v1, got: v{tbl.format_version}"


@pytest.fixture(scope="session", autouse=True)
def table_v2_with_null(session_catalog: Catalog, arrow_table_with_null: pa.Table) -> None:
    identifier = "default.arrow_table_v2_with_null"
    tbl = _create_table(session_catalog, identifier, {"format-version": "2"}, [arrow_table_with_null])
    assert tbl.format_version == 2, f"Expected v2, got: v{tbl.format_version}"


@pytest.fixture(scope="session", autouse=True)
def table_v2_without_data(session_catalog: Catalog, arrow_table_without_data: pa.Table) -> None:
    identifier = "default.arrow_table_v2_without_data"
    tbl = _create_table(session_catalog, identifier, {"format-version": "2"}, [arrow_table_without_data])
    assert tbl.format_version == 2, f"Expected v2, got: v{tbl.format_version}"


@pytest.fixture(scope="session", autouse=True)
def table_v2_with_only_nulls(session_catalog: Catalog, arrow_table_with_only_nulls: pa.Table) -> None:
    identifier = "default.arrow_table_v2_with_only_nulls"
    tbl = _create_table(session_catalog, identifier, {"format-version": "2"}, [arrow_table_with_only_nulls])
    assert tbl.format_version == 2, f"Expected v2, got: v{tbl.format_version}"


@pytest.fixture(scope="session", autouse=True)
def table_v2_appended_with_null(session_catalog: Catalog, arrow_table_with_null: pa.Table) -> None:
    identifier = "default.arrow_table_v2_appended_with_null"
    tbl = _create_table(session_catalog, identifier, {"format-version": "2"}, 2 * [arrow_table_with_null])
    assert tbl.format_version == 2, f"Expected v2, got: v{tbl.format_version}"


@pytest.fixture(scope="session", autouse=True)
def table_v1_v2_appended_with_null(session_catalog: Catalog, arrow_table_with_null: pa.Table) -> None:
    identifier = "default.arrow_table_v1_v2_appended_with_null"
    tbl = _create_table(session_catalog, identifier, {"format-version": "1"}, [arrow_table_with_null])
    assert tbl.format_version == 1, f"Expected v1, got: v{tbl.format_version}"

    with tbl.transaction() as tx:
        tx.upgrade_table_version(format_version=2)

    tbl.append(arrow_table_with_null)

    assert tbl.format_version == 2, f"Expected v2, got: v{tbl.format_version}"


@pytest.mark.integration
@pytest.mark.parametrize("format_version", [1, 2])
def test_query_count(spark: SparkSession, format_version: int) -> None:
    df = spark.table(f"default.arrow_table_v{format_version}_with_null")
    assert df.count() == 3, "Expected 3 rows"


@pytest.mark.integration
@pytest.mark.parametrize("format_version", [1, 2])
def test_query_filter_null(spark: SparkSession, arrow_table_with_null: pa.Table, format_version: int) -> None:
    identifier = f"default.arrow_table_v{format_version}_with_null"
    df = spark.table(identifier)
    for col in arrow_table_with_null.column_names:
        assert df.where(f"{col} is null").count() == 1, f"Expected 1 row for {col}"
        assert df.where(f"{col} is not null").count() == 2, f"Expected 2 rows for {col}"


@pytest.mark.integration
@pytest.mark.parametrize("format_version", [1, 2])
def test_query_filter_without_data(spark: SparkSession, arrow_table_with_null: pa.Table, format_version: int) -> None:
    identifier = f"default.arrow_table_v{format_version}_without_data"
    df = spark.table(identifier)
    for col in arrow_table_with_null.column_names:
        assert df.where(f"{col} is null").count() == 0, f"Expected 0 row for {col}"
        assert df.where(f"{col} is not null").count() == 0, f"Expected 0 row for {col}"


@pytest.mark.integration
@pytest.mark.parametrize("format_version", [1, 2])
def test_query_filter_only_nulls(spark: SparkSession, arrow_table_with_null: pa.Table, format_version: int) -> None:
    identifier = f"default.arrow_table_v{format_version}_with_only_nulls"
    df = spark.table(identifier)
    for col in arrow_table_with_null.column_names:
        assert df.where(f"{col} is null").count() == 2, f"Expected 2 rows for {col}"
        assert df.where(f"{col} is not null").count() == 0, f"Expected 0 row for {col}"


@pytest.mark.integration
@pytest.mark.parametrize("format_version", [1, 2])
def test_query_filter_appended_null(spark: SparkSession, arrow_table_with_null: pa.Table, format_version: int) -> None:
    identifier = f"default.arrow_table_v{format_version}_appended_with_null"
    df = spark.table(identifier)
    for col in arrow_table_with_null.column_names:
        assert df.where(f"{col} is null").count() == 2, f"Expected 1 row for {col}"
        assert df.where(f"{col} is not null").count() == 4, f"Expected 2 rows for {col}"


@pytest.mark.integration
def test_query_filter_v1_v2_append_null(
    spark: SparkSession,
    arrow_table_with_null: pa.Table,
) -> None:
    identifier = "default.arrow_table_v1_v2_appended_with_null"
    df = spark.table(identifier)
    for col in arrow_table_with_null.column_names:
        assert df.where(f"{col} is null").count() == 2, f"Expected 1 row for {col}"
        assert df.where(f"{col} is not null").count() == 4, f"Expected 2 rows for {col}"


@pytest.mark.integration
def test_summaries(spark: SparkSession, session_catalog: Catalog, arrow_table_with_null: pa.Table) -> None:
    identifier = "default.arrow_table_summaries"
    tbl = _create_table(session_catalog, identifier, {"format-version": "1"}, 2 * [arrow_table_with_null])
    tbl.overwrite(arrow_table_with_null)

    rows = spark.sql(
        f"""
        SELECT operation, summary
        FROM {identifier}.snapshots
        ORDER BY committed_at ASC
    """
    ).collect()

    operations = [row.operation for row in rows]
    assert operations == ["append", "append", "delete", "append"]

    summaries = [row.summary for row in rows]

    file_size = int(summaries[0]["added-files-size"])
    assert file_size > 0

    # Append
    assert summaries[0] == {
        "added-data-files": "1",
        "added-files-size": str(file_size),
        "added-records": "3",
        "total-data-files": "1",
        "total-delete-files": "0",
        "total-equality-deletes": "0",
        "total-files-size": str(file_size),
        "total-position-deletes": "0",
        "total-records": "3",
    }

    # Append
    assert summaries[1] == {
        "added-data-files": "1",
        "added-files-size": str(file_size),
        "added-records": "3",
        "total-data-files": "2",
        "total-delete-files": "0",
        "total-equality-deletes": "0",
        "total-files-size": str(file_size * 2),
        "total-position-deletes": "0",
        "total-records": "6",
    }

    # Delete
    assert summaries[2] == {
        "deleted-data-files": "2",
        "deleted-records": "6",
        "removed-files-size": str(file_size * 2),
        "total-data-files": "0",
        "total-delete-files": "0",
        "total-equality-deletes": "0",
        "total-files-size": "0",
        "total-position-deletes": "0",
        "total-records": "0",
    }

    # Append
    assert summaries[3] == {
        "added-data-files": "1",
        "added-files-size": str(file_size),
        "added-records": "3",
        "total-data-files": "1",
        "total-delete-files": "0",
        "total-equality-deletes": "0",
        "total-files-size": str(file_size),
        "total-position-deletes": "0",
        "total-records": "3",
    }


@pytest.mark.integration
def test_summaries_partial_overwrite(spark: SparkSession, session_catalog: Catalog) -> None:
    identifier = "default.test_summaries_partial_overwrite"
    TEST_DATA = {
        "id": [1, 2, 3, 1, 1],
        "name": ["AB", "CD", "EF", "CD", "EF"],
    }
    pa_schema = pa.schema(
        [
            pa.field("id", pa.int32()),
            pa.field("name", pa.string()),
        ]
    )
    arrow_table = pa.Table.from_pydict(TEST_DATA, schema=pa_schema)
    tbl = _create_table(session_catalog, identifier, {"format-version": "2"}, schema=pa_schema)
    with tbl.update_spec() as txn:
        txn.add_identity("id")
    tbl.append(arrow_table)

    assert len(tbl.inspect.data_files()) == 3

    tbl.delete(delete_filter="id == 1 and name = 'AB'")  # partial overwrite data from 1 data file

    rows = spark.sql(
        f"""
        SELECT operation, summary
        FROM {identifier}.snapshots
        ORDER BY committed_at ASC
    """
    ).collect()

    operations = [row.operation for row in rows]
    assert operations == ["append", "overwrite"]

    summaries = [row.summary for row in rows]

    file_size = int(summaries[0]["added-files-size"])
    assert file_size > 0

    # APPEND
    assert "added-files-size" in summaries[0]
    assert "total-files-size" in summaries[0]
    assert summaries[0] == {
        "added-data-files": "3",
        "added-files-size": summaries[0]["added-files-size"],
        "added-records": "5",
        "changed-partition-count": "3",
        "total-data-files": "3",
        "total-delete-files": "0",
        "total-equality-deletes": "0",
        "total-files-size": summaries[0]["total-files-size"],
        "total-position-deletes": "0",
        "total-records": "5",
    }
    # Java produces:
    # {
    #     "added-data-files": "1",
    #     "added-files-size": "707",
    #     "added-records": "2",
    #     "app-id": "local-1743678304626",
    #     "changed-partition-count": "1",
    #     "deleted-data-files": "1",
    #     "deleted-records": "3",
    #     "engine-name": "spark",
    #     "engine-version": "3.5.5",
    #     "iceberg-version": "Apache Iceberg 1.8.1 (commit 9ce0fcf0af7becf25ad9fc996c3bad2afdcfd33d)",
    #     "removed-files-size": "693",
    #     "spark.app.id": "local-1743678304626",
    #     "total-data-files": "3",
    #     "total-delete-files": "0",
    #     "total-equality-deletes": "0",
    #     "total-files-size": "1993",
    #     "total-position-deletes": "0",
    #     "total-records": "4"
    # }
    files = tbl.inspect.data_files()
    assert len(files) == 3
    assert "added-files-size" in summaries[1]
    assert "removed-files-size" in summaries[1]
    assert "total-files-size" in summaries[1]
    assert summaries[1] == {
        "added-data-files": "1",
        "added-files-size": summaries[1]["added-files-size"],
        "added-records": "2",
        "changed-partition-count": "1",
        "deleted-data-files": "1",
        "deleted-records": "3",
        "removed-files-size": summaries[1]["removed-files-size"],
        "total-data-files": "3",
        "total-delete-files": "0",
        "total-equality-deletes": "0",
        "total-files-size": summaries[1]["total-files-size"],
        "total-position-deletes": "0",
        "total-records": "4",
    }
    assert len(tbl.scan().to_pandas()) == 4


@pytest.mark.integration
def test_data_files(spark: SparkSession, session_catalog: Catalog, arrow_table_with_null: pa.Table) -> None:
    identifier = "default.arrow_data_files"
    tbl = _create_table(session_catalog, identifier, {"format-version": "1"}, [])

    tbl.append(arrow_table_with_null)
    # should produce a DELETE entry
    tbl.overwrite(arrow_table_with_null)
    # Since we don't rewrite, this should produce a new manifest with an ADDED entry
    tbl.append(arrow_table_with_null)

    rows = spark.sql(
        f"""
        SELECT added_data_files_count, existing_data_files_count, deleted_data_files_count
        FROM {identifier}.all_manifests
    """
    ).collect()

    assert [row.added_data_files_count for row in rows] == [1, 0, 1, 1, 1]
    assert [row.existing_data_files_count for row in rows] == [0, 0, 0, 0, 0]
    assert [row.deleted_data_files_count for row in rows] == [0, 1, 0, 0, 0]


@pytest.mark.integration
@pytest.mark.parametrize("format_version", [1, 2])
def test_object_storage_data_files(
    spark: SparkSession, session_catalog: Catalog, arrow_table_with_null: pa.Table, format_version: int
) -> None:
    tbl = _create_table(
        session_catalog=session_catalog,
        identifier="default.object_stored",
        properties={"format-version": format_version, TableProperties.OBJECT_STORE_ENABLED: True},
        data=[arrow_table_with_null],
    )
    tbl.append(arrow_table_with_null)

    paths = tbl.inspect.data_files().to_pydict()["file_path"]
    assert len(paths) == 2

    for location in paths:
        assert location.startswith("s3://warehouse/default/object_stored/data/")
        parts = location.split("/")
        assert len(parts) == 11

        # Entropy binary directories should have been injected
        for dir_name in parts[6:10]:
            assert dir_name
            assert all(c in "01" for c in dir_name)


@pytest.mark.integration
def test_python_writes_with_spark_snapshot_reads(
    spark: SparkSession, session_catalog: Catalog, arrow_table_with_null: pa.Table
) -> None:
    identifier = "default.python_writes_with_spark_snapshot_reads"
    tbl = _create_table(session_catalog, identifier, {"format-version": "1"}, [])

    def get_current_snapshot_id(identifier: str) -> int:
        return (
            spark.sql(f"SELECT snapshot_id FROM {identifier}.snapshots order by committed_at desc limit 1")
            .collect()[0]
            .snapshot_id
        )

    tbl.append(arrow_table_with_null)
    assert tbl.current_snapshot().snapshot_id == get_current_snapshot_id(identifier)  # type: ignore
    tbl.overwrite(arrow_table_with_null)
    assert tbl.current_snapshot().snapshot_id == get_current_snapshot_id(identifier)  # type: ignore
    tbl.append(arrow_table_with_null)
    assert tbl.current_snapshot().snapshot_id == get_current_snapshot_id(identifier)  # type: ignore


@pytest.mark.integration
@pytest.mark.parametrize("format_version", [1, 2])
def test_python_writes_special_character_column_with_spark_reads(
    spark: SparkSession, session_catalog: Catalog, format_version: int
) -> None:
    identifier = "default.python_writes_special_character_column_with_spark_reads"
    column_name_with_special_character = "letter/abc"
    TEST_DATA_WITH_SPECIAL_CHARACTER_COLUMN = {
        column_name_with_special_character: ["a", None, "z"],
        "id": [1, 2, 3],
        "name": ["AB", "CD", "EF"],
        "address": [
            {"street": "123", "city": "SFO", "zip": 12345, column_name_with_special_character: "a"},
            {"street": "456", "city": "SW", "zip": 67890, column_name_with_special_character: "b"},
            {"street": "789", "city": "Random", "zip": 10112, column_name_with_special_character: "c"},
        ],
    }
    pa_schema = pa.schema(
        [
            pa.field(column_name_with_special_character, pa.string()),
            pa.field("id", pa.int32()),
            pa.field("name", pa.string()),
            pa.field(
                "address",
                pa.struct(
                    [
                        pa.field("street", pa.string()),
                        pa.field("city", pa.string()),
                        pa.field("zip", pa.int32()),
                        pa.field(column_name_with_special_character, pa.string()),
                    ]
                ),
            ),
        ]
    )
    arrow_table_with_special_character_column = pa.Table.from_pydict(TEST_DATA_WITH_SPECIAL_CHARACTER_COLUMN, schema=pa_schema)
    tbl = _create_table(session_catalog, identifier, {"format-version": format_version}, schema=pa_schema)

    tbl.append(arrow_table_with_special_character_column)
    spark_df = spark.sql(f"SELECT * FROM {identifier}").toPandas()
    pyiceberg_df = tbl.scan().to_pandas()
    assert spark_df.equals(pyiceberg_df)


@pytest.mark.integration
@pytest.mark.parametrize("format_version", [1, 2])
def test_python_writes_dictionary_encoded_column_with_spark_reads(
    spark: SparkSession, session_catalog: Catalog, format_version: int
) -> None:
    identifier = "default.python_writes_dictionary_encoded_column_with_spark_reads"
    TEST_DATA = {
        "id": [1, 2, 3, 1, 1],
        "name": ["AB", "CD", "EF", "CD", "EF"],
    }
    pa_schema = pa.schema(
        [
            pa.field("id", pa.dictionary(pa.int32(), pa.int32(), False)),
            pa.field("name", pa.dictionary(pa.int32(), pa.string(), False)),
        ]
    )
    arrow_table = pa.Table.from_pydict(TEST_DATA, schema=pa_schema)

    tbl = _create_table(session_catalog, identifier, {"format-version": format_version}, schema=pa_schema)

    tbl.append(arrow_table)
    spark_df = spark.sql(f"SELECT * FROM {identifier}").toPandas()
    pyiceberg_df = tbl.scan().to_pandas()

    # We're just interested in the content, PyIceberg actually makes a nice Categorical out of it:
    # E       AssertionError: Attributes of DataFrame.iloc[:, 1] (column name="name") are different
    # E
    # E       Attribute "dtype" are different
    # E       [left]:  object
    # E       [right]: CategoricalDtype(categories=['AB', 'CD', 'EF'], ordered=False, categories_dtype=object)
    pandas.testing.assert_frame_equal(spark_df, pyiceberg_df, check_dtype=False, check_categorical=False)


@pytest.mark.integration
@pytest.mark.parametrize("format_version", [1, 2])
def test_python_writes_with_small_and_large_types_spark_reads(
    spark: SparkSession, session_catalog: Catalog, format_version: int
) -> None:
    identifier = "default.python_writes_with_small_and_large_types_spark_reads"
    TEST_DATA = {
        "foo": ["a", None, "z"],
        "id": [1, 2, 3],
        "name": ["AB", "CD", "EF"],
        "address": [
            {"street": "123", "city": "SFO", "zip": 12345, "bar": "a"},
            {"street": "456", "city": "SW", "zip": 67890, "bar": "b"},
            {"street": "789", "city": "Random", "zip": 10112, "bar": "c"},
        ],
    }
    pa_schema = pa.schema(
        [
            pa.field("foo", pa.string()),
            pa.field("id", pa.int32()),
            pa.field("name", pa.string()),
            pa.field(
                "address",
                pa.struct(
                    [
                        pa.field("street", pa.string()),
                        pa.field("city", pa.string()),
                        pa.field("zip", pa.int32()),
                        pa.field("bar", pa.string()),
                    ]
                ),
            ),
        ]
    )
    arrow_table = pa.Table.from_pydict(TEST_DATA, schema=pa_schema)
    tbl = _create_table(session_catalog, identifier, {"format-version": format_version}, schema=pa_schema)

    tbl.append(arrow_table)
    spark_df = spark.sql(f"SELECT * FROM {identifier}").toPandas()
    pyiceberg_df = tbl.scan().to_pandas()
    assert spark_df.equals(pyiceberg_df)
    arrow_table_on_read = tbl.scan().to_arrow()
    assert arrow_table_on_read.schema == pa.schema(
        [
            pa.field("foo", pa.string()),
            pa.field("id", pa.int32()),
            pa.field("name", pa.string()),
            pa.field(
                "address",
                pa.struct(
                    [
                        pa.field("street", pa.string()),
                        pa.field("city", pa.string()),
                        pa.field("zip", pa.int32()),
                        pa.field("bar", pa.string()),
                    ]
                ),
            ),
        ]
    )


@pytest.mark.integration
def test_write_bin_pack_data_files(spark: SparkSession, session_catalog: Catalog, arrow_table_with_null: pa.Table) -> None:
    identifier = "default.write_bin_pack_data_files"
    tbl = _create_table(session_catalog, identifier, {"format-version": "1"}, [])

    def get_data_files_count(identifier: str) -> int:
        return spark.sql(
            f"""
            SELECT *
            FROM {identifier}.files
        """
        ).count()

    # writes 1 data file since the table is smaller than default target file size
    assert arrow_table_with_null.nbytes < TableProperties.WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT
    tbl.append(arrow_table_with_null)
    assert get_data_files_count(identifier) == 1

    # writes 1 data file as long as table is smaller than default target file size
    bigger_arrow_tbl = pa.concat_tables([arrow_table_with_null] * 10)
    assert bigger_arrow_tbl.nbytes < TableProperties.WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT
    tbl.overwrite(bigger_arrow_tbl)
    assert get_data_files_count(identifier) == 1

    # writes multiple data files once target file size is overridden
    target_file_size = arrow_table_with_null.nbytes
    tbl = tbl.transaction().set_properties({TableProperties.WRITE_TARGET_FILE_SIZE_BYTES: target_file_size}).commit_transaction()
    assert str(target_file_size) == tbl.properties.get(TableProperties.WRITE_TARGET_FILE_SIZE_BYTES)
    assert target_file_size < bigger_arrow_tbl.nbytes
    tbl.overwrite(bigger_arrow_tbl)
    assert get_data_files_count(identifier) == 10

    # writes half the number of data files when target file size doubles
    target_file_size = arrow_table_with_null.nbytes * 2
    tbl = tbl.transaction().set_properties({TableProperties.WRITE_TARGET_FILE_SIZE_BYTES: target_file_size}).commit_transaction()
    assert str(target_file_size) == tbl.properties.get(TableProperties.WRITE_TARGET_FILE_SIZE_BYTES)
    assert target_file_size < bigger_arrow_tbl.nbytes
    tbl.overwrite(bigger_arrow_tbl)
    assert get_data_files_count(identifier) == 5


@pytest.mark.integration
@pytest.mark.parametrize("format_version", [1, 2])
@pytest.mark.parametrize(
    "properties, expected_compression_name",
    [
        # REST catalog uses Zstandard by default: https://github.com/apache/iceberg/pull/8593
        ({}, "ZSTD"),
        ({"write.parquet.compression-codec": "uncompressed"}, "UNCOMPRESSED"),
        ({"write.parquet.compression-codec": "gzip", "write.parquet.compression-level": "1"}, "GZIP"),
        ({"write.parquet.compression-codec": "zstd", "write.parquet.compression-level": "1"}, "ZSTD"),
        ({"write.parquet.compression-codec": "snappy"}, "SNAPPY"),
    ],
)
def test_write_parquet_compression_properties(
    spark: SparkSession,
    session_catalog: Catalog,
    arrow_table_with_null: pa.Table,
    format_version: int,
    properties: dict[str, Any],
    expected_compression_name: str,
) -> None:
    identifier = "default.write_parquet_compression_properties"

    tbl = _create_table(session_catalog, identifier, {"format-version": format_version, **properties}, [arrow_table_with_null])

    data_file_paths = [task.file.file_path for task in tbl.scan().plan_files()]

    fs = S3FileSystem(
        endpoint_override=session_catalog.properties["s3.endpoint"],
        access_key=session_catalog.properties["s3.access-key-id"],
        secret_key=session_catalog.properties["s3.secret-access-key"],
    )
    uri = urlparse(data_file_paths[0])
    with fs.open_input_file(f"{uri.netloc}{uri.path}") as f:
        parquet_metadata = pq.read_metadata(f)
        compression = parquet_metadata.row_group(0).column(0).compression

    assert compression == expected_compression_name


@pytest.mark.integration
@pytest.mark.parametrize(
    "properties, expected_kwargs",
    [
        ({"write.parquet.page-size-bytes": "42"}, {"data_page_size": 42}),
        ({"write.parquet.dict-size-bytes": "42"}, {"dictionary_pagesize_limit": 42}),
    ],
)
def test_write_parquet_other_properties(
    mocker: MockerFixture,
    spark: SparkSession,
    session_catalog: Catalog,
    arrow_table_with_null: pa.Table,
    properties: dict[str, Any],
    expected_kwargs: dict[str, Any],
) -> None:
    identifier = "default.test_write_parquet_other_properties"

    # The properties we test cannot be checked on the resulting Parquet file, so we spy on the ParquetWriter call instead
    ParquetWriter = mocker.spy(pq, "ParquetWriter")
    _create_table(session_catalog, identifier, properties, [arrow_table_with_null])

    call_kwargs = ParquetWriter.call_args[1]
    for key, value in expected_kwargs.items():
        assert call_kwargs.get(key) == value


@pytest.mark.integration
@pytest.mark.parametrize(
    "properties",
    [
        {"write.parquet.row-group-size-bytes": "42"},
        {"write.parquet.bloom-filter-enabled.column.bool": "42"},
        {"write.parquet.bloom-filter-max-bytes": "42"},
    ],
)
def test_write_parquet_unsupported_properties(
    spark: SparkSession,
    session_catalog: Catalog,
    arrow_table_with_null: pa.Table,
    properties: dict[str, str],
) -> None:
    identifier = "default.write_parquet_unsupported_properties"

    tbl = _create_table(session_catalog, identifier, properties, [])
    with pytest.warns(UserWarning, match=r"Parquet writer option.*"):
        tbl.append(arrow_table_with_null)


@pytest.mark.integration
@pytest.mark.parametrize("format_version", [1, 2])
def test_spark_writes_orc_pyiceberg_reads(spark: SparkSession, session_catalog: Catalog, format_version: int) -> None:
    """Test that ORC files written by Spark can be read by PyIceberg."""
    identifier = f"default.spark_writes_orc_pyiceberg_reads_v{format_version}"

    # Create test data
    test_data = [
        (1, "Alice", 25, True),
        (2, "Bob", 30, False),
        (3, "Charlie", 35, True),
        (4, "David", 28, True),
        (5, "Eve", 32, False),
    ]

    # Create Spark DataFrame
    spark_df = spark.createDataFrame(test_data, ["id", "name", "age", "is_active"])

    # Ensure a clean slate to avoid replacing a v2 table with v1
    spark.sql(f"DROP TABLE IF EXISTS {identifier}")

    # Create table with Spark using ORC format and desired format-version
    spark_df.writeTo(identifier).using("iceberg").tableProperty("write.format.default", "orc").tableProperty(
        "format-version", str(format_version)
    ).createOrReplace()

    # Write data with ORC format using Spark
    spark_df.writeTo(identifier).using("iceberg").append()

    # Read with PyIceberg - this is the main focus of our validation
    tbl = session_catalog.load_table(identifier)
    pyiceberg_df = tbl.scan().to_pandas()

    # Verify PyIceberg results have the expected number of rows
    assert len(pyiceberg_df) == 10  # 5 rows from create + 5 rows from append

    # Verify PyIceberg column names
    assert list(pyiceberg_df.columns) == ["id", "name", "age", "is_active"]

    # Verify PyIceberg data integrity - check the actual data values
    expected_data = [
        (1, "Alice", 25, True),
        (2, "Bob", 30, False),
        (3, "Charlie", 35, True),
        (4, "David", 28, True),
        (5, "Eve", 32, False),
    ]

    # Verify PyIceberg results contain the expected data (appears twice due to create + append)
    pyiceberg_data = list(
        zip(pyiceberg_df["id"], pyiceberg_df["name"], pyiceberg_df["age"], pyiceberg_df["is_active"], strict=True)
    )
    assert pyiceberg_data == expected_data + expected_data  # Data should appear twice

    # Verify PyIceberg data types are correct
    assert pyiceberg_df["id"].dtype == "int64"
    assert pyiceberg_df["name"].dtype == "object"  # string
    assert pyiceberg_df["age"].dtype == "int64"
    assert pyiceberg_df["is_active"].dtype == "bool"

    # Cross-validate with Spark to ensure consistency (ensure deterministic ordering)
    spark_result = spark.sql(f"SELECT * FROM {identifier}").toPandas()
    sort_cols = ["id", "name", "age", "is_active"]
    spark_result = spark_result.sort_values(by=sort_cols).reset_index(drop=True)
    pyiceberg_df = pyiceberg_df.sort_values(by=sort_cols).reset_index(drop=True)
    pandas.testing.assert_frame_equal(spark_result, pyiceberg_df, check_dtype=False)

    # Verify the files are actually ORC format
    files = list(tbl.scan().plan_files())
    assert len(files) > 0
    for file_task in files:
        assert file_task.file.file_format == FileFormat.ORC


@pytest.mark.integration
def test_invalid_arguments(spark: SparkSession, session_catalog: Catalog, arrow_table_with_null: pa.Table) -> None:
    identifier = "default.arrow_data_files"
    tbl = _create_table(session_catalog, identifier, {"format-version": "1"}, [])

    with pytest.raises(ValueError, match="Expected PyArrow table, got: not a df"):
        tbl.overwrite("not a df")

    with pytest.raises(ValueError, match="Expected PyArrow table, got: not a df"):
        tbl.append("not a df")


@pytest.mark.integration
def test_summaries_with_only_nulls(
    spark: SparkSession, session_catalog: Catalog, arrow_table_without_data: pa.Table, arrow_table_with_only_nulls: pa.Table
) -> None:
    identifier = "default.arrow_table_summaries_with_only_nulls"
    tbl = _create_table(
        session_catalog, identifier, {"format-version": "1"}, [arrow_table_without_data, arrow_table_with_only_nulls]
    )
    tbl.overwrite(arrow_table_without_data)

    rows = spark.sql(
        f"""
        SELECT operation, summary
        FROM {identifier}.snapshots
        ORDER BY committed_at ASC
    """
    ).collect()

    operations = [row.operation for row in rows]
    assert operations == ["append", "append", "delete", "append"]

    summaries = [row.summary for row in rows]

    file_size = int(summaries[1]["added-files-size"])
    assert file_size > 0

    assert summaries[0] == {
        "total-data-files": "0",
        "total-delete-files": "0",
        "total-equality-deletes": "0",
        "total-files-size": "0",
        "total-position-deletes": "0",
        "total-records": "0",
    }

    assert summaries[1] == {
        "added-data-files": "1",
        "added-files-size": str(file_size),
        "added-records": "2",
        "total-data-files": "1",
        "total-delete-files": "0",
        "total-equality-deletes": "0",
        "total-files-size": str(file_size),
        "total-position-deletes": "0",
        "total-records": "2",
    }

    assert summaries[2] == {
        "deleted-data-files": "1",
        "deleted-records": "2",
        "removed-files-size": str(file_size),
        "total-data-files": "0",
        "total-delete-files": "0",
        "total-equality-deletes": "0",
        "total-files-size": "0",
        "total-position-deletes": "0",
        "total-records": "0",
    }

    assert summaries[3] == {
        "total-data-files": "0",
        "total-delete-files": "0",
        "total-equality-deletes": "0",
        "total-files-size": "0",
        "total-position-deletes": "0",
        "total-records": "0",
    }


@pytest.mark.integration
def test_duckdb_url_import(warehouse: Path, arrow_table_with_null: pa.Table) -> None:
    os.environ["TZ"] = "Etc/UTC"
    time.tzset()
    tz = pytz.timezone(os.environ["TZ"])

    catalog = SqlCatalog("test_sql_catalog", uri="sqlite:///:memory:", warehouse=f"/{warehouse}")
    catalog.create_namespace("default")

    identifier = "default.arrow_table_v1_with_null"
    tbl = _create_table(catalog, identifier, {}, [arrow_table_with_null])
    location = tbl.metadata_location

    import duckdb

    duckdb.sql("INSTALL iceberg; LOAD iceberg;")
    result = duckdb.sql(
        f"""
    SELECT *
    FROM iceberg_scan('{location}')
    """
    ).fetchall()

    assert result == [
        (
            False,
            "a",
            "aaaaaaaaaaaaaaaaaaaaaa",
            1,
            1,
            0.0,
            0.0,
            datetime(2023, 1, 1, 19, 25),
            datetime(2023, 1, 1, 19, 25, tzinfo=tz),
            date(2023, 1, 1),
            b"\x01",
            b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00",
        ),
        (None, None, None, None, None, None, None, None, None, None, None, None),
        (
            True,
            "z",
            "zzzzzzzzzzzzzzzzzzzzzz",
            9,
            9,
            0.8999999761581421,
            0.9,
            datetime(2023, 3, 1, 19, 25),
            datetime(2023, 3, 1, 19, 25, tzinfo=tz),
            date(2023, 3, 1),
            b"\x12",
            b"\x11\x11\x11\x11\x11\x11\x11\x11\x11\x11\x11\x11\x11\x11\x11\x11",
        ),
    ]


@pytest.mark.integration
@pytest.mark.parametrize("format_version", [1, 2])
def test_write_and_evolve(session_catalog: Catalog, format_version: int) -> None:
    identifier = f"default.arrow_write_data_and_evolve_schema_v{format_version}"

    try:
        session_catalog.drop_table(identifier=identifier)
    except NoSuchTableError:
        pass

    pa_table = pa.Table.from_pydict(
        {
            "foo": ["a", None, "z"],
        },
        schema=pa.schema([pa.field("foo", pa.string(), nullable=True)]),
    )

    tbl = session_catalog.create_table(
        identifier=identifier, schema=pa_table.schema, properties={"format-version": str(format_version)}
    )

    pa_table_with_column = pa.Table.from_pydict(
        {
            "foo": ["a", None, "z"],
            "bar": [19, None, 25],
        },
        schema=pa.schema(
            [
                pa.field("foo", pa.string(), nullable=True),
                pa.field("bar", pa.int32(), nullable=True),
            ]
        ),
    )

    with tbl.transaction() as txn:
        with txn.update_schema() as schema_txn:
            schema_txn.union_by_name(pa_table_with_column.schema)

        txn.append(pa_table_with_column)
        txn.overwrite(pa_table_with_column)
        txn.delete("foo = 'a'")


@pytest.mark.integration
@pytest.mark.parametrize("format_version", [1, 2])
@pytest.mark.parametrize("catalog", [pytest.lazy_fixture("session_catalog_hive"), pytest.lazy_fixture("session_catalog")])
def test_create_table_transaction(catalog: Catalog, format_version: int) -> None:
    identifier = f"default.arrow_create_table_transaction_{catalog.name}_{format_version}"

    try:
        catalog.drop_table(identifier=identifier)
    except NoSuchTableError:
        pass

    pa_table = pa.Table.from_pydict(
        {
            "foo": ["a", None, "z"],
        },
        schema=pa.schema([pa.field("foo", pa.string(), nullable=True)]),
    )

    pa_table_with_column = pa.Table.from_pydict(
        {
            "foo": ["a", None, "z"],
            "bar": [19, None, 25],
        },
        schema=pa.schema(
            [
                pa.field("foo", pa.string(), nullable=True),
                pa.field("bar", pa.int32(), nullable=True),
            ]
        ),
    )

    with catalog.create_table_transaction(
        identifier=identifier, schema=pa_table.schema, properties={"format-version": str(format_version)}
    ) as txn:
        with txn.update_snapshot().fast_append() as snapshot_update:
            for data_file in _dataframe_to_data_files(table_metadata=txn.table_metadata, df=pa_table, io=txn._table.io):
                snapshot_update.append_data_file(data_file)

        with txn.update_schema() as schema_txn:
            schema_txn.union_by_name(pa_table_with_column.schema)

        with txn.update_snapshot().fast_append() as snapshot_update:
            for data_file in _dataframe_to_data_files(
                table_metadata=txn.table_metadata, df=pa_table_with_column, io=txn._table.io
            ):
                snapshot_update.append_data_file(data_file)

    tbl = catalog.load_table(identifier=identifier)
    assert tbl.format_version == format_version
    assert len(tbl.scan().to_arrow()) == 6


@pytest.mark.integration
@pytest.mark.parametrize("format_version", [1, 2])
@pytest.mark.parametrize("catalog", [pytest.lazy_fixture("session_catalog_hive"), pytest.lazy_fixture("session_catalog")])
def test_create_table_with_non_default_values(catalog: Catalog, table_schema_with_all_types: Schema, format_version: int) -> None:
    identifier = f"default.arrow_create_table_transaction_with_non_default_values_{catalog.name}_{format_version}"
    identifier_ref = f"default.arrow_create_table_transaction_with_non_default_values_ref_{catalog.name}_{format_version}"

    try:
        catalog.drop_table(identifier=identifier)
    except NoSuchTableError:
        pass

    try:
        catalog.drop_table(identifier=identifier_ref)
    except NoSuchTableError:
        pass

    iceberg_spec = PartitionSpec(
        *[PartitionField(source_id=2, field_id=1001, transform=IdentityTransform(), name="integer_partition")]
    )

    sort_order = SortOrder(*[SortField(source_id=2, transform=IdentityTransform(), direction=SortDirection.ASC)])

    txn = catalog.create_table_transaction(
        identifier=identifier,
        schema=table_schema_with_all_types,
        partition_spec=iceberg_spec,
        sort_order=sort_order,
        properties={"format-version": format_version},
    )
    txn.commit_transaction()

    tbl = catalog.load_table(identifier)

    tbl_ref = catalog.create_table(
        identifier=identifier_ref,
        schema=table_schema_with_all_types,
        partition_spec=iceberg_spec,
        sort_order=sort_order,
        properties={"format-version": format_version},
    )

    assert tbl.format_version == tbl_ref.format_version
    assert tbl.schema() == tbl_ref.schema()
    assert tbl.schemas() == tbl_ref.schemas()
    assert tbl.spec() == tbl_ref.spec()
    assert tbl.specs() == tbl_ref.specs()
    assert tbl.sort_order() == tbl_ref.sort_order()
    assert tbl.sort_orders() == tbl_ref.sort_orders()


@pytest.mark.integration
@pytest.mark.parametrize("format_version", [1, 2])
def test_table_properties_int_value(
    session_catalog: Catalog,
    arrow_table_with_null: pa.Table,
    format_version: int,
) -> None:
    # table properties can be set to int, but still serialized to string
    property_with_int = {"property_name": 42}
    identifier = "default.test_table_properties_int_value"

    tbl = _create_table(
        session_catalog, identifier, {"format-version": format_version, **property_with_int}, [arrow_table_with_null]
    )
    assert isinstance(tbl.properties["property_name"], str)


@pytest.mark.integration
@pytest.mark.parametrize("format_version", [1, 2])
def test_table_properties_raise_for_none_value(
    session_catalog: Catalog,
    arrow_table_with_null: pa.Table,
    format_version: int,
) -> None:
    property_with_none = {"property_name": None}
    identifier = "default.test_table_properties_raise_for_none_value"

    with pytest.raises(ValidationError) as exc_info:
        _ = _create_table(
            session_catalog, identifier, {"format-version": format_version, **property_with_none}, [arrow_table_with_null]
        )
    assert "None type is not a supported value in properties: property_name" in str(exc_info.value)


@pytest.mark.integration
@pytest.mark.parametrize("format_version", [1, 2])
def test_inspect_snapshots(
    spark: SparkSession, session_catalog: Catalog, arrow_table_with_null: pa.Table, format_version: int
) -> None:
    identifier = "default.table_metadata_snapshots"
    tbl = _create_table(session_catalog, identifier, properties={"format-version": format_version})

    tbl.append(arrow_table_with_null)
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
    assert df["parent_id"][1:].to_pylist() == df["snapshot_id"][:-1].to_pylist()

    assert [operation.as_py() for operation in df["operation"]] == ["append", "delete", "append", "append"]

    for manifest_list in df["manifest_list"]:
        assert manifest_list.as_py().startswith("s3://")

    file_size = int(next(value for key, value in df["summary"][0].as_py() if key == "added-files-size"))
    assert file_size > 0

    # Append
    assert df["summary"][0].as_py() == [
        ("added-files-size", str(file_size)),
        ("added-data-files", "1"),
        ("added-records", "3"),
        ("total-data-files", "1"),
        ("total-delete-files", "0"),
        ("total-records", "3"),
        ("total-files-size", str(file_size)),
        ("total-position-deletes", "0"),
        ("total-equality-deletes", "0"),
    ]

    # Delete
    assert df["summary"][1].as_py() == [
        ("removed-files-size", str(file_size)),
        ("deleted-data-files", "1"),
        ("deleted-records", "3"),
        ("total-data-files", "0"),
        ("total-delete-files", "0"),
        ("total-records", "0"),
        ("total-files-size", "0"),
        ("total-position-deletes", "0"),
        ("total-equality-deletes", "0"),
    ]

    lhs = spark.table(f"{identifier}.snapshots").toPandas()
    rhs = df.to_pandas()
    for column in df.column_names:
        for left, right in zip(lhs[column].to_list(), rhs[column].to_list(), strict=True):
            if column == "summary":
                # Arrow returns a list of tuples, instead of a dict
                right = dict(right)

            if isinstance(left, float) and math.isnan(left) and isinstance(right, float) and math.isnan(right):
                # NaN != NaN in Python
                continue

            assert left == right, f"Difference in column {column}: {left} != {right}"


@pytest.mark.integration
def test_write_within_transaction(spark: SparkSession, session_catalog: Catalog, arrow_table_with_null: pa.Table) -> None:
    identifier = "default.write_in_open_transaction"
    tbl = _create_table(session_catalog, identifier, {"format-version": "1"}, [])

    def get_metadata_entries_count(identifier: str) -> int:
        return spark.sql(
            f"""
            SELECT *
            FROM {identifier}.metadata_log_entries
        """
        ).count()

    # one metadata entry from table creation
    assert get_metadata_entries_count(identifier) == 1

    # one more metadata entry from transaction
    with tbl.transaction() as tx:
        tx.set_properties({"test": "1"})
        tx.append(arrow_table_with_null)
    assert get_metadata_entries_count(identifier) == 2

    # two more metadata entries added from two separate transactions
    tbl.transaction().set_properties({"test": "2"}).commit_transaction()
    tbl.append(arrow_table_with_null)
    assert get_metadata_entries_count(identifier) == 4


@pytest.mark.integration
@pytest.mark.parametrize("format_version", [1, 2])
def test_hive_catalog_storage_descriptor(
    session_catalog_hive: HiveCatalog,
    pa_schema: pa.Schema,
    arrow_table_with_null: pa.Table,
    spark: SparkSession,
    format_version: int,
) -> None:
    tbl = _create_table(
        session_catalog_hive, "default.test_storage_descriptor", {"format-version": format_version}, [arrow_table_with_null]
    )

    # check if pyiceberg can read the table
    assert len(tbl.scan().to_arrow()) == 3
    # check if spark can read the table
    assert spark.sql("SELECT * FROM hive.default.test_storage_descriptor").count() == 3


@pytest.mark.integration
@pytest.mark.parametrize("format_version", [1, 2])
def test_hive_catalog_storage_descriptor_has_changed(
    session_catalog_hive: HiveCatalog,
    pa_schema: pa.Schema,
    arrow_table_with_null: pa.Table,
    spark: SparkSession,
    format_version: int,
) -> None:
    tbl = _create_table(
        session_catalog_hive, "default.test_storage_descriptor", {"format-version": format_version}, [arrow_table_with_null]
    )

    with tbl.transaction() as tx:
        with tx.update_schema() as schema:
            schema.update_column("string_long", doc="this is string_long")
            schema.update_column("binary", doc="this is binary")

    with session_catalog_hive._client as open_client:
        hive_table = session_catalog_hive._get_hive_table(open_client, "default", "test_storage_descriptor")
        assert "this is string_long" in str(hive_table.sd)
        assert "this is binary" in str(hive_table.sd)


@pytest.mark.integration
@pytest.mark.parametrize("catalog", [pytest.lazy_fixture("session_catalog_hive"), pytest.lazy_fixture("session_catalog")])
def test_sanitize_character_partitioned(catalog: Catalog) -> None:
    table_name = "default.test_table_partitioned_sanitized_character"
    try:
        catalog.drop_table(table_name)
    except NoSuchTableError:
        pass

    tbl = _create_table(
        session_catalog=catalog,
        identifier=table_name,
        schema=Schema(NestedField(field_id=1, name="some.id", type=IntegerType(), required=True)),
        partition_spec=PartitionSpec(
            PartitionField(source_id=1, field_id=1000, name="some.id_identity", transform=IdentityTransform())
        ),
        data=[pa.Table.from_arrays([range(22)], schema=pa.schema([pa.field("some.id", pa.int32(), nullable=False)]))],
    )

    assert len(tbl.scan().to_arrow()) == 22


@pytest.mark.integration
@pytest.mark.parametrize("catalog", [pytest.lazy_fixture("session_catalog")])
def test_sanitize_character_partitioned_avro_bug(catalog: Catalog) -> None:
    table_name = "default.test_table_partitioned_sanitized_character_avro"
    try:
        catalog.drop_table(table_name)
    except NoSuchTableError:
        pass

    schema = Schema(
        NestedField(id=1, name="😎", field_type=StringType(), required=False),
    )

    partition_spec = PartitionSpec(
        PartitionField(
            source_id=1,
            field_id=1001,
            transform=IdentityTransform(),
            name="😎",
        )
    )

    tbl = _create_table(
        session_catalog=catalog,
        identifier=table_name,
        schema=schema,
        partition_spec=partition_spec,
        data=[
            pa.Table.from_arrays(
                [pa.array([str(i) for i in range(22)])], schema=pa.schema([pa.field("😎", pa.string(), nullable=False)])
            )
        ],
    )

    assert len(tbl.scan().to_arrow()) == 22

    # verify that we can read the table with DuckDB
    import duckdb

    location = tbl.metadata_location
    duckdb.sql("INSTALL iceberg; LOAD iceberg;")
    # Configure S3 settings for DuckDB to match the catalog configuration
    duckdb.sql("SET s3_endpoint='localhost:9000';")
    duckdb.sql("SET s3_access_key_id='admin';")
    duckdb.sql("SET s3_secret_access_key='password';")
    duckdb.sql("SET s3_use_ssl=false;")
    duckdb.sql("SET s3_url_style='path';")
    result = duckdb.sql(f"SELECT * FROM iceberg_scan('{location}')").fetchall()
    assert len(result) == 22


@pytest.mark.integration
@pytest.mark.parametrize("format_version", [1, 2])
def test_cross_platform_special_character_compatibility(
    spark: SparkSession, session_catalog: Catalog, format_version: int
) -> None:
    """Test cross-platform compatibility with special characters in column names."""
    identifier = "default.test_cross_platform_special_characters"

    # Test various special characters that need sanitization
    special_characters = [
        "😎",  # emoji - Java produces _xD83D_xDE0E, Python produces _x1F60E
        "a.b",  # dot - both should produce a_x2Eb
        "a#b",  # hash - both should produce a_x23b
        "9x",  # starts with digit - both should produce _9x
        "x_",  # valid - should remain unchanged
        "letter/abc",  # slash - both should produce letter_x2Fabc
    ]

    for i, special_char in enumerate(special_characters):
        table_name = f"{identifier}_{format_version}_{i}"
        pyiceberg_table_name = f"{identifier}_pyiceberg_{format_version}_{i}"

        try:
            session_catalog.drop_table(table_name)
        except Exception:
            pass
        try:
            session_catalog.drop_table(pyiceberg_table_name)
        except Exception:
            pass

        try:
            # Test 1: Spark writes, PyIceberg reads
            spark_df = spark.createDataFrame([("test_value",)], [special_char])
            spark_df.writeTo(table_name).using("iceberg").createOrReplace()

            # Read with PyIceberg table scan
            tbl = session_catalog.load_table(table_name)
            pyiceberg_df = tbl.scan().to_pandas()
            assert len(pyiceberg_df) == 1
            assert special_char in pyiceberg_df.columns
            assert pyiceberg_df.iloc[0][special_char] == "test_value"

            # Test 2: PyIceberg writes, Spark reads
            from pyiceberg.schema import Schema
            from pyiceberg.types import NestedField, StringType

            schema = Schema(NestedField(field_id=1, name=special_char, field_type=StringType(), required=True))

            tbl_pyiceberg = session_catalog.create_table(
                identifier=pyiceberg_table_name, schema=schema, properties={"format-version": str(format_version)}
            )

            import pyarrow as pa

            # Create PyArrow schema with required field to match Iceberg schema
            pa_schema = pa.schema([pa.field(special_char, pa.string(), nullable=False)])
            data = pa.Table.from_pydict({special_char: ["pyiceberg_value"]}, schema=pa_schema)
            tbl_pyiceberg.append(data)

            # Read with Spark
            spark_df_read = spark.table(pyiceberg_table_name)
            spark_result = spark_df_read.collect()

            # Verify data integrity
            assert len(spark_result) == 1
            assert special_char in spark_df_read.columns
            assert spark_result[0][special_char] == "pyiceberg_value"

        finally:
            try:
                session_catalog.drop_table(table_name)
            except Exception:
                pass
            try:
                session_catalog.drop_table(pyiceberg_table_name)
            except Exception:
                pass


@pytest.mark.integration
@pytest.mark.parametrize("format_version", [1, 2])
def test_table_write_subset_of_schema(session_catalog: Catalog, arrow_table_with_null: pa.Table, format_version: int) -> None:
    identifier = "default.test_table_write_subset_of_schema"
    tbl = _create_table(session_catalog, identifier, {"format-version": format_version}, [arrow_table_with_null])
    arrow_table_without_some_columns = arrow_table_with_null.combine_chunks().drop(arrow_table_with_null.column_names[0])
    assert len(arrow_table_without_some_columns.columns) < len(arrow_table_with_null.columns)
    tbl.overwrite(arrow_table_without_some_columns)
    tbl.append(arrow_table_without_some_columns)
    # overwrite and then append should produce twice the data
    assert len(tbl.scan().to_arrow()) == len(arrow_table_without_some_columns) * 2


@pytest.mark.integration
@pytest.mark.parametrize("format_version", [1, 2])
@pytest.mark.filterwarnings("ignore:Delete operation did not match any records")
def test_table_write_out_of_order_schema(session_catalog: Catalog, arrow_table_with_null: pa.Table, format_version: int) -> None:
    identifier = "default.test_table_write_out_of_order_schema"
    # rotate the schema fields by 1
    fields = list(arrow_table_with_null.schema)
    rotated_fields = fields[1:] + fields[:1]
    rotated_schema = pa.schema(rotated_fields)
    assert arrow_table_with_null.schema != rotated_schema
    tbl = _create_table(session_catalog, identifier, {"format-version": format_version}, schema=rotated_schema)

    tbl.overwrite(arrow_table_with_null)

    tbl.append(arrow_table_with_null)
    # overwrite and then append should produce twice the data
    assert len(tbl.scan().to_arrow()) == len(arrow_table_with_null) * 2


@pytest.mark.integration
@pytest.mark.parametrize("format_version", [1, 2])
def test_table_write_schema_with_valid_nullability_diff(
    spark: SparkSession, session_catalog: Catalog, arrow_table_with_null: pa.Table, format_version: int
) -> None:
    identifier = "default.test_table_write_with_valid_nullability_diff"
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
    tbl = _create_table(session_catalog, identifier, {"format-version": format_version}, [arrow_table], schema=table_schema)
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
def test_table_write_schema_with_valid_upcast(
    spark: SparkSession,
    session_catalog: Catalog,
    format_version: int,
    table_schema_with_promoted_types: Schema,
    pyarrow_schema_with_promoted_types: pa.Schema,
    pyarrow_table_with_promoted_types: pa.Table,
) -> None:
    identifier = "default.test_table_write_with_valid_upcast"

    tbl = _create_table(
        session_catalog,
        identifier,
        {"format-version": format_version},
        [pyarrow_table_with_promoted_types],
        schema=table_schema_with_promoted_types,
    )
    # table's long field should cast to long on read
    written_arrow_table = tbl.scan().to_arrow()
    assert written_arrow_table == pyarrow_table_with_promoted_types.cast(
        pa.schema(
            (
                pa.field("long", pa.int64(), nullable=True),
                pa.field("list", pa.list_(pa.int64()), nullable=False),
                pa.field("map", pa.map_(pa.string(), pa.int64()), nullable=False),
                pa.field("double", pa.float64(), nullable=True),  # can support upcasting float to double
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
def test_write_all_timestamp_precision(
    mocker: MockerFixture,
    spark: SparkSession,
    session_catalog: Catalog,
    format_version: int,
    arrow_table_schema_with_all_timestamp_precisions: pa.Schema,
    arrow_table_with_all_timestamp_precisions: pa.Table,
    arrow_table_schema_with_all_microseconds_timestamp_precisions: pa.Schema,
) -> None:
    identifier = "default.table_all_timestamp_precision"
    mocker.patch.dict(os.environ, values={"PYICEBERG_DOWNCAST_NS_TIMESTAMP_TO_US_ON_WRITE": "True"})

    tbl = _create_table(
        session_catalog,
        identifier,
        {"format-version": format_version},
        data=[arrow_table_with_all_timestamp_precisions],
        schema=arrow_table_schema_with_all_timestamp_precisions,
    )
    tbl.overwrite(arrow_table_with_all_timestamp_precisions)
    written_arrow_table = tbl.scan().to_arrow()

    assert written_arrow_table.schema == arrow_table_schema_with_all_microseconds_timestamp_precisions
    assert written_arrow_table == arrow_table_with_all_timestamp_precisions.cast(
        arrow_table_schema_with_all_microseconds_timestamp_precisions, safe=False
    )
    lhs = spark.table(f"{identifier}").toPandas()
    rhs = written_arrow_table.to_pandas()

    for column in written_arrow_table.column_names:
        for left, right in zip(lhs[column].to_list(), rhs[column].to_list(), strict=True):
            if pd.isnull(left):
                assert pd.isnull(right)
            else:
                # Check only upto microsecond precision since Spark loaded dtype is timezone unaware
                # and supports upto microsecond precision
                assert left.timestamp() == right.timestamp(), f"Difference in column {column}: {left} != {right}"


@pytest.mark.integration
@pytest.mark.parametrize("format_version", [1, 2])
def test_merge_manifests(session_catalog: Catalog, arrow_table_with_null: pa.Table, format_version: int) -> None:
    tbl_a = _create_table(
        session_catalog,
        "default.merge_manifest_a",
        {"commit.manifest-merge.enabled": "true", "commit.manifest.min-count-to-merge": "1", "format-version": format_version},
        [],
    )
    tbl_b = _create_table(
        session_catalog,
        "default.merge_manifest_b",
        {
            "commit.manifest-merge.enabled": "true",
            "commit.manifest.min-count-to-merge": "1",
            "commit.manifest.target-size-bytes": "1",
            "format-version": format_version,
        },
        [],
    )
    tbl_c = _create_table(
        session_catalog,
        "default.merge_manifest_c",
        {"commit.manifest.min-count-to-merge": "1", "format-version": format_version},
        [],
    )

    # tbl_a should merge all manifests into 1
    tbl_a.append(arrow_table_with_null)
    tbl_a.append(arrow_table_with_null)
    tbl_a.append(arrow_table_with_null)

    # tbl_b should not merge any manifests because the target size is too small
    tbl_b.append(arrow_table_with_null)
    tbl_b.append(arrow_table_with_null)
    tbl_b.append(arrow_table_with_null)

    # tbl_c should not merge any manifests because merging is disabled
    tbl_c.append(arrow_table_with_null)
    tbl_c.append(arrow_table_with_null)
    tbl_c.append(arrow_table_with_null)

    assert len(tbl_a.current_snapshot().manifests(tbl_a.io)) == 1  # type: ignore
    assert len(tbl_b.current_snapshot().manifests(tbl_b.io)) == 3  # type: ignore
    assert len(tbl_c.current_snapshot().manifests(tbl_c.io)) == 3  # type: ignore

    # tbl_a and tbl_c should contain the same data
    assert tbl_a.scan().to_arrow().equals(tbl_c.scan().to_arrow())
    # tbl_b and tbl_c should contain the same data
    assert tbl_b.scan().to_arrow().equals(tbl_c.scan().to_arrow())


@pytest.mark.integration
@pytest.mark.parametrize("format_version", [1, 2])
def test_merge_manifests_file_content(session_catalog: Catalog, arrow_table_with_null: pa.Table, format_version: int) -> None:
    tbl_a = _create_table(
        session_catalog,
        "default.merge_manifest_a",
        {"commit.manifest-merge.enabled": "true", "commit.manifest.min-count-to-merge": "1", "format-version": format_version},
        [],
    )

    # tbl_a should merge all manifests into 1
    tbl_a.append(arrow_table_with_null)

    tbl_a_first_entries = tbl_a.inspect.entries().to_pydict()
    first_snapshot_id = tbl_a_first_entries["snapshot_id"][0]
    first_data_file_path = tbl_a_first_entries["data_file"][0]["file_path"]

    tbl_a.append(arrow_table_with_null)
    tbl_a.append(arrow_table_with_null)

    assert len(tbl_a.current_snapshot().manifests(tbl_a.io)) == 1  # type: ignore

    # verify the sequence number of tbl_a's only manifest file
    tbl_a_manifest = tbl_a.current_snapshot().manifests(tbl_a.io)[0]  # type: ignore
    assert tbl_a_manifest.sequence_number == (3 if format_version == 2 else 0)
    assert tbl_a_manifest.min_sequence_number == (1 if format_version == 2 else 0)

    # verify the manifest entries of tbl_a, in which the manifests are merged
    tbl_a_entries = tbl_a.inspect.entries().to_pydict()
    assert tbl_a_entries["status"] == [1, 0, 0]
    assert tbl_a_entries["sequence_number"] == [3, 2, 1] if format_version == 2 else [0, 0, 0]
    assert tbl_a_entries["file_sequence_number"] == [3, 2, 1] if format_version == 2 else [0, 0, 0]
    for i in range(3):
        tbl_a_data_file = tbl_a_entries["data_file"][i]
        assert tbl_a_data_file["column_sizes"] == [
            (1, 51),
            (2, 80),
            (3, 130),
            (4, 96),
            (5, 120),
            (6, 96),
            (7, 120),
            (8, 120),
            (9, 120),
            (10, 96),
            (11, 80),
            (12, 111),
        ]
        assert tbl_a_data_file["content"] == 0
        assert tbl_a_data_file["equality_ids"] is None
        assert tbl_a_data_file["file_format"] == "PARQUET"
        assert tbl_a_data_file["file_path"].startswith("s3://warehouse/default/merge_manifest_a/data/")
        if tbl_a_data_file["file_path"] == first_data_file_path:
            # verify that the snapshot id recorded should be the one where the file was added
            assert tbl_a_entries["snapshot_id"][i] == first_snapshot_id
        assert tbl_a_data_file["key_metadata"] is None
        assert tbl_a_data_file["lower_bounds"] == [
            (1, b"\x00"),
            (2, b"a"),
            (3, b"aaaaaaaaaaaaaaaa"),
            (4, b"\x01\x00\x00\x00"),
            (5, b"\x01\x00\x00\x00\x00\x00\x00\x00"),
            (6, b"\x00\x00\x00\x80"),
            (7, b"\x00\x00\x00\x00\x00\x00\x00\x80"),
            (8, b"\x00\x9bj\xca8\xf1\x05\x00"),
            (9, b"\x00\x9bj\xca8\xf1\x05\x00"),
            (10, b"\x9eK\x00\x00"),
            (11, b"\x01"),
            (12, b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"),
        ]
        assert tbl_a_data_file["nan_value_counts"] == []
        assert tbl_a_data_file["null_value_counts"] == [
            (1, 1),
            (2, 1),
            (3, 1),
            (4, 1),
            (5, 1),
            (6, 1),
            (7, 1),
            (8, 1),
            (9, 1),
            (10, 1),
            (11, 1),
            (12, 1),
        ]
        assert tbl_a_data_file["partition"] == {}
        assert tbl_a_data_file["record_count"] == 3
        assert tbl_a_data_file["sort_order_id"] is None
        assert tbl_a_data_file["split_offsets"] == [4]
        assert tbl_a_data_file["upper_bounds"] == [
            (1, b"\x01"),
            (2, b"z"),
            (3, b"zzzzzzzzzzzzzzz{"),
            (4, b"\t\x00\x00\x00"),
            (5, b"\t\x00\x00\x00\x00\x00\x00\x00"),
            (6, b"fff?"),
            (7, b"\xcd\xcc\xcc\xcc\xcc\xcc\xec?"),
            (8, b"\x00\xbb\r\xab\xdb\xf5\x05\x00"),
            (9, b"\x00\xbb\r\xab\xdb\xf5\x05\x00"),
            (10, b"\xd9K\x00\x00"),
            (11, b"\x12"),
            (12, b"\x11\x11\x11\x11\x11\x11\x11\x11\x11\x11\x11\x11\x11\x11\x11\x11"),
        ]
        assert tbl_a_data_file["value_counts"] == [
            (1, 3),
            (2, 3),
            (3, 3),
            (4, 3),
            (5, 3),
            (6, 3),
            (7, 3),
            (8, 3),
            (9, 3),
            (10, 3),
            (11, 3),
            (12, 3),
        ]


@pytest.mark.integration
def test_rest_catalog_with_empty_catalog_name_append_data(session_catalog: Catalog, arrow_table_with_null: pa.Table) -> None:
    identifier = "default.test_rest_append"
    test_catalog = load_catalog(
        "",  # intentionally empty
        **session_catalog.properties,
    )
    tbl = _create_table(test_catalog, identifier, data=[])
    tbl.append(arrow_table_with_null)


@pytest.mark.integration
def test_table_v1_with_null_nested_namespace(session_catalog: Catalog, arrow_table_with_null: pa.Table) -> None:
    identifier = "default.table_v1_with_null_nested_namespace"
    tbl = _create_table(session_catalog, identifier, {"format-version": "1"}, [arrow_table_with_null])
    assert tbl.format_version == 1, f"Expected v1, got: v{tbl.format_version}"

    assert session_catalog.load_table(identifier) is not None
    assert session_catalog.table_exists(identifier)

    # We expect no error here
    session_catalog.drop_table(identifier)


@pytest.mark.integration
def test_view_exists(
    spark: SparkSession,
    session_catalog: Catalog,
) -> None:
    identifier = "default.some_view"
    spark.sql(
        f"""
        CREATE VIEW {identifier}
        AS
        (SELECT 1 as some_col)
    """
    ).collect()
    assert session_catalog.view_exists(identifier)
    session_catalog.drop_view(identifier)  # clean up


@pytest.mark.integration
def test_overwrite_all_data_with_filter(session_catalog: Catalog) -> None:
    schema = Schema(
        NestedField(1, "id", StringType(), required=True),
        NestedField(2, "name", StringType(), required=False),
        identifier_field_ids=[1],
    )

    data = pa.Table.from_pylist(
        [
            {"id": "1", "name": "Amsterdam"},
            {"id": "2", "name": "San Francisco"},
            {"id": "3", "name": "Drachten"},
        ],
        schema=schema.as_arrow(),
    )

    identifier = "default.test_overwrite_all_data_with_filter"
    tbl = _create_table(session_catalog, identifier, data=[data], schema=schema)
    tbl.overwrite(data, In("id", ["1", "2", "3"]))

    assert len(tbl.scan().to_arrow()) == 3


@pytest.mark.integration
def test_delete_threshold(session_catalog: Catalog) -> None:
    schema = Schema(
        NestedField(field_id=101, name="id", field_type=LongType(), required=True),
        NestedField(field_id=103, name="created_at", field_type=DateType(), required=False),
        NestedField(field_id=104, name="relevancy_score", field_type=DoubleType(), required=False),
    )

    partition_spec = PartitionSpec(PartitionField(source_id=103, field_id=2000, transform=DayTransform(), name="created_at_day"))

    try:
        session_catalog.drop_table(
            identifier="default.scores",
        )
    except NoSuchTableError:
        pass

    session_catalog.create_table(
        identifier="default.scores",
        schema=schema,
        partition_spec=partition_spec,
    )

    # Parameters
    num_rows = 100  # Number of rows in the dataframe
    id_min, id_max = 1, 10000
    date_start, date_end = date(2024, 1, 1), date(2024, 2, 1)

    # Generate the 'id' column
    id_column = [random.randint(id_min, id_max) for _ in range(num_rows)]

    # Generate the 'created_at' column as dates only
    date_range = pd.date_range(start=date_start, end=date_end, freq="D").to_list()  # Daily frequency for dates
    created_at_column = [random.choice(date_range) for _ in range(num_rows)]  # Convert to string (YYYY-MM-DD format)

    # Generate the 'relevancy_score' column with a peak around 0.1
    relevancy_score_column = [random.betavariate(2, 20) for _ in range(num_rows)]  # Adjusting parameters to peak around 0.1

    # Create the dataframe
    df = pd.DataFrame({"id": id_column, "created_at": created_at_column, "relevancy_score": relevancy_score_column})

    iceberg_table = session_catalog.load_table("default.scores")

    # Convert the pandas DataFrame to a PyArrow Table with the Iceberg schema
    arrow_schema = iceberg_table.schema().as_arrow()
    docs_table = pa.Table.from_pandas(df, schema=arrow_schema)

    # Append the data to the Iceberg table
    iceberg_table.append(docs_table)

    delete_condition = GreaterThanOrEqual("relevancy_score", 0.1)
    lower_before = len(iceberg_table.scan(row_filter=Not(delete_condition)).to_arrow())
    assert len(iceberg_table.scan(row_filter=Not(delete_condition)).to_arrow()) == lower_before
    iceberg_table.delete(delete_condition)
    assert len(iceberg_table.scan().to_arrow()) == lower_before


@pytest.mark.integration
def test_rewrite_manifest_after_partition_evolution(session_catalog: Catalog) -> None:
    random.seed(876)
    N = 1440
    d = {
        "timestamp": pa.array([datetime(2023, 1, 1, 0, 0, 0) + timedelta(minutes=i) for i in range(N)]),
        "category": pa.array([random.choice(["A", "B", "C"]) for _ in range(N)]),
        "value": pa.array([random.gauss(0, 1) for _ in range(N)]),
    }
    data = pa.Table.from_pydict(d)

    try:
        session_catalog.drop_table(
            identifier="default.test_error_table",
        )
    except NoSuchTableError:
        pass

    table = session_catalog.create_table(
        "default.test_error_table",
        schema=data.schema,
    )

    with table.update_spec() as update:
        update.add_field("timestamp", transform=HourTransform())

    table.append(data)

    with table.update_spec() as update:
        update.add_field("category", transform=IdentityTransform())

    data_ = data.filter(
        (pc.field("category") == "A")
        & (pc.field("timestamp") >= datetime(2023, 1, 1, 0))
        & (pc.field("timestamp") < datetime(2023, 1, 1, 1))
    )

    table.overwrite(
        df=data_,
        overwrite_filter=And(
            And(
                GreaterThanOrEqual("timestamp", datetime(2023, 1, 1, 0).isoformat()),
                LessThan("timestamp", datetime(2023, 1, 1, 1).isoformat()),
            ),
            EqualTo("category", "A"),
        ),
    )


@pytest.mark.integration
def test_writing_null_structs(session_catalog: Catalog) -> None:
    import pyarrow as pa

    schema = pa.schema(
        [
            pa.field(
                "struct_field_1",
                pa.struct(
                    [
                        pa.field("string_nested_1", pa.string()),
                        pa.field("int_item_2", pa.int32()),
                        pa.field("float_item_2", pa.float32()),
                    ]
                ),
            ),
        ]
    )

    records = [
        {
            "struct_field_1": {
                "string_nested_1": "nest_1",
                "int_item_2": 1234,
                "float_item_2": 1.234,
            },
        },
        {},
    ]

    try:
        session_catalog.drop_table(
            identifier="default.test_writing_null_structs",
        )
    except NoSuchTableError:
        pass

    table = session_catalog.create_table("default.test_writing_null_structs", schema)

    pyarrow_table: pa.Table = pa.Table.from_pylist(records, schema=schema)
    table.append(pyarrow_table)

    assert pyarrow_table.to_pandas()["struct_field_1"].tolist() == table.scan().to_pandas()["struct_field_1"].tolist()


@pytest.mark.integration
@pytest.mark.parametrize("format_version", [1, 2])
def test_abort_table_transaction_on_exception(
    spark: SparkSession, session_catalog: Catalog, arrow_table_with_null: pa.Table, format_version: int
) -> None:
    identifier = "default.table_test_abort_table_transaction_on_exception"
    tbl = _create_table(session_catalog, identifier, properties={"format-version": format_version})

    # Pre-populate some data
    tbl.append(arrow_table_with_null)
    table_size = len(arrow_table_with_null)
    assert len(tbl.scan().to_pandas()) == table_size

    # try to commit a transaction that raises exception at the middle
    with pytest.raises(ValueError):
        with tbl.transaction() as txn:
            txn.append(arrow_table_with_null)
            raise ValueError
            txn.append(arrow_table_with_null)  # type: ignore

    # Validate the transaction is aborted and no partial update is applied
    assert len(tbl.scan().to_pandas()) == table_size  # type: ignore


@pytest.mark.integration
def test_write_optional_list(session_catalog: Catalog) -> None:
    identifier = "default.test_write_optional_list"
    schema = Schema(
        NestedField(field_id=1, name="name", field_type=StringType(), required=False),
        NestedField(
            field_id=3,
            name="my_list",
            field_type=ListType(element_id=45, element=StringType(), element_required=False),
            required=False,
        ),
    )
    session_catalog.create_table_if_not_exists(identifier, schema)

    df_1 = pa.Table.from_pylist(
        [
            {"name": "one", "my_list": ["test"]},
            {"name": "another", "my_list": ["test"]},
        ]
    )
    session_catalog.load_table(identifier).append(df_1)

    assert len(session_catalog.load_table(identifier).scan().to_arrow()) == 2

    df_2 = pa.Table.from_pylist(
        [
            {"name": "one"},
            {"name": "another"},
        ]
    )
    session_catalog.load_table(identifier).append(df_2)

    assert len(session_catalog.load_table(identifier).scan().to_arrow()) == 4


@pytest.mark.integration
@pytest.mark.parametrize("format_version", [1, 2])
def test_double_commit_transaction(
    spark: SparkSession, session_catalog: Catalog, arrow_table_with_null: pa.Table, format_version: int
) -> None:
    identifier = "default.arrow_data_files"
    tbl = _create_table(session_catalog, identifier, {"format-version": format_version}, [])

    assert len(tbl.metadata.metadata_log) == 0

    with tbl.transaction() as tx:
        tx.append(arrow_table_with_null)
        tx.commit_transaction()

    assert len(tbl.metadata.metadata_log) == 1


@pytest.mark.integration
@pytest.mark.parametrize("format_version", [1, 2])
def test_evolve_and_write(
    spark: SparkSession, session_catalog: Catalog, arrow_table_with_null: pa.Table, format_version: int
) -> None:
    identifier = "default.test_evolve_and_write"
    tbl = _create_table(session_catalog, identifier, properties={"format-version": format_version}, schema=Schema())
    other_table = session_catalog.load_table(identifier)

    numbers = pa.array([1, 2, 3, 4], type=pa.int32())

    with tbl.update_schema() as upd:
        # This is not known by other_table
        upd.add_column("id", IntegerType())

    with other_table.transaction() as tx:
        # Refreshes the underlying metadata, and the schema
        other_table.refresh()
        tx.append(
            pa.Table.from_arrays(
                [
                    numbers,
                ],
                schema=pa.schema(
                    [
                        pa.field("id", pa.int32(), nullable=True),
                    ]
                ),
            )
        )

    assert session_catalog.load_table(identifier).scan().to_arrow().column(0).combine_chunks() == numbers


@pytest.mark.integration
def test_read_write_decimals(session_catalog: Catalog) -> None:
    """Roundtrip decimal types to make sure that we correctly write them as ints"""
    identifier = "default.test_read_write_decimals"

    arrow_table = pa.Table.from_pydict(
        {
            "decimal8": pa.array([Decimal("123.45"), Decimal("678.91")], pa.decimal128(8, 2)),
            "decimal16": pa.array([Decimal("12345679.123456"), Decimal("67891234.678912")], pa.decimal128(16, 6)),
            "decimal19": pa.array([Decimal("1234567890123.123456"), Decimal("9876543210703.654321")], pa.decimal128(19, 6)),
        },
    )

    tbl = _create_table(
        session_catalog,
        identifier,
        properties={"format-version": 2},
        schema=Schema(
            NestedField(1, "decimal8", DecimalType(8, 2)),
            NestedField(2, "decimal16", DecimalType(16, 6)),
            NestedField(3, "decimal19", DecimalType(19, 6)),
        ),
    )

    tbl.append(arrow_table)

    assert tbl.scan().to_arrow() == arrow_table


@pytest.mark.integration
@pytest.mark.parametrize(
    "transform",
    [
        IdentityTransform(),
        # Bucket is disabled because of an issue in Iceberg Java:
        # https://github.com/apache/iceberg/pull/13324
        # BucketTransform(32)
    ],
)
def test_uuid_partitioning(session_catalog: Catalog, spark: SparkSession, transform: Transform) -> None:  # type: ignore
    identifier = f"default.test_uuid_partitioning_{str(transform).replace('[32]', '')}"

    schema = Schema(NestedField(field_id=1, name="uuid", field_type=UUIDType(), required=True))

    try:
        session_catalog.drop_table(identifier=identifier)
    except NoSuchTableError:
        pass

    partition_spec = PartitionSpec(PartitionField(source_id=1, field_id=1000, transform=transform, name="uuid_identity"))

    import pyarrow as pa

    arr_table = pa.Table.from_pydict(
        {
            "uuid": [
                uuid.UUID("00000000-0000-0000-0000-000000000000").bytes,
                uuid.UUID("11111111-1111-1111-1111-111111111111").bytes,
            ],
        },
        schema=pa.schema(
            [
                # Uuid not yet supported, so we have to stick with `binary(16)`
                # https://github.com/apache/arrow/issues/46468
                pa.field("uuid", pa.binary(16), nullable=False),
            ]
        ),
    )

    tbl = session_catalog.create_table(
        identifier=identifier,
        schema=schema,
        partition_spec=partition_spec,
    )

    tbl.append(arr_table)

    lhs = [r[0] for r in spark.table(identifier).collect()]
    rhs = [str(u.as_py()) for u in tbl.scan().to_arrow()["uuid"].combine_chunks()]
    assert lhs == rhs


@pytest.mark.integration
def test_avro_compression_codecs(session_catalog: Catalog, arrow_table_with_null: pa.Table) -> None:
    identifier = "default.test_avro_compression_codecs"
    tbl = _create_table(session_catalog, identifier, schema=arrow_table_with_null.schema, data=[arrow_table_with_null])

    current_snapshot = tbl.current_snapshot()
    assert current_snapshot is not None

    with tbl.io.new_input(current_snapshot.manifest_list).open() as f:
        reader = fastavro.reader(f)
        assert reader.codec == "deflate"

    with tbl.transaction() as tx:
        tx.set_properties(**{TableProperties.WRITE_AVRO_COMPRESSION: "null"})  #  type: ignore

    tbl.append(arrow_table_with_null)

    current_snapshot = tbl.current_snapshot()
    assert current_snapshot is not None

    with tbl.io.new_input(current_snapshot.manifest_list).open() as f:
        reader = fastavro.reader(f)
        assert reader.codec == "null"


@pytest.mark.integration
def test_append_to_non_existing_branch(session_catalog: Catalog, arrow_table_with_null: pa.Table) -> None:
    identifier = "default.test_non_existing_branch"
    tbl = _create_table(session_catalog, identifier, {"format-version": "2"}, [])
    with pytest.raises(
        CommitFailedException, match=f"Table has no snapshots and can only be written to the {MAIN_BRANCH} BRANCH."
    ):
        tbl.append(arrow_table_with_null, branch="non_existing_branch")


@pytest.mark.integration
def test_append_to_existing_branch(session_catalog: Catalog, arrow_table_with_null: pa.Table) -> None:
    identifier = "default.test_existing_branch_append"
    branch = "existing_branch"
    tbl = _create_table(session_catalog, identifier, {"format-version": "2"}, [arrow_table_with_null])

    assert tbl.metadata.current_snapshot_id is not None

    tbl.manage_snapshots().create_branch(snapshot_id=tbl.metadata.current_snapshot_id, branch_name=branch).commit()
    tbl.append(arrow_table_with_null, branch=branch)

    assert len(tbl.scan().use_ref(branch).to_arrow()) == 6
    assert len(tbl.scan().to_arrow()) == 3
    branch_snapshot = tbl.metadata.snapshot_by_name(branch)
    assert branch_snapshot is not None
    main_snapshot = tbl.metadata.snapshot_by_name("main")
    assert main_snapshot is not None
    assert branch_snapshot.parent_snapshot_id == main_snapshot.snapshot_id


@pytest.mark.integration
def test_delete_to_existing_branch(session_catalog: Catalog, arrow_table_with_null: pa.Table) -> None:
    identifier = "default.test_existing_branch_delete"
    branch = "existing_branch"
    tbl = _create_table(session_catalog, identifier, {"format-version": "2"}, [arrow_table_with_null])

    assert tbl.metadata.current_snapshot_id is not None

    tbl.manage_snapshots().create_branch(snapshot_id=tbl.metadata.current_snapshot_id, branch_name=branch).commit()
    tbl.delete(delete_filter="int = 9", branch=branch)

    assert len(tbl.scan().use_ref(branch).to_arrow()) == 2
    assert len(tbl.scan().to_arrow()) == 3
    branch_snapshot = tbl.metadata.snapshot_by_name(branch)
    assert branch_snapshot is not None
    main_snapshot = tbl.metadata.snapshot_by_name("main")
    assert main_snapshot is not None
    assert branch_snapshot.parent_snapshot_id == main_snapshot.snapshot_id


@pytest.mark.integration
def test_overwrite_to_existing_branch(session_catalog: Catalog, arrow_table_with_null: pa.Table) -> None:
    identifier = "default.test_existing_branch_overwrite"
    branch = "existing_branch"
    tbl = _create_table(session_catalog, identifier, {"format-version": "2"}, [arrow_table_with_null])

    assert tbl.metadata.current_snapshot_id is not None

    tbl.manage_snapshots().create_branch(snapshot_id=tbl.metadata.current_snapshot_id, branch_name=branch).commit()
    tbl.overwrite(arrow_table_with_null, branch=branch)

    assert len(tbl.scan().use_ref(branch).to_arrow()) == 3
    assert len(tbl.scan().to_arrow()) == 3
    branch_snapshot = tbl.metadata.snapshot_by_name(branch)
    assert branch_snapshot is not None and branch_snapshot.parent_snapshot_id is not None
    delete_snapshot = tbl.metadata.snapshot_by_id(branch_snapshot.parent_snapshot_id)
    assert delete_snapshot is not None
    main_snapshot = tbl.metadata.snapshot_by_name("main")
    assert main_snapshot is not None
    assert (
        delete_snapshot.parent_snapshot_id == main_snapshot.snapshot_id
    )  # Currently overwrite is a delete followed by an append operation


@pytest.mark.integration
def test_intertwined_branch_writes(session_catalog: Catalog, arrow_table_with_null: pa.Table) -> None:
    identifier = "default.test_intertwined_branch_operations"
    branch1 = "existing_branch_1"
    branch2 = "existing_branch_2"

    tbl = _create_table(session_catalog, identifier, {"format-version": "2"}, [arrow_table_with_null])

    assert tbl.metadata.current_snapshot_id is not None

    tbl.manage_snapshots().create_branch(snapshot_id=tbl.metadata.current_snapshot_id, branch_name=branch1).commit()

    tbl.delete("int = 9", branch=branch1)

    tbl.append(arrow_table_with_null)

    tbl.manage_snapshots().create_branch(snapshot_id=tbl.metadata.current_snapshot_id, branch_name=branch2).commit()

    tbl.overwrite(arrow_table_with_null, branch=branch2)

    assert len(tbl.scan().use_ref(branch1).to_arrow()) == 2
    assert len(tbl.scan().use_ref(branch2).to_arrow()) == 3
    assert len(tbl.scan().to_arrow()) == 6


@pytest.mark.integration
def test_branch_spark_write_py_read(session_catalog: Catalog, spark: SparkSession, arrow_table_with_null: pa.Table) -> None:
    # Initialize table with branch
    identifier = "default.test_branch_spark_write_py_read"
    tbl = _create_table(session_catalog, identifier, {"format-version": "2"}, [arrow_table_with_null])
    branch = "existing_spark_branch"

    # Create branch in Spark
    spark.sql(f"ALTER TABLE {identifier} CREATE BRANCH {branch}")

    # Spark Write
    spark.sql(
        f"""
            DELETE FROM {identifier}.branch_{branch}
            WHERE int = 9
        """
    )

    # Refresh table to get new refs
    tbl.refresh()

    # Python Read
    assert len(tbl.scan().to_arrow()) == 3
    assert len(tbl.scan().use_ref(branch).to_arrow()) == 2


@pytest.mark.integration
def test_branch_py_write_spark_read(session_catalog: Catalog, spark: SparkSession, arrow_table_with_null: pa.Table) -> None:
    # Initialize table with branch
    identifier = "default.test_branch_py_write_spark_read"
    tbl = _create_table(session_catalog, identifier, {"format-version": "2"}, [arrow_table_with_null])
    branch = "existing_py_branch"

    assert tbl.metadata.current_snapshot_id is not None

    # Create branch
    tbl.manage_snapshots().create_branch(snapshot_id=tbl.metadata.current_snapshot_id, branch_name=branch).commit()

    # Python Write
    tbl.delete("int = 9", branch=branch)

    # Spark Read
    main_df = spark.sql(
        f"""
            SELECT *
            FROM {identifier}
        """
    )
    branch_df = spark.sql(
        f"""
            SELECT *
            FROM {identifier}.branch_{branch}
        """
    )
    assert main_df.count() == 3
    assert branch_df.count() == 2


@pytest.mark.integration
def test_nanosecond_support_on_catalog(
    session_catalog: Catalog, arrow_table_schema_with_all_timestamp_precisions: pa.Schema
) -> None:
    identifier = "default.test_nanosecond_support_on_catalog"

    catalog = load_catalog("default", type="in-memory")
    catalog.create_namespace("ns")

    _create_table(session_catalog, identifier, {"format-version": "3"}, schema=arrow_table_schema_with_all_timestamp_precisions)

    with pytest.raises(NotImplementedError, match="Writing V3 is not yet supported"):
        catalog.create_table(
            "ns.table1", schema=arrow_table_schema_with_all_timestamp_precisions, properties={"format-version": "3"}
        )

    with pytest.raises(
        UnsupportedPyArrowTypeException, match=re.escape("Column 'timestamp_ns' has an unsupported type: timestamp[ns]")
    ):
        _create_table(
            session_catalog, identifier, {"format-version": "2"}, schema=arrow_table_schema_with_all_timestamp_precisions
        )


@pytest.mark.integration
@pytest.mark.parametrize("format_version", [1, 2])
def test_stage_only_delete(
    spark: SparkSession, session_catalog: Catalog, arrow_table_with_null: pa.Table, format_version: int
) -> None:
    identifier = f"default.test_stage_only_delete_files_v{format_version}"
    iceberg_spec = PartitionSpec(
        *[PartitionField(source_id=4, field_id=1001, transform=IdentityTransform(), name="integer_partition")]
    )
    tbl = _create_table(
        session_catalog, identifier, {"format-version": str(format_version)}, [arrow_table_with_null], iceberg_spec
    )

    current_snapshot = tbl.metadata.current_snapshot_id
    assert current_snapshot is not None

    original_count = len(tbl.scan().to_arrow())
    assert original_count == 3

    tbl.delete("int = 9", branch=None)

    # a new delete snapshot is added
    snapshots = tbl.snapshots()
    assert len(snapshots) == 2
    # snapshot main ref has not changed
    assert current_snapshot == tbl.metadata.current_snapshot_id
    assert len(tbl.scan().to_arrow()) == original_count

    # Write to main branch
    tbl.append(arrow_table_with_null)

    # Main ref has changed
    assert current_snapshot != tbl.metadata.current_snapshot_id
    assert len(tbl.scan().to_arrow()) == 6
    snapshots = tbl.snapshots()
    assert len(snapshots) == 3

    rows = spark.sql(
        f"""
                SELECT operation, parent_id
                FROM {identifier}.snapshots
                ORDER BY committed_at ASC
            """
    ).collect()
    operations = [row.operation for row in rows]
    parent_snapshot_id = [row.parent_id for row in rows]
    assert operations == ["append", "delete", "append"]
    # both subsequent parent id should be the first snapshot id
    assert parent_snapshot_id == [None, current_snapshot, current_snapshot]


@pytest.mark.integration
@pytest.mark.parametrize("format_version", [1, 2])
def test_stage_only_append(
    spark: SparkSession, session_catalog: Catalog, arrow_table_with_null: pa.Table, format_version: int
) -> None:
    identifier = f"default.test_stage_only_fast_append_files_v{format_version}"
    tbl = _create_table(session_catalog, identifier, {"format-version": str(format_version)}, [arrow_table_with_null])

    current_snapshot = tbl.metadata.current_snapshot_id
    assert current_snapshot is not None

    original_count = len(tbl.scan().to_arrow())
    assert original_count == 3

    # Write to staging branch
    tbl.append(arrow_table_with_null, branch=None)

    # Main ref has not changed and data is not yet appended
    assert current_snapshot == tbl.metadata.current_snapshot_id
    assert len(tbl.scan().to_arrow()) == original_count
    # There should be a new staged snapshot
    snapshots = tbl.snapshots()
    assert len(snapshots) == 2

    # Write to main branch
    tbl.append(arrow_table_with_null)

    # Main ref has changed
    assert current_snapshot != tbl.metadata.current_snapshot_id
    assert len(tbl.scan().to_arrow()) == 6
    snapshots = tbl.snapshots()
    assert len(snapshots) == 3

    rows = spark.sql(
        f"""
            SELECT operation, parent_id
            FROM {identifier}.snapshots
            ORDER BY committed_at ASC
        """
    ).collect()
    operations = [row.operation for row in rows]
    parent_snapshot_id = [row.parent_id for row in rows]
    assert operations == ["append", "append", "append"]
    # both subsequent parent id should be the first snapshot id
    assert parent_snapshot_id == [None, current_snapshot, current_snapshot]


@pytest.mark.integration
@pytest.mark.parametrize("format_version", [1, 2])
def test_stage_only_overwrite_files(
    spark: SparkSession, session_catalog: Catalog, arrow_table_with_null: pa.Table, format_version: int
) -> None:
    identifier = f"default.test_stage_only_overwrite_files_v{format_version}"
    tbl = _create_table(session_catalog, identifier, {"format-version": str(format_version)}, [arrow_table_with_null])
    first_snapshot = tbl.metadata.current_snapshot_id

    # duplicate data with a new insert
    tbl.append(arrow_table_with_null)

    second_snapshot = tbl.metadata.current_snapshot_id
    assert second_snapshot is not None
    original_count = len(tbl.scan().to_arrow())
    assert original_count == 6

    # write to non-main branch
    tbl.overwrite(arrow_table_with_null, branch=None)
    assert second_snapshot == tbl.metadata.current_snapshot_id
    assert len(tbl.scan().to_arrow()) == original_count
    snapshots = tbl.snapshots()
    # overwrite will create 2 snapshots
    assert len(snapshots) == 4

    # Write to main branch again
    tbl.append(arrow_table_with_null)

    # Main ref has changed
    assert second_snapshot != tbl.metadata.current_snapshot_id
    assert len(tbl.scan().to_arrow()) == 9
    snapshots = tbl.snapshots()
    assert len(snapshots) == 5

    rows = spark.sql(
        f"""
                    SELECT operation, parent_id, snapshot_id
                    FROM {identifier}.snapshots
                    ORDER BY committed_at ASC
                """
    ).collect()
    operations = [row.operation for row in rows]
    parent_snapshot_id = [row.parent_id for row in rows]
    assert operations == ["append", "append", "delete", "append", "append"]

    assert parent_snapshot_id == [None, first_snapshot, second_snapshot, second_snapshot, second_snapshot]


@pytest.mark.skip("V3 writer support is not enabled.")
@pytest.mark.integration
def test_v3_write_and_read_row_lineage(spark: SparkSession, session_catalog: Catalog) -> None:
    """Test writing to a v3 table and reading with Spark."""
    identifier = "default.test_v3_write_and_read"
    tbl = _create_table(session_catalog, identifier, {"format-version": "3"})
    assert tbl.format_version == 3, f"Expected v3, got: v{tbl.format_version}"
    initial_next_row_id = tbl.metadata.next_row_id or 0

    test_data = pa.Table.from_pydict(
        {
            "bool": [True, False, True],
            "string": ["a", "b", "c"],
            "string_long": ["a_long", "b_long", "c_long"],
            "int": [1, 2, 3],
            "long": [11, 22, 33],
            "float": [1.1, 2.2, 3.3],
            "double": [1.11, 2.22, 3.33],
            "timestamp": [datetime(2023, 1, 1, 1, 1, 1), datetime(2023, 2, 2, 2, 2, 2), datetime(2023, 3, 3, 3, 3, 3)],
            "timestamptz": [
                datetime(2023, 1, 1, 1, 1, 1, tzinfo=pytz.utc),
                datetime(2023, 2, 2, 2, 2, 2, tzinfo=pytz.utc),
                datetime(2023, 3, 3, 3, 3, 3, tzinfo=pytz.utc),
            ],
            "date": [date(2023, 1, 1), date(2023, 2, 2), date(2023, 3, 3)],
            "binary": [b"\x01", b"\x02", b"\x03"],
            "fixed": [b"1234567890123456", b"1234567890123456", b"1234567890123456"],
        },
        schema=TABLE_SCHEMA.as_arrow(),
    )

    tbl.append(test_data)

    assert tbl.metadata.next_row_id == initial_next_row_id + len(test_data), (
        "Expected next_row_id to be incremented by the number of added rows"
    )
