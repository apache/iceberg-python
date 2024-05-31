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
import time
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict
from urllib.parse import urlparse

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import pytz
from pyarrow.fs import S3FileSystem
from pydantic_core import ValidationError
from pyspark.sql import SparkSession
from pytest_mock.plugin import MockerFixture

from pyiceberg.catalog import Catalog
from pyiceberg.catalog.hive import HiveCatalog
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table import TableProperties, _dataframe_to_data_files
from pyiceberg.transforms import IdentityTransform
from pyiceberg.types import IntegerType, NestedField
from tests.conftest import TEST_DATA_WITH_NULL
from utils import _create_table


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
@pytest.mark.parametrize("col", TEST_DATA_WITH_NULL.keys())
@pytest.mark.parametrize("format_version", [1, 2])
def test_query_filter_null(spark: SparkSession, col: str, format_version: int) -> None:
    identifier = f"default.arrow_table_v{format_version}_with_null"
    df = spark.table(identifier)
    assert df.where(f"{col} is null").count() == 1, f"Expected 1 row for {col}"
    assert df.where(f"{col} is not null").count() == 2, f"Expected 2 rows for {col}"


@pytest.mark.integration
@pytest.mark.parametrize("col", TEST_DATA_WITH_NULL.keys())
@pytest.mark.parametrize("format_version", [1, 2])
def test_query_filter_without_data(spark: SparkSession, col: str, format_version: int) -> None:
    identifier = f"default.arrow_table_v{format_version}_without_data"
    df = spark.table(identifier)
    assert df.where(f"{col} is null").count() == 0, f"Expected 0 row for {col}"
    assert df.where(f"{col} is not null").count() == 0, f"Expected 0 row for {col}"


@pytest.mark.integration
@pytest.mark.parametrize("col", TEST_DATA_WITH_NULL.keys())
@pytest.mark.parametrize("format_version", [1, 2])
def test_query_filter_only_nulls(spark: SparkSession, col: str, format_version: int) -> None:
    identifier = f"default.arrow_table_v{format_version}_with_only_nulls"
    df = spark.table(identifier)
    assert df.where(f"{col} is null").count() == 2, f"Expected 2 rows for {col}"
    assert df.where(f"{col} is not null").count() == 0, f"Expected 0 row for {col}"


@pytest.mark.integration
@pytest.mark.parametrize("col", TEST_DATA_WITH_NULL.keys())
@pytest.mark.parametrize("format_version", [1, 2])
def test_query_filter_appended_null(spark: SparkSession, col: str, format_version: int) -> None:
    identifier = f"default.arrow_table_v{format_version}_appended_with_null"
    df = spark.table(identifier)
    assert df.where(f"{col} is null").count() == 2, f"Expected 1 row for {col}"
    assert df.where(f"{col} is not null").count() == 4, f"Expected 2 rows for {col}"


@pytest.mark.integration
@pytest.mark.parametrize("col", TEST_DATA_WITH_NULL.keys())
def test_query_filter_v1_v2_append_null(spark: SparkSession, col: str) -> None:
    identifier = "default.arrow_table_v1_v2_appended_with_null"
    df = spark.table(identifier)
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
    assert operations == ["append", "append", "overwrite"]

    summaries = [row.summary for row in rows]

    assert summaries[0] == {
        "added-data-files": "1",
        "added-files-size": "5459",
        "added-records": "3",
        "total-data-files": "1",
        "total-delete-files": "0",
        "total-equality-deletes": "0",
        "total-files-size": "5459",
        "total-position-deletes": "0",
        "total-records": "3",
    }

    assert summaries[1] == {
        "added-data-files": "1",
        "added-files-size": "5459",
        "added-records": "3",
        "total-data-files": "2",
        "total-delete-files": "0",
        "total-equality-deletes": "0",
        "total-files-size": "10918",
        "total-position-deletes": "0",
        "total-records": "6",
    }

    assert summaries[2] == {
        "added-data-files": "1",
        "added-files-size": "5459",
        "added-records": "3",
        "deleted-data-files": "2",
        "deleted-records": "6",
        "removed-files-size": "10918",
        "total-data-files": "1",
        "total-delete-files": "0",
        "total-equality-deletes": "0",
        "total-files-size": "5459",
        "total-position-deletes": "0",
        "total-records": "3",
    }


@pytest.mark.integration
def test_data_files(spark: SparkSession, session_catalog: Catalog, arrow_table_with_null: pa.Table) -> None:
    identifier = "default.arrow_data_files"
    tbl = _create_table(session_catalog, identifier, {"format-version": "1"}, [])

    tbl.overwrite(arrow_table_with_null)
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

    assert [row.added_data_files_count for row in rows] == [1, 1, 0, 1, 1]
    assert [row.existing_data_files_count for row in rows] == [0, 0, 0, 0, 0]
    assert [row.deleted_data_files_count for row in rows] == [0, 0, 1, 0, 0]


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

    tbl.overwrite(arrow_table_with_null)
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
    pa_schema = pa.schema([
        pa.field(column_name_with_special_character, pa.string()),
        pa.field("id", pa.int32()),
        pa.field("name", pa.string()),
        pa.field(
            "address",
            pa.struct([
                pa.field("street", pa.string()),
                pa.field("city", pa.string()),
                pa.field("zip", pa.int32()),
                pa.field(column_name_with_special_character, pa.string()),
            ]),
        ),
    ])
    arrow_table_with_special_character_column = pa.Table.from_pydict(TEST_DATA_WITH_SPECIAL_CHARACTER_COLUMN, schema=pa_schema)
    tbl = _create_table(session_catalog, identifier, {"format-version": format_version}, schema=pa_schema)

    tbl.overwrite(arrow_table_with_special_character_column)
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
    pa_schema = pa.schema([
        pa.field("id", pa.dictionary(pa.int32(), pa.int32(), False)),
        pa.field("name", pa.dictionary(pa.int32(), pa.string(), False)),
    ])
    arrow_table = pa.Table.from_pydict(TEST_DATA, schema=pa_schema)

    tbl = _create_table(session_catalog, identifier, {"format-version": format_version}, schema=pa_schema)

    tbl.overwrite(arrow_table)
    spark_df = spark.sql(f"SELECT * FROM {identifier}").toPandas()
    pyiceberg_df = tbl.scan().to_pandas()
    assert spark_df.equals(pyiceberg_df)


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
    tbl.overwrite(arrow_table_with_null)
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
    properties: Dict[str, Any],
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
    properties: Dict[str, Any],
    expected_kwargs: Dict[str, Any],
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
        {"write.parquet.page-row-limit": "42"},
        {"write.parquet.bloom-filter-enabled.column.bool": "42"},
        {"write.parquet.bloom-filter-max-bytes": "42"},
    ],
)
def test_write_parquet_unsupported_properties(
    spark: SparkSession,
    session_catalog: Catalog,
    arrow_table_with_null: pa.Table,
    properties: Dict[str, str],
) -> None:
    identifier = "default.write_parquet_unsupported_properties"

    tbl = _create_table(session_catalog, identifier, properties, [])
    with pytest.raises(NotImplementedError):
        tbl.append(arrow_table_with_null)


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
    assert operations == ["append", "append", "overwrite"]

    summaries = [row.summary for row in rows]

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
        "added-files-size": "4239",
        "added-records": "2",
        "total-data-files": "1",
        "total-delete-files": "0",
        "total-equality-deletes": "0",
        "total-files-size": "4239",
        "total-position-deletes": "0",
        "total-records": "2",
    }

    assert summaries[2] == {
        "removed-files-size": "4239",
        "total-equality-deletes": "0",
        "total-position-deletes": "0",
        "deleted-data-files": "1",
        "total-delete-files": "0",
        "total-files-size": "0",
        "deleted-records": "2",
        "total-data-files": "0",
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
        schema=pa.schema([
            pa.field("foo", pa.string(), nullable=True),
            pa.field("bar", pa.int32(), nullable=True),
        ]),
    )

    with tbl.transaction() as txn:
        with txn.update_schema() as schema_txn:
            schema_txn.union_by_name(pa_table_with_column.schema)

        with txn.update_snapshot().fast_append() as snapshot_update:
            for data_file in _dataframe_to_data_files(table_metadata=txn.table_metadata, df=pa_table_with_column, io=tbl.io):
                snapshot_update.append_data_file(data_file)


@pytest.mark.integration
@pytest.mark.parametrize("format_version", [1, 2])
@pytest.mark.parametrize("catalog", [pytest.lazy_fixture("session_catalog_hive"), pytest.lazy_fixture("session_catalog")])
def test_create_table_transaction(catalog: Catalog, format_version: int) -> None:
    if format_version == 1 and isinstance(catalog, RestCatalog):
        pytest.skip(
            "There is a bug in the REST catalog (maybe server side) that prevents create and commit a staged version 1 table"
        )

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
        schema=pa.schema([
            pa.field("foo", pa.string(), nullable=True),
            pa.field("bar", pa.int32(), nullable=True),
        ]),
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


@pytest.mark.parametrize("format_version", [1, 2])
def table_write_subset_of_schema(session_catalog: Catalog, arrow_table_with_null: pa.Table, format_version: int) -> None:
    identifier = "default.table_append_subset_of_schema"
    tbl = _create_table(session_catalog, identifier, {"format-version": format_version}, [arrow_table_with_null])
    arrow_table_without_some_columns = arrow_table_with_null.combine_chunks().drop(arrow_table_with_null.column_names[0])
    assert len(arrow_table_without_some_columns.columns) < len(arrow_table_with_null.columns)
    tbl.overwrite(arrow_table_without_some_columns)
    tbl.append(arrow_table_without_some_columns)
    # overwrite and then append should produce twice the data
    assert len(tbl.scan().to_arrow()) == len(arrow_table_without_some_columns) * 2
