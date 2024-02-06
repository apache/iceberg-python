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
import uuid
from datetime import date, datetime
from typing import Any, Dict, List
from urllib.parse import urlparse

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pyarrow.fs import S3FileSystem
from pyspark.sql import SparkSession
from pytest_mock.plugin import MockerFixture

from pyiceberg.catalog import Catalog, Properties, Table, load_catalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError, NoSuchTableError
from pyiceberg.schema import Schema
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


@pytest.fixture()
def catalog() -> Catalog:
    catalog = load_catalog(
        "local",
        **{
            "type": "rest",
            "uri": "http://localhost:8181",
            "s3.endpoint": "http://localhost:9000",
            "s3.access-key-id": "admin",
            "s3.secret-access-key": "password",
        },
    )

    try:
        catalog.create_namespace("default")
    except NamespaceAlreadyExistsError:
        pass

    return catalog


TEST_DATA_WITH_NULL = {
    'bool': [False, None, True],
    'string': ['a', None, 'z'],
    # Go over the 16 bytes to kick in truncation
    'string_long': ['a' * 22, None, 'z' * 22],
    'int': [1, None, 9],
    'long': [1, None, 9],
    'float': [0.0, None, 0.9],
    'double': [0.0, None, 0.9],
    'timestamp': [datetime(2023, 1, 1, 19, 25, 00), None, datetime(2023, 3, 1, 19, 25, 00)],
    'timestamptz': [datetime(2023, 1, 1, 19, 25, 00), None, datetime(2023, 3, 1, 19, 25, 00)],
    'date': [date(2023, 1, 1), None, date(2023, 3, 1)],
    # Not supported by Spark
    # 'time': [time(1, 22, 0), None, time(19, 25, 0)],
    # Not natively supported by Arrow
    # 'uuid': [uuid.UUID('00000000-0000-0000-0000-000000000000').bytes, None, uuid.UUID('11111111-1111-1111-1111-111111111111').bytes],
    'binary': [b'\01', None, b'\22'],
    'fixed': [
        uuid.UUID('00000000-0000-0000-0000-000000000000').bytes,
        None,
        uuid.UUID('11111111-1111-1111-1111-111111111111').bytes,
    ],
}

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


@pytest.fixture(scope="session")
def session_catalog() -> Catalog:
    return load_catalog(
        "local",
        **{
            "type": "rest",
            "uri": "http://localhost:8181",
            "s3.endpoint": "http://localhost:9000",
            "s3.access-key-id": "admin",
            "s3.secret-access-key": "password",
        },
    )


@pytest.fixture(scope="session")
def pa_schema() -> pa.Schema:
    return pa.schema([
        ("bool", pa.bool_()),
        ("string", pa.string()),
        ("string_long", pa.string()),
        ("int", pa.int32()),
        ("long", pa.int64()),
        ("float", pa.float32()),
        ("double", pa.float64()),
        ("timestamp", pa.timestamp(unit="us")),
        ("timestamptz", pa.timestamp(unit="us", tz="UTC")),
        ("date", pa.date32()),
        # Not supported by Spark
        # ("time", pa.time64("us")),
        # Not natively supported by Arrow
        # ("uuid", pa.fixed(16)),
        ("binary", pa.binary()),
        ("fixed", pa.binary(16)),
    ])


@pytest.fixture(scope="session")
def arrow_table_with_null(pa_schema: pa.Schema) -> pa.Table:
    """PyArrow table with all kinds of columns"""
    return pa.Table.from_pydict(TEST_DATA_WITH_NULL, schema=pa_schema)


@pytest.fixture(scope="session")
def arrow_table_without_data(pa_schema: pa.Schema) -> pa.Table:
    """PyArrow table with all kinds of columns"""
    return pa.Table.from_pylist([], schema=pa_schema)


@pytest.fixture(scope="session")
def arrow_table_with_only_nulls(pa_schema: pa.Schema) -> pa.Table:
    """PyArrow table with all kinds of columns"""
    return pa.Table.from_pylist([{}, {}], schema=pa_schema)


def _create_table(session_catalog: Catalog, identifier: str, properties: Properties, data: List[pa.Table]) -> Table:
    try:
        session_catalog.drop_table(identifier=identifier)
    except NoSuchTableError:
        pass

    tbl = session_catalog.create_table(identifier=identifier, schema=TABLE_SCHEMA, properties=properties)
    for d in data:
        tbl.append(d)

    return tbl


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


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    import importlib.metadata
    import os

    spark_version = ".".join(importlib.metadata.version("pyspark").split(".")[:2])
    scala_version = "2.12"
    iceberg_version = "1.4.3"

    os.environ["PYSPARK_SUBMIT_ARGS"] = (
        f"--packages org.apache.iceberg:iceberg-spark-runtime-{spark_version}_{scala_version}:{iceberg_version},"
        f"org.apache.iceberg:iceberg-aws-bundle:{iceberg_version} pyspark-shell"
    )
    os.environ["AWS_REGION"] = "us-east-1"
    os.environ["AWS_ACCESS_KEY_ID"] = "admin"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "password"

    spark = (
        SparkSession.builder.appName("PyIceberg integration test")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.integration", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.integration.catalog-impl", "org.apache.iceberg.rest.RESTCatalog")
        .config("spark.sql.catalog.integration.uri", "http://localhost:8181")
        .config("spark.sql.catalog.integration.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.integration.warehouse", "s3://warehouse/wh/")
        .config("spark.sql.catalog.integration.s3.endpoint", "http://localhost:9000")
        .config("spark.sql.catalog.integration.s3.path-style-access", "true")
        .config("spark.sql.defaultCatalog", "integration")
        .getOrCreate()
    )

    return spark


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
    assert df.where(f"{col} is not null").count() == 0, f"Expected 0 rows for {col}"


@pytest.mark.integration
@pytest.mark.parametrize("col", TEST_DATA_WITH_NULL.keys())
@pytest.mark.parametrize("format_version", [1, 2])
def test_query_filter_only_nulls(spark: SparkSession, col: str, format_version: int) -> None:
    identifier = f"default.arrow_table_v{format_version}_with_only_nulls"
    df = spark.table(identifier)
    assert df.where(f"{col} is null").count() == 2, f"Expected 2 row for {col}"
    assert df.where(f"{col} is not null").count() == 0, f"Expected 0 rows for {col}"


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
    assert operations == ['append', 'append', 'overwrite']

    summaries = [row.summary for row in rows]

    assert summaries[0] == {
        'added-data-files': '1',
        'added-files-size': '5459',
        'added-records': '3',
        'total-data-files': '1',
        'total-delete-files': '0',
        'total-equality-deletes': '0',
        'total-files-size': '5459',
        'total-position-deletes': '0',
        'total-records': '3',
    }

    assert summaries[1] == {
        'added-data-files': '1',
        'added-files-size': '5459',
        'added-records': '3',
        'total-data-files': '2',
        'total-delete-files': '0',
        'total-equality-deletes': '0',
        'total-files-size': '10918',
        'total-position-deletes': '0',
        'total-records': '6',
    }

    assert summaries[2] == {
        'added-data-files': '1',
        'added-files-size': '5459',
        'added-records': '3',
        'deleted-data-files': '2',
        'deleted-records': '6',
        'removed-files-size': '10918',
        'total-data-files': '1',
        'total-delete-files': '0',
        'total-equality-deletes': '0',
        'total-files-size': '5459',
        'total-position-deletes': '0',
        'total-records': '3',
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
@pytest.mark.parametrize("format_version", ["1", "2"])
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
    format_version: str,
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
    print(type(mocker))
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
    tbl = _create_table(session_catalog, identifier, {'format-version': '1'}, [])

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
        session_catalog, identifier, {'format-version': '1'}, [arrow_table_without_data, arrow_table_with_only_nulls]
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
    assert operations == ['append', 'append', 'overwrite']

    summaries = [row.summary for row in rows]

    assert summaries[0] == {
        'total-data-files': '0',
        'total-delete-files': '0',
        'total-equality-deletes': '0',
        'total-files-size': '0',
        'total-position-deletes': '0',
        'total-records': '0',
    }

    assert summaries[1] == {
        'added-data-files': '1',
        'added-files-size': '4239',
        'added-records': '2',
        'total-data-files': '1',
        'total-delete-files': '0',
        'total-equality-deletes': '0',
        'total-files-size': '4239',
        'total-position-deletes': '0',
        'total-records': '2',
    }

    assert summaries[0] == {
        'total-data-files': '0',
        'total-delete-files': '0',
        'total-equality-deletes': '0',
        'total-files-size': '0',
        'total-position-deletes': '0',
        'total-records': '0',
    }
