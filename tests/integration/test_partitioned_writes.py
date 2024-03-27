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
from datetime import date, datetime, timezone
from typing import Union

import pyarrow as pa
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException

from pyiceberg.catalog import Catalog, load_catalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError, NoSuchTableError
from pyiceberg.expressions import BooleanExpression, EqualTo, IsNull, Reference
from pyiceberg.expressions.literals import (
    TimestampLiteral,
)
from pyiceberg.partitioning import PartitionField, PartitionSpec, arrow_to_iceberg_representation
from pyiceberg.schema import Schema
from pyiceberg.transforms import IdentityTransform
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
    'float': [0.0, None, 0.8],
    'double': [0.0, None, 0.8],
    'timestamp': [datetime(2023, 1, 1, 19, 25, 00), None, datetime(2023, 3, 1, 19, 25, 00)],
    'timestamptz': [
        datetime(2023, 1, 1, 19, 25, 00),
        None,
        datetime(2023, 3, 1, 19, 25, 00),
    ],
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
    NestedField(field_id=11, name="binary", field_type=BinaryType(), required=False),
    NestedField(field_id=12, name="fixed", field_type=FixedType(16), required=False),
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
def arrow_table_with_null() -> pa.Table:
    """PyArrow table with all kinds of columns"""
    pa_schema = pa.schema([
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
        ("binary", pa.large_binary()),
        ("fixed", pa.binary(16)),
    ])
    return pa.Table.from_pydict(TEST_DATA_WITH_NULL, schema=pa_schema)


@pytest.fixture(scope="session", autouse=True)
def table_v1_with_null_partitioned(session_catalog: Catalog, arrow_table_with_null: pa.Table) -> None:
    partition_cols = [
        'int',
        'bool',
        'string',
        "string_long",
        "long",
        "float",
        "double",
        "date",
        "timestamptz",
        "timestamp",
        "binary",
    ]
    for partition_col in partition_cols:
        identifier = f"default.arrow_table_v1_with_null_partitioned_on_col_{partition_col}"

        try:
            session_catalog.drop_table(identifier=identifier)
        except NoSuchTableError:
            pass
        nested_field = TABLE_SCHEMA.find_field(partition_col)
        source_id = nested_field.field_id

        tbl = session_catalog.create_table(
            identifier=identifier,
            schema=TABLE_SCHEMA,
            partition_spec=PartitionSpec(
                PartitionField(source_id=source_id, field_id=1001, transform=IdentityTransform(), name=partition_col)
            ),
            properties={'format-version': '1'},
        )
        tbl.append(arrow_table_with_null)

        assert tbl.format_version == 1, f"Expected v1, got: v{tbl.format_version}"


@pytest.fixture(scope="session", autouse=True)
def table_v1_appended_with_null_partitioned(session_catalog: Catalog, arrow_table_with_null: pa.Table) -> None:
    partition_cols = [
        'int',
        'bool',
        'string',
        "string_long",
        "long",
        "float",
        "double",
        "date",
        "timestamptz",
        "timestamp",
        "binary",
    ]
    for partition_col in partition_cols:
        identifier = f"default.arrow_table_v1_appended_with_null_partitioned_on_col_{partition_col}"

        try:
            session_catalog.drop_table(identifier=identifier)
        except NoSuchTableError:
            pass

        nested_field = TABLE_SCHEMA.find_field(partition_col)
        source_id = nested_field.field_id
        tbl = session_catalog.create_table(
            identifier=identifier,
            schema=TABLE_SCHEMA,
            partition_spec=PartitionSpec(
                PartitionField(source_id=source_id, field_id=1001, transform=IdentityTransform(), name=partition_col)
            ),  # name has to be the same for identity transform
            properties={'format-version': '1'},
        )

        for _ in range(2):
            tbl.append(arrow_table_with_null)
        assert tbl.format_version == 1, f"Expected v1, got: v{tbl.format_version}"


@pytest.fixture(scope="session", autouse=True)
def table_v2_with_null_partitioned(session_catalog: Catalog, arrow_table_with_null: pa.Table) -> None:
    partition_cols = [
        'int',
        'bool',
        'string',
        "string_long",
        "long",
        "float",
        "double",
        "date",
        "timestamptz",
        "timestamp",
        "binary",
    ]
    for partition_col in partition_cols:
        identifier = f"default.arrow_table_v2_with_null_partitioned_on_col_{partition_col}"

        try:
            session_catalog.drop_table(identifier=identifier)
        except NoSuchTableError:
            pass
        nested_field = TABLE_SCHEMA.find_field(partition_col)
        source_id = nested_field.field_id

        tbl = session_catalog.create_table(
            identifier=identifier,
            schema=TABLE_SCHEMA,
            partition_spec=PartitionSpec(
                PartitionField(source_id=source_id, field_id=1001, transform=IdentityTransform(), name=partition_col)
            ),
            properties={'format-version': '2'},
        )
        tbl.append(arrow_table_with_null)

        assert tbl.format_version == 2, f"Expected v2, got: v{tbl.format_version}"


@pytest.fixture(scope="session", autouse=True)
def table_v2_appended_with_null_partitioned(session_catalog: Catalog, arrow_table_with_null: pa.Table) -> None:
    partition_cols = [
        'int',
        'bool',
        'string',
        "string_long",
        "long",
        "float",
        "double",
        "date",
        "timestamptz",
        "timestamp",
        "binary",
    ]
    for partition_col in partition_cols:
        identifier = f"default.arrow_table_v2_appended_with_null_partitioned_on_col_{partition_col}"

        try:
            session_catalog.drop_table(identifier=identifier)
        except NoSuchTableError:
            pass

        nested_field = TABLE_SCHEMA.find_field(partition_col)
        source_id = nested_field.field_id
        tbl = session_catalog.create_table(
            identifier=identifier,
            schema=TABLE_SCHEMA,
            partition_spec=PartitionSpec(
                PartitionField(source_id=source_id, field_id=1001, transform=IdentityTransform(), name=partition_col)
            ),
            properties={'format-version': '2'},
        )

        for _ in range(2):
            tbl.append(arrow_table_with_null)
        assert tbl.format_version == 2, f"Expected v2, got: v{tbl.format_version}"


@pytest.fixture(scope="session", autouse=True)
def table_v1_v2_appended_with_null(session_catalog: Catalog, arrow_table_with_null: pa.Table) -> None:
    partition_cols = [
        'int',
        'bool',
        'string',
        "string_long",
        "long",
        "float",
        "double",
        "date",
        "timestamptz",
        "timestamp",
        "binary",
    ]
    for partition_col in partition_cols:
        identifier = f"default.arrow_table_v1_v2_appended_with_null_partitioned_on_col_{partition_col}"

        try:
            session_catalog.drop_table(identifier=identifier)
        except NoSuchTableError:
            pass

        nested_field = TABLE_SCHEMA.find_field(partition_col)
        source_id = nested_field.field_id
        tbl = session_catalog.create_table(
            identifier=identifier,
            schema=TABLE_SCHEMA,
            partition_spec=PartitionSpec(
                PartitionField(source_id=source_id, field_id=1001, transform=IdentityTransform(), name=partition_col)
            ),
            properties={'format-version': '1'},
        )
        tbl.append(arrow_table_with_null)

        assert tbl.format_version == 1, f"Expected v1, got: v{tbl.format_version}"

        with tbl.transaction() as tx:
            tx.upgrade_table_version(format_version=2)

        tbl.append(arrow_table_with_null)

        assert tbl.format_version == 2, f"Expected v2, got: v{tbl.format_version}"


@pytest.mark.integration
@pytest.mark.parametrize(
    "part_col", ['int', 'bool', 'string', "string_long", "long", "float", "double", "date", 'timestamp', 'timestamptz']
)
@pytest.mark.parametrize("format_version", [1, 2])
def test_query_filter_null_partitioned(spark: SparkSession, part_col: str, format_version: int) -> None:
    identifier = f"default.arrow_table_v{format_version}_with_null_partitioned_on_col_{part_col}"
    df = spark.table(identifier)
    for col in TEST_DATA_WITH_NULL.keys():
        assert df.where(f"{col} is not null").count() == 2, f"Expected 2 rows for {col}"


@pytest.mark.integration
@pytest.mark.parametrize(
    "part_col", ['int', 'bool', 'string', "string_long", "long", "float", "double", "date", "timestamptz", "timestamp", "binary"]
)
@pytest.mark.parametrize("format_version", [1, 2])
def test_query_filter_appended_null_partitioned(spark: SparkSession, part_col: str, format_version: int) -> None:
    identifier = f"default.arrow_table_v{format_version}_appended_with_null_partitioned_on_col_{part_col}"
    df = spark.table(identifier)
    for col in TEST_DATA_WITH_NULL.keys():
        df = spark.table(identifier)
        assert df.where(f"{col} is not null").count() == 4, f"Expected 6 rows for {col}"


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
@pytest.mark.parametrize(
    "part_col", ['int', 'bool', 'string', "string_long", "long", "float", "double", "date", "timestamptz", "timestamp", "binary"]
)
def test_query_filter_v1_v2_append_null(spark: SparkSession, part_col: str) -> None:
    identifier = f"default.arrow_table_v1_v2_appended_with_null_partitioned_on_col_{part_col}"
    df = spark.table(identifier)
    for col in TEST_DATA_WITH_NULL.keys():
        df = spark.table(identifier)
        assert df.where(f"{col} is not null").count() == 4, f"Expected 4 row for {col}"


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
    tbl.overwrite(arrow_table_with_null)
    tbl.append(arrow_table_with_null)
    tbl.overwrite(arrow_table_with_null, overwrite_filter="int=1")
    tbl.dynamic_overwrite(arrow_table_with_null)
    tbl.dynamic_overwrite(arrow_table_with_null.slice(0, 2))

    rows = spark.sql(
        f"""
        SELECT operation, summary
        FROM {identifier}.snapshots
        ORDER BY committed_at ASC
    """
    ).collect()

    operations = [row.operation for row in rows]
    assert operations == ['append', 'append', 'overwrite', 'append', 'overwrite', 'overwrite', 'overwrite']

    summaries = [row.summary for row in rows]

    # append 3 new data files with 3 records, giving a total of 3 files and 3 records
    assert summaries[0] == {
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
    # append another 3 new data files with 3 records, giving a total of 6 files and 6 records
    assert summaries[1] == {
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
    # overwrite entire table, adding 3 new data files with 3 records, deleting all 6 old data files and records
    assert summaries[2] == {
        'removed-files-size': '30058',
        'added-data-files': '3',
        'total-equality-deletes': '0',
        'added-records': '3',
        'total-position-deletes': '0',
        'deleted-data-files': '6',
        'added-files-size': '15029',
        'total-delete-files': '0',
        'total-files-size': '15029',
        'deleted-records': '6',
        'total-records': '3',
        'total-data-files': '3',
    }
    # append another 3 new data files with 3 records, giving a total of 6 files and 6 records
    assert summaries[3] == {
        'added-data-files': '3',
        'total-equality-deletes': '0',
        'added-records': '3',
        'total-position-deletes': '0',
        'added-files-size': '15029',
        'total-delete-files': '0',
        'total-files-size': '30058',
        'total-data-files': '6',
        'total-records': '6',
    }
    # static overwrite which deletes 2 record (one from step3, one from step4) and 2 datafile, adding 3 new data files and 3 records, so total data files and records are 6 - 2 + 3 = 7
    assert summaries[4] == {
        'removed-files-size': '10790',
        'added-data-files': '3',
        'total-equality-deletes': '0',
        'added-records': '3',
        'deleted-data-files': '2',
        'total-position-deletes': '0',
        'added-files-size': '15029',
        'total-delete-files': '0',
        'deleted-records': '2',
        'total-files-size': '34297',
        'total-data-files': '7',
        'total-records': '7',
    }
    # dynamic overwrite which touches all partition keys and thus delete all datafiles, adding 3 new data files and 3 records, so total data files and records are 7 - 7 + 3 = 3
    assert summaries[5] == {
        'added-data-files': '3',
        'total-equality-deletes': '0',
        'added-records': '3',
        'total-position-deletes': '0',
        'added-files-size': '15029',
        'total-delete-files': '0',
        'total-files-size': '15029',
        'total-data-files': '3',
        'total-records': '3',
        'removed-files-size': '34297',
        'deleted-data-files': '7',
        'deleted-records': '7',
    }
    # dynamic overwrite which touches 2 partition values and gets 2 data files deleted. so total data files are 3 - 2 + 2 =3
    assert summaries[6] == {
        'removed-files-size': '9634',
        'added-data-files': '2',
        'total-equality-deletes': '0',
        'added-records': '2',
        'deleted-data-files': '2',
        'total-position-deletes': '0',
        'added-files-size': '9634',
        'total-delete-files': '0',
        'deleted-records': '2',
        'total-files-size': '15029',
        'total-data-files': '3',
        'total-records': '3',
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
    tbl.overwrite(arrow_table_with_null)
    tbl.append(arrow_table_with_null)
    tbl.overwrite(arrow_table_with_null, overwrite_filter="int=1")
    tbl.dynamic_overwrite(arrow_table_with_null)
    tbl.dynamic_overwrite(arrow_table_with_null.slice(0, 2))

    # Snapshot 1: first append links to 1 manifest file (M1)
    # Snapshot 2: second append's manifest list links to  2 manifest files (M1, M2)
    # Snapshot 3: third operation of full static overwrite's manifest list is linked to 2 manifest files (M3 which has 3 added files; M4 which has 6 deleted entries from M1 and M2)
    # Snapshot 4: fourth operation of append manifest list abandons M4 since it has no existing or added entries and keeps M3 and added M5 with 3 added files
    # Snapshot 5: fifth operation of static overwrite's manifest list is linked to one filtered manifest M7 which filters and merges M5 and M6 where each has 1 entrys are deleted (int=1 matching the filter) and 2 entries marked as existed, this operation
    #             also links to M6 which adds 3 entries.
    # Snapshot 6: six operation of dynamic overwrite with 3 partition keys links its manifestlist to 2 new manifest lists:
    #             one M8 for adding 3 new datafiles
    #             one M9 for filtered previous manifest file which deletes all 7 data files from S5([M6, M7])
    # Snapshot 7: seventh operation of dynamic overwrite with 2 partition keys links its manifestlist to 2 new manifest lists:
    #             one M10 for adding 3 new datafiles
    #             one M11 for filtered previous manifest file which deletes 2 out of 3 data files added from S6
    #             and keeps the remaining one as existing.

    # So we have flattened list of [[M1], [M1, M2], [M3, M4], [M3, M5], [M6, M7], [M8, M9], [M10, M11]]
    #                           for[ S1,   S2,      S3,       S4,       S5,       S6,       S7        ].

    # where: add      exist      delete    added_by
    # M1      3         0           0        S1
    # M2      3         0           0        S2
    # M3      3         0           0        S3
    # M4      0         0           6        S3
    # M5      3         0           0        S4
    # M6      3         0           0        S5
    # M7      0         4           2        S5
    # M8      3         0           0        S6
    # M9      0         0           7        S6
    # M10     2         0           0        S7
    # M11     0         1           2        S7

    spark.sql(
        f"""
            REFRESH TABLE {identifier}
        """
    )
    rows = spark.sql(
        f"""
        SELECT added_data_files_count, existing_data_files_count, deleted_data_files_count
        FROM {identifier}.all_manifests
    """
    ).collect()

    assert [row.added_data_files_count for row in rows] == [3, 3, 3, 3, 0, 3, 3, 3, 0, 3, 0, 2, 0]
    assert [row.existing_data_files_count for row in rows] == [0, 0, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 1]
    assert [row.deleted_data_files_count for row in rows] == [0, 0, 0, 0, 6, 0, 0, 0, 2, 0, 7, 0, 2]


@pytest.mark.integration
def test_invalid_arguments(spark: SparkSession, session_catalog: Catalog, arrow_table_with_null: pa.Table) -> None:
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
        tbl.overwrite("not a df")

    with pytest.raises(ValueError, match="Expected PyArrow table, got: not a df"):
        tbl.append("not a df")


@pytest.mark.integration
@pytest.mark.parametrize(
    "part_col, expr",
    [
        ('int', 'int=1'),
        ('int', EqualTo(Reference("int"), 1)),
        ('int', 'int is NULL'),
        ('int', IsNull(Reference("int"))),
        ('bool', "bool == 'true'"),
        ('bool', EqualTo(Reference("bool"), True)),
        ('bool', 'bool is NULL'),
        ('bool', IsNull(Reference("bool"))),
        ('long', 'long=1'),
        ('long', EqualTo(Reference("long"), 1)),
        ('long', 'long is NULL'),
        ('long', IsNull(Reference("long"))),
        ('date', "date='2023-01-01'"),
        ('date', EqualTo(Reference("date"), arrow_to_iceberg_representation(DateType(), date(2023, 1, 1)))),
        ('date', 'date is NULL'),
        ('date', IsNull(Reference("date"))),
        ('timestamp', "timestamp='2023-01-01T19:25:00'"),
        (
            'timestamp',
            EqualTo(Reference("timestamp"), arrow_to_iceberg_representation(TimestampType(), datetime(2023, 1, 1, 19, 25))),
        ),
        ('timestamp', 'timestamp is NULL'),
        ('timestamp', IsNull(Reference("timestamp"))),
        ('timestamptz', "timestamptz='2023-01-01T19:25:00+00:00'"),
        (
            'timestamptz',
            EqualTo(
                Reference("timestamptz"),
                TimestampLiteral(
                    arrow_to_iceberg_representation(TimestamptzType(), datetime(2023, 1, 1, 19, 25, 00, tzinfo=timezone.utc))
                ),
            ),
        ),
        ('timestamptz', 'timestamptz is NULL'),
        ('timestamptz', IsNull(Reference("timestamptz"))),
    ],
)
def test_query_filter_after_append_overwrite_table_with_expr(
    spark: SparkSession,
    session_catalog: Catalog,
    part_col: str,
    expr: Union[str, BooleanExpression],
    arrow_table_with_null: pa.Table,
) -> None:
    identifier = f"default.arrow_table_v1_appended_overwrite_partitioned_on_col_{part_col}"
    try:
        spark.sql(f"drop table {identifier}")
    except AnalysisException:
        pass

    nested_field = TABLE_SCHEMA.find_field(part_col)
    source_id = nested_field.field_id
    tbl = session_catalog.create_table(
        identifier=identifier,
        schema=TABLE_SCHEMA,
        partition_spec=PartitionSpec(
            PartitionField(source_id=source_id, field_id=1001, transform=IdentityTransform(), name=part_col)
        ),
        properties={'format-version': '1'},
    )

    for _ in range(2):
        tbl.append(arrow_table_with_null)
    tbl.overwrite(arrow_table_with_null, expr)

    iceberg_table = session_catalog.load_table(identifier=identifier)
    spark.sql(f"select * from {identifier}").show(20, False)
    assert iceberg_table.scan(row_filter=expr).to_arrow().num_rows == 1
    assert iceberg_table.scan().to_arrow().num_rows == 7
