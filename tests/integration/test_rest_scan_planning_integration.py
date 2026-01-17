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
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from typing import Any
from uuid import uuid4

import pyarrow as pa
import pytest
from pyspark.sql import SparkSession

from pyiceberg.catalog import Catalog, load_catalog
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.expressions import (
    And,
    BooleanExpression,
    EqualTo,
    GreaterThan,
    GreaterThanOrEqual,
    In,
    IsNull,
    LessThan,
    LessThanOrEqual,
    Not,
    NotEqualTo,
    NotIn,
    NotNull,
    Or,
    StartsWith,
)
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table import ALWAYS_TRUE, Table
from pyiceberg.transforms import (
    IdentityTransform,
)
from pyiceberg.types import (
    BinaryType,
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    FixedType,
    LongType,
    NestedField,
    StringType,
    TimestampType,
    TimestamptzType,
    TimeType,
    UUIDType,
)


@pytest.fixture(scope="session")
def scan_catalog() -> Catalog:
    catalog = load_catalog(
        "local",
        **{
            "type": "rest",
            "uri": "http://localhost:8181",
            "s3.endpoint": "http://localhost:9000",
            "s3.access-key-id": "admin",
            "s3.secret-access-key": "password",
            "rest-scan-planning-enabled": "true",
        },
    )
    catalog.create_namespace_if_not_exists("default")
    return catalog


def recreate_table(catalog: Catalog, identifier: str, **kwargs: Any) -> Table:
    """Drop table if exists and create a new one."""
    try:
        catalog.drop_table(identifier)
    except NoSuchTableError:
        pass
    return catalog.create_table(identifier, **kwargs)


def _assert_remote_scan_matches_local_scan(
    rest_table: Table,
    session_catalog: Catalog,
    identifier: str,
    row_filter: BooleanExpression = ALWAYS_TRUE,
) -> None:
    rest_tasks = list(rest_table.scan(row_filter=row_filter).plan_files())
    rest_paths = {task.file.file_path for task in rest_tasks}

    local_table = session_catalog.load_table(identifier)
    local_tasks = list(local_table.scan(row_filter=row_filter).plan_files())
    local_paths = {task.file.file_path for task in local_tasks}

    assert rest_paths == local_paths


@pytest.mark.integration
def test_rest_scan_matches_local(scan_catalog: RestCatalog, session_catalog: Catalog) -> None:
    identifier = "default.test_rest_scan"

    table = recreate_table(
        scan_catalog,
        identifier,
        schema=Schema(
            NestedField(1, "id", LongType()),
            NestedField(2, "data", StringType()),
            NestedField(3, "num", LongType()),
        ),
    )
    table.append(pa.Table.from_pydict({"id": [1, 2, 3], "data": ["a", "b", "c"], "num": [10, 20, 30]}))
    table.append(pa.Table.from_pydict({"id": [4, 5, 6], "data": ["d", "e", "f"], "num": [40, 50, 60]}))

    try:
        _assert_remote_scan_matches_local_scan(table, session_catalog, identifier)
    finally:
        scan_catalog.drop_table(identifier)


@pytest.mark.integration
def test_rest_scan_with_filter(scan_catalog: RestCatalog, session_catalog: Catalog) -> None:
    identifier = "default.test_rest_scan_filter"

    table = recreate_table(
        scan_catalog,
        identifier,
        schema=Schema(
            NestedField(1, "id", LongType()),
            NestedField(2, "data", LongType()),
        ),
    )
    table.append(pa.Table.from_pydict({"id": [1, 2, 3], "data": [10, 20, 30]}))

    try:
        _assert_remote_scan_matches_local_scan(
            table,
            session_catalog,
            identifier,
            row_filter=And(GreaterThan("data", 5), LessThan("data", 25)),
        )

        _assert_remote_scan_matches_local_scan(
            table,
            session_catalog,
            identifier,
            row_filter=EqualTo("id", 1),
        )
    finally:
        scan_catalog.drop_table(identifier)


@pytest.mark.integration
def test_rest_scan_with_deletes(spark: SparkSession, scan_catalog: RestCatalog, session_catalog: Catalog) -> None:
    identifier = "default.test_rest_scan_deletes"

    spark.sql(f"DROP TABLE IF EXISTS {identifier}")
    spark.sql(f"""
        CREATE TABLE {identifier} (id bigint, data bigint)
        USING iceberg
        TBLPROPERTIES(
            'format-version' = 2,
            'write.delete.mode'='merge-on-read'
        )
    """)
    spark.sql(f"INSERT INTO {identifier} VALUES (1, 10), (2, 20), (3, 30)")
    spark.sql(f"DELETE FROM {identifier} WHERE id = 2")

    try:
        rest_table = scan_catalog.load_table(identifier)
        rest_tasks = list(rest_table.scan().plan_files())
        rest_paths = {task.file.file_path for task in rest_tasks}
        rest_delete_paths = {delete.file_path for task in rest_tasks for delete in task.delete_files}

        local_table = session_catalog.load_table(identifier)
        local_tasks = list(local_table.scan().plan_files())
        local_paths = {task.file.file_path for task in local_tasks}
        local_delete_paths = {delete.file_path for task in local_tasks for delete in task.delete_files}

        assert rest_paths == local_paths
        assert rest_delete_paths == local_delete_paths
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {identifier}")


@pytest.mark.integration
def test_rest_scan_with_partitioning(scan_catalog: RestCatalog, session_catalog: Catalog) -> None:
    identifier = "default.test_rest_scan_partitioned"

    schema = Schema(
        NestedField(1, "id", LongType()),
        NestedField(2, "category", StringType()),
        NestedField(3, "data", LongType()),
    )
    partition_spec = PartitionSpec(PartitionField(2, 1000, IdentityTransform(), "category"))

    table = recreate_table(scan_catalog, identifier, schema=schema, partition_spec=partition_spec)

    table.append(pa.Table.from_pydict({"id": [1, 2], "category": ["a", "a"], "data": [10, 20]}))
    table.append(pa.Table.from_pydict({"id": [3, 4], "category": ["b", "b"], "data": [30, 40]}))

    try:
        _assert_remote_scan_matches_local_scan(table, session_catalog, identifier)

        # test filter against partition
        _assert_remote_scan_matches_local_scan(
            table,
            session_catalog,
            identifier,
            row_filter=EqualTo("category", "a"),
        )
    finally:
        scan_catalog.drop_table(identifier)


@pytest.mark.integration
def test_rest_scan_primitive_types(scan_catalog: RestCatalog, session_catalog: Catalog) -> None:
    identifier = "default.test_primitives"

    schema = Schema(
        NestedField(1, "bool_col", BooleanType()),
        NestedField(2, "long_col", LongType()),
        NestedField(3, "double_col", DoubleType()),
        NestedField(4, "decimal_col", DecimalType(10, 2)),
        NestedField(5, "string_col", StringType()),
        NestedField(6, "date_col", DateType()),
        NestedField(7, "time_col", TimeType()),
        NestedField(8, "timestamp_col", TimestampType()),
        NestedField(9, "timestamptz_col", TimestamptzType()),
        NestedField(10, "uuid_col", UUIDType()),
        NestedField(11, "fixed_col", FixedType(16)),
        NestedField(12, "binary_col", BinaryType()),
    )

    table = recreate_table(scan_catalog, identifier, schema=schema)

    now = datetime.now()
    now_tz = datetime.now(tz=timezone.utc)
    today = date.today()
    uuid1, uuid2, uuid3 = uuid4(), uuid4(), uuid4()

    arrow_table = pa.Table.from_pydict(
        {
            "bool_col": [True, False, True],
            "long_col": [100, 200, 300],
            "double_col": [1.11, 2.22, 3.33],
            "decimal_col": [Decimal("1.23"), Decimal("4.56"), Decimal("7.89")],
            "string_col": ["a", "b", "c"],
            "date_col": [today, today - timedelta(days=1), today - timedelta(days=2)],
            "time_col": [time(8, 30, 0), time(12, 0, 0), time(18, 45, 30)],
            "timestamp_col": [now, now - timedelta(hours=1), now - timedelta(hours=2)],
            "timestamptz_col": [now_tz, now_tz - timedelta(hours=1), now_tz - timedelta(hours=2)],
            "uuid_col": [uuid1.bytes, uuid2.bytes, uuid3.bytes],
            "fixed_col": [b"0123456789abcdef", b"abcdef0123456789", b"fedcba9876543210"],
            "binary_col": [b"hello", b"world", b"test"],
        },
        schema=schema.as_arrow(),
    )
    table.append(arrow_table)

    try:
        _assert_remote_scan_matches_local_scan(table, session_catalog, identifier)
    finally:
        scan_catalog.drop_table(identifier)


@pytest.mark.integration
def test_rest_scan_with_filters(scan_catalog: RestCatalog, session_catalog: Catalog) -> None:
    identifier = "default.test_complex_filters"

    schema = Schema(
        NestedField(1, "id", LongType()),
        NestedField(2, "name", StringType()),
        NestedField(3, "value", LongType()),
        NestedField(4, "optional", StringType(), required=False),
    )

    table = recreate_table(scan_catalog, identifier, schema=schema)

    table.append(
        pa.Table.from_pydict(
            {
                "id": list(range(1, 21)),
                "name": [f"item_{i}" for i in range(1, 21)],
                "value": [i * 100 for i in range(1, 21)],
                "optional": [None if i % 3 == 0 else f"opt_{i}" for i in range(1, 21)],
            }
        )
    )

    try:
        filters = [
            EqualTo("id", 10),
            NotEqualTo("id", 10),
            GreaterThan("value", 1000),
            GreaterThanOrEqual("value", 1000),
            LessThan("value", 500),
            LessThanOrEqual("value", 500),
            In("id", [1, 5, 10, 15]),
            NotIn("id", [1, 5, 10, 15]),
            IsNull("optional"),
            NotNull("optional"),
            StartsWith("name", "item_1"),
            And(GreaterThan("id", 5), LessThan("id", 15)),
            Or(EqualTo("id", 1), EqualTo("id", 20)),
            Not(EqualTo("id", 10)),
        ]

        for filter_expr in filters:
            _assert_remote_scan_matches_local_scan(table, session_catalog, identifier, row_filter=filter_expr)
    finally:
        scan_catalog.drop_table(identifier)


@pytest.mark.integration
def test_rest_scan_empty_table(scan_catalog: RestCatalog, session_catalog: Catalog) -> None:
    identifier = "default.test_empty_table"

    schema = Schema(
        NestedField(1, "id", LongType()),
        NestedField(2, "data", StringType()),
    )

    table = recreate_table(scan_catalog, identifier, schema=schema)

    try:
        rest_tasks = list(table.scan().plan_files())
        local_table = session_catalog.load_table(identifier)
        local_tasks = list(local_table.scan().plan_files())

        assert len(rest_tasks) == 0
        assert len(local_tasks) == 0
    finally:
        scan_catalog.drop_table(identifier)
