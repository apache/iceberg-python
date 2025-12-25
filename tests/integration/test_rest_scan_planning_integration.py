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
from typing import Any

import pyarrow as pa
import pytest
from pyspark.sql import SparkSession

from pyiceberg.catalog import Catalog, load_catalog
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.expressions import And, BooleanExpression, EqualTo, GreaterThan, LessThan
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table import ALWAYS_TRUE, Table
from pyiceberg.transforms import IdentityTransform
from pyiceberg.types import LongType, NestedField, StringType


@pytest.fixture(scope="session")
def scan_catalog() -> Catalog:
    return load_catalog(
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


def recreate_table(catalog: Catalog, identifier: str, **kwargs: Any) -> Table:
    """Drop table if exists and create a new one."""
    try:
        catalog.drop_table(identifier)
    except Exception:
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
