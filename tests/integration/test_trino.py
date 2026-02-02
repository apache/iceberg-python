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
"""Integration tests for Trino engine."""

import uuid

import pyarrow as pa
import pytest
from sqlalchemy import Connection, text

from pyiceberg.catalog import Catalog
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import BucketTransform, IdentityTransform, Transform
from pyiceberg.types import NestedField, UUIDType

TEST_NAMESPACE = "test_trino_namespace"
TEST_NAMESPACE_IDENTIFIER = (TEST_NAMESPACE,)


@pytest.mark.trino
def test_schema_exists_in_trino(trino_rest_conn: Connection, catalog: RestCatalog) -> None:
    """Verifies that an Iceberg namespace correctly appears as a schema in Trino.

    This test ensures the synchronization between Iceberg's namespace concept and
    Trino's schema concept, confirming that after creating a namespace in the Iceberg
    catalog, it becomes visible as a schema in the Trino environment.
    """
    if catalog.namespace_exists(TEST_NAMESPACE_IDENTIFIER):
        catalog.drop_namespace(TEST_NAMESPACE_IDENTIFIER)
    catalog.create_namespace_if_not_exists(TEST_NAMESPACE_IDENTIFIER)

    assert catalog.namespace_exists(TEST_NAMESPACE_IDENTIFIER)


@pytest.mark.trino
@pytest.mark.integration
@pytest.mark.parametrize(
    "transform",
    [
        IdentityTransform(),
        BucketTransform(num_buckets=32),
    ],
)
@pytest.mark.parametrize(
    "catalog,trino_conn",
    [
        (pytest.lazy_fixture("session_catalog_hive"), pytest.lazy_fixture("trino_hive_conn")),
        (pytest.lazy_fixture("session_catalog"), pytest.lazy_fixture("trino_rest_conn")),
    ],
)
def test_uuid_partitioning_with_trino(catalog: Catalog, trino_conn: Connection, transform: Transform) -> None:  # type: ignore
    """Test UUID partitioning using Trino engine.

    This test verifies that UUID-partitioned tables created via PyIceberg can be
    correctly queried through Trino. It tests both Identity and Bucket transforms
    on UUID columns, which are not fully supported in Spark but work in Trino.
    """
    identifier = f"default.test_uuid_partitioning_{str(transform).replace('[32]', '')}"

    schema = Schema(NestedField(field_id=1, name="uuid", field_type=UUIDType(), required=True))

    try:
        catalog.drop_table(identifier=identifier)
    except NoSuchTableError:
        pass

    partition_spec = PartitionSpec(
        PartitionField(source_id=1, field_id=1000, transform=transform, name=f"uuid_{str(transform).replace('[32]', '')}")
    )

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

    tbl = catalog.create_table(
        identifier=identifier,
        schema=schema,
        partition_spec=partition_spec,
    )

    tbl.append(arr_table)
    rows = trino_conn.execute(text(f"SELECT * FROM {identifier}")).fetchall()
    lhs = sorted([r[0] for r in rows])
    rhs = sorted([u.as_py() for u in tbl.scan().to_arrow()["uuid"].combine_chunks()])
    assert lhs == rhs
