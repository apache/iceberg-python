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


from typing import Any, Dict

import pytest

from pyiceberg.catalog import PropertiesUpdateSummary
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.exceptions import (
    BadRequestError,
    NamespaceAlreadyExistsError,
    NoSuchIdentifierError,
    NoSuchNamespaceError,
    NoSuchTableError,
    NoSuchViewError,
    TableAlreadyExistsError,
)
from pyiceberg.io import load_file_io
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.table.metadata import TableMetadataV2
from pyiceberg.table.sorting import NullOrder, SortDirection, SortField, SortOrder
from pyiceberg.transforms import IdentityTransform, TruncateTransform

TEST_NAMESPACE_IDENTIFIER = ("rest_integration_ns",)
TEST_TABLE_IDENTIFIER = ("rest_integration_ns", "rest_integration_tbl")
TEST_TABLE_IDENTIFIER_RENAME = ("rest_integration_ns", "renamed_rest_integration_tbl")

EXAMPLE_table_metadata_no_snapshot_v2 = {
    "format-version": 2,
    "table-uuid": "bf289591-dcc0-4234-ad4f-5c3eed811a29",
    "location": f"s3://warehouse/{TEST_TABLE_IDENTIFIER[0]}/{TEST_TABLE_IDENTIFIER[1]}",
    "last-updated-ms": 1657810967051,
    "last-column-id": 3,
    "schema": {
        "type": "struct",
        "schema-id": 0,
        "identifier-field-ids": [2],
        "fields": [
            {"id": 1, "name": "foo", "required": False, "type": "string"},
            {"id": 2, "name": "bar", "required": True, "type": "int"},
            {"id": 3, "name": "baz", "required": False, "type": "boolean"},
        ],
    },
    "current-schema-id": 0,
    "schemas": [
        {
            "type": "struct",
            "fields": (
                {"id": 1, "name": "foo", "type": "string", "required": False},
                {"id": 2, "name": "bar", "type": "int", "required": True},
                {"id": 3, "name": "baz", "type": "boolean", "required": False},
            ),
            "schema-id": 0,
            "identifier-field-ids": [2],
        }
    ],
    "partition-specs": [{"spec-id": 0, "fields": ()}],
    "default-spec-id": 0,
    "last-partition-id": 999,
    "default-sort-order-id": 0,
    "sort-orders": [{"order-id": 0, "fields": []}],
    "properties": {
        "write.parquet.compression-codec": "zstd",
    },
    "refs": {},
    "snapshots": [],
    "snapshot-log": [],
    "metadata-log": [],
}

EXAMPLE_table_metadata_no_snapshot_partitioned_v2 = {
    "format-version": 2,
    "table-uuid": "bf289591-dcc0-4234-ad4f-5c3eed811a29",
    "location": f"s3://warehouse/{TEST_TABLE_IDENTIFIER[0]}/{TEST_TABLE_IDENTIFIER[1]}",
    "last-updated-ms": 1657810967051,
    "last-column-id": 3,
    "schema": {
        "type": "struct",
        "schema-id": 0,
        "identifier-field-ids": [2],
        "fields": [
            {"id": 1, "name": "foo", "required": False, "type": "string"},
            {"id": 2, "name": "bar", "required": True, "type": "int"},
            {"id": 3, "name": "baz", "required": False, "type": "boolean"},
        ],
    },
    "current-schema-id": 0,
    "schemas": [
        {
            "type": "struct",
            "fields": (
                {"id": 1, "name": "foo", "type": "string", "required": False},
                {"id": 2, "name": "bar", "type": "int", "required": True},
                {"id": 3, "name": "baz", "type": "boolean", "required": False},
            ),
            "schema-id": 0,
            "identifier-field-ids": [2],
        }
    ],
    "partition-specs": [
        {"spec-id": 0, "fields": ({"source-id": 1, "field-id": 1000, "transform": "truncate[3]", "name": "id"},)}
    ],
    "default-spec-id": 0,
    "last-partition-id": 1000,
    "default-sort-order-id": 1,
    "sort-orders": [
        {
            "order-id": 1,
            "fields": [
                {"source-id": 2, "transform": "identity", "direction": SortDirection.ASC, "null-order": NullOrder.NULLS_FIRST}
            ],
        }
    ],
    "properties": {
        "owner": "fokko",
        "write.parquet.compression-codec": "zstd",
    },
    "refs": {},
    "snapshots": [],
    "snapshot-log": [],
    "metadata-log": [],
}


@pytest.fixture
def table_metadata_no_snapshot_v2() -> Dict[str, Any]:
    return EXAMPLE_table_metadata_no_snapshot_v2


@pytest.fixture
def table_metadata_no_snapshot_partitioned_v2() -> Dict[str, Any]:
    return EXAMPLE_table_metadata_no_snapshot_partitioned_v2


@pytest.fixture
def rest_integration_example_metadata_partitioned_v2(table_metadata_no_snapshot_partitioned_v2: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "metadata-location": f"s3://warehouse/{TEST_TABLE_IDENTIFIER[0]}/{TEST_TABLE_IDENTIFIER[1]}",
        "metadata": table_metadata_no_snapshot_partitioned_v2,
        "config": {
            "client.factory": "io.tabular.iceberg.catalog.TabularAwsClientFactory",
            "region": "us-west-2",
        },
    }


@pytest.fixture
def example_table_metadata_with_no_location(table_metadata_no_snapshot_v2: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "metadata": table_metadata_no_snapshot_v2,
        "config": {
            "client.factory": "io.tabular.iceberg.catalog.TabularAwsClientFactory",
            "region": "us-west-2",
        },
    }


@pytest.fixture
def table_metadata_no_snapshot_rest(table_metadata_no_snapshot_v2: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "metadata-location": f"s3://warehouse/{TEST_TABLE_IDENTIFIER[0]}/{TEST_TABLE_IDENTIFIER[1]}",
        "metadata": table_metadata_no_snapshot_v2,
        "config": {
            "client.factory": "io.tabular.iceberg.catalog.TabularAwsClientFactory",
            "region": "us-west-2",
        },
    }


@pytest.mark.integration
@pytest.fixture(scope="function")
@pytest.mark.parametrize("catalog", [pytest.lazy_fixture("session_catalog"), pytest.lazy_fixture("test_clean_up")])
def test_clean_up(catalog: RestCatalog) -> None:
    print("BEGINNING TEST CLEAN UP")
    for namespaces_tuple in catalog.list_namespaces():
        print(namespaces_tuple)
        namespace_name = namespaces_tuple[0]
        print(namespace_name)
        if TEST_NAMESPACE_IDENTIFIER[0] in namespace_name:
            for identifier in catalog.list_tables(namespace_name):
                print(identifier)
                catalog.purge_table(identifier)
            print(namespace_name)
            print(catalog.list_namespaces())
            if catalog.namespace_exists(TEST_NAMESPACE_IDENTIFIER):
                catalog.drop_namespace(TEST_NAMESPACE_IDENTIFIER)
            print(catalog.list_namespaces())

    print("FINISHED TEST CLEAN UP")


@pytest.mark.integration
@pytest.mark.parametrize("catalog,clean_up", [(pytest.lazy_fixture("session_catalog"), pytest.lazy_fixture("test_clean_up"))])
def test_create_namespace_200(catalog: RestCatalog, clean_up: Any) -> None:
    catalog.create_namespace(TEST_NAMESPACE_IDENTIFIER)
    assert TEST_NAMESPACE_IDENTIFIER in catalog.list_namespaces()


@pytest.mark.integration
@pytest.mark.parametrize("catalog,clean_up", [(pytest.lazy_fixture("session_catalog"), pytest.lazy_fixture("test_clean_up"))])
def test_create_namespace_if_exists_409(catalog: RestCatalog, clean_up: Any) -> None:
    catalog.create_namespace(TEST_NAMESPACE_IDENTIFIER)
    catalog.create_namespace_if_not_exists(TEST_NAMESPACE_IDENTIFIER)
    assert TEST_NAMESPACE_IDENTIFIER in catalog.list_namespaces()


@pytest.mark.integration
@pytest.mark.parametrize("catalog,clean_up", [(pytest.lazy_fixture("session_catalog"), pytest.lazy_fixture("test_clean_up"))])
def test_create_namespace_409(catalog: RestCatalog, clean_up: Any) -> None:
    catalog.create_namespace(TEST_NAMESPACE_IDENTIFIER)

    with pytest.raises(NamespaceAlreadyExistsError) as e:
        catalog.create_namespace(TEST_NAMESPACE_IDENTIFIER)
    assert "Namespace already exists" in str(e.value)


@pytest.mark.integration
@pytest.mark.parametrize("catalog,clean_up", [(pytest.lazy_fixture("session_catalog"), pytest.lazy_fixture("test_clean_up"))])
def test_drop_namespace_404(catalog: RestCatalog, clean_up: Any) -> None:
    with pytest.raises(NoSuchNamespaceError) as e:
        catalog.drop_namespace(TEST_NAMESPACE_IDENTIFIER)
    assert "Namespace does not exist" in str(e.value)


@pytest.mark.integration
@pytest.mark.parametrize("catalog,clean_up", [(pytest.lazy_fixture("session_catalog"), pytest.lazy_fixture("test_clean_up"))])
def test_drop_namespace_409(catalog: RestCatalog, table_schema_simple: Schema, clean_up: Any) -> None:
    catalog.create_namespace(TEST_NAMESPACE_IDENTIFIER)
    catalog.create_table(TEST_TABLE_IDENTIFIER, schema=table_schema_simple)

    with pytest.raises(BadRequestError) as e:  
        catalog.drop_namespace(TEST_NAMESPACE_IDENTIFIER)
    assert f"Namespace {TEST_NAMESPACE_IDENTIFIER[0]} is not empty" in str(e.value)


@pytest.mark.integration
@pytest.mark.parametrize("catalog,clean_up", [(pytest.lazy_fixture("session_catalog"), pytest.lazy_fixture("test_clean_up"))])
def test_load_namespace_properties_200(catalog: RestCatalog, clean_up: Any) -> None:
    catalog.create_namespace(TEST_NAMESPACE_IDENTIFIER, properties={"prop": "yes"})

    assert "yes" == catalog.load_namespace_properties(TEST_NAMESPACE_IDENTIFIER)["prop"]



@pytest.mark.integration
@pytest.mark.parametrize("catalog,clean_up", [(pytest.lazy_fixture("session_catalog"), pytest.lazy_fixture("test_clean_up"))])
def test_load_namespace_properties_404(catalog: RestCatalog, clean_up: Any) -> None:
    with pytest.raises(NoSuchNamespaceError) as e:
        catalog.load_namespace_properties(TEST_NAMESPACE_IDENTIFIER)["prop"]
    assert "Namespace does not exist" in str(e.value)


# Update Properties
@pytest.mark.integration
@pytest.mark.parametrize("catalog,clean_up", [(pytest.lazy_fixture("session_catalog"), pytest.lazy_fixture("test_clean_up"))])
def test_update_namespace_properties_200(catalog: RestCatalog, clean_up: Any) -> None:
    catalog.create_namespace(TEST_NAMESPACE_IDENTIFIER, properties={"prop": "yes", "abc": "abc"})
    assert PropertiesUpdateSummary(removed=["abc"], updated=["prop"], missing=["def"]) == catalog.update_namespace_properties(
        TEST_NAMESPACE_IDENTIFIER, {"abc", "def"}, {"prop": "yes"}
    )


@pytest.mark.integration
@pytest.mark.parametrize("catalog,clean_up", [(pytest.lazy_fixture("session_catalog"), pytest.lazy_fixture("test_clean_up"))])
def test_update_namespace_properties_404(catalog: RestCatalog, clean_up: Any) -> None:
    with pytest.raises(NoSuchNamespaceError) as e:
        catalog.update_namespace_properties(TEST_NAMESPACE_IDENTIFIER, {"abc", "def"}, {"prop": "yes"})
    assert "Namespace does not exist" in str(e.value)


@pytest.mark.integration
@pytest.mark.parametrize("catalog,clean_up", [(pytest.lazy_fixture("session_catalog"), pytest.lazy_fixture("test_clean_up"))])
def test_namespace_exists_204(catalog: RestCatalog, clean_up: Any) -> None:
    catalog.create_namespace(TEST_NAMESPACE_IDENTIFIER)

    assert catalog.namespace_exists(TEST_NAMESPACE_IDENTIFIER)


@pytest.mark.integration
@pytest.mark.parametrize("catalog,clean_up", [(pytest.lazy_fixture("session_catalog"), pytest.lazy_fixture("test_clean_up"))])
def test_namespace_exists_404(catalog: RestCatalog, clean_up: Any) -> None:
    assert not catalog.namespace_exists(TEST_NAMESPACE_IDENTIFIER)


@pytest.mark.integration
@pytest.mark.parametrize("catalog,clean_up", [(pytest.lazy_fixture("session_catalog"), pytest.lazy_fixture("test_clean_up"))])
def test_namespace_empty(catalog: RestCatalog, clean_up: Any) -> None:
    assert not catalog.namespace_exists("")


@pytest.mark.integration
@pytest.mark.parametrize("catalog,clean_up", [(pytest.lazy_fixture("session_catalog"), pytest.lazy_fixture("test_clean_up"))])
def test_load_table_200(
    catalog: RestCatalog, clean_up: Any, table_schema_simple: Schema, table_metadata_no_snapshot_rest: Dict[str, Any]
) -> None:
    catalog.create_namespace(TEST_NAMESPACE_IDENTIFIER)
    catalog.create_table(TEST_TABLE_IDENTIFIER, schema=table_schema_simple)

    actual = catalog.load_table(TEST_TABLE_IDENTIFIER)

    expected_metadata = table_metadata_no_snapshot_rest["metadata"]
    expected_metadata["table-uuid"] = actual.metadata.model_dump()["table-uuid"]  # Note! Generated ID
    expected_metadata["last-updated-ms"] = actual.metadata.model_dump()["last-updated-ms"]  # Note! Generated TIMESTAMP

    expected = Table(
        identifier=TEST_TABLE_IDENTIFIER,
        metadata_location=table_metadata_no_snapshot_rest["metadata-location"],
        metadata=TableMetadataV2(**expected_metadata),
        io=load_file_io(),
        catalog=catalog,
    )
    assert actual.metadata.model_dump() == expected.metadata.model_dump()


@pytest.mark.integration
@pytest.mark.parametrize("catalog,clean_up", [(pytest.lazy_fixture("session_catalog"), pytest.lazy_fixture("test_clean_up"))])
def test_load_table_honor_access_delegation(
    catalog: RestCatalog, clean_up: Any, table_schema_simple: Schema, table_metadata_no_snapshot_rest: Dict[str, Any]
) -> None:
    catalog = RestCatalog(
        "rest",
        **{
            "uri": "http://localhost:8181",
            "token": "Some-jwt-token",
            "header.X-Iceberg-Access-Delegation": "remote-signing",
        },
    )

    catalog.create_namespace(TEST_NAMESPACE_IDENTIFIER)
    catalog.create_table(TEST_TABLE_IDENTIFIER, schema=table_schema_simple)

    actual = catalog.load_table(TEST_TABLE_IDENTIFIER)

    expected_metadata = table_metadata_no_snapshot_rest["metadata"]
    expected_metadata["table-uuid"] = actual.metadata.model_dump()["table-uuid"]  # Note! Generated ID
    expected_metadata["last-updated-ms"] = actual.metadata.model_dump()["last-updated-ms"]  # Note! Generated TIMESTAMP

    expected = Table(
        identifier=TEST_TABLE_IDENTIFIER,
        metadata_location=table_metadata_no_snapshot_rest["metadata-location"],
        metadata=TableMetadataV2(**expected_metadata),
        io=load_file_io(),
        catalog=catalog,
    )

    assert actual.metadata.model_dump() == expected.metadata.model_dump()
    assert "remote-signing" == catalog._session.headers["X-Iceberg-Access-Delegation"]


@pytest.mark.integration
@pytest.mark.parametrize("catalog,clean_up", [(pytest.lazy_fixture("session_catalog"), pytest.lazy_fixture("test_clean_up"))])
def test_load_table_from_self_identifier_200(
    catalog: RestCatalog, clean_up: Any, table_schema_simple: Schema, table_metadata_no_snapshot_rest: Dict[str, Any]
) -> None:
    catalog.create_namespace(TEST_NAMESPACE_IDENTIFIER)
    catalog.create_table(TEST_TABLE_IDENTIFIER, schema=table_schema_simple)

    table = catalog.load_table(TEST_TABLE_IDENTIFIER)
    actual = catalog.load_table(table.name())

    expected_metadata = table_metadata_no_snapshot_rest["metadata"]
    expected_metadata["table-uuid"] = actual.metadata.model_dump()["table-uuid"]  # Note! Generated ID
    expected_metadata["last-updated-ms"] = actual.metadata.model_dump()["last-updated-ms"]  # Note! Generated TIMESTAMP

    expected = Table(
        identifier=TEST_TABLE_IDENTIFIER,
        metadata_location=table_metadata_no_snapshot_rest["metadata-location"],
        metadata=TableMetadataV2(**expected_metadata),
        io=load_file_io(),
        catalog=catalog,
    )
    assert actual.metadata.model_dump() == expected.metadata.model_dump()


@pytest.mark.integration
@pytest.mark.parametrize("catalog,clean_up", [(pytest.lazy_fixture("session_catalog"), pytest.lazy_fixture("test_clean_up"))])
def test_load_table_404(catalog: RestCatalog, clean_up: Any) -> None:
    catalog.create_namespace(TEST_NAMESPACE_IDENTIFIER)

    with pytest.raises(NoSuchTableError) as e:
        catalog.load_table((TEST_NAMESPACE_IDENTIFIER[0], "does_not_exist"))
    assert "Table does not exist" in str(e.value)




@pytest.mark.integration
@pytest.mark.parametrize("catalog,clean_up", [(pytest.lazy_fixture("session_catalog"), pytest.lazy_fixture("test_clean_up"))])
def test_table_exists_204(catalog: RestCatalog, clean_up: Any, table_schema_simple: Schema) -> None:
    catalog.create_namespace(TEST_NAMESPACE_IDENTIFIER)
    catalog.create_table(TEST_TABLE_IDENTIFIER, schema=table_schema_simple)

    assert catalog.table_exists(TEST_TABLE_IDENTIFIER)


@pytest.mark.integration
@pytest.mark.parametrize("catalog,clean_up", [(pytest.lazy_fixture("session_catalog"), pytest.lazy_fixture("test_clean_up"))])
def test_table_exists_404(catalog: RestCatalog, clean_up: Any) -> None:
    assert not catalog.table_exists(TEST_TABLE_IDENTIFIER)


@pytest.mark.integration
@pytest.mark.parametrize("catalog,clean_up", [(pytest.lazy_fixture("session_catalog"), pytest.lazy_fixture("test_clean_up"))])
def test_drop_table_404(catalog: RestCatalog, clean_up: Any) -> None:
    catalog.create_namespace(TEST_NAMESPACE_IDENTIFIER)

    with pytest.raises(NoSuchTableError) as e:
        catalog.drop_table(TEST_TABLE_IDENTIFIER)
    assert "Table does not exist" in str(e.value)


@pytest.mark.integration
@pytest.mark.parametrize("catalog,clean_up", [(pytest.lazy_fixture("session_catalog"), pytest.lazy_fixture("test_clean_up"))])
def test_create_table_200(
    catalog: RestCatalog,
    clean_up: Any,
    table_schema_simple: Schema,
    rest_integration_example_metadata_partitioned_v2: Dict[str, Any],
) -> None:
    catalog.create_namespace(TEST_NAMESPACE_IDENTIFIER)
    actual = catalog.create_table(
        identifier=TEST_TABLE_IDENTIFIER,
        schema=table_schema_simple,
        location=None,
        partition_spec=PartitionSpec(
            PartitionField(source_id=1, field_id=1000, transform=TruncateTransform(width=3), name="id"), spec_id=1
        ),
        sort_order=SortOrder(SortField(source_id=2, transform=IdentityTransform())),
        properties={"owner": "fokko"},
    )

    expected_metadata = rest_integration_example_metadata_partitioned_v2["metadata"]
    expected_metadata["table-uuid"] = actual.metadata.model_dump()["table-uuid"]  # Note! Generated ID
    expected_metadata["last-updated-ms"] = actual.metadata.model_dump()["last-updated-ms"]  # Note! Generated TIMESTAMP

    expected = Table(
        identifier=TEST_TABLE_IDENTIFIER,
        metadata_location=rest_integration_example_metadata_partitioned_v2["metadata-location"],
        metadata=TableMetadataV2(**expected_metadata),
        io=load_file_io(),
        catalog=catalog,
    )

    assert actual.metadata.model_dump() == expected.metadata.model_dump()


@pytest.mark.integration
@pytest.mark.parametrize("catalog,clean_up", [(pytest.lazy_fixture("session_catalog"), pytest.lazy_fixture("test_clean_up"))])
def test_create_table_with_given_location_removes_trailing_slash_200(
    catalog: RestCatalog, clean_up: Any, table_schema_simple: Schema
) -> None:
    location = "s3://warehouse/database/table-custom-location"

    catalog.create_namespace(TEST_NAMESPACE_IDENTIFIER)

    actual = catalog.create_table(
        identifier=TEST_TABLE_IDENTIFIER,
        schema=table_schema_simple,
        location=f"{location}/",
        partition_spec=PartitionSpec(
            PartitionField(source_id=1, field_id=1000, transform=TruncateTransform(width=3), name="id"), spec_id=1
        ),
        sort_order=SortOrder(SortField(source_id=2, transform=IdentityTransform())),
        properties={"owner": "fokko"},
    )

    assert actual.metadata.model_dump()["location"] == location


@pytest.mark.integration
@pytest.mark.parametrize("catalog,clean_up", [(pytest.lazy_fixture("session_catalog"), pytest.lazy_fixture("test_clean_up"))])
def test_create_staged_table_200(
    catalog: RestCatalog,
    clean_up: Any,
    table_schema_simple: Schema,
    rest_integration_example_metadata_partitioned_v2: Dict[str, Any],
) -> None:
    catalog.create_namespace(TEST_NAMESPACE_IDENTIFIER)

    txn = catalog.create_table_transaction(
        identifier=TEST_TABLE_IDENTIFIER,
        schema=table_schema_simple,
        location=None,
        partition_spec=PartitionSpec(
            PartitionField(source_id=1, field_id=1000, transform=TruncateTransform(width=3), name="id"), spec_id=1
        ),
        sort_order=SortOrder(SortField(source_id=2, transform=IdentityTransform())),
        properties={"owner": "fokko"},
    )

    actual = txn.commit_transaction()

    assert actual.metadata.model_dump()["properties"]["created-at"]

    expected_metadata = rest_integration_example_metadata_partitioned_v2["metadata"]
    expected_metadata["table-uuid"] = actual.metadata.model_dump()["table-uuid"]  # Note! Generated ID
    expected_metadata["last-updated-ms"] = actual.metadata.model_dump()["last-updated-ms"]  # Note! Generated TIMESTAMP
    expected_metadata["properties"]["created-at"] = actual.metadata.model_dump()["properties"][
        "created-at"
    ]  # Note! Generated TIMESTAMP

    expected = Table(
        identifier=TEST_TABLE_IDENTIFIER,
        metadata_location=rest_integration_example_metadata_partitioned_v2["metadata-location"],
        metadata=TableMetadataV2(**expected_metadata),
        io=load_file_io(),
        catalog=catalog,
    )

    assert actual.metadata.model_dump() == expected.metadata.model_dump()


@pytest.mark.integration
@pytest.mark.parametrize("catalog,clean_up", [(pytest.lazy_fixture("session_catalog"), pytest.lazy_fixture("test_clean_up"))])
def test_create_table_409(catalog: RestCatalog, clean_up: Any, table_schema_simple: Schema) -> None:
    catalog.create_namespace(TEST_NAMESPACE_IDENTIFIER)
    catalog.create_table(
        identifier=TEST_TABLE_IDENTIFIER,
        schema=table_schema_simple,
        location=None,
        partition_spec=PartitionSpec(
            PartitionField(source_id=1, field_id=1000, transform=TruncateTransform(width=3), name="id"), spec_id=1
        ),
        sort_order=SortOrder(SortField(source_id=2, transform=IdentityTransform())),
        properties={"owner": "fokko"},
    )

    with pytest.raises(TableAlreadyExistsError) as e:
        catalog.create_table(
            identifier=TEST_TABLE_IDENTIFIER,
            schema=table_schema_simple,
            location=None,
            partition_spec=PartitionSpec(
                PartitionField(source_id=1, field_id=1000, transform=TruncateTransform(width=3), name="id"), spec_id=1
            ),
            sort_order=SortOrder(SortField(source_id=2, transform=IdentityTransform())),
            properties={"owner": "fokko"},
        )

    assert "Table already exists" in str(e.value)


@pytest.mark.integration
@pytest.mark.parametrize("catalog,clean_up", [(pytest.lazy_fixture("session_catalog"), pytest.lazy_fixture("test_clean_up"))])
def test_create_table_if_not_exists_200(catalog: RestCatalog, clean_up: Any, table_schema_simple: Schema) -> None:
    catalog.create_namespace(TEST_NAMESPACE_IDENTIFIER)
    table1 = catalog.create_table(
        identifier=TEST_TABLE_IDENTIFIER,
        schema=table_schema_simple,
        location=None,
        partition_spec=PartitionSpec(
            PartitionField(source_id=1, field_id=1000, transform=TruncateTransform(width=3), name="id"), spec_id=1
        ),
        sort_order=SortOrder(SortField(source_id=2, transform=IdentityTransform())),
        properties={"owner": "fokko"},
    )

    table2 = catalog.create_table_if_not_exists(
        identifier=TEST_TABLE_IDENTIFIER,
        schema=table_schema_simple,
        location=None,
        partition_spec=PartitionSpec(
            PartitionField(source_id=1, field_id=1000, transform=TruncateTransform(width=3), name="id"), spec_id=1
        ),
        sort_order=SortOrder(SortField(source_id=2, transform=IdentityTransform())),
        properties={"owner": "fokko"},
    )

    assert table1 == table2


@pytest.mark.integration
@pytest.mark.parametrize("catalog,clean_up", [(pytest.lazy_fixture("session_catalog"), pytest.lazy_fixture("test_clean_up"))])
def test_delete_namespace_204(catalog: RestCatalog, clean_up: Any) -> None:
    catalog.create_namespace(TEST_NAMESPACE_IDENTIFIER)
    catalog.drop_namespace(TEST_NAMESPACE_IDENTIFIER)

    assert not catalog.namespace_exists(TEST_NAMESPACE_IDENTIFIER)


@pytest.mark.integration
@pytest.mark.parametrize("catalog,clean_up", [(pytest.lazy_fixture("session_catalog"), pytest.lazy_fixture("test_clean_up"))])
def test_delete_table_204(catalog: RestCatalog, clean_up: Any, table_schema_simple: Schema) -> None:
    catalog.create_namespace(TEST_NAMESPACE_IDENTIFIER)
    catalog.create_table(TEST_TABLE_IDENTIFIER, schema=table_schema_simple)

    catalog.drop_table(TEST_TABLE_IDENTIFIER)

    assert not catalog.table_exists(TEST_TABLE_IDENTIFIER)


@pytest.mark.integration
@pytest.mark.parametrize("catalog,clean_up", [(pytest.lazy_fixture("session_catalog"), pytest.lazy_fixture("test_clean_up"))])
def test_delete_table_from_self_identifier_204(catalog: RestCatalog, clean_up: Any, table_schema_simple: Schema) -> None:
    catalog.create_namespace(TEST_NAMESPACE_IDENTIFIER)
    catalog.create_table(TEST_TABLE_IDENTIFIER, schema=table_schema_simple)
    table = catalog.load_table(TEST_TABLE_IDENTIFIER)
    catalog.drop_table(table.name())

    assert not catalog.table_exists(table.name())


@pytest.mark.integration
@pytest.mark.parametrize("catalog,clean_up", [(pytest.lazy_fixture("session_catalog"), pytest.lazy_fixture("test_clean_up"))])
def test_delete_table_404(catalog: RestCatalog, clean_up: Any) -> None:
    catalog.create_namespace(TEST_NAMESPACE_IDENTIFIER)

    with pytest.raises(NoSuchTableError) as e:
        catalog.drop_table(TEST_TABLE_IDENTIFIER)

    assert "Table does not exist" in str(e.value)




@pytest.mark.integration
@pytest.mark.parametrize("catalog,clean_up", [(pytest.lazy_fixture("session_catalog"), pytest.lazy_fixture("test_clean_up"))])
def test_rename_table_200(
    catalog: RestCatalog, clean_up: Any, table_schema_simple: Schema, table_metadata_no_snapshot_rest: Dict[str, Any]
) -> None:
    catalog.create_namespace(TEST_NAMESPACE_IDENTIFIER)

    catalog.create_table(TEST_TABLE_IDENTIFIER, table_schema_simple)

    actual = catalog.rename_table(TEST_TABLE_IDENTIFIER, TEST_TABLE_IDENTIFIER_RENAME)

    expected_metadata = table_metadata_no_snapshot_rest["metadata"]
    expected_metadata["table-uuid"] = actual.metadata.model_dump()["table-uuid"]  # Note! Generated ID
    expected_metadata["last-updated-ms"] = actual.metadata.model_dump()["last-updated-ms"]  # Note! Generated TIMESTAMP

    expected = Table(
        identifier=TEST_TABLE_IDENTIFIER_RENAME,
        metadata_location=table_metadata_no_snapshot_rest["metadata-location"],
        metadata=TableMetadataV2(**expected_metadata),
        io=load_file_io(),
        catalog=catalog,
    )

    assert not catalog.table_exists(TEST_TABLE_IDENTIFIER)
    assert actual.metadata.model_dump() == expected.metadata.model_dump()


@pytest.mark.integration
@pytest.mark.parametrize("catalog,clean_up", [(pytest.lazy_fixture("session_catalog"), pytest.lazy_fixture("test_clean_up"))])
def test_rename_table_from_self_identifier_200(
    catalog: RestCatalog, clean_up: Any, table_schema_simple: Schema, table_metadata_no_snapshot_rest: Dict[str, Any]
) -> None:
    catalog.create_namespace(TEST_NAMESPACE_IDENTIFIER)

    original_table = catalog.create_table(TEST_TABLE_IDENTIFIER, table_schema_simple)

    actual = catalog.rename_table(original_table.name(), TEST_TABLE_IDENTIFIER_RENAME)

    expected_metadata = table_metadata_no_snapshot_rest["metadata"]
    expected_metadata["table-uuid"] = actual.metadata.model_dump()["table-uuid"]  # Note! Generated ID
    expected_metadata["last-updated-ms"] = actual.metadata.model_dump()["last-updated-ms"]  # Note! Generated TIMESTAMP

    expected = Table(
        identifier=TEST_TABLE_IDENTIFIER_RENAME,
        metadata_location=table_metadata_no_snapshot_rest["metadata-location"],
        metadata=TableMetadataV2(**expected_metadata),
        io=load_file_io(),
        catalog=catalog,
    )

    assert not catalog.table_exists(TEST_TABLE_IDENTIFIER)
    assert actual.metadata.model_dump() == expected.metadata.model_dump()


@pytest.mark.integration
@pytest.mark.parametrize("catalog,clean_up", [(pytest.lazy_fixture("session_catalog"), pytest.lazy_fixture("test_clean_up"))])
def test_create_table_missing_namespace(catalog: RestCatalog, clean_up: Any, table_schema_simple: Schema) -> None:
    assert not catalog.namespace_exists(TEST_NAMESPACE_IDENTIFIER)

    catalog.create_table(TEST_TABLE_IDENTIFIER, schema=table_schema_simple)
    assert catalog.table_exists(TEST_TABLE_IDENTIFIER)


@pytest.mark.integration
@pytest.mark.parametrize("catalog,clean_up", [(pytest.lazy_fixture("session_catalog"), pytest.lazy_fixture("test_clean_up"))])
def test_load_table_invalid_namespace(catalog: RestCatalog, clean_up: Any) -> None:
    table = "table"
    with pytest.raises(NoSuchIdentifierError) as e:
        catalog.load_table(table)
    assert f"Missing namespace or invalid identifier: {table}" in str(e.value)


@pytest.mark.integration
@pytest.mark.parametrize("catalog,clean_up", [(pytest.lazy_fixture("session_catalog"), pytest.lazy_fixture("test_clean_up"))])
def test_drop_table_invalid_namespace(catalog: RestCatalog, clean_up: Any) -> None:
    table = "table"
    with pytest.raises(NoSuchIdentifierError) as e:
        catalog.drop_table(table)
    assert f"Missing namespace or invalid identifier: {table}" in str(e.value)


@pytest.mark.integration
@pytest.mark.parametrize("catalog,clean_up", [(pytest.lazy_fixture("session_catalog"), pytest.lazy_fixture("test_clean_up"))])
def test_purge_table_invalid_namespace(catalog: RestCatalog, clean_up: Any) -> None:
    table = "table"
    with pytest.raises(NoSuchIdentifierError) as e:
        catalog.purge_table(table)
    assert f"Missing namespace or invalid identifier: {table}" in str(e.value)


@pytest.mark.integration
@pytest.mark.parametrize("catalog,clean_up", [(pytest.lazy_fixture("session_catalog"), pytest.lazy_fixture("test_clean_up"))])
def test_create_namespace_invalid_namespace(catalog: RestCatalog, clean_up: Any) -> None:
    with pytest.raises(NoSuchNamespaceError) as e:
        catalog.create_namespace(())
    assert "Empty namespace identifier" in str(e.value)


@pytest.mark.integration
@pytest.mark.parametrize("catalog,clean_up", [(pytest.lazy_fixture("session_catalog"), pytest.lazy_fixture("test_clean_up"))])
def test_drop_namespace_invalid_namespace(catalog: RestCatalog, clean_up: Any) -> None:
    with pytest.raises(NoSuchNamespaceError) as e:
        catalog.drop_namespace(())
    assert "Empty namespace identifier" in str(e.value)


@pytest.mark.integration
@pytest.mark.parametrize("catalog,clean_up", [(pytest.lazy_fixture("session_catalog"), pytest.lazy_fixture("test_clean_up"))])
def test_load_namespace_properties_invalid_namespace(catalog: RestCatalog, clean_up: Any) -> None:
    with pytest.raises(NoSuchNamespaceError) as e:
        catalog.load_namespace_properties(())
    assert "Empty namespace identifier" in str(e.value)


@pytest.mark.integration
@pytest.mark.parametrize("catalog,clean_up", [(pytest.lazy_fixture("session_catalog"), pytest.lazy_fixture("test_clean_up"))])
def test_update_namespace_properties_invalid_namespace(catalog: RestCatalog, clean_up: Any) -> None:
    with pytest.raises(NoSuchNamespaceError) as e:
        catalog.update_namespace_properties(())
    assert "Empty namespace identifier" in str(e.value)


# @pytest.mark.integration
# def test_request_session_with_ssl_ca_bundle() -> None:
#     catalog_properties = {
#         "uri": "http://localhost:8181",
#         "token": "some-jwt-token",
#         "ssl": {
#             "cabundle": "path_to_ca_bundle",
#         },
#     }
#     catalog = RestCatalog("rest", **catalog_properties)  # type: ignore
#     print(catalog.namespace_exists(TEST_NAMESPACE_IDENTIFIER))
#     with pytest.raises(NoSuchNamespaceError) as e:
#         RestCatalog("rest", **catalog_properties)  # type: ignore
#     assert "Empty namespace identifier" in str(e.value)


@pytest.mark.integration
@pytest.mark.parametrize("catalog,clean_up", [(pytest.lazy_fixture("session_catalog"), pytest.lazy_fixture("test_clean_up"))])
def test_request_session_with_ssl_client_cert(catalog: RestCatalog, clean_up: Any) -> None:
    catalog_properties = {
        "uri": "http://localhost:8181",
        "token": "some-jwt-token",
        "ssl": {
            "client": {
                "cert": "path_to_client_cert",
                "key": "path_to_client_key",
            }
        },
    }
    with pytest.raises(OSError) as e:
        RestCatalog("rest", **catalog_properties)  # type: ignore
    assert "Could not find the TLS certificate file, invalid path: path_to_client_cert" in str(e.value)


# @pytest.mark.integration
# @pytest.mark.parametrize("catalog,clean_up", [(pytest.lazy_fixture("session_catalog"), pytest.lazy_fixture("test_clean_up"))])
# def test_table_identifier_in_commit_table_request(
#     catalog: RestCatalog, clean_up: Any,table_schema_simple: Schema, table_metadata_no_snapshot_rest: Dict[str, Any]
# ) -> None:
#     catalog.create_namespace(TEST_NAMESPACE_IDENTIFIER)
#     catalog.create_table(TEST_TABLE_IDENTIFIER, schema=table_schema_simple)
#     table = Table(
#         identifier=TEST_TABLE_IDENTIFIER,
#         metadata=table_metadata_no_snapshot_rest,  # type:ignore
#         metadata_location=None,  # type:ignore
#         io=None,  # type:ignore
#         catalog=catalog,
#     )

#     actual = catalog.commit_table(table, (), ())
#     print("ACTUAL")
#     print(actual)
#     catalog._session.
#     assert not catalog.table_exists(TEST_TABLE_IDENTIFIER)

#     catalog.drop_table(TEST_TABLE_IDENTIFIER)
#     catalog.drop_namespace(TEST_NAMESPACE_IDENTIFIER)


@pytest.mark.integration
@pytest.mark.parametrize("catalog,clean_up", [(pytest.lazy_fixture("session_catalog"), pytest.lazy_fixture("test_clean_up"))])
def test_create_namespace_if_not_exists(catalog: RestCatalog, clean_up: Any) -> None:
    if catalog.namespace_exists(TEST_NAMESPACE_IDENTIFIER):
        catalog.drop_namespace(TEST_NAMESPACE_IDENTIFIER)

    catalog.create_namespace_if_not_exists(TEST_NAMESPACE_IDENTIFIER)

    assert catalog.namespace_exists(TEST_NAMESPACE_IDENTIFIER)


@pytest.mark.integration
@pytest.mark.parametrize("catalog,clean_up", [(pytest.lazy_fixture("session_catalog"), pytest.lazy_fixture("test_clean_up"))])
def test_create_namespace_if_already_existing(catalog: RestCatalog, clean_up: Any) -> None:
    if not catalog.namespace_exists(TEST_NAMESPACE_IDENTIFIER):
        catalog.create_namespace(TEST_NAMESPACE_IDENTIFIER)

    catalog.create_namespace_if_not_exists(TEST_NAMESPACE_IDENTIFIER)

    assert catalog.namespace_exists(TEST_NAMESPACE_IDENTIFIER)


@pytest.mark.integration
@pytest.mark.parametrize("catalog,clean_up", [(pytest.lazy_fixture("session_catalog"), pytest.lazy_fixture("test_clean_up"))])
def test_drop_view_invalid_namespace(catalog: RestCatalog, clean_up: Any) -> None:
    view = "view"

    with pytest.raises(NoSuchIdentifierError) as e:
        catalog.drop_view(view)

    assert f"Missing namespace or invalid identifier: {view}" in str(e.value)


@pytest.mark.integration
@pytest.mark.parametrize("catalog,clean_up", [(pytest.lazy_fixture("session_catalog"), pytest.lazy_fixture("test_clean_up"))])
def test_drop_view_404(catalog: RestCatalog, clean_up: Any) -> None:
    with pytest.raises(NoSuchViewError) as e:
        catalog.drop_view((TEST_NAMESPACE_IDENTIFIER[0], "NO_VIEW"))

    assert "View does not exist" in str(e.value)


# @pytest.mark.integration
# @pytest.mark.parametrize("catalog,clean_up", [(pytest.lazy_fixture("session_catalog"), pytest.lazy_fixture("test_clean_up"))])
# def test_drop_view_204(catalog: RestCatalog, clean_up: Any,table_schema_simple: Schema) -> None:
#     catalog.create_namespace(TEST_NAMESPACE_IDENTIFIER)
#     catalog.create_table(TEST_TABLE_IDENTIFIER)
#     print(catalog.list_views(TEST_NAMESPACE_IDENTIFIER))

#     assert False
#     catalog.drop_namespace(TEST_NAMESPACE_IDENTIFIER)
