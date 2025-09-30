#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.
# pylint:disable=redefined-outer-name


from pathlib import PosixPath
from typing import Union

import pyarrow as pa
import pytest
from pydantic_core import ValidationError
from pytest_lazyfixture import lazy_fixture

from pyiceberg.catalog import Catalog, load_catalog
from pyiceberg.catalog.memory import InMemoryCatalog
from pyiceberg.exceptions import (
    NamespaceAlreadyExistsError,
    NamespaceNotEmptyError,
    NoSuchNamespaceError,
    NoSuchTableError,
    TableAlreadyExistsError,
)
from pyiceberg.io import WAREHOUSE
from pyiceberg.io.pyarrow import schema_to_pyarrow
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table import (
    Table,
    TableProperties,
)
from pyiceberg.table.update import (
    AddSchemaUpdate,
    SetCurrentSchemaUpdate,
)
from pyiceberg.transforms import IdentityTransform
from pyiceberg.typedef import EMPTY_DICT, Properties
from pyiceberg.types import IntegerType, LongType, NestedField, StringType


@pytest.fixture
def catalog(tmp_path: PosixPath) -> InMemoryCatalog:
    return InMemoryCatalog("test.in_memory.catalog", **{WAREHOUSE: tmp_path.absolute().as_posix(), "test.key": "test.value"})


TEST_TABLE_IDENTIFIER = ("com", "organization", "department", "my_table")
TEST_TABLE_NAMESPACE = ("com", "organization", "department")
TEST_TABLE_NAME = "my_table"
TEST_TABLE_SCHEMA = Schema(
    NestedField(1, "x", LongType(), required=True),
    NestedField(2, "y", LongType(), doc="comment", required=True),
    NestedField(3, "z", LongType(), required=True),
)
TEST_TABLE_PARTITION_SPEC = PartitionSpec(PartitionField(name="x", transform=IdentityTransform(), source_id=1, field_id=1000))
TEST_TABLE_PROPERTIES = {"key1": "value1", "key2": "value2"}
NO_SUCH_TABLE_ERROR = "Table does not exist: com.organization.department.my_table"
TABLE_ALREADY_EXISTS_ERROR = "Table com.organization.department.my_table already exists"
NAMESPACE_ALREADY_EXISTS_ERROR = "Namespace \\('com', 'organization', 'department'\\) already exists"
# TODO: consolidate namespace error messages then remove this
DROP_NOT_EXISTING_NAMESPACE_ERROR = "Namespace does not exist: \\('com', 'organization', 'department'\\)"
NO_SUCH_NAMESPACE_ERROR = "Namespace com.organization.department does not exists"
NAMESPACE_NOT_EMPTY_ERROR = "Namespace com.organization.department is not empty"


def given_catalog_has_a_table(
    catalog: InMemoryCatalog,
    properties: Properties = EMPTY_DICT,
) -> Table:
    catalog.create_namespace(TEST_TABLE_NAMESPACE)
    return catalog.create_table(
        identifier=TEST_TABLE_IDENTIFIER,
        schema=TEST_TABLE_SCHEMA,
        partition_spec=TEST_TABLE_PARTITION_SPEC,
        properties=properties or TEST_TABLE_PROPERTIES,
    )


def test_load_catalog_in_memory() -> None:
    assert load_catalog("catalog", type="in-memory")


def test_load_catalog_impl_not_full_path() -> None:
    with pytest.raises(ValueError) as exc_info:
        load_catalog("catalog", **{"py-catalog-impl": "CustomCatalog"})

    assert "py-catalog-impl should be full path (module.CustomCatalog), got: CustomCatalog" in str(exc_info.value)


def test_load_catalog_impl_does_not_exist() -> None:
    with pytest.raises(ValueError) as exc_info:
        load_catalog("catalog", **{"py-catalog-impl": "pyiceberg.does.not.exist.Catalog"})

    assert "Could not initialize Catalog: pyiceberg.does.not.exist.Catalog" in str(exc_info.value)


def test_load_catalog_has_type_and_impl() -> None:
    with pytest.raises(ValueError) as exc_info:
        load_catalog("catalog", **{"py-catalog-impl": "pyiceberg.does.not.exist.Catalog", "type": "sql"})

    assert (
        "Must not set both catalog type and py-catalog-impl configurations, "
        "but found type sql and py-catalog-impl pyiceberg.does.not.exist.Catalog" in str(exc_info.value)
    )


def test_namespace_from_tuple() -> None:
    # Given
    identifier = ("com", "organization", "department", "my_table")
    # When
    namespace_from = Catalog.namespace_from(identifier)
    # Then
    assert namespace_from == ("com", "organization", "department")


def test_namespace_from_str() -> None:
    # Given
    identifier = "com.organization.department.my_table"
    # When
    namespace_from = Catalog.namespace_from(identifier)
    # Then
    assert namespace_from == ("com", "organization", "department")


def test_name_from_tuple() -> None:
    # Given
    identifier = ("com", "organization", "department", "my_table")
    # When
    name_from = Catalog.table_name_from(identifier)
    # Then
    assert name_from == "my_table"


def test_name_from_str() -> None:
    # Given
    identifier = "com.organization.department.my_table"
    # When
    name_from = Catalog.table_name_from(identifier)
    # Then
    assert name_from == "my_table"


def test_create_table(catalog: InMemoryCatalog) -> None:
    catalog.create_namespace(TEST_TABLE_NAMESPACE)
    table = catalog.create_table(
        identifier=TEST_TABLE_IDENTIFIER,
        schema=TEST_TABLE_SCHEMA,
        partition_spec=TEST_TABLE_PARTITION_SPEC,
        properties=TEST_TABLE_PROPERTIES,
    )
    assert catalog.load_table(TEST_TABLE_IDENTIFIER) == table


def test_create_table_location_override(catalog: InMemoryCatalog) -> None:
    new_location = f"{catalog._warehouse_location}/new_location"
    catalog.create_namespace(TEST_TABLE_NAMESPACE)
    table = catalog.create_table(
        identifier=TEST_TABLE_IDENTIFIER,
        schema=TEST_TABLE_SCHEMA,
        location=new_location,
        partition_spec=TEST_TABLE_PARTITION_SPEC,
        properties=TEST_TABLE_PROPERTIES,
    )
    assert catalog.load_table(TEST_TABLE_IDENTIFIER) == table
    assert table.location() == new_location


def test_create_table_removes_trailing_slash_from_location(catalog: InMemoryCatalog) -> None:
    new_location = f"{catalog._warehouse_location}/new_location"
    catalog.create_namespace(TEST_TABLE_NAMESPACE)
    table = catalog.create_table(
        identifier=TEST_TABLE_IDENTIFIER,
        schema=TEST_TABLE_SCHEMA,
        location=f"{new_location}/",
        partition_spec=TEST_TABLE_PARTITION_SPEC,
        properties=TEST_TABLE_PROPERTIES,
    )
    assert catalog.load_table(TEST_TABLE_IDENTIFIER) == table
    assert table.location() == new_location


@pytest.mark.parametrize(
    "schema,expected",
    [
        (lazy_fixture("pyarrow_schema_simple_without_ids"), lazy_fixture("iceberg_schema_simple_no_ids")),
        (lazy_fixture("iceberg_schema_simple"), lazy_fixture("iceberg_schema_simple")),
        (lazy_fixture("iceberg_schema_nested"), lazy_fixture("iceberg_schema_nested")),
        (lazy_fixture("pyarrow_schema_nested_without_ids"), lazy_fixture("iceberg_schema_nested_no_ids")),
    ],
)
def test_convert_schema_if_needed(
    schema: Union[Schema, pa.Schema],
    expected: Schema,
    catalog: InMemoryCatalog,
) -> None:
    assert expected == catalog._convert_schema_if_needed(schema)


def test_create_table_pyarrow_schema(catalog: InMemoryCatalog, pyarrow_schema_simple_without_ids: pa.Schema) -> None:
    catalog.create_namespace(TEST_TABLE_NAMESPACE)
    table = catalog.create_table(
        identifier=TEST_TABLE_IDENTIFIER,
        schema=pyarrow_schema_simple_without_ids,
        properties=TEST_TABLE_PROPERTIES,
    )
    assert catalog.load_table(TEST_TABLE_IDENTIFIER) == table


def test_create_table_raises_error_when_table_already_exists(catalog: InMemoryCatalog) -> None:
    # Given
    given_catalog_has_a_table(catalog)
    # When
    with pytest.raises(TableAlreadyExistsError, match=TABLE_ALREADY_EXISTS_ERROR):
        catalog.create_table(
            identifier=TEST_TABLE_IDENTIFIER,
            schema=TEST_TABLE_SCHEMA,
        )


def test_load_table(catalog: InMemoryCatalog) -> None:
    # Given
    given_table = given_catalog_has_a_table(catalog)
    # When
    table = catalog.load_table(TEST_TABLE_IDENTIFIER)
    # Then
    assert table == given_table


def test_load_table_from_self_identifier(catalog: InMemoryCatalog) -> None:
    # Given
    given_table = given_catalog_has_a_table(catalog)
    # When
    intermediate = catalog.load_table(TEST_TABLE_IDENTIFIER)
    table = catalog.load_table(intermediate._identifier)
    # Then
    assert table == given_table


def test_table_raises_error_on_table_not_found(catalog: InMemoryCatalog) -> None:
    with pytest.raises(NoSuchTableError, match=NO_SUCH_TABLE_ERROR):
        catalog.load_table(TEST_TABLE_IDENTIFIER)


def test_table_exists(catalog: InMemoryCatalog) -> None:
    # Given
    given_catalog_has_a_table(catalog)
    # Then
    assert catalog.table_exists(TEST_TABLE_IDENTIFIER)


def test_table_exists_on_table_not_found(catalog: InMemoryCatalog) -> None:
    assert not catalog.table_exists(TEST_TABLE_IDENTIFIER)


def test_drop_table(catalog: InMemoryCatalog) -> None:
    # Given
    given_catalog_has_a_table(catalog)
    # When
    catalog.drop_table(TEST_TABLE_IDENTIFIER)
    # Then
    with pytest.raises(NoSuchTableError, match=NO_SUCH_TABLE_ERROR):
        catalog.load_table(TEST_TABLE_IDENTIFIER)


def test_drop_table_from_self_identifier(catalog: InMemoryCatalog) -> None:
    # Given
    table = given_catalog_has_a_table(catalog)
    # When
    catalog.drop_table(table._identifier)
    # Then
    with pytest.raises(NoSuchTableError, match=NO_SUCH_TABLE_ERROR):
        catalog.load_table(table._identifier)
    with pytest.raises(NoSuchTableError, match=NO_SUCH_TABLE_ERROR):
        catalog.load_table(TEST_TABLE_IDENTIFIER)


def test_drop_table_that_does_not_exist_raise_error(catalog: InMemoryCatalog) -> None:
    with pytest.raises(NoSuchTableError, match=NO_SUCH_TABLE_ERROR):
        catalog.load_table(TEST_TABLE_IDENTIFIER)


def test_purge_table(catalog: InMemoryCatalog) -> None:
    # Given
    given_catalog_has_a_table(catalog)
    # When
    catalog.purge_table(TEST_TABLE_IDENTIFIER)
    # Then
    with pytest.raises(NoSuchTableError, match=NO_SUCH_TABLE_ERROR):
        catalog.load_table(TEST_TABLE_IDENTIFIER)


def test_rename_table(catalog: InMemoryCatalog) -> None:
    # Given
    given_catalog_has_a_table(catalog)

    # When
    new_table = "new.namespace.new_table"
    catalog.create_namespace(("new", "namespace"))
    table = catalog.rename_table(TEST_TABLE_IDENTIFIER, new_table)

    # Then
    assert table._identifier == Catalog.identifier_to_tuple(new_table)

    # And
    table = catalog.load_table(new_table)
    assert table._identifier == Catalog.identifier_to_tuple(new_table)

    # And
    assert catalog._namespace_exists(table._identifier[:-1])

    # And
    with pytest.raises(NoSuchTableError, match=NO_SUCH_TABLE_ERROR):
        catalog.load_table(TEST_TABLE_IDENTIFIER)


def test_rename_table_from_self_identifier(catalog: InMemoryCatalog) -> None:
    # Given
    table = given_catalog_has_a_table(catalog)

    # When
    new_table_name = "new.namespace.new_table"
    catalog.create_namespace(("new", "namespace"))
    new_table = catalog.rename_table(table._identifier, new_table_name)

    # Then
    assert new_table._identifier == Catalog.identifier_to_tuple(new_table_name)

    # And
    new_table = catalog.load_table(new_table._identifier)
    assert new_table._identifier == Catalog.identifier_to_tuple(new_table_name)

    # And
    assert catalog._namespace_exists(new_table._identifier[:-1])

    # And
    with pytest.raises(NoSuchTableError, match=NO_SUCH_TABLE_ERROR):
        catalog.load_table(table._identifier)
    with pytest.raises(NoSuchTableError, match=NO_SUCH_TABLE_ERROR):
        catalog.load_table(TEST_TABLE_IDENTIFIER)


def test_create_namespace(catalog: InMemoryCatalog) -> None:
    # When
    catalog.create_namespace(TEST_TABLE_NAMESPACE, TEST_TABLE_PROPERTIES)

    # Then
    assert catalog._namespace_exists(TEST_TABLE_NAMESPACE)
    assert TEST_TABLE_PROPERTIES == catalog.load_namespace_properties(TEST_TABLE_NAMESPACE)


def test_create_namespace_raises_error_on_existing_namespace(catalog: InMemoryCatalog) -> None:
    # Given
    catalog.create_namespace(TEST_TABLE_NAMESPACE, TEST_TABLE_PROPERTIES)
    # When
    with pytest.raises(NamespaceAlreadyExistsError, match=NAMESPACE_ALREADY_EXISTS_ERROR):
        catalog.create_namespace(TEST_TABLE_NAMESPACE, TEST_TABLE_PROPERTIES)


def test_get_namespace_metadata_raises_error_when_namespace_does_not_exist(catalog: InMemoryCatalog) -> None:
    with pytest.raises(NoSuchNamespaceError, match=NO_SUCH_NAMESPACE_ERROR):
        catalog.load_namespace_properties(TEST_TABLE_NAMESPACE)


def test_list_namespaces(catalog: InMemoryCatalog) -> None:
    # Given
    catalog.create_namespace(TEST_TABLE_NAMESPACE, TEST_TABLE_PROPERTIES)
    # When
    namespaces = catalog.list_namespaces()
    # Then
    assert TEST_TABLE_NAMESPACE[:1] in namespaces

    # When
    namespaces = catalog.list_namespaces(TEST_TABLE_NAMESPACE)
    # Then
    assert not namespaces


def test_drop_namespace(catalog: InMemoryCatalog) -> None:
    # Given
    catalog.create_namespace(TEST_TABLE_NAMESPACE, TEST_TABLE_PROPERTIES)
    # When
    catalog.drop_namespace(TEST_TABLE_NAMESPACE)
    # Then
    assert not catalog._namespace_exists(TEST_TABLE_NAMESPACE)


def test_drop_namespace_raises_error_when_namespace_does_not_exist(catalog: InMemoryCatalog) -> None:
    with pytest.raises(NoSuchNamespaceError, match=DROP_NOT_EXISTING_NAMESPACE_ERROR):
        catalog.drop_namespace(TEST_TABLE_NAMESPACE)


def test_drop_namespace_raises_error_when_namespace_not_empty(catalog: InMemoryCatalog) -> None:
    # Given
    given_catalog_has_a_table(catalog)
    # When
    with pytest.raises(NamespaceNotEmptyError, match=NAMESPACE_NOT_EMPTY_ERROR):
        catalog.drop_namespace(TEST_TABLE_NAMESPACE)


def test_list_tables(catalog: InMemoryCatalog) -> None:
    # Given
    given_catalog_has_a_table(catalog)
    # When
    tables = catalog.list_tables(namespace=TEST_TABLE_NAMESPACE)
    # Then
    assert tables
    assert TEST_TABLE_IDENTIFIER in tables


def test_list_tables_under_a_namespace(catalog: InMemoryCatalog) -> None:
    # Given
    given_catalog_has_a_table(catalog)
    new_namespace = ("new", "namespace")
    catalog.create_namespace(new_namespace)
    # When
    all_tables = catalog.list_tables(namespace=TEST_TABLE_NAMESPACE)
    new_namespace_tables = catalog.list_tables(new_namespace)
    # Then
    assert all_tables
    assert TEST_TABLE_IDENTIFIER in all_tables
    assert new_namespace_tables == []


def test_update_namespace_metadata(catalog: InMemoryCatalog) -> None:
    # Given
    catalog.create_namespace(TEST_TABLE_NAMESPACE, TEST_TABLE_PROPERTIES)

    # When
    new_metadata = {"key3": "value3", "key4": "value4"}
    summary = catalog.update_namespace_properties(TEST_TABLE_NAMESPACE, updates=new_metadata)

    # Then
    assert catalog._namespace_exists(TEST_TABLE_NAMESPACE)
    assert new_metadata.items() <= catalog.load_namespace_properties(TEST_TABLE_NAMESPACE).items()
    assert summary.removed == []
    assert sorted(summary.updated) == ["key3", "key4"]
    assert summary.missing == []


def test_update_namespace_metadata_removals(catalog: InMemoryCatalog) -> None:
    # Given
    catalog.create_namespace(TEST_TABLE_NAMESPACE, TEST_TABLE_PROPERTIES)

    # When
    new_metadata = {"key3": "value3", "key4": "value4"}
    remove_metadata = {"key1"}
    summary = catalog.update_namespace_properties(TEST_TABLE_NAMESPACE, remove_metadata, new_metadata)

    # Then
    assert catalog._namespace_exists(TEST_TABLE_NAMESPACE)
    assert new_metadata.items() <= catalog.load_namespace_properties(TEST_TABLE_NAMESPACE).items()
    assert remove_metadata.isdisjoint(catalog.load_namespace_properties(TEST_TABLE_NAMESPACE).keys())
    assert summary.removed == ["key1"]
    assert sorted(summary.updated) == ["key3", "key4"]
    assert summary.missing == []


def test_update_namespace_metadata_raises_error_when_namespace_does_not_exist(catalog: InMemoryCatalog) -> None:
    with pytest.raises(NoSuchNamespaceError, match=NO_SUCH_NAMESPACE_ERROR):
        catalog.update_namespace_properties(TEST_TABLE_NAMESPACE, updates=TEST_TABLE_PROPERTIES)


def test_commit_table(catalog: InMemoryCatalog) -> None:
    # Given
    given_table = given_catalog_has_a_table(catalog)
    new_schema = Schema(
        NestedField(1, "x", LongType()),
        NestedField(2, "y", LongType(), doc="comment"),
        NestedField(3, "z", LongType()),
        NestedField(4, "add", LongType()),
    )

    # When
    response = given_table.catalog.commit_table(
        given_table,
        updates=(
            AddSchemaUpdate(schema=new_schema),
            SetCurrentSchemaUpdate(schema_id=-1),
        ),
        requirements=(),
    )

    # Then
    assert response.metadata.table_uuid == given_table.metadata.table_uuid
    assert len(response.metadata.schemas) == 2
    assert response.metadata.schemas[1] == new_schema
    assert response.metadata.current_schema_id == new_schema.schema_id


def test_add_column(catalog: InMemoryCatalog) -> None:
    given_table = given_catalog_has_a_table(catalog)

    given_table.update_schema().add_column(path="new_column1", field_type=IntegerType()).commit()

    assert given_table.schema() == Schema(
        NestedField(field_id=1, name="x", field_type=LongType(), required=True),
        NestedField(field_id=2, name="y", field_type=LongType(), required=True, doc="comment"),
        NestedField(field_id=3, name="z", field_type=LongType(), required=True),
        NestedField(field_id=4, name="new_column1", field_type=IntegerType(), required=False),
        identifier_field_ids=[],
    )
    assert given_table.schema().schema_id == 1

    transaction = given_table.transaction()
    transaction.update_schema().add_column(path="new_column2", field_type=IntegerType(), doc="doc").commit()
    transaction.commit_transaction()

    assert given_table.schema() == Schema(
        NestedField(field_id=1, name="x", field_type=LongType(), required=True),
        NestedField(field_id=2, name="y", field_type=LongType(), required=True, doc="comment"),
        NestedField(field_id=3, name="z", field_type=LongType(), required=True),
        NestedField(field_id=4, name="new_column1", field_type=IntegerType(), required=False),
        NestedField(field_id=5, name="new_column2", field_type=IntegerType(), required=False, doc="doc"),
        identifier_field_ids=[],
    )
    assert given_table.schema().schema_id == 2


def test_add_column_with_statement(catalog: InMemoryCatalog) -> None:
    given_table = given_catalog_has_a_table(catalog)

    with given_table.update_schema() as tx:
        tx.add_column(path="new_column1", field_type=IntegerType())

    assert given_table.schema() == Schema(
        NestedField(field_id=1, name="x", field_type=LongType(), required=True),
        NestedField(field_id=2, name="y", field_type=LongType(), required=True, doc="comment"),
        NestedField(field_id=3, name="z", field_type=LongType(), required=True),
        NestedField(field_id=4, name="new_column1", field_type=IntegerType(), required=False),
        identifier_field_ids=[],
    )
    assert given_table.schema().schema_id == 1

    with given_table.transaction() as tx:
        tx.update_schema().add_column(path="new_column2", field_type=IntegerType(), doc="doc").commit()

    assert given_table.schema() == Schema(
        NestedField(field_id=1, name="x", field_type=LongType(), required=True),
        NestedField(field_id=2, name="y", field_type=LongType(), required=True, doc="comment"),
        NestedField(field_id=3, name="z", field_type=LongType(), required=True),
        NestedField(field_id=4, name="new_column1", field_type=IntegerType(), required=False),
        NestedField(field_id=5, name="new_column2", field_type=IntegerType(), required=False, doc="doc"),
        identifier_field_ids=[],
    )
    assert given_table.schema().schema_id == 2


def test_catalog_repr(catalog: InMemoryCatalog) -> None:
    s = repr(catalog)
    assert s == "test.in_memory.catalog (<class 'pyiceberg.catalog.memory.InMemoryCatalog'>)"


def test_table_properties_int_value(catalog: InMemoryCatalog) -> None:
    # table properties can be set to int, but still serialized to string
    property_with_int = {"property_name": 42}
    given_table = given_catalog_has_a_table(catalog, properties=property_with_int)
    assert isinstance(given_table.properties["property_name"], str)


def test_table_properties_raise_for_none_value(catalog: InMemoryCatalog) -> None:
    property_with_none = {"property_name": None}
    with pytest.raises(ValidationError) as exc_info:
        _ = given_catalog_has_a_table(catalog, properties=property_with_none)
    assert "None type is not a supported value in properties: property_name" in str(exc_info.value)


def test_table_writes_metadata_to_custom_location(catalog: InMemoryCatalog) -> None:
    metadata_path = f"{catalog._warehouse_location}/custom/path"
    catalog.create_namespace(TEST_TABLE_NAMESPACE)
    table = catalog.create_table(
        identifier=TEST_TABLE_IDENTIFIER,
        schema=TEST_TABLE_SCHEMA,
        partition_spec=TEST_TABLE_PARTITION_SPEC,
        properties={TableProperties.WRITE_METADATA_PATH: metadata_path},
    )
    df = pa.Table.from_pylist([{"x": 123, "y": 456, "z": 789}], schema=schema_to_pyarrow(TEST_TABLE_SCHEMA))
    table.append(df)
    manifests = table.current_snapshot().manifests(table.io)  # type: ignore
    location_provider = table.location_provider()

    assert location_provider.new_metadata_location("").startswith(metadata_path)
    assert manifests[0].manifest_path.startswith(metadata_path)
    assert table.location() != metadata_path
    assert table.metadata_location.startswith(metadata_path)


def test_table_writes_metadata_to_default_path(catalog: InMemoryCatalog) -> None:
    catalog.create_namespace(TEST_TABLE_NAMESPACE)
    table = catalog.create_table(
        identifier=TEST_TABLE_IDENTIFIER,
        schema=TEST_TABLE_SCHEMA,
        partition_spec=TEST_TABLE_PARTITION_SPEC,
        properties=TEST_TABLE_PROPERTIES,
    )
    metadata_path = f"{table.location()}/metadata"
    df = pa.Table.from_pylist([{"x": 123, "y": 456, "z": 789}], schema=schema_to_pyarrow(TEST_TABLE_SCHEMA))
    table.append(df)
    manifests = table.current_snapshot().manifests(table.io)  # type: ignore
    location_provider = table.location_provider()

    assert location_provider.new_metadata_location("").startswith(metadata_path)
    assert manifests[0].manifest_path.startswith(metadata_path)
    assert table.metadata_location.startswith(metadata_path)


def test_table_metadata_writes_reflect_latest_path(catalog: InMemoryCatalog) -> None:
    catalog.create_namespace(TEST_TABLE_NAMESPACE)
    table = catalog.create_table(
        identifier=TEST_TABLE_IDENTIFIER,
        schema=TEST_TABLE_SCHEMA,
        partition_spec=TEST_TABLE_PARTITION_SPEC,
    )

    initial_metadata_path = f"{table.location()}/metadata"
    assert table.location_provider().new_metadata_location("metadata.json") == f"{initial_metadata_path}/metadata.json"

    # update table with new path for metadata
    new_metadata_path = f"{table.location()}/custom/path"
    table.transaction().set_properties({TableProperties.WRITE_METADATA_PATH: new_metadata_path}).commit_transaction()

    assert table.location_provider().new_metadata_location("metadata.json") == f"{new_metadata_path}/metadata.json"


class TestCatalogClose:
    """Test catalog close functionality."""

    def test_in_memory_catalog_close(self, catalog: InMemoryCatalog) -> None:
        """Test that InMemoryCatalog close method works."""
        # Should not raise any exception
        catalog.close()

    def test_in_memory_catalog_context_manager(self, catalog: InMemoryCatalog) -> None:
        """Test that InMemoryCatalog works as a context manager."""
        with InMemoryCatalog("test") as cat:
            assert cat.name == "test"
            # Create a namespace and table to test functionality
            cat.create_namespace("test_db")
            schema = Schema(NestedField(1, "name", StringType(), required=True))
            cat.create_table(("test_db", "test_table"), schema)

        # InMemoryCatalog inherits close from SqlCatalog, so engine should be disposed
        assert hasattr(cat, "engine")
