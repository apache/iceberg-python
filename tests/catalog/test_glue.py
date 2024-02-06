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
from typing import Any, Dict, List
from unittest import mock

import boto3
import pyarrow as pa
import pytest
from moto import mock_aws

from pyiceberg.catalog.glue import GlueCatalog
from pyiceberg.exceptions import (
    NamespaceAlreadyExistsError,
    NamespaceNotEmptyError,
    NoSuchIcebergTableError,
    NoSuchNamespaceError,
    NoSuchPropertyException,
    NoSuchTableError,
    TableAlreadyExistsError,
)
from pyiceberg.schema import Schema
from pyiceberg.types import IntegerType
from tests.conftest import BUCKET_NAME, TABLE_METADATA_LOCATION_REGEX


@mock_aws
def test_create_table_with_database_location(
    _glue: boto3.client,
    _bucket_initialize: None,
    moto_endpoint_url: str,
    table_schema_nested: Schema,
    database_name: str,
    table_name: str,
) -> None:
    catalog_name = "glue"
    identifier = (database_name, table_name)
    test_catalog = GlueCatalog(catalog_name, **{"s3.endpoint": moto_endpoint_url})
    test_catalog.create_namespace(namespace=database_name, properties={"location": f"s3://{BUCKET_NAME}/{database_name}.db"})
    table = test_catalog.create_table(identifier, table_schema_nested)
    assert table.identifier == (catalog_name,) + identifier
    assert TABLE_METADATA_LOCATION_REGEX.match(table.metadata_location)
    assert test_catalog._parse_metadata_version(table.metadata_location) == 0

    # Ensure schema is also pushed to Glue
    table_info = _glue.get_table(
        DatabaseName=database_name,
        Name=table_name,
    )
    storage_descriptor = table_info["Table"]["StorageDescriptor"]
    columns = storage_descriptor["Columns"]
    assert len(columns) == len(table_schema_nested.fields)
    assert columns[0] == {
        "Name": "foo",
        "Type": "string",
        "Parameters": {"iceberg.field.id": "1", "iceberg.field.optional": "true", "iceberg.field.current": "true"},
    }

    assert storage_descriptor["Location"] == f"s3://{BUCKET_NAME}/{database_name}.db/{table_name}"


@mock_aws
def test_create_table_with_default_warehouse(
    _bucket_initialize: None, moto_endpoint_url: str, table_schema_nested: Schema, database_name: str, table_name: str
) -> None:
    catalog_name = "glue"
    identifier = (database_name, table_name)
    test_catalog = GlueCatalog(catalog_name, **{"s3.endpoint": moto_endpoint_url, "warehouse": f"s3://{BUCKET_NAME}"})
    test_catalog.create_namespace(namespace=database_name)
    table = test_catalog.create_table(identifier, table_schema_nested)
    assert table.identifier == (catalog_name,) + identifier
    assert TABLE_METADATA_LOCATION_REGEX.match(table.metadata_location)
    assert test_catalog._parse_metadata_version(table.metadata_location) == 0


@mock_aws
def test_create_table_with_given_location(
    _bucket_initialize: None, moto_endpoint_url: str, table_schema_nested: Schema, database_name: str, table_name: str
) -> None:
    catalog_name = "glue"
    identifier = (database_name, table_name)
    test_catalog = GlueCatalog(catalog_name, **{"s3.endpoint": moto_endpoint_url})
    test_catalog.create_namespace(namespace=database_name)
    table = test_catalog.create_table(
        identifier=identifier, schema=table_schema_nested, location=f"s3://{BUCKET_NAME}/{database_name}.db/{table_name}"
    )
    assert table.identifier == (catalog_name,) + identifier
    assert TABLE_METADATA_LOCATION_REGEX.match(table.metadata_location)
    assert test_catalog._parse_metadata_version(table.metadata_location) == 0


@mock_aws
def test_create_table_with_pyarrow_schema(
    _bucket_initialize: None,
    moto_endpoint_url: str,
    pyarrow_schema_simple_without_ids: pa.Schema,
    database_name: str,
    table_name: str,
) -> None:
    catalog_name = "glue"
    identifier = (database_name, table_name)
    test_catalog = GlueCatalog(catalog_name, **{"s3.endpoint": moto_endpoint_url})
    test_catalog.create_namespace(namespace=database_name)
    table = test_catalog.create_table(
        identifier=identifier,
        schema=pyarrow_schema_simple_without_ids,
        location=f"s3://{BUCKET_NAME}/{database_name}.db/{table_name}",
    )
    assert table.identifier == (catalog_name,) + identifier
    assert TABLE_METADATA_LOCATION_REGEX.match(table.metadata_location)
    assert test_catalog._parse_metadata_version(table.metadata_location) == 0


@mock_aws
def test_create_table_with_no_location(
    _bucket_initialize: None, moto_endpoint_url: str, table_schema_nested: Schema, database_name: str, table_name: str
) -> None:
    catalog_name = "glue"
    identifier = (database_name, table_name)
    test_catalog = GlueCatalog(catalog_name, **{"s3.endpoint": moto_endpoint_url})
    test_catalog.create_namespace(namespace=database_name)
    with pytest.raises(ValueError):
        test_catalog.create_table(identifier=identifier, schema=table_schema_nested)


@mock_aws
def test_create_table_with_strips(
    _bucket_initialize: None, moto_endpoint_url: str, table_schema_nested: Schema, database_name: str, table_name: str
) -> None:
    catalog_name = "glue"
    identifier = (database_name, table_name)
    test_catalog = GlueCatalog(catalog_name, **{"s3.endpoint": moto_endpoint_url})
    test_catalog.create_namespace(namespace=database_name, properties={"location": f"s3://{BUCKET_NAME}/{database_name}.db/"})
    table = test_catalog.create_table(identifier, table_schema_nested)
    assert table.identifier == (catalog_name,) + identifier
    assert TABLE_METADATA_LOCATION_REGEX.match(table.metadata_location)
    assert test_catalog._parse_metadata_version(table.metadata_location) == 0


@mock_aws
def test_create_table_with_strips_bucket_root(
    _bucket_initialize: None, moto_endpoint_url: str, table_schema_nested: Schema, database_name: str, table_name: str
) -> None:
    catalog_name = "glue"
    identifier = (database_name, table_name)
    test_catalog = GlueCatalog("glue", **{"s3.endpoint": moto_endpoint_url, "warehouse": f"s3://{BUCKET_NAME}/"})
    test_catalog.create_namespace(namespace=database_name)
    table_strip = test_catalog.create_table(identifier, table_schema_nested)
    assert table_strip.identifier == (catalog_name,) + identifier
    assert TABLE_METADATA_LOCATION_REGEX.match(table_strip.metadata_location)
    assert test_catalog._parse_metadata_version(table_strip.metadata_location) == 0


@mock_aws
def test_create_table_with_no_database(
    _bucket_initialize: None, moto_endpoint_url: str, table_schema_nested: Schema, database_name: str, table_name: str
) -> None:
    identifier = (database_name, table_name)
    test_catalog = GlueCatalog("glue", **{"s3.endpoint": moto_endpoint_url})
    with pytest.raises(NoSuchNamespaceError):
        test_catalog.create_table(identifier=identifier, schema=table_schema_nested)


@mock_aws
def test_create_duplicated_table(
    _bucket_initialize: None, moto_endpoint_url: str, table_schema_nested: Schema, database_name: str, table_name: str
) -> None:
    identifier = (database_name, table_name)
    test_catalog = GlueCatalog("glue", **{"s3.endpoint": moto_endpoint_url, "warehouse": f"s3://{BUCKET_NAME}/"})
    test_catalog.create_namespace(namespace=database_name)
    test_catalog.create_table(identifier, table_schema_nested)
    with pytest.raises(TableAlreadyExistsError):
        test_catalog.create_table(identifier, table_schema_nested)


@mock_aws
def test_load_table(
    _bucket_initialize: None, moto_endpoint_url: str, table_schema_nested: Schema, database_name: str, table_name: str
) -> None:
    catalog_name = "glue"
    identifier = (database_name, table_name)
    test_catalog = GlueCatalog(catalog_name, **{"s3.endpoint": moto_endpoint_url, "warehouse": f"s3://{BUCKET_NAME}/"})
    test_catalog.create_namespace(namespace=database_name)
    test_catalog.create_table(identifier, table_schema_nested)
    table = test_catalog.load_table(identifier)
    assert table.identifier == (catalog_name,) + identifier
    assert TABLE_METADATA_LOCATION_REGEX.match(table.metadata_location)
    assert test_catalog._parse_metadata_version(table.metadata_location) == 0


@mock_aws
def test_load_table_from_self_identifier(
    _bucket_initialize: None, moto_endpoint_url: str, table_schema_nested: Schema, database_name: str, table_name: str
) -> None:
    catalog_name = "glue"
    identifier = (database_name, table_name)
    test_catalog = GlueCatalog(catalog_name, **{"s3.endpoint": moto_endpoint_url, "warehouse": f"s3://{BUCKET_NAME}/"})
    test_catalog.create_namespace(namespace=database_name)
    intermediate = test_catalog.create_table(identifier, table_schema_nested)
    table = test_catalog.load_table(intermediate.identifier)
    assert table.identifier == (catalog_name,) + identifier
    assert TABLE_METADATA_LOCATION_REGEX.match(table.metadata_location)


@mock_aws
def test_load_non_exist_table(_bucket_initialize: None, moto_endpoint_url: str, database_name: str, table_name: str) -> None:
    identifier = (database_name, table_name)
    test_catalog = GlueCatalog("glue", **{"s3.endpoint": moto_endpoint_url, "warehouse": f"s3://{BUCKET_NAME}/"})
    test_catalog.create_namespace(namespace=database_name)
    with pytest.raises(NoSuchTableError):
        test_catalog.load_table(identifier)


@mock_aws
def test_drop_table(
    _bucket_initialize: None, moto_endpoint_url: str, table_schema_nested: Schema, database_name: str, table_name: str
) -> None:
    catalog_name = "glue"
    identifier = (database_name, table_name)
    test_catalog = GlueCatalog(catalog_name, **{"s3.endpoint": moto_endpoint_url, "warehouse": f"s3://{BUCKET_NAME}/"})
    test_catalog.create_namespace(namespace=database_name)
    test_catalog.create_table(identifier, table_schema_nested)
    table = test_catalog.load_table(identifier)
    assert table.identifier == (catalog_name,) + identifier
    assert TABLE_METADATA_LOCATION_REGEX.match(table.metadata_location)
    test_catalog.drop_table(identifier)
    with pytest.raises(NoSuchTableError):
        test_catalog.load_table(identifier)


@mock_aws
def test_drop_table_from_self_identifier(
    _bucket_initialize: None, moto_endpoint_url: str, table_schema_nested: Schema, database_name: str, table_name: str
) -> None:
    catalog_name = "glue"
    identifier = (database_name, table_name)
    test_catalog = GlueCatalog(catalog_name, **{"s3.endpoint": moto_endpoint_url, "warehouse": f"s3://{BUCKET_NAME}/"})
    test_catalog.create_namespace(namespace=database_name)
    test_catalog.create_table(identifier, table_schema_nested)
    table = test_catalog.load_table(identifier)
    assert table.identifier == (catalog_name,) + identifier
    assert TABLE_METADATA_LOCATION_REGEX.match(table.metadata_location)
    test_catalog.drop_table(table.identifier)
    with pytest.raises(NoSuchTableError):
        test_catalog.load_table(identifier)
    with pytest.raises(NoSuchTableError):
        test_catalog.load_table(table.identifier)


@mock_aws
def test_drop_non_exist_table(_bucket_initialize: None, moto_endpoint_url: str, database_name: str, table_name: str) -> None:
    identifier = (database_name, table_name)
    test_catalog = GlueCatalog("glue", **{"s3.endpoint": moto_endpoint_url, "warehouse": f"s3://{BUCKET_NAME}/"})
    with pytest.raises(NoSuchTableError):
        test_catalog.drop_table(identifier)


@mock_aws
def test_rename_table(
    _bucket_initialize: None, moto_endpoint_url: str, table_schema_nested: Schema, database_name: str, table_name: str
) -> None:
    catalog_name = "glue"
    new_table_name = f"{table_name}_new"
    identifier = (database_name, table_name)
    new_identifier = (database_name, new_table_name)
    test_catalog = GlueCatalog("glue", **{"s3.endpoint": moto_endpoint_url, "warehouse": f"s3://{BUCKET_NAME}/"})
    test_catalog.create_namespace(namespace=database_name)
    table = test_catalog.create_table(identifier, table_schema_nested)
    assert table.identifier == (catalog_name,) + identifier
    assert TABLE_METADATA_LOCATION_REGEX.match(table.metadata_location)
    assert test_catalog._parse_metadata_version(table.metadata_location) == 0
    test_catalog.rename_table(identifier, new_identifier)
    new_table = test_catalog.load_table(new_identifier)
    assert new_table.identifier == (catalog_name,) + new_identifier
    # the metadata_location should not change
    assert new_table.metadata_location == table.metadata_location
    # old table should be dropped
    with pytest.raises(NoSuchTableError):
        test_catalog.load_table(identifier)


@mock_aws
def test_rename_table_from_self_identifier(
    _bucket_initialize: None, moto_endpoint_url: str, table_schema_nested: Schema, database_name: str, table_name: str
) -> None:
    catalog_name = "glue"
    new_table_name = f"{table_name}_new"
    identifier = (database_name, table_name)
    new_identifier = (database_name, new_table_name)
    test_catalog = GlueCatalog("glue", **{"s3.endpoint": moto_endpoint_url, "warehouse": f"s3://{BUCKET_NAME}/"})
    test_catalog.create_namespace(namespace=database_name)
    table = test_catalog.create_table(identifier, table_schema_nested)
    assert table.identifier == (catalog_name,) + identifier
    assert TABLE_METADATA_LOCATION_REGEX.match(table.metadata_location)
    test_catalog.rename_table(table.identifier, new_identifier)
    new_table = test_catalog.load_table(new_identifier)
    assert new_table.identifier == (catalog_name,) + new_identifier
    # the metadata_location should not change
    assert new_table.metadata_location == table.metadata_location
    # old table should be dropped
    with pytest.raises(NoSuchTableError):
        test_catalog.load_table(identifier)
    with pytest.raises(NoSuchTableError):
        test_catalog.load_table(table.identifier)


@mock_aws
def test_rename_table_no_params(
    _glue: boto3.client, _bucket_initialize: None, moto_endpoint_url: str, database_name: str, table_name: str
) -> None:
    new_database_name = f"{database_name}_new"
    new_table_name = f"{table_name}_new"
    identifier = (database_name, table_name)
    new_identifier = (new_database_name, new_table_name)
    test_catalog = GlueCatalog("glue", **{"s3.endpoint": moto_endpoint_url, "warehouse": f"s3://{BUCKET_NAME}/"})
    test_catalog.create_namespace(namespace=database_name)
    test_catalog.create_namespace(namespace=new_database_name)
    _glue.create_table(
        DatabaseName=database_name,
        TableInput={"Name": table_name, "TableType": "EXTERNAL_TABLE", "Parameters": {"table_type": "iceberg"}},
    )
    with pytest.raises(NoSuchPropertyException):
        test_catalog.rename_table(identifier, new_identifier)


@mock_aws
def test_rename_non_iceberg_table(
    _glue: boto3.client, _bucket_initialize: None, moto_endpoint_url: str, database_name: str, table_name: str
) -> None:
    new_database_name = f"{database_name}_new"
    new_table_name = f"{table_name}_new"
    identifier = (database_name, table_name)
    new_identifier = (new_database_name, new_table_name)
    test_catalog = GlueCatalog("glue", **{"s3.endpoint": moto_endpoint_url, "warehouse": f"s3://{BUCKET_NAME}/"})
    test_catalog.create_namespace(namespace=database_name)
    test_catalog.create_namespace(namespace=new_database_name)
    _glue.create_table(
        DatabaseName=database_name,
        TableInput={
            "Name": table_name,
            "TableType": "EXTERNAL_TABLE",
            "Parameters": {"table_type": "noniceberg", "metadata_location": "test"},
        },
    )
    with pytest.raises(NoSuchIcebergTableError):
        test_catalog.rename_table(identifier, new_identifier)


@mock_aws
def test_list_tables(
    _bucket_initialize: None,
    moto_endpoint_url: str,
    table_schema_nested: Schema,
    database_name: str,
    table_name: str,
    table_list: List[str],
) -> None:
    test_catalog = GlueCatalog("glue", **{"s3.endpoint": moto_endpoint_url, "warehouse": f"s3://{BUCKET_NAME}/"})
    test_catalog.create_namespace(namespace=database_name)
    for table_name in table_list:
        test_catalog.create_table((database_name, table_name), table_schema_nested)
    loaded_table_list = test_catalog.list_tables(database_name)
    for table_name in table_list:
        assert (database_name, table_name) in loaded_table_list


@mock_aws
def test_list_namespaces(_bucket_initialize: None, moto_endpoint_url: str, database_list: List[str]) -> None:
    test_catalog = GlueCatalog("glue", **{"s3.endpoint": moto_endpoint_url})
    for database_name in database_list:
        test_catalog.create_namespace(namespace=database_name)
    loaded_database_list = test_catalog.list_namespaces()
    for database_name in database_list:
        assert (database_name,) in loaded_database_list


@mock_aws
def test_create_namespace_no_properties(_bucket_initialize: None, moto_endpoint_url: str, database_name: str) -> None:
    test_catalog = GlueCatalog("glue", **{"s3.endpoint": moto_endpoint_url})
    test_catalog.create_namespace(namespace=database_name)
    loaded_database_list = test_catalog.list_namespaces()
    assert len(loaded_database_list) == 1
    assert (database_name,) in loaded_database_list
    properties = test_catalog.load_namespace_properties(database_name)
    assert properties == {}


@mock_aws
def test_create_namespace_with_comment_and_location(_bucket_initialize: None, moto_endpoint_url: str, database_name: str) -> None:
    test_location = f"s3://{BUCKET_NAME}/{database_name}.db"
    test_properties = {
        "comment": "this is a test description",
        "location": test_location,
    }
    test_catalog = GlueCatalog("glue", **{"s3.endpoint": moto_endpoint_url})
    test_catalog.create_namespace(namespace=database_name, properties=test_properties)
    loaded_database_list = test_catalog.list_namespaces()
    assert len(loaded_database_list) == 1
    assert (database_name,) in loaded_database_list
    properties = test_catalog.load_namespace_properties(database_name)
    assert properties["comment"] == "this is a test description"
    assert properties["location"] == test_location


@mock_aws
def test_create_duplicated_namespace(_bucket_initialize: None, moto_endpoint_url: str, database_name: str) -> None:
    test_catalog = GlueCatalog("glue", **{"s3.endpoint": moto_endpoint_url})
    test_catalog.create_namespace(namespace=database_name)
    loaded_database_list = test_catalog.list_namespaces()
    assert len(loaded_database_list) == 1
    assert (database_name,) in loaded_database_list
    with pytest.raises(NamespaceAlreadyExistsError):
        test_catalog.create_namespace(namespace=database_name, properties={"test": "test"})


@mock_aws
def test_drop_namespace(_bucket_initialize: None, moto_endpoint_url: str, database_name: str) -> None:
    test_catalog = GlueCatalog("glue", **{"s3.endpoint": moto_endpoint_url})
    test_catalog.create_namespace(namespace=database_name)
    loaded_database_list = test_catalog.list_namespaces()
    assert len(loaded_database_list) == 1
    assert (database_name,) in loaded_database_list
    test_catalog.drop_namespace(database_name)
    loaded_database_list = test_catalog.list_namespaces()
    assert len(loaded_database_list) == 0


@mock_aws
def test_drop_non_empty_namespace(
    _bucket_initialize: None, moto_endpoint_url: str, table_schema_nested: Schema, database_name: str, table_name: str
) -> None:
    identifier = (database_name, table_name)
    test_catalog = GlueCatalog("glue", **{"s3.endpoint": moto_endpoint_url, "warehouse": f"s3://{BUCKET_NAME}/"})
    test_catalog.create_namespace(namespace=database_name)
    test_catalog.create_table(identifier, table_schema_nested)
    assert len(test_catalog.list_tables(database_name)) == 1
    with pytest.raises(NamespaceNotEmptyError):
        test_catalog.drop_namespace(database_name)


@mock_aws
def test_drop_non_exist_namespace(_bucket_initialize: None, moto_endpoint_url: str, database_name: str) -> None:
    test_catalog = GlueCatalog("glue", **{"s3.endpoint": moto_endpoint_url})
    with pytest.raises(NoSuchNamespaceError):
        test_catalog.drop_namespace(database_name)


@mock_aws
def test_load_namespace_properties(_bucket_initialize: None, moto_endpoint_url: str, database_name: str) -> None:
    test_location = f"s3://{BUCKET_NAME}/{database_name}.db"
    test_properties = {
        "comment": "this is a test description",
        "location": test_location,
        "test_property1": "1",
        "test_property2": "2",
        "test_property3": "3",
    }
    test_catalog = GlueCatalog("glue", **{"s3.endpoint": moto_endpoint_url})
    test_catalog.create_namespace(database_name, test_properties)
    listed_properties = test_catalog.load_namespace_properties(database_name)
    for k, v in listed_properties.items():
        assert k in test_properties
        assert v == test_properties[k]


@mock_aws
def test_load_non_exist_namespace_properties(_bucket_initialize: None, moto_endpoint_url: str, database_name: str) -> None:
    test_catalog = GlueCatalog("glue", **{"s3.endpoint": moto_endpoint_url})
    with pytest.raises(NoSuchNamespaceError):
        test_catalog.load_namespace_properties(database_name)


@mock_aws
def test_update_namespace_properties(_bucket_initialize: None, moto_endpoint_url: str, database_name: str) -> None:
    test_properties = {
        "comment": "this is a test description",
        "location": f"s3://{BUCKET_NAME}/{database_name}.db",
        "test_property1": "1",
        "test_property2": "2",
        "test_property3": "3",
    }
    removals = {"test_property1", "test_property2", "test_property3", "should_not_removed"}
    updates = {"test_property4": "4", "test_property5": "5", "comment": "updated test description"}
    test_catalog = GlueCatalog("glue", **{"s3.endpoint": moto_endpoint_url})
    test_catalog.create_namespace(database_name, test_properties)
    update_report = test_catalog.update_namespace_properties(database_name, removals, updates)
    for k in updates.keys():
        assert k in update_report.updated
    for k in removals:
        if k == "should_not_removed":
            assert k in update_report.missing
        else:
            assert k in update_report.removed
    assert "updated test description" == test_catalog.load_namespace_properties(database_name)["comment"]
    test_catalog.drop_namespace(database_name)


@mock_aws
def test_load_empty_namespace_properties(_bucket_initialize: None, moto_endpoint_url: str, database_name: str) -> None:
    test_catalog = GlueCatalog("glue", **{"s3.endpoint": moto_endpoint_url})
    test_catalog.create_namespace(database_name)
    listed_properties = test_catalog.load_namespace_properties(database_name)
    assert listed_properties == {}


@mock_aws
def test_load_default_namespace_properties(_glue, _bucket_initialize: None, moto_endpoint_url: str, database_name: str) -> None:  # type: ignore
    # simulate creating database with default settings through AWS Glue Web Console
    _glue.create_database(DatabaseInput={"Name": database_name})
    test_catalog = GlueCatalog("glue", **{"s3.endpoint": moto_endpoint_url})
    listed_properties = test_catalog.load_namespace_properties(database_name)
    assert listed_properties == {}


@mock_aws
def test_update_namespace_properties_overlap_update_removal(
    _bucket_initialize: None, moto_endpoint_url: str, database_name: str
) -> None:
    test_properties = {
        "comment": "this is a test description",
        "location": f"s3://{BUCKET_NAME}/{database_name}.db",
        "test_property1": "1",
        "test_property2": "2",
        "test_property3": "3",
    }
    removals = {"test_property1", "test_property2", "test_property3", "should_not_removed"}
    updates = {"test_property1": "4", "test_property5": "5", "comment": "updated test description"}
    test_catalog = GlueCatalog("glue", **{"s3.endpoint": moto_endpoint_url})
    test_catalog.create_namespace(database_name, test_properties)
    with pytest.raises(ValueError):
        test_catalog.update_namespace_properties(database_name, removals, updates)
    # should not modify the properties
    assert test_catalog.load_namespace_properties(database_name) == test_properties


@mock_aws
def test_passing_profile_name() -> None:
    session_properties: Dict[str, Any] = {
        "aws_access_key_id": "abc",
        "aws_secret_access_key": "def",
        "aws_session_token": "ghi",
        "region_name": "eu-central-1",
        "profile_name": "sandbox",
        "botocore_session": None,
    }
    test_properties = {"type": "glue"}
    test_properties.update(session_properties)

    with mock.patch("boto3.Session") as mock_session:
        test_catalog = GlueCatalog("glue", **test_properties)

    mock_session.assert_called_with(**session_properties)
    assert test_catalog.glue is mock_session().client()


@mock_aws
def test_commit_table_update_schema(
    _glue: boto3.client,
    _bucket_initialize: None,
    moto_endpoint_url: str,
    table_schema_nested: Schema,
    database_name: str,
    table_name: str,
) -> None:
    catalog_name = "glue"
    identifier = (database_name, table_name)
    test_catalog = GlueCatalog(catalog_name, **{"s3.endpoint": moto_endpoint_url, "warehouse": f"s3://{BUCKET_NAME}"})
    test_catalog.create_namespace(namespace=database_name)
    table = test_catalog.create_table(identifier, table_schema_nested)
    original_table_metadata = table.metadata

    assert TABLE_METADATA_LOCATION_REGEX.match(table.metadata_location)
    assert test_catalog._parse_metadata_version(table.metadata_location) == 0
    assert original_table_metadata.current_schema_id == 0

    transaction = table.transaction()
    update = transaction.update_schema()
    update.add_column(path="b", field_type=IntegerType())
    update.commit()
    transaction.commit_transaction()

    updated_table_metadata = table.metadata

    assert TABLE_METADATA_LOCATION_REGEX.match(table.metadata_location)
    assert test_catalog._parse_metadata_version(table.metadata_location) == 1
    assert updated_table_metadata.current_schema_id == 1
    assert len(updated_table_metadata.schemas) == 2
    new_schema = next(schema for schema in updated_table_metadata.schemas if schema.schema_id == 1)
    assert new_schema
    assert new_schema == update._apply()
    assert new_schema.find_field("b").field_type == IntegerType()

    # Ensure schema is also pushed to Glue
    table_info = _glue.get_table(
        DatabaseName=database_name,
        Name=table_name,
    )
    storage_descriptor = table_info["Table"]["StorageDescriptor"]
    columns = storage_descriptor["Columns"]
    assert len(columns) == len(table_schema_nested.fields) + 1
    assert columns[-1] == {
        "Name": "b",
        "Type": "int",
        "Parameters": {"iceberg.field.id": "18", "iceberg.field.optional": "true", "iceberg.field.current": "true"},
    }
    assert storage_descriptor["Location"] == f"s3://{BUCKET_NAME}/{database_name}.db/{table_name}"


@mock_aws
def test_commit_table_properties(
    _bucket_initialize: None, moto_endpoint_url: str, table_schema_nested: Schema, database_name: str, table_name: str
) -> None:
    catalog_name = "glue"
    identifier = (database_name, table_name)
    test_catalog = GlueCatalog(catalog_name, **{"s3.endpoint": moto_endpoint_url, "warehouse": f"s3://{BUCKET_NAME}"})
    test_catalog.create_namespace(namespace=database_name)
    table = test_catalog.create_table(identifier=identifier, schema=table_schema_nested, properties={"test_a": "test_a"})

    assert test_catalog._parse_metadata_version(table.metadata_location) == 0

    transaction = table.transaction()
    transaction.set_properties(test_a="test_aa", test_b="test_b", test_c="test_c")
    transaction.remove_properties("test_b")
    transaction.commit_transaction()

    updated_table_metadata = table.metadata
    assert test_catalog._parse_metadata_version(table.metadata_location) == 1
    assert updated_table_metadata.properties == {"test_a": "test_aa", "test_c": "test_c"}
