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

from pyiceberg.catalog import METADATA_LOCATION, TABLE_TYPE
from pyiceberg.catalog.dynamodb import (
    ACTIVE,
    DYNAMODB_COL_CREATED_AT,
    DYNAMODB_COL_IDENTIFIER,
    DYNAMODB_COL_NAMESPACE,
    DYNAMODB_TABLE_NAME_DEFAULT,
    CatalogCache,
    CatalogEvent,
    CatalogEventContext,
    DynamoDbCatalog,
    _add_property_prefix,
)
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
from pyiceberg.typedef import Properties
from tests.conftest import (
    BUCKET_NAME,
    TABLE_METADATA_LOCATION_REGEX,
    UNIFIED_AWS_SESSION_PROPERTIES,
)


@mock_aws
def test_create_dynamodb_catalog_with_table_name(_dynamodb, _bucket_initialize: None) -> None:  # type: ignore
    DynamoDbCatalog("test_ddb_catalog")
    response = _dynamodb.describe_table(TableName=DYNAMODB_TABLE_NAME_DEFAULT)
    assert response["Table"]["TableName"] == DYNAMODB_TABLE_NAME_DEFAULT
    assert response["Table"]["TableStatus"] == ACTIVE

    custom_table_name = "custom_table_name"
    DynamoDbCatalog("test_ddb_catalog", **{"table-name": custom_table_name})
    response = _dynamodb.describe_table(TableName=custom_table_name)
    assert response["Table"]["TableName"] == custom_table_name
    assert response["Table"]["TableStatus"] == ACTIVE


@mock_aws
def test_create_table_with_database_location(
    _bucket_initialize: None, moto_endpoint_url: str, table_schema_nested: Schema, database_name: str, table_name: str
) -> None:
    catalog_name = "test_ddb_catalog"
    identifier = (database_name, table_name)
    test_catalog = DynamoDbCatalog(catalog_name, **{"s3.endpoint": moto_endpoint_url})
    test_catalog.create_namespace(namespace=database_name, properties={"location": f"s3://{BUCKET_NAME}/{database_name}.db"})
    table = test_catalog.create_table(identifier, table_schema_nested)
    assert table.name() == identifier
    assert TABLE_METADATA_LOCATION_REGEX.match(table.metadata_location)


@mock_aws
def test_create_table_with_pyarrow_schema(
    _bucket_initialize: None,
    moto_endpoint_url: str,
    pyarrow_schema_simple_without_ids: pa.Schema,
    database_name: str,
    table_name: str,
) -> None:
    catalog_name = "test_ddb_catalog"
    identifier = (database_name, table_name)
    test_catalog = DynamoDbCatalog(catalog_name, **{"s3.endpoint": moto_endpoint_url})
    test_catalog.create_namespace(namespace=database_name, properties={"location": f"s3://{BUCKET_NAME}/{database_name}.db"})
    table = test_catalog.create_table(identifier, pyarrow_schema_simple_without_ids)
    assert table.name() == identifier
    assert TABLE_METADATA_LOCATION_REGEX.match(table.metadata_location)


@mock_aws
def test_create_table_with_default_warehouse(
    _bucket_initialize: None, moto_endpoint_url: str, table_schema_nested: Schema, database_name: str, table_name: str
) -> None:
    catalog_name = "test_ddb_catalog"
    identifier = (database_name, table_name)
    test_catalog = DynamoDbCatalog(catalog_name, **{"s3.endpoint": moto_endpoint_url, "warehouse": f"s3://{BUCKET_NAME}"})
    test_catalog.create_namespace(namespace=database_name)
    table = test_catalog.create_table(identifier, table_schema_nested)
    assert table.name() == identifier
    assert TABLE_METADATA_LOCATION_REGEX.match(table.metadata_location)


@mock_aws
def test_create_table_with_given_location(
    _bucket_initialize: None, moto_endpoint_url: str, table_schema_nested: Schema, database_name: str, table_name: str
) -> None:
    catalog_name = "test_ddb_catalog"
    identifier = (database_name, table_name)
    test_catalog = DynamoDbCatalog(catalog_name, **{"s3.endpoint": moto_endpoint_url})
    test_catalog.create_namespace(namespace=database_name)
    table = test_catalog.create_table(
        identifier=identifier, schema=table_schema_nested, location=f"s3://{BUCKET_NAME}/{database_name}.db/{table_name}"
    )
    assert table.name() == identifier
    assert TABLE_METADATA_LOCATION_REGEX.match(table.metadata_location)


@mock_aws
def test_create_table_removes_trailing_slash_in_location(
    _bucket_initialize: None, moto_endpoint_url: str, table_schema_nested: Schema, database_name: str, table_name: str
) -> None:
    catalog_name = "test_ddb_catalog"
    identifier = (database_name, table_name)
    test_catalog = DynamoDbCatalog(catalog_name, **{"s3.endpoint": moto_endpoint_url})
    test_catalog.create_namespace(namespace=database_name)
    location = f"s3://{BUCKET_NAME}/{database_name}.db/{table_name}"
    table = test_catalog.create_table(identifier=identifier, schema=table_schema_nested, location=f"{location}/")
    assert table.name() == identifier
    assert table.location() == location
    assert TABLE_METADATA_LOCATION_REGEX.match(table.metadata_location)


@mock_aws
def test_create_table_with_no_location(
    _bucket_initialize: None, table_schema_nested: Schema, database_name: str, table_name: str
) -> None:
    identifier = (database_name, table_name)
    test_catalog = DynamoDbCatalog("test_ddb_catalog")
    test_catalog.create_namespace(namespace=database_name)
    with pytest.raises(ValueError):
        test_catalog.create_table(identifier=identifier, schema=table_schema_nested)


@mock_aws
def test_create_table_with_strips(
    _bucket_initialize: None, moto_endpoint_url: str, table_schema_nested: Schema, database_name: str, table_name: str
) -> None:
    catalog_name = "test_ddb_catalog"
    identifier = (database_name, table_name)
    test_catalog = DynamoDbCatalog(catalog_name, **{"s3.endpoint": moto_endpoint_url})
    test_catalog.create_namespace(namespace=database_name, properties={"location": f"s3://{BUCKET_NAME}/{database_name}.db/"})
    table = test_catalog.create_table(identifier, table_schema_nested)
    assert table.name() == identifier
    assert TABLE_METADATA_LOCATION_REGEX.match(table.metadata_location)


@mock_aws
def test_create_table_with_strips_bucket_root(
    _bucket_initialize: None, moto_endpoint_url: str, table_schema_nested: Schema, database_name: str, table_name: str
) -> None:
    catalog_name = "test_ddb_catalog"
    identifier = (database_name, table_name)
    test_catalog = DynamoDbCatalog(catalog_name, **{"s3.endpoint": moto_endpoint_url, "warehouse": f"s3://{BUCKET_NAME}/"})
    test_catalog.create_namespace(namespace=database_name)
    table_strip = test_catalog.create_table(identifier, table_schema_nested)
    assert table_strip.name() == identifier
    assert TABLE_METADATA_LOCATION_REGEX.match(table_strip.metadata_location)


@mock_aws
def test_create_table_with_no_database(
    _bucket_initialize: None, table_schema_nested: Schema, database_name: str, table_name: str
) -> None:
    identifier = (database_name, table_name)
    test_catalog = DynamoDbCatalog("test_ddb_catalog")
    with pytest.raises(NoSuchNamespaceError):
        test_catalog.create_table(identifier=identifier, schema=table_schema_nested)


@mock_aws
def test_create_duplicated_table(
    _bucket_initialize: None, moto_endpoint_url: str, table_schema_nested: Schema, database_name: str, table_name: str
) -> None:
    identifier = (database_name, table_name)
    test_catalog = DynamoDbCatalog("test_ddb_catalog", **{"warehouse": f"s3://{BUCKET_NAME}", "s3.endpoint": moto_endpoint_url})
    test_catalog.create_namespace(namespace=database_name)
    test_catalog.create_table(identifier, table_schema_nested)
    with pytest.raises(TableAlreadyExistsError):
        test_catalog.create_table(identifier, table_schema_nested)


@mock_aws
def test_create_table_if_not_exists_duplicated_table(
    _bucket_initialize: None, moto_endpoint_url: str, table_schema_nested: Schema, database_name: str, table_name: str
) -> None:
    identifier = (database_name, table_name)
    test_catalog = DynamoDbCatalog("test_ddb_catalog", **{"warehouse": f"s3://{BUCKET_NAME}", "s3.endpoint": moto_endpoint_url})
    test_catalog.create_namespace(namespace=database_name)
    table1 = test_catalog.create_table(identifier, table_schema_nested)
    table2 = test_catalog.create_table_if_not_exists(identifier, table_schema_nested)
    assert table1.name() == table2.name()


@mock_aws
def test_load_table(
    _bucket_initialize: None, moto_endpoint_url: str, table_schema_nested: Schema, database_name: str, table_name: str
) -> None:
    catalog_name = "test_ddb_catalog"
    identifier = (database_name, table_name)
    test_catalog = DynamoDbCatalog(catalog_name, **{"warehouse": f"s3://{BUCKET_NAME}", "s3.endpoint": moto_endpoint_url})
    test_catalog.create_namespace(namespace=database_name)
    test_catalog.create_table(identifier, table_schema_nested)
    table = test_catalog.load_table(identifier)
    assert table.name() == identifier
    assert TABLE_METADATA_LOCATION_REGEX.match(table.metadata_location)


@mock_aws
def test_load_table_from_self_identifier(
    _bucket_initialize: None, moto_endpoint_url: str, table_schema_nested: Schema, database_name: str, table_name: str
) -> None:
    catalog_name = "test_ddb_catalog"
    identifier = (database_name, table_name)
    test_catalog = DynamoDbCatalog(catalog_name, **{"warehouse": f"s3://{BUCKET_NAME}", "s3.endpoint": moto_endpoint_url})
    test_catalog.create_namespace(namespace=database_name)
    test_catalog.create_table(identifier, table_schema_nested)
    intermediate = test_catalog.load_table(identifier)
    table = test_catalog.load_table(intermediate.name())
    assert table.name() == identifier
    assert TABLE_METADATA_LOCATION_REGEX.match(table.metadata_location)


@mock_aws
def test_load_non_exist_table(_bucket_initialize: None, database_name: str, table_name: str) -> None:
    identifier = (database_name, table_name)
    test_catalog = DynamoDbCatalog("test_ddb_catalog", warehouse=f"s3://{BUCKET_NAME}")
    test_catalog.create_namespace(namespace=database_name)
    with pytest.raises(NoSuchTableError):
        test_catalog.load_table(identifier)


@mock_aws
def test_drop_table(
    _bucket_initialize: None, moto_endpoint_url: str, table_schema_nested: Schema, database_name: str, table_name: str
) -> None:
    catalog_name = "test_ddb_catalog"
    identifier = (database_name, table_name)
    test_catalog = DynamoDbCatalog(catalog_name, **{"warehouse": f"s3://{BUCKET_NAME}", "s3.endpoint": moto_endpoint_url})
    test_catalog.create_namespace(namespace=database_name)
    test_catalog.create_table(identifier, table_schema_nested)
    table = test_catalog.load_table(identifier)
    assert table.name() == identifier
    assert TABLE_METADATA_LOCATION_REGEX.match(table.metadata_location)
    test_catalog.drop_table(identifier)
    with pytest.raises(NoSuchTableError):
        test_catalog.load_table(identifier)


@mock_aws
def test_drop_table_from_self_identifier(
    _bucket_initialize: None, moto_endpoint_url: str, table_schema_nested: Schema, database_name: str, table_name: str
) -> None:
    catalog_name = "test_ddb_catalog"
    identifier = (database_name, table_name)
    test_catalog = DynamoDbCatalog(catalog_name, **{"warehouse": f"s3://{BUCKET_NAME}", "s3.endpoint": moto_endpoint_url})
    test_catalog.create_namespace(namespace=database_name)
    test_catalog.create_table(identifier, table_schema_nested)
    table = test_catalog.load_table(identifier)
    assert table.name() == identifier
    assert TABLE_METADATA_LOCATION_REGEX.match(table.metadata_location)
    test_catalog.drop_table(table.name())
    with pytest.raises(NoSuchTableError):
        test_catalog.load_table(identifier)
    with pytest.raises(NoSuchTableError):
        test_catalog.load_table(table.name())


@mock_aws
def test_drop_non_exist_table(_bucket_initialize: None, database_name: str, table_name: str) -> None:
    identifier = (database_name, table_name)
    test_catalog = DynamoDbCatalog("test_ddb_catalog", warehouse=f"s3://{BUCKET_NAME}")
    with pytest.raises(NoSuchTableError):
        test_catalog.drop_table(identifier)


@mock_aws
def test_rename_table(
    _bucket_initialize: None, moto_endpoint_url: str, table_schema_nested: Schema, database_name: str, table_name: str
) -> None:
    catalog_name = "test_ddb_catalog"
    new_table_name = f"{table_name}_new"
    identifier = (database_name, table_name)
    new_identifier = (database_name, new_table_name)
    test_catalog = DynamoDbCatalog(catalog_name, **{"warehouse": f"s3://{BUCKET_NAME}", "s3.endpoint": moto_endpoint_url})
    test_catalog.create_namespace(namespace=database_name)
    table = test_catalog.create_table(identifier, table_schema_nested)
    assert table.name() == identifier
    assert TABLE_METADATA_LOCATION_REGEX.match(table.metadata_location)
    test_catalog.rename_table(identifier, new_identifier)
    new_table = test_catalog.load_table(new_identifier)
    assert new_table.name() == new_identifier
    # the metadata_location should not change
    assert new_table.metadata_location == table.metadata_location
    # old table should be dropped
    with pytest.raises(NoSuchTableError):
        test_catalog.load_table(identifier)


@mock_aws
def test_rename_table_from_self_identifier(
    _bucket_initialize: None, moto_endpoint_url: str, table_schema_nested: Schema, database_name: str, table_name: str
) -> None:
    catalog_name = "test_ddb_catalog"
    new_table_name = f"{table_name}_new"
    identifier = (database_name, table_name)
    new_identifier = (database_name, new_table_name)
    test_catalog = DynamoDbCatalog(catalog_name, **{"warehouse": f"s3://{BUCKET_NAME}", "s3.endpoint": moto_endpoint_url})
    test_catalog.create_namespace(namespace=database_name)
    table = test_catalog.create_table(identifier, table_schema_nested)
    assert table.name() == identifier
    assert TABLE_METADATA_LOCATION_REGEX.match(table.metadata_location)
    test_catalog.rename_table(table.name(), new_identifier)
    new_table = test_catalog.load_table(new_identifier)
    assert new_table.name() == new_identifier
    # the metadata_location should not change
    assert new_table.metadata_location == table.metadata_location
    # old table should be dropped
    with pytest.raises(NoSuchTableError):
        test_catalog.load_table(identifier)
    with pytest.raises(NoSuchTableError):
        test_catalog.load_table(table.name())


@mock_aws
def test_fail_on_rename_table_with_missing_required_params(_bucket_initialize: None, database_name: str, table_name: str) -> None:
    new_database_name = f"{database_name}_new"
    new_table_name = f"{table_name}_new"
    identifier = (database_name, table_name)
    new_identifier = (new_database_name, new_table_name)
    test_catalog = DynamoDbCatalog("test_ddb_catalog", warehouse=f"s3://{BUCKET_NAME}")
    test_catalog.create_namespace(namespace=database_name)
    test_catalog.create_namespace(namespace=new_database_name)

    # Missing required params
    # pylint: disable=W0212
    test_catalog._put_dynamo_item(
        item={
            DYNAMODB_COL_IDENTIFIER: {"S": f"{database_name}.{table_name}"},
            DYNAMODB_COL_NAMESPACE: {"S": database_name},
        },
        condition_expression=f"attribute_not_exists({DYNAMODB_COL_IDENTIFIER})",
    )

    with pytest.raises(NoSuchPropertyException):
        test_catalog.rename_table(identifier, new_identifier)


@mock_aws
def test_fail_on_rename_non_iceberg_table(
    _dynamodb: boto3.client, _bucket_initialize: None, database_name: str, table_name: str
) -> None:
    new_database_name = f"{database_name}_new"
    new_table_name = f"{table_name}_new"
    identifier = (database_name, table_name)
    new_identifier = (new_database_name, new_table_name)
    test_catalog = DynamoDbCatalog("test_ddb_catalog", warehouse=f"s3://{BUCKET_NAME}")
    test_catalog.create_namespace(namespace=database_name)
    test_catalog.create_namespace(namespace=new_database_name)

    # Wrong TABLE_TYPE param
    # pylint: disable=W0212
    test_catalog._put_dynamo_item(
        item={
            DYNAMODB_COL_IDENTIFIER: {"S": f"{database_name}.{table_name}"},
            DYNAMODB_COL_NAMESPACE: {"S": database_name},
            DYNAMODB_COL_CREATED_AT: {"S": "test-1873287263487623"},
            _add_property_prefix(TABLE_TYPE): {"S": "non-iceberg-table-type"},
            _add_property_prefix(METADATA_LOCATION): {"S": "test-metadata-location"},
        },
        condition_expression=f"attribute_not_exists({DYNAMODB_COL_IDENTIFIER})",
    )

    with pytest.raises(NoSuchIcebergTableError):
        test_catalog.rename_table(identifier, new_identifier)


@mock_aws
def test_list_tables(
    _bucket_initialize: None, moto_endpoint_url: str, table_schema_nested: Schema, database_name: str, table_list: List[str]
) -> None:
    test_catalog = DynamoDbCatalog("test_ddb_catalog", **{"warehouse": f"s3://{BUCKET_NAME}", "s3.endpoint": moto_endpoint_url})
    test_catalog.create_namespace(namespace=database_name)
    for table_name in table_list:
        test_catalog.create_table((database_name, table_name), table_schema_nested)
    loaded_table_list = test_catalog.list_tables(database_name)
    for table_name in table_list:
        assert (database_name, table_name) in loaded_table_list


@mock_aws
def test_list_namespaces(_bucket_initialize: None, database_list: List[str]) -> None:
    test_catalog = DynamoDbCatalog("test_ddb_catalog")
    for database_name in database_list:
        test_catalog.create_namespace(namespace=database_name)
    loaded_database_list = test_catalog.list_namespaces()
    for database_name in database_list:
        assert (database_name,) in loaded_database_list


@mock_aws
def test_create_namespace_no_properties(_bucket_initialize: None, database_name: str) -> None:
    test_catalog = DynamoDbCatalog("test_ddb_catalog")
    test_catalog.create_namespace(namespace=database_name)
    loaded_database_list = test_catalog.list_namespaces()
    assert len(loaded_database_list) == 1
    assert (database_name,) in loaded_database_list
    properties = test_catalog.load_namespace_properties(database_name)
    assert properties == {}


@mock_aws
def test_create_namespace_with_comment_and_location(_bucket_initialize: None, database_name: str) -> None:
    test_location = f"s3://{BUCKET_NAME}/{database_name}.db"
    test_properties = {
        "comment": "this is a test description",
        "location": test_location,
    }
    test_catalog = DynamoDbCatalog("test_ddb_catalog")
    test_catalog.create_namespace(namespace=database_name, properties=test_properties)
    loaded_database_list = test_catalog.list_namespaces()
    assert len(loaded_database_list) == 1
    assert (database_name,) in loaded_database_list
    properties = test_catalog.load_namespace_properties(database_name)
    assert properties["comment"] == "this is a test description"
    assert properties["location"] == test_location


@mock_aws
def test_create_duplicated_namespace(_bucket_initialize: None, database_name: str) -> None:
    test_catalog = DynamoDbCatalog("test_ddb_catalog")
    test_catalog.create_namespace(namespace=database_name)
    loaded_database_list = test_catalog.list_namespaces()
    assert len(loaded_database_list) == 1
    assert (database_name,) in loaded_database_list
    with pytest.raises(NamespaceAlreadyExistsError):
        test_catalog.create_namespace(namespace=database_name, properties={"test": "test"})


@mock_aws
def test_drop_namespace(_bucket_initialize: None, database_name: str) -> None:
    test_catalog = DynamoDbCatalog("test_ddb_catalog")
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
    test_catalog = DynamoDbCatalog("test_ddb_catalog", **{"warehouse": f"s3://{BUCKET_NAME}", "s3.endpoint": moto_endpoint_url})
    test_catalog.create_namespace(namespace=database_name)
    test_catalog.create_table(identifier, table_schema_nested)
    assert len(test_catalog.list_tables(database_name)) == 1
    with pytest.raises(NamespaceNotEmptyError):
        test_catalog.drop_namespace(database_name)


@mock_aws
def test_drop_non_exist_namespace(_bucket_initialize: None, database_name: str) -> None:
    test_catalog = DynamoDbCatalog("test_ddb_catalog")
    with pytest.raises(NoSuchNamespaceError):
        test_catalog.drop_namespace(database_name)


@mock_aws
def test_load_namespace_properties(_bucket_initialize: None, database_name: str) -> None:
    test_location = f"s3://{BUCKET_NAME}/{database_name}.db"
    test_properties = {
        "comment": "this is a test description",
        "location": test_location,
        "test_property1": "1",
        "test_property2": "2",
        "test_property3": "3",
    }
    test_catalog = DynamoDbCatalog("test_ddb_catalog")
    test_catalog.create_namespace(database_name, test_properties)
    listed_properties = test_catalog.load_namespace_properties(database_name)
    for k, v in listed_properties.items():
        assert k in test_properties
        assert v == test_properties[k]


@mock_aws
def test_load_non_exist_namespace_properties(_bucket_initialize: None, database_name: str) -> None:
    test_catalog = DynamoDbCatalog("test_ddb_catalog")
    with pytest.raises(NoSuchNamespaceError):
        test_catalog.load_namespace_properties(database_name)


@mock_aws
def test_update_namespace_properties(_bucket_initialize: None, database_name: str) -> None:
    test_properties = {
        "comment": "this is a test description",
        "location": f"s3://{BUCKET_NAME}/{database_name}.db",
        "test_property1": "1",
        "test_property2": "2",
        "test_property3": "3",
    }
    removals = {"test_property1", "test_property2", "test_property3", "should_not_removed"}
    updates = {"test_property4": "4", "test_property5": "5", "comment": "updated test description"}
    test_catalog = DynamoDbCatalog("test_ddb_catalog")
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
def test_load_empty_namespace_properties(_bucket_initialize: None, database_name: str) -> None:
    test_catalog = DynamoDbCatalog("test_ddb_catalog")
    test_catalog.create_namespace(database_name)
    listed_properties = test_catalog.load_namespace_properties(database_name)
    assert listed_properties == {}


@mock_aws
def test_update_namespace_properties_overlap_update_removal(_bucket_initialize: None, database_name: str) -> None:
    test_properties = {
        "comment": "this is a test description",
        "location": f"s3://{BUCKET_NAME}/{database_name}.db",
        "test_property1": "1",
        "test_property2": "2",
        "test_property3": "3",
    }
    removals = {"test_property1", "test_property2", "test_property3", "should_not_removed"}
    updates = {"test_property1": "4", "test_property5": "5", "comment": "updated test description"}
    test_catalog = DynamoDbCatalog("test_ddb_catalog")
    test_catalog.create_namespace(database_name, test_properties)
    with pytest.raises(ValueError):
        test_catalog.update_namespace_properties(database_name, removals, updates)
    # should not modify the properties
    assert test_catalog.load_namespace_properties(database_name) == test_properties


@mock_aws
def test_passing_glue_session_properties() -> None:
    session_properties: Properties = {
        "dynamodb.access-key-id": "dynamodb.access-key-id",
        "dynamodb.secret-access-key": "dynamodb.secret-access-key",
        "dynamodb.profile-name": "dynamodb.profile-name",
        "dynamodb.region": "dynamodb.region",
        "dynamodb.session-token": "dynamodb.session-token",
        **UNIFIED_AWS_SESSION_PROPERTIES,
    }

    with mock.patch("boto3.Session") as mock_session:
        mock_client = mock.Mock()
        mock_session.return_value.client.return_value = mock_client
        mock_client.describe_table.return_value = {"Table": {"TableStatus": "ACTIVE"}}
        test_catalog = DynamoDbCatalog("dynamodb", **session_properties)

        mock_session.assert_called_with(
            aws_access_key_id="dynamodb.access-key-id",
            aws_secret_access_key="dynamodb.secret-access-key",
            aws_session_token="dynamodb.session-token",
            region_name="dynamodb.region",
            profile_name="dynamodb.profile-name",
            botocore_session=None,
        )
        assert test_catalog.dynamodb is mock_session().client()


@mock_aws
def test_passing_unified_session_properties_to_dynamodb() -> None:
    session_properties: Properties = {
        "dynamodb.profile-name": "dynamodb.profile-name",
        **UNIFIED_AWS_SESSION_PROPERTIES,
    }

    with mock.patch("boto3.Session") as mock_session:
        mock_client = mock.Mock()
        mock_session.return_value.client.return_value = mock_client
        mock_client.describe_table.return_value = {"Table": {"TableStatus": "ACTIVE"}}
        test_catalog = DynamoDbCatalog("dynamodb", **session_properties)

        mock_session.assert_called_with(
            aws_access_key_id="client.access-key-id",
            aws_secret_access_key="client.secret-access-key",
            aws_session_token="client.session-token",
            region_name="client.region",
            profile_name="dynamodb.profile-name",
            botocore_session=None,
        )
        assert test_catalog.dynamodb is mock_session().client()


@mock_aws
def test_table_exists(
    _bucket_initialize: None, moto_endpoint_url: str, table_schema_nested: Schema, database_name: str, table_name: str
) -> None:
    identifier = (database_name, table_name)
    test_catalog = DynamoDbCatalog("test_ddb_catalog", **{"warehouse": f"s3://{BUCKET_NAME}", "s3.endpoint": moto_endpoint_url})
    test_catalog.create_namespace(namespace=database_name)
    test_catalog.create_table(identifier, table_schema_nested)
    # Act and Assert for an existing table
    assert test_catalog.table_exists(identifier) is True
    # Act and Assert for an non-existing  table
    assert test_catalog.table_exists(("non", "exist")) is False


@mock_aws
def test_dynamodb_client_override() -> None:
    catalog_name = "glue"
    test_client = boto3.client("dynamodb", region_name="us-west-2")
    test_catalog = DynamoDbCatalog(catalog_name, test_client)
    assert test_catalog.dynamodb is test_client


# ============================================================================
# Enhancement Tests: Callback Hooks, Metadata Caching, Retry Strategy
# ============================================================================


def test_catalog_cache_operations() -> None:
    """Test CatalogCache class basic operations."""
    cache = CatalogCache(ttl_seconds=300)

    # Test set and get
    cache.set("key1", "value1")
    assert cache.get("key1") == "value1"

    # Test get on non-existent key
    assert cache.get("nonexistent") is None

    # Test invalidate
    cache.set("key2", "value2")
    cache.invalidate("key2")
    assert cache.get("key2") is None

    # Test size
    cache.set("k1", "v1")
    cache.set("k2", "v2")
    cache.set("k3", "v3")
    assert cache.size() == 4  # k1, k2, k3, plus key1 from earlier

    # Test clear
    cache.clear()
    assert cache.size() == 0
    assert cache.get("k1") is None


def test_catalog_cache_ttl_expiration() -> None:
    """Test CatalogCache TTL expiration."""
    import time

    # Create cache with 1 second TTL
    cache = CatalogCache(ttl_seconds=1)

    # Set value
    cache.set("key", "value")
    assert cache.get("key") == "value"

    # Wait for expiration
    time.sleep(1.1)

    # Value should be expired
    assert cache.get("key") is None


def test_catalog_event_context_creation() -> None:
    """Test CatalogEventContext dataclass creation."""
    # Test basic context
    ctx = CatalogEventContext(
        event=CatalogEvent.PRE_CREATE_TABLE,
        catalog_name="test_catalog",
        identifier=("db", "table"),
        metadata_location="s3://bucket/metadata.json",
    )

    assert ctx.event == CatalogEvent.PRE_CREATE_TABLE
    assert ctx.catalog_name == "test_catalog"
    assert ctx.identifier == ("db", "table")
    assert ctx.metadata_location == "s3://bucket/metadata.json"
    assert ctx.error is None
    assert isinstance(ctx.extra, dict)

    # Test with error
    test_error = ValueError("test error")
    error_ctx = CatalogEventContext(
        event=CatalogEvent.ON_ERROR,
        catalog_name="test",
        error=test_error,
    )
    assert error_ctx.error == test_error

    # Test with extra data
    extra_ctx = CatalogEventContext(
        event=CatalogEvent.POST_COMMIT,
        catalog_name="test",
        extra={"key": "value", "count": 42},
    )
    assert extra_ctx.extra["key"] == "value"
    assert extra_ctx.extra["count"] == 42


def test_catalog_event_enum() -> None:
    """Test CatalogEvent enum completeness."""
    # Test all event types exist
    events = [
        CatalogEvent.PRE_CREATE_TABLE,
        CatalogEvent.POST_CREATE_TABLE,
        CatalogEvent.PRE_UPDATE_TABLE,
        CatalogEvent.POST_UPDATE_TABLE,
        CatalogEvent.PRE_DROP_TABLE,
        CatalogEvent.POST_DROP_TABLE,
        CatalogEvent.PRE_COMMIT,
        CatalogEvent.POST_COMMIT,
        CatalogEvent.PRE_REGISTER_TABLE,
        CatalogEvent.POST_REGISTER_TABLE,
        CatalogEvent.ON_ERROR,
        CatalogEvent.ON_CONCURRENT_CONFLICT,
    ]

    assert len(events) == 12

    # Test event values
    assert CatalogEvent.PRE_CREATE_TABLE.value == "pre_create_table"
    assert CatalogEvent.POST_COMMIT.value == "post_commit"
    assert CatalogEvent.ON_ERROR.value == "on_error"


@mock_aws
def test_catalog_hook_registration(_dynamodb, _bucket_initialize: None) -> None:  # type: ignore
    """Test hook registration and triggering."""
    # Track hook calls
    hook_calls: List[Dict[str, Any]] = []

    def test_hook(ctx: CatalogEventContext) -> None:
        hook_calls.append(
            {
                "event": ctx.event.value,
                "catalog": ctx.catalog_name,
                "identifier": ctx.identifier,
            }
        )

    # Create catalog
    catalog = DynamoDbCatalog("test_catalog")

    # Register hooks
    catalog.register_hook(CatalogEvent.PRE_CREATE_TABLE, test_hook)
    catalog.register_hook(CatalogEvent.POST_CREATE_TABLE, test_hook)
    catalog.register_hook(CatalogEvent.ON_ERROR, test_hook)

    assert len(catalog._event_hooks[CatalogEvent.PRE_CREATE_TABLE]) == 1
    assert len(catalog._event_hooks[CatalogEvent.POST_CREATE_TABLE]) == 1

    # Trigger a hook manually
    test_ctx = CatalogEventContext(
        event=CatalogEvent.PRE_CREATE_TABLE,
        catalog_name="test_catalog",
        identifier=("db", "table"),
    )
    catalog._trigger_hooks(CatalogEvent.PRE_CREATE_TABLE, test_ctx)

    assert len(hook_calls) == 1
    assert hook_calls[0]["event"] == "pre_create_table"
    assert hook_calls[0]["identifier"] == ("db", "table")

    # Test multiple hooks for same event
    hook_calls_2: List[str] = []
    catalog.register_hook(CatalogEvent.PRE_CREATE_TABLE, lambda ctx: hook_calls_2.append(ctx.event.value))

    catalog._trigger_hooks(CatalogEvent.PRE_CREATE_TABLE, test_ctx)
    assert len(hook_calls) == 2  # First hook called again
    assert len(hook_calls_2) == 1  # Second hook called


@mock_aws
def test_catalog_hooks_on_create_table(
    _bucket_initialize: None, moto_endpoint_url: str, table_schema_nested: Schema, database_name: str, table_name: str
) -> None:
    """Test that hooks are triggered during create_table operation."""
    events_fired: List[str] = []

    def audit_hook(ctx: CatalogEventContext) -> None:
        events_fired.append(ctx.event.value)

    identifier = (database_name, table_name)
    catalog = DynamoDbCatalog("test_catalog", **{"warehouse": f"s3://{BUCKET_NAME}", "s3.endpoint": moto_endpoint_url})

    # Register hooks
    catalog.register_hook(CatalogEvent.PRE_CREATE_TABLE, audit_hook)
    catalog.register_hook(CatalogEvent.POST_CREATE_TABLE, audit_hook)

    # Create namespace and table
    catalog.create_namespace(namespace=database_name)
    catalog.create_table(identifier, table_schema_nested)

    # Verify hooks were triggered
    assert "pre_create_table" in events_fired
    assert "post_create_table" in events_fired


@mock_aws
def test_catalog_hooks_on_drop_table(
    _bucket_initialize: None, moto_endpoint_url: str, table_schema_nested: Schema, database_name: str, table_name: str
) -> None:
    """Test that hooks are triggered during drop_table operation."""
    events_fired: List[str] = []

    def audit_hook(ctx: CatalogEventContext) -> None:
        events_fired.append(ctx.event.value)

    identifier = (database_name, table_name)
    catalog = DynamoDbCatalog("test_catalog", **{"warehouse": f"s3://{BUCKET_NAME}", "s3.endpoint": moto_endpoint_url})

    # Register hooks
    catalog.register_hook(CatalogEvent.PRE_DROP_TABLE, audit_hook)
    catalog.register_hook(CatalogEvent.POST_DROP_TABLE, audit_hook)

    # Create and drop table
    catalog.create_namespace(namespace=database_name)
    catalog.create_table(identifier, table_schema_nested)
    catalog.drop_table(identifier)

    # Verify hooks were triggered
    assert "pre_drop_table" in events_fired
    assert "post_drop_table" in events_fired


@mock_aws
def test_cache_configuration_enabled(_dynamodb, _bucket_initialize: None) -> None:  # type: ignore
    """Test catalog with cache enabled."""
    properties: Properties = {
        "warehouse": f"s3://{BUCKET_NAME}",
        "dynamodb.cache.enabled": "true",
        "dynamodb.cache.ttl-seconds": "600",
    }
    catalog = DynamoDbCatalog("test_catalog", **properties)
    assert catalog._cache is not None
    assert catalog._cache.ttl.total_seconds() == 600


@mock_aws
def test_cache_configuration_disabled(_dynamodb, _bucket_initialize: None) -> None:  # type: ignore
    """Test catalog with cache disabled."""
    properties: Properties = {
        "warehouse": f"s3://{BUCKET_NAME}",
        "dynamodb.cache.enabled": "false",
    }
    catalog = DynamoDbCatalog("test_catalog", **properties)
    assert catalog._cache is None


@mock_aws
def test_cache_default_configuration(_dynamodb, _bucket_initialize: None) -> None:  # type: ignore
    """Test catalog cache default configuration."""
    catalog = DynamoDbCatalog("test_catalog")
    assert catalog._cache is not None  # Cache enabled by default
    assert catalog._cache.ttl.total_seconds() == 300  # Default TTL


@mock_aws
def test_cache_helper_methods(_dynamodb, _bucket_initialize: None) -> None:  # type: ignore
    """Test catalog cache helper methods."""
    catalog = DynamoDbCatalog("test_catalog")

    # Test cache key generation
    key1 = catalog._get_cache_key(("db", "table"))
    assert key1 == "table:db.table"

    key2 = catalog._get_cache_key("db.table")
    assert key2 == "table:db.table"

    # Test cache invalidation helper (cache should be enabled by default)
    assert catalog._cache is not None
    catalog._cache.set(key1, "test_value")
    assert catalog._cache.get(key1) == "test_value"

    catalog._invalidate_cache(("db", "table"))
    assert catalog._cache.get(key1) is None


@mock_aws
def test_cache_on_load_table(
    _bucket_initialize: None, moto_endpoint_url: str, table_schema_nested: Schema, database_name: str, table_name: str
) -> None:
    """Test that load_table uses cache when enabled."""
    identifier = (database_name, table_name)
    catalog = DynamoDbCatalog(
        "test_catalog",
        **{
            "warehouse": f"s3://{BUCKET_NAME}",
            "s3.endpoint": moto_endpoint_url,
            "dynamodb.cache.enabled": "true",
        },
    )

    # Create table
    catalog.create_namespace(namespace=database_name)
    catalog.create_table(identifier, table_schema_nested)

    # First load - should populate cache
    table1 = catalog.load_table(identifier)
    cache_key = catalog._get_cache_key(identifier)
    assert catalog._cache is not None
    assert catalog._cache.get(cache_key) is not None

    # Second load - should use cache
    table2 = catalog.load_table(identifier)
    assert table1.metadata_location == table2.metadata_location


@mock_aws
def test_cache_invalidation_on_drop(
    _bucket_initialize: None, moto_endpoint_url: str, table_schema_nested: Schema, database_name: str, table_name: str
) -> None:
    """Test that cache is invalidated when table is dropped."""
    identifier = (database_name, table_name)
    catalog = DynamoDbCatalog(
        "test_catalog",
        **{
            "warehouse": f"s3://{BUCKET_NAME}",
            "s3.endpoint": moto_endpoint_url,
            "dynamodb.cache.enabled": "true",
        },
    )

    # Create and load table (populates cache)
    catalog.create_namespace(namespace=database_name)
    catalog.create_table(identifier, table_schema_nested)
    catalog.load_table(identifier)

    cache_key = catalog._get_cache_key(identifier)
    assert catalog._cache is not None
    assert catalog._cache.get(cache_key) is not None

    # Drop table - should invalidate cache
    catalog.drop_table(identifier)
    assert catalog._cache.get(cache_key) is None


@mock_aws
def test_retry_strategy_default_configuration(_dynamodb, _bucket_initialize: None) -> None:  # type: ignore
    """Test default retry strategy configuration."""
    catalog = DynamoDbCatalog("test_catalog")
    assert catalog._max_retries == 5
    assert catalog._retry_multiplier == 1.5
    assert catalog._retry_min_wait == 0.1  # 100ms converted to seconds
    assert catalog._retry_max_wait == 10.0  # 10000ms converted to seconds


@mock_aws
def test_retry_strategy_custom_configuration(_dynamodb, _bucket_initialize: None) -> None:  # type: ignore
    """Test custom retry strategy configuration."""
    properties: Properties = {
        "warehouse": f"s3://{BUCKET_NAME}",
        "dynamodb.max-retries": "3",
        "dynamodb.retry-multiplier": "2.0",
        "dynamodb.retry-min-wait-ms": "50",
        "dynamodb.retry-max-wait-ms": "5000",
    }
    catalog = DynamoDbCatalog("test_catalog", **properties)
    assert catalog._max_retries == 3
    assert catalog._retry_multiplier == 2.0
    assert catalog._retry_min_wait == 0.05  # 50ms
    assert catalog._retry_max_wait == 5.0  # 5000ms


@mock_aws
def test_all_enhancements_integrated(
    _dynamodb: Any,
    _bucket_initialize: None,
    moto_endpoint_url: str,
    table_schema_nested: Schema,
    database_name: str,
    table_name: str,
) -> None:
    """Test that all three enhancements work together."""
    events: List[str] = []

    def track_event(ctx: CatalogEventContext) -> None:
        events.append(ctx.event.value)

    identifier = (database_name, table_name)
    properties: Properties = {
        "warehouse": f"s3://{BUCKET_NAME}",
        "s3.endpoint": moto_endpoint_url,
        "dynamodb.cache.enabled": "true",
        "dynamodb.cache.ttl-seconds": "600",
        "dynamodb.max-retries": "3",
        "dynamodb.retry-multiplier": "2.0",
    }
    catalog = DynamoDbCatalog("test_catalog", **properties)

    # Register hooks
    catalog.register_hook(CatalogEvent.PRE_CREATE_TABLE, track_event)
    catalog.register_hook(CatalogEvent.POST_CREATE_TABLE, track_event)
    catalog.register_hook(CatalogEvent.PRE_DROP_TABLE, track_event)
    catalog.register_hook(CatalogEvent.POST_DROP_TABLE, track_event)

    # Verify all enhancements are active
    assert catalog._cache is not None, "Cache should be enabled"
    assert catalog._max_retries == 3, "Retry max should be 3"
    assert len(catalog._event_hooks[CatalogEvent.PRE_CREATE_TABLE]) == 1, "Hook should be registered"

    # Perform operations
    catalog.create_namespace(namespace=database_name)
    catalog.create_table(identifier, table_schema_nested)

    # Load table (should use cache)
    catalog.load_table(identifier)
    cache_key = catalog._get_cache_key(identifier)
    assert catalog._cache.get(cache_key) is not None

    # Drop table (should trigger hooks and invalidate cache)
    catalog.drop_table(identifier)
    assert catalog._cache.get(cache_key) is None

    # Verify hooks were triggered
    assert "pre_create_table" in events
    assert "post_create_table" in events
    assert "pre_drop_table" in events
    assert "post_drop_table" in events
