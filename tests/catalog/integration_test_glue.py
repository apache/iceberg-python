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

import time
from typing import Any, Dict, Generator, List
from uuid import uuid4

import boto3
import pyarrow as pa
import pytest
from botocore.exceptions import ClientError

from pyiceberg.catalog import Catalog
from pyiceberg.catalog.glue import GlueCatalog
from pyiceberg.exceptions import (
    NamespaceAlreadyExistsError,
    NamespaceNotEmptyError,
    NoSuchNamespaceError,
    NoSuchTableError,
    TableAlreadyExistsError,
)
from pyiceberg.io.pyarrow import schema_to_pyarrow
from pyiceberg.schema import Schema
from pyiceberg.types import IntegerType
from tests.conftest import clean_up, get_bucket_name, get_s3_path

# The number of tables/databases used in list_table/namespace test
LIST_TEST_NUMBER = 2
CATALOG_NAME = "glue"


@pytest.fixture(name="glue", scope="module")
def fixture_glue_client() -> boto3.client:
    yield boto3.client("glue")


@pytest.fixture(name="test_catalog", scope="module")
def fixture_test_catalog() -> Generator[Catalog, None, None]:
    """Configure the pre- and post-setting of aws integration test."""
    test_catalog = GlueCatalog(CATALOG_NAME, warehouse=get_s3_path(get_bucket_name()))
    yield test_catalog
    clean_up(test_catalog)


class AthenaQueryHelper:
    _athena_client: boto3.client
    _s3_resource: boto3.resource
    _output_bucket: str
    _output_path: str

    def __init__(self) -> None:
        self._s3_resource = boto3.resource("s3")
        self._athena_client = boto3.client("athena")
        self._output_bucket = get_bucket_name()
        self._output_path = f"athena_results_{uuid4()}"

    def get_query_results(self, query: str) -> List[Dict[str, Any]]:
        query_execution_id = self._athena_client.start_query_execution(
            QueryString=query, ResultConfiguration={"OutputLocation": f"s3://{self._output_bucket}/{self._output_path}"}
        )["QueryExecutionId"]

        while True:
            result = self._athena_client.get_query_execution(QueryExecutionId=query_execution_id)["QueryExecution"]["Status"]
            query_status = result["State"]
            assert query_status not in [
                "FAILED",
                "CANCELLED",
            ], f"""
   Athena query with the string failed or was cancelled:
   Query: {query}
   Status: {query_status}
   Reason: {result["StateChangeReason"]}"""

            if query_status not in ["QUEUED", "RUNNING"]:
                break
            time.sleep(0.5)

        # No pagination for now, assume that we are not doing large queries
        return self._athena_client.get_query_results(QueryExecutionId=query_execution_id)["ResultSet"]["Rows"]

    def clean_up(self) -> None:
        bucket = self._s3_resource.Bucket(self._output_bucket)
        for obj in bucket.objects.filter(Prefix=f"{self._output_path}/"):
            self._s3_resource.Object(bucket.name, obj.key).delete()


@pytest.fixture(name="athena", scope="module")
def fixture_athena_helper() -> Generator[AthenaQueryHelper, None, None]:
    query_helper = AthenaQueryHelper()
    yield query_helper
    query_helper.clean_up()


def test_create_table(
    test_catalog: Catalog,
    s3: boto3.client,
    table_schema_nested: Schema,
    table_name: str,
    database_name: str,
    athena: AthenaQueryHelper,
) -> None:
    identifier = (database_name, table_name)
    test_catalog.create_namespace(database_name)
    test_catalog.create_table(identifier, table_schema_nested, get_s3_path(get_bucket_name(), database_name, table_name))
    table = test_catalog.load_table(identifier)
    assert table.identifier == (CATALOG_NAME,) + identifier
    metadata_location = table.metadata_location.split(get_bucket_name())[1][1:]
    s3.head_object(Bucket=get_bucket_name(), Key=metadata_location)
    assert test_catalog._parse_metadata_version(table.metadata_location) == 0

    table.append(
        pa.Table.from_pylist(
            [
                {
                    "foo": "foo_val",
                    "bar": 1,
                    "baz": False,
                    "qux": ["x", "y"],
                    "quux": {"key": {"subkey": 2}},
                    "location": [{"latitude": 1.1}],
                    "person": {"name": "some_name", "age": 23},
                }
            ],
            schema=schema_to_pyarrow(table.schema()),
        ),
    )

    assert athena.get_query_results(f'SELECT * FROM "{database_name}"."{table_name}"') == [
        {
            "Data": [
                {"VarCharValue": "foo"},
                {"VarCharValue": "bar"},
                {"VarCharValue": "baz"},
                {"VarCharValue": "qux"},
                {"VarCharValue": "quux"},
                {"VarCharValue": "location"},
                {"VarCharValue": "person"},
            ]
        },
        {
            "Data": [
                {"VarCharValue": "foo_val"},
                {"VarCharValue": "1"},
                {"VarCharValue": "false"},
                {"VarCharValue": "[x, y]"},
                {"VarCharValue": "{key={subkey=2}}"},
                {"VarCharValue": "[{latitude=1.1, longitude=null}]"},
                {"VarCharValue": "{name=some_name, age=23}"},
            ]
        },
    ]


def test_create_table_with_invalid_location(table_schema_nested: Schema, table_name: str, database_name: str) -> None:
    identifier = (database_name, table_name)
    test_catalog_no_warehouse = GlueCatalog("glue")
    test_catalog_no_warehouse.create_namespace(database_name)
    with pytest.raises(ValueError):
        test_catalog_no_warehouse.create_table(identifier, table_schema_nested)
    test_catalog_no_warehouse.drop_namespace(database_name)


def test_create_table_with_default_location(
    test_catalog: Catalog, s3: boto3.client, table_schema_nested: Schema, table_name: str, database_name: str
) -> None:
    identifier = (database_name, table_name)
    test_catalog.create_namespace(database_name)
    test_catalog.create_table(identifier, table_schema_nested)
    table = test_catalog.load_table(identifier)
    assert table.identifier == (CATALOG_NAME,) + identifier
    metadata_location = table.metadata_location.split(get_bucket_name())[1][1:]
    s3.head_object(Bucket=get_bucket_name(), Key=metadata_location)
    assert test_catalog._parse_metadata_version(table.metadata_location) == 0


def test_create_table_with_invalid_database(test_catalog: Catalog, table_schema_nested: Schema, table_name: str) -> None:
    identifier = ("invalid", table_name)
    with pytest.raises(NoSuchNamespaceError):
        test_catalog.create_table(identifier, table_schema_nested)


def test_create_duplicated_table(test_catalog: Catalog, table_schema_nested: Schema, table_name: str, database_name: str) -> None:
    test_catalog.create_namespace(database_name)
    test_catalog.create_table((database_name, table_name), table_schema_nested)
    with pytest.raises(TableAlreadyExistsError):
        test_catalog.create_table((database_name, table_name), table_schema_nested)


def test_load_table(test_catalog: Catalog, table_schema_nested: Schema, table_name: str, database_name: str) -> None:
    identifier = (database_name, table_name)
    test_catalog.create_namespace(database_name)
    table = test_catalog.create_table(identifier, table_schema_nested)
    loaded_table = test_catalog.load_table(identifier)
    assert table.identifier == loaded_table.identifier
    assert table.metadata_location == loaded_table.metadata_location
    assert table.metadata == loaded_table.metadata
    assert test_catalog._parse_metadata_version(table.metadata_location) == 0


def test_list_tables(test_catalog: Catalog, table_schema_nested: Schema, database_name: str, table_list: List[str]) -> None:
    test_catalog.create_namespace(database_name)
    for table_name in table_list:
        test_catalog.create_table((database_name, table_name), table_schema_nested)
    identifier_list = test_catalog.list_tables(database_name)
    assert len(identifier_list) == LIST_TEST_NUMBER
    for table_name in table_list:
        assert (database_name, table_name) in identifier_list


def test_rename_table(
    test_catalog: Catalog, s3: boto3.client, table_schema_nested: Schema, table_name: str, database_name: str
) -> None:
    new_database_name = f"{database_name}_new"
    test_catalog.create_namespace(database_name)
    test_catalog.create_namespace(new_database_name)
    new_table_name = f"rename-{table_name}"
    identifier = (database_name, table_name)
    table = test_catalog.create_table(identifier, table_schema_nested)
    assert test_catalog._parse_metadata_version(table.metadata_location) == 0
    assert table.identifier == (CATALOG_NAME,) + identifier
    new_identifier = (new_database_name, new_table_name)
    test_catalog.rename_table(identifier, new_identifier)
    new_table = test_catalog.load_table(new_identifier)
    assert new_table.identifier == (CATALOG_NAME,) + new_identifier
    assert new_table.metadata_location == table.metadata_location
    metadata_location = new_table.metadata_location.split(get_bucket_name())[1][1:]
    s3.head_object(Bucket=get_bucket_name(), Key=metadata_location)
    with pytest.raises(NoSuchTableError):
        test_catalog.load_table(identifier)


def test_drop_table(test_catalog: Catalog, table_schema_nested: Schema, table_name: str, database_name: str) -> None:
    identifier = (database_name, table_name)
    test_catalog.create_namespace(database_name)
    table = test_catalog.create_table(identifier, table_schema_nested)
    assert table.identifier == (CATALOG_NAME,) + identifier
    test_catalog.drop_table(identifier)
    with pytest.raises(NoSuchTableError):
        test_catalog.load_table(identifier)


def test_purge_table(
    test_catalog: Catalog, s3: boto3.client, table_schema_nested: Schema, table_name: str, database_name: str
) -> None:
    identifier = (database_name, table_name)
    test_catalog.create_namespace(database_name)
    test_catalog.create_table(identifier, table_schema_nested)
    table = test_catalog.load_table(identifier)
    assert table.identifier == (CATALOG_NAME,) + identifier
    metadata_location = table.metadata_location.split(get_bucket_name())[1][1:]
    s3.head_object(Bucket=get_bucket_name(), Key=metadata_location)
    test_catalog.purge_table(identifier)
    with pytest.raises(NoSuchTableError):
        test_catalog.load_table(identifier)
    with pytest.raises(ClientError):
        s3.head_object(Bucket=get_bucket_name(), Key=metadata_location)


def test_create_namespace(test_catalog: Catalog, database_name: str) -> None:
    test_catalog.create_namespace(database_name)
    assert (database_name,) in test_catalog.list_namespaces()


def test_create_duplicate_namespace(test_catalog: Catalog, database_name: str) -> None:
    test_catalog.create_namespace(database_name)
    with pytest.raises(NamespaceAlreadyExistsError):
        test_catalog.create_namespace(database_name)


def test_create_namespace_with_comment_and_location(test_catalog: Catalog, database_name: str) -> None:
    test_location = get_s3_path(get_bucket_name(), database_name)
    test_properties = {
        "comment": "this is a test description",
        "location": test_location,
    }
    test_catalog.create_namespace(namespace=database_name, properties=test_properties)
    loaded_database_list = test_catalog.list_namespaces()
    assert (database_name,) in loaded_database_list
    properties = test_catalog.load_namespace_properties(database_name)
    assert properties["comment"] == "this is a test description"
    assert properties["location"] == test_location


def test_list_namespaces(test_catalog: Catalog, database_list: List[str]) -> None:
    for database_name in database_list:
        test_catalog.create_namespace(database_name)
    db_list = test_catalog.list_namespaces()
    for database_name in database_list:
        assert (database_name,) in db_list
    assert len(test_catalog.list_namespaces(list(database_list)[0])) == 0


def test_drop_namespace(test_catalog: Catalog, table_schema_nested: Schema, database_name: str, table_name: str) -> None:
    test_catalog.create_namespace(database_name)
    assert (database_name,) in test_catalog.list_namespaces()
    test_catalog.create_table((database_name, table_name), table_schema_nested)
    with pytest.raises(NamespaceNotEmptyError):
        test_catalog.drop_namespace(database_name)
    test_catalog.drop_table((database_name, table_name))
    test_catalog.drop_namespace(database_name)
    assert (database_name,) not in test_catalog.list_namespaces()


def test_load_namespace_properties(test_catalog: Catalog, database_name: str) -> None:
    warehouse_location = get_s3_path(get_bucket_name())
    test_properties = {
        "comment": "this is a test description",
        "location": f"{warehouse_location}/{database_name}.db",
        "test_property1": "1",
        "test_property2": "2",
        "test_property3": "3",
    }

    test_catalog.create_namespace(database_name, test_properties)
    listed_properties = test_catalog.load_namespace_properties(database_name)
    for k, v in listed_properties.items():
        assert k in test_properties
        assert v == test_properties[k]


def test_load_empty_namespace_properties(test_catalog: Catalog, database_name: str) -> None:
    test_catalog.create_namespace(database_name)
    listed_properties = test_catalog.load_namespace_properties(database_name)
    assert listed_properties == {}


def test_load_default_namespace_properties(test_catalog: Catalog, glue: boto3.client, database_name: str) -> None:
    # simulate creating database with default settings through AWS Glue Web Console
    glue.create_database(DatabaseInput={"Name": database_name})
    listed_properties = test_catalog.load_namespace_properties(database_name)
    assert listed_properties == {}


def test_update_namespace_properties(test_catalog: Catalog, database_name: str) -> None:
    warehouse_location = get_s3_path(get_bucket_name())
    test_properties = {
        "comment": "this is a test description",
        "location": f"{warehouse_location}/{database_name}.db",
        "test_property1": "1",
        "test_property2": "2",
        "test_property3": "3",
    }
    removals = {"test_property1", "test_property2", "test_property3", "should_not_removed"}
    updates = {"test_property4": "4", "test_property5": "5", "comment": "updated test description"}
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


def test_commit_table_update_schema(
    test_catalog: Catalog, table_schema_nested: Schema, database_name: str, table_name: str, athena: AthenaQueryHelper
) -> None:
    identifier = (database_name, table_name)
    test_catalog.create_namespace(namespace=database_name)
    table = test_catalog.create_table(identifier, table_schema_nested)
    original_table_metadata = table.metadata

    assert test_catalog._parse_metadata_version(table.metadata_location) == 0
    assert original_table_metadata.current_schema_id == 0

    assert athena.get_query_results(f'SELECT * FROM "{database_name}"."{table_name}"') == [
        {
            "Data": [
                {"VarCharValue": "foo"},
                {"VarCharValue": "bar"},
                {"VarCharValue": "baz"},
                {"VarCharValue": "qux"},
                {"VarCharValue": "quux"},
                {"VarCharValue": "location"},
                {"VarCharValue": "person"},
            ]
        }
    ]

    transaction = table.transaction()
    update = transaction.update_schema()
    update.add_column(path="b", field_type=IntegerType())
    update.commit()
    transaction.commit_transaction()

    updated_table_metadata = table.metadata

    assert test_catalog._parse_metadata_version(table.metadata_location) == 1
    assert updated_table_metadata.current_schema_id == 1
    assert len(updated_table_metadata.schemas) == 2
    new_schema = next(schema for schema in updated_table_metadata.schemas if schema.schema_id == 1)
    assert new_schema
    assert new_schema == update._apply()
    assert new_schema.find_field("b").field_type == IntegerType()

    table.append(
        pa.Table.from_pylist(
            [
                {
                    "foo": "foo_val",
                    "bar": 1,
                    "location": [{"latitude": 1.1}],
                    "person": {"name": "some_name", "age": 23},
                    "b": 2,
                }
            ],
            schema=schema_to_pyarrow(new_schema),
        ),
    )

    assert athena.get_query_results(f'SELECT * FROM "{database_name}"."{table_name}"') == [
        {
            "Data": [
                {"VarCharValue": "foo"},
                {"VarCharValue": "bar"},
                {"VarCharValue": "baz"},
                {"VarCharValue": "qux"},
                {"VarCharValue": "quux"},
                {"VarCharValue": "location"},
                {"VarCharValue": "person"},
                {"VarCharValue": "b"},
            ]
        },
        {
            "Data": [
                {"VarCharValue": "foo_val"},
                {"VarCharValue": "1"},
                {},
                {"VarCharValue": "[]"},
                {"VarCharValue": "{}"},
                {"VarCharValue": "[{latitude=1.1, longitude=null}]"},
                {"VarCharValue": "{name=some_name, age=23}"},
                {"VarCharValue": "2"},
            ]
        },
    ]


def test_commit_table_properties(test_catalog: Catalog, table_schema_nested: Schema, database_name: str, table_name: str) -> None:
    identifier = (database_name, table_name)
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
