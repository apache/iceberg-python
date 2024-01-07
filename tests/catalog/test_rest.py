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
# pylint: disable=redefined-outer-name,unused-argument
import os
from typing import Any, Dict, cast
from unittest import mock

import pytest
from requests_mock import Mocker

import pyiceberg
from pyiceberg.catalog import PropertiesUpdateSummary, Table, load_catalog
from pyiceberg.catalog.rest import AUTH_URL, RestCatalog
from pyiceberg.exceptions import (
    NamespaceAlreadyExistsError,
    NoSuchNamespaceError,
    NoSuchTableError,
    OAuthError,
    TableAlreadyExistsError,
)
from pyiceberg.io import load_file_io
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table.metadata import TableMetadataV1
from pyiceberg.table.sorting import SortField, SortOrder
from pyiceberg.transforms import IdentityTransform, TruncateTransform
from pyiceberg.typedef import RecursiveDict
from pyiceberg.utils.config import Config

TEST_URI = "https://iceberg-test-catalog/"
TEST_CREDENTIALS = "client:secret"
TEST_AUTH_URL = "https://auth-endpoint/"
TEST_TOKEN = "some_jwt_token"
TEST_HEADERS = {
    "Content-type": "application/json",
    "X-Client-Version": "0.14.1",
    "User-Agent": f"PyIceberg/{pyiceberg.__version__}",
    "Authorization": f"Bearer {TEST_TOKEN}",
}
OAUTH_TEST_HEADERS = {
    "Content-type": "application/x-www-form-urlencoded",
}


@pytest.fixture
def example_table_metadata_with_snapshot_v1_rest_json(example_table_metadata_with_snapshot_v1: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "metadata-location": "s3://warehouse/database/table/metadata/00001-5f2f8166-244c-4eae-ac36-384ecdec81fc.gz.metadata.json",
        "metadata": example_table_metadata_with_snapshot_v1,
        "config": {
            "client.factory": "io.tabular.iceberg.catalog.TabularAwsClientFactory",
            "region": "us-west-2",
        },
    }


@pytest.fixture
def example_table_metadata_no_snapshot_v1_rest_json(example_table_metadata_no_snapshot_v1: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "metadata-location": "s3://warehouse/database/table/metadata.json",
        "metadata": example_table_metadata_no_snapshot_v1,
        "config": {
            "client.factory": "io.tabular.iceberg.catalog.TabularAwsClientFactory",
            "region": "us-west-2",
        },
    }


@pytest.fixture
def rest_mock(requests_mock: Mocker) -> Mocker:
    """Takes the default requests_mock and adds the config endpoint to it

    This endpoint is called when initializing the rest catalog
    """
    requests_mock.get(
        f"{TEST_URI}v1/config",
        json={"defaults": {}, "overrides": {}},
        status_code=200,
    )
    return requests_mock


def test_no_uri_supplied() -> None:
    with pytest.raises(KeyError):
        RestCatalog("production")


def test_token_200(rest_mock: Mocker) -> None:
    rest_mock.post(
        f"{TEST_URI}v1/oauth/tokens",
        json={
            "access_token": TEST_TOKEN,
            "token_type": "Bearer",
            "expires_in": 86400,
            "issued_token_type": "urn:ietf:params:oauth:token-type:access_token",
        },
        status_code=200,
        request_headers=OAUTH_TEST_HEADERS,
    )
    assert (
        RestCatalog("rest", uri=TEST_URI, credential=TEST_CREDENTIALS)._session.headers["Authorization"]  # pylint: disable=W0212
        == f"Bearer {TEST_TOKEN}"
    )


def test_token_200_w_auth_url(rest_mock: Mocker) -> None:
    rest_mock.post(
        TEST_AUTH_URL,
        json={
            "access_token": TEST_TOKEN,
            "token_type": "Bearer",
            "expires_in": 86400,
            "issued_token_type": "urn:ietf:params:oauth:token-type:access_token",
        },
        status_code=200,
        request_headers=OAUTH_TEST_HEADERS,
    )
    # pylint: disable=W0212
    assert (
        RestCatalog("rest", uri=TEST_URI, credential=TEST_CREDENTIALS, **{AUTH_URL: TEST_AUTH_URL})._session.headers[
            "Authorization"
        ]
        == f"Bearer {TEST_TOKEN}"
    )
    # pylint: enable=W0212


def test_config_200(requests_mock: Mocker) -> None:
    requests_mock.get(
        f"{TEST_URI}v1/config",
        json={"defaults": {}, "overrides": {}},
        status_code=200,
    )
    requests_mock.post(
        f"{TEST_URI}v1/oauth/tokens",
        json={
            "access_token": TEST_TOKEN,
            "token_type": "Bearer",
            "expires_in": 86400,
            "issued_token_type": "urn:ietf:params:oauth:token-type:access_token",
        },
        status_code=200,
        request_headers=OAUTH_TEST_HEADERS,
    )
    RestCatalog("rest", uri=TEST_URI, credential=TEST_CREDENTIALS, warehouse="s3://some-bucket")

    assert requests_mock.called
    assert requests_mock.call_count == 2

    history = requests_mock.request_history
    assert history[1].method == "GET"
    assert history[1].url == "https://iceberg-test-catalog/v1/config?warehouse=s3%3A%2F%2Fsome-bucket"


def test_token_400(rest_mock: Mocker) -> None:
    rest_mock.post(
        f"{TEST_URI}v1/oauth/tokens",
        json={"error": "invalid_client", "error_description": "Credentials for key invalid_key do not match"},
        status_code=400,
        request_headers=OAUTH_TEST_HEADERS,
    )

    with pytest.raises(OAuthError) as e:
        RestCatalog("rest", uri=TEST_URI, credential=TEST_CREDENTIALS)
    assert str(e.value) == "invalid_client: Credentials for key invalid_key do not match"


def test_token_401(rest_mock: Mocker) -> None:
    message = "invalid_client"
    rest_mock.post(
        f"{TEST_URI}v1/oauth/tokens",
        json={"error": "invalid_client", "error_description": "Unknown or invalid client"},
        status_code=401,
        request_headers=OAUTH_TEST_HEADERS,
    )

    with pytest.raises(OAuthError) as e:
        RestCatalog("rest", uri=TEST_URI, credential=TEST_CREDENTIALS)
    assert message in str(e.value)


def test_list_tables_200(rest_mock: Mocker) -> None:
    namespace = "examples"
    rest_mock.get(
        f"{TEST_URI}v1/namespaces/{namespace}/tables",
        json={"identifiers": [{"namespace": ["examples"], "name": "fooshare"}]},
        status_code=200,
        request_headers=TEST_HEADERS,
    )

    assert RestCatalog("rest", uri=TEST_URI, token=TEST_TOKEN).list_tables(namespace) == [("examples", "fooshare")]


def test_list_tables_200_sigv4(rest_mock: Mocker) -> None:
    namespace = "examples"
    rest_mock.get(
        f"{TEST_URI}v1/namespaces/{namespace}/tables",
        json={"identifiers": [{"namespace": ["examples"], "name": "fooshare"}]},
        status_code=200,
        request_headers=TEST_HEADERS,
    )

    assert RestCatalog("rest", **{"uri": TEST_URI, "token": TEST_TOKEN, "rest.sigv4-enabled": "true"}).list_tables(namespace) == [
        ("examples", "fooshare")
    ]
    assert rest_mock.called


def test_list_tables_404(rest_mock: Mocker) -> None:
    namespace = "examples"
    rest_mock.get(
        f"{TEST_URI}v1/namespaces/{namespace}/tables",
        json={
            "error": {
                "message": "Namespace does not exist: personal in warehouse 8bcb0838-50fc-472d-9ddb-8feb89ef5f1e",
                "type": "NoSuchNamespaceException",
                "code": 404,
            }
        },
        status_code=404,
        request_headers=TEST_HEADERS,
    )
    with pytest.raises(NoSuchNamespaceError) as e:
        RestCatalog("rest", uri=TEST_URI, token=TEST_TOKEN).list_tables(namespace)
    assert "Namespace does not exist" in str(e.value)


def test_list_namespaces_200(rest_mock: Mocker) -> None:
    rest_mock.get(
        f"{TEST_URI}v1/namespaces",
        json={"namespaces": [["default"], ["examples"], ["fokko"], ["system"]]},
        status_code=200,
        request_headers=TEST_HEADERS,
    )
    assert RestCatalog("rest", uri=TEST_URI, token=TEST_TOKEN).list_namespaces() == [
        ("default",),
        ("examples",),
        ("fokko",),
        ("system",),
    ]


def test_list_namespace_with_parent_200(rest_mock: Mocker) -> None:
    rest_mock.get(
        f"{TEST_URI}v1/namespaces?parent=accounting",
        json={"namespaces": [["tax"]]},
        status_code=200,
        request_headers=TEST_HEADERS,
    )
    assert RestCatalog("rest", uri=TEST_URI, token=TEST_TOKEN).list_namespaces(("accounting",)) == [
        ("accounting", "tax"),
    ]


def test_create_namespace_200(rest_mock: Mocker) -> None:
    namespace = "leden"
    rest_mock.post(
        f"{TEST_URI}v1/namespaces",
        json={"namespace": [namespace], "properties": {}},
        status_code=200,
        request_headers=TEST_HEADERS,
    )
    RestCatalog("rest", uri=TEST_URI, token=TEST_TOKEN).create_namespace(namespace)


def test_create_namespace_409(rest_mock: Mocker) -> None:
    namespace = "examples"
    rest_mock.post(
        f"{TEST_URI}v1/namespaces",
        json={
            "error": {
                "message": "Namespace already exists: fokko in warehouse 8bcb0838-50fc-472d-9ddb-8feb89ef5f1e",
                "type": "AlreadyExistsException",
                "code": 409,
            }
        },
        status_code=409,
        request_headers=TEST_HEADERS,
    )
    with pytest.raises(NamespaceAlreadyExistsError) as e:
        RestCatalog("rest", uri=TEST_URI, token=TEST_TOKEN).create_namespace(namespace)
    assert "Namespace already exists" in str(e.value)


def test_drop_namespace_404(rest_mock: Mocker) -> None:
    namespace = "examples"
    rest_mock.delete(
        f"{TEST_URI}v1/namespaces/{namespace}",
        json={
            "error": {
                "message": "Namespace does not exist: leden in warehouse 8bcb0838-50fc-472d-9ddb-8feb89ef5f1e",
                "type": "NoSuchNamespaceException",
                "code": 404,
            }
        },
        status_code=404,
        request_headers=TEST_HEADERS,
    )
    with pytest.raises(NoSuchNamespaceError) as e:
        RestCatalog("rest", uri=TEST_URI, token=TEST_TOKEN).drop_namespace(namespace)
    assert "Namespace does not exist" in str(e.value)


def test_load_namespace_properties_200(rest_mock: Mocker) -> None:
    namespace = "leden"
    rest_mock.get(
        f"{TEST_URI}v1/namespaces/{namespace}",
        json={"namespace": ["fokko"], "properties": {"prop": "yes"}},
        status_code=204,
        request_headers=TEST_HEADERS,
    )
    assert RestCatalog("rest", uri=TEST_URI, token=TEST_TOKEN).load_namespace_properties(namespace) == {"prop": "yes"}


def test_load_namespace_properties_404(rest_mock: Mocker) -> None:
    namespace = "leden"
    rest_mock.get(
        f"{TEST_URI}v1/namespaces/{namespace}",
        json={
            "error": {
                "message": "Namespace does not exist: fokko22 in warehouse 8bcb0838-50fc-472d-9ddb-8feb89ef5f1e",
                "type": "NoSuchNamespaceException",
                "code": 404,
            }
        },
        status_code=404,
        request_headers=TEST_HEADERS,
    )
    with pytest.raises(NoSuchNamespaceError) as e:
        RestCatalog("rest", uri=TEST_URI, token=TEST_TOKEN).load_namespace_properties(namespace)
    assert "Namespace does not exist" in str(e.value)


def test_update_namespace_properties_200(rest_mock: Mocker) -> None:
    rest_mock.post(
        f"{TEST_URI}v1/namespaces/fokko/properties",
        json={"removed": [], "updated": ["prop"], "missing": ["abc"]},
        status_code=200,
        request_headers=TEST_HEADERS,
    )
    response = RestCatalog("rest", uri=TEST_URI, token=TEST_TOKEN).update_namespace_properties(
        ("fokko",), {"abc"}, {"prop": "yes"}
    )

    assert response == PropertiesUpdateSummary(removed=[], updated=["prop"], missing=["abc"])


def test_update_namespace_properties_404(rest_mock: Mocker) -> None:
    rest_mock.post(
        f"{TEST_URI}v1/namespaces/fokko/properties",
        json={
            "error": {
                "message": "Namespace does not exist: does_not_exists in warehouse 8bcb0838-50fc-472d-9ddb-8feb89ef5f1e",
                "type": "NoSuchNamespaceException",
                "code": 404,
            }
        },
        status_code=404,
        request_headers=TEST_HEADERS,
    )
    with pytest.raises(NoSuchNamespaceError) as e:
        RestCatalog("rest", uri=TEST_URI, token=TEST_TOKEN).update_namespace_properties(("fokko",), {"abc"}, {"prop": "yes"})
    assert "Namespace does not exist" in str(e.value)


def test_load_table_200(rest_mock: Mocker, example_table_metadata_with_snapshot_v1_rest_json: Dict[str, Any]) -> None:
    rest_mock.get(
        f"{TEST_URI}v1/namespaces/fokko/tables/table",
        json=example_table_metadata_with_snapshot_v1_rest_json,
        status_code=200,
        request_headers=TEST_HEADERS,
    )
    catalog = RestCatalog("rest", uri=TEST_URI, token=TEST_TOKEN)
    actual = catalog.load_table(("fokko", "table"))
    expected = Table(
        identifier=("rest", "fokko", "table"),
        metadata_location=example_table_metadata_with_snapshot_v1_rest_json["metadata-location"],
        metadata=TableMetadataV1(**example_table_metadata_with_snapshot_v1_rest_json["metadata"]),
        io=load_file_io(),
        catalog=catalog,
    )
    # First compare the dicts
    assert actual.metadata.model_dump() == expected.metadata.model_dump()
    assert actual == expected


def test_load_table_from_self_identifier_200(
    rest_mock: Mocker, example_table_metadata_with_snapshot_v1_rest_json: Dict[str, Any]
) -> None:
    rest_mock.get(
        f"{TEST_URI}v1/namespaces/pdames/tables/table",
        json=example_table_metadata_with_snapshot_v1_rest_json,
        status_code=200,
        request_headers=TEST_HEADERS,
    )
    catalog = RestCatalog("rest", uri=TEST_URI, token=TEST_TOKEN)
    table = catalog.load_table(("pdames", "table"))
    actual = catalog.load_table(table.identifier)
    expected = Table(
        identifier=("rest", "pdames", "table"),
        metadata_location=example_table_metadata_with_snapshot_v1_rest_json["metadata-location"],
        metadata=TableMetadataV1(**example_table_metadata_with_snapshot_v1_rest_json["metadata"]),
        io=load_file_io(),
        catalog=catalog,
    )
    assert actual.metadata.model_dump() == expected.metadata.model_dump()
    assert actual == expected


def test_load_table_404(rest_mock: Mocker) -> None:
    rest_mock.get(
        f"{TEST_URI}v1/namespaces/fokko/tables/does_not_exists",
        json={
            "error": {
                "message": "Table does not exist: examples.does_not_exists in warehouse 8bcb0838-50fc-472d-9ddb-8feb89ef5f1e",
                "type": "NoSuchNamespaceErrorException",
                "code": 404,
            }
        },
        status_code=404,
        request_headers=TEST_HEADERS,
    )

    with pytest.raises(NoSuchTableError) as e:
        RestCatalog("rest", uri=TEST_URI, token=TEST_TOKEN).load_table(("fokko", "does_not_exists"))
    assert "Table does not exist" in str(e.value)


def test_drop_table_404(rest_mock: Mocker) -> None:
    rest_mock.delete(
        f"{TEST_URI}v1/namespaces/fokko/tables/does_not_exists",
        json={
            "error": {
                "message": "Table does not exist: fokko.does_not_exists in warehouse 8bcb0838-50fc-472d-9ddb-8feb89ef5f1e",
                "type": "NoSuchNamespaceErrorException",
                "code": 404,
            }
        },
        status_code=404,
        request_headers=TEST_HEADERS,
    )

    with pytest.raises(NoSuchTableError) as e:
        RestCatalog("rest", uri=TEST_URI, token=TEST_TOKEN).drop_table(("fokko", "does_not_exists"))
    assert "Table does not exist" in str(e.value)


def test_create_table_200(
    rest_mock: Mocker, table_schema_simple: Schema, example_table_metadata_no_snapshot_v1_rest_json: Dict[str, Any]
) -> None:
    rest_mock.post(
        f"{TEST_URI}v1/namespaces/fokko/tables",
        json=example_table_metadata_no_snapshot_v1_rest_json,
        status_code=200,
        request_headers=TEST_HEADERS,
    )
    catalog = RestCatalog("rest", uri=TEST_URI, token=TEST_TOKEN)
    actual = catalog.create_table(
        identifier=("fokko", "fokko2"),
        schema=table_schema_simple,
        location=None,
        partition_spec=PartitionSpec(
            PartitionField(source_id=1, field_id=1000, transform=TruncateTransform(width=3), name="id"), spec_id=1
        ),
        sort_order=SortOrder(SortField(source_id=2, transform=IdentityTransform())),
        properties={"owner": "fokko"},
    )
    expected = Table(
        identifier=("rest", "fokko", "fokko2"),
        metadata_location=example_table_metadata_no_snapshot_v1_rest_json["metadata-location"],
        metadata=TableMetadataV1(**example_table_metadata_no_snapshot_v1_rest_json["metadata"]),
        io=load_file_io(),
        catalog=catalog,
    )
    assert actual == expected


def test_create_table_409(rest_mock: Mocker, table_schema_simple: Schema) -> None:
    rest_mock.post(
        f"{TEST_URI}v1/namespaces/fokko/tables",
        json={
            "error": {
                "message": "Table already exists: fokko.already_exists in warehouse 8bcb0838-50fc-472d-9ddb-8feb89ef5f1e",
                "type": "AlreadyExistsException",
                "code": 409,
            }
        },
        status_code=409,
        request_headers=TEST_HEADERS,
    )

    with pytest.raises(TableAlreadyExistsError) as e:
        RestCatalog("rest", uri=TEST_URI, token=TEST_TOKEN).create_table(
            identifier=("fokko", "fokko2"),
            schema=table_schema_simple,
            location=None,
            partition_spec=PartitionSpec(
                PartitionField(source_id=1, field_id=1000, transform=TruncateTransform(width=3), name="id")
            ),
            sort_order=SortOrder(SortField(source_id=2, transform=IdentityTransform())),
            properties={"owner": "fokko"},
        )
    assert "Table already exists" in str(e.value)


def test_register_table_200(
    rest_mock: Mocker, table_schema_simple: Schema, example_table_metadata_no_snapshot_v1_rest_json: Dict[str, Any]
) -> None:
    rest_mock.post(
        f"{TEST_URI}v1/namespaces/default/register",
        json=example_table_metadata_no_snapshot_v1_rest_json,
        status_code=200,
        request_headers=TEST_HEADERS,
    )
    catalog = RestCatalog("rest", uri=TEST_URI, token=TEST_TOKEN)
    actual = catalog.register_table(
        identifier=("default", "registered_table"), metadata_location="s3://warehouse/database/table/metadata.json"
    )
    expected = Table(
        identifier=("rest", "default", "registered_table"),
        metadata_location=example_table_metadata_no_snapshot_v1_rest_json["metadata-location"],
        metadata=TableMetadataV1(**example_table_metadata_no_snapshot_v1_rest_json["metadata"]),
        io=load_file_io(),
        catalog=catalog,
    )
    assert actual.metadata.model_dump() == expected.metadata.model_dump()
    assert actual.metadata_location == expected.metadata_location
    assert actual.identifier == expected.identifier


def test_register_table_409(rest_mock: Mocker, table_schema_simple: Schema) -> None:
    rest_mock.post(
        f"{TEST_URI}v1/namespaces/default/register",
        json={
            "error": {
                "message": "Table already exists: fokko.fokko2 in warehouse 8bcb0838-50fc-472d-9ddb-8feb89ef5f1e",
                "type": "AlreadyExistsException",
                "code": 409,
            }
        },
        status_code=409,
        request_headers=TEST_HEADERS,
    )

    catalog = RestCatalog("rest", uri=TEST_URI, token=TEST_TOKEN)
    with pytest.raises(TableAlreadyExistsError) as e:
        catalog.register_table(
            identifier=("default", "registered_table"), metadata_location="s3://warehouse/database/table/metadata.json"
        )
    assert "Table already exists" in str(e.value)


def test_delete_namespace_204(rest_mock: Mocker) -> None:
    namespace = "example"
    rest_mock.delete(
        f"{TEST_URI}v1/namespaces/{namespace}",
        json={},
        status_code=204,
        request_headers=TEST_HEADERS,
    )
    RestCatalog("rest", uri=TEST_URI, token=TEST_TOKEN).drop_namespace(namespace)


def test_delete_table_204(rest_mock: Mocker) -> None:
    rest_mock.delete(
        f"{TEST_URI}v1/namespaces/example/tables/fokko",
        json={},
        status_code=204,
        request_headers=TEST_HEADERS,
    )
    RestCatalog("rest", uri=TEST_URI, token=TEST_TOKEN).drop_table(("example", "fokko"))


def test_delete_table_from_self_identifier_204(
    rest_mock: Mocker, example_table_metadata_with_snapshot_v1_rest_json: Dict[str, Any]
) -> None:
    rest_mock.get(
        f"{TEST_URI}v1/namespaces/pdames/tables/table",
        json=example_table_metadata_with_snapshot_v1_rest_json,
        status_code=200,
        request_headers=TEST_HEADERS,
    )
    catalog = RestCatalog("rest", uri=TEST_URI, token=TEST_TOKEN)
    table = catalog.load_table(("pdames", "table"))
    rest_mock.delete(
        f"{TEST_URI}v1/namespaces/pdames/tables/table",
        json={},
        status_code=204,
        request_headers=TEST_HEADERS,
    )
    catalog.drop_table(table.identifier)


def test_rename_table_200(rest_mock: Mocker, example_table_metadata_with_snapshot_v1_rest_json: Dict[str, Any]) -> None:
    rest_mock.post(
        f"{TEST_URI}v1/tables/rename",
        json={
            "source": {"namespace": ("pdames",), "name": "source"},
            "destination": {"namespace": ("pdames",), "name": "destination"},
        },
        status_code=200,
        request_headers=TEST_HEADERS,
    )
    rest_mock.get(
        f"{TEST_URI}v1/namespaces/pdames/tables/destination",
        json=example_table_metadata_with_snapshot_v1_rest_json,
        status_code=200,
        request_headers=TEST_HEADERS,
    )
    catalog = RestCatalog("rest", uri=TEST_URI, token=TEST_TOKEN)
    from_identifier = ("pdames", "source")
    to_identifier = ("pdames", "destination")
    actual = catalog.rename_table(from_identifier, to_identifier)
    expected = Table(
        identifier=("rest", "pdames", "destination"),
        metadata_location=example_table_metadata_with_snapshot_v1_rest_json["metadata-location"],
        metadata=TableMetadataV1(**example_table_metadata_with_snapshot_v1_rest_json["metadata"]),
        io=load_file_io(),
        catalog=catalog,
    )
    assert actual.metadata.model_dump() == expected.metadata.model_dump()
    assert actual == expected


def test_rename_table_from_self_identifier_200(
    rest_mock: Mocker, example_table_metadata_with_snapshot_v1_rest_json: Dict[str, Any]
) -> None:
    rest_mock.get(
        f"{TEST_URI}v1/namespaces/pdames/tables/source",
        json=example_table_metadata_with_snapshot_v1_rest_json,
        status_code=200,
        request_headers=TEST_HEADERS,
    )
    catalog = RestCatalog("rest", uri=TEST_URI, token=TEST_TOKEN)
    from_identifier = ("pdames", "source")
    to_identifier = ("pdames", "destination")
    table = catalog.load_table(from_identifier)
    rest_mock.post(
        f"{TEST_URI}v1/tables/rename",
        json={
            "source": {"namespace": ("pdames",), "name": "source"},
            "destination": {"namespace": ("pdames",), "name": "destination"},
        },
        status_code=200,
        request_headers=TEST_HEADERS,
    )
    rest_mock.get(
        f"{TEST_URI}v1/namespaces/pdames/tables/destination",
        json=example_table_metadata_with_snapshot_v1_rest_json,
        status_code=200,
        request_headers=TEST_HEADERS,
    )
    actual = catalog.rename_table(table.identifier, to_identifier)
    expected = Table(
        identifier=("rest", "pdames", "destination"),
        metadata_location=example_table_metadata_with_snapshot_v1_rest_json["metadata-location"],
        metadata=TableMetadataV1(**example_table_metadata_with_snapshot_v1_rest_json["metadata"]),
        io=load_file_io(),
        catalog=catalog,
    )
    assert actual.metadata.model_dump() == expected.metadata.model_dump()
    assert actual == expected


def test_delete_table_404(rest_mock: Mocker) -> None:
    rest_mock.delete(
        f"{TEST_URI}v1/namespaces/example/tables/fokko",
        json={
            "error": {
                "message": "Table does not exist: fokko.fokko2 in warehouse 8bcb0838-50fc-472d-9ddb-8feb89ef5f1e",
                "type": "NoSuchTableException",
                "code": 404,
            }
        },
        status_code=404,
        request_headers=TEST_HEADERS,
    )
    with pytest.raises(NoSuchTableError) as e:
        RestCatalog("rest", uri=TEST_URI, token=TEST_TOKEN).drop_table(("example", "fokko"))
    assert "Table does not exist" in str(e.value)


def test_create_table_missing_namespace(rest_mock: Mocker, table_schema_simple: Schema) -> None:
    table = "table"
    with pytest.raises(NoSuchTableError) as e:
        # Missing namespace
        RestCatalog("rest", uri=TEST_URI, token=TEST_TOKEN).create_table(table, table_schema_simple)
    assert f"Missing namespace or invalid identifier: {table}" in str(e.value)


def test_load_table_invalid_namespace(rest_mock: Mocker) -> None:
    table = "table"
    with pytest.raises(NoSuchTableError) as e:
        # Missing namespace
        RestCatalog("rest", uri=TEST_URI, token=TEST_TOKEN).load_table(table)
    assert f"Missing namespace or invalid identifier: {table}" in str(e.value)


def test_drop_table_invalid_namespace(rest_mock: Mocker) -> None:
    table = "table"
    with pytest.raises(NoSuchTableError) as e:
        # Missing namespace
        RestCatalog("rest", uri=TEST_URI, token=TEST_TOKEN).drop_table(table)
    assert f"Missing namespace or invalid identifier: {table}" in str(e.value)


def test_purge_table_invalid_namespace(rest_mock: Mocker) -> None:
    table = "table"
    with pytest.raises(NoSuchTableError) as e:
        # Missing namespace
        RestCatalog("rest", uri=TEST_URI, token=TEST_TOKEN).purge_table(table)
    assert f"Missing namespace or invalid identifier: {table}" in str(e.value)


def test_create_namespace_invalid_namespace(rest_mock: Mocker) -> None:
    with pytest.raises(NoSuchNamespaceError) as e:
        # Missing namespace
        RestCatalog("rest", uri=TEST_URI, token=TEST_TOKEN).create_namespace(())
    assert "Empty namespace identifier" in str(e.value)


def test_drop_namespace_invalid_namespace(rest_mock: Mocker) -> None:
    with pytest.raises(NoSuchNamespaceError) as e:
        # Missing namespace
        RestCatalog("rest", uri=TEST_URI, token=TEST_TOKEN).drop_namespace(())
    assert "Empty namespace identifier" in str(e.value)


def test_load_namespace_properties_invalid_namespace(rest_mock: Mocker) -> None:
    with pytest.raises(NoSuchNamespaceError) as e:
        # Missing namespace
        RestCatalog("rest", uri=TEST_URI, token=TEST_TOKEN).load_namespace_properties(())
    assert "Empty namespace identifier" in str(e.value)


def test_update_namespace_properties_invalid_namespace(rest_mock: Mocker) -> None:
    with pytest.raises(NoSuchNamespaceError) as e:
        # Missing namespace
        RestCatalog("rest", uri=TEST_URI, token=TEST_TOKEN).update_namespace_properties(())
    assert "Empty namespace identifier" in str(e.value)


def test_request_session_with_ssl_ca_bundle() -> None:
    # Given
    catalog_properties = {
        "uri": TEST_URI,
        "token": TEST_TOKEN,
        "ssl": {
            "cabundle": "path_to_ca_bundle",
        },
    }
    with pytest.raises(OSError) as e:
        # Missing namespace
        RestCatalog("rest", **catalog_properties)  # type: ignore
    assert "Could not find a suitable TLS CA certificate bundle, invalid path: path_to_ca_bundle" in str(e.value)


def test_request_session_with_ssl_client_cert() -> None:
    # Given
    catalog_properties = {
        "uri": TEST_URI,
        "token": TEST_TOKEN,
        "ssl": {
            "client": {
                "cert": "path_to_client_cert",
                "key": "path_to_client_key",
            }
        },
    }
    with pytest.raises(OSError) as e:
        # Missing namespace
        RestCatalog("rest", **catalog_properties)  # type: ignore
    assert "Could not find the TLS certificate file, invalid path: path_to_client_cert" in str(e.value)


EXAMPLE_ENV = {"PYICEBERG_CATALOG__PRODUCTION__URI": TEST_URI}


@mock.patch.dict(os.environ, EXAMPLE_ENV)
@mock.patch("pyiceberg.catalog.Config.get_catalog_config")
def test_catalog_from_environment_variables(catalog_config_mock: mock.Mock, rest_mock: Mocker) -> None:
    env_config: RecursiveDict = Config._from_environment_variables({})
    catalog_config_mock.return_value = cast(RecursiveDict, env_config.get("catalog")).get("production")
    catalog = cast(RestCatalog, load_catalog("production"))
    assert catalog.uri == TEST_URI


@mock.patch.dict(os.environ, EXAMPLE_ENV)
@mock.patch("pyiceberg.catalog._ENV_CONFIG.get_catalog_config")
def test_catalog_from_environment_variables_override(catalog_config_mock: mock.Mock, rest_mock: Mocker) -> None:
    rest_mock.get(
        "https://other-service.io/api/v1/config",
        json={"defaults": {}, "overrides": {}},
        status_code=200,
    )
    env_config: RecursiveDict = Config._from_environment_variables({})

    catalog_config_mock.return_value = cast(RecursiveDict, env_config.get("catalog")).get("production")
    catalog = cast(RestCatalog, load_catalog("production", uri="https://other-service.io/api"))
    assert catalog.uri == "https://other-service.io/api"


def test_catalog_from_parameters_empty_env(rest_mock: Mocker) -> None:
    rest_mock.get(
        "https://other-service.io/api/v1/config",
        json={"defaults": {}, "overrides": {}},
        status_code=200,
    )

    catalog = cast(RestCatalog, load_catalog("production", uri="https://other-service.io/api"))
    assert catalog.uri == "https://other-service.io/api"
