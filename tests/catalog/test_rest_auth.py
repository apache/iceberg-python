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

import base64
from pathlib import Path
from typing import cast
from unittest.mock import MagicMock, call, patch

import pytest
import requests
from requests_mock import Mocker

from pyiceberg.catalog import load_catalog
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.catalog.rest.auth import (
    AuthManagerAdapter,
    BasicAuthManager,
    EntraAuthManager,
    GoogleAuthManager,
    NoopAuthManager,
)
from pyiceberg.typedef import Properties
from pyiceberg.utils.config import Config

TEST_URI = "https://iceberg-test-catalog/"
GOOGLE_CREDS_URI = "https://oauth2.googleapis.com/token"


def _assert_load_catalog_auth_config_from_yaml(
    yaml_config: str,
    tmp_path: Path,
    requests_mock: Mocker,
) -> tuple[str, Properties]:
    (tmp_path / ".pyiceberg.yaml").write_text(yaml_config, encoding="utf-8")
    with patch.dict("os.environ", {"PYICEBERG_HOME": str(tmp_path)}, clear=True):
        config = Config()

    requests_mock.get(f"{TEST_URI}v1/config", json={"defaults": {}, "overrides": {}}, status_code=200)
    fake_auth_manager = MagicMock()
    fake_auth_manager.auth_header.return_value = None

    with (
        patch("pyiceberg.catalog._ENV_CONFIG", config),
        patch("pyiceberg.catalog.rest.AuthManagerFactory.create", return_value=fake_auth_manager) as create_auth_manager,
    ):
        catalog = load_catalog("default", type="rest", uri=TEST_URI)

    assert isinstance(catalog, RestCatalog)
    assert create_auth_manager.call_args_list
    configured_auth_manager_call = create_auth_manager.call_args_list[0]
    assert all(auth_manager_call == configured_auth_manager_call for auth_manager_call in create_auth_manager.call_args_list)
    configured_manager, configured_properties = configured_auth_manager_call.args
    assert isinstance(configured_manager, str)
    assert isinstance(configured_properties, dict)
    return configured_manager, cast(Properties, configured_properties)


def _assert_load_catalog_auth_config_from_environment(
    environment: dict[str, str],
    requests_mock: Mocker,
) -> tuple[str, Properties]:
    requests_mock.get(f"{TEST_URI}v1/config", json={"defaults": {}, "overrides": {}}, status_code=200)
    fake_auth_manager = MagicMock()
    fake_auth_manager.auth_header.return_value = None

    with patch.dict("os.environ", environment, clear=True), patch.object(Config, "_from_configuration_files", return_value=None):
        config = Config()
        with (
            patch("pyiceberg.catalog._ENV_CONFIG", config),
            patch("pyiceberg.catalog.rest.AuthManagerFactory.create", return_value=fake_auth_manager) as create_auth_manager,
        ):
            catalog = load_catalog("default", type="rest", uri=TEST_URI)

        assert isinstance(catalog, RestCatalog)
        assert create_auth_manager.call_args_list
        configured_auth_manager_call = create_auth_manager.call_args_list[0]
        assert all(auth_manager_call == configured_auth_manager_call for auth_manager_call in create_auth_manager.call_args_list)
        configured_manager, configured_properties = configured_auth_manager_call.args
        assert isinstance(configured_manager, str)
        assert isinstance(configured_properties, dict)
        return configured_manager, cast(Properties, configured_properties)


def test_load_catalog_with_yaml_and_environment_noop_auth(tmp_path: Path, requests_mock: Mocker) -> None:
    yaml_config = """
catalog:
  default:
    auth:
      type: noop
"""
    environment = {"PYICEBERG_CATALOG__DEFAULT__AUTH__TYPE": "noop"}

    yaml_auth_config = _assert_load_catalog_auth_config_from_yaml(yaml_config, tmp_path, requests_mock)
    environment_auth_config = _assert_load_catalog_auth_config_from_environment(environment, requests_mock)

    assert yaml_auth_config == environment_auth_config == ("noop", {})


def test_load_catalog_with_yaml_and_environment_basic_auth(tmp_path: Path, requests_mock: Mocker) -> None:
    yaml_config = """
catalog:
  default:
    auth:
      type: basic
      basic:
        username: user
        password: password
"""
    environment = {
        "PYICEBERG_CATALOG__DEFAULT__AUTH__TYPE": "basic",
        "PYICEBERG_CATALOG__DEFAULT__AUTH__BASIC__USERNAME": "user",
        "PYICEBERG_CATALOG__DEFAULT__AUTH__BASIC__PASSWORD": "password",
    }
    yaml_auth_config = _assert_load_catalog_auth_config_from_yaml(yaml_config, tmp_path, requests_mock)
    environment_auth_config = _assert_load_catalog_auth_config_from_environment(environment, requests_mock)

    assert yaml_auth_config == environment_auth_config == ("basic", {"username": "user", "password": "password"})


def test_load_catalog_with_yaml_and_environment_custom_auth(tmp_path: Path, requests_mock: Mocker) -> None:
    yaml_config = """
catalog:
  default:
    auth:
      type: custom
      impl: pyiceberg.catalog.rest.auth.BasicAuthManager
      custom:
        username: user
        password: password
"""
    environment = {
        "PYICEBERG_CATALOG__DEFAULT__AUTH__TYPE": "custom",
        "PYICEBERG_CATALOG__DEFAULT__AUTH__IMPL": "pyiceberg.catalog.rest.auth.BasicAuthManager",
        "PYICEBERG_CATALOG__DEFAULT__AUTH__CUSTOM__USERNAME": "user",
        "PYICEBERG_CATALOG__DEFAULT__AUTH__CUSTOM__PASSWORD": "password",
    }
    yaml_auth_config = _assert_load_catalog_auth_config_from_yaml(yaml_config, tmp_path, requests_mock)
    environment_auth_config = _assert_load_catalog_auth_config_from_environment(environment, requests_mock)

    assert (
        yaml_auth_config
        == environment_auth_config
        == (
            "pyiceberg.catalog.rest.auth.BasicAuthManager",
            {"username": "user", "password": "password"},
        )
    )


def test_load_catalog_with_yaml_and_environment_oauth2_auth(tmp_path: Path, requests_mock: Mocker) -> None:
    yaml_config = """
catalog:
  default:
    auth:
      type: oauth2
      oauth2:
        client_id: client
        client_secret: secret
        token_url: https://identity.example.com/token
        scope: catalog
        refresh_margin: 30
        expires_in: 3600
"""
    environment = {
        "PYICEBERG_CATALOG__DEFAULT__AUTH__TYPE": "oauth2",
        "PYICEBERG_CATALOG__DEFAULT__AUTH__OAUTH2__CLIENT_ID": "client",
        "PYICEBERG_CATALOG__DEFAULT__AUTH__OAUTH2__CLIENT_SECRET": "secret",
        "PYICEBERG_CATALOG__DEFAULT__AUTH__OAUTH2__TOKEN_URL": "https://identity.example.com/token",
        "PYICEBERG_CATALOG__DEFAULT__AUTH__OAUTH2__SCOPE": "catalog",
        "PYICEBERG_CATALOG__DEFAULT__AUTH__OAUTH2__REFRESH_MARGIN": "30",
        "PYICEBERG_CATALOG__DEFAULT__AUTH__OAUTH2__EXPIRES_IN": "3600",
    }

    yaml_auth_config = _assert_load_catalog_auth_config_from_yaml(yaml_config, tmp_path, requests_mock)
    environment_auth_config = _assert_load_catalog_auth_config_from_environment(environment, requests_mock)

    yaml_manager, yaml_properties = yaml_auth_config
    environment_manager, environment_properties = environment_auth_config
    assert yaml_manager == environment_manager == "oauth2"

    # OAuth2AuthManager requires client_id, but environment variables cannot preserve `_`,
    # so options such as client_id cannot be configured through environment variables.
    # Both parsed configurations are asserted explicitly to document this limitation.
    assert yaml_properties == {
        "client_id": "client",
        "client_secret": "secret",
        "token_url": "https://identity.example.com/token",
        "scope": "catalog",
        "refresh_margin": "30",
        "expires_in": "3600",
    }
    assert environment_properties == {
        "client-id": "client",
        "client-secret": "secret",
        "token-url": "https://identity.example.com/token",
        "scope": "catalog",
        "refresh-margin": "30",
        "expires-in": "3600",
    }


def test_load_catalog_with_yaml_and_environment_google_auth(tmp_path: Path, requests_mock: Mocker) -> None:
    yaml_config = """
catalog:
  default:
    auth:
      type: google
      google:
        credentials_path: /path/to/credentials.json
        scopes:
          - scope-a
          - scope-b
"""
    environment = {
        "PYICEBERG_CATALOG__DEFAULT__AUTH__TYPE": "google",
        "PYICEBERG_CATALOG__DEFAULT__AUTH__GOOGLE__CREDENTIALS_PATH": "/path/to/credentials.json",
        "PYICEBERG_CATALOG__DEFAULT__AUTH__GOOGLE__SCOPES": "scope-a,scope-b",
        "GOOGLE_APPLICATION_CREDENTIALS": "/path/to/credentials.json",
    }

    yaml_auth_config = _assert_load_catalog_auth_config_from_yaml(yaml_config, tmp_path, requests_mock)
    environment_auth_config = _assert_load_catalog_auth_config_from_environment(environment, requests_mock)

    yaml_manager, yaml_properties = yaml_auth_config
    environment_manager, environment_properties = environment_auth_config
    assert yaml_manager == environment_manager == "google"

    # GoogleAuthManager requires credentials_path and list scopes, but environment variables cannot preserve `_` or lists,
    # so these options cannot be configured through PyIceberg-prefixed environment variables.
    # Both parsed configurations are asserted explicitly to document this limitation.
    assert yaml_properties == {
        "credentials_path": "/path/to/credentials.json",
        "scopes": ["scope-a", "scope-b"],
    }
    assert environment_properties == {
        "credentials-path": "/path/to/credentials.json",
        "scopes": "scope-a,scope-b",
    }
    # Credentials can instead be configured through Google Auth's native environment variables, but scopes cannot.
    assert environment["GOOGLE_APPLICATION_CREDENTIALS"] == yaml_properties["credentials_path"]


def test_load_catalog_with_yaml_and_environment_entra_auth(tmp_path: Path, requests_mock: Mocker) -> None:
    yaml_config = """
catalog:
  default:
    auth:
      type: entra
      entra:
        scopes:
          - https://storage.azure.com/.default
        managed_identity_client_id: client-id
"""
    environment = {
        "PYICEBERG_CATALOG__DEFAULT__AUTH__TYPE": "entra",
        "PYICEBERG_CATALOG__DEFAULT__AUTH__ENTRA__SCOPES": "https://storage.azure.com/.default",
        "PYICEBERG_CATALOG__DEFAULT__AUTH__ENTRA__MANAGED_IDENTITY_CLIENT_ID": "client-id",
        "AZURE_TENANT_ID": "tenant-id",
        "AZURE_CLIENT_ID": "client-id",
        "AZURE_CLIENT_SECRET": "client-secret",
        "AZURE_AUTHORITY_HOST": "https://login.microsoftonline.com",
    }

    yaml_auth_config = _assert_load_catalog_auth_config_from_yaml(yaml_config, tmp_path, requests_mock)
    environment_auth_config = _assert_load_catalog_auth_config_from_environment(environment, requests_mock)

    yaml_manager, yaml_properties = yaml_auth_config
    environment_manager, environment_properties = environment_auth_config
    assert yaml_manager == environment_manager == "entra"

    # EntraAuthManager requires list scopes and managed_identity_client_id,
    # but environment variables cannot preserve lists or `_`, so these options cannot be configured through them.
    # Both parsed configurations are asserted explicitly to document this limitation.
    assert yaml_properties == {
        "scopes": ["https://storage.azure.com/.default"],
        "managed_identity_client_id": "client-id",
    }
    assert environment_properties == {
        "scopes": "https://storage.azure.com/.default",
        "managed-identity-client-id": "client-id",
    }
    # Credentials can instead be configured through Azure Identity's native environment variables.
    assert environment["AZURE_CLIENT_ID"] == yaml_properties["managed_identity_client_id"]


def test_load_catalog_environment_auth_config_overrides_yaml(tmp_path: Path, requests_mock: Mocker) -> None:
    (tmp_path / ".pyiceberg.yaml").write_text(
        """
catalog:
  default:
    auth:
      type: basic
      basic:
        username: yaml-user
        password: yaml-password
""",
        encoding="utf-8",
    )
    environment = {
        "PYICEBERG_HOME": str(tmp_path),
        "PYICEBERG_CATALOG__DEFAULT__AUTH__BASIC__PASSWORD": "environment-password",
    }
    with patch.dict("os.environ", environment, clear=True):
        config = Config()

    requests_mock.get(f"{TEST_URI}v1/config", json={"defaults": {}, "overrides": {}}, status_code=200)
    fake_auth_manager = MagicMock()
    fake_auth_manager.auth_header.return_value = None

    with (
        patch("pyiceberg.catalog._ENV_CONFIG", config),
        patch("pyiceberg.catalog.rest.AuthManagerFactory.create", return_value=fake_auth_manager) as create_auth_manager,
    ):
        catalog = load_catalog("default", type="rest", uri=TEST_URI)

    assert isinstance(catalog, RestCatalog)
    assert create_auth_manager.call_args_list
    assert all(
        auth_manager_call == call("basic", {"username": "yaml-user", "password": "environment-password"})
        for auth_manager_call in create_auth_manager.call_args_list
    )


def test_load_catalog_rejects_bare_auth_string() -> None:
    config = MagicMock(spec=Config)
    config.get_catalog_config.return_value = {"auth": "entra"}

    with patch("pyiceberg.catalog._ENV_CONFIG", config), pytest.raises(ValueError, match="PYICEBERG_CATALOG__<NAME>__AUTH__TYPE"):
        load_catalog("default", type="rest", uri=TEST_URI)


@pytest.fixture
def rest_mock(requests_mock: Mocker) -> Mocker:
    requests_mock.get(
        TEST_URI,
        json={},
        status_code=200,
    )
    return requests_mock


@pytest.fixture
def google_mock(requests_mock: Mocker) -> Mocker:
    requests_mock.post(GOOGLE_CREDS_URI, json={"access_token": "aaaabbb"}, status_code=200)
    requests_mock.get(
        TEST_URI,
        json={},
        status_code=200,
    )
    return requests_mock


def test_noop_auth_header(rest_mock: Mocker) -> None:
    auth_manager = NoopAuthManager()
    session = requests.Session()
    session.auth = AuthManagerAdapter(auth_manager)

    session.get(TEST_URI)
    history = rest_mock.request_history
    assert len(history) == 1
    actual_headers = history[0].headers
    assert "Authorization" not in actual_headers


def test_basic_auth_header(rest_mock: Mocker) -> None:
    username = "testuser"
    password = "testpassword"
    expected_token = base64.b64encode(f"{username}:{password}".encode()).decode()
    expected_header = f"Basic {expected_token}"

    auth_manager = BasicAuthManager(username=username, password=password)
    session = requests.Session()
    session.auth = AuthManagerAdapter(auth_manager)

    session.get(TEST_URI)
    history = rest_mock.request_history
    assert len(history) == 1
    actual_headers = history[0].headers
    assert actual_headers["Authorization"] == expected_header


@patch("google.auth.transport.requests.Request")
@patch("google.auth.default")
def test_google_auth_manager_default_credentials(
    mock_google_auth_default: MagicMock, mock_google_request: MagicMock, rest_mock: Mocker
) -> None:
    """Test GoogleAuthManager with default application credentials."""
    mock_credentials = MagicMock()
    mock_credentials.token = "test_token"
    mock_google_auth_default.return_value = (mock_credentials, "test_project")

    auth_manager = GoogleAuthManager()
    session = requests.Session()
    session.auth = AuthManagerAdapter(auth_manager)
    session.get(TEST_URI)

    mock_google_auth_default.assert_called_once_with(scopes=None)
    mock_credentials.refresh.assert_called_once_with(mock_google_request.return_value)
    history = rest_mock.request_history
    assert len(history) == 1
    actual_headers = history[0].headers
    assert actual_headers["Authorization"] == "Bearer test_token"


@patch("google.auth.transport.requests.Request")
@patch("google.auth.load_credentials_from_file")
def test_google_auth_manager_with_credentials_file(
    mock_load_creds: MagicMock, mock_google_request: MagicMock, rest_mock: Mocker
) -> None:
    """Test GoogleAuthManager with a credentials file path."""
    mock_credentials = MagicMock()
    mock_credentials.token = "file_token"
    mock_load_creds.return_value = (mock_credentials, "test_project_file")

    auth_manager = GoogleAuthManager(credentials_path="/fake/path.json")
    session = requests.Session()
    session.auth = AuthManagerAdapter(auth_manager)
    session.get(TEST_URI)

    mock_load_creds.assert_called_once_with("/fake/path.json", scopes=None)
    mock_credentials.refresh.assert_called_once_with(mock_google_request.return_value)
    history = rest_mock.request_history
    assert len(history) == 1
    actual_headers = history[0].headers
    assert actual_headers["Authorization"] == "Bearer file_token"


@patch("google.auth.transport.requests.Request")
@patch("google.auth.load_credentials_from_file")
def test_google_auth_manager_with_credentials_file_and_scopes(
    mock_load_creds: MagicMock, mock_google_request: MagicMock, rest_mock: Mocker
) -> None:
    """Test GoogleAuthManager with a credentials file path and scopes."""
    mock_credentials = MagicMock()
    mock_credentials.token = "scoped_token"
    mock_load_creds.return_value = (mock_credentials, "test_project_scoped")
    scopes = ["https://www.googleapis.com/auth/bigquery"]

    auth_manager = GoogleAuthManager(credentials_path="/fake/path.json", scopes=scopes)
    session = requests.Session()
    session.auth = AuthManagerAdapter(auth_manager)
    session.get(TEST_URI)

    mock_load_creds.assert_called_once_with("/fake/path.json", scopes=scopes)
    mock_credentials.refresh.assert_called_once_with(mock_google_request.return_value)
    history = rest_mock.request_history
    assert len(history) == 1
    actual_headers = history[0].headers
    assert actual_headers["Authorization"] == "Bearer scoped_token"


def test_google_auth_manager_import_error() -> None:
    """Test GoogleAuthManager raises ImportError if google-auth is not installed."""
    with patch.dict("sys.modules", {"google.auth": None, "google.auth.transport.requests": None}):
        with pytest.raises(ImportError, match="Google Auth libraries not found. Please install 'google-auth'."):
            GoogleAuthManager()


@patch("azure.identity.DefaultAzureCredential")
def test_entra_auth_manager_default_credential(mock_default_cred: MagicMock, rest_mock: Mocker) -> None:
    """Test EntraAuthManager with DefaultAzureCredential."""
    mock_credential_instance = MagicMock()
    mock_token = MagicMock()
    mock_token.token = "entra_default_token"
    mock_token.expires_on = 9999999999  # Far future timestamp
    mock_credential_instance.get_token.return_value = mock_token
    mock_default_cred.return_value = mock_credential_instance

    auth_manager = EntraAuthManager()
    session = requests.Session()
    session.auth = AuthManagerAdapter(auth_manager)
    session.get(TEST_URI)

    mock_default_cred.assert_called_once_with()
    mock_credential_instance.get_token.assert_called_once_with("https://storage.azure.com/.default")
    history = rest_mock.request_history
    assert len(history) == 1
    actual_headers = history[0].headers
    assert actual_headers["Authorization"] == "Bearer entra_default_token"


@patch("azure.identity.DefaultAzureCredential")
def test_entra_auth_manager_with_managed_identity_client_id(mock_default_cred: MagicMock, rest_mock: Mocker) -> None:
    """Test EntraAuthManager with managed_identity_client_id passed to DefaultAzureCredential."""
    mock_credential_instance = MagicMock()
    mock_token = MagicMock()
    mock_token.token = "entra_mi_token"
    mock_token.expires_on = 9999999999
    mock_credential_instance.get_token.return_value = mock_token
    mock_default_cred.return_value = mock_credential_instance

    auth_manager = EntraAuthManager(managed_identity_client_id="user-assigned-client-id")
    session = requests.Session()
    session.auth = AuthManagerAdapter(auth_manager)
    session.get(TEST_URI)

    mock_default_cred.assert_called_once_with(managed_identity_client_id="user-assigned-client-id")
    mock_credential_instance.get_token.assert_called_once_with("https://storage.azure.com/.default")
    history = rest_mock.request_history
    assert len(history) == 1
    actual_headers = history[0].headers
    assert actual_headers["Authorization"] == "Bearer entra_mi_token"


@patch("azure.identity.DefaultAzureCredential")
def test_entra_auth_manager_custom_scopes(mock_default_cred: MagicMock, rest_mock: Mocker) -> None:
    """Test EntraAuthManager with custom scopes."""
    mock_credential_instance = MagicMock()
    mock_token = MagicMock()
    mock_token.token = "entra_custom_scope_token"
    mock_token.expires_on = 9999999999
    mock_credential_instance.get_token.return_value = mock_token
    mock_default_cred.return_value = mock_credential_instance

    custom_scopes = ["https://datalake.azure.net/.default", "https://storage.azure.com/.default"]
    auth_manager = EntraAuthManager(scopes=custom_scopes)
    session = requests.Session()
    session.auth = AuthManagerAdapter(auth_manager)
    session.get(TEST_URI)

    mock_default_cred.assert_called_once_with()
    mock_credential_instance.get_token.assert_called_once_with(*custom_scopes)
    history = rest_mock.request_history
    assert len(history) == 1
    actual_headers = history[0].headers
    assert actual_headers["Authorization"] == "Bearer entra_custom_scope_token"


def test_entra_auth_manager_import_error() -> None:
    """Test EntraAuthManager raises ImportError if azure-identity is not installed."""
    with patch.dict("sys.modules", {"azure.identity": None}):
        with pytest.raises(ImportError, match="Azure Identity library not found"):
            EntraAuthManager()


@patch("azure.identity.DefaultAzureCredential")
def test_entra_auth_manager_token_failure(mock_default_cred: MagicMock, rest_mock: Mocker) -> None:
    """Test EntraAuthManager raises exception when token acquisition fails."""
    mock_credential_instance = MagicMock()
    mock_credential_instance.get_token.side_effect = Exception("Failed to acquire token")
    mock_default_cred.return_value = mock_credential_instance

    auth_manager = EntraAuthManager()
    session = requests.Session()
    session.auth = AuthManagerAdapter(auth_manager)

    with pytest.raises(Exception, match="Failed to acquire token"):
        session.get(TEST_URI)

    # Verify no requests were made with a blank/missing auth header
    history = rest_mock.request_history
    assert len(history) == 0
