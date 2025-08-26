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
from unittest.mock import MagicMock, patch

import pytest
import requests
from requests_mock import Mocker

from pyiceberg.catalog.rest.auth import AuthManagerAdapter, BasicAuthManager, GoogleAuthManager, NoopAuthManager

TEST_URI = "https://iceberg-test-catalog/"
GOOGLE_CREDS_URI = "https://oauth2.googleapis.com/token"


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
