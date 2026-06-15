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

import time
from unittest.mock import MagicMock

import pytest

from pyiceberg.catalog.rest.credentials_provider import (
    CREDENTIALS_ENDPOINT,
    LoadCredentialsResponse,
    VendedCredentialsProvider,
)
from pyiceberg.catalog.rest.scan_planning import StorageCredential

CATALOG_URI = "http://localhost:8181"
CREDENTIALS_PATH = "v1/credentials"

BASE_PROPS = {
    "uri": CATALOG_URI,
    CREDENTIALS_ENDPOINT: CREDENTIALS_PATH,
    "s3.access-key-id": "initial-key",
    "s3.secret-access-key": "initial-secret",
    "s3.session-token": "initial-token",
}

REFRESH_RESPONSE = LoadCredentialsResponse(
    credentials=[
        StorageCredential(
            prefix="s3://",
            config={
                "s3.access-key-id": "refreshed-key",
                "s3.secret-access-key": "refreshed-secret",
                "s3.session-token": "refreshed-token",
            },
        )
    ]
)


def _make_session(response: LoadCredentialsResponse = REFRESH_RESPONSE) -> MagicMock:
    session = MagicMock()
    mock_response = MagicMock()
    mock_response.text = response.model_dump_json(by_alias=True)
    mock_response.raise_for_status.return_value = None
    session.get.return_value = mock_response
    return session


def test_get_credentials_no_expiry_returns_static_creds() -> None:
    """When no expiry is set, credentials are returned from properties without an HTTP call."""
    session = _make_session()
    provider = VendedCredentialsProvider(session, BASE_PROPS)
    creds = provider.get_credentials()

    session.get.assert_not_called()
    assert creds["s3.access-key-id"] == "initial-key"
    assert creds["s3.secret-access-key"] == "initial-secret"
    assert creds["s3.session-token"] == "initial-token"


def test_get_credentials_far_expiry_returns_static_creds() -> None:
    """When expiry is far in the future (>300s), no refresh is triggered."""
    far_future_ms = str(int((time.time() + 3600) * 1000))  # expires in 1 hour
    props = {**BASE_PROPS, "s3.session-token-expires-at-ms": far_future_ms}
    session = _make_session()
    provider = VendedCredentialsProvider(session, props)
    creds = provider.get_credentials()

    session.get.assert_not_called()
    assert creds["s3.access-key-id"] == "initial-key"


def test_get_credentials_near_expiry_calls_refresh_endpoint() -> None:
    """When expiry is within 300s, the refresh endpoint is called and new creds returned."""
    near_expiry_ms = str(int((time.time() + 60) * 1000))  # expires in 60s
    props = {**BASE_PROPS, "s3.session-token-expires-at-ms": near_expiry_ms}
    session = _make_session()
    provider = VendedCredentialsProvider(session, props)
    creds = provider.get_credentials()

    session.get.assert_called_once_with(f"{CATALOG_URI}/{CREDENTIALS_PATH}")
    assert creds["s3.access-key-id"] == "refreshed-key"
    assert creds["s3.secret-access-key"] == "refreshed-secret"
    assert creds["s3.session-token"] == "refreshed-token"


def test_get_credentials_raises_on_empty_credentials() -> None:
    """An empty credentials list in the refresh response raises ValueError."""
    near_expiry_ms = str(int((time.time() + 60) * 1000))
    props = {**BASE_PROPS, "s3.session-token-expires-at-ms": near_expiry_ms}
    empty_response = LoadCredentialsResponse(credentials=[])
    provider = VendedCredentialsProvider(_make_session(empty_response), props)

    with pytest.raises(ValueError, match="empty"):
        provider.get_credentials()


def test_get_credentials_raises_on_multiple_credentials() -> None:
    """More than one credential in the refresh response raises ValueError."""
    near_expiry_ms = str(int((time.time() + 60) * 1000))
    props = {**BASE_PROPS, "s3.session-token-expires-at-ms": near_expiry_ms}
    multi_response = LoadCredentialsResponse(
        credentials=[
            StorageCredential(prefix="s3://", config={}),
            StorageCredential(prefix="s3://b", config={}),
        ]
    )
    provider = VendedCredentialsProvider(_make_session(multi_response), props)

    with pytest.raises(ValueError, match="only one"):
        provider.get_credentials()


def test_build_refresh_endpoint_strips_trailing_slash() -> None:
    props = {**BASE_PROPS, "uri": "http://localhost:8181/"}
    provider = VendedCredentialsProvider(MagicMock(), props)
    assert provider._build_refresh_endpoint() == f"http://localhost:8181/{CREDENTIALS_PATH}"


def test_build_refresh_endpoint_raises_without_uri() -> None:
    props = {CREDENTIALS_ENDPOINT: CREDENTIALS_PATH}
    provider = VendedCredentialsProvider(MagicMock(), props)

    from pyiceberg.exceptions import ValidationException

    with pytest.raises(ValidationException):
        provider._build_refresh_endpoint()


def test_needs_refresh_true_when_near_expiry() -> None:
    near_expiry_ms = str(int((time.time() + 60) * 1000))
    provider = VendedCredentialsProvider(MagicMock(), {**BASE_PROPS, "s3.session-token-expires-at-ms": near_expiry_ms})
    assert provider.needs_refresh() is True


def test_needs_refresh_false_when_far_expiry() -> None:
    far_expiry_ms = str(int((time.time() + 3600) * 1000))
    provider = VendedCredentialsProvider(MagicMock(), {**BASE_PROPS, "s3.session-token-expires-at-ms": far_expiry_ms})
    assert provider.needs_refresh() is False


def test_needs_refresh_false_when_no_expiry() -> None:
    provider = VendedCredentialsProvider(MagicMock(), BASE_PROPS)
    assert provider.needs_refresh() is False


def test_get_credentials_updates_internal_properties_after_refresh() -> None:
    """After a refresh, _properties holds the new expiry so needs_refresh() sees the updated state."""
    far_future_ms = str(int((time.time() + 3600) * 1000))
    refreshed_response = LoadCredentialsResponse(
        credentials=[
            StorageCredential(
                prefix="s3://",
                config={
                    "s3.access-key-id": "new-key",
                    "s3.secret-access-key": "new-secret",
                    "s3.session-token": "new-token",
                    "s3.session-token-expires-at-ms": far_future_ms,
                },
            )
        ]
    )
    near_expiry_ms = str(int((time.time() + 60) * 1000))
    props = {**BASE_PROPS, "s3.session-token-expires-at-ms": near_expiry_ms}
    provider = VendedCredentialsProvider(_make_session(refreshed_response), props)

    assert provider.needs_refresh() is True
    provider.get_credentials()
    assert provider.needs_refresh() is False
    assert provider._properties["s3.session-token-expires-at-ms"] == far_future_ms
