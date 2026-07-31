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
import hashlib
from unittest.mock import MagicMock, patch

import pytest
import requests
from requests_mock import Mocker

from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.catalog.rest.auth import AuthManagerAdapter, BasicAuthManager, EntraAuthManager, GoogleAuthManager, NoopAuthManager

TEST_URI = "https://iceberg-test-catalog/"
GOOGLE_CREDS_URI = "https://oauth2.googleapis.com/token"


@pytest.fixture
def rest_mock(requests_mock: Mocker) -> Mocker:
    requests_mock.get(
        TEST_URI,
        json={},
        status_code=200,
    )
    requests_mock.get(
        f"{TEST_URI}v1/config",
        json={"defaults": {}, "overrides": {}},
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


def test_sign_request_default_is_noop() -> None:
    """AuthManager.sign_request default implementation must not modify the request."""
    manager = NoopAuthManager()
    prepared = requests.Request("GET", TEST_URI).prepare()
    original_headers = dict(prepared.headers)

    result = manager.sign_request(prepared)

    assert result is prepared
    assert dict(result.headers) == original_headers


def test_sigv4_auth_manager_signs_with_java_reference_values() -> None:
    """SigV4AuthManager.sign_request must match Iceberg Java reference header values."""
    import boto3

    from pyiceberg.catalog.rest.auth import SigV4AuthManager

    boto_session = boto3.Session(
        aws_access_key_id="id",
        aws_secret_access_key="secret",
        region_name="us-east-1",
    )
    manager = SigV4AuthManager(
        delegate=NoopAuthManager(),
        boto_session=boto_session,
        region="us-east-1",
        service="execute-api",
    )

    # Non-empty body: base64 SHA-256 (Iceberg Java TestRESTSigV4AuthSession.java L177)
    body = b'{"namespace":["ns"],"properties":{}}'
    prepared = requests.Request("POST", "https://example.com/v1/namespaces", data=body).prepare()
    manager.sign_request(prepared)
    assert prepared.headers["x-amz-content-sha256"] == base64.b64encode(hashlib.sha256(body).digest()).decode()
    assert prepared.headers["x-amz-content-sha256"] == "yc5oAKPWjHY4sW8XQq0l/3aNrrXJKBycVFNnDEGMfww="
    assert prepared.headers["Authorization"].startswith("AWS4-HMAC-SHA256 Credential=")

    # Empty body: hex EMPTY_BODY_SHA256 (Iceberg Java TestRESTSigV4AuthSession.java L121)
    prepared_empty = requests.Request("GET", "https://example.com/v1/config").prepare()
    manager.sign_request(prepared_empty)
    assert prepared_empty.headers["x-amz-content-sha256"] == "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"


def test_sigv4_auth_manager_relocates_delegate_authorization() -> None:
    """When the delegate sets Authorization, SigV4 relocates it to Original-Authorization."""
    import boto3

    from pyiceberg.catalog.rest.auth import SigV4AuthManager

    boto_session = boto3.Session(aws_access_key_id="id", aws_secret_access_key="secret", region_name="us-east-1")
    manager = SigV4AuthManager(
        delegate=BasicAuthManager(username="user", password="pass"),
        boto_session=boto_session,
        region="us-east-1",
        service="execute-api",
    )
    adapter = AuthManagerAdapter(manager)

    prepared = requests.Request("GET", "https://example.com/v1/config").prepare()
    adapter(prepared)

    # SigV4 owns Authorization; the delegate's Basic header is relocated.
    assert prepared.headers["Authorization"].startswith("AWS4-HMAC-SHA256 Credential=")
    assert prepared.headers["Original-Authorization"].startswith("Basic ")
    # Relocated header is signed (in SignedHeaders), matching Iceberg Java.
    assert "original-authorization" in prepared.headers["Authorization"]


@pytest.mark.filterwarnings(
    "ignore:Deprecated in 0.11.0, will be removed in 1.0.0. The property rest.sigv4-enabled is deprecated:DeprecationWarning"
)
def test_sigv4_legacy_config_builds_sigv4_auth_manager(rest_mock: Mocker) -> None:
    """Legacy rest.sigv4-enabled config produces a SigV4AuthManager."""
    from pyiceberg.catalog.rest.auth import SigV4AuthManager

    catalog = RestCatalog(
        "rest",
        **{
            "uri": TEST_URI,
            "rest.sigv4-enabled": "true",
            "rest.signing-region": "us-east-1",
            "client.access-key-id": "id",
            "client.secret-access-key": "secret",
        },
    )
    assert isinstance(catalog._auth_manager, SigV4AuthManager)


def test_sigv4_auth_type_config_builds_sigv4_auth_manager(rest_mock: Mocker) -> None:
    """New auth.type=sigv4 config produces a SigV4AuthManager wrapping the delegate."""
    from pyiceberg.catalog.rest.auth import SigV4AuthManager

    catalog = RestCatalog(
        "rest",
        **{  # type: ignore
            "uri": TEST_URI,
            "auth": {"type": "sigv4", "sigv4": {"delegate": {"type": "noop"}}},
            "rest.signing-region": "us-east-1",
            "client.access-key-id": "id",
            "client.secret-access-key": "secret",
        },
    )
    assert isinstance(catalog._auth_manager, SigV4AuthManager)


def test_sigv4_auth_type_rejects_auth_impl(rest_mock: Mocker) -> None:
    """auth.impl is only valid with auth.type=custom, not sigv4."""
    with pytest.raises(ValueError, match="auth.impl can only be specified when using custom auth.type"):
        RestCatalog(
            "rest",
            **{  # type: ignore
                "uri": TEST_URI,
                "auth": {"type": "sigv4", "impl": "my.custom.AuthManager"},
                "rest.signing-region": "us-east-1",
                "client.access-key-id": "id",
                "client.secret-access-key": "secret",
            },
        )


def test_sigv4_rejects_sigv4_delegate(rest_mock: Mocker) -> None:
    """A SigV4 delegate cannot itself be sigv4, matching Iceberg Java's AuthManagers check."""
    with pytest.raises(ValueError, match="Cannot delegate a SigV4 auth manager to another SigV4 auth manager"):
        RestCatalog(
            "rest",
            **{  # type: ignore
                "uri": TEST_URI,
                "auth": {"type": "sigv4", "sigv4": {"delegate": {"type": "sigv4"}}},
                "rest.signing-region": "us-east-1",
                "client.access-key-id": "id",
                "client.secret-access-key": "secret",
            },
        )


def test_sigv4_legacy_flag_emits_deprecation_warning(rest_mock: Mocker) -> None:
    """The legacy rest.sigv4-enabled flag warns and points at auth.type=sigv4, matching Iceberg Java."""
    with pytest.warns(DeprecationWarning, match="rest.sigv4-enabled is deprecated"):
        RestCatalog(
            "rest",
            **{
                "uri": TEST_URI,
                "rest.sigv4-enabled": "true",
                "rest.signing-region": "us-east-1",
                "client.access-key-id": "id",
                "client.secret-access-key": "secret",
            },
        )


@pytest.mark.filterwarnings(
    "ignore:Deprecated in 0.11.0, will be removed in 1.0.0. The property rest.sigv4-enabled is deprecated:DeprecationWarning"
)
def test_sigv4_sign_request_without_body(rest_mock: Mocker) -> None:
    from pyiceberg.catalog.rest.auth import EMPTY_BODY_SHA256

    existing_token = "existing_token"

    catalog = RestCatalog(
        "rest",
        **{
            "uri": TEST_URI,
            "token": existing_token,
            "rest.sigv4-enabled": "true",
            "rest.signing-region": "us-west-2",
            "client.access-key-id": "id",
            "client.secret-access-key": "secret",
        },
    )

    # prepare_request applies session.auth, which signs via SigV4AuthManager.
    prepared = catalog._session.prepare_request(requests.Request("GET", f"{TEST_URI}v1/config"))

    auth_header = prepared.headers["Authorization"]
    assert auth_header.startswith("AWS4-HMAC-SHA256 Credential=")
    assert prepared.headers["Original-Authorization"] == f"Bearer {existing_token}"
    assert prepared.headers["x-amz-content-sha256"] == EMPTY_BODY_SHA256
    # Verify the signature format: Credential, SignedHeaders, Signature
    assert "Credential=" in auth_header
    assert "SignedHeaders=" in auth_header
    assert "Signature=" in auth_header
    # x-amz-content-sha256 should be in signed headers
    assert "x-amz-content-sha256" in auth_header


@pytest.mark.filterwarnings(
    "ignore:Deprecated in 0.11.0, will be removed in 1.0.0. The property rest.sigv4-enabled is deprecated:DeprecationWarning"
)
def test_sigv4_sign_request_with_body(rest_mock: Mocker) -> None:
    existing_token = "existing_token"

    catalog = RestCatalog(
        "rest",
        **{
            "uri": TEST_URI,
            "token": existing_token,
            "rest.sigv4-enabled": "true",
            "rest.signing-region": "us-west-2",
            "client.access-key-id": "id",
            "client.secret-access-key": "secret",
        },
    )

    prepared = catalog._session.prepare_request(
        requests.Request(
            "POST",
            f"{TEST_URI}v1/namespaces",
            data={"namespace": "asdfasd"},
        )
    )

    auth_header = prepared.headers["Authorization"]
    assert auth_header.startswith("AWS4-HMAC-SHA256 Credential=")
    assert "SignedHeaders=" in auth_header
    # Conflicting Authorization header is relocated
    assert prepared.headers["Original-Authorization"] == f"Bearer {existing_token}"
    # Non-empty body should have base64-encoded SHA256
    content_sha256 = prepared.headers["x-amz-content-sha256"]
    assert prepared.body is not None
    body_bytes = prepared.body.encode("utf-8") if isinstance(prepared.body, str) else prepared.body
    expected_sha256 = base64.b64encode(hashlib.sha256(body_bytes).digest()).decode()
    assert content_sha256 == expected_sha256
    # x-amz-content-sha256 should be in signed headers
    assert "x-amz-content-sha256" in auth_header


@pytest.mark.filterwarnings(
    "ignore:Deprecated in 0.11.0, will be removed in 1.0.0. The property rest.sigv4-enabled is deprecated:DeprecationWarning"
)
def test_sigv4_content_sha256_with_bytes_body(rest_mock: Mocker) -> None:
    existing_token = "existing_token"

    catalog = RestCatalog(
        "rest",
        **{
            "uri": TEST_URI,
            "token": existing_token,
            "rest.sigv4-enabled": "true",
            "rest.signing-region": "us-west-2",
            "client.access-key-id": "id",
            "client.secret-access-key": "secret",
        },
    )

    body_content = b'{"namespace": "test_namespace"}'
    prepared = catalog._session.prepare_request(
        requests.Request(
            "POST",
            f"{TEST_URI}v1/namespaces",
            data=body_content,
        )
    )

    assert prepared.headers["Authorization"].startswith("AWS4-HMAC-SHA256 Credential=")
    assert "SignedHeaders=" in prepared.headers["Authorization"]
    content_sha256 = prepared.headers["x-amz-content-sha256"]
    expected_sha256 = base64.b64encode(hashlib.sha256(body_content).digest()).decode()
    assert content_sha256 == expected_sha256


@pytest.mark.filterwarnings(
    "ignore:Deprecated in 0.11.0, will be removed in 1.0.0. The property rest.sigv4-enabled is deprecated:DeprecationWarning"
)
def test_sigv4_conflicting_sigv4_headers(rest_mock: Mocker) -> None:
    from pyiceberg.catalog.rest.auth import EMPTY_BODY_SHA256

    catalog = RestCatalog(
        "rest",
        **{
            "uri": TEST_URI,
            "rest.sigv4-enabled": "true",
            "rest.signing-region": "us-west-2",
            "client.access-key-id": "id",
            "client.secret-access-key": "secret",
        },
    )

    # Build an unsigned prepared request, then inject conflicting SigV4 headers.
    prepared = requests.Request("GET", f"{TEST_URI}v1/config").prepare()
    prepared.headers["x-amz-content-sha256"] = "fake"
    prepared.headers["X-Amz-Date"] = "fake"

    # session.auth is the AuthManagerAdapter; calling it signs the request.
    auth = catalog._session.auth
    assert isinstance(auth, AuthManagerAdapter)
    auth(prepared)

    # Matching Java SDK: conflicting headers are relocated with "Original-" prefix
    assert prepared.headers.get("Original-x-amz-content-sha256") == "fake"
    assert prepared.headers.get("Original-X-Amz-Date") == "fake"
    # SigV4 headers are set correctly after signing
    assert prepared.headers["Authorization"].startswith("AWS4-HMAC-SHA256 Credential=")
    assert prepared.headers["x-amz-content-sha256"] == EMPTY_BODY_SHA256
    assert "X-Amz-Date" in prepared.headers


@pytest.mark.filterwarnings(
    "ignore:Deprecated in 0.11.0, will be removed in 1.0.0. The property rest.sigv4-enabled is deprecated:DeprecationWarning"
)
def test_sigv4_canonical_request_uses_hex_payload(rest_mock: Mocker) -> None:
    """Verify that the canonical request uses hex-encoded payload hash, not the base64 header value."""
    from typing import Any

    from botocore.auth import SigV4Auth

    catalog = RestCatalog(
        "rest",
        **{
            "uri": TEST_URI,
            "token": "token",
            "rest.sigv4-enabled": "true",
            "rest.signing-region": "us-west-2",
            "client.access-key-id": "id",
            "client.secret-access-key": "secret",
        },
    )

    body_content = b'{"namespace": "test"}'

    # Capture the canonical request string during signing
    captured_canonical = []
    original_add_auth = SigV4Auth.add_auth

    def capturing_add_auth(self: Any, request: Any) -> None:
        captured_canonical.append(self.canonical_request(request))
        original_add_auth(self, request)

    # Signing now happens inside prepare_request (via session.auth).
    with patch.object(SigV4Auth, "add_auth", capturing_add_auth):
        prepared = catalog._session.prepare_request(
            requests.Request(
                "POST",
                f"{TEST_URI}v1/namespaces",
                data=body_content,
            )
        )

    assert len(captured_canonical) == 1
    canonical_lines = captured_canonical[0].split("\n")
    # Last line of canonical request is the payload hash
    payload_hash = canonical_lines[-1]
    # Must be hex-encoded (64 hex chars), not base64
    assert len(payload_hash) == 64
    assert payload_hash == hashlib.sha256(body_content).hexdigest()
    # Meanwhile the header is base64-encoded
    assert prepared.headers["x-amz-content-sha256"] == base64.b64encode(hashlib.sha256(body_content).digest()).decode()


@pytest.mark.filterwarnings(
    "ignore:Deprecated in 0.11.0, will be removed in 1.0.0. The property rest.sigv4-enabled is deprecated:DeprecationWarning"
)
def test_sigv4_content_sha256_matches_iceberg_java_reference(rest_mock: Mocker) -> None:
    """Pin byte-for-byte equivalence with Iceberg Java TestRESTSigV4AuthSession (L121, L177)."""
    java_reference_body = b'{"namespace":["ns"],"properties":{}}'
    java_reference_base64 = "yc5oAKPWjHY4sW8XQq0l/3aNrrXJKBycVFNnDEGMfww="
    java_reference_empty_hex = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

    catalog = RestCatalog(
        "rest",
        **{
            "uri": TEST_URI,
            "rest.sigv4-enabled": "true",
            "rest.signing-region": "us-east-1",
            "client.access-key-id": "id",
            "client.secret-access-key": "secret",
        },
    )

    # Non-empty body: must match Java's base64 reference value exactly
    prepared_with_body = catalog._session.prepare_request(
        requests.Request("POST", f"{TEST_URI}v1/namespaces", data=java_reference_body)
    )
    assert prepared_with_body.headers["x-amz-content-sha256"] == java_reference_base64

    # Empty body: must match Java's hex reference value exactly
    prepared_empty = catalog._session.prepare_request(requests.Request("GET", f"{TEST_URI}v1/config"))
    assert prepared_empty.headers["x-amz-content-sha256"] == java_reference_empty_hex


def test_sigv4_unsupported_body_type_raises() -> None:
    """Unsupported body types (e.g. file-like) raise a clear error rather than crashing in hashlib."""
    import boto3

    from pyiceberg.catalog.rest.auth import NoopAuthManager, SigV4AuthManager

    boto_session = boto3.Session(
        aws_access_key_id="id",
        aws_secret_access_key="secret",
        region_name="us-east-1",
    )
    manager = SigV4AuthManager(
        delegate=NoopAuthManager(),
        boto_session=boto_session,
        region="us-east-1",
        service="execute-api",
    )

    prepared = requests.Request("POST", f"{TEST_URI}v1/namespaces").prepare()
    # Inject an unsupported body type (a list — not str/bytes)
    prepared.body = ["not", "a", "valid", "body"]  # type: ignore[assignment]

    with pytest.raises(TypeError, match="Unsupported request body type for SigV4 signing"):
        manager.sign_request(prepared)


@pytest.mark.filterwarnings(
    "ignore:Deprecated in 0.11.0, will be removed in 1.0.0. The property rest.sigv4-enabled is deprecated:DeprecationWarning"
)
def test_sigv4_uses_client_profile_name(rest_mock: Mocker) -> None:
    import boto3

    # Use a real boto3.Session for credential resolution (signing runs during
    # config fetch), but spy on the constructor to assert the profile is honored.
    real_session = boto3.Session(
        aws_access_key_id="id",
        aws_secret_access_key="secret",
        region_name="us-west-2",
    )

    with patch("boto3.Session", return_value=real_session) as mock_session:
        RestCatalog(
            "rest",
            **{
                "uri": TEST_URI,
                "token": "token",
                "rest.sigv4-enabled": "true",
                "rest.signing-region": "us-west-2",
                "client.profile-name": "rest-profile",
            },
        )

    mock_session.assert_called_with(
        profile_name="rest-profile",
        region_name=None,
        botocore_session=None,
        aws_access_key_id=None,
        aws_secret_access_key=None,
        aws_session_token=None,
    )


def test_empty_body_sha256_importable_from_rest_package() -> None:
    """EMPTY_BODY_SHA256 was public in released 0.11.x; the re-export must survive."""
    from pyiceberg.catalog.rest import EMPTY_BODY_SHA256

    assert EMPTY_BODY_SHA256 == "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"


def test_sigv4_relocates_lowercase_authorization() -> None:
    """Header handling is case-insensitive: a lowercase authorization header is still relocated."""
    import boto3

    from pyiceberg.catalog.rest.auth import SigV4AuthManager

    boto_session = boto3.Session(aws_access_key_id="id", aws_secret_access_key="secret", region_name="us-east-1")
    manager = SigV4AuthManager(delegate=NoopAuthManager(), boto_session=boto_session, region="us-east-1")

    prepared = requests.Request("GET", "https://example.com/v1/config", headers={"authorization": "Bearer tok"}).prepare()
    manager.sign_request(prepared)

    assert prepared.headers["Authorization"].startswith("AWS4-HMAC-SHA256 Credential=")
    assert prepared.headers["Original-Authorization"] == "Bearer tok"


def test_sigv4_overwrites_conflicting_content_sha256_casing() -> None:
    """A caller-supplied X-Amz-Content-SHA256 in different casing must not duplicate or survive signing."""
    import boto3

    from pyiceberg.catalog.rest.auth import SigV4AuthManager

    boto_session = boto3.Session(aws_access_key_id="id", aws_secret_access_key="secret", region_name="us-east-1")
    manager = SigV4AuthManager(delegate=NoopAuthManager(), boto_session=boto_session, region="us-east-1")

    body = b'{"k":"v"}'
    prepared = requests.Request(
        "POST", "https://example.com/v1/namespaces", data=body, headers={"X-Amz-Content-SHA256": "bogus"}
    ).prepare()
    manager.sign_request(prepared)

    expected = base64.b64encode(hashlib.sha256(body).digest()).decode()
    values = [value for key, value in prepared.headers.items() if key.lower() == "x-amz-content-sha256"]
    assert values == [expected]
    assert prepared.headers["Original-X-Amz-Content-SHA256"] == "bogus"


def test_refresh_token_unwraps_sigv4_auth_manager() -> None:
    """Legacy OAuth refresh must reach the delegate through a SigV4 wrapper."""
    from requests import Session

    from pyiceberg.catalog.rest.auth import LegacyOAuth2AuthManager, SigV4AuthManager

    legacy = MagicMock(spec=LegacyOAuth2AuthManager)
    manager = SigV4AuthManager(delegate=legacy, boto_session=MagicMock(), region="us-east-1")
    catalog = object.__new__(RestCatalog)
    session = Session()
    session.auth = AuthManagerAdapter(manager)
    catalog._session = session

    catalog._refresh_token()

    legacy._refresh_token.assert_called_once()


def test_sigv4_custom_delegate_resolves_impl(rest_mock: Mocker) -> None:
    """A custom delegate resolves through delegate.impl, like top-level custom auth."""
    from pyiceberg.catalog.rest.auth import SigV4AuthManager

    catalog = RestCatalog(
        "rest",
        **{  # type: ignore
            "uri": TEST_URI,
            "auth": {
                "type": "sigv4",
                "sigv4": {
                    "delegate": {
                        "type": "custom",
                        "impl": "pyiceberg.catalog.rest.auth.BasicAuthManager",
                        "custom": {"username": "u", "password": "p"},
                    }
                },
            },
            "rest.signing-region": "us-east-1",
            "client.access-key-id": "id",
            "client.secret-access-key": "secret",
        },
    )
    assert isinstance(catalog._auth_manager, SigV4AuthManager)
    assert isinstance(catalog._auth_manager.delegate, BasicAuthManager)


def test_sigv4_custom_delegate_requires_impl(rest_mock: Mocker) -> None:
    """A custom delegate without impl fails fast instead of importing the literal string."""
    with pytest.raises(ValueError, match="auth.sigv4.delegate.impl must be specified"):
        RestCatalog(
            "rest",
            **{  # type: ignore
                "uri": TEST_URI,
                "auth": {"type": "sigv4", "sigv4": {"delegate": {"type": "custom"}}},
                "rest.signing-region": "us-east-1",
                "client.access-key-id": "id",
                "client.secret-access-key": "secret",
            },
        )


def test_sigv4_non_custom_delegate_rejects_impl(rest_mock: Mocker) -> None:
    """delegate.impl is only valid for a custom delegate, mirroring top-level auth.impl."""
    with pytest.raises(ValueError, match="auth.sigv4.delegate.impl can only be specified"):
        RestCatalog(
            "rest",
            **{  # type: ignore
                "uri": TEST_URI,
                "auth": {
                    "type": "sigv4",
                    "sigv4": {"delegate": {"type": "noop", "impl": "pyiceberg.catalog.rest.auth.BasicAuthManager"}},
                },
                "rest.signing-region": "us-east-1",
                "client.access-key-id": "id",
                "client.secret-access-key": "secret",
            },
        )


def test_reauthenticating_session_resigns_same_origin_redirect(requests_mock: Mocker) -> None:
    """Same-origin redirects are re-signed for the redirected URL, mirroring Java per-send signing."""
    import boto3

    from pyiceberg.catalog.rest.auth import ReauthenticatingSession, SigV4AuthManager

    boto_session = boto3.Session(aws_access_key_id="id", aws_secret_access_key="secret", region_name="us-east-1")
    manager = SigV4AuthManager(
        delegate=BasicAuthManager(username="u", password="p"), boto_session=boto_session, region="us-east-1"
    )

    session = ReauthenticatingSession()
    session.auth = AuthManagerAdapter(manager)

    requests_mock.get("https://example.com/v1/old", status_code=302, headers={"Location": "https://example.com/v1/new"})
    requests_mock.get("https://example.com/v1/new", json={})

    response = session.get("https://example.com/v1/old")

    assert response.status_code == 200
    first, second = requests_mock.request_history
    assert second.url == "https://example.com/v1/new"
    assert first.headers["Authorization"].startswith("AWS4-HMAC-SHA256 Credential=")
    assert second.headers["Authorization"].startswith("AWS4-HMAC-SHA256 Credential=")
    assert second.headers["Authorization"] != first.headers["Authorization"]
    assert second.headers["Original-Authorization"].startswith("Basic ")


def test_reauthenticating_session_strips_credentials_cross_origin(requests_mock: Mocker) -> None:
    """Cross-origin redirects must not forward any credential-bearing header."""
    import boto3

    from pyiceberg.catalog.rest.auth import ReauthenticatingSession, SigV4AuthManager

    boto_session = boto3.Session(
        aws_access_key_id="id", aws_secret_access_key="secret", aws_session_token="sts-token", region_name="us-east-1"
    )
    manager = SigV4AuthManager(
        delegate=BasicAuthManager(username="u", password="p"), boto_session=boto_session, region="us-east-1"
    )

    session = ReauthenticatingSession()
    session.auth = AuthManagerAdapter(manager)

    requests_mock.get("https://example.com/v1/old", status_code=302, headers={"Location": "https://other.example.net/v1/new"})
    requests_mock.get("https://other.example.net/v1/new", json={})

    # Stripping must be by Original-* prefix, not a fixed list.
    response = session.get("https://example.com/v1/old", headers={"Original-Original-Authorization": "Bearer stale"})

    assert response.status_code == 200
    first, second = requests_mock.request_history
    assert first.headers["Authorization"].startswith("AWS4-HMAC-SHA256 Credential=")
    assert first.headers["Original-Authorization"].startswith("Basic ")
    assert first.headers["X-Amz-Security-Token"] == "sts-token"
    for header in (
        "Authorization",
        "Original-Authorization",
        "Original-Original-Authorization",
        "X-Amz-Security-Token",
        "X-Amz-Date",
        "x-amz-content-sha256",
    ):
        assert header not in second.headers


def test_sigv4_signing_scoped_to_url_prefix(requests_mock: Mocker) -> None:
    """URLs outside signing_url_prefix are not signed; the delegate header still applies."""
    import boto3

    from pyiceberg.catalog.rest.auth import ReauthenticatingSession, SigV4AuthManager

    boto_session = boto3.Session(aws_access_key_id="id", aws_secret_access_key="secret", region_name="us-east-1")
    manager = SigV4AuthManager(
        delegate=BasicAuthManager(username="u", password="p"),
        boto_session=boto_session,
        region="us-east-1",
        signing_url_prefix="https://catalog.example.com/",
    )

    session = ReauthenticatingSession()
    session.auth = AuthManagerAdapter(manager)

    requests_mock.post("https://auth.other.example.net/token", json={"access_token": "t", "token_type": "bearer"})
    requests_mock.get("https://catalog.example.com/v1/config", json={})

    session.post("https://auth.other.example.net/token", data={"grant_type": "client_credentials"})
    session.get("https://catalog.example.com/v1/config")

    token_request, catalog_request = requests_mock.request_history
    assert token_request.headers["Authorization"].startswith("Basic ")
    assert "X-Amz-Date" not in token_request.headers
    assert catalog_request.headers["Authorization"].startswith("AWS4-HMAC-SHA256 Credential=")


def test_sigv4_catalog_wires_signing_prefix_to_catalog_uri(rest_mock: Mocker) -> None:
    """The catalog scopes SigV4 signing to its own URI."""
    from pyiceberg.catalog.rest.auth import SigV4AuthManager

    catalog = RestCatalog(
        "rest",
        **{  # type: ignore
            "uri": TEST_URI,
            "auth": {"type": "sigv4", "sigv4": {"delegate": {"type": "noop"}}},
            "rest.signing-region": "us-east-1",
            "client.access-key-id": "id",
            "client.secret-access-key": "secret",
        },
    )
    assert isinstance(catalog._auth_manager, SigV4AuthManager)
    assert catalog._auth_manager._signing_url_prefix == TEST_URI


def test_plain_manager_keeps_static_authorization_on_same_origin_redirect(requests_mock: Mocker) -> None:
    """A header-less manager must not lose a caller-supplied Authorization on same-origin redirects."""
    from pyiceberg.catalog.rest.auth import ReauthenticatingSession

    session = ReauthenticatingSession()
    session.auth = AuthManagerAdapter(NoopAuthManager())

    requests_mock.get("https://example.com/v1/old", status_code=307, headers={"Location": "https://example.com/v1/new"})
    requests_mock.get("https://example.com/v1/new", json={})

    session.get("https://example.com/v1/old", headers={"Authorization": "Bearer table-token"})

    _, second = requests_mock.request_history
    assert second.headers["Authorization"] == "Bearer table-token"


def test_sigv4_discards_stale_self_signature_and_keeps_static_bearer(requests_mock: Mocker) -> None:
    """A stale SigV4 signature is discarded on redirect; a static bearer relocates once."""
    import boto3

    from pyiceberg.catalog.rest.auth import ReauthenticatingSession, SigV4AuthManager

    boto_session = boto3.Session(aws_access_key_id="id", aws_secret_access_key="secret", region_name="us-east-1")
    manager = SigV4AuthManager(delegate=NoopAuthManager(), boto_session=boto_session, region="us-east-1")

    session = ReauthenticatingSession()
    session.auth = AuthManagerAdapter(manager)

    requests_mock.get("https://example.com/v1/old", status_code=307, headers={"Location": "https://example.com/v1/new"})
    requests_mock.get("https://example.com/v1/new", json={})

    session.get("https://example.com/v1/old", headers={"Authorization": "Bearer table-token"})

    first, second = requests_mock.request_history
    for hop in (first, second):
        assert hop.headers["Authorization"].startswith("AWS4-HMAC-SHA256 Credential=")
        assert hop.headers["Original-Authorization"] == "Bearer table-token"
    assert "Original-Original-Authorization" not in second.headers


def test_sigv4_rotating_delegate_does_not_spawn_recursive_relocation(requests_mock: Mocker) -> None:
    """A delegate that rotates its token across hops must not resurrect the old token."""
    import boto3

    from pyiceberg.catalog.rest.auth import AuthManager, ReauthenticatingSession, SigV4AuthManager

    class RotatingAuthManager(AuthManager):
        def __init__(self) -> None:
            self.calls = 0

        def auth_header(self) -> str:
            self.calls += 1
            return f"Bearer token-{self.calls}"

    boto_session = boto3.Session(aws_access_key_id="id", aws_secret_access_key="secret", region_name="us-east-1")
    manager = SigV4AuthManager(delegate=RotatingAuthManager(), boto_session=boto_session, region="us-east-1")

    session = ReauthenticatingSession()
    session.auth = AuthManagerAdapter(manager)

    requests_mock.get("https://example.com/v1/old", status_code=307, headers={"Location": "https://example.com/v1/new"})
    requests_mock.get("https://example.com/v1/new", json={})

    session.get("https://example.com/v1/old")

    first, second = requests_mock.request_history
    assert first.headers["Original-Authorization"] == "Bearer token-1"
    assert second.headers["Original-Authorization"] == "Bearer token-2"
    assert "Original-Original-Authorization" not in second.headers
    assert "token-1" not in str(second.headers)


def test_sigv4_signing_prefix_match_is_case_insensitive() -> None:
    """Prefix matching mirrors requests' adapter-mount matching (case-insensitive)."""
    import boto3

    from pyiceberg.catalog.rest.auth import SigV4AuthManager

    boto_session = boto3.Session(aws_access_key_id="id", aws_secret_access_key="secret", region_name="us-east-1")
    manager = SigV4AuthManager(
        delegate=NoopAuthManager(),
        boto_session=boto_session,
        region="us-east-1",
        signing_url_prefix="https://Catalog.Example.COM/",
    )

    prepared = requests.Request("GET", "https://catalog.example.com/v1/config").prepare()
    manager.sign_request(prepared)
    assert prepared.headers["Authorization"].startswith("AWS4-HMAC-SHA256 Credential=")


def test_sigv4_file_io_receives_pickleable_delegate(rest_mock: Mocker) -> None:
    """FileIO gets the pickleable delegate, not the SigV4 wrapper."""
    import pickle

    from pyiceberg.catalog.rest.auth import AUTH_MANAGER, SigV4AuthManager

    catalog = RestCatalog(
        "rest",
        **{  # type: ignore
            "uri": TEST_URI,
            "auth": {"type": "sigv4", "sigv4": {"delegate": {"type": "noop"}}},
            "rest.signing-region": "us-east-1",
            "client.access-key-id": "id",
            "client.secret-access-key": "secret",
        },
    )
    file_io = catalog._load_file_io()
    assert isinstance(file_io.properties[AUTH_MANAGER], NoopAuthManager)
    assert not isinstance(file_io.properties[AUTH_MANAGER], SigV4AuthManager)
    pickle.dumps(file_io)


def test_sigv4_delegate_without_type_raises(rest_mock: Mocker) -> None:
    """A non-empty delegate block without a type is a config error, not 'no delegate'."""
    with pytest.raises(ValueError, match="auth.sigv4.delegate.type must be defined"):
        RestCatalog(
            "rest",
            **{  # type: ignore
                "uri": TEST_URI,
                "auth": {"type": "sigv4", "sigv4": {"delegate": {"oauth2": {"client_id": "x"}}}},
                "rest.signing-region": "us-east-1",
                "client.access-key-id": "id",
                "client.secret-access-key": "secret",
            },
        )


def test_stripped_auth_stays_stripped_across_foreign_same_origin_hops(requests_mock: Mocker) -> None:
    """A -> B (cross-origin, stripped) -> B (same-origin) must NOT re-apply catalog auth on B."""
    import boto3

    from pyiceberg.catalog.rest.auth import ReauthenticatingSession, SigV4AuthManager

    boto_session = boto3.Session(aws_access_key_id="id", aws_secret_access_key="secret", region_name="us-east-1")
    manager = SigV4AuthManager(
        delegate=BasicAuthManager(username="u", password="p"),
        boto_session=boto_session,
        region="us-east-1",
        signing_url_prefix="https://catalog.example.com/",
    )

    session = ReauthenticatingSession(trusted_auth_origin="https://catalog.example.com/")
    session.auth = AuthManagerAdapter(manager)

    requests_mock.get("https://catalog.example.com/v1/a", status_code=302, headers={"Location": "https://other.example.net/b"})
    requests_mock.get("https://other.example.net/b", status_code=302, headers={"Location": "https://other.example.net/c"})
    requests_mock.get("https://other.example.net/c", json={})

    session.get("https://catalog.example.com/v1/a")

    first, second, third = requests_mock.request_history
    assert first.headers["Authorization"].startswith("AWS4-HMAC-SHA256 Credential=")
    for hop in (second, third):
        assert "Authorization" not in hop.headers
        assert "Original-Authorization" not in hop.headers


def test_auth_reapplied_when_chain_returns_to_trusted_origin(requests_mock: Mocker) -> None:
    """A -> B (stripped) -> back to A: the trusted origin gets fresh auth again."""
    import boto3

    from pyiceberg.catalog.rest.auth import ReauthenticatingSession, SigV4AuthManager

    boto_session = boto3.Session(aws_access_key_id="id", aws_secret_access_key="secret", region_name="us-east-1")
    manager = SigV4AuthManager(
        delegate=BasicAuthManager(username="u", password="p"),
        boto_session=boto_session,
        region="us-east-1",
        signing_url_prefix="https://catalog.example.com/",
    )

    session = ReauthenticatingSession(trusted_auth_origin="https://catalog.example.com/")
    session.auth = AuthManagerAdapter(manager)

    requests_mock.get("https://catalog.example.com/v1/a", status_code=302, headers={"Location": "https://other.example.net/b"})
    requests_mock.get("https://other.example.net/b", status_code=302, headers={"Location": "https://catalog.example.com/v1/c"})
    requests_mock.get("https://catalog.example.com/v1/c", json={})

    session.get("https://catalog.example.com/v1/a")

    _, second, third = requests_mock.request_history
    assert "Authorization" not in second.headers
    assert third.headers["Authorization"].startswith("AWS4-HMAC-SHA256 Credential=")
    assert third.headers["Original-Authorization"].startswith("Basic ")


def test_sigv4_without_delegate_preserves_legacy_token(rest_mock: Mocker) -> None:
    """Migrating rest.sigv4-enabled + token to auth.type=sigv4 keeps the legacy OAuth delegate."""
    from pyiceberg.catalog.rest.auth import LegacyOAuth2AuthManager, SigV4AuthManager

    catalog = RestCatalog(
        "rest",
        **{  # type: ignore
            "uri": TEST_URI,
            "token": "legacy-token",
            "auth": {"type": "sigv4"},
            "rest.signing-region": "us-east-1",
            "client.access-key-id": "id",
            "client.secret-access-key": "secret",
        },
    )
    assert isinstance(catalog._auth_manager, SigV4AuthManager)
    assert isinstance(catalog._auth_manager.delegate, LegacyOAuth2AuthManager)
    assert catalog._auth_manager.delegate.auth_header() == "Bearer legacy-token"


def test_redirect_leaving_signing_prefix_clears_stale_artifacts(requests_mock: Mocker) -> None:
    """A same-origin redirect that leaves the signing scope must not carry the old hop's signature."""
    import boto3

    from pyiceberg.catalog.rest.auth import ReauthenticatingSession, SigV4AuthManager

    boto_session = boto3.Session(
        aws_access_key_id="id", aws_secret_access_key="secret", aws_session_token="sts-token", region_name="us-east-1"
    )
    manager = SigV4AuthManager(
        delegate=BasicAuthManager(username="u", password="p"),
        boto_session=boto_session,
        region="us-east-1",
        signing_url_prefix="https://host.example.com/catalog/",
    )

    session = ReauthenticatingSession(trusted_auth_origin="https://host.example.com/catalog/")
    session.auth = AuthManagerAdapter(manager)

    requests_mock.get(
        "https://host.example.com/catalog/v1/a", status_code=302, headers={"Location": "https://host.example.com/other"}
    )
    requests_mock.get("https://host.example.com/other", json={})

    session.get("https://host.example.com/catalog/v1/a")

    first, second = requests_mock.request_history
    assert first.headers["Authorization"].startswith("AWS4-HMAC-SHA256 Credential=")
    assert second.headers["Authorization"].startswith("Basic ")
    for header in ("X-Amz-Date", "X-Amz-Security-Token", "x-amz-content-sha256", "Original-Authorization"):
        assert header not in second.headers


def test_redirect_leaving_signing_prefix_with_headerless_delegate(requests_mock: Mocker) -> None:
    """With a header-less delegate, the stale AWS4 Authorization itself must be dropped."""
    import boto3

    from pyiceberg.catalog.rest.auth import ReauthenticatingSession, SigV4AuthManager

    boto_session = boto3.Session(aws_access_key_id="id", aws_secret_access_key="secret", region_name="us-east-1")
    manager = SigV4AuthManager(
        delegate=NoopAuthManager(),
        boto_session=boto_session,
        region="us-east-1",
        signing_url_prefix="https://host.example.com/catalog/",
    )

    session = ReauthenticatingSession(trusted_auth_origin="https://host.example.com/catalog/")
    session.auth = AuthManagerAdapter(manager)

    requests_mock.get(
        "https://host.example.com/catalog/v1/a", status_code=302, headers={"Location": "https://host.example.com/other"}
    )
    requests_mock.get("https://host.example.com/other", json={})

    session.get("https://host.example.com/catalog/v1/a")

    _, second = requests_mock.request_history
    for header in ("Authorization", "X-Amz-Date", "x-amz-content-sha256", "Original-Authorization"):
        assert header not in second.headers


def test_sigv4_applies_delegate_sign_request_hook() -> None:
    """A delegate's sign_request mutations must be applied (and signed) when nested."""
    import boto3

    from pyiceberg.catalog.rest.auth import AuthManager, SigV4AuthManager

    class HookedAuthManager(AuthManager):
        def auth_header(self) -> None:
            return None

        def sign_request(self, request: requests.PreparedRequest) -> requests.PreparedRequest:
            request.headers["X-Delegate-Hook"] = "applied"
            return request

    boto_session = boto3.Session(aws_access_key_id="id", aws_secret_access_key="secret", region_name="us-east-1")
    manager = SigV4AuthManager(delegate=HookedAuthManager(), boto_session=boto_session, region="us-east-1")

    prepared = requests.Request("GET", "https://example.com/v1/config").prepare()
    manager.sign_request(prepared)

    assert prepared.headers["X-Delegate-Hook"] == "applied"
    assert "x-delegate-hook" in prepared.headers["Authorization"]


def test_reauthenticating_session_survives_pickle() -> None:
    """The trusted origin must survive a Session pickle round trip."""
    import pickle

    from pyiceberg.catalog.rest.auth import ReauthenticatingSession

    session = ReauthenticatingSession(trusted_auth_origin="https://cat.example.com/")
    restored = pickle.loads(pickle.dumps(session))
    assert restored._trusted_auth_origin == "https://cat.example.com/"


def test_cross_origin_strips_custom_auth_headers(requests_mock: Mocker) -> None:
    """Headers introduced by a custom sign_request hook must not cross origins."""
    from pyiceberg.catalog.rest.auth import AuthManager, ReauthenticatingSession

    class ApiKeyAuthManager(AuthManager):
        def auth_header(self) -> None:
            return None

        def sign_request(self, request: requests.PreparedRequest) -> requests.PreparedRequest:
            request.headers["X-Api-Key"] = "secret-key"
            return request

    session = ReauthenticatingSession(trusted_auth_origin="https://example.com/")
    session.auth = AuthManagerAdapter(ApiKeyAuthManager())

    requests_mock.get("https://example.com/v1/a", status_code=302, headers={"Location": "https://other.example.net/b"})
    requests_mock.get("https://other.example.net/b", json={})

    session.get("https://example.com/v1/a")

    first, second = requests_mock.request_history
    assert first.headers["X-Api-Key"] == "secret-key"
    assert "X-Api-Key" not in second.headers


def test_redirect_honors_copy_returning_sign_request(requests_mock: Mocker) -> None:
    """An auth hook that returns a replacement request must take effect on redirects."""
    import boto3

    from pyiceberg.catalog.rest.auth import AuthManager, ReauthenticatingSession, SigV4AuthManager

    class CopyingAuthManager(AuthManager):
        def auth_header(self) -> None:
            return None

        def sign_request(self, request: requests.PreparedRequest) -> requests.PreparedRequest:
            replacement = request.copy()
            replacement.headers["X-Copy"] = "yes"
            return replacement

    boto_session = boto3.Session(aws_access_key_id="id", aws_secret_access_key="secret", region_name="us-east-1")
    manager = SigV4AuthManager(delegate=CopyingAuthManager(), boto_session=boto_session, region="us-east-1")

    session = ReauthenticatingSession(trusted_auth_origin="https://example.com/")
    session.auth = AuthManagerAdapter(manager)

    requests_mock.get("https://example.com/v1/a", status_code=302, headers={"Location": "https://example.com/v1/b"})
    requests_mock.get("https://example.com/v1/b", json={})

    session.get("https://example.com/v1/a")

    first, second = requests_mock.request_history
    assert first.headers["X-Copy"] == "yes"
    assert second.headers["X-Copy"] == "yes"
    # The redirected hop carries a fresh signature, not the discarded stale one.
    assert second.headers["Authorization"].startswith("AWS4-HMAC-SHA256 Credential=")
    assert second.headers["Authorization"] != first.headers["Authorization"]


def test_stable_custom_auth_header_stripped_after_intermediate_hop(requests_mock: Mocker) -> None:
    """A -> A (same-origin) -> B: tracking must survive the middle hop so B gets no credential."""
    from pyiceberg.catalog.rest.auth import AuthManager, ReauthenticatingSession

    class ApiKeyAuthManager(AuthManager):
        def auth_header(self) -> None:
            return None

        def sign_request(self, request: requests.PreparedRequest) -> requests.PreparedRequest:
            request.headers["X-Api-Key"] = "secret-key"
            return request

    session = ReauthenticatingSession(trusted_auth_origin="https://example.com/")
    session.auth = AuthManagerAdapter(ApiKeyAuthManager())

    requests_mock.get("https://example.com/v1/a", status_code=302, headers={"Location": "https://example.com/v1/b"})
    requests_mock.get("https://example.com/v1/b", status_code=302, headers={"Location": "https://other.example.net/c"})
    requests_mock.get("https://other.example.net/c", json={})

    session.get("https://example.com/v1/a")

    first, second, third = requests_mock.request_history
    assert first.headers["X-Api-Key"] == "secret-key"
    assert second.headers["X-Api-Key"] == "secret-key"
    assert "X-Api-Key" not in third.headers


def test_copy_returning_manager_tracking_survives_chain(requests_mock: Mocker) -> None:
    """A -> A -> B with a copy-returning manager: stable credentials still stripped at B."""
    from pyiceberg.catalog.rest.auth import AuthManager, ReauthenticatingSession

    class CopyingApiKeyManager(AuthManager):
        def auth_header(self) -> None:
            return None

        def sign_request(self, request: requests.PreparedRequest) -> requests.PreparedRequest:
            replacement = request.copy()
            replacement.headers["X-Api-Key"] = "secret-key"
            return replacement

    session = ReauthenticatingSession(trusted_auth_origin="https://example.com/")
    session.auth = AuthManagerAdapter(CopyingApiKeyManager())

    requests_mock.get("https://example.com/v1/a", status_code=302, headers={"Location": "https://example.com/v1/b"})
    requests_mock.get("https://example.com/v1/b", status_code=302, headers={"Location": "https://other.example.net/c"})
    requests_mock.get("https://other.example.net/c", json={})

    session.get("https://example.com/v1/a")

    first, second, third = requests_mock.request_history
    assert first.headers["X-Api-Key"] == "secret-key"
    assert second.headers["X-Api-Key"] == "secret-key"
    assert "X-Api-Key" not in third.headers


def test_signing_scope_normalizes_default_port() -> None:
    """An explicit default port in the configured prefix must still match normalized URLs."""
    import boto3

    from pyiceberg.catalog.rest.auth import SigV4AuthManager

    boto_session = boto3.Session(aws_access_key_id="id", aws_secret_access_key="secret", region_name="us-east-1")
    manager = SigV4AuthManager(
        delegate=NoopAuthManager(),
        boto_session=boto_session,
        region="us-east-1",
        signing_url_prefix="https://catalog.example.com:443/",
    )

    prepared = requests.Request("GET", "https://catalog.example.com/v1/config").prepare()
    manager.sign_request(prepared)
    assert prepared.headers["Authorization"].startswith("AWS4-HMAC-SHA256 Credential=")


def test_sigv4_file_io_pickles_with_legacy_delegate(rest_mock: Mocker) -> None:
    """FileIO from a sigv4 catalog with the legacy token fallback must pickle."""
    import pickle

    catalog = RestCatalog(
        "rest",
        **{  # type: ignore
            "uri": TEST_URI,
            "token": "legacy-token",
            "auth": {"type": "sigv4"},
            "rest.signing-region": "us-east-1",
            "client.access-key-id": "id",
            "client.secret-access-key": "secret",
        },
    )
    pickle.dumps(catalog._load_file_io())


def test_deserialized_sigv4_manager_refuses_to_sign() -> None:
    """A pickled SigV4AuthManager restores auth_header/delegate but refuses to sign."""
    import pickle

    import boto3

    from pyiceberg.catalog.rest.auth import SigV4AuthManager

    boto_session = boto3.Session(aws_access_key_id="id", aws_secret_access_key="secret", region_name="us-east-1")
    manager = SigV4AuthManager(
        delegate=BasicAuthManager(username="u", password="p"), boto_session=boto_session, region="us-east-1"
    )

    restored = pickle.loads(pickle.dumps(manager))
    assert restored.auth_header().startswith("Basic ")
    with pytest.raises(ValueError, match="cannot sign after deserialization"):
        restored.sign_request(requests.Request("GET", "https://example.com/v1/config").prepare())
