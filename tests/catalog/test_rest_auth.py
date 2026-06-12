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
