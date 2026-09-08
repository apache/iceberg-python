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

import base64
import importlib
import logging
import threading
import time
from abc import ABC, abstractmethod
from functools import cache, cached_property
from typing import Any

import requests
from requests import HTTPError, PreparedRequest, Response, Session
from requests.auth import AuthBase
from requests.structures import CaseInsensitiveDict

from pyiceberg.catalog.rest.response import TokenResponse, _handle_non_200_response
from pyiceberg.exceptions import OAuthError

AUTH_MANAGER = "auth.manager"

COLON = ":"
logger = logging.getLogger(__name__)

# SHA-256 of an empty payload. Used as the x-amz-content-sha256 header value for
# empty-body requests, matching Iceberg Java's RESTSigV4AuthSession workaround.
EMPTY_BODY_SHA256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"


@cache
def _iceberg_sigv4_auth_class() -> type:
    """Lazily build the botocore SigV4Auth subclass (botocore is an optional dependency)."""
    from urllib import parse

    from botocore.auth import SigV4Auth
    from botocore.awsrequest import AWSRequest

    class _IcebergSigV4Auth(SigV4Auth):
        def canonical_request(self, request: AWSRequest) -> str:
            # Override forces the hex payload hash in the canonical request even when
            # the x-amz-content-sha256 header is base64 (see SigV4AuthManager.sign_request).
            # Mirrors botocore <=1.42.x SigV4Auth.canonical_request layout:
            # https://github.com/boto/botocore/blob/1.42.85/botocore/auth.py#L622-L637
            cr = [request.method.upper()]
            path = self._normalize_url_path(parse.urlsplit(request.url).path)
            cr.append(path)
            cr.append(self.canonical_query_string(request))
            headers_to_sign = self.headers_to_sign(request)
            cr.append(self.canonical_headers(headers_to_sign) + "\n")
            cr.append(self.signed_headers(headers_to_sign))
            cr.append(self.payload(request))
            return "\n".join(cr)

    return _IcebergSigV4Auth


class AuthManager(ABC):
    """
    Abstract base class for Authentication Managers used to supply authorization headers to HTTP clients (e.g. requests.Session).

    Subclasses must implement the `auth_header` method to return an Authorization header value.
    """

    @abstractmethod
    def auth_header(self) -> str | None:
        """Return the Authorization header value, or None if not applicable."""

    def sign_request(self, request: PreparedRequest) -> PreparedRequest:
        """Optionally sign or otherwise modify the prepared request.

        The default implementation is a no-op. Override for request-signing
        schemes such as SigV4 that must inspect the full request.
        """
        return request


class NoopAuthManager(AuthManager):
    """Auth Manager implementation with no auth."""

    def auth_header(self) -> str | None:
        return None


class BasicAuthManager(AuthManager):
    """AuthManager implementation that supports basic password auth."""

    def __init__(self, username: str, password: str):
        credentials = f"{username}:{password}"
        self._token = base64.b64encode(credentials.encode()).decode()

    def auth_header(self) -> str:
        return f"Basic {self._token}"


class LegacyOAuth2AuthManager(AuthManager):
    """Legacy OAuth2 AuthManager implementation.

    This class exists for backward compatibility, and will be removed in
    PyIceberg 1.0.0 in favor of OAuth2AuthManager.
    """

    _session: Session
    _auth_url: str | None
    _token: str | None
    _credential: str | None
    _optional_oauth_params: dict[str, str] | None

    def __init__(
        self,
        session: Session,
        auth_url: str | None = None,
        credential: str | None = None,
        initial_token: str | None = None,
        optional_oauth_params: dict[str, str] | None = None,
    ):
        self._session = session
        self._auth_url = auth_url
        self._token = initial_token
        self._credential = credential
        self._optional_oauth_params = optional_oauth_params
        self._refresh_token()

    def _fetch_access_token(self, credential: str) -> str:
        if COLON in credential:
            client_id, client_secret = credential.split(COLON, maxsplit=1)
        else:
            client_id, client_secret = None, credential

        data = {"grant_type": "client_credentials", "client_id": client_id, "client_secret": client_secret}

        if self._optional_oauth_params:
            data.update(self._optional_oauth_params)

        if self._auth_url is None:
            raise ValueError("Cannot fetch access token from undefined auth_url")

        response = self._session.post(
            url=self._auth_url, data=data, headers={**self._session.headers, "Content-type": "application/x-www-form-urlencoded"}
        )
        try:
            response.raise_for_status()
        except HTTPError as exc:
            _handle_non_200_response(exc, {400: OAuthError, 401: OAuthError})

        return TokenResponse.model_validate_json(response.text).access_token

    def _refresh_token(self) -> None:
        if self._credential is not None:
            self._token = self._fetch_access_token(self._credential)

    def auth_header(self) -> str:
        return f"Bearer {self._token}"


class OAuth2TokenProvider:
    """Thread-safe OAuth2 token provider with token refresh support."""

    client_id: str
    client_secret: str
    token_url: str
    scope: str | None
    refresh_margin: int
    expires_in: int | None

    _token: str | None
    _expires_at: int
    _lock: threading.Lock

    def __init__(
        self,
        client_id: str,
        client_secret: str,
        token_url: str,
        scope: str | None = None,
        refresh_margin: int = 60,
        expires_in: int | None = None,
    ):
        self.client_id = client_id
        self.client_secret = client_secret
        self.token_url = token_url
        self.scope = scope
        self.refresh_margin = refresh_margin
        self.expires_in = expires_in

        self._token = None
        self._expires_at = 0
        self._lock = threading.Lock()

    @cached_property
    def _client_secret_header(self) -> str:
        creds = f"{self.client_id}:{self.client_secret}"
        creds_bytes = creds.encode("utf-8")
        b64_creds = base64.b64encode(creds_bytes).decode("utf-8")
        return f"Basic {b64_creds}"

    def _refresh_token(self) -> None:
        data = {"grant_type": "client_credentials"}
        if self.scope:
            data["scope"] = self.scope

        response = requests.post(self.token_url, data=data, headers={"Authorization": self._client_secret_header})
        response.raise_for_status()
        result = response.json()

        self._token = result["access_token"]
        expires_in = result.get("expires_in", self.expires_in)
        if expires_in is None:
            raise ValueError(
                "The expiration time of the Token must be provided by the Server in the Access Token Response "
                "in `expires_in` field, or by the PyIceberg Client."
            )
        self._expires_at = time.monotonic() + expires_in - self.refresh_margin

    def get_token(self) -> str:
        with self._lock:
            if not self._token or time.monotonic() >= self._expires_at:
                self._refresh_token()
            if self._token is None:
                raise ValueError("Authorization token is None after refresh")
            return self._token


class OAuth2AuthManager(AuthManager):
    """Auth Manager implementation that supports OAuth2 as defined in IETF RFC6749."""

    def __init__(
        self,
        client_id: str,
        client_secret: str,
        token_url: str,
        scope: str | None = None,
        refresh_margin: int = 60,
        expires_in: int | None = None,
    ):
        self.token_provider = OAuth2TokenProvider(
            client_id,
            client_secret,
            token_url,
            scope,
            refresh_margin,
            expires_in,
        )

    def auth_header(self) -> str:
        return f"Bearer {self.token_provider.get_token()}"


class GoogleAuthManager(AuthManager):
    """An auth manager that is responsible for handling Google credentials."""

    def __init__(self, credentials_path: str | None = None, scopes: list[str] | None = None):
        """
        Initialize GoogleAuthManager.

        Args:
            credentials_path: Optional path to Google credentials JSON file.
            scopes: Optional list of OAuth2 scopes.
        """
        try:
            import google.auth
            import google.auth.transport.requests
        except ImportError as e:
            raise ImportError("Google Auth libraries not found. Please install 'google-auth'.") from e

        if credentials_path:
            self.credentials, _ = google.auth.load_credentials_from_file(credentials_path, scopes=scopes)
        else:
            logger.info("Using Google Default Application Credentials")
            self.credentials, _ = google.auth.default(scopes=scopes)
        self._auth_request = google.auth.transport.requests.Request()

    def auth_header(self) -> str:
        self.credentials.refresh(self._auth_request)
        return f"Bearer {self.credentials.token}"


class EntraAuthManager(AuthManager):
    """Auth Manager implementation that supports Microsoft Entra ID (Azure AD) authentication.

    This manager uses the Azure Identity library's DefaultAzureCredential which automatically
    tries multiple authentication methods including environment variables, managed identity,
    and Azure CLI.

    See https://learn.microsoft.com/en-us/azure/developer/python/sdk/authentication/credential-chains
    for more details on DefaultAzureCredential.
    """

    DEFAULT_SCOPE = "https://storage.azure.com/.default"

    def __init__(
        self,
        scopes: list[str] | None = None,
        **credential_kwargs: Any,
    ):
        """
        Initialize EntraAuthManager.

        Args:
            scopes: List of OAuth2 scopes. Defaults to ["https://storage.azure.com/.default"].
            **credential_kwargs: Arguments passed to DefaultAzureCredential.
                Supported authentication methods:
                - Environment Variables: Set AZURE_TENANT_ID, AZURE_CLIENT_ID, AZURE_CLIENT_SECRET
                - Managed Identity: Works automatically on Azure; for user-assigned, pass managed_identity_client_id
                - Azure CLI: Works automatically if logged in via `az login`
                - Workload Identity: Works automatically in AKS with workload identity configured  # codespell:ignore aks
        """
        try:
            from azure.identity import DefaultAzureCredential
        except ImportError as e:
            raise ImportError("Azure Identity library not found. Please install with: pip install pyiceberg[entra-auth]") from e

        self._scopes = scopes or [self.DEFAULT_SCOPE]
        self._lock = threading.Lock()
        self._token: str | None = None
        self._expires_at: float = 0
        self._credential = DefaultAzureCredential(**credential_kwargs)

    def _refresh_token(self) -> None:
        """Refresh the access token from Azure."""
        token = self._credential.get_token(*self._scopes)
        self._token = token.token
        # expires_on is a Unix timestamp; add a 60-second margin for safety
        self._expires_at = token.expires_on - 60

    def _get_token(self) -> str:
        """Get a valid access token, refreshing if necessary."""
        with self._lock:
            if not self._token or time.time() >= self._expires_at:
                self._refresh_token()
            if self._token is None:
                raise ValueError("Failed to obtain Entra access token")
            return self._token

    def auth_header(self) -> str:
        """Return the Authorization header value with a valid Bearer token."""
        return f"Bearer {self._get_token()}"


class SigV4AuthManager(AuthManager):
    """AuthManager that signs requests with AWS SigV4, wrapping a delegate AuthManager.

    Mirrors Iceberg Java's RESTSigV4AuthManager: the delegate AuthManager handles
    header-based auth (e.g. OAuth2), then SigV4 signs the resulting request.
    """

    def __init__(
        self,
        delegate: AuthManager,
        boto_session: Any,
        region: str | None,
        service: str = "execute-api",
        signing_url_prefix: str | None = None,
    ):
        """Initialize SigV4AuthManager.

        Args:
            delegate: AuthManager that supplies header-based auth before signing.
            boto_session: A boto3.Session used to resolve AWS credentials.
            region: SigV4 signing region; falls back to the boto session's region.
            service: SigV4 signing service name.
            signing_url_prefix: When set, only URLs under this prefix are signed;
                the delegate header still applies everywhere.
        """
        self._delegate = delegate
        self._boto_session = boto_session
        self._region = region
        self._service = service
        self._signing_url_prefix = signing_url_prefix

    @property
    def delegate(self) -> AuthManager:
        """The wrapped header-based AuthManager."""
        return self._delegate

    def auth_header(self) -> str | None:
        return self._delegate.auth_header()

    def __getstate__(self) -> dict[str, Any]:
        """Serialize without the boto3 session; restored instances refuse to sign."""
        state = self.__dict__.copy()
        state["_boto_session"] = None
        return state

    def _in_signing_scope(self, url: str) -> bool:
        """Prefix match with normalized scheme/host/default port."""
        from urllib import parse

        default_ports = {"http": 80, "https": 443}

        def parts(value: str) -> tuple[str, str, int | None, str]:
            split = parse.urlsplit(value)
            scheme = split.scheme.lower()
            return scheme, (split.hostname or "").lower(), split.port or default_ports.get(scheme), split.path.lower()

        p_scheme, p_host, p_port, p_path = parts(self._signing_url_prefix or "")
        u_scheme, u_host, u_port, u_path = parts(url)
        return (p_scheme, p_host, p_port) == (u_scheme, u_host, u_port) and u_path.startswith(p_path)

    def sign_request(self, request: PreparedRequest) -> PreparedRequest:
        import hashlib
        from urllib import parse

        from botocore.awsrequest import AWSRequest

        # Apply the delegate's own hook first so its mutations are signed too.
        request = self._delegate.sign_request(request)

        # Only sign catalog requests (case-insensitive, like adapter mounts); a
        # redirect leaving the signing scope must not keep stale artifacts.
        if self._signing_url_prefix and not self._in_signing_scope(str(request.url)):
            if str(request.headers.get("Authorization", "")).startswith("AWS4-HMAC-SHA256"):
                del request.headers["Authorization"]
            for header in ("X-Amz-Date", "x-amz-content-sha256", "X-Amz-Security-Token"):
                request.headers.pop(header, None)
            for header in [h for h in request.headers if h.lower().startswith("original-")]:
                del request.headers[header]
            return request

        if self._boto_session is None:
            raise ValueError("SigV4AuthManager cannot sign after deserialization: the boto3 session is not serialized")
        credentials = self._boto_session.get_credentials().get_frozen_credentials()
        region = self._region or self._boto_session.region_name

        url = str(request.url).split("?")[0]
        query = str(parse.urlsplit(request.url).query)
        params = dict(parse.parse_qsl(query))

        # remove the connection header as it will be updated after signing
        if "connection" in request.headers:
            del request.headers["connection"]

        # Match Iceberg Java's AWS SDK v2 flexible-checksum signing:
        # x-amz-content-sha256 header is base64 for non-empty bodies, hex for empty.
        # The SigV4 canonical request still uses hex (enforced in _iceberg_sigv4_auth_class).
        # Ref: https://github.com/apache/iceberg/blob/main/aws/src/main/java/org/apache/iceberg/aws/RESTSigV4AuthSession.java
        if request.body:
            if isinstance(request.body, str):
                body_bytes = request.body.encode("utf-8")
            elif isinstance(request.body, (bytes, bytearray)):
                body_bytes = bytes(request.body)
            else:
                raise TypeError(
                    f"Unsupported request body type for SigV4 signing: {type(request.body).__name__}; expected str or bytes."
                )
            content_sha256_header = base64.b64encode(hashlib.sha256(body_bytes).digest()).decode()
        else:
            content_sha256_header = EMPTY_BODY_SHA256

        # Case-insensitive: a plain dict would keep duplicate header spellings and
        # let the signed canonical headers diverge from what is sent.
        signing_headers = CaseInsensitiveDict(request.headers)
        # A previous hop's own SigV4 signature is never a delegate header: discard,
        # don't relocate.
        if str(signing_headers.get("Authorization", "")).startswith("AWS4-HMAC-SHA256"):
            signing_headers.pop("Authorization")
        # Relocate Authorization before signing so it lands in SignedHeaders, like Java.
        if "Authorization" in signing_headers:
            signing_headers["Original-Authorization"] = signing_headers.pop("Authorization")
        signing_headers["x-amz-content-sha256"] = content_sha256_header

        aws_request = AWSRequest(method=request.method, url=url, params=params, data=request.body, headers=dict(signing_headers))

        _iceberg_sigv4_auth_class()(credentials, self._service, region).add_auth(aws_request)

        original_header = CaseInsensitiveDict(request.headers)
        signed_headers = CaseInsensitiveDict(dict(aws_request.headers))
        relocated_headers = {}

        # Relocate conflicting headers; Authorization/Original-Authorization are
        # managed above and must never re-relocate into Original-Original-* chains.
        for header, value in original_header.items():
            if header.lower() in ("authorization", "original-authorization"):
                continue
            if header in signed_headers and signed_headers[header] != value:
                relocated_headers[f"Original-{header}"] = value

        request.headers.update(relocated_headers)
        request.headers.update(signed_headers)
        return request


class AuthManagerAdapter(AuthBase):
    """A `requests.auth.AuthBase` adapter for integrating an `AuthManager` into a `requests.Session`.

    This adapter automatically attaches the appropriate Authorization header to every request.
    This adapter is useful when working with `requests.Session.auth`
    and allows reuse of authentication strategies defined by `AuthManager`.
    This AuthManagerAdapter is only intended to be used against the REST Catalog
    Server that expects the Authorization Header.
    """

    def __init__(self, auth_manager: AuthManager):
        """
        Initialize AuthManagerAdapter.

        Args:
            auth_manager (AuthManager): An instance of an AuthManager subclass.
        """
        self.auth_manager = auth_manager

    def __call__(self, request: PreparedRequest) -> PreparedRequest:
        """
        Modify the outgoing request to include the Authorization header and any signature.

        Args:
            request (requests.PreparedRequest): The HTTP request being prepared.

        Returns:
            requests.PreparedRequest: The modified request.
        """
        # Capture tracking before signing: sign_request may return a fresh request.
        tracked = {str(header) for header in getattr(request, "_auth_applied_headers", ())}
        before = {key.lower(): value for key, value in request.headers.items()}
        if auth_header := self.auth_manager.auth_header():
            request.headers["Authorization"] = auth_header
        # Header first, then sign: a request-signing AuthManager (e.g. SigV4) must
        # see the Authorization header so it can relocate it before signing.
        request = self.auth_manager.sign_request(request)
        # Union with earlier hops: a stable credential re-applied with an unchanged
        # value yields an empty diff and would otherwise be forgotten.
        tracked.update(key for key, value in request.headers.items() if before.get(key.lower()) != value)
        request._auth_applied_headers = sorted(tracked)  # type: ignore[attr-defined]
        return request


class ReauthenticatingSession(Session):
    """A Session that re-applies `self.auth` to redirected requests.

    `requests` invokes session auth only on the original request, so a
    request-signing AuthManager (e.g. SigV4) would send a signature computed for the
    pre-redirect URL. Auth is re-applied only to `trusted_auth_origin` (the catalog
    URI): a redirect chain that leaves it stays stripped of all credential-bearing
    headers, and is re-authenticated only if it returns.
    """

    # Class-level default: Session pickles only __attrs__; old pickles fall back here.
    _trusted_auth_origin: str | None = None
    __attrs__ = [*Session.__attrs__, "_trusted_auth_origin"]

    def __init__(self, trusted_auth_origin: str | None = None) -> None:
        super().__init__()
        self._trusted_auth_origin = trusted_auth_origin

    # Stripped on cross-origin redirects (requests strips only Authorization);
    # relocated credentials are stripped separately by their Original-* prefix.
    _SIGNING_ARTIFACT_HEADERS = ("X-Amz-Security-Token", "X-Amz-Date", "x-amz-content-sha256")

    def rebuild_auth(self, prepared_request: PreparedRequest, response: Response) -> None:
        original_url = str(response.request.url)
        super().rebuild_auth(prepared_request, response)
        cross_origin_hop = self.should_strip_auth(original_url, str(prepared_request.url))
        if cross_origin_hop:
            # Strip whatever auth introduced, plus relocated headers and artifacts.
            for header in getattr(response.request, "_auth_applied_headers", ()):
                prepared_request.headers.pop(header, None)
            for header in [h for h in prepared_request.headers if h.lower().startswith("original-")]:
                del prepared_request.headers[header]
            for header in self._SIGNING_ARTIFACT_HEADERS:
                prepared_request.headers.pop(header, None)
        if callable(self.auth) and self._may_reapply_auth(response, prepared_request, cross_origin_hop):
            # copy() drops attributes: carry tracking forward for the adapter's union.
            if prev_tracked := getattr(response.request, "_auth_applied_headers", ()):
                prepared_request._auth_applied_headers = list(prev_tracked)  # type: ignore[attr-defined]
            # prepare_auth honors auth callables that return a replacement request;
            # a caller-supplied static Authorization survives header-less managers.
            prepared_request.prepare_auth(self.auth)

    def _may_reapply_auth(self, response: Response, prepared_request: PreparedRequest, cross_origin_hop: bool) -> bool:
        """Whether re-applying auth on this hop is safe.

        A hop that is same-origin with its predecessor may still sit on a foreign
        host reached through an earlier cross-origin redirect (A -> B -> B), so the
        decision is made against the trusted origin, not the previous hop.
        """
        if self._trusted_auth_origin is not None:
            return not self.should_strip_auth(self._trusted_auth_origin, str(prepared_request.url))
        if cross_origin_hop:
            return False
        # No trusted origin: re-apply only if the previous hop still carried auth.
        return "Authorization" in response.request.headers


class AuthManagerFactory:
    _registry: dict[str, type["AuthManager"]] = {}

    @classmethod
    def register(cls, name: str, auth_manager_class: type["AuthManager"]) -> None:
        """
        Register a string name to a known AuthManager class.

        Args:
            name (str): unique name like 'oauth2' to register the AuthManager with
            auth_manager_class (Type["AuthManager"]): Implementation of AuthManager

        Returns:
            None
        """
        cls._registry[name] = auth_manager_class

    @classmethod
    def create(cls, class_or_name: str, config: dict[str, Any]) -> AuthManager:
        """
        Create an AuthManager by name or fully-qualified class path.

        Args:
            class_or_name (str): Either a name like 'oauth2' or a full class path like 'my.module.CustomAuthManager'
            config (Dict[str, Any]): Configuration passed to the AuthManager constructor

        Returns:
            AuthManager: An instantiated AuthManager subclass
        """
        if class_or_name in cls._registry:
            manager_cls = cls._registry[class_or_name]
        else:
            try:
                module_path, class_name = class_or_name.rsplit(".", 1)
                module = importlib.import_module(module_path)
                manager_cls = getattr(module, class_name)
            except Exception as err:
                raise ValueError(f"Could not load AuthManager class for '{class_or_name}'") from err

        return manager_cls(**config)


AuthManagerFactory.register("noop", NoopAuthManager)
AuthManagerFactory.register("basic", BasicAuthManager)
AuthManagerFactory.register("legacyoauth2", LegacyOAuth2AuthManager)
AuthManagerFactory.register("oauth2", OAuth2AuthManager)
AuthManagerFactory.register("google", GoogleAuthManager)
AuthManagerFactory.register("entra", EntraAuthManager)
