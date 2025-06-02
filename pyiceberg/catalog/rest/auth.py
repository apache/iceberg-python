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
from abc import ABC, abstractmethod
from typing import Any, Dict, Optional, Type

from requests import HTTPError, PreparedRequest, Session
from requests.auth import AuthBase

from pyiceberg.catalog.rest.response import TokenResponse, _handle_non_200_response
from pyiceberg.exceptions import OAuthError

COLON = ":"


class AuthManager(ABC):
    """
    Abstract base class for Authentication Managers used to supply authorization headers to HTTP clients (e.g. requests.Session).

    Subclasses must implement the `auth_header` method to return an Authorization header value.
    """

    @abstractmethod
    def auth_header(self) -> Optional[str]:
        """Return the Authorization header value, or None if not applicable."""


class NoopAuthManager(AuthManager):
    def auth_header(self) -> Optional[str]:
        return None


class BasicAuthManager(AuthManager):
    def __init__(self, username: str, password: str):
        credentials = f"{username}:{password}"
        self._token = base64.b64encode(credentials.encode()).decode()

    def auth_header(self) -> str:
        return f"Basic {self._token}"


class LegacyOAuth2AuthManager(AuthManager):
    _session: Session
    _auth_url: Optional[str]
    _token: Optional[str]
    _credential: Optional[str]
    _optional_oauth_params: Optional[Dict[str, str]]

    def __init__(
        self,
        session: Session,
        auth_url: Optional[str] = None,
        credential: Optional[str] = None,
        initial_token: Optional[str] = None,
        optional_oauth_params: Optional[Dict[str, str]] = None,
    ):
        self._session = session
        self._auth_url = auth_url
        self._token = initial_token
        self._credential = credential
        self._optional_oauth_params = optional_oauth_params
        self._refresh_token()

    def _fetch_access_token(self, credential: str) -> str:
        if COLON in credential:
            client_id, client_secret = credential.split(COLON)
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


class AuthManagerAdapter(AuthBase):
    """A `requests.auth.AuthBase` adapter that integrates an `AuthManager` into a `requests.Session` to automatically attach the appropriate Authorization header to every request.

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
        Modify the outgoing request to include the Authorization header.

        Args:
            request (requests.PreparedRequest): The HTTP request being prepared.

        Returns:
            requests.PreparedRequest: The modified request with Authorization header.
        """
        if auth_header := self.auth_manager.auth_header():
            request.headers["Authorization"] = auth_header
        return request


class AuthManagerFactory:
    _registry: Dict[str, Type["AuthManager"]] = {}

    @classmethod
    def register(cls, name: str, auth_manager_class: Type["AuthManager"]) -> None:
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
    def create(cls, class_or_name: str, config: Dict[str, Any]) -> AuthManager:
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
