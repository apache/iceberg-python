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
from abc import ABC, abstractmethod
from typing import Optional

from requests import PreparedRequest
from requests.auth import AuthBase


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
