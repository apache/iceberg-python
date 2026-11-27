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

import abc
from typing import TYPE_CHECKING, Any

import requests
from requests import HTTPError

from pyiceberg.catalog import TOKEN, URI
from pyiceberg.catalog.rest import _user_agent
from pyiceberg.catalog.rest.auth import AUTH_MANAGER
from pyiceberg.exceptions import SignError
from pyiceberg.io import S3_SIGNER, S3_SIGNER_ENDPOINT, S3_SIGNER_ENDPOINT_DEFAULT, S3_SIGNER_URI
from pyiceberg.typedef import Properties
from pyiceberg.utils.properties import get_header_properties

if TYPE_CHECKING:
    from botocore.awsrequest import AWSRequest


class S3RequestSigner(abc.ABC):
    """Abstract base class for S3 request signers."""

    properties: Properties

    def __init__(self, properties: Properties) -> None:
        self.properties = properties

    @abc.abstractmethod
    def __call__(self, request: "AWSRequest", **_: Any) -> None:
        pass


class S3V4RestSigner(S3RequestSigner):
    """An S3 request signer that uses an external REST signing service to sign requests."""

    _session: requests.Session

    def __init__(self, properties: Properties) -> None:
        super().__init__(properties)
        self._session = requests.Session()

    def __call__(self, request: "AWSRequest", operation_name: str | None = None, **_: Any) -> None:
        signer_url = self.properties.get(S3_SIGNER_URI, self.properties[URI]).rstrip("/")
        signer_endpoint = self.properties.get(S3_SIGNER_ENDPOINT, S3_SIGNER_ENDPOINT_DEFAULT)

        signer_headers: dict[str, str] = {}

        auth_header: str | None = None
        if auth_manager := self.properties.get(AUTH_MANAGER):
            auth_header = auth_manager.auth_header()
        elif token := self.properties.get(TOKEN):
            auth_header = f"Bearer {token}"

        if auth_header:
            signer_headers["Authorization"] = auth_header

        signer_headers.update(get_header_properties(self.properties))
        signer_headers["User-Agent"] = _user_agent()

        signer_body = {
            "method": request.method,
            "region": request.context["client_region"],
            "uri": request.url,
            "headers": {key: [val] for key, val in request.headers.items()},
        }
        if operation_name == "DeleteObjects":
            signer_body["body"] = request.body.decode("utf-8")

        response = self._session.post(f"{signer_url}/{signer_endpoint.strip()}", headers=signer_headers, json=signer_body)
        try:
            response.raise_for_status()
            response_json = response.json()
        except HTTPError as e:
            raise SignError(f"Failed to sign request {response.status_code}: {signer_body}") from e

        for key, value in response_json["headers"].items():
            del request.headers[key]
            request.headers.add_header(key, ", ".join(value))

        request.url = response_json["uri"]


SIGNERS: dict[str, type[S3RequestSigner]] = {"S3V4RestSigner": S3V4RestSigner}


def create_s3_request_signer(properties: Properties) -> S3RequestSigner:
    """Create the configured S3 request signer.

    Args:
        properties: Catalog and FileIO properties.

    Returns:
        The configured signer instance.

    Raises:
        ValueError: If the configured signer is not available.
    """
    signer = properties.get(S3_SIGNER)
    if signer_cls := SIGNERS.get(signer):
        return signer_cls(properties)
    raise ValueError(f"Signer not available: {signer}")
