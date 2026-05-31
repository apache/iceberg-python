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
from __future__ import annotations

from typing import Any
from urllib import parse

import boto3
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest
from requests import PreparedRequest, Session
from requests.adapters import HTTPAdapter

from pyiceberg.catalog import BOTOCORE_SESSION
from pyiceberg.io import (
    AWS_ACCESS_KEY_ID,
    AWS_PROFILE_NAME,
    AWS_REGION,
    AWS_SECRET_ACCESS_KEY,
    AWS_SESSION_TOKEN,
)
from pyiceberg.utils.properties import get_first_property_value, property_as_int

SIGV4_REGION = "rest.signing-region"
SIGV4_SERVICE = "rest.signing-name"
SIGV4_MAX_RETRIES = "rest.sigv4.max-retries"
SIGV4_MAX_RETRIES_DEFAULT = 10
EMPTY_BODY_SHA256: str = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"


class SigV4Adapter(HTTPAdapter):
    def __init__(self, **properties: str):
        self._properties = properties
        max_retries = property_as_int(self._properties, SIGV4_MAX_RETRIES, SIGV4_MAX_RETRIES_DEFAULT)
        super().__init__(max_retries=max_retries)
        self._boto_session = boto3.Session(
            profile_name=get_first_property_value(self._properties, AWS_PROFILE_NAME),
            region_name=get_first_property_value(self._properties, AWS_REGION),
            botocore_session=self._properties.get(BOTOCORE_SESSION),
            aws_access_key_id=get_first_property_value(self._properties, AWS_ACCESS_KEY_ID),
            aws_secret_access_key=get_first_property_value(self._properties, AWS_SECRET_ACCESS_KEY),
            aws_session_token=get_first_property_value(self._properties, AWS_SESSION_TOKEN),
        )

    def add_headers(self, request: PreparedRequest, **kwargs: Any) -> None:  # pylint: disable=W0613
        credentials = self._boto_session.get_credentials().get_frozen_credentials()
        region = self._properties.get(SIGV4_REGION, self._boto_session.region_name)
        service = self._properties.get(SIGV4_SERVICE, "execute-api")

        url = str(request.url).split("?")[0]
        query = str(parse.urlsplit(request.url).query)
        params = dict(parse.parse_qsl(query))

        # remove the connection header as it will be updated after signing
        if "connection" in request.headers:
            del request.headers["connection"]
        # For empty bodies, explicitly set the content hash header to the SHA256 of an empty string
        if not request.body:
            request.headers["x-amz-content-sha256"] = EMPTY_BODY_SHA256

        aws_request = AWSRequest(method=request.method, url=url, params=params, data=request.body, headers=dict(request.headers))

        SigV4Auth(credentials, service, region).add_auth(aws_request)
        original_header = request.headers
        signed_headers = aws_request.headers
        relocated_headers = {}

        # relocate headers if there is a conflict with signed headers
        for header, value in original_header.items():
            if header in signed_headers and signed_headers[header] != value:
                relocated_headers[f"Original-{header}"] = value

        request.headers.update(relocated_headers)
        request.headers.update(signed_headers)


def init_sigv4(session: Session, uri: str, properties: dict[str, str]) -> None:
    session.mount(uri, SigV4Adapter(**properties))
