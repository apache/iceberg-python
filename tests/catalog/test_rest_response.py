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

import json

import pytest
from requests import HTTPError, Response

from pyiceberg.catalog.rest.response import _handle_non_200_response
from pyiceberg.exceptions import (
    AuthorizationExpiredError,
    BadRequestError,
    ForbiddenError,
    NoSuchTableError,
    OAuthError,
    RESTError,
    ServerError,
    ServiceUnavailableError,
    TooManyRequestsError,
    UnauthorizedError,
)


def _make_http_error(status_code: int, body: str = "", reason: str | None = None) -> HTTPError:
    response = Response()
    response.status_code = status_code
    response._content = body.encode("utf-8") if body else b""
    if reason is not None:
        response.reason = reason
    return HTTPError(response=response)


def _error_body(message: str, error_type: str, code: int) -> str:
    return json.dumps({"error": {"message": message, "type": error_type, "code": code}})


@pytest.mark.parametrize(
    "status_code, expected_exception",
    [
        (400, BadRequestError),
        (401, UnauthorizedError),
        (403, ForbiddenError),
        (419, AuthorizationExpiredError),
        (422, RESTError),
        (429, TooManyRequestsError),
        (501, NotImplementedError),
        (503, ServiceUnavailableError),
        (500, ServerError),
        (502, ServerError),
        (504, ServerError),
        (999, RESTError),
    ],
)
def test_status_code_maps_to_exception(status_code: int, expected_exception: type[Exception]) -> None:
    body = _error_body("something went wrong", "SomeError", status_code)
    exc = _make_http_error(status_code, body=body)

    with pytest.raises(expected_exception, match="SomeError: something went wrong"):
        _handle_non_200_response(exc, {})


def test_error_handler_overrides_default_mapping() -> None:
    body = _error_body("Table does not exist: ns.tbl", "NoSuchTableException", 404)
    exc = _make_http_error(404, body=body)

    with pytest.raises(NoSuchTableError, match="NoSuchTableException: Table does not exist: ns.tbl"):
        _handle_non_200_response(exc, {404: NoSuchTableError})


@pytest.mark.parametrize(
    "status_code, body, expected_exception",
    [
        (500, "not json at all", ServerError),
        (400, '{"unexpected": "structure"}', BadRequestError),
    ],
)
def test_unparseable_body_falls_back_to_validation_error(
    status_code: int, body: str, expected_exception: type[Exception]
) -> None:
    exc = _make_http_error(status_code, body=body)

    with pytest.raises(expected_exception, match="Received unexpected JSON Payload"):
        _handle_non_200_response(exc, {})


def test_empty_body_bypasses_pydantic() -> None:
    exc = _make_http_error(403, body="", reason="Forbidden")

    with pytest.raises(ForbiddenError, match="ForbiddenError: RestError: Forbidden"):
        _handle_non_200_response(exc, {})


def test_empty_body_falls_back_to_http_status_phrase() -> None:
    exc = _make_http_error(503, body="")
    exc.response.reason = None

    with pytest.raises(ServiceUnavailableError, match="ServiceUnavailableError: RestError: Service Unavailable"):
        _handle_non_200_response(exc, {})


def test_oauth_error_with_description() -> None:
    body = json.dumps({
        "error": "invalid_client",
        "error_description": "Client authentication failed",
    })
    exc = _make_http_error(401, body=body)

    with pytest.raises(OAuthError, match="invalid_client: Client authentication failed"):
        _handle_non_200_response(exc, {401: OAuthError})


def test_oauth_error_with_uri() -> None:
    body = json.dumps({
        "error": "invalid_scope",
        "error_description": "scope not allowed",
        "error_uri": "https://example.com/help",
    })
    exc = _make_http_error(400, body=body)

    with pytest.raises(OAuthError, match=r"invalid_scope: scope not allowed \(https://example.com/help\)"):
        _handle_non_200_response(exc, {400: OAuthError})


def test_oauth_error_without_description() -> None:
    body = json.dumps({"error": "invalid_grant"})
    exc = _make_http_error(401, body=body)

    with pytest.raises(OAuthError, match="^invalid_grant$"):
        _handle_non_200_response(exc, {401: OAuthError})


def test_none_response_raises_value_error() -> None:
    exc = HTTPError()
    exc.response = None

    with pytest.raises(ValueError, match="Did not receive a response"):
        _handle_non_200_response(exc, {})
