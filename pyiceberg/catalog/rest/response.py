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
from json import JSONDecodeError
from typing import Literal, TypeAlias

from pydantic import Field, ValidationError
from requests import HTTPError

from pyiceberg.exceptions import (
    AuthorizationExpiredError,
    BadRequestError,
    CommitFailedException,
    CommitStateUnknownException,
    ForbiddenError,
    NamespaceAlreadyExistsError,
    NamespaceNotEmptyError,
    NoSuchNamespaceError,
    NoSuchTableError,
    NoSuchViewError,
    OAuthError,
    RESTError,
    ServerError,
    ServiceUnavailableError,
    TableAlreadyExistsError,
    UnauthorizedError,
    ViewAlreadyExistsError,
)
from pyiceberg.typedef import IcebergBaseModel


class TokenResponse(IcebergBaseModel):
    access_token: str = Field()
    token_type: str = Field()
    expires_in: int | None = Field(default=None)
    issued_token_type: str | None = Field(default=None)
    refresh_token: str | None = Field(default=None)
    scope: str | None = Field(default=None)


class ErrorResponseMessage(IcebergBaseModel):
    message: str = Field()
    type: str = Field()
    code: int = Field()


class ErrorResponse(IcebergBaseModel):
    error: ErrorResponseMessage = Field()


class OAuthErrorResponse(IcebergBaseModel):
    error: Literal[
        "invalid_request", "invalid_client", "invalid_grant", "unauthorized_client", "unsupported_grant_type", "invalid_scope"
    ]
    error_description: str | None = None
    error_uri: str | None = None


_ErrorHandler: TypeAlias = dict[int, type[Exception]]


class ErrorHandlers:
    """
    Utility class providing static methods to handle HTTP errors for table, namespace, and view operations.

    Maps HTTP error responses to appropriate custom exceptions, ensuring consistent error handling.
    """

    @staticmethod
    def default_error_handler(exc: HTTPError) -> None:
        _handle_non_200_response(exc, {})

    @staticmethod
    def namespace_error_handler(exc: HTTPError) -> None:
        handler: _ErrorHandler = {
            400: BadRequestError,
            404: NoSuchNamespaceError,
            409: NamespaceAlreadyExistsError,
            422: RESTError,
        }

        if "NamespaceNotEmpty" in exc.response.text:
            handler[400] = NamespaceNotEmptyError

        _handle_non_200_response(exc, handler)

    @staticmethod
    def drop_namespace_error_handler(exc: HTTPError) -> None:
        handler: _ErrorHandler = {404: NoSuchNamespaceError, 409: NamespaceNotEmptyError}

        _handle_non_200_response(exc, handler)

    @staticmethod
    def table_error_handler(exc: HTTPError) -> None:
        handler: _ErrorHandler = {404: NoSuchTableError, 409: TableAlreadyExistsError}

        if "NoSuchNamespace" in exc.response.text:
            handler[404] = NoSuchNamespaceError

        _handle_non_200_response(exc, handler)

    @staticmethod
    def commit_error_handler(exc: HTTPError) -> None:
        handler: _ErrorHandler = {
            404: NoSuchTableError,
            409: CommitFailedException,
            500: CommitStateUnknownException,
            502: CommitStateUnknownException,
            503: CommitStateUnknownException,
            504: CommitStateUnknownException,
        }

        _handle_non_200_response(exc, handler)

    @staticmethod
    def view_error_handler(exc: HTTPError) -> None:
        handler: _ErrorHandler = {404: NoSuchViewError, 409: ViewAlreadyExistsError}

        if "NoSuchNamespace" in exc.response.text:
            handler[404] = NoSuchNamespaceError

        _handle_non_200_response(exc, handler)


def _handle_non_200_response(exc: HTTPError, handler: _ErrorHandler) -> None:
    exception: type[Exception]

    if exc.response is None:
        raise ValueError("Did not receive a response")

    code = exc.response.status_code

    default_handler: _ErrorHandler = {
        400: BadRequestError,
        401: UnauthorizedError,
        403: ForbiddenError,
        419: AuthorizationExpiredError,
        422: RESTError,
        501: NotImplementedError,
        503: ServiceUnavailableError,
    }

    # Merge handler passed with default handler map, if no match exception will be ServerError or RESTError
    exception = handler.get(code, default_handler.get(code, ServerError if 500 <= code < 600 else RESTError))

    try:
        if exception == OAuthError:
            # The OAuthErrorResponse has a different format
            error = OAuthErrorResponse.model_validate_json(exc.response.text)
            response = str(error.error)
            if description := error.error_description:
                response += f": {description}"
            if uri := error.error_uri:
                response += f" ({uri})"
        else:
            error = ErrorResponse.model_validate_json(exc.response.text).error
            response = f"{error.type}: {error.message}"
    except JSONDecodeError:
        # In the case we don't have a proper response
        response = f"RESTError {exc.response.status_code}: Could not decode json payload: {exc.response.text}"
    except ValidationError as e:
        # In the case we don't have a proper response
        errs = ", ".join(err["msg"] for err in e.errors())
        response = f"RESTError {exc.response.status_code}: Received unexpected JSON Payload: {exc.response.text}, errors: {errs}"

    raise exception(response) from exc
