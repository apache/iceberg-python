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
#
from datetime import datetime

from pydantic import Field
from requests import HTTPError, Session

from pyiceberg.catalog import URI
from pyiceberg.catalog.rest.response import _handle_non_200_response
from pyiceberg.catalog.rest.scan_planning import StorageCredential
from pyiceberg.exceptions import ValidationException
from pyiceberg.io import (
    AWS_ACCESS_KEY_ID,
    AWS_SECRET_ACCESS_KEY,
    AWS_SESSION_TOKEN,
    S3_ACCESS_KEY_ID,
    S3_SECRET_ACCESS_KEY,
    S3_SESSION_TOKEN,
    S3_SESSION_TOKEN_EXPIRES_AT_MS,
)
from pyiceberg.typedef import IcebergBaseModel, Properties
from pyiceberg.utils.properties import get_first_property_value

CREDENTIALS_ENDPOINT = "client.refresh-credentials-endpoint"
REFRESH_CREDENTIALS_ENABLED = "client.refresh-credentials-enabled"


class LoadCredentialsResponse(IcebergBaseModel):
    credentials: list[StorageCredential] = Field(alias="storage-credentials")


class VendedCredentialsProvider:
    _session: Session
    _properties: Properties

    def __init__(self, session: Session, properties: Properties):
        self._session = session
        self._properties = properties

    def _extract_s3_credentials_from(self, props: Properties) -> tuple[str | None, str | None, str | None, str | None]:
        """Extract only S3 credentials from properties."""
        access_key = get_first_property_value(props, S3_ACCESS_KEY_ID, AWS_ACCESS_KEY_ID)
        secret_key = get_first_property_value(props, S3_SECRET_ACCESS_KEY, AWS_SECRET_ACCESS_KEY)
        session_token = get_first_property_value(props, S3_SESSION_TOKEN, AWS_SESSION_TOKEN)
        expiry = get_first_property_value(props, S3_SESSION_TOKEN_EXPIRES_AT_MS)

        return access_key, secret_key, session_token, expiry

    def _to_credentials_property_map(
        self, access_key: str | None, secret_key: str | None, session_token: str | None, expiry: str | None
    ) -> Properties:
        return {
            S3_ACCESS_KEY_ID: access_key,
            S3_SECRET_ACCESS_KEY: secret_key,
            S3_SESSION_TOKEN: session_token,
            S3_SESSION_TOKEN_EXPIRES_AT_MS: expiry,
        }

    def needs_refresh(self) -> bool:
        """Return True if the S3 session token expires within 300s."""
        expiry = get_first_property_value(self._properties, S3_SESSION_TOKEN_EXPIRES_AT_MS)
        if expiry is None:
            return False
        expires_at = datetime.fromtimestamp(int(expiry) / 1000)
        seconds_remaining = (expires_at - datetime.now()).total_seconds()
        return seconds_remaining < 300

    def _build_refresh_endpoint(self) -> str:
        """Build credential refresh endpoint from properties."""
        catalog_uri = get_first_property_value(self._properties, URI)
        credentials_path = get_first_property_value(self._properties, CREDENTIALS_ENDPOINT)

        if catalog_uri is None:
            raise ValidationException("Invalid catalog endpoint: None")

        if credentials_path is None:
            raise ValidationException("Invalid credentials endpoint: None")

        return str(catalog_uri).rstrip("/") + "/" + str(credentials_path).lstrip("/")

    def _get_new_credentials(self) -> LoadCredentialsResponse | None:
        try:
            http_response = self._session.get(self._build_refresh_endpoint())
            http_response.raise_for_status()
            return LoadCredentialsResponse.model_validate_json(http_response.text)
        except HTTPError as exc:
            _handle_non_200_response(exc, {})
        return None

    def get_credentials(self) -> Properties:
        """Retrieve current S3 credentials, refreshing from the endpoint if near expiry."""
        access_key, secret_key, session_token, expiry = self._extract_s3_credentials_from(self._properties)

        if not self.needs_refresh():
            return self._to_credentials_property_map(access_key, secret_key, session_token, expiry)

        creds = self._get_new_credentials()

        if creds is None:
            raise ValidationException("Load credential response is None")
        if not creds.credentials:
            raise ValueError("Invalid S3 Credentials: empty")
        if len(creds.credentials) > 1:
            raise ValueError("Invalid S3 Credentials: only one S3 credential should exists")

        updated_creds = self._extract_s3_credentials_from(creds.credentials[0].config)
        updated_map = self._to_credentials_property_map(*updated_creds)

        # Update internal properties with new credentials
        self._properties = {**self._properties, **updated_map}

        return updated_map
