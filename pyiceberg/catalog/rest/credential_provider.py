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
from __future__ import annotations

import threading
from collections.abc import Callable
from datetime import datetime
from typing import TYPE_CHECKING
from urllib.parse import urlparse

from pyiceberg.catalog.rest.scan_planning import StorageCredential
from pyiceberg.io import S3_SESSION_TOKEN_EXPIRES_AT_MS
from pyiceberg.typedef import Properties
from pyiceberg.utils.properties import get_first_property_value

if TYPE_CHECKING:
    from pyiceberg.catalog.rest import LoadCredentialsResponse

REFRESH_CREDENTIALS_ENABLED = "client.refresh-credentials-enabled"


def is_s3_credential_expired(config: Properties, threshold_seconds: int = 300) -> bool:
    """Return True if the S3 session token expires within threshold_seconds (5 mins)."""
    if expiry := get_first_property_value(config, S3_SESSION_TOKEN_EXPIRES_AT_MS):
        expires_at = datetime.fromtimestamp(int(expiry) / 1000)
        seconds_remaining = (expires_at - datetime.now()).total_seconds()
        return seconds_remaining < threshold_seconds
    return False


# Per-scheme hooks for detecting whether a resolved credential needs to be refreshed.
# Other schemes (e.g. gs, abfss) can register here later.
NEEDS_REFRESH_BY_SCHEME: dict[str, Callable[[Properties], bool]] = {
    "s3": is_s3_credential_expired,
    "s3a": is_s3_credential_expired,
    "s3n": is_s3_credential_expired,
}


def resolve_storage_credentials(storage_credentials: list[StorageCredential], location: str | None) -> Properties:
    """Resolve the best-matching storage credential by longest prefix match.

    Mirrors the Java implementation in S3FileIO.clientForStoragePath() which iterates
    over storage credential prefixes and selects the one with the longest match.

    See: https://github.com/apache/iceberg/blob/main/aws/src/main/java/org/apache/iceberg/aws/s3/S3FileIO.java
    """
    if not storage_credentials or not location:
        return {}

    best_match: StorageCredential | None = None
    for cred in storage_credentials:
        if location.startswith(cred.prefix):
            if best_match is None or len(cred.prefix) > len(best_match.prefix):
                best_match = cred

    return best_match.config if best_match else {}


class CredentialsProvider:
    """Vended-credential refresh and location-based lookup for a REST catalog table."""

    _storage_credentials: list[StorageCredential]
    _refresh_fn: Callable[[], LoadCredentialsResponse]
    _needs_refresh_by_scheme: dict[str, Callable[[Properties], bool]]
    _lock: threading.Lock

    def __init__(
        self,
        storage_credentials: list[StorageCredential],
        refresh_fn: Callable[[], LoadCredentialsResponse],
        needs_refresh_by_scheme: dict[str, Callable[[Properties], bool]] | None = None,
    ):
        self._storage_credentials = storage_credentials
        self._refresh_fn = refresh_fn
        self._needs_refresh_by_scheme = (
            needs_refresh_by_scheme if needs_refresh_by_scheme is not None else NEEDS_REFRESH_BY_SCHEME
        )
        self._lock = threading.Lock()

    def _can_refresh(self, location: str) -> bool:
        scheme = urlparse(location).scheme
        refresh_by_scheme = self._needs_refresh_by_scheme.get(scheme)
        config = resolve_storage_credentials(self._storage_credentials, location)
        return config != {} and refresh_by_scheme is not None and refresh_by_scheme(config)

    def properties_for(self, location: str) -> Properties:
        """Return the credential properties that apply to the given location, refreshing if needed."""
        config = resolve_storage_credentials(self._storage_credentials, location)

        if self._can_refresh(location):
            with self._lock:
                if self._can_refresh(location):
                    response = self._refresh_fn()
                    self._storage_credentials = response.storage_credentials
                config = resolve_storage_credentials(self._storage_credentials, location)

        return config
