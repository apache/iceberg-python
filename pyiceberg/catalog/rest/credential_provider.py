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

import logging
import threading
from collections.abc import Callable
from dataclasses import dataclass, field, replace
from time import time
from typing import TYPE_CHECKING
from urllib.parse import urlparse

from pyiceberg.catalog.rest.scan_planning import StorageCredential
from pyiceberg.io import (
    GCS_TOKEN,
    GCS_TOKEN_EXPIRES_AT_MS,
    S3_ACCESS_KEY_ID,
    S3_SECRET_ACCESS_KEY,
    S3_SESSION_TOKEN,
    S3_SESSION_TOKEN_EXPIRES_AT_MS,
)
from pyiceberg.typedef import Properties

if TYPE_CHECKING:
    from pyiceberg.catalog.rest import LoadCredentialsResponse

logger = logging.getLogger(__name__)

REFRESH_CREDENTIALS_ENABLED = "client.refresh-credentials-enabled"
_REFRESH_MARGIN_MS = 300_000
_REFRESH_RETRY_DELAY_MS = 30_000


@dataclass(frozen=True)
class _CredentialPolicy:
    required_properties: tuple[str, ...]
    expiration_properties: tuple[str, ...]

    def expiration_ms(self, config: Properties) -> int | None:
        for key in self.expiration_properties:
            if key in config:
                try:
                    return int(config[key])
                except (TypeError, ValueError, OverflowError):
                    raise ValueError(f"Invalid expiration property: {key}") from None
        return None

    def normalize(self, config: Properties) -> Properties:
        properties = dict(config)
        if (expiration := self.expiration_ms(properties)) is not None:
            for key in self.required_properties:
                if not properties.get(key):
                    raise ValueError(f"Missing vended credential property: {key}")
            for key in self.expiration_properties:
                properties.pop(key, None)
            properties[self.expiration_properties[0]] = str(expiration)
        return properties


_S3_POLICY = _CredentialPolicy((S3_ACCESS_KEY_ID, S3_SECRET_ACCESS_KEY, S3_SESSION_TOKEN), (S3_SESSION_TOKEN_EXPIRES_AT_MS,))
_GCS_POLICY = _CredentialPolicy((GCS_TOKEN,), (GCS_TOKEN_EXPIRES_AT_MS, "gcs.oauth2.token_expires_at"))
_POLICIES_BY_SCHEME = {
    "s3": _S3_POLICY,
    "s3a": _S3_POLICY,
    "s3n": _S3_POLICY,
    "gs": _GCS_POLICY,
    "gcs": _GCS_POLICY,
}


def is_s3_credential_expired(config: Properties, threshold_seconds: int = 300) -> bool:
    """Return whether an S3 credential expires within the given threshold."""
    expiration = _S3_POLICY.expiration_ms(config)
    return expiration is not None and expiration - time() * 1000 <= threshold_seconds * 1000


def resolve_storage_credentials(storage_credentials: list[StorageCredential], location: str | None) -> Properties:
    """Return credential properties for the longest matching storage prefix."""
    if location is None:
        return {}
    match = max(
        (cred for cred in storage_credentials if location.startswith(cred.prefix)), key=lambda c: len(c.prefix), default=None
    )
    if match is None:
        return {}
    return dict(match.config)


def _credentials_from_config(config: Properties, location: str) -> list[StorageCredential]:
    scheme = urlparse(location).scheme
    policy = _POLICIES_BY_SCHEME.get(scheme)
    if policy is None or policy.expiration_ms(config) is None:
        return []
    keys = (*policy.required_properties, *policy.expiration_properties)
    properties = {key: config[key] for key in keys if key in config}
    return [StorageCredential(prefix=f"{scheme}://", config=properties)]


@dataclass(frozen=True)
class _CredentialState:
    prefix: str
    config: Properties = field(repr=False)
    expiration_ms: int | None
    refresh_at_ms: int | None


def _credential_states(credentials: list[StorageCredential], received_at_ms: int) -> tuple[_CredentialState, ...]:
    states = []
    for credential in credentials:
        policy = _POLICIES_BY_SCHEME.get(urlparse(credential.prefix).scheme)
        config = policy.normalize(credential.config) if policy is not None else dict(credential.config)
        expiration = policy.expiration_ms(config) if policy is not None else None
        refresh_at = None
        if expiration is not None:
            refresh_at = expiration - min(_REFRESH_MARGIN_MS, max(0, expiration - received_at_ms) // 2)
        states.append(_CredentialState(credential.prefix, config, expiration, refresh_at))
    return tuple(states)


def _matching_state(states: tuple[_CredentialState, ...], location: str) -> _CredentialState | None:
    return max((state for state in states if location.startswith(state.prefix)), key=lambda s: len(s.prefix), default=None)


class CredentialsProvider:
    """Resolve and refresh prefix-scoped storage credentials for a REST table."""

    def __init__(
        self,
        storage_credentials: list[StorageCredential],
        refresh_fn: Callable[[], LoadCredentialsResponse],
    ) -> None:
        self._states = _credential_states(storage_credentials, int(time() * 1000))
        self._refresh_fn = refresh_fn
        self._lock = threading.Lock()

    def __getstate__(self) -> dict[str, object]:
        """Serialize absolute refresh deadlines without the process-local lock."""
        return {key: value for key, value in self.__dict__.items() if key != "_lock"}

    def __setstate__(self, state: dict[str, object]) -> None:
        """Restore credential state and create a new lock."""
        self.__dict__.update(state)
        self._lock = threading.Lock()

    def properties_for(self, location: str) -> Properties:
        """Return the current properties for a location, refreshing when due."""
        match = _matching_state(self._states, location)
        if match is None:
            return {}
        if match.refresh_at_ms is not None and time() * 1000 >= match.refresh_at_ms:
            with self._lock:
                match = _matching_state(self._states, location)
                if match is None:
                    raise ValueError("No vended credentials for the requested location after refresh")
                if match.refresh_at_ms is not None and time() * 1000 >= match.refresh_at_ms:
                    try:
                        response = self._refresh_fn()
                        now_ms = int(time() * 1000)
                        states = _credential_states(response.storage_credentials, now_ms)
                        refreshed = _matching_state(states, location)
                        if refreshed is None:
                            raise ValueError("Credential refresh returned no matching storage credentials")
                        if refreshed.expiration_ms is None:
                            raise ValueError("Refreshed storage credentials have no expiration")
                        if refreshed.expiration_ms <= now_ms:
                            raise ValueError("Refreshed storage credentials have already expired")
                    except Exception as exc:
                        now_ms = int(time() * 1000)
                        if match.expiration_ms is None or now_ms >= match.expiration_ms:
                            raise
                        retry_at_ms = min(now_ms + _REFRESH_RETRY_DELAY_MS, match.expiration_ms)
                        self._states = tuple(
                            replace(state, refresh_at_ms=retry_at_ms) if state is match else state for state in self._states
                        )
                        logger.warning(
                            "Credential refresh failed (%s); reusing unexpired storage credentials", type(exc).__name__
                        )
                    else:
                        self._states = states
                        match = refreshed
        return dict(match.config)
