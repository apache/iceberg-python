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
"""Envelope key management for table encryption.

A master key held by the key management service wraps key encryption keys (KEKs), which are
stored in table metadata. A KEK wraps the key metadata of each snapshot's manifest list.
"""

from __future__ import annotations

import time
import uuid
from collections.abc import Iterable

from pyiceberg.encryption.ciphers import AesGcmCipher, AesKeySize, SecureKey
from pyiceberg.encryption.key_metadata import StandardKeyMetadata
from pyiceberg.encryption.kms import KeyManagementClient
from pyiceberg.table import TableProperties
from pyiceberg.table.metadata import EncryptedKey, TableMetadata, TableMetadataV3
from pyiceberg.utils.properties import property_as_int

KEK_CREATED_AT_PROPERTY = "KEY_TIMESTAMP"

KEK_LIFESPAN_MS = 730 * 24 * 60 * 60 * 1000

KEK_CACHE_TTL_SECONDS = 3600


class EncryptionManager:
    """Generates file keys and wraps manifest list key metadata for an encrypted table."""

    def __init__(
        self,
        kms_client: KeyManagementClient,
        table_key_id: str,
        encryption_keys: Iterable[EncryptedKey] = (),
        key_size: AesKeySize = AesKeySize.BITS_128,
    ) -> None:
        self._kms_client = kms_client
        self._table_key_id = table_key_id
        self._key_size = key_size
        self._encryption_keys = {key.key_id: key for key in encryption_keys}
        self._kek_cache: dict[str, tuple[SecureKey, float]] = {}

    @classmethod
    def from_table_metadata(cls, kms_client: KeyManagementClient | None, metadata: TableMetadata) -> EncryptionManager | None:
        """Create a manager for the table, or return None if the table is not encrypted.

        Args:
            kms_client (KeyManagementClient | None): Wraps and unwraps KEKs with the table's master key.
            metadata (TableMetadata): The table's metadata, holding `encryption.key-id` and its encryption keys.
        """
        if (table_key_id := metadata.properties.get(TableProperties.ENCRYPTION_KEY_ID)) is None:
            return None

        if not isinstance(metadata, TableMetadataV3):
            raise ValueError(f"Invalid properties for v{metadata.format_version}: {TableProperties.ENCRYPTION_KEY_ID}")

        if kms_client is None:
            raise ValueError(f"Table has {TableProperties.ENCRYPTION_KEY_ID} set but no key management client was provided")

        key_length = property_as_int(metadata.properties, TableProperties.ENCRYPTION_DATA_KEY_LENGTH)
        key_size = AesKeySize.from_key_length(
            TableProperties.ENCRYPTION_DATA_KEY_LENGTH_DEFAULT if key_length is None else key_length
        )

        return cls(kms_client, table_key_id, metadata.encryption_keys, key_size)

    @property
    def encryption_keys(self) -> list[EncryptedKey]:
        """The table's encryption keys, including KEKs and manifest list keys this manager created."""
        return list(self._encryption_keys.values())

    def generate_key_metadata(self) -> StandardKeyMetadata:
        """Generate key metadata for a new file, with a fresh key and AAD prefix."""
        return StandardKeyMetadata.generate(self._key_size)

    def encrypt_manifest_list_key_metadata(self, key_metadata: StandardKeyMetadata) -> str:
        """Wrap a manifest list's key metadata with the active KEK, creating a KEK if none is active.

        Returns the id of the new encryption key, to record as the snapshot's `key-id`.

        Args:
            key_metadata (StandardKeyMetadata): The manifest list's key metadata, including its `file_length`.
        """
        kek = self._find_active_kek() or self._create_kek()
        wrapped = AesGcmCipher(self._unwrap_kek(kek)).encrypt(key_metadata.to_bytes(), _kek_timestamp_aad(kek))

        key = EncryptedKey(key_id=str(uuid.uuid4()), encrypted_key_metadata=wrapped, encrypted_by_id=kek.key_id)
        self._encryption_keys[key.key_id] = key
        return key.key_id

    def decrypt_manifest_list_key_metadata(self, key_id: str) -> StandardKeyMetadata:
        """Unwrap the manifest list key metadata stored under `key_id`.

        Args:
            key_id (str): The snapshot's `key-id`.
        """
        if (key := self._encryption_keys.get(key_id)) is None:
            raise ValueError(f"Encryption key not found: {key_id}")

        if key.encrypted_by_id is None:
            raise ValueError(f"Encryption key {key_id} has no encrypted-by-id")

        if (kek := self._encryption_keys.get(key.encrypted_by_id)) is None:
            raise ValueError(f"KEK not found in encryption keys: {key.encrypted_by_id}")

        aad = _kek_timestamp_aad(kek)
        cipher = AesGcmCipher(self._unwrap_kek(kek))
        try:
            plaintext = cipher.decrypt(key.encrypted_key_metadata, aad)
        except ValueError as e:
            raise ValueError(f"Failed to unwrap key metadata with KEK {kek.key_id}") from e

        return StandardKeyMetadata.from_bytes(plaintext)

    def _create_kek(self) -> EncryptedKey:
        if self._kms_client.supports_key_generation():
            generated = self._kms_client.generate_key(self._table_key_id)
            plaintext, wrapped = SecureKey(generated.key), generated.wrapped_key
        else:
            plaintext = SecureKey.generate(self._key_size)
            wrapped = self._kms_client.wrap_key(plaintext.key, self._table_key_id)

        kek = EncryptedKey(
            key_id=str(uuid.uuid4()),
            encrypted_key_metadata=wrapped,
            encrypted_by_id=self._table_key_id,
            properties={KEK_CREATED_AT_PROPERTY: str(_now_ms())},
        )
        self._kek_cache[kek.key_id] = (plaintext, time.monotonic())
        self._encryption_keys[kek.key_id] = kek
        return kek

    def _find_active_kek(self) -> EncryptedKey | None:
        keks = [
            key
            for key in self._encryption_keys.values()
            if key.encrypted_by_id == self._table_key_id and not _is_kek_expired(key)
        ]
        return max(keks, key=lambda kek: _kek_created_at(kek) or 0, default=None)

    def _unwrap_kek(self, kek: EncryptedKey) -> SecureKey:
        if (cached := self._kek_cache.get(kek.key_id)) is not None and time.monotonic() - cached[1] < KEK_CACHE_TTL_SECONDS:
            return cached[0]

        if kek.encrypted_by_id is None:
            raise ValueError(f"KEK {kek.key_id} has no encrypted-by-id")

        plaintext = SecureKey(self._kms_client.unwrap_key(kek.encrypted_key_metadata, kek.encrypted_by_id))
        self._kek_cache[kek.key_id] = (plaintext, time.monotonic())
        return plaintext


def _now_ms() -> int:
    return time.time_ns() // 1_000_000


def _kek_created_at(kek: EncryptedKey) -> int | None:
    try:
        return int(kek.properties[KEK_CREATED_AT_PROPERTY])
    except (KeyError, ValueError):
        return None


def _is_kek_expired(kek: EncryptedKey) -> bool:
    created_at = _kek_created_at(kek)
    return created_at is None or _now_ms() - created_at >= KEK_LIFESPAN_MS


def _kek_timestamp_aad(kek: EncryptedKey) -> bytes:
    """Return the KEK's creation timestamp, which authenticates everything it wraps so the timestamp cannot be altered."""
    if (timestamp := kek.properties.get(KEK_CREATED_AT_PROPERTY)) is None:
        raise ValueError(f"KEK {kek.key_id} is missing required {KEK_CREATED_AT_PROPERTY} property")

    return timestamp.encode("utf-8")
