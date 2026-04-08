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
"""Encryption manager implementing two-layer envelope key management.

Key hierarchy:
  - Master Key (in KMS) wraps KEK
  - KEK wraps DEK (using local AES-GCM)
  - DEK encrypts data (manifest lists, manifests, data files)

The KEK timestamp is used as AAD when wrapping/unwrapping DEKs
to prevent timestamp tampering attacks.
"""

from __future__ import annotations

import logging

from pyiceberg.encryption.ciphers import aes_gcm_decrypt, decrypt_ags1_stream
from pyiceberg.encryption.key_metadata import StandardKeyMetadata
from pyiceberg.encryption.kms import KeyManagementClient

logger = logging.getLogger(__name__)

KEK_CREATED_AT_PROPERTY = "KEY_TIMESTAMP"


class EncryptedKey:
    """Represents an encrypted key entry from table metadata."""

    def __init__(
        self,
        key_id: str,
        encrypted_key_metadata: bytes,
        encrypted_by_id: str | None = None,
        properties: dict[str, str] | None = None,
    ) -> None:
        self.key_id = key_id
        self.encrypted_key_metadata = encrypted_key_metadata
        self.encrypted_by_id = encrypted_by_id
        self.properties = properties or {}

    def __repr__(self) -> str:
        """Return a string representation of the EncryptedKey."""
        return (
            f"EncryptedKey(key_id={self.key_id!r}, "
            f"encrypted_by_id={self.encrypted_by_id!r}, "
            f"metadata_len={len(self.encrypted_key_metadata)})"
        )


class EncryptionManager:
    """Manages encryption/decryption for an Iceberg table.

    Orchestrates the two-layer envelope key management:
    1. Unwrap KEK via KMS using master key
    2. Use KEK to decrypt manifest list/manifest key metadata (with timestamp AAD)
    3. Parse StandardKeyMetadata to get DEK + AAD prefix
    4. Decrypt AGS1 streams or provide FileDecryptionProperties for Parquet
    """

    def __init__(
        self,
        kms_client: KeyManagementClient,
        encryption_keys: dict[str, EncryptedKey] | None = None,
    ) -> None:
        self._kms = kms_client
        self._encryption_keys = encryption_keys or {}
        self._kek_cache: dict[str, bytes] = {}

    def _unwrap_kek(self, kek: EncryptedKey) -> bytes:
        """Unwrap a KEK using the KMS, with caching."""
        if kek.key_id in self._kek_cache:
            return self._kek_cache[kek.key_id]

        if not kek.encrypted_by_id:
            raise ValueError(f"KEK '{kek.key_id}' has no encrypted_by_id")

        plaintext = self._kms.unwrap_key(kek.encrypted_key_metadata, kek.encrypted_by_id)
        self._kek_cache[kek.key_id] = plaintext
        return plaintext

    def _unwrap_dek(self, wrapped_dek: bytes, kek_key_id: str) -> bytes:
        """Unwrap a DEK using the specified KEK.

        Uses the KEK timestamp as AAD to prevent timestamp tampering.
        """
        kek = self._encryption_keys.get(kek_key_id)
        if kek is None:
            raise ValueError(f"KEK not found in encryption keys: {kek_key_id}")

        kek_bytes = self._unwrap_kek(kek)

        # Use KEK timestamp as AAD to prevent tampering
        aad = kek.properties.get(KEK_CREATED_AT_PROPERTY)
        aad_bytes = aad.encode("utf-8") if aad else None

        return aes_gcm_decrypt(kek_bytes, wrapped_dek, aad=aad_bytes)

    def unwrap_key_metadata(self, encrypted_key: EncryptedKey) -> bytes:
        """Unwrap key metadata that was KEK-wrapped.

        Given an EncryptedKey entry (e.g., from a snapshot's key-id mapping),
        unwrap it using the KEK identified by encrypted_by_id.
        """
        if not encrypted_key.encrypted_by_id:
            raise ValueError(f"EncryptedKey '{encrypted_key.key_id}' has no encrypted_by_id")

        return self._unwrap_dek(
            encrypted_key.encrypted_key_metadata,
            encrypted_key.encrypted_by_id,
        )

    def decrypt_manifest_list(self, encrypted_data: bytes, snapshot_key_id: str) -> bytes:
        """Decrypt an AGS1-encrypted manifest list.

        1. Look up the EncryptedKey for the snapshot's key_id
        2. Unwrap the key metadata using the KEK
        3. Parse StandardKeyMetadata to get DEK + AAD prefix
        4. Decrypt the AGS1 stream
        """
        encrypted_key = self._encryption_keys.get(snapshot_key_id)
        if encrypted_key is None:
            raise ValueError(f"Snapshot key not found in encryption keys: {snapshot_key_id}")

        # Unwrap the key metadata
        key_metadata_bytes = self.unwrap_key_metadata(encrypted_key)
        key_metadata = StandardKeyMetadata.deserialize(key_metadata_bytes)

        return decrypt_ags1_stream(
            key=key_metadata.encryption_key,
            encrypted_data=encrypted_data,
            aad_prefix=key_metadata.aad_prefix,
        )

    def decrypt_manifest(self, encrypted_data: bytes, key_metadata_bytes: bytes) -> bytes:
        """Decrypt an AGS1-encrypted manifest file.

        The key_metadata_bytes are from ManifestFile.key_metadata -- these contain
        the plaintext DEK and AAD prefix (NOT wrapped by KEK, since they're already
        stored inside the encrypted manifest list).
        """
        key_metadata = StandardKeyMetadata.deserialize(key_metadata_bytes)

        return decrypt_ags1_stream(
            key=key_metadata.encryption_key,
            encrypted_data=encrypted_data,
            aad_prefix=key_metadata.aad_prefix,
        )
