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
import os
import struct

import pytest

from pyiceberg.encryption.ciphers import (
    GCM_STREAM_MAGIC,
    NONCE_LENGTH,
    aes_gcm_encrypt,
    stream_block_aad,
)
from pyiceberg.encryption.key_metadata import StandardKeyMetadata
from pyiceberg.encryption.kms import InMemoryKms
from pyiceberg.encryption.manager import EncryptedKey, EncryptionManager


def _make_ags1_stream(key: bytes, plaintext: bytes, aad_prefix: bytes, plain_block_size: int = 1024 * 1024) -> bytes:
    """Build an AGS1 encrypted stream for testing."""
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM

    aesgcm = AESGCM(key)
    header = GCM_STREAM_MAGIC + struct.pack("<I", plain_block_size)
    blocks = bytearray()
    offset = 0
    block_index = 0

    while offset < len(plaintext):
        block_plain = plaintext[offset : offset + plain_block_size]
        nonce = os.urandom(NONCE_LENGTH)
        aad = stream_block_aad(aad_prefix, block_index)
        ciphertext_with_tag = aesgcm.encrypt(nonce, block_plain, aad)
        blocks.extend(nonce + ciphertext_with_tag)
        offset += plain_block_size
        block_index += 1

    return header + bytes(blocks)


def _build_test_encryption_manager() -> tuple[EncryptionManager, bytes, bytes, str]:
    """Build an EncryptionManager with test keys, mimicking the REST catalog flow.

    Returns (manager, dek, aad_prefix, manifest_list_key_id).
    """
    master_key = b"0123456789012345"  # 16 bytes, standard Iceberg test key "keyA"
    kms = InMemoryKms(master_keys={"keyA": master_key})

    # Create a KEK (simulating what the REST catalog would provide)
    kek_bytes = os.urandom(16)
    kek_wrapped = kms.wrap_key(kek_bytes, "keyA")
    kek_timestamp = "1234567890"

    # Create a DEK for the manifest list
    dek = os.urandom(16)
    aad_prefix = os.urandom(8)
    key_metadata = StandardKeyMetadata(encryption_key=dek, aad_prefix=aad_prefix)
    key_metadata_bytes = key_metadata.serialize()

    # Wrap the DEK key metadata with the KEK (using timestamp as AAD)
    wrapped_dek = aes_gcm_encrypt(kek_bytes, key_metadata_bytes, aad=kek_timestamp.encode("utf-8"))

    encryption_keys = {
        "kek-1": EncryptedKey(
            key_id="kek-1",
            encrypted_key_metadata=kek_wrapped,
            encrypted_by_id="keyA",
            properties={"KEY_TIMESTAMP": kek_timestamp},
        ),
        "mlk-1": EncryptedKey(
            key_id="mlk-1",
            encrypted_key_metadata=wrapped_dek,
            encrypted_by_id="kek-1",
        ),
    }

    manager = EncryptionManager(kms_client=kms, encryption_keys=encryption_keys)
    return manager, dek, aad_prefix, "mlk-1"


class TestEncryptionManager:
    def test_decrypt_manifest_list(self) -> None:
        manager, dek, aad_prefix, mlk_id = _build_test_encryption_manager()
        plaintext = b"manifest list content here"
        encrypted_stream = _make_ags1_stream(dek, plaintext, aad_prefix)
        result = manager.decrypt_manifest_list(encrypted_stream, mlk_id)
        assert result == plaintext

    def test_decrypt_manifest(self) -> None:
        dek = os.urandom(16)
        aad_prefix = os.urandom(8)
        plaintext = b"manifest content here"
        encrypted_stream = _make_ags1_stream(dek, plaintext, aad_prefix)
        key_metadata = StandardKeyMetadata(encryption_key=dek, aad_prefix=aad_prefix)

        # The manager only needs a KMS for KEK unwrapping; manifest decryption
        # uses the plaintext key metadata directly (from inside the encrypted manifest list)
        kms = InMemoryKms()
        manager = EncryptionManager(kms_client=kms)
        result = manager.decrypt_manifest(encrypted_stream, key_metadata.serialize())
        assert result == plaintext

    def test_kek_caching(self) -> None:
        """KEK should be unwrapped once and cached."""
        manager, dek, aad_prefix, mlk_id = _build_test_encryption_manager()
        plaintext = b"test"
        encrypted = _make_ags1_stream(dek, plaintext, aad_prefix)

        # Decrypt twice
        manager.decrypt_manifest_list(encrypted, mlk_id)
        manager.decrypt_manifest_list(encrypted, mlk_id)

        # KEK should be cached
        assert "kek-1" in manager._kek_cache

    def test_missing_snapshot_key(self) -> None:
        kms = InMemoryKms()
        manager = EncryptionManager(kms_client=kms)
        with pytest.raises(ValueError, match="Snapshot key not found"):
            manager.decrypt_manifest_list(b"data", "nonexistent-key")

    def test_missing_kek(self) -> None:
        kms = InMemoryKms()
        encryption_keys = {
            "mlk-1": EncryptedKey(
                key_id="mlk-1",
                encrypted_key_metadata=b"wrapped",
                encrypted_by_id="missing-kek",
            ),
        }
        manager = EncryptionManager(kms_client=kms, encryption_keys=encryption_keys)
        with pytest.raises(ValueError, match="KEK not found"):
            manager.decrypt_manifest_list(b"data", "mlk-1")

    def test_kek_without_encrypted_by_id(self) -> None:
        kms = InMemoryKms(master_keys={"keyA": os.urandom(16)})
        encryption_keys = {
            "kek-1": EncryptedKey(key_id="kek-1", encrypted_key_metadata=b"data"),
            "mlk-1": EncryptedKey(
                key_id="mlk-1",
                encrypted_key_metadata=b"wrapped",
                encrypted_by_id="kek-1",
            ),
        }
        manager = EncryptionManager(kms_client=kms, encryption_keys=encryption_keys)
        with pytest.raises(ValueError, match="has no encrypted_by_id"):
            manager.decrypt_manifest_list(b"data", "mlk-1")
