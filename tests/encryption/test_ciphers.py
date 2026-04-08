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
    GCM_TAG_LENGTH,
    NONCE_LENGTH,
    aes_gcm_decrypt,
    aes_gcm_encrypt,
    decrypt_ags1_stream,
    stream_block_aad,
)


class TestAesGcm:
    def test_roundtrip(self) -> None:
        key = os.urandom(16)
        plaintext = b"hello, encryption"
        ciphertext = aes_gcm_encrypt(key, plaintext)
        assert aes_gcm_decrypt(key, ciphertext) == plaintext

    def test_roundtrip_with_aad(self) -> None:
        key = os.urandom(16)
        plaintext = b"hello, encryption"
        aad = b"additional-data"
        ciphertext = aes_gcm_encrypt(key, plaintext, aad=aad)
        assert aes_gcm_decrypt(key, ciphertext, aad=aad) == plaintext

    def test_wrong_key_fails(self) -> None:
        from cryptography.exceptions import InvalidTag

        key = os.urandom(16)
        wrong_key = os.urandom(16)
        ciphertext = aes_gcm_encrypt(key, b"secret")
        with pytest.raises(InvalidTag):
            aes_gcm_decrypt(wrong_key, ciphertext)

    def test_wrong_aad_fails(self) -> None:
        from cryptography.exceptions import InvalidTag

        key = os.urandom(16)
        ciphertext = aes_gcm_encrypt(key, b"secret", aad=b"correct")
        with pytest.raises(InvalidTag):
            aes_gcm_decrypt(key, ciphertext, aad=b"wrong")

    def test_wire_format(self) -> None:
        """Verify the wire format: nonce(12) || ciphertext || tag(16)."""
        key = os.urandom(16)
        plaintext = b"test"
        ciphertext = aes_gcm_encrypt(key, plaintext)
        # Minimum size: nonce + tag + at least len(plaintext) of ciphertext
        assert len(ciphertext) == NONCE_LENGTH + len(plaintext) + GCM_TAG_LENGTH

    def test_ciphertext_too_short(self) -> None:
        key = os.urandom(16)
        with pytest.raises(ValueError, match="Ciphertext too short"):
            aes_gcm_decrypt(key, b"short")


class TestStreamBlockAad:
    def test_with_prefix(self) -> None:
        aad = stream_block_aad(b"prefix", 0)
        assert aad == b"prefix" + struct.pack("<I", 0)

    def test_with_prefix_nonzero_index(self) -> None:
        aad = stream_block_aad(b"prefix", 42)
        assert aad == b"prefix" + struct.pack("<I", 42)

    def test_without_prefix(self) -> None:
        aad = stream_block_aad(b"", 7)
        assert aad == struct.pack("<I", 7)


def _encrypt_ags1_stream(key: bytes, plaintext: bytes, aad_prefix: bytes, plain_block_size: int = 1024 * 1024) -> bytes:
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


class TestAgs1Stream:
    def test_roundtrip_single_block(self) -> None:
        key = os.urandom(16)
        plaintext = b"hello AGS1 stream"
        aad_prefix = b"test-aad"
        encrypted = _encrypt_ags1_stream(key, plaintext, aad_prefix)
        assert decrypt_ags1_stream(key, encrypted, aad_prefix) == plaintext

    def test_roundtrip_multi_block(self) -> None:
        """Test with a small block size to force multiple blocks."""
        key = os.urandom(16)
        plaintext = b"A" * 200
        aad_prefix = b"multi"
        # Use a 64-byte block size to get multiple blocks
        encrypted = _encrypt_ags1_stream(key, plaintext, aad_prefix, plain_block_size=64)
        assert decrypt_ags1_stream(key, encrypted, aad_prefix) == plaintext

    def test_roundtrip_empty_payload(self) -> None:
        key = os.urandom(16)
        header = GCM_STREAM_MAGIC + struct.pack("<I", 1024 * 1024)
        assert decrypt_ags1_stream(key, header, b"aad") == b""

    def test_invalid_magic(self) -> None:
        data = b"XXXX" + struct.pack("<I", 1024 * 1024)
        with pytest.raises(ValueError, match="Invalid AGS1 magic"):
            decrypt_ags1_stream(os.urandom(16), data, b"")

    def test_too_short(self) -> None:
        with pytest.raises(ValueError, match="AGS1 stream too short"):
            decrypt_ags1_stream(os.urandom(16), b"AGS1", b"")

    def test_custom_block_size(self) -> None:
        """Verify the block size from the header is respected, not hardcoded."""
        key = os.urandom(16)
        plaintext = b"B" * 300
        aad_prefix = b"custom"
        # Encrypt with a 100-byte block size
        encrypted = _encrypt_ags1_stream(key, plaintext, aad_prefix, plain_block_size=100)
        # Verify the header contains 100
        assert struct.unpack_from("<I", encrypted, 4)[0] == 100
        # Decrypt should work because it reads the block size from the header
        assert decrypt_ags1_stream(key, encrypted, aad_prefix) == plaintext

    def test_truncated_block(self) -> None:
        key = os.urandom(16)
        # Header + a few bytes that are too short for even nonce+tag
        data = GCM_STREAM_MAGIC + struct.pack("<I", 1024 * 1024) + b"short"
        with pytest.raises(ValueError, match="Truncated AGS1 block"):
            decrypt_ags1_stream(key, data, b"")
