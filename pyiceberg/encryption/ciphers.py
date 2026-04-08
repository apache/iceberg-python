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
"""AES-GCM encryption/decryption primitives and AGS1 stream decryption."""

from __future__ import annotations

import os
import struct

from cryptography.hazmat.primitives.ciphers.aead import AESGCM

NONCE_LENGTH = 12
GCM_TAG_LENGTH = 16


def aes_gcm_encrypt(key: bytes, plaintext: bytes, aad: bytes | None = None) -> bytes:
    """Encrypt using AES-GCM. Returns nonce || ciphertext || tag."""
    nonce = os.urandom(NONCE_LENGTH)
    aesgcm = AESGCM(key)
    ciphertext_with_tag = aesgcm.encrypt(nonce, plaintext, aad)
    return nonce + ciphertext_with_tag


def aes_gcm_decrypt(key: bytes, ciphertext: bytes, aad: bytes | None = None) -> bytes:
    """Decrypt AES-GCM data in format: nonce || ciphertext || tag."""
    if len(ciphertext) < NONCE_LENGTH + GCM_TAG_LENGTH:
        raise ValueError(f"Ciphertext too short: {len(ciphertext)} bytes")
    nonce = ciphertext[:NONCE_LENGTH]
    encrypted_data = ciphertext[NONCE_LENGTH:]
    aesgcm = AESGCM(key)
    return aesgcm.decrypt(nonce, encrypted_data, aad)


# AGS1 stream constants
GCM_STREAM_MAGIC = b"AGS1"
GCM_STREAM_HEADER_LENGTH = 8  # 4 magic + 4 block size


def stream_block_aad(aad_prefix: bytes, block_index: int) -> bytes:
    """Construct per-block AAD for AGS1 stream encryption.

    Format: aad_prefix || block_index (4 bytes, little-endian).
    """
    index_bytes = struct.pack("<I", block_index)
    if not aad_prefix:
        return index_bytes
    return aad_prefix + index_bytes


def decrypt_ags1_stream(key: bytes, encrypted_data: bytes, aad_prefix: bytes) -> bytes:
    """Decrypt an entire AGS1 stream and return the plaintext.

    AGS1 format:
      - Header: "AGS1" (4 bytes) + plain_block_size (4 bytes LE)
      - Blocks: each block is nonce(12) + ciphertext(up to 1MB) + tag(16)
      - Each block's AAD = aad_prefix + block_index (4 bytes LE)

    """
    if len(encrypted_data) < GCM_STREAM_HEADER_LENGTH:
        raise ValueError(f"AGS1 stream too short: {len(encrypted_data)} bytes")

    magic = encrypted_data[:4]
    if magic != GCM_STREAM_MAGIC:
        raise ValueError(f"Invalid AGS1 magic: {magic!r}, expected {GCM_STREAM_MAGIC!r}")

    plain_block_size = struct.unpack_from("<I", encrypted_data, 4)[0]
    cipher_block_size = plain_block_size + NONCE_LENGTH + GCM_TAG_LENGTH

    stream_data = encrypted_data[GCM_STREAM_HEADER_LENGTH:]
    if not stream_data:
        return b""

    aesgcm = AESGCM(key)
    result = bytearray()
    offset = 0
    block_index = 0

    while offset < len(stream_data):
        # Determine this block's cipher size
        remaining = len(stream_data) - offset
        if remaining >= cipher_block_size:
            block_cipher_size = cipher_block_size
        else:
            block_cipher_size = remaining

        if block_cipher_size < NONCE_LENGTH + GCM_TAG_LENGTH:
            raise ValueError(
                f"Truncated AGS1 block at offset {offset}: {block_cipher_size} bytes (minimum {NONCE_LENGTH + GCM_TAG_LENGTH})"
            )

        block_data = stream_data[offset : offset + block_cipher_size]
        nonce = block_data[:NONCE_LENGTH]
        ciphertext_with_tag = block_data[NONCE_LENGTH:]

        aad = stream_block_aad(aad_prefix, block_index)
        plaintext = aesgcm.decrypt(nonce, ciphertext_with_tag, aad)
        result.extend(plaintext)

        offset += block_cipher_size
        block_index += 1

    return bytes(result)
