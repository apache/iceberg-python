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
"""AES-GCM primitives and Iceberg AGS1 stream decryption."""

from __future__ import annotations

import os
import struct

from cryptography.hazmat.primitives.ciphers.aead import AESGCM

NONCE_LENGTH = 12
GCM_TAG_LENGTH = 16


def aes_gcm_encrypt(key: bytes, plaintext: bytes, aad: bytes | None = None) -> bytes:
    nonce = os.urandom(NONCE_LENGTH)
    return nonce + AESGCM(key).encrypt(nonce, plaintext, aad)


def aes_gcm_decrypt(key: bytes, ciphertext: bytes, aad: bytes | None = None) -> bytes:
    if len(ciphertext) < NONCE_LENGTH + GCM_TAG_LENGTH:
        raise ValueError(f"Ciphertext too short: {len(ciphertext)} bytes")
    return AESGCM(key).decrypt(ciphertext[:NONCE_LENGTH], ciphertext[NONCE_LENGTH:], aad)


GCM_STREAM_MAGIC = b"AGS1"
GCM_STREAM_HEADER_LENGTH = 8  # 4 magic + 4 little-endian block size


def stream_block_aad(aad_prefix: bytes, block_index: int) -> bytes:
    return aad_prefix + struct.pack("<I", block_index)


def decrypt_ags1_stream(key: bytes, encrypted_data: bytes, aad_prefix: bytes) -> bytes:
    """Decrypt an Iceberg AGS1 stream.

    Layout: "AGS1" (4) | plain_block_size LE (4) | one or more {nonce(12) | cipher | tag(16)} blocks.
    Each block's AAD is `aad_prefix || block_index_le32`.
    """
    if len(encrypted_data) < GCM_STREAM_HEADER_LENGTH:
        raise ValueError(f"AGS1 stream too short: {len(encrypted_data)} bytes")
    if encrypted_data[:4] != GCM_STREAM_MAGIC:
        raise ValueError(f"Invalid AGS1 magic: {encrypted_data[:4]!r}")

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
        block_cipher_size = min(cipher_block_size, len(stream_data) - offset)
        if block_cipher_size < NONCE_LENGTH + GCM_TAG_LENGTH:
            raise ValueError(f"Truncated AGS1 block at offset {offset}: {block_cipher_size} bytes")

        block = stream_data[offset : offset + block_cipher_size]
        result.extend(
            aesgcm.decrypt(block[:NONCE_LENGTH], block[NONCE_LENGTH:], stream_block_aad(aad_prefix, block_index))
        )
        offset += block_cipher_size
        block_index += 1

    return bytes(result)
