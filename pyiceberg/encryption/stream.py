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
"""AES GCM Stream (AGS1) encryption, used to encrypt manifests and manifest lists.

An AGS1 stream is an 8 byte header followed by a sequence of AES-GCM blocks::

    "AGS1" || plain_block_size (4 bytes, little endian)
    nonce || ciphertext || tag        (block 0, up to plain_block_size of plaintext)
    nonce || ciphertext || tag        (block 1..n, the last of which may be shorter)

Each block authenticates `aad_prefix || block_index` as additional data, so blocks cannot
be reordered or moved between files.

The spec gives the last block a non-zero length, which makes a bare header its encoding of an
empty file. Java, iceberg-rust and PyIceberg all require at least one block instead, so an
empty file is a header followed by a single empty block. apache/iceberg#18219 tracks which of
the two forms writers should produce.

`_encrypt_ags1` and `_decrypt_ags1` work on whole files, matching how PyIceberg reads and writes
Avro, so their implementation can move to pyiceberg-core without changing callers. They stay
private until the manifest reader and writer use them.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from pyiceberg.encryption.ciphers import AesGcmCipher, SecureKey

if TYPE_CHECKING:
    from pyiceberg.encryption.key_metadata import StandardKeyMetadata

_GCM_STREAM_MAGIC = b"AGS1"
_PLAIN_BLOCK_SIZE = 1024 * 1024
_GCM_STREAM_HEADER = _GCM_STREAM_MAGIC + _PLAIN_BLOCK_SIZE.to_bytes(4, "little")
_GCM_STREAM_HEADER_LENGTH = len(_GCM_STREAM_HEADER)
_BLOCK_OVERHEAD = AesGcmCipher.NONCE_LENGTH + AesGcmCipher.TAG_LENGTH
_CIPHER_BLOCK_SIZE = _PLAIN_BLOCK_SIZE + _BLOCK_OVERHEAD
_MIN_STREAM_LENGTH = _GCM_STREAM_HEADER_LENGTH + _BLOCK_OVERHEAD


def _encrypt_ags1(plaintext: bytes, key_metadata: StandardKeyMetadata) -> bytes:
    """Encrypt `plaintext` into an AGS1 stream.

    An empty plaintext is written as the header plus one empty block, as Java and iceberg-rust do.

    Args:
        plaintext (bytes): The file contents to encrypt.
        key_metadata (StandardKeyMetadata): The file's key and AAD prefix. Its `file_length` is ignored, and
            callers must record the length of the returned stream as the file's `file_length`.
    """
    cipher = AesGcmCipher(SecureKey(key_metadata.encryption_key))
    starts = range(0, len(plaintext), _PLAIN_BLOCK_SIZE) or range(1)
    return _GCM_STREAM_HEADER + b"".join(
        cipher.encrypt(plaintext[start : start + _PLAIN_BLOCK_SIZE], _stream_block_aad(key_metadata.aad_prefix, index))
        for index, start in enumerate(starts)
    )


def _decrypt_ags1(data: bytes, key_metadata: StandardKeyMetadata) -> bytes:
    """Decrypt an AGS1 stream, authenticating every block.

    Args:
        data (bytes): The encrypted file contents.
        key_metadata (StandardKeyMetadata): The file's key, AAD prefix and `file_length`. The length is required and
            must match `data`: dropping trailing blocks leaves every remaining block valid, so only the trusted length
            from key metadata exposes a truncated file.
    """
    if key_metadata.file_length is None:
        raise ValueError("Cannot decrypt AGS1 stream: key metadata has no file_length")

    if len(data) != key_metadata.file_length:
        raise ValueError(f"Invalid AGS1 stream: key metadata expects {key_metadata.file_length} bytes, got {len(data)}")

    if len(data) < _MIN_STREAM_LENGTH:
        raise ValueError(f"Invalid AGS1 stream: expected at least {_MIN_STREAM_LENGTH} bytes, got {len(data)}")

    _check_stream_header(data)

    last_block_length = (len(data) - _GCM_STREAM_HEADER_LENGTH) % _CIPHER_BLOCK_SIZE
    if 0 < last_block_length < _BLOCK_OVERHEAD:
        raise ValueError(f"Truncated AGS1 stream: last block is {last_block_length} bytes, expected at least {_BLOCK_OVERHEAD}")

    cipher = AesGcmCipher(SecureKey(key_metadata.encryption_key))
    starts = range(_GCM_STREAM_HEADER_LENGTH, len(data), _CIPHER_BLOCK_SIZE)
    return b"".join(
        cipher.decrypt(data[start : start + _CIPHER_BLOCK_SIZE], _stream_block_aad(key_metadata.aad_prefix, index))
        for index, start in enumerate(starts)
    )


def _stream_block_aad(aad_prefix: bytes | None, block_index: int) -> bytes:
    """Return the additional authenticated data for the block at `block_index`."""
    return (aad_prefix or b"") + block_index.to_bytes(4, "little")


def _check_stream_header(data: bytes) -> None:
    """Check that `data` starts with an AGS1 header declaring the supported block size."""
    if (magic := data[: len(_GCM_STREAM_MAGIC)]) != _GCM_STREAM_MAGIC:
        raise ValueError(f"Invalid AGS1 header: magic {magic!r} does not match {_GCM_STREAM_MAGIC!r}")

    plain_block_size = int.from_bytes(data[len(_GCM_STREAM_MAGIC) : _GCM_STREAM_HEADER_LENGTH], "little")
    if plain_block_size != _PLAIN_BLOCK_SIZE:
        raise ValueError(f"Unsupported AGS1 block size: {plain_block_size} (expected {_PLAIN_BLOCK_SIZE})")
