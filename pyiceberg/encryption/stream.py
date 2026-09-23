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
"""Format primitives for the AGS1 stream, used to encrypt manifests and manifest lists.

An AGS1 stream is an 8 byte header followed by a sequence of AES-GCM blocks::

    "AGS1" || plain_block_size (4 bytes, little endian)
    nonce || ciphertext || tag        (block 0, up to PLAIN_BLOCK_SIZE of plaintext)
    nonce || ciphertext || tag        (block 1..n, the last of which may be shorter)

Each block authenticates `aad_prefix || block_index` as additional data, so blocks cannot
be reordered or moved between files. A stream holds at least one block, so an empty file is
a header followed by a single empty block rather than a bare header.
"""

from __future__ import annotations

from dataclasses import dataclass

from pyiceberg.encryption.ciphers import AesGcmCipher

GCM_STREAM_MAGIC = b"AGS1"
PLAIN_BLOCK_SIZE = 1024 * 1024
GCM_STREAM_HEADER_LENGTH = len(GCM_STREAM_MAGIC) + 4
BLOCK_OVERHEAD = AesGcmCipher.NONCE_LENGTH + AesGcmCipher.TAG_LENGTH
CIPHER_BLOCK_SIZE = PLAIN_BLOCK_SIZE + BLOCK_OVERHEAD
BLOCK_INDEX_LENGTH = 4
MAX_BLOCKS = 2 ** (8 * BLOCK_INDEX_LENGTH) - 1
MIN_STREAM_LENGTH = GCM_STREAM_HEADER_LENGTH + BLOCK_OVERHEAD


def stream_block_aad(aad_prefix: bytes | None, block_index: int) -> bytes:
    """Return the additional authenticated data for the block at `block_index`.

    Args:
        aad_prefix (bytes | None): The file's AAD prefix, from its key metadata.
        block_index (int): The zero-based index of the block within the stream.
    """
    return (aad_prefix or b"") + block_index.to_bytes(BLOCK_INDEX_LENGTH, "little")


def encode_stream_header() -> bytes:
    """Encode the AGS1 header that precedes the first block."""
    return GCM_STREAM_MAGIC + PLAIN_BLOCK_SIZE.to_bytes(4, "little")


def decode_stream_header(header: bytes) -> int:
    """Decode an AGS1 header, returning the plaintext block size it declares.

    Args:
        header (bytes): At least `GCM_STREAM_HEADER_LENGTH` bytes from the start of the stream.
    """
    if len(header) < GCM_STREAM_HEADER_LENGTH:
        raise ValueError(f"Invalid AGS1 header: expected {GCM_STREAM_HEADER_LENGTH} bytes, got {len(header)}")

    if (magic := header[: len(GCM_STREAM_MAGIC)]) != GCM_STREAM_MAGIC:
        raise ValueError(f"Invalid AGS1 header: magic {magic!r} does not match {GCM_STREAM_MAGIC!r}")

    plain_block_size = int.from_bytes(header[len(GCM_STREAM_MAGIC) : GCM_STREAM_HEADER_LENGTH], "little")
    if plain_block_size != PLAIN_BLOCK_SIZE:
        raise ValueError(f"Unsupported AGS1 block size: {plain_block_size} (expected {PLAIN_BLOCK_SIZE})")

    return plain_block_size


def calculate_plaintext_length(encrypted_length: int) -> int:
    """Return the plaintext length of an AGS1 stream that occupies `encrypted_length` bytes.

    Args:
        encrypted_length (int): The stream's length, which must be the trusted `file_length` from the file's
            `StandardKeyMetadata`, never a file system stat. The spec requires the trusted length because a
            stat lets an attacker drop trailing blocks while every remaining block still authenticates.
    """
    if encrypted_length < MIN_STREAM_LENGTH:
        raise ValueError(f"Invalid AGS1 stream: expected at least {MIN_STREAM_LENGTH} bytes, got {encrypted_length}")

    stream_length = encrypted_length - GCM_STREAM_HEADER_LENGTH
    full_blocks, cipher_bytes_in_last_block = divmod(stream_length, CIPHER_BLOCK_SIZE)
    if cipher_bytes_in_last_block == 0:
        return full_blocks * PLAIN_BLOCK_SIZE

    if cipher_bytes_in_last_block < BLOCK_OVERHEAD:
        raise ValueError(
            f"Truncated AGS1 stream: last block is {cipher_bytes_in_last_block} bytes, expected at least {BLOCK_OVERHEAD}"
        )

    return full_blocks * PLAIN_BLOCK_SIZE + cipher_bytes_in_last_block - BLOCK_OVERHEAD


@dataclass(frozen=True)
class Ags1Layout:
    """Where each block of an AGS1 stream sits, derived from the trusted encrypted file length.

    Only the final block may hold less than `PLAIN_BLOCK_SIZE` of plaintext, so the layout
    follows from the encrypted length alone, without reading the stream.
    """

    plaintext_length: int
    num_blocks: int
    last_cipher_block_size: int

    @classmethod
    def from_encrypted_length(cls, encrypted_length: int) -> Ags1Layout:
        """Derive the layout of an AGS1 stream that occupies `encrypted_length` bytes.

        Args:
            encrypted_length (int): The stream's length, which must be the trusted `file_length` from the file's
                `StandardKeyMetadata`, never a file system stat. See `calculate_plaintext_length`.
        """
        plaintext_length = calculate_plaintext_length(encrypted_length)
        stream_length = encrypted_length - GCM_STREAM_HEADER_LENGTH
        full_blocks, cipher_bytes_in_last_block = divmod(stream_length, CIPHER_BLOCK_SIZE)
        if cipher_bytes_in_last_block == 0:
            num_blocks, last_cipher_block_size = full_blocks, CIPHER_BLOCK_SIZE
        else:
            num_blocks, last_cipher_block_size = full_blocks + 1, cipher_bytes_in_last_block

        if num_blocks > MAX_BLOCKS:
            raise ValueError(f"AGS1 streams hold at most {MAX_BLOCKS} blocks, but {encrypted_length} bytes needs {num_blocks}")

        return cls(plaintext_length=plaintext_length, num_blocks=num_blocks, last_cipher_block_size=last_cipher_block_size)

    def _check_block_index(self, block_index: int) -> None:
        if not 0 <= block_index < self.num_blocks:
            raise ValueError(f"Block index out of range: {block_index} (stream holds {self.num_blocks} blocks)")

    def cipher_block_size(self, block_index: int) -> int:
        """Return the encrypted size of the block at `block_index`."""
        self._check_block_index(block_index)
        return self.last_cipher_block_size if block_index == self.num_blocks - 1 else CIPHER_BLOCK_SIZE

    def plain_block_size(self, block_index: int) -> int:
        """Return the plaintext size of the block at `block_index`."""
        return self.cipher_block_size(block_index) - BLOCK_OVERHEAD

    def encrypted_block_offset(self, block_index: int) -> int:
        """Return the offset of the block at `block_index` within the encrypted stream."""
        self._check_block_index(block_index)
        return GCM_STREAM_HEADER_LENGTH + block_index * CIPHER_BLOCK_SIZE

    def block_index_for(self, plaintext_offset: int) -> int:
        """Return the index of the block holding `plaintext_offset`."""
        if not 0 <= plaintext_offset < self.plaintext_length:
            raise ValueError(f"Plaintext offset out of range: {plaintext_offset} (stream holds {self.plaintext_length} bytes)")
        return plaintext_offset // PLAIN_BLOCK_SIZE
