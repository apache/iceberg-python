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

from pathlib import Path

import pytest

from pyiceberg.encryption.ciphers import AesGcmCipher, SecureKey
from pyiceberg.encryption.stream import (
    BLOCK_OVERHEAD,
    CIPHER_BLOCK_SIZE,
    GCM_STREAM_HEADER_LENGTH,
    GCM_STREAM_MAGIC,
    MAX_BLOCKS,
    MIN_STREAM_LENGTH,
    PLAIN_BLOCK_SIZE,
    Ags1Layout,
    calculate_plaintext_length,
    decode_stream_header,
    encode_stream_header,
    stream_block_aad,
)

KEY = SecureKey(b"0123456789012345")
AAD_PREFIX = b"0123456789abcdef"

# The header a Java `AesGcmOutputStream` writes: "AGS1" then 1 MiB as a little-endian int32.
JAVA_HEADER = b"AGS1\x00\x00\x10\x00"

# Streams written by Java's `AesGcmOutputStream`, with the parameters documented in ags1/README.md.
AGS1_FIXTURES = Path(__file__).parent / "ags1"
FIXTURE_KEY = SecureKey(bytes(range(16)))
FIXTURE_AAD_PREFIX = b"pyiceberg-ags1"


def build_stream(plaintext: bytes, aad_prefix: bytes | None = AAD_PREFIX) -> bytes:
    """Encrypt `plaintext` into an AGS1 stream, as an output stream implementation would."""
    blocks = [
        AesGcmCipher(KEY).encrypt(plaintext[start : start + PLAIN_BLOCK_SIZE], stream_block_aad(aad_prefix, index))
        for index, start in enumerate(range(0, len(plaintext), PLAIN_BLOCK_SIZE))
    ]
    return encode_stream_header() + b"".join(blocks)


def decrypt_stream(stream: bytes, key: SecureKey, aad_prefix: bytes | None) -> bytes:
    """Decrypt an AGS1 stream block by block, as an input stream implementation would."""
    layout = Ags1Layout.from_encrypted_length(len(stream))
    return b"".join(
        AesGcmCipher(key).decrypt(
            stream[layout.encrypted_block_offset(index) : layout.encrypted_block_offset(index) + layout.cipher_block_size(index)],
            stream_block_aad(aad_prefix, index),
        )
        for index in range(layout.num_blocks)
    )


def fixture_plaintext(length: int) -> bytes:
    """Return the plaintext the AGS1 fixtures encrypt: byte `i` is `i % 251`."""
    return (bytes(range(251)) * (length // 251 + 1))[:length]


def test_format_constants() -> None:
    assert GCM_STREAM_MAGIC == b"AGS1"
    assert PLAIN_BLOCK_SIZE == 1024 * 1024
    assert GCM_STREAM_HEADER_LENGTH == 8
    assert BLOCK_OVERHEAD == 28
    assert CIPHER_BLOCK_SIZE == PLAIN_BLOCK_SIZE + BLOCK_OVERHEAD
    assert MAX_BLOCKS == 2**32 - 1
    assert MIN_STREAM_LENGTH == 36


def test_encode_stream_header_matches_java() -> None:
    assert encode_stream_header() == JAVA_HEADER


def test_decode_stream_header() -> None:
    assert decode_stream_header(JAVA_HEADER) == PLAIN_BLOCK_SIZE
    assert decode_stream_header(encode_stream_header()) == PLAIN_BLOCK_SIZE


def test_decode_stream_header_ignores_trailing_block_bytes() -> None:
    assert decode_stream_header(JAVA_HEADER + b"block bytes") == PLAIN_BLOCK_SIZE


@pytest.mark.parametrize("length", [0, 4, 7])
def test_decode_stream_header_rejects_a_short_header(length: int) -> None:
    with pytest.raises(ValueError, match=f"Invalid AGS1 header: expected 8 bytes, got {length}"):
        decode_stream_header(bytes(length))


def test_decode_stream_header_rejects_the_wrong_magic() -> None:
    with pytest.raises(ValueError, match="magic b'AGS2' does not match b'AGS1'"):
        decode_stream_header(b"AGS2\x00\x00\x10\x00")


def test_decode_stream_header_rejects_an_unsupported_block_size() -> None:
    with pytest.raises(ValueError, match=f"Unsupported AGS1 block size: 512 \\(expected {PLAIN_BLOCK_SIZE}\\)"):
        decode_stream_header(GCM_STREAM_MAGIC + (512).to_bytes(4, "little"))


@pytest.mark.parametrize(
    "block_index, expected",
    [(0, b"\x00\x00\x00\x00"), (1, b"\x01\x00\x00\x00"), (258, b"\x02\x01\x00\x00"), (MAX_BLOCKS, b"\xff\xff\xff\xff")],
)
def test_stream_block_aad_encodes_the_index_little_endian(block_index: int, expected: bytes) -> None:
    assert stream_block_aad(None, block_index) == expected
    assert stream_block_aad(b"", block_index) == expected
    assert stream_block_aad(AAD_PREFIX, block_index) == AAD_PREFIX + expected


@pytest.mark.parametrize(
    "encrypted_length, expected",
    [
        (GCM_STREAM_HEADER_LENGTH + BLOCK_OVERHEAD, 0),
        (GCM_STREAM_HEADER_LENGTH + BLOCK_OVERHEAD + 100, 100),
        (GCM_STREAM_HEADER_LENGTH + CIPHER_BLOCK_SIZE, PLAIN_BLOCK_SIZE),
        (GCM_STREAM_HEADER_LENGTH + CIPHER_BLOCK_SIZE + BLOCK_OVERHEAD + 5, PLAIN_BLOCK_SIZE + 5),
        (GCM_STREAM_HEADER_LENGTH + 2 * CIPHER_BLOCK_SIZE, 2 * PLAIN_BLOCK_SIZE),
    ],
)
def test_calculate_plaintext_length(encrypted_length: int, expected: int) -> None:
    assert calculate_plaintext_length(encrypted_length) == expected


@pytest.mark.parametrize("encrypted_length", [0, 1, 7, GCM_STREAM_HEADER_LENGTH, MIN_STREAM_LENGTH - 1])
def test_calculate_plaintext_length_rejects_a_stream_shorter_than_one_block(encrypted_length: int) -> None:
    """A header alone is not a stream, matching Java's `MIN_STREAM_LENGTH` and iceberg-rust."""
    with pytest.raises(ValueError, match=f"expected at least {MIN_STREAM_LENGTH} bytes, got {encrypted_length}"):
        calculate_plaintext_length(encrypted_length)


@pytest.mark.parametrize("last_block_size", [1, 27])
def test_calculate_plaintext_length_rejects_a_truncated_last_block(last_block_size: int) -> None:
    with pytest.raises(ValueError, match=f"last block is {last_block_size} bytes, expected at least 28"):
        calculate_plaintext_length(GCM_STREAM_HEADER_LENGTH + CIPHER_BLOCK_SIZE + last_block_size)


@pytest.mark.parametrize(
    "encrypted_length, plaintext_length, num_blocks, last_cipher_block_size",
    [
        (GCM_STREAM_HEADER_LENGTH + BLOCK_OVERHEAD, 0, 1, BLOCK_OVERHEAD),
        (GCM_STREAM_HEADER_LENGTH + BLOCK_OVERHEAD + 100, 100, 1, BLOCK_OVERHEAD + 100),
        (GCM_STREAM_HEADER_LENGTH + CIPHER_BLOCK_SIZE, PLAIN_BLOCK_SIZE, 1, CIPHER_BLOCK_SIZE),
        (GCM_STREAM_HEADER_LENGTH + CIPHER_BLOCK_SIZE + BLOCK_OVERHEAD + 5, PLAIN_BLOCK_SIZE + 5, 2, BLOCK_OVERHEAD + 5),
        (GCM_STREAM_HEADER_LENGTH + 2 * CIPHER_BLOCK_SIZE, 2 * PLAIN_BLOCK_SIZE, 2, CIPHER_BLOCK_SIZE),
    ],
)
def test_layout_from_encrypted_length(
    encrypted_length: int, plaintext_length: int, num_blocks: int, last_cipher_block_size: int
) -> None:
    layout = Ags1Layout.from_encrypted_length(encrypted_length)

    assert layout == Ags1Layout(
        plaintext_length=plaintext_length, num_blocks=num_blocks, last_cipher_block_size=last_cipher_block_size
    )


def test_layout_rejects_a_header_only_stream() -> None:
    """Every stream holds at least one block, so a bare header has no layout."""
    with pytest.raises(ValueError, match=f"expected at least {MIN_STREAM_LENGTH} bytes, got {GCM_STREAM_HEADER_LENGTH}"):
        Ags1Layout.from_encrypted_length(GCM_STREAM_HEADER_LENGTH)


def test_layout_rejects_more_blocks_than_the_index_can_address() -> None:
    encrypted_length = GCM_STREAM_HEADER_LENGTH + (MAX_BLOCKS + 1) * CIPHER_BLOCK_SIZE

    with pytest.raises(ValueError, match=f"AGS1 streams hold at most {MAX_BLOCKS} blocks"):
        Ags1Layout.from_encrypted_length(encrypted_length)


def test_layout_block_sizes_and_offsets() -> None:
    layout = Ags1Layout.from_encrypted_length(GCM_STREAM_HEADER_LENGTH + 2 * CIPHER_BLOCK_SIZE + BLOCK_OVERHEAD + 7)

    assert layout.num_blocks == 3
    assert layout.cipher_block_size(0) == layout.cipher_block_size(1) == CIPHER_BLOCK_SIZE
    assert layout.plain_block_size(0) == layout.plain_block_size(1) == PLAIN_BLOCK_SIZE
    assert layout.cipher_block_size(2) == BLOCK_OVERHEAD + 7
    assert layout.plain_block_size(2) == 7
    assert layout.encrypted_block_offset(0) == GCM_STREAM_HEADER_LENGTH
    assert layout.encrypted_block_offset(1) == GCM_STREAM_HEADER_LENGTH + CIPHER_BLOCK_SIZE
    assert layout.encrypted_block_offset(2) == GCM_STREAM_HEADER_LENGTH + 2 * CIPHER_BLOCK_SIZE


@pytest.mark.parametrize("block_index", [-1, 1, 2])
def test_layout_rejects_an_out_of_range_block_index(block_index: int) -> None:
    layout = Ags1Layout.from_encrypted_length(GCM_STREAM_HEADER_LENGTH + CIPHER_BLOCK_SIZE)

    with pytest.raises(ValueError, match=f"Block index out of range: {block_index} \\(stream holds 1 blocks\\)"):
        layout.cipher_block_size(block_index)

    with pytest.raises(ValueError, match=f"Block index out of range: {block_index}"):
        layout.encrypted_block_offset(block_index)


@pytest.mark.parametrize(
    "plaintext_offset, expected",
    [(0, 0), (1, 0), (PLAIN_BLOCK_SIZE - 1, 0), (PLAIN_BLOCK_SIZE, 1), (PLAIN_BLOCK_SIZE + 6, 1)],
)
def test_layout_block_index_for_plaintext_offset(plaintext_offset: int, expected: int) -> None:
    layout = Ags1Layout.from_encrypted_length(GCM_STREAM_HEADER_LENGTH + CIPHER_BLOCK_SIZE + BLOCK_OVERHEAD + 7)

    assert layout.block_index_for(plaintext_offset) == expected


@pytest.mark.parametrize("plaintext_offset", [-1, PLAIN_BLOCK_SIZE])
def test_layout_rejects_an_out_of_range_plaintext_offset(plaintext_offset: int) -> None:
    layout = Ags1Layout.from_encrypted_length(GCM_STREAM_HEADER_LENGTH + CIPHER_BLOCK_SIZE)

    with pytest.raises(ValueError, match=f"Plaintext offset out of range: {plaintext_offset}"):
        layout.block_index_for(plaintext_offset)


@pytest.mark.parametrize("plaintext_length", [1, 100, PLAIN_BLOCK_SIZE, PLAIN_BLOCK_SIZE + 7, 2 * PLAIN_BLOCK_SIZE])
def test_layout_describes_a_real_stream(plaintext_length: int) -> None:
    """The layout derived from a stream's length must match the stream that was written."""
    plaintext = bytes(range(256)) * (plaintext_length // 256) + bytes(plaintext_length % 256)
    stream = build_stream(plaintext)

    layout = Ags1Layout.from_encrypted_length(len(stream))

    assert decode_stream_header(stream) == PLAIN_BLOCK_SIZE
    assert layout.plaintext_length == plaintext_length
    assert layout.num_blocks == -(-plaintext_length // PLAIN_BLOCK_SIZE)

    decrypted = b""
    for index in range(layout.num_blocks):
        offset = layout.encrypted_block_offset(index)
        block = stream[offset : offset + layout.cipher_block_size(index)]
        decrypted += AesGcmCipher(KEY).decrypt(block, stream_block_aad(AAD_PREFIX, index))

    assert decrypted == plaintext


def test_blocks_cannot_be_reordered() -> None:
    stream = build_stream(bytes(PLAIN_BLOCK_SIZE + 7))
    layout = Ags1Layout.from_encrypted_length(len(stream))
    first_block = stream[layout.encrypted_block_offset(0) : layout.encrypted_block_offset(1)]

    with pytest.raises(ValueError, match="wrong decryption key; or corrupt/tampered data"):
        AesGcmCipher(KEY).decrypt(first_block, stream_block_aad(AAD_PREFIX, 1))


def test_blocks_cannot_be_moved_between_files() -> None:
    stream = build_stream(bytes(100))
    layout = Ags1Layout.from_encrypted_length(len(stream))
    block = stream[layout.encrypted_block_offset(0) :]

    with pytest.raises(ValueError, match="wrong decryption key; or corrupt/tampered data"):
        AesGcmCipher(KEY).decrypt(block, stream_block_aad(b"another file's prefix", 0))


@pytest.mark.parametrize(
    "name, plaintext_length, num_blocks, aad_prefix",
    [
        ("empty.ags1", 0, 1, FIXTURE_AAD_PREFIX),
        ("partial-block.ags1", 100, 1, FIXTURE_AAD_PREFIX),
        ("partial-block-no-aad.ags1", 100, 1, None),
        ("aligned-multi-block.ags1", 2 * PLAIN_BLOCK_SIZE, 2, FIXTURE_AAD_PREFIX),
    ],
)
def test_decrypts_a_java_written_stream(name: str, plaintext_length: int, num_blocks: int, aad_prefix: bytes | None) -> None:
    """A stream written by Java must decrypt with the layout derived from its length alone."""
    stream = (AGS1_FIXTURES / name).read_bytes()

    layout = Ags1Layout.from_encrypted_length(len(stream))

    assert stream[:GCM_STREAM_HEADER_LENGTH] == encode_stream_header()
    assert decode_stream_header(stream) == PLAIN_BLOCK_SIZE
    assert layout.plaintext_length == plaintext_length
    assert layout.num_blocks == num_blocks
    assert decrypt_stream(stream, FIXTURE_KEY, aad_prefix) == fixture_plaintext(plaintext_length)


def test_java_encodes_an_empty_file_as_one_empty_block() -> None:
    """Java writes a header plus one empty block for an empty file, which is the shortest stream accepted."""
    stream = (AGS1_FIXTURES / "empty.ags1").read_bytes()

    assert len(stream) == MIN_STREAM_LENGTH
    assert Ags1Layout.from_encrypted_length(len(stream)) == Ags1Layout(
        plaintext_length=0, num_blocks=1, last_cipher_block_size=BLOCK_OVERHEAD
    )


def test_java_appends_no_trailing_block_to_a_block_aligned_stream() -> None:
    """A block-aligned write ends on its last full block, so the length holds no extra empty block."""
    stream = (AGS1_FIXTURES / "aligned-multi-block.ags1").read_bytes()

    assert len(stream) == GCM_STREAM_HEADER_LENGTH + 2 * CIPHER_BLOCK_SIZE


def test_a_truncated_java_stream_still_authenticates() -> None:
    """Dropping a trailing block leaves every remaining block valid, so the length must come from key metadata."""
    stream = (AGS1_FIXTURES / "aligned-multi-block.ags1").read_bytes()

    truncated = stream[: GCM_STREAM_HEADER_LENGTH + CIPHER_BLOCK_SIZE]

    assert decrypt_stream(truncated, FIXTURE_KEY, FIXTURE_AAD_PREFIX) == fixture_plaintext(PLAIN_BLOCK_SIZE)
