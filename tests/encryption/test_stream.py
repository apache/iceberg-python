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
    _BLOCK_OVERHEAD,
    _CIPHER_BLOCK_SIZE,
    _GCM_STREAM_HEADER_LENGTH,
    _GCM_STREAM_MAGIC,
    _MAX_BLOCKS,
    _MIN_STREAM_LENGTH,
    _PLAIN_BLOCK_SIZE,
    _Ags1Layout,
    _calculate_plaintext_length,
    _decode_stream_header,
    _encode_stream_header,
    _stream_block_aad,
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
        AesGcmCipher(KEY).encrypt(plaintext[start : start + _PLAIN_BLOCK_SIZE], _stream_block_aad(aad_prefix, index))
        for index, start in enumerate(range(0, len(plaintext), _PLAIN_BLOCK_SIZE))
    ]
    return _encode_stream_header() + b"".join(blocks)


def decrypt_stream(stream: bytes, key: SecureKey, aad_prefix: bytes | None) -> bytes:
    """Decrypt an AGS1 stream block by block, as an input stream implementation would."""
    layout = _Ags1Layout.from_encrypted_length(len(stream))
    return b"".join(
        AesGcmCipher(key).decrypt(
            stream[layout.encrypted_block_offset(index) : layout.encrypted_block_offset(index) + layout.cipher_block_size(index)],
            _stream_block_aad(aad_prefix, index),
        )
        for index in range(layout.num_blocks)
    )


def fixture_plaintext(length: int) -> bytes:
    """Return the plaintext the AGS1 fixtures encrypt: byte `i` is `i % 251`."""
    return (bytes(range(251)) * (length // 251 + 1))[:length]


def test_format_constants() -> None:
    assert _GCM_STREAM_MAGIC == b"AGS1"
    assert _PLAIN_BLOCK_SIZE == 1024 * 1024
    assert _GCM_STREAM_HEADER_LENGTH == 8
    assert _BLOCK_OVERHEAD == 28
    assert _CIPHER_BLOCK_SIZE == _PLAIN_BLOCK_SIZE + _BLOCK_OVERHEAD
    assert _MAX_BLOCKS == 2**32 - 1
    assert _MIN_STREAM_LENGTH == 36


def test_encode_stream_header_matches_java() -> None:
    assert _encode_stream_header() == JAVA_HEADER


def test_decode_stream_header() -> None:
    assert _decode_stream_header(JAVA_HEADER) == _PLAIN_BLOCK_SIZE
    assert _decode_stream_header(_encode_stream_header()) == _PLAIN_BLOCK_SIZE


def test_decode_stream_header_ignores_trailing_block_bytes() -> None:
    assert _decode_stream_header(JAVA_HEADER + b"block bytes") == _PLAIN_BLOCK_SIZE


@pytest.mark.parametrize("length", [0, 4, 7])
def test_decode_stream_header_rejects_a_short_header(length: int) -> None:
    with pytest.raises(ValueError, match=f"Invalid AGS1 header: expected 8 bytes, got {length}"):
        _decode_stream_header(bytes(length))


def test_decode_stream_header_rejects_the_wrong_magic() -> None:
    with pytest.raises(ValueError, match="magic b'AGS2' does not match b'AGS1'"):
        _decode_stream_header(b"AGS2\x00\x00\x10\x00")


def test_decode_stream_header_rejects_an_unsupported_block_size() -> None:
    with pytest.raises(ValueError, match=f"Unsupported AGS1 block size: 512 \\(expected {_PLAIN_BLOCK_SIZE}\\)"):
        _decode_stream_header(_GCM_STREAM_MAGIC + (512).to_bytes(4, "little"))


@pytest.mark.parametrize(
    "block_index, expected",
    [(0, b"\x00\x00\x00\x00"), (1, b"\x01\x00\x00\x00"), (258, b"\x02\x01\x00\x00"), (_MAX_BLOCKS, b"\xff\xff\xff\xff")],
)
def test_stream_block_aad_encodes_the_index_little_endian(block_index: int, expected: bytes) -> None:
    assert _stream_block_aad(None, block_index) == expected
    assert _stream_block_aad(b"", block_index) == expected
    assert _stream_block_aad(AAD_PREFIX, block_index) == AAD_PREFIX + expected


@pytest.mark.parametrize(
    "encrypted_length, expected",
    [
        (_GCM_STREAM_HEADER_LENGTH + _BLOCK_OVERHEAD, 0),
        (_GCM_STREAM_HEADER_LENGTH + _BLOCK_OVERHEAD + 100, 100),
        (_GCM_STREAM_HEADER_LENGTH + _CIPHER_BLOCK_SIZE, _PLAIN_BLOCK_SIZE),
        (_GCM_STREAM_HEADER_LENGTH + _CIPHER_BLOCK_SIZE + _BLOCK_OVERHEAD + 5, _PLAIN_BLOCK_SIZE + 5),
        (_GCM_STREAM_HEADER_LENGTH + 2 * _CIPHER_BLOCK_SIZE, 2 * _PLAIN_BLOCK_SIZE),
    ],
)
def test_calculate_plaintext_length(encrypted_length: int, expected: int) -> None:
    assert _calculate_plaintext_length(encrypted_length) == expected


@pytest.mark.parametrize("encrypted_length", [0, 1, 7, _GCM_STREAM_HEADER_LENGTH, _MIN_STREAM_LENGTH - 1])
def test_calculate_plaintext_length_rejects_a_stream_shorter_than_one_block(encrypted_length: int) -> None:
    """A header alone is not a stream, matching Java's `_MIN_STREAM_LENGTH` and iceberg-rust."""
    with pytest.raises(ValueError, match=f"expected at least {_MIN_STREAM_LENGTH} bytes, got {encrypted_length}"):
        _calculate_plaintext_length(encrypted_length)


@pytest.mark.parametrize("last_block_size", [1, 27])
def test_calculate_plaintext_length_rejects_a_truncated_last_block(last_block_size: int) -> None:
    with pytest.raises(ValueError, match=f"last block is {last_block_size} bytes, expected at least 28"):
        _calculate_plaintext_length(_GCM_STREAM_HEADER_LENGTH + _CIPHER_BLOCK_SIZE + last_block_size)


@pytest.mark.parametrize(
    "encrypted_length, plaintext_length, num_blocks, last_cipher_block_size",
    [
        (_GCM_STREAM_HEADER_LENGTH + _BLOCK_OVERHEAD, 0, 1, _BLOCK_OVERHEAD),
        (_GCM_STREAM_HEADER_LENGTH + _BLOCK_OVERHEAD + 100, 100, 1, _BLOCK_OVERHEAD + 100),
        (_GCM_STREAM_HEADER_LENGTH + _CIPHER_BLOCK_SIZE, _PLAIN_BLOCK_SIZE, 1, _CIPHER_BLOCK_SIZE),
        (_GCM_STREAM_HEADER_LENGTH + _CIPHER_BLOCK_SIZE + _BLOCK_OVERHEAD + 5, _PLAIN_BLOCK_SIZE + 5, 2, _BLOCK_OVERHEAD + 5),
        (_GCM_STREAM_HEADER_LENGTH + 2 * _CIPHER_BLOCK_SIZE, 2 * _PLAIN_BLOCK_SIZE, 2, _CIPHER_BLOCK_SIZE),
    ],
)
def test_layout_from_encrypted_length(
    encrypted_length: int, plaintext_length: int, num_blocks: int, last_cipher_block_size: int
) -> None:
    layout = _Ags1Layout.from_encrypted_length(encrypted_length)

    assert layout == _Ags1Layout(
        plaintext_length=plaintext_length, num_blocks=num_blocks, last_cipher_block_size=last_cipher_block_size
    )


def test_layout_rejects_a_header_only_stream() -> None:
    """Every stream holds at least one block, so a bare header has no layout."""
    with pytest.raises(ValueError, match=f"expected at least {_MIN_STREAM_LENGTH} bytes, got {_GCM_STREAM_HEADER_LENGTH}"):
        _Ags1Layout.from_encrypted_length(_GCM_STREAM_HEADER_LENGTH)


def test_layout_rejects_more_blocks_than_the_index_can_address() -> None:
    encrypted_length = _GCM_STREAM_HEADER_LENGTH + (_MAX_BLOCKS + 1) * _CIPHER_BLOCK_SIZE

    with pytest.raises(ValueError, match=f"AGS1 streams hold at most {_MAX_BLOCKS} blocks"):
        _Ags1Layout.from_encrypted_length(encrypted_length)


def test_layout_block_sizes_and_offsets() -> None:
    layout = _Ags1Layout.from_encrypted_length(_GCM_STREAM_HEADER_LENGTH + 2 * _CIPHER_BLOCK_SIZE + _BLOCK_OVERHEAD + 7)

    assert layout.num_blocks == 3
    assert layout.cipher_block_size(0) == layout.cipher_block_size(1) == _CIPHER_BLOCK_SIZE
    assert layout.plain_block_size(0) == layout.plain_block_size(1) == _PLAIN_BLOCK_SIZE
    assert layout.cipher_block_size(2) == _BLOCK_OVERHEAD + 7
    assert layout.plain_block_size(2) == 7
    assert layout.encrypted_block_offset(0) == _GCM_STREAM_HEADER_LENGTH
    assert layout.encrypted_block_offset(1) == _GCM_STREAM_HEADER_LENGTH + _CIPHER_BLOCK_SIZE
    assert layout.encrypted_block_offset(2) == _GCM_STREAM_HEADER_LENGTH + 2 * _CIPHER_BLOCK_SIZE


@pytest.mark.parametrize("block_index", [-1, 1, 2])
def test_layout_rejects_an_out_of_range_block_index(block_index: int) -> None:
    layout = _Ags1Layout.from_encrypted_length(_GCM_STREAM_HEADER_LENGTH + _CIPHER_BLOCK_SIZE)

    with pytest.raises(ValueError, match=f"Block index out of range: {block_index} \\(stream holds 1 blocks\\)"):
        layout.cipher_block_size(block_index)

    with pytest.raises(ValueError, match=f"Block index out of range: {block_index}"):
        layout.encrypted_block_offset(block_index)


@pytest.mark.parametrize(
    "plaintext_offset, expected",
    [(0, 0), (1, 0), (_PLAIN_BLOCK_SIZE - 1, 0), (_PLAIN_BLOCK_SIZE, 1), (_PLAIN_BLOCK_SIZE + 6, 1)],
)
def test_layout_block_index_for_plaintext_offset(plaintext_offset: int, expected: int) -> None:
    layout = _Ags1Layout.from_encrypted_length(_GCM_STREAM_HEADER_LENGTH + _CIPHER_BLOCK_SIZE + _BLOCK_OVERHEAD + 7)

    assert layout.block_index_for(plaintext_offset) == expected


@pytest.mark.parametrize("plaintext_offset", [-1, _PLAIN_BLOCK_SIZE])
def test_layout_rejects_an_out_of_range_plaintext_offset(plaintext_offset: int) -> None:
    layout = _Ags1Layout.from_encrypted_length(_GCM_STREAM_HEADER_LENGTH + _CIPHER_BLOCK_SIZE)

    with pytest.raises(ValueError, match=f"Plaintext offset out of range: {plaintext_offset}"):
        layout.block_index_for(plaintext_offset)


@pytest.mark.parametrize("plaintext_length", [1, 100, _PLAIN_BLOCK_SIZE, _PLAIN_BLOCK_SIZE + 7, 2 * _PLAIN_BLOCK_SIZE])
def test_layout_describes_a_real_stream(plaintext_length: int) -> None:
    """The layout derived from a stream's length must match the stream that was written."""
    plaintext = bytes(range(256)) * (plaintext_length // 256) + bytes(plaintext_length % 256)
    stream = build_stream(plaintext)

    layout = _Ags1Layout.from_encrypted_length(len(stream))

    assert _decode_stream_header(stream) == _PLAIN_BLOCK_SIZE
    assert layout.plaintext_length == plaintext_length
    assert layout.num_blocks == -(-plaintext_length // _PLAIN_BLOCK_SIZE)

    decrypted = b""
    for index in range(layout.num_blocks):
        offset = layout.encrypted_block_offset(index)
        block = stream[offset : offset + layout.cipher_block_size(index)]
        decrypted += AesGcmCipher(KEY).decrypt(block, _stream_block_aad(AAD_PREFIX, index))

    assert decrypted == plaintext


def test_blocks_cannot_be_reordered() -> None:
    stream = build_stream(bytes(_PLAIN_BLOCK_SIZE + 7))
    layout = _Ags1Layout.from_encrypted_length(len(stream))
    first_block = stream[layout.encrypted_block_offset(0) : layout.encrypted_block_offset(1)]

    with pytest.raises(ValueError, match="wrong decryption key; or corrupt/tampered data"):
        AesGcmCipher(KEY).decrypt(first_block, _stream_block_aad(AAD_PREFIX, 1))


def test_blocks_cannot_be_moved_between_files() -> None:
    stream = build_stream(bytes(100))
    layout = _Ags1Layout.from_encrypted_length(len(stream))
    block = stream[layout.encrypted_block_offset(0) :]

    with pytest.raises(ValueError, match="wrong decryption key; or corrupt/tampered data"):
        AesGcmCipher(KEY).decrypt(block, _stream_block_aad(b"another file's prefix", 0))


@pytest.mark.parametrize(
    "name, plaintext_length, num_blocks, aad_prefix",
    [
        ("empty.ags1", 0, 1, FIXTURE_AAD_PREFIX),
        ("partial-block.ags1", 100, 1, FIXTURE_AAD_PREFIX),
        ("partial-block-no-aad.ags1", 100, 1, None),
        ("aligned-multi-block.ags1", 2 * _PLAIN_BLOCK_SIZE, 2, FIXTURE_AAD_PREFIX),
    ],
)
def test_decrypts_a_java_written_stream(name: str, plaintext_length: int, num_blocks: int, aad_prefix: bytes | None) -> None:
    """A stream written by Java must decrypt with the layout derived from its length alone."""
    stream = (AGS1_FIXTURES / name).read_bytes()

    layout = _Ags1Layout.from_encrypted_length(len(stream))

    assert stream[:_GCM_STREAM_HEADER_LENGTH] == _encode_stream_header()
    assert _decode_stream_header(stream) == _PLAIN_BLOCK_SIZE
    assert layout.plaintext_length == plaintext_length
    assert layout.num_blocks == num_blocks
    assert decrypt_stream(stream, FIXTURE_KEY, aad_prefix) == fixture_plaintext(plaintext_length)


def test_java_encodes_an_empty_file_as_one_empty_block() -> None:
    """Java writes a header plus one empty block for an empty file, which is the shortest stream accepted."""
    stream = (AGS1_FIXTURES / "empty.ags1").read_bytes()

    assert len(stream) == _MIN_STREAM_LENGTH
    assert _Ags1Layout.from_encrypted_length(len(stream)) == _Ags1Layout(
        plaintext_length=0, num_blocks=1, last_cipher_block_size=_BLOCK_OVERHEAD
    )


def test_java_appends_no_trailing_block_to_a_block_aligned_stream() -> None:
    """A block-aligned write ends on its last full block, so the length holds no extra empty block."""
    stream = (AGS1_FIXTURES / "aligned-multi-block.ags1").read_bytes()

    assert len(stream) == _GCM_STREAM_HEADER_LENGTH + 2 * _CIPHER_BLOCK_SIZE


def test_a_truncated_java_stream_still_authenticates() -> None:
    """Dropping a trailing block leaves every remaining block valid, so the length must come from key metadata."""
    stream = (AGS1_FIXTURES / "aligned-multi-block.ags1").read_bytes()

    truncated = stream[: _GCM_STREAM_HEADER_LENGTH + _CIPHER_BLOCK_SIZE]

    assert decrypt_stream(truncated, FIXTURE_KEY, FIXTURE_AAD_PREFIX) == fixture_plaintext(_PLAIN_BLOCK_SIZE)
