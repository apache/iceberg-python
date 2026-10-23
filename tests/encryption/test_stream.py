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

import pytest

from pyiceberg.encryption.key_metadata import StandardKeyMetadata
from pyiceberg.encryption.stream import (
    _BLOCK_OVERHEAD,
    _CIPHER_BLOCK_SIZE,
    _GCM_STREAM_HEADER,
    _GCM_STREAM_HEADER_LENGTH,
    _MIN_STREAM_LENGTH,
    _PLAIN_BLOCK_SIZE,
    _decrypt_ags1,
    _encrypt_ags1,
    _stream_block_aad,
)

KEY = b"0123456789012345"
AAD_PREFIX = b"0123456789abcdef"
KEY_METADATA = StandardKeyMetadata(encryption_key=KEY, aad_prefix=AAD_PREFIX)

# The header a Java `AesGcmOutputStream` writes: "AGS1" then 1 MiB as a little-endian int32.
JAVA_HEADER = b"AGS1\x00\x00\x10\x00"


# Streams written by Java's `AesGcmOutputFile` (Apache Iceberg 1.11.0), so AGS1 is checked against another
# implementation's bytes rather than only its own round trip. Multi-block vectors are left out because each block
# adds 1 MiB. #4010 tracks replacing these with vectors from iceberg-verification.
FIXTURE_KEY = bytes(range(16))
FIXTURE_AAD_PREFIX = b"pyiceberg-ags1"
JAVA_STREAMS = {
    # An empty file is the header plus one empty block, not a bare header.
    "empty": bytes.fromhex("4147533100001000178c6b4fd72e166bc1667356c865baed13878ed51c4340f6426501ec"),
    "partial-block": bytes.fromhex(
        "4147533100001000c02998e75683447a486f7dd3dd6c9f8a2a10fbfc956b15fdb35611534fe8d1414c81b17a477b914b"
        "fa0b515a5060cff94301feb0e85535402a2a2966214e20274635d4c19ae525d4da042ac51ed7db0c6698c731df0ba637"
        "4aff8b8d5c05297b8896abbfabe09c3ce1a2389c46d14718c17b0a3e33a4ddebc05a11b18ec9c576"
    ),
    # Written with a null AAD prefix, so the block index alone is the AAD.
    "partial-block-no-aad": bytes.fromhex(
        "4147533100001000117d00679bfa455f29880fe9dcd8d2ae9b8a7e81d40dbce6061631524e5ec61cbac9914317e7e60f"
        "dff52b32a779b3ae5339ea8859a40b3454a986a7f36b62f6e46699b036b6abe2ae043a86aee0ac5873066bab2420802c"
        "f970eede664723b455a224b99a5866c862c81640393168355e2acbe90e677a6dbaadab9ffa5cf3dd"
    ),
}


def decrypt(stream: bytes, key: bytes = KEY, aad_prefix: bytes | None = AAD_PREFIX) -> bytes:
    """Decrypt `stream`, trusting its own length as the key metadata's `file_length`."""
    return _decrypt_ags1(stream, StandardKeyMetadata(encryption_key=key, aad_prefix=aad_prefix, file_length=len(stream)))


def patterned_plaintext(length: int) -> bytes:
    """Return a plaintext whose byte `i` is `i % 251`, so a misordered block changes the bytes."""
    return (bytes(range(251)) * (length // 251 + 1))[:length]


def test_format_constants() -> None:
    assert _GCM_STREAM_HEADER == JAVA_HEADER
    assert _PLAIN_BLOCK_SIZE == 1024 * 1024
    assert _GCM_STREAM_HEADER_LENGTH == 8
    assert _BLOCK_OVERHEAD == 28
    assert _CIPHER_BLOCK_SIZE == _PLAIN_BLOCK_SIZE + _BLOCK_OVERHEAD
    assert _MIN_STREAM_LENGTH == 36


@pytest.mark.parametrize(
    "block_index, expected",
    [(0, b"\x00\x00\x00\x00"), (1, b"\x01\x00\x00\x00"), (258, b"\x02\x01\x00\x00"), (2**32 - 1, b"\xff\xff\xff\xff")],
)
def test_stream_block_aad_encodes_the_index_little_endian(block_index: int, expected: bytes) -> None:
    assert _stream_block_aad(None, block_index) == expected
    assert _stream_block_aad(b"", block_index) == expected
    assert _stream_block_aad(AAD_PREFIX, block_index) == AAD_PREFIX + expected


@pytest.mark.parametrize("aad_prefix", [AAD_PREFIX, None])
@pytest.mark.parametrize("plaintext_length", [0, 1, 100, _PLAIN_BLOCK_SIZE, _PLAIN_BLOCK_SIZE + 7, 2 * _PLAIN_BLOCK_SIZE])
def test_round_trip(plaintext_length: int, aad_prefix: bytes | None) -> None:
    plaintext = patterned_plaintext(plaintext_length)

    stream = _encrypt_ags1(plaintext, StandardKeyMetadata(encryption_key=KEY, aad_prefix=aad_prefix))

    assert stream.startswith(JAVA_HEADER)
    assert decrypt(stream, aad_prefix=aad_prefix) == plaintext


@pytest.mark.parametrize(
    "plaintext_length, encrypted_length",
    [
        (0, _MIN_STREAM_LENGTH),
        (100, _GCM_STREAM_HEADER_LENGTH + _BLOCK_OVERHEAD + 100),
        (_PLAIN_BLOCK_SIZE, _GCM_STREAM_HEADER_LENGTH + _CIPHER_BLOCK_SIZE),
        (_PLAIN_BLOCK_SIZE + 7, _GCM_STREAM_HEADER_LENGTH + _CIPHER_BLOCK_SIZE + _BLOCK_OVERHEAD + 7),
        (2 * _PLAIN_BLOCK_SIZE, _GCM_STREAM_HEADER_LENGTH + 2 * _CIPHER_BLOCK_SIZE),
    ],
)
def test_encrypted_length(plaintext_length: int, encrypted_length: int) -> None:
    """An empty file gets one empty block, and a block-aligned file gets no trailing empty block, as in Java."""
    assert len(_encrypt_ags1(bytes(plaintext_length), KEY_METADATA)) == encrypted_length


@pytest.mark.parametrize(
    "name, plaintext_length, aad_prefix",
    [
        ("empty", 0, FIXTURE_AAD_PREFIX),
        ("partial-block", 100, FIXTURE_AAD_PREFIX),
        ("partial-block-no-aad", 100, None),
    ],
)
def test_decrypts_a_java_written_stream(name: str, plaintext_length: int, aad_prefix: bytes | None) -> None:
    stream = JAVA_STREAMS[name]
    key_metadata = StandardKeyMetadata(encryption_key=FIXTURE_KEY, aad_prefix=aad_prefix, file_length=len(stream))

    assert _decrypt_ags1(stream, key_metadata) == patterned_plaintext(plaintext_length)
    assert len(_encrypt_ags1(patterned_plaintext(plaintext_length), key_metadata)) == len(stream)


def test_decrypt_requires_file_length() -> None:
    stream = _encrypt_ags1(b"data", KEY_METADATA)

    with pytest.raises(ValueError, match="key metadata has no file_length"):
        _decrypt_ags1(stream, KEY_METADATA)


def test_a_truncated_stream_still_authenticates() -> None:
    """Dropping a trailing block leaves every remaining block valid, so the length must come from key metadata."""
    stream = _encrypt_ags1(patterned_plaintext(2 * _PLAIN_BLOCK_SIZE), KEY_METADATA)

    truncated = stream[: _GCM_STREAM_HEADER_LENGTH + _CIPHER_BLOCK_SIZE]

    assert decrypt(truncated) == patterned_plaintext(_PLAIN_BLOCK_SIZE)


@pytest.mark.parametrize("change", ["truncate", "extend"])
def test_decrypt_rejects_a_length_other_than_the_trusted_length(change: str) -> None:
    stream = _encrypt_ags1(patterned_plaintext(2 * _PLAIN_BLOCK_SIZE), KEY_METADATA)
    key_metadata = StandardKeyMetadata(encryption_key=KEY, aad_prefix=AAD_PREFIX, file_length=len(stream))
    changed = stream[: _GCM_STREAM_HEADER_LENGTH + _CIPHER_BLOCK_SIZE] if change == "truncate" else stream + b"\x00"

    with pytest.raises(ValueError, match=f"key metadata expects {len(stream)} bytes, got {len(changed)}"):
        _decrypt_ags1(changed, key_metadata)


@pytest.mark.parametrize("length", [0, 7, _GCM_STREAM_HEADER_LENGTH, _MIN_STREAM_LENGTH - 1])
def test_decrypt_rejects_a_stream_shorter_than_one_block(length: int) -> None:
    """A header alone is not a stream, matching Java's `MIN_STREAM_LENGTH` and iceberg-rust."""
    with pytest.raises(ValueError, match=f"expected at least {_MIN_STREAM_LENGTH} bytes, got {length}"):
        decrypt((JAVA_HEADER + bytes(_BLOCK_OVERHEAD))[:length])


def test_decrypt_rejects_the_wrong_magic() -> None:
    with pytest.raises(ValueError, match="magic b'AGS2' does not match b'AGS1'"):
        decrypt(b"AGS2" + JAVA_HEADER[4:] + bytes(_BLOCK_OVERHEAD))


def test_decrypt_rejects_an_unsupported_block_size() -> None:
    with pytest.raises(ValueError, match=f"Unsupported AGS1 block size: 512 \\(expected {_PLAIN_BLOCK_SIZE}\\)"):
        decrypt(b"AGS1" + (512).to_bytes(4, "little") + bytes(_BLOCK_OVERHEAD))


@pytest.mark.parametrize("last_block_length", [1, _BLOCK_OVERHEAD - 1])
def test_decrypt_rejects_a_truncated_last_block(last_block_length: int) -> None:
    with pytest.raises(ValueError, match=f"last block is {last_block_length} bytes, expected at least {_BLOCK_OVERHEAD}"):
        decrypt(JAVA_HEADER + bytes(_CIPHER_BLOCK_SIZE + last_block_length))


def test_decrypt_rejects_reordered_blocks() -> None:
    stream = _encrypt_ags1(bytes(2 * _PLAIN_BLOCK_SIZE), KEY_METADATA)
    first, second = (
        stream[_GCM_STREAM_HEADER_LENGTH : _GCM_STREAM_HEADER_LENGTH + _CIPHER_BLOCK_SIZE],
        stream[_GCM_STREAM_HEADER_LENGTH + _CIPHER_BLOCK_SIZE :],
    )

    with pytest.raises(ValueError, match="wrong decryption key; or corrupt/tampered data"):
        decrypt(JAVA_HEADER + second + first)


def test_decrypt_rejects_another_files_aad_prefix() -> None:
    stream = _encrypt_ags1(bytes(100), KEY_METADATA)

    with pytest.raises(ValueError, match="wrong decryption key; or corrupt/tampered data"):
        decrypt(stream, aad_prefix=b"another file's prefix")


def test_decrypt_rejects_the_wrong_key() -> None:
    stream = _encrypt_ags1(bytes(100), KEY_METADATA)

    with pytest.raises(ValueError, match="wrong decryption key; or corrupt/tampered data"):
        decrypt(stream, key=b"1123456789012345")
