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

import io

import pytest

from pyiceberg.avro.encoder import BinaryEncoder
from pyiceberg.encryption.key_metadata import KEY_METADATA_V1, StandardKeyMetadata

AES128_KEY = b"0123456789012345"

# Version byte, then the Avro-encoded encryption_key: zigzag length 16 followed by the key
ENCODED_PREFIX = b"\x01\x20" + AES128_KEY


def encode_key_metadata(encryption_key: bytes) -> bytes:
    """Encode key metadata directly, bypassing StandardKeyMetadata's validation."""
    output = io.BytesIO()
    encoder = BinaryEncoder(output)
    encoder.write(bytes([KEY_METADATA_V1]))
    encoder.write_bytes(encryption_key)
    encoder.write_int(0)
    encoder.write_int(0)
    return output.getvalue()


@pytest.mark.parametrize(
    "key_metadata, encoded",
    [
        (StandardKeyMetadata(encryption_key=AES128_KEY), ENCODED_PREFIX + b"\x00\x00"),
        (StandardKeyMetadata(encryption_key=AES128_KEY, aad_prefix=b"ad"), ENCODED_PREFIX + b"\x02\x04ad\x00"),
        (
            StandardKeyMetadata(encryption_key=AES128_KEY, aad_prefix=b"ad", file_length=1024),
            ENCODED_PREFIX + b"\x02\x04ad\x02\x80\x10",
        ),
        (StandardKeyMetadata(encryption_key=AES128_KEY, aad_prefix=b""), ENCODED_PREFIX + b"\x02\x00\x00"),
    ],
)
def test_key_metadata_serialization(key_metadata: StandardKeyMetadata, encoded: bytes) -> None:
    assert key_metadata.to_bytes() == encoded
    assert StandardKeyMetadata.from_bytes(encoded) == key_metadata


def test_key_metadata_defaults() -> None:
    key_metadata = StandardKeyMetadata(encryption_key=AES128_KEY)

    assert key_metadata.aad_prefix is None
    assert key_metadata.file_length is None


def test_key_metadata_repr_redacts_encryption_key() -> None:
    key_metadata = StandardKeyMetadata(encryption_key=AES128_KEY)

    assert "encryption_key" not in repr(key_metadata)
    assert repr(AES128_KEY) not in repr(key_metadata)


def test_key_metadata_empty_buffer() -> None:
    with pytest.raises(ValueError, match="Empty key metadata"):
        StandardKeyMetadata.from_bytes(b"")


@pytest.mark.parametrize("data", [b"\x02", b"\x02" + ENCODED_PREFIX[1:] + b"\x00\x00"])
def test_key_metadata_unsupported_version(data: bytes) -> None:
    with pytest.raises(ValueError, match="Unsupported key metadata version: 2"):
        StandardKeyMetadata.from_bytes(data)


@pytest.mark.parametrize("key_length", [16, 24, 32])
def test_key_metadata_accepts_aes_key_lengths(key_length: int) -> None:
    key_metadata = StandardKeyMetadata(encryption_key=bytes(key_length))

    assert StandardKeyMetadata.from_bytes(key_metadata.to_bytes()) == key_metadata


@pytest.mark.parametrize("key_length", [0, 4, 15, 20, 33])
def test_key_metadata_rejects_invalid_key_length(key_length: int) -> None:
    with pytest.raises(ValueError, match="Invalid encryption key in key metadata"):
        StandardKeyMetadata(encryption_key=bytes(key_length))


@pytest.mark.parametrize("key_length", [0, 4, 15, 20, 33])
def test_key_metadata_decode_rejects_invalid_key_length(key_length: int) -> None:
    with pytest.raises(ValueError, match="Invalid encryption key in key metadata"):
        StandardKeyMetadata.from_bytes(encode_key_metadata(bytes(key_length)))
