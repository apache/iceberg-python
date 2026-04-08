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
"""StandardKeyMetadata Avro serialization.

Wire format: ``0x01 version byte || Avro-encoded fields``

Avro schema:
  - encryption_key: bytes (required)
  - aad_prefix: union[null, bytes] (optional)
  - file_length: union[null, long] (optional)
"""

from __future__ import annotations

from dataclasses import dataclass

V1 = 0x01


def _read_avro_long(data: bytes, offset: int) -> tuple[int, int]:
    """Read a zigzag-encoded Avro long from data at offset. Returns (value, new_offset)."""
    result = 0
    shift = 0
    while True:
        if offset >= len(data):
            raise ValueError("Unexpected end of Avro data reading long")
        b = data[offset]
        offset += 1
        result |= (b & 0x7F) << shift
        if (b & 0x80) == 0:
            break
        shift += 7
    # Zigzag decode
    return (result >> 1) ^ -(result & 1), offset


def _read_avro_bytes(data: bytes, offset: int) -> tuple[bytes, int]:
    """Read Avro bytes (length-prefixed). Returns (bytes_value, new_offset)."""
    length, offset = _read_avro_long(data, offset)
    if length < 0:
        raise ValueError(f"Negative Avro bytes length: {length}")
    end = offset + length
    if end > len(data):
        raise ValueError("Unexpected end of Avro data reading bytes")
    return data[offset:end], end


@dataclass(frozen=True)
class StandardKeyMetadata:
    """Standard key metadata for Iceberg table encryption.

    Contains the plaintext encryption key (DEK), AAD prefix, and optional file length.
    """

    encryption_key: bytes
    aad_prefix: bytes = b""
    file_length: int | None = None

    @staticmethod
    def deserialize(data: bytes) -> StandardKeyMetadata:
        """Deserialize from wire format: ``0x01 version || Avro-encoded fields``."""
        if not data:
            raise ValueError("Empty key metadata buffer")

        version = data[0]
        if version != V1:
            raise ValueError(f"Unsupported key metadata version: {version}")

        offset = 1

        # Read encryption_key (required bytes)
        encryption_key, offset = _read_avro_bytes(data, offset)

        # Read aad_prefix (optional: union[null, bytes])
        union_index, offset = _read_avro_long(data, offset)
        if union_index == 0:
            aad_prefix = b""
        elif union_index == 1:
            aad_prefix, offset = _read_avro_bytes(data, offset)
        else:
            raise ValueError(f"Invalid union index for aad_prefix: {union_index}")

        # Read file_length (optional: union[null, long])
        file_length = None
        if offset < len(data):
            union_index, offset = _read_avro_long(data, offset)
            if union_index == 0:
                file_length = None
            elif union_index == 1:
                file_length, offset = _read_avro_long(data, offset)
            else:
                raise ValueError(f"Invalid union index for file_length: {union_index}")

        return StandardKeyMetadata(
            encryption_key=encryption_key,
            aad_prefix=aad_prefix,
            file_length=file_length,
        )

    def serialize(self) -> bytes:
        """Serialize to wire format: ``0x01 version || Avro-encoded fields``."""
        parts = [bytes([V1])]

        # encryption_key (required bytes)
        parts.append(_encode_avro_bytes(self.encryption_key))

        # aad_prefix (union[null, bytes])
        if self.aad_prefix:
            parts.append(_encode_avro_long(1))  # union index 1 = bytes
            parts.append(_encode_avro_bytes(self.aad_prefix))
        else:
            parts.append(_encode_avro_long(0))  # union index 0 = null

        # file_length (union[null, long])
        if self.file_length is not None:
            parts.append(_encode_avro_long(1))  # union index 1 = long
            parts.append(_encode_avro_long(self.file_length))
        else:
            parts.append(_encode_avro_long(0))  # union index 0 = null

        return b"".join(parts)


def _encode_avro_long(value: int) -> bytes:
    """Encode a long as zigzag-encoded Avro varint."""
    # Zigzag encode
    n = (value << 1) ^ (value >> 63)
    result = bytearray()
    while n & ~0x7F:
        result.append((n & 0x7F) | 0x80)
        n >>= 7
    result.append(n & 0x7F)
    return bytes(result)


def _encode_avro_bytes(data: bytes) -> bytes:
    """Encode bytes with Avro length prefix."""
    return _encode_avro_long(len(data)) + data
