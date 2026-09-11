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
"""Key metadata for encrypted manifests, manifest lists and data files."""

from __future__ import annotations

import io
from dataclasses import dataclass

from pyiceberg.avro.decoder import new_decoder
from pyiceberg.avro.encoder import BinaryEncoder
from pyiceberg.avro.resolver import construct_reader, construct_writer
from pyiceberg.schema import Schema
from pyiceberg.typedef import Record
from pyiceberg.types import BinaryType, LongType, NestedField

KEY_METADATA_V1 = 1

AES_KEY_LENGTHS = (16, 24, 32)

KEY_METADATA_SCHEMA_V1 = Schema(
    NestedField(field_id=0, name="encryption_key", field_type=BinaryType(), required=True),
    NestedField(field_id=1, name="aad_prefix", field_type=BinaryType(), required=False),
    NestedField(field_id=2, name="file_length", field_type=LongType(), required=False),
)


@dataclass(frozen=True)
class StandardKeyMetadata:
    """The key and AAD prefix needed to decrypt a single file.

    Wire format is a version byte followed by an Avro datum of `KEY_METADATA_SCHEMA_V1`,
    byte-compatible with Java's `StandardKeyMetadata`.
    """

    encryption_key: bytes
    aad_prefix: bytes | None = None
    file_length: int | None = None

    def __post_init__(self) -> None:
        """Reject invalid key lengths here rather than only on decode, so an invalid instance cannot exist."""
        if len(self.encryption_key) not in AES_KEY_LENGTHS:
            raise ValueError(
                f"Invalid encryption key in key metadata: expected one of {AES_KEY_LENGTHS} bytes, got {len(self.encryption_key)}"
            )

    @classmethod
    def from_bytes(cls, data: bytes) -> StandardKeyMetadata:
        """Decode key metadata from its wire format."""
        if not data:
            raise ValueError("Empty key metadata")

        if (version := data[0]) != KEY_METADATA_V1:
            raise ValueError(f"Unsupported key metadata version: {version}")

        record = construct_reader(KEY_METADATA_SCHEMA_V1).read(new_decoder(data[1:]))
        return cls(encryption_key=record[0], aad_prefix=record[1], file_length=record[2])

    def to_bytes(self) -> bytes:
        """Encode key metadata to its wire format."""
        output = io.BytesIO()
        encoder = BinaryEncoder(output)
        encoder.write(bytes([KEY_METADATA_V1]))
        record = Record(self.encryption_key, self.aad_prefix, self.file_length)
        construct_writer(KEY_METADATA_SCHEMA_V1).write(encoder, record)
        return output.getvalue()
