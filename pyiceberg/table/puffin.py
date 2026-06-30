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
from typing import TYPE_CHECKING

import zstandard
from pydantic import Field

from pyiceberg.typedef import IcebergBaseModel
from pyiceberg.utils.deprecated import deprecated

if TYPE_CHECKING:
    import pyarrow as pa

# Short for: Puffin Fratercula arctica, version 1
MAGIC_BYTES = b"PFA1"


class PuffinBlobMetadata(IcebergBaseModel):
    type: str = Field()
    fields: list[int] = Field()
    snapshot_id: int = Field(alias="snapshot-id")
    sequence_number: int = Field(alias="sequence-number")
    offset: int = Field()
    length: int = Field()
    compression_codec: str | None = Field(alias="compression-codec", default=None)
    properties: dict[str, str] = Field(default_factory=dict)


class Footer(IcebergBaseModel):
    blobs: list[PuffinBlobMetadata] = Field()
    properties: dict[str, str] = Field(default_factory=dict)


class PuffinFile:
    footer: Footer
    _file_bytes: bytes

    def __init__(self, puffin: bytes) -> None:
        for magic_bytes in [puffin[:4], puffin[-4:]]:
            if magic_bytes != MAGIC_BYTES:
                raise ValueError(f"Incorrect magic bytes, expected {MAGIC_BYTES!r}, got {magic_bytes!r}")

        # One flag is set, the rest should be zero
        # byte 0 (first)
        # - bit 0 (lowest bit): whether FooterPayload is compressed
        # - all other bits are reserved for future use and should be set to 0 on write
        flags = puffin[-8:-4]
        if flags[0] != 0:
            raise ValueError("The Puffin-file has a compressed footer, which is not yet supported")

        # 4 byte integer is always signed, in a two's complement representation, stored little-endian.
        footer_payload_size_int = int.from_bytes(puffin[-12:-8], byteorder="little")

        self.footer = Footer.model_validate_json(puffin[-(footer_payload_size_int + 12) : -12])
        self._file_bytes = puffin

    def get_blob_payload(self, blob: PuffinBlobMetadata) -> bytes:
        raw = self._file_bytes[blob.offset : blob.offset + blob.length]
        if blob.compression_codec is None:
            return raw
        if blob.compression_codec == "zstd":
            return zstandard.ZstdDecompressor().decompress(raw)
        raise ValueError(f"Unsupported compression codec: {blob.compression_codec!r}")

    @deprecated(deprecated_in="0.12.0", removed_in="0.13.0", help_message="Use deletion_vectors_from_puffin_file(...) instead")
    def to_vector(self) -> dict[str, "pa.ChunkedArray"]:
        from pyiceberg.table.deletion_vector import deletion_vectors_from_puffin_file  # local import avoids the cycle

        return {dv.referenced_data_file: dv.to_vector() for dv in deletion_vectors_from_puffin_file(self)}
