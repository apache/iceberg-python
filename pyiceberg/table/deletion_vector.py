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
import math
import struct
import zlib
from typing import TYPE_CHECKING

from pyroaring import BitMap, FrozenBitMap

from pyiceberg.table.puffin import PuffinFile

if TYPE_CHECKING:
    import pyarrow as pa

    from pyiceberg.io import FileIO
    from pyiceberg.manifest import DataFile

EMPTY_BITMAP = FrozenBitMap()
MAX_JAVA_SIGNED = int(math.pow(2, 31)) - 1
PROPERTY_REFERENCED_DATA_FILE = "referenced-data-file"
_MAX_DELETION_VECTOR_CONTENT_SIZE = 2**31 - 1
_DV_BLOB_LENGTH = struct.Struct(">I")
_DV_BLOB_MAGIC = struct.Struct("<I")
_DV_BLOB_CRC = struct.Struct(">I")
_DV_BLOB_MAGIC_NUMBER = 1681511377
_ROARING_BITMAP_COUNT_SIZE_BYTES = 8
_DV_BLOB_MIN_SIZE_BYTES = _DV_BLOB_LENGTH.size + _DV_BLOB_MAGIC.size + _ROARING_BITMAP_COUNT_SIZE_BYTES + _DV_BLOB_CRC.size


class DeletionVector:
    referenced_data_file: str
    _bitmaps: list[BitMap]

    def __init__(self, referenced_data_file: str, bitmaps: list[BitMap]) -> None:
        self.referenced_data_file = referenced_data_file
        self._bitmaps = bitmaps

    @staticmethod
    def _deserialize_bitmap(pl: bytes) -> list[BitMap]:
        number_of_bitmaps = int.from_bytes(pl[0:8], byteorder="little")
        pl = pl[8:]

        bitmaps = []
        last_key = -1
        for _ in range(number_of_bitmaps):
            key = int.from_bytes(pl[0:4], byteorder="little")
            if key < 0:
                raise ValueError(f"Invalid unsigned key: {key}")
            if key <= last_key:
                raise ValueError("Keys must be sorted in ascending order")
            if key > MAX_JAVA_SIGNED:
                raise ValueError(f"Key {key} is too large, max {MAX_JAVA_SIGNED} to maintain compatibility with Java impl")
            pl = pl[4:]

            while last_key < key - 1:
                bitmaps.append(EMPTY_BITMAP)
                last_key += 1

            bm = BitMap().deserialize(pl)
            # TODO: Optimize this
            pl = pl[len(bm.serialize()) :]
            bitmaps.append(bm)

            last_key = key

        return bitmaps

    @staticmethod
    def _bitmaps_to_chunked_array(bitmaps: list[BitMap]) -> "pa.ChunkedArray":
        import pyarrow as pa

        return pa.chunked_array([(key_pos << 32) + pos for pos in bitmap] for key_pos, bitmap in enumerate(bitmaps))

    def to_vector(self) -> "pa.ChunkedArray":
        return self._bitmaps_to_chunked_array(self._bitmaps)


def _deserialize_dv_blob(blob: bytes, record_count: int | None = None) -> list[BitMap]:
    # The DV blob encoding matches Iceberg Java's BitmapPositionDeleteIndex:
    # 4-byte big-endian bitmap-data length, 4-byte little-endian magic number,
    # portable Roaring bitmap data, and 4-byte big-endian CRC-32.
    if len(blob) < _DV_BLOB_MIN_SIZE_BYTES:
        raise ValueError(f"Invalid deletion vector blob length: {len(blob)}")

    bitmap_data_length = _DV_BLOB_LENGTH.unpack_from(blob)[0]
    expected_bitmap_data_length = len(blob) - _DV_BLOB_LENGTH.size - _DV_BLOB_CRC.size
    if bitmap_data_length != expected_bitmap_data_length:
        raise ValueError(f"Invalid bitmap data length: {bitmap_data_length}, expected {expected_bitmap_data_length}")

    bitmap_data_offset = _DV_BLOB_LENGTH.size
    crc_offset = bitmap_data_offset + bitmap_data_length
    bitmap_data = blob[bitmap_data_offset:crc_offset]

    magic_number = _DV_BLOB_MAGIC.unpack_from(bitmap_data)[0]
    if magic_number != _DV_BLOB_MAGIC_NUMBER:
        raise ValueError(f"Invalid magic number: {magic_number}, expected {_DV_BLOB_MAGIC_NUMBER}")

    checksum = zlib.crc32(bitmap_data) & 0xFFFFFFFF
    expected_checksum = _DV_BLOB_CRC.unpack_from(blob, crc_offset)[0]
    if checksum != expected_checksum:
        raise ValueError("Invalid CRC")

    bitmaps = DeletionVector._deserialize_bitmap(bitmap_data[_DV_BLOB_MAGIC.size :])
    if record_count is not None:
        cardinality = sum(len(bitmap) for bitmap in bitmaps)
        if cardinality != record_count:
            raise ValueError(f"Invalid cardinality: {cardinality}, expected {record_count}")

    return bitmaps


def _validate_deletion_vector_content(data_file: "DataFile") -> tuple[int, int, str]:
    content_offset = data_file.content_offset
    content_size_in_bytes = data_file.content_size_in_bytes
    referenced_data_file = data_file.referenced_data_file

    if content_offset is None:
        raise ValueError(f"Invalid deletion vector, content offset is missing: {data_file.file_path}")
    if content_size_in_bytes is None:
        raise ValueError(f"Invalid deletion vector, content size is missing: {data_file.file_path}")
    if content_offset < 0:
        raise ValueError(f"Invalid deletion vector, content offset cannot be negative: {content_offset}")
    if content_size_in_bytes < 0:
        raise ValueError(f"Invalid deletion vector, content size cannot be negative: {content_size_in_bytes}")
    if content_size_in_bytes > _MAX_DELETION_VECTOR_CONTENT_SIZE:
        raise ValueError(f"Cannot read deletion vector larger than 2GB: {content_size_in_bytes}")
    if referenced_data_file is None:
        raise ValueError(f"Invalid deletion vector, referenced data file is missing: {data_file.file_path}")

    return content_offset, content_size_in_bytes, referenced_data_file


def read_deletion_vector(io: "FileIO", data_file: "DataFile") -> DeletionVector:
    content_offset, content_size_in_bytes, referenced_data_file = _validate_deletion_vector_content(data_file)

    with io.new_input(data_file.file_path).open() as fi:
        fi.seek(content_offset)
        payload = fi.read(content_size_in_bytes)

    if len(payload) != content_size_in_bytes:
        raise ValueError(f"Could not read deletion vector, expected {content_size_in_bytes} bytes, got {len(payload)}")

    return DeletionVector(
        referenced_data_file=referenced_data_file,
        bitmaps=_deserialize_dv_blob(payload, data_file.record_count),
    )


def deletion_vectors_from_puffin_file(puffin_file: PuffinFile) -> list[DeletionVector]:
    return [
        DeletionVector(
            referenced_data_file=blob.properties[PROPERTY_REFERENCED_DATA_FILE],
            bitmaps=DeletionVector._deserialize_bitmap(puffin_file.get_blob_payload(blob)),
        )
        for blob in puffin_file.footer.blobs
    ]
