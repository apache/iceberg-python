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
import struct
import zlib
from os import path
from pathlib import Path

import pytest
from pyroaring import BitMap

from pyiceberg.io.pyarrow import PyArrowFileIO
from pyiceberg.manifest import DataFile, DataFileContent, FileFormat
from pyiceberg.table.deletion_vector import (
    _DV_BLOB_MAGIC_NUMBER,
    DELETION_VECTOR_MAGIC,
    PROPERTY_REFERENCED_DATA_FILE,
    ROW_POSITION_FIELD_ID,
    DeletionVector,
    _deserialize_dv_blob,
    _has_deletion_vector_content_reference,
    read_deletion_vectors,
)


def _open_file(file: str) -> bytes:
    cur_dir = path.dirname(path.realpath(__file__))
    with open(f"{cur_dir}/bitmaps/{file}", "rb") as f:
        return f.read()


def _dv_blob(bitmap_payload: bytes) -> bytes:
    bitmap_data = struct.pack("<I", _DV_BLOB_MAGIC_NUMBER) + bitmap_payload
    return struct.pack(">I", len(bitmap_data)) + bitmap_data + struct.pack(">I", zlib.crc32(bitmap_data) & 0xFFFFFFFF)


def _bitmap_payload() -> bytes:
    return (1).to_bytes(8, byteorder="little") + (0).to_bytes(4, byteorder="little") + BitMap([1, 3, 5]).serialize()


def test_deserialize_deletion_vector_blob() -> None:
    actual = _deserialize_dv_blob(_dv_blob(_bitmap_payload()), record_count=3)

    assert actual == [BitMap([1, 3, 5])]


def test_deserialize_deletion_vector_blob_invalid_length() -> None:
    with pytest.raises(ValueError, match="Invalid bitmap data length"):
        _deserialize_dv_blob(_dv_blob(_bitmap_payload())[:-1])


def test_deserialize_deletion_vector_blob_invalid_magic() -> None:
    bitmap_data = struct.pack("<I", _DV_BLOB_MAGIC_NUMBER + 1) + _bitmap_payload()
    blob = struct.pack(">I", len(bitmap_data)) + bitmap_data + struct.pack(">I", zlib.crc32(bitmap_data) & 0xFFFFFFFF)

    with pytest.raises(ValueError, match="Invalid magic number"):
        _deserialize_dv_blob(blob)


def test_deserialize_deletion_vector_blob_invalid_crc() -> None:
    blob = bytearray(_dv_blob(_bitmap_payload()))
    blob[-1] ^= 1

    with pytest.raises(ValueError, match="Invalid CRC"):
        _deserialize_dv_blob(bytes(blob))


def test_deserialize_deletion_vector_blob_invalid_cardinality() -> None:
    with pytest.raises(ValueError, match="Invalid cardinality"):
        _deserialize_dv_blob(_dv_blob(_bitmap_payload()), record_count=4)


def _data_file(
    content_offset: int | None = None,
    content_size_in_bytes: int | None = None,
    referenced_data_file: str | None = None,
) -> DataFile:
    return DataFile.from_args(
        _table_format_version=3,
        content=DataFileContent.POSITION_DELETES,
        file_path="deletes.puffin",
        file_format=FileFormat.PUFFIN,
        record_count=1,
        content_offset=content_offset,
        content_size_in_bytes=content_size_in_bytes,
        referenced_data_file=referenced_data_file,
    )


def test_has_deletion_vector_content_reference() -> None:
    assert not _has_deletion_vector_content_reference(_data_file())
    assert _has_deletion_vector_content_reference(_data_file(content_offset=0))
    assert _has_deletion_vector_content_reference(_data_file(content_size_in_bytes=1))
    assert _has_deletion_vector_content_reference(_data_file(referenced_data_file="data.parquet"))


def test_read_external_deletion_vector_content_range() -> None:
    """Read an externally generated Iceberg DV blob from a .bin content range."""
    fixture_path = Path(__file__).parent / "deletion_vector" / "v1" / "deletion-vector.bin"
    # The V3 manifest points past the one-byte file prefix to the complete DV blob.
    dv = DataFile.from_args(
        _table_format_version=3,
        content=DataFileContent.POSITION_DELETES,
        file_path=str(fixture_path),
        file_format=FileFormat.PUFFIN,
        record_count=3,
        content_offset=1,
        content_size_in_bytes=46,
        referenced_data_file="data.parquet",
    )

    deletion_vectors = read_deletion_vectors(PyArrowFileIO(), dv)

    assert len(deletion_vectors) == 1
    assert deletion_vectors[0].referenced_data_file == "data.parquet"
    assert deletion_vectors[0].to_vector().to_pylist() == [1, 4, 5]


def test_map_empty() -> None:
    puffin = _open_file("64mapempty.bin")

    expected: list[BitMap] = []
    actual = DeletionVector._deserialize_bitmap(puffin)

    assert expected == actual


def test_map_bitvals() -> None:
    puffin = _open_file("64map32bitvals.bin")

    expected = [BitMap([0, 1, 2, 3, 4, 5, 6, 7, 8, 9])]
    actual = DeletionVector._deserialize_bitmap(puffin)

    assert expected == actual


def test_map_spread_vals() -> None:
    puffin = _open_file("64mapspreadvals.bin")

    expected = [
        BitMap([0, 1, 2, 3, 4, 5, 6, 7, 8, 9]),
        BitMap([0, 1, 2, 3, 4, 5, 6, 7, 8, 9]),
        BitMap([0, 1, 2, 3, 4, 5, 6, 7, 8, 9]),
        BitMap([0, 1, 2, 3, 4, 5, 6, 7, 8, 9]),
        BitMap([0, 1, 2, 3, 4, 5, 6, 7, 8, 9]),
        BitMap([0, 1, 2, 3, 4, 5, 6, 7, 8, 9]),
        BitMap([0, 1, 2, 3, 4, 5, 6, 7, 8, 9]),
        BitMap([0, 1, 2, 3, 4, 5, 6, 7, 8, 9]),
        BitMap([0, 1, 2, 3, 4, 5, 6, 7, 8, 9]),
        BitMap([0, 1, 2, 3, 4, 5, 6, 7, 8, 9]),
    ]
    actual = DeletionVector._deserialize_bitmap(puffin)

    assert expected == actual


def test_map_high_vals() -> None:
    puffin = _open_file("64maphighvals.bin")

    with pytest.raises(ValueError, match="Key 4022190063 is too large, max 2147483647 to maintain compatibility with Java impl"):
        _ = DeletionVector._deserialize_bitmap(puffin)


@pytest.mark.parametrize(
    "positions",
    [
        [1, 2, 3],
        [0],
        [3, 1, 2, 1, 3],  # unordered with duplicates
        [0, 1, 5, (1 << 32) + 7, (2 << 32) + 4],  # spread across multiple bitmap keys
    ],
)
def test_serialize_bitmap_round_trips(positions: list[int]) -> None:
    dv = DeletionVector.from_positions("file.parquet", positions)

    serialized = DeletionVector._serialize_bitmap(dv._bitmaps)
    assert DeletionVector._serialize_bitmap(DeletionVector._deserialize_bitmap(serialized)) == serialized
    assert dv.to_vector().to_pylist() == sorted(set(positions))


def test_from_positions_rejects_negative() -> None:
    with pytest.raises(ValueError, match="Invalid position: -1, positions must be non-negative"):
        DeletionVector.from_positions("file.parquet", [1, -1, 2])


def test_from_positions_rejects_empty() -> None:
    with pytest.raises(ValueError, match="Deletion vector must contain at least one position"):
        DeletionVector.from_positions("file.parquet", [])


def test_to_blob_metadata() -> None:
    blob = DeletionVector.from_positions("s3://bucket/file.parquet", [1, 2, 3, 3]).to_blob()

    assert blob.metadata.type == "deletion-vector-v1"
    assert blob.metadata.fields == [ROW_POSITION_FIELD_ID]
    assert blob.metadata.properties[PROPERTY_REFERENCED_DATA_FILE] == "s3://bucket/file.parquet"
    # duplicates collapse, so cardinality counts distinct positions
    assert blob.metadata.properties["cardinality"] == "3"
    # offset and length are placeholders until PuffinWriter assembles the file
    assert blob.metadata.offset == 0
    assert blob.metadata.length == 0


def test_to_blob_payload_layout() -> None:
    blob = DeletionVector.from_positions("file.parquet", [1, 2, 3]).to_blob()

    # Layout: length (4B big-endian) | DV magic (4B) | vector | CRC-32 (4B big-endian),
    # where the length and CRC-32 both cover the magic bytes plus the vector.
    length_prefix = int.from_bytes(blob.payload[0:4], "big")
    assert blob.payload[4:8] == DELETION_VECTOR_MAGIC
    vector = blob.payload[8 : 4 + length_prefix]
    assert length_prefix == len(DELETION_VECTOR_MAGIC) + len(vector)
    assert len(blob.payload) == 4 + length_prefix + 4
    assert DeletionVector._deserialize_bitmap(vector) == DeletionVector.from_positions("file.parquet", [1, 2, 3])._bitmaps
