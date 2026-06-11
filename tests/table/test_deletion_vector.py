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

import pytest
from pyroaring import BitMap

from pyiceberg.table.deletion_vector import _DV_BLOB_MAGIC_NUMBER, DeletionVector, _deserialize_dv_blob


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
