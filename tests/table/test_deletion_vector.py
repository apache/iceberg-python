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
from os import path

import pytest
from pyroaring import BitMap

from pyiceberg.table.deletion_vector import (
    DELETION_VECTOR_MAGIC,
    MAX_POSITION,
    PROPERTY_REFERENCED_DATA_FILE,
    ROW_POSITION_FIELD_ID,
    DeletionVector,
)


def _open_file(file: str) -> bytes:
    cur_dir = path.dirname(path.realpath(__file__))
    with open(f"{cur_dir}/bitmaps/{file}", "rb") as f:
        return f.read()


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
    with pytest.raises(ValueError, match=f"Invalid position: -1, must be between 0 and {MAX_POSITION}"):
        DeletionVector.from_positions("file.parquet", [1, -1, 2])


def test_from_positions_rejects_out_of_range() -> None:
    too_large = MAX_POSITION + 1
    with pytest.raises(ValueError, match=f"Invalid position: {too_large}, must be between 0 and {MAX_POSITION}"):
        DeletionVector.from_positions("file.parquet", [1, too_large, 2])


def test_serialize_bitmap_run_optimizes_contiguous_run() -> None:
    # A long contiguous run serializes compactly thanks to run-length encoding and still round-trips.
    positions = list(range(100_000))
    dv = DeletionVector.from_positions("file.parquet", positions)

    serialized = DeletionVector._serialize_bitmap(dv._bitmaps)
    assert len(serialized) < 1_000  # ~16 KB without run optimization
    assert DeletionVector._deserialize_bitmap(serialized) == dv._bitmaps


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
