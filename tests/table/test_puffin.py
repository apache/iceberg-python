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

from pyiceberg.table.puffin import PROPERTY_REFERENCED_DATA_FILE, PuffinFile, PuffinWriter, _deserialize_bitmap


def _open_file(file: str) -> bytes:
    cur_dir = path.dirname(path.realpath(__file__))
    with open(f"{cur_dir}/bitmaps/{file}", "rb") as f:
        return f.read()


def test_map_empty() -> None:
    puffin = _open_file("64mapempty.bin")

    expected: list[BitMap] = []
    actual = _deserialize_bitmap(puffin)

    assert expected == actual


def test_map_bitvals() -> None:
    puffin = _open_file("64map32bitvals.bin")

    expected = [BitMap([0, 1, 2, 3, 4, 5, 6, 7, 8, 9])]
    actual = _deserialize_bitmap(puffin)

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
    actual = _deserialize_bitmap(puffin)

    assert expected == actual


def test_map_high_vals() -> None:
    puffin = _open_file("64maphighvals.bin")

    with pytest.raises(ValueError, match="Key 4022190063 is too large, max 2147483647 to maintain compatibility with Java impl"):
        _ = _deserialize_bitmap(puffin)


def test_puffin_round_trip() -> None:
    # Define some deletion positions for multiple files
    deletions1 = [10, 20, 30]
    deletions2 = [5, (1 << 32) + 1]  # Test with a high-bit position

    file1_path = "path/to/data1.parquet"
    file2_path = "path/to/data2.parquet"

    # Write the Puffin file
    writer = PuffinWriter()
    writer.add(positions=deletions1, referenced_data_file=file1_path)
    writer.add(positions=deletions2, referenced_data_file=file2_path)
    puffin_bytes = writer.finish()

    # Read the Puffin file back
    reader = PuffinFile(puffin_bytes)

    # Assert footer metadata
    assert len(reader.footer.blobs) == 2

    blob1_meta = reader.footer.blobs[0]
    assert blob1_meta.properties[PROPERTY_REFERENCED_DATA_FILE] == file1_path
    assert blob1_meta.properties["cardinality"] == str(len(deletions1))

    blob2_meta = reader.footer.blobs[1]
    assert blob2_meta.properties[PROPERTY_REFERENCED_DATA_FILE] == file2_path
    assert blob2_meta.properties["cardinality"] == str(len(deletions2))

    # Assert the content of deletion vectors
    read_vectors = reader.to_vector()

    assert file1_path in read_vectors
    assert file2_path in read_vectors

    assert read_vectors[file1_path].to_pylist() == sorted(deletions1)
    assert read_vectors[file2_path].to_pylist() == sorted(deletions2)


def test_write_and_read_puffin_file() -> None:
    writer = PuffinWriter()
    writer.add(positions=[1, 2, 3], referenced_data_file="file1.parquet")
    writer.add(positions=[4, 5, 6], referenced_data_file="file2.parquet")
    puffin_bytes = writer.finish()

    reader = PuffinFile(puffin_bytes)

    assert len(reader.footer.blobs) == 2
    blob1 = reader.footer.blobs[0]
    blob2 = reader.footer.blobs[1]

    assert blob1.properties["referenced-data-file"] == "file1.parquet"
    assert blob1.properties["cardinality"] == "3"
    assert blob1.type == "deletion-vector-v1"
    assert blob1.snapshot_id == -1
    assert blob1.sequence_number == -1
    assert blob1.compression_codec is None

    assert blob2.properties["referenced-data-file"] == "file2.parquet"
    assert blob2.properties["cardinality"] == "3"

    vectors = reader.to_vector()
    assert len(vectors) == 2
    assert vectors["file1.parquet"].to_pylist() == [1, 2, 3]
    assert vectors["file2.parquet"].to_pylist() == [4, 5, 6]


def test_puffin_file_with_no_blobs() -> None:
    writer = PuffinWriter()
    puffin_bytes = writer.finish()

    reader = PuffinFile(puffin_bytes)
    assert len(reader.footer.blobs) == 0
    assert len(reader.to_vector()) == 0
