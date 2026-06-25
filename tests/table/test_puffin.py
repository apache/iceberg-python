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
from pathlib import Path

import pytest

from pyiceberg import __version__
from pyiceberg.io.pyarrow import PyArrowFileIO
from pyiceberg.table.deletion_vector import DeletionVector, deletion_vectors_from_puffin_file
from pyiceberg.table.puffin import MAGIC_BYTES, PuffinFile, PuffinWriter


def _open_file(file: str) -> bytes:
    cur_dir = path.dirname(path.realpath(__file__))
    with open(f"{cur_dir}/puffin/{file}", "rb") as f:
        return f.read()


def test_read_empty_uncompressed() -> None:
    puffin_bytes = _open_file("v1/empty-puffin-uncompressed.bin")
    pf = PuffinFile(puffin_bytes)

    assert pf.footer.blobs == []
    assert pf.footer.properties == {}


def test_read_two_blobs_uncompressed() -> None:
    puffin_bytes = _open_file("v1/sample-metric-data-uncompressed.bin")
    pf = PuffinFile(puffin_bytes)

    assert pf.footer.properties == {"created-by": "Test 1234"}
    assert len(pf.footer.blobs) == 2

    blob1 = pf.footer.blobs[0]
    assert blob1.type == "some-blob"
    assert blob1.fields == [1]
    assert blob1.snapshot_id == 2
    assert blob1.sequence_number == 1
    assert blob1.compression_codec is None
    assert pf.get_blob_payload(blob1) == b"abcdefghi"

    blob2 = pf.footer.blobs[1]
    assert blob2.type == "some-other-blob"
    assert blob2.fields == [2]
    assert blob2.compression_codec is None
    assert pf.get_blob_payload(blob2) == (
        b"some blob \x00 binary data \xf0\x9f\xa4\xaf that is not very very very very very very long, is it?"
    )


def _write(tmp_path: Path, *deletion_vectors: DeletionVector, created_by: str | None = None) -> Path:
    puffin_path = tmp_path / "test.puffin"
    with PuffinWriter(PyArrowFileIO().new_output(str(puffin_path)), created_by=created_by) as writer:
        for dv in deletion_vectors:
            writer.add_blob(dv.to_blob())
    return puffin_path


def test_puffin_writer_round_trips_single_blob(tmp_path: Path) -> None:
    positions = [0, 1, 5, (1 << 32) + 7]
    puffin_path = _write(tmp_path, DeletionVector.from_positions("file.parquet", positions))

    reader = PuffinFile(puffin_path.read_bytes())
    dvs = deletion_vectors_from_puffin_file(reader)

    assert len(dvs) == 1
    assert dvs[0].referenced_data_file == "file.parquet"
    assert dvs[0].to_vector().to_pylist() == sorted(positions)


def test_puffin_writer_round_trips_multiple_blobs(tmp_path: Path) -> None:
    puffin_path = _write(
        tmp_path,
        DeletionVector.from_positions("file1.parquet", [1, 2, 3]),
        DeletionVector.from_positions("file2.parquet", [4, 5, 6]),
    )

    reader = PuffinFile(puffin_path.read_bytes())
    dvs = deletion_vectors_from_puffin_file(reader)

    assert {dv.referenced_data_file: dv.to_vector().to_pylist() for dv in dvs} == {
        "file1.parquet": [1, 2, 3],
        "file2.parquet": [4, 5, 6],
    }


def test_puffin_writer_writes_magic_bytes_and_offsets(tmp_path: Path) -> None:
    puffin_path = _write(tmp_path, DeletionVector.from_positions("file.parquet", [1, 2, 3]))
    puffin_bytes = puffin_path.read_bytes()

    assert puffin_bytes[:4] == MAGIC_BYTES
    assert puffin_bytes[-4:] == MAGIC_BYTES

    blob = PuffinFile(puffin_bytes).footer.blobs[0]
    # PuffinWriter fills in the placeholder offset and length while assembling the file
    assert blob.offset > 0
    assert blob.length > 0


def test_puffin_writer_default_created_by(tmp_path: Path) -> None:
    puffin_path = _write(tmp_path, DeletionVector.from_positions("file.parquet", [1]))

    reader = PuffinFile(puffin_path.read_bytes())
    assert reader.footer.properties["created-by"] == f"PyIceberg version {__version__}"


def test_puffin_writer_custom_created_by(tmp_path: Path) -> None:
    puffin_path = _write(tmp_path, DeletionVector.from_positions("file.parquet", [1]), created_by="my-test-app")

    reader = PuffinFile(puffin_path.read_bytes())
    assert reader.footer.properties["created-by"] == "my-test-app"


def test_puffin_writer_file_size_via_output_file(tmp_path: Path) -> None:
    puffin_path = tmp_path / "test.puffin"
    output_file = PyArrowFileIO().new_output(str(puffin_path))
    with PuffinWriter(output_file) as writer:
        writer.add_blob(DeletionVector.from_positions("file.parquet", [1, 2, 3]).to_blob())

    assert len(output_file) == len(puffin_path.read_bytes())


def test_puffin_writer_empty(tmp_path: Path) -> None:
    puffin_path = _write(tmp_path)

    reader = PuffinFile(puffin_path.read_bytes())
    assert reader.footer.blobs == []
    assert deletion_vectors_from_puffin_file(reader) == []


def test_add_blob_to_closed_writer_raises(tmp_path: Path) -> None:
    output_file = PyArrowFileIO().new_output(str(tmp_path / "test.puffin"))
    writer = PuffinWriter(output_file)
    with writer:
        writer.add_blob(DeletionVector.from_positions("file.parquet", [1]).to_blob())

    with pytest.raises(RuntimeError, match="Cannot add blob to closed Puffin writer"):
        writer.add_blob(DeletionVector.from_positions("file.parquet", [2]).to_blob())
