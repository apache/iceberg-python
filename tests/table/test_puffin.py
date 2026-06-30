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

from pyiceberg.table.puffin import PuffinFile


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
