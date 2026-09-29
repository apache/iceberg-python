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
import json
from os import path

import pytest
from datasketches import compact_theta_sketch, update_theta_sketch

from pyiceberg.table.puffin import MAGIC_BYTES, PuffinFile
from pyiceberg.table.theta_sketch import ThetaSketch, theta_sketches_from_puffin_file


def _open_fixture(file: str) -> bytes:
    cur_dir = path.dirname(path.realpath(__file__))
    with open(f"{cur_dir}/puffin/v1/{file}", "rb") as f:
        return f.read()


def _make_sketch(values: list[int]) -> compact_theta_sketch:
    ts = update_theta_sketch()
    for v in values:
        ts.update(v)
    return ts.compact()


@pytest.fixture
def empty_sketch_bytes() -> bytes:
    return update_theta_sketch().compact().serialize()


@pytest.fixture
def three_value_sketch_bytes() -> bytes:
    return _make_sketch([1, 2, 3]).serialize()


def test_empty_sketch(empty_sketch_bytes: bytes) -> None:
    sketch = compact_theta_sketch.deserialize(empty_sketch_bytes)
    ts = ThetaSketch(field_id=1, sketch=sketch)

    assert ts.is_empty()
    assert ts.get_estimate() == 0.0


def test_sketch_estimate(three_value_sketch_bytes: bytes) -> None:
    sketch = compact_theta_sketch.deserialize(three_value_sketch_bytes)
    ts = ThetaSketch(field_id=1, sketch=sketch)

    assert not ts.is_empty()
    assert ts.get_estimate() == pytest.approx(3.0)
    assert not ts.is_estimation_mode()


def test_sketch_bounds_exact_mode(three_value_sketch_bytes: bytes) -> None:
    sketch = compact_theta_sketch.deserialize(three_value_sketch_bytes)
    ts = ThetaSketch(field_id=1, sketch=sketch)

    assert ts.get_lower_bound(1) == pytest.approx(3.0)
    assert ts.get_upper_bound(1) == pytest.approx(3.0)


def test_sketch_field_id() -> None:
    sketch = _make_sketch([10, 20, 30])
    ts = ThetaSketch(field_id=42, sketch=sketch)

    assert ts.field_id == 42


def test_sketch_property() -> None:
    sketch = _make_sketch([1, 2])
    ts = ThetaSketch(field_id=1, sketch=sketch)

    assert ts.sketch is sketch


def test_estimation_mode() -> None:
    ts_builder = update_theta_sketch(lg_k=5)
    for i in range(100):
        ts_builder.update(i)
    sketch = ts_builder.compact()
    ts = ThetaSketch(field_id=1, sketch=sketch)

    assert ts.is_estimation_mode()
    assert ts.get_estimate() > 0
    assert ts.get_lower_bound(1) <= ts.get_estimate()
    assert ts.get_upper_bound(1) >= ts.get_estimate()


def _build_puffin_file(blob_bytes: bytes, field_ids: list[int], snapshot_id: int = 1) -> bytes:
    # Puffin layout: magic(4) + blobs + footer_json + footer_size(4) + flags(4) + magic(4)
    # Blob offsets are file-absolute; first blob starts immediately after the 4-byte magic.
    blob_offset = 4
    footer = {
        "blobs": [
            {
                "type": "apache-datasketches-theta-v1",
                "snapshot-id": snapshot_id,
                "sequence-number": 1,
                "fields": field_ids,
                "offset": blob_offset,
                "length": len(blob_bytes),
            }
        ],
        "properties": {},
    }
    footer_json = json.dumps(footer, separators=(",", ":")).encode("utf-8")
    footer_size_bytes = len(footer_json).to_bytes(4, byteorder="little")
    flags = b"\x00\x00\x00\x00"
    return MAGIC_BYTES + blob_bytes + footer_json + footer_size_bytes + flags + MAGIC_BYTES


def test_theta_sketches_from_puffin_file_single_field(three_value_sketch_bytes: bytes) -> None:
    puffin_bytes = _build_puffin_file(three_value_sketch_bytes, field_ids=[5])
    puffin_file = PuffinFile(puffin_bytes)

    sketches = theta_sketches_from_puffin_file(puffin_file)

    assert len(sketches) == 1
    assert sketches[0].field_id == 5
    assert sketches[0].get_estimate() == pytest.approx(3.0)


def test_theta_sketches_from_puffin_file_multiple_fields(three_value_sketch_bytes: bytes) -> None:
    puffin_bytes = _build_puffin_file(three_value_sketch_bytes, field_ids=[1, 2, 3])
    puffin_file = PuffinFile(puffin_bytes)

    sketches = theta_sketches_from_puffin_file(puffin_file)

    assert len(sketches) == 3
    assert [s.field_id for s in sketches] == [1, 2, 3]
    for sketch in sketches:
        assert sketch.get_estimate() == pytest.approx(3.0)


def test_theta_sketches_from_puffin_file_empty_sketch(empty_sketch_bytes: bytes) -> None:
    puffin_bytes = _build_puffin_file(empty_sketch_bytes, field_ids=[7])
    puffin_file = PuffinFile(puffin_bytes)

    sketches = theta_sketches_from_puffin_file(puffin_file)

    assert len(sketches) == 1
    assert sketches[0].is_empty()
    assert sketches[0].get_estimate() == 0.0


def test_theta_sketches_from_trino_written_puffin_file() -> None:
    puffin_file = PuffinFile(_open_fixture("theta-sketches.puffin"))
    sketches = theta_sketches_from_puffin_file(puffin_file)

    assert len(sketches) == 3
    assert [s.field_id for s in sketches] == [1, 2, 3]
    for sketch in sketches:
        assert sketch.get_estimate() == pytest.approx(5.0)
