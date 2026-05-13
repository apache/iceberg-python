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
from __future__ import annotations

import struct
import uuid
from typing import Any

import pytest

from pyiceberg.avro.decoder import new_decoder
from pyiceberg.avro.encoder import MemoryBinaryEncoder

ENCODERS: list[Any] = [MemoryBinaryEncoder]
try:
    from pyiceberg.avro.encoder_fast import CythonBinaryEncoder

    ENCODERS.append(CythonBinaryEncoder)
except ModuleNotFoundError:
    pass


@pytest.fixture(params=ENCODERS, ids=[c.__name__ for c in ENCODERS])
def encoder(request: pytest.FixtureRequest) -> Any:
    return request.param()


def test_write(encoder: Any) -> None:
    _input = b"\x12\x34\x56"
    encoder.write(_input)
    assert encoder.getvalue() == _input


def test_write_boolean(encoder: Any) -> None:
    encoder.write_boolean(True)
    encoder.write_boolean(False)
    assert encoder.getvalue() == struct.pack("??", True, False)


def test_write_int(encoder: Any) -> None:
    _1byte_input = 2
    _2byte_input = 7466
    _3byte_input = 523490
    _4byte_input = 86561570
    _5byte_input = 2510416930
    _6byte_input = 734929016866
    _7byte_input = 135081528772642
    _8byte_input = 35124861473277986

    encoder.write_int(_1byte_input)
    encoder.write_int(_2byte_input)
    encoder.write_int(_3byte_input)
    encoder.write_int(_4byte_input)
    encoder.write_int(_5byte_input)
    encoder.write_int(_6byte_input)
    encoder.write_int(_7byte_input)
    encoder.write_int(_8byte_input)

    buffer = encoder.getvalue()

    assert buffer[0:1] == b"\x04"
    assert buffer[1:3] == b"\xd4\x74"
    assert buffer[3:6] == b"\xc4\xf3\x3f"
    assert buffer[6:10] == b"\xc4\xcc\xc6\x52"
    assert buffer[10:15] == b"\xc4\xb0\x8f\xda\x12"
    assert buffer[15:21] == b"\xc4\xe0\xf6\xd2\xe3\x2a"
    assert buffer[21:28] == b"\xc4\xa0\xce\xe8\xe3\xb6\x3d"
    assert buffer[28:36] == b"\xc4\xa0\xb2\xae\x83\xf8\xe4\x7c"


def test_write_float(encoder: Any) -> None:
    _input = 3.14159265359
    encoder.write_float(_input)
    assert encoder.getvalue() == struct.pack("<f", _input)


def test_write_double(encoder: Any) -> None:
    _input = 3.14159265359
    encoder.write_double(_input)
    assert encoder.getvalue() == struct.pack("<d", _input)


def test_write_bytes(encoder: Any) -> None:
    _input = b"\x12\x34\x56"
    encoder.write_bytes(_input)
    assert encoder.getvalue() == b"".join([b"\x06", _input])


def test_write_utf8(encoder: Any) -> None:
    _input = "That, my liege, is how we know the Earth to be banana-shaped."
    bin_input = _input.encode()
    encoder.write_utf8(_input)
    assert encoder.getvalue() == b"".join([b"\x7a", bin_input])


def test_write_uuid(encoder: Any) -> None:
    _input = uuid.UUID("12345678-1234-5678-1234-567812345678")
    encoder.write_uuid(_input)
    buf = encoder.getvalue()
    assert len(buf) == 16
    assert buf == b"\x124Vx\x124Vx\x124Vx\x124Vx"


@pytest.mark.parametrize(
    "v",
    [0, 1, -1, 63, 64, -64, -65, 127, 128, -128, 2**31 - 1, -(2**31), 2**62, -(2**62), 2**63 - 1, -(2**63)],
)
def test_int_round_trip(encoder: Any, v: int) -> None:
    encoder.write_int(v)
    decoder = new_decoder(encoder.getvalue())
    assert decoder.read_int() == v


def test_encoders_byte_identical() -> None:
    """Both encoder implementations must produce identical output."""
    if len(ENCODERS) < 2:
        pytest.skip("Cython encoder not built")

    def fill(enc: Any) -> bytes:
        enc.write_boolean(True)
        for v in [0, 1, -1, 7466, -523490, 2**62, -(2**62)]:
            enc.write_int(v)
        enc.write_float(1.5)
        enc.write_double(3.14159265359)
        enc.write_bytes(b"\x00\xff" * 50)
        enc.write_utf8("hello ☃ snowman")
        enc.write_uuid(uuid.UUID("12345678-1234-5678-1234-567812345678"))
        return enc.getvalue()

    outputs = [fill(cls()) for cls in ENCODERS]
    assert outputs[0] == outputs[1]
