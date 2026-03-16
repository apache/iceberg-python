#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.
# pylint: disable=protected-access

import base64
import os
import struct
from io import BytesIO

import pytest

from pyiceberg.exceptions import HiveAuthError
from pyiceberg.utils.hadoop_credentials import (
    HADOOP_TOKEN_FILE_LOCATION,
    _read_hadoop_bytes,
    _read_hadoop_text,
    _read_hadoop_vint,
    read_hive_delegation_token,
)


def _write_vint(value: int) -> bytes:
    """Encode a non-negative integer as a Hadoop VInt (simplified for tests)."""
    if value <= 0x7F:
        return bytes([value])
    # For values > 127, use 2-byte encoding (sufficient for test data)
    return bytes([0x80 | ((value >> 8) & 0x7F), value & 0xFF])


def _write_bytes(data: bytes) -> bytes:
    """Write VInt-prefixed byte array."""
    return _write_vint(len(data)) + data


def _write_text(text: str) -> bytes:
    """Write VInt-prefixed UTF-8 string."""
    encoded = text.encode("utf-8")
    return _write_vint(len(encoded)) + encoded


def _build_token_file(tokens: list[tuple[bytes, bytes, str, str]]) -> bytes:
    """Build a valid HDTS binary file with the given tokens.

    Each token is (identifier_bytes, password_bytes, kind, service).
    """
    buf = bytearray()
    buf.extend(b"HDTS")  # magic
    buf.append(0)  # version
    buf.extend(_write_vint(len(tokens)))
    for identifier, password, kind, service in tokens:
        buf.extend(_write_bytes(identifier))
        buf.extend(_write_bytes(password))
        buf.extend(_write_text(kind))
        buf.extend(_write_text(service))
    return bytes(buf)


def test_read_hadoop_vint_single_byte() -> None:
    stream = BytesIO(bytes([42]))
    assert _read_hadoop_vint(stream) == 42


def test_read_hadoop_vint_zero() -> None:
    stream = BytesIO(bytes([0]))
    assert _read_hadoop_vint(stream) == 0


def test_read_hadoop_vint_max_single_byte() -> None:
    stream = BytesIO(bytes([0x7F]))
    assert _read_hadoop_vint(stream) == 127


def test_read_hadoop_vint_empty_stream() -> None:
    stream = BytesIO(b"")
    with pytest.raises(HiveAuthError, match="Unexpected end of token file"):
        _read_hadoop_vint(stream)


def test_read_hadoop_bytes() -> None:
    data = b"hello"
    stream = BytesIO(_write_bytes(data))
    assert _read_hadoop_bytes(stream) == data


def test_read_hadoop_text() -> None:
    stream = BytesIO(_write_text("hello"))
    assert _read_hadoop_text(stream) == "hello"


def test_read_hive_delegation_token_valid(tmp_path: object, monkeypatch: pytest.MonkeyPatch) -> None:
    identifier = b"test-identifier-bytes"
    password = b"test-password-bytes"
    token_data = _build_token_file([
        (identifier, password, "HIVE_DELEGATION_TOKEN", "hive_service"),
    ])

    token_file = os.path.join(str(tmp_path), "token_file")
    with open(token_file, "wb") as f:
        f.write(token_data)

    monkeypatch.setenv(HADOOP_TOKEN_FILE_LOCATION, token_file)
    result_id, result_pw = read_hive_delegation_token()

    assert result_id == base64.b64encode(identifier).decode("ascii")
    assert result_pw == base64.b64encode(password).decode("ascii")


def test_read_hive_delegation_token_multiple_tokens(tmp_path: object, monkeypatch: pytest.MonkeyPatch) -> None:
    """The parser should find the HIVE_DELEGATION_TOKEN even if other tokens come first."""
    identifier = b"hive-id"
    password = b"hive-pw"
    token_data = _build_token_file([
        (b"hdfs-id", b"hdfs-pw", "HDFS_DELEGATION_TOKEN", "hdfs_service"),
        (identifier, password, "HIVE_DELEGATION_TOKEN", "hive_service"),
    ])

    token_file = os.path.join(str(tmp_path), "token_file")
    with open(token_file, "wb") as f:
        f.write(token_data)

    monkeypatch.setenv(HADOOP_TOKEN_FILE_LOCATION, token_file)
    result_id, result_pw = read_hive_delegation_token()

    assert result_id == base64.b64encode(identifier).decode("ascii")
    assert result_pw == base64.b64encode(password).decode("ascii")


def test_read_hive_delegation_token_env_not_set(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(HADOOP_TOKEN_FILE_LOCATION, raising=False)
    with pytest.raises(HiveAuthError, match="HADOOP_TOKEN_FILE_LOCATION.*not set"):
        read_hive_delegation_token()


def test_read_hive_delegation_token_file_not_found(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(HADOOP_TOKEN_FILE_LOCATION, "/nonexistent/path/token_file")
    with pytest.raises(HiveAuthError, match="not found"):
        read_hive_delegation_token()


def test_read_hive_delegation_token_bad_magic(tmp_path: object, monkeypatch: pytest.MonkeyPatch) -> None:
    token_file = os.path.join(str(tmp_path), "token_file")
    with open(token_file, "wb") as f:
        f.write(b"BAAD\x00\x00")

    monkeypatch.setenv(HADOOP_TOKEN_FILE_LOCATION, token_file)
    with pytest.raises(HiveAuthError, match="Invalid Hadoop token file magic"):
        read_hive_delegation_token()


def test_read_hive_delegation_token_unsupported_version(tmp_path: object, monkeypatch: pytest.MonkeyPatch) -> None:
    token_file = os.path.join(str(tmp_path), "token_file")
    with open(token_file, "wb") as f:
        f.write(b"HDTS\x01\x00")  # version 1

    monkeypatch.setenv(HADOOP_TOKEN_FILE_LOCATION, token_file)
    with pytest.raises(HiveAuthError, match="Unsupported.*version"):
        read_hive_delegation_token()


def test_read_hive_delegation_token_no_hive_token(tmp_path: object, monkeypatch: pytest.MonkeyPatch) -> None:
    token_data = _build_token_file([
        (b"hdfs-id", b"hdfs-pw", "HDFS_DELEGATION_TOKEN", "hdfs_service"),
    ])

    token_file = os.path.join(str(tmp_path), "token_file")
    with open(token_file, "wb") as f:
        f.write(token_data)

    monkeypatch.setenv(HADOOP_TOKEN_FILE_LOCATION, token_file)
    with pytest.raises(HiveAuthError, match="No HIVE_DELEGATION_TOKEN found"):
        read_hive_delegation_token()


def test_read_hive_delegation_token_truncated(tmp_path: object, monkeypatch: pytest.MonkeyPatch) -> None:
    # Build a valid file and then truncate it
    token_data = _build_token_file([
        (b"test-id", b"test-pw", "HIVE_DELEGATION_TOKEN", "hive_service"),
    ])
    truncated = token_data[:10]  # Cut off in the middle

    token_file = os.path.join(str(tmp_path), "token_file")
    with open(token_file, "wb") as f:
        f.write(truncated)

    monkeypatch.setenv(HADOOP_TOKEN_FILE_LOCATION, token_file)
    with pytest.raises(HiveAuthError, match="Unexpected end of token file"):
        read_hive_delegation_token()
