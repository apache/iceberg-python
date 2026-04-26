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
import pathlib
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
    """Encode an integer as a Hadoop VInt (matching Java WritableUtils.writeVLong)."""
    if -112 <= value <= 127:
        return struct.pack("b", value)
    negative = value < 0
    work = ~value if negative else value
    # Java: len starts at -120 (negative) or -112 (positive),
    # decrements for each significant byte in the value
    prefix = -120 if negative else -112
    tmp = work
    while tmp != 0:
        tmp >>= 8
        prefix -= 1
    num_bytes = (-119 - prefix) if negative else (-111 - prefix)
    result = struct.pack("b", prefix)
    result += work.to_bytes(num_bytes, byteorder="big", signed=False)
    return result


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


# --- VInt unit tests ---


def test_read_hadoop_vint_single_byte() -> None:
    stream = BytesIO(bytes([42]))
    assert _read_hadoop_vint(stream) == 42


def test_read_hadoop_vint_zero() -> None:
    stream = BytesIO(bytes([0]))
    assert _read_hadoop_vint(stream) == 0


def test_read_hadoop_vint_max_single_byte() -> None:
    stream = BytesIO(bytes([0x7F]))
    assert _read_hadoop_vint(stream) == 127


def test_read_hadoop_vint_negative_single_byte() -> None:
    """Values -112 through -1 are single-byte in Hadoop VInt."""
    for value in [-1, -50, -112]:
        encoded = _write_vint(value)
        assert _read_hadoop_vint(BytesIO(encoded)) == value


def test_read_hadoop_vint_multi_byte_positive() -> None:
    """Values > 127 require multi-byte encoding."""
    for value in [128, 255, 256, 1000, 65535]:
        encoded = _write_vint(value)
        assert _read_hadoop_vint(BytesIO(encoded)) == value


def test_read_hadoop_vint_multi_byte_negative() -> None:
    """Values < -112 require multi-byte encoding with negative flag."""
    for value in [-113, -200, -1000]:
        encoded = _write_vint(value)
        assert _read_hadoop_vint(BytesIO(encoded)) == value


def test_read_hadoop_vint_empty_stream() -> None:
    stream = BytesIO(b"")
    with pytest.raises(HiveAuthError, match="Unexpected end of token file"):
        _read_hadoop_vint(stream)


def test_read_hadoop_vint_truncated_multi_byte() -> None:
    """Multi-byte VInt with missing payload bytes should raise."""
    # Prefix byte for 2-byte positive value, but no payload
    stream = BytesIO(struct.pack("b", -113))
    with pytest.raises(HiveAuthError, match="Unexpected end of token file"):
        _read_hadoop_vint(stream)


# --- Bytes/Text unit tests ---


def test_read_hadoop_bytes() -> None:
    data = b"hello"
    stream = BytesIO(_write_bytes(data))
    assert _read_hadoop_bytes(stream) == data


def test_read_hadoop_text() -> None:
    stream = BytesIO(_write_text("hello"))
    assert _read_hadoop_text(stream) == "hello"


def test_read_hadoop_text_invalid_utf8() -> None:
    """Invalid UTF-8 in text field should raise HiveAuthError."""
    invalid_bytes = b"\xff\xfe"
    raw = _write_vint(len(invalid_bytes)) + invalid_bytes
    with pytest.raises(HiveAuthError, match="invalid UTF-8"):
        _read_hadoop_text(BytesIO(raw))


# --- Token file tests ---


def test_read_hive_delegation_token_valid(tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch) -> None:
    identifier = b"test-identifier-bytes"
    password = b"test-password-bytes"
    token_data = _build_token_file(
        [
            (identifier, password, "HIVE_DELEGATION_TOKEN", "hive_service"),
        ]
    )

    token_file = tmp_path / "token_file"
    token_file.write_bytes(token_data)

    monkeypatch.setenv(HADOOP_TOKEN_FILE_LOCATION, str(token_file))
    result_id, result_pw = read_hive_delegation_token()

    assert result_id == base64.b64encode(identifier).decode("ascii")
    assert result_pw == base64.b64encode(password).decode("ascii")


def test_read_hive_delegation_token_multiple_tokens(tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """The parser should find the HIVE_DELEGATION_TOKEN even if other tokens come first."""
    identifier = b"hive-id"
    password = b"hive-pw"
    token_data = _build_token_file(
        [
            (b"hdfs-id", b"hdfs-pw", "HDFS_DELEGATION_TOKEN", "hdfs_service"),
            (identifier, password, "HIVE_DELEGATION_TOKEN", "hive_service"),
        ]
    )

    token_file = tmp_path / "token_file"
    token_file.write_bytes(token_data)

    monkeypatch.setenv(HADOOP_TOKEN_FILE_LOCATION, str(token_file))
    result_id, result_pw = read_hive_delegation_token()

    assert result_id == base64.b64encode(identifier).decode("ascii")
    assert result_pw == base64.b64encode(password).decode("ascii")


def test_read_hive_delegation_token_env_not_set(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(HADOOP_TOKEN_FILE_LOCATION, raising=False)
    with pytest.raises(HiveAuthError, match="HADOOP_TOKEN_FILE_LOCATION.*not set"):
        read_hive_delegation_token()


def test_read_hive_delegation_token_file_not_found(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(HADOOP_TOKEN_FILE_LOCATION, "/nonexistent/path/token_file")
    with pytest.raises(HiveAuthError, match="Cannot read Hadoop token file"):
        read_hive_delegation_token()


def test_read_hive_delegation_token_permission_error(tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Permission errors on the token file should raise HiveAuthError."""
    token_file = tmp_path / "token_file"
    token_file.write_bytes(b"HDTS\x00\x00")
    token_file.chmod(0o000)

    monkeypatch.setenv(HADOOP_TOKEN_FILE_LOCATION, str(token_file))
    try:
        with pytest.raises(HiveAuthError, match="Cannot read Hadoop token file"):
            read_hive_delegation_token()
    finally:
        token_file.chmod(0o644)


def test_read_hive_delegation_token_bad_magic(tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch) -> None:
    token_file = tmp_path / "token_file"
    token_file.write_bytes(b"BAAD\x00\x00")

    monkeypatch.setenv(HADOOP_TOKEN_FILE_LOCATION, str(token_file))
    with pytest.raises(HiveAuthError, match="Invalid Hadoop token file magic"):
        read_hive_delegation_token()


def test_read_hive_delegation_token_unsupported_version(tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch) -> None:
    token_file = tmp_path / "token_file"
    token_file.write_bytes(b"HDTS\x01\x00")  # version 1

    monkeypatch.setenv(HADOOP_TOKEN_FILE_LOCATION, str(token_file))
    with pytest.raises(HiveAuthError, match="Unsupported.*version"):
        read_hive_delegation_token()


def test_read_hive_delegation_token_no_hive_token(tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch) -> None:
    token_data = _build_token_file(
        [
            (b"hdfs-id", b"hdfs-pw", "HDFS_DELEGATION_TOKEN", "hdfs_service"),
        ]
    )

    token_file = tmp_path / "token_file"
    token_file.write_bytes(token_data)

    monkeypatch.setenv(HADOOP_TOKEN_FILE_LOCATION, str(token_file))
    with pytest.raises(HiveAuthError, match="No HIVE_DELEGATION_TOKEN found"):
        read_hive_delegation_token()


def test_read_hive_delegation_token_truncated(tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch) -> None:
    # Build a valid file and then truncate it
    token_data = _build_token_file(
        [
            (b"test-id", b"test-pw", "HIVE_DELEGATION_TOKEN", "hive_service"),
        ]
    )
    truncated = token_data[:10]  # Cut off in the middle

    token_file = tmp_path / "token_file"
    token_file.write_bytes(truncated)

    monkeypatch.setenv(HADOOP_TOKEN_FILE_LOCATION, str(token_file))
    with pytest.raises(HiveAuthError, match="Unexpected end of token file"):
        read_hive_delegation_token()
