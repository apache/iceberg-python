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

"""Hadoop Delegation Token Service (HDTS) file parser.

Reads delegation tokens from the binary token file pointed to by
the ``$HADOOP_TOKEN_FILE_LOCATION`` environment variable.
"""

from __future__ import annotations

import base64
import os
from io import BytesIO

from pyiceberg.exceptions import HiveAuthError

HADOOP_TOKEN_FILE_LOCATION = "HADOOP_TOKEN_FILE_LOCATION"
HIVE_DELEGATION_TOKEN_KIND = "HIVE_DELEGATION_TOKEN"
HDTS_MAGIC = b"HDTS"
HDTS_SUPPORTED_VERSION = 0


def _read_hadoop_vint(stream: BytesIO) -> int:
    """Decode a Hadoop WritableUtils VInt/VLong from a byte stream."""
    first = stream.read(1)
    if not first:
        raise HiveAuthError("Unexpected end of token file while reading VInt")
    b = first[0]
    if b <= 0x7F:
        return b
    # Number of additional bytes is encoded in leading 1-bits
    num_extra = 0
    mask = 0x80
    while b & mask:
        num_extra += 1
        mask >>= 1
    # First byte contributes the remaining bits
    result = b & (mask - 1)
    extra = stream.read(num_extra)
    if len(extra) != num_extra:
        raise HiveAuthError("Unexpected end of token file while reading VInt")
    for byte in extra:
        result = (result << 8) | byte
    # Sign-extend if negative (high bit of decoded value is set)
    if result >= (1 << (8 * num_extra + (8 - num_extra - 1) - 1)):
        result -= 1 << (8 * num_extra + (8 - num_extra - 1))
    return result


def _read_hadoop_bytes(stream: BytesIO) -> bytes:
    """Read a VInt-prefixed byte array from a Hadoop token stream."""
    length = _read_hadoop_vint(stream)
    if length < 0:
        raise HiveAuthError(f"Invalid byte array length: {length}")
    data = stream.read(length)
    if len(data) != length:
        raise HiveAuthError("Unexpected end of token file while reading byte array")
    return data


def _read_hadoop_text(stream: BytesIO) -> str:
    """Read a VInt-prefixed UTF-8 string from a Hadoop token stream."""
    return _read_hadoop_bytes(stream).decode("utf-8")


def read_hive_delegation_token() -> tuple[str, str]:
    """Read a Hive delegation token from ``$HADOOP_TOKEN_FILE_LOCATION``.

    Returns:
        A ``(identifier, password)`` tuple where both values are
        base64-encoded strings suitable for SASL DIGEST-MD5 auth.

    Raises:
        HiveAuthError: If the token file is missing, malformed, or
            does not contain a ``HIVE_DELEGATION_TOKEN``.
    """
    token_file = os.environ.get(HADOOP_TOKEN_FILE_LOCATION)
    if not token_file:
        raise HiveAuthError(
            f"${HADOOP_TOKEN_FILE_LOCATION} environment variable is not set. "
            "A Hadoop delegation token file is required for DIGEST-MD5 authentication."
        )

    try:
        with open(token_file, "rb") as f:
            data = f.read()
    except FileNotFoundError:
        raise HiveAuthError(f"Hadoop token file not found: {token_file}")

    stream = BytesIO(data)

    magic = stream.read(4)
    if magic != HDTS_MAGIC:
        raise HiveAuthError(f"Invalid Hadoop token file magic: expected {HDTS_MAGIC!r}, got {magic!r}")

    version_byte = stream.read(1)
    if not version_byte:
        raise HiveAuthError("Unexpected end of token file while reading version")
    version = version_byte[0]
    if version != HDTS_SUPPORTED_VERSION:
        raise HiveAuthError(f"Unsupported Hadoop token file version: {version}")

    num_tokens = _read_hadoop_vint(stream)

    for _ in range(num_tokens):
        # Each token entry: identifier_bytes, password_bytes, kind_text, service_text
        identifier_bytes = _read_hadoop_bytes(stream)
        password_bytes = _read_hadoop_bytes(stream)
        kind = _read_hadoop_text(stream)
        _service = _read_hadoop_text(stream)

        if kind == HIVE_DELEGATION_TOKEN_KIND:
            return (
                base64.b64encode(identifier_bytes).decode("ascii"),
                base64.b64encode(password_bytes).decode("ascii"),
            )

    raise HiveAuthError(
        f"No {HIVE_DELEGATION_TOKEN_KIND} found in token file: {token_file}. "
        f"File contains {num_tokens} token(s)."
    )
