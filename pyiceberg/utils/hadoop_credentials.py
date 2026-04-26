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
    """Decode a Hadoop WritableUtils VInt/VLong from a byte stream.

    Matches the encoding in Java's ``WritableUtils.readVInt``/``readVLong``:
    - If the first byte (interpreted as signed) is >= -112, it *is* the value.
    - Otherwise the first byte encodes both a negativity flag and the number
      of additional big-endian payload bytes that carry the actual value.
    """
    first = stream.read(1)
    if not first:
        raise HiveAuthError("Unexpected end of token file while reading VInt")
    # Reinterpret as signed byte to match Java's signed-byte semantics
    b = first[0]
    if b > 127:
        b -= 256
    if b >= -112:
        return b
    negative = b < -120
    length = (-119 - b) if negative else (-111 - b)
    extra = stream.read(length)
    if len(extra) != length:
        raise HiveAuthError("Unexpected end of token file while reading VInt")
    result = 0
    for byte_val in extra:
        result = (result << 8) | byte_val
    if negative:
        result = ~result
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
    raw = _read_hadoop_bytes(stream)
    try:
        return raw.decode("utf-8")
    except UnicodeDecodeError as e:
        raise HiveAuthError(f"Token file contains invalid UTF-8 in text field: {e}") from e


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
    except OSError as e:
        raise HiveAuthError(f"Cannot read Hadoop token file {token_file}: {e}") from e

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
        f"No {HIVE_DELEGATION_TOKEN_KIND} found in token file: {token_file}. File contains {num_tokens} token(s)."
    )
