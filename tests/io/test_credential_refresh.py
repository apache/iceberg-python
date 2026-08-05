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
"""End-to-end tests that the FileIO backends rebuild their filesystem when vended creds change."""

from typing import Any
from unittest import mock

from pyiceberg.io.fsspec import FsspecFileIO
from pyiceberg.io.pyarrow import PyArrowFileIO
from pyiceberg.typedef import Properties

LOCATION = "s3://warehouse/database/table/data/00000.parquet"


class _FakeProvider:
    """A credentials provider whose returned creds can be swapped between calls."""

    def __init__(self, creds: Properties) -> None:
        self.creds = creds

    def properties_for(self, location: str) -> Properties:
        return self.creds


def test_pyarrow_rebuilds_fs_when_credentials_change() -> None:
    provider = _FakeProvider({"s3.session-token": "token-1"})
    io = PyArrowFileIO()
    io.set_credentials_provider(provider)

    seen_tokens: list[Any] = []

    def fake_s3_fs(netloc: Any, properties: Properties) -> object:
        seen_tokens.append(properties.get("s3.session-token"))
        return object()

    with mock.patch.object(PyArrowFileIO, "_initialize_s3_fs", side_effect=fake_s3_fs):
        fs1 = io.new_input(LOCATION)._filesystem
        fs2 = io.new_input(LOCATION)._filesystem  # unchanged creds -> cached
        provider.creds = {"s3.session-token": "token-2"}
        fs3 = io.new_input(LOCATION)._filesystem  # rotated creds -> rebuilt

    assert fs1 is fs2
    assert fs3 is not fs1
    assert seen_tokens == ["token-1", "token-2"]


def test_pyarrow_without_provider_uses_single_fs() -> None:
    io = PyArrowFileIO()
    build_count = 0

    def fake_s3_fs(netloc: Any, properties: Properties) -> object:
        nonlocal build_count
        build_count += 1
        return object()

    with mock.patch.object(PyArrowFileIO, "_initialize_s3_fs", side_effect=fake_s3_fs):
        io.new_input(LOCATION)
        io.new_input(LOCATION)

    assert build_count == 1


def test_fsspec_rebuilds_fs_when_credentials_change() -> None:
    provider = _FakeProvider({"s3.session-token": "token-1"})
    io = FsspecFileIO(properties={})
    io.set_credentials_provider(provider)

    seen_tokens: list[Any] = []

    def fake_s3(properties: Properties) -> object:
        seen_tokens.append(properties.get("s3.session-token"))
        return object()

    io._scheme_to_fs = {"s3": fake_s3}

    fs1 = io.new_input(LOCATION)._fs
    fs2 = io.new_input(LOCATION)._fs  # unchanged creds -> cached
    provider.creds = {"s3.session-token": "token-2"}
    fs3 = io.new_input(LOCATION)._fs  # rotated creds -> rebuilt

    assert fs1 is fs2
    assert fs3 is not fs1
    assert seen_tokens == ["token-1", "token-2"]
