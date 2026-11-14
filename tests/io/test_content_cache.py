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
from unittest.mock import patch

import pyarrow as pa
import pytest

from pyiceberg.catalog import Catalog
from pyiceberg.io import (
    IO_MANIFEST_CACHE_ENABLED,
    IO_MANIFEST_CACHE_EXPIRATION_INTERVAL_MS,
    IO_MANIFEST_CACHE_MAX_CONTENT_LENGTH,
    IO_MANIFEST_CACHE_MAX_TOTAL_BYTES,
    IO_MANIFEST_CACHE_MAX_TOTAL_BYTES_DEFAULT,
)
from pyiceberg.io.content_cache import ContentCache
from pyiceberg.io.pyarrow import PyArrowFileIO


class _FakeClock:
    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now


def test_cache_disabled_by_default() -> None:
    assert PyArrowFileIO().content_cache is None


def test_cache_enabled_by_properties() -> None:
    cache = PyArrowFileIO({IO_MANIFEST_CACHE_ENABLED: "true"}).content_cache

    assert cache is not None
    assert cache.max_total_bytes == IO_MANIFEST_CACHE_MAX_TOTAL_BYTES_DEFAULT


def test_cache_returns_put_bytes() -> None:
    cache = ContentCache(max_total_bytes=10, max_content_length=10, expiration_interval_ms=60_000)

    assert cache.get("a") is None
    cache.put("a", b"123")
    assert cache.get("a") == b"123"


def test_cache_skips_entries_over_max_content_length() -> None:
    cache = ContentCache(max_total_bytes=100, max_content_length=3, expiration_interval_ms=60_000)

    cache.put("a", b"1234")

    assert cache.get("a") is None


def test_cache_skips_entries_over_max_total_bytes() -> None:
    cache = ContentCache(max_total_bytes=3, max_content_length=10, expiration_interval_ms=60_000)

    cache.put("a", b"1234")

    assert cache.get("a") is None


def test_cache_evicts_least_recently_used() -> None:
    cache = ContentCache(max_total_bytes=4, max_content_length=4, expiration_interval_ms=60_000)

    cache.put("a", b"11")
    cache.put("b", b"22")
    assert cache.get("a") == b"11"
    cache.put("c", b"33")

    assert cache.get("b") is None
    assert cache.get("a") == b"11"
    assert cache.get("c") == b"33"


def test_cache_expires_entries() -> None:
    clock = _FakeClock()
    cache = ContentCache(max_total_bytes=10, max_content_length=10, expiration_interval_ms=1_000, clock=clock)

    cache.put("a", b"11")
    clock.now = 1.0

    assert cache.get("a") is None


def test_manifest_list_is_read_once(catalog: Catalog) -> None:
    catalog.create_namespace("default")
    table = catalog.create_table("default.cached_manifest_list", schema=pa.schema([pa.field("x", pa.int64())]))
    table.append(pa.table({"x": [1, 2, 3]}))

    snapshot = table.current_snapshot()
    assert snapshot is not None

    io = PyArrowFileIO({IO_MANIFEST_CACHE_ENABLED: "true"})
    with patch.object(io, "new_input", side_effect=io.new_input) as new_input:
        snapshot.manifests(io)
        snapshot.manifests(io)

    assert new_input.call_count == 1


def test_manifest_entries_are_read_once(catalog: Catalog) -> None:
    catalog.create_namespace("default")
    table = catalog.create_table("default.cached_manifest", schema=pa.schema([pa.field("x", pa.int64())]))
    table.append(pa.table({"x": [1, 2, 3]}))

    snapshot = table.current_snapshot()
    assert snapshot is not None

    io = PyArrowFileIO({IO_MANIFEST_CACHE_ENABLED: "true"})
    manifest = snapshot.manifests(io)[0]

    with patch.object(io, "new_input", side_effect=io.new_input) as new_input:
        manifest.fetch_manifest_entry(io)
        manifest.fetch_manifest_entry(io)

    assert new_input.call_count == 1


def test_reads_are_not_cached_when_disabled(catalog: Catalog) -> None:
    catalog.create_namespace("default")
    table = catalog.create_table("default.uncached_manifest", schema=pa.schema([pa.field("x", pa.int64())]))
    table.append(pa.table({"x": [1, 2, 3]}))

    snapshot = table.current_snapshot()
    assert snapshot is not None

    io = PyArrowFileIO()
    with patch.object(io, "new_input", side_effect=io.new_input) as new_input:
        snapshot.manifests(io)
        snapshot.manifests(io)

    assert new_input.call_count == 2


def test_cache_properties_are_read_from_table_properties(catalog: Catalog) -> None:
    catalog.create_namespace("default")
    catalog.create_table(
        "default.cache_properties",
        schema=pa.schema([pa.field("x", pa.int64())]),
        properties={
            IO_MANIFEST_CACHE_ENABLED: "true",
            IO_MANIFEST_CACHE_MAX_TOTAL_BYTES: "1024",
            IO_MANIFEST_CACHE_MAX_CONTENT_LENGTH: "512",
            IO_MANIFEST_CACHE_EXPIRATION_INTERVAL_MS: "1000",
        },
    )

    cache = catalog.load_table("default.cache_properties").io.content_cache

    assert cache is not None
    assert cache.max_total_bytes == 1024
    assert cache.max_content_length == 512
    assert cache.expiration_interval_seconds == 1.0


@pytest.mark.parametrize("enabled", ["false", "0", ""])
def test_cache_stays_disabled_when_property_is_not_truthy(enabled: str) -> None:
    assert PyArrowFileIO({IO_MANIFEST_CACHE_ENABLED: enabled}).content_cache is None
