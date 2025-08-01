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
from pathlib import PosixPath

import pyarrow as pa
import pytest

from pyiceberg.catalog import Catalog
from pyiceberg.catalog.memory import InMemoryCatalog
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.expressions import LessThanOrEqual
from pyiceberg.manifest import ManifestContent, ManifestEntryStatus


@pytest.fixture
def catalog(tmp_path: PosixPath) -> InMemoryCatalog:
    catalog = InMemoryCatalog("test.in_memory.catalog", warehouse=tmp_path.absolute().as_posix())
    catalog.create_namespace("default")
    return catalog


def _drop_table(catalog: Catalog, identifier: str) -> None:
    try:
        catalog.drop_table(identifier)
    except NoSuchTableError:
        pass


def test_manifest_entry_after_deletes(catalog: Catalog) -> None:
    identifier = "default.test_manifest_entry_after_deletes"
    _drop_table(catalog, identifier)

    schema = pa.schema(
        [
            ("id", pa.int32()),
            ("name", pa.string()),
        ]
    )

    table = catalog.create_table(identifier, schema)
    data = pa.Table.from_pylist(
        [
            {"id": 1, "name": "foo"},
            {"id": 2, "name": "bar"},
            {"id": 3, "name": "bar"},
            {"id": 4, "name": "bar"},
        ],
        schema=schema,
    )
    table.append(data)

    def assert_manifest_entry(expected_status: ManifestEntryStatus, expected_snapshot_id: int) -> None:
        current_snapshot = table.refresh().current_snapshot()
        manifest_files = current_snapshot.manifests(table.io)
        assert len(manifest_files) == 1

        entries = manifest_files[0].fetch_manifest_entry(table.io, discard_deleted=False)
        assert len(entries) == 1
        entry = entries[0]
        assert entry.status == expected_status
        assert entry.snapshot_id == expected_snapshot_id

    before_delete_snapshot_id = table.current_snapshot().snapshot_id
    assert_manifest_entry(ManifestEntryStatus.ADDED, before_delete_snapshot_id)

    table.delete(LessThanOrEqual("id", 4))
    after_delete_snapshot_id = table.refresh().current_snapshot().snapshot_id
    assert_manifest_entry(ManifestEntryStatus.DELETED, after_delete_snapshot_id)
