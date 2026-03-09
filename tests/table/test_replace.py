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
from pyiceberg.catalog import Catalog
from pyiceberg.manifest import DataFile, DataFileContent, FileFormat
from pyiceberg.schema import Schema
from pyiceberg.table.snapshots import Operation
from pyiceberg.typedef import Record


def test_replace_api(catalog: Catalog) -> None:
    # Setup a basic table using the catalog fixture
    catalog.create_namespace("default")
    table = catalog.create_table(
        identifier="default.test_replace",
        schema=Schema(),
    )

    # Create mock DataFiles for the test
    file_to_delete = DataFile.from_args(
        file_path="s3://bucket/test/data/deleted.parquet",
        file_format=FileFormat.PARQUET,
        partition=Record(),
        record_count=100,
        file_size_in_bytes=1024,
        content=DataFileContent.DATA,
    )
    file_to_delete.spec_id = 0

    file_to_add = DataFile.from_args(
        file_path="s3://bucket/test/data/added.parquet",
        file_format=FileFormat.PARQUET,
        partition=Record(),
        record_count=100,
        file_size_in_bytes=1024,
        content=DataFileContent.DATA,
    )
    file_to_add.spec_id = 0

    # Initially append to have something to replace
    with table.transaction() as tx:
        with tx.update_snapshot().fast_append() as append_snapshot:
            append_snapshot.append_data_file(file_to_delete)

    # Verify initial append snapshot
    assert len(table.history()) == 1
    snapshot = table.current_snapshot()
    assert snapshot is not None
    assert snapshot.summary is not None
    assert snapshot.summary["operation"] == Operation.APPEND

    # Call the replace API
    table.replace(files_to_delete=[file_to_delete], files_to_add=[file_to_add])

    # Verify the replacement created a REPLACE snapshot
    assert len(table.history()) == 2
    snapshot = table.current_snapshot()
    assert snapshot is not None
    assert snapshot.summary is not None
    assert snapshot.summary["operation"] == Operation.REPLACE

    # Verify the correct files are added and deleted
    # The summary property tracks these counts
    assert snapshot.summary["added-data-files"] == "1"
    assert snapshot.summary["deleted-data-files"] == "1"
    assert snapshot.summary["added-records"] == "100"
    assert snapshot.summary["deleted-records"] == "100"

    # Verify the new file exists in the new manifest
    manifest_files = snapshot.manifests(table.io)
    assert len(manifest_files) == 2  # One for ADDED, one for DELETED

    # Check that sequence numbers were handled properly natively by verifying the manifest contents
    entries = []
    for manifest in manifest_files:
        for entry in manifest.fetch_manifest_entry(table.io, discard_deleted=False):
            entries.append(entry)

    # One entry for ADDED (new file), one for DELETED (old file)
    assert len(entries) == 2


def test_replace_empty_files(catalog: Catalog) -> None:
    # Setup a basic table using the catalog fixture
    catalog.create_namespace("default")
    table = catalog.create_table(
        identifier="default.test_replace_empty",
        schema=Schema(),
    )

    # Replacing empty lists should not throw errors, but should produce no changes.
    table.replace([], [])

    # History should be completely empty since no files were rewritten
    assert len(table.history()) == 0
    assert table.current_snapshot() is None
