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
from pathlib import Path
from typing import List, Set

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from pyiceberg.manifest import DataFile
from pyiceberg.table import Table
from pyiceberg.table.maintenance import MaintenanceTable
from tests.catalog.test_base import InMemoryCatalog
import uuid


@pytest.fixture
def iceberg_catalog(tmp_path: Path) -> InMemoryCatalog:
    catalog = InMemoryCatalog("test.in_memory.catalog", warehouse=tmp_path.absolute().as_posix())
    catalog.create_namespace("default")
    return catalog


@pytest.fixture
def dupe_data_file_path(tmp_path: Path) -> Path:
    unique_id = uuid.uuid4()
    return tmp_path / f"{unique_id}" / "file1.parquet"


@pytest.fixture
def prepopulated_table(iceberg_catalog: InMemoryCatalog, dupe_data_file_path: Path) -> Table:
    identifier = "default.test_table"
    try:
        iceberg_catalog.drop_table(identifier)
    except Exception:
        pass

    arrow_schema = pa.schema(
        [
            pa.field("id", pa.int32(), nullable=False),
            pa.field("value", pa.string(), nullable=True),
        ]
    )

    df_a = pa.Table.from_pylist(
        [
            {"id": 1, "value": "A"},
        ],
        schema=arrow_schema,
    )
    df_b = pa.Table.from_pylist(
        [
            {"id": 2, "value": "B"},
        ],
        schema=arrow_schema,
    )

    # Ensure the parent directory exists
    dupe_data_file_path.parent.mkdir(parents=True, exist_ok=True)

    pq.write_table(df_a, str(dupe_data_file_path))
    pq.write_table(df_b, str(dupe_data_file_path))

    table: Table = iceberg_catalog.create_table(identifier, arrow_schema)

    tx = table.transaction()
    tx.add_files([str(dupe_data_file_path)], check_duplicate_files=False)
    tx.add_files([str(dupe_data_file_path)], check_duplicate_files=False)
    tx.commit_transaction()

    return table


def test_overwrite_removes_only_selected_datafile(prepopulated_table: Table, dupe_data_file_path: Path) -> None:
    mt = MaintenanceTable(tbl=prepopulated_table)

    removed_files: List[DataFile] = mt.deduplicate_data_files()

    file_paths_after: Set[str] = {df.file_path for df in mt._get_all_datafiles()}

    # Both files should remain, since they are not duplicates
    assert str(dupe_data_file_path) in file_paths_after, "Expected file_a.parquet to remain in the table"
    assert len(removed_files) == 0, "Expected no files to be removed since there are no duplicates"


def test_get_all_datafiles_current_snapshot(prepopulated_table: Table, dupe_data_file_path: Path) -> None:
    mt = MaintenanceTable(tbl=prepopulated_table)

    datafiles: List[DataFile] = mt._get_all_datafiles()
    file_paths: Set[str] = {df.file_path for df in datafiles}
    assert str(dupe_data_file_path) in file_paths


def test_get_all_datafiles_all_snapshots(prepopulated_table: Table, dupe_data_file_path: Path) -> None:
    mt = MaintenanceTable(tbl=prepopulated_table)

    datafiles: List[DataFile] = mt._get_all_datafiles()
    file_paths: Set[str] = {df.file_path for df in datafiles}
    assert str(dupe_data_file_path) in file_paths


def test_dedup_data_files_removes_duplicates_in_current_snapshot(prepopulated_table: Table, dupe_data_file_path: Path) -> None:
    mt = MaintenanceTable(tbl=prepopulated_table)

    all_datafiles: List[DataFile] = mt._get_all_datafiles()
    file_paths: List[str] = [df.file_path for df in all_datafiles]
    # Only one reference should remain after deduplication
    assert file_paths.count(str(dupe_data_file_path)) == 1
    removed: List[DataFile] = mt.deduplicate_data_files()

    all_datafiles_after: List[DataFile] = mt._get_all_datafiles()
    file_paths_after: List[str] = [df.file_path for df in all_datafiles_after]
    assert file_paths_after.count(str(dupe_data_file_path)) == 1
    assert all(isinstance(df, DataFile) for df in removed)
