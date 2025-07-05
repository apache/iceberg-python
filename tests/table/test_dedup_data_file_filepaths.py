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
from pyarrow import Table as pa_table

from pyiceberg.manifest import DataFile
from pyiceberg.table import Table
from pyiceberg.table.maintenance import MaintenanceTable
from tests.catalog.test_base import InMemoryCatalog


@pytest.fixture
def iceberg_catalog(tmp_path: Path) -> InMemoryCatalog:
    catalog = InMemoryCatalog("test.in_memory.catalog", warehouse=tmp_path.absolute().as_posix())
    catalog.create_namespace("default")
    return catalog


def test_overwrite_removes_only_selected_datafile(iceberg_catalog: InMemoryCatalog, tmp_path: Path) -> None:
    identifier = "default.test_overwrite_removes_only_selected_datafile"
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
    df_a = pa_table.from_pylist(
        [
            {"id": 1, "value": "A", "file_path": "path/to/file_a"},
        ],
        schema=arrow_schema,
    )
    df_b = pa_table.from_pylist(
        [
            {"id": 1, "value": "A", "file_path": "path/to/file_a"},
        ],
        schema=arrow_schema,
    )

    parquet_path_a = str(tmp_path / "file_a.parquet")
    parquet_path_b = str(tmp_path / "file_a.parquet")
    pq.write_table(df_a, parquet_path_a)
    pq.write_table(df_b, parquet_path_b)

    table: Table = iceberg_catalog.create_table(identifier, arrow_schema)

    tx = table.transaction()
    tx.add_files([parquet_path_a], check_duplicate_files=False)
    tx.add_files([parquet_path_b], check_duplicate_files=False)
    tx.commit_transaction()

    mt = MaintenanceTable(tbl=table)

    mt.tbl.maintenance.deduplicate_data_files(scan_all_partitions=True, scan_all_snapshots=True)

    file_paths: List[str] = [chunk.as_py() for chunk in mt.tbl.inspect.data_files().to_pylist()]

    assert len(file_paths) == len(set(file_paths)), "Duplicate file paths found in the table"


def test_get_all_datafiles_current_snapshot(iceberg_table: Table, tmp_path: Path) -> None:
    mt = MaintenanceTable(iceberg_table)
    df1 = pa.Table.from_pylist([{"id": 1, "value": "A"}])
    df2 = pa.Table.from_pylist([{"id": 2, "value": "B"}])
    path1 = str(tmp_path / "file1.parquet")
    path2 = str(tmp_path / "file2.parquet")
    pq.write_table(df1, path1)
    pq.write_table(df2, path2)
    tx = mt.tbl.transaction()
    tx.add_files([path1, path2])
    tx.commit_transaction()
    datafiles: List[DataFile] = mt._get_all_datafiles(scan_all_snapshots=False)
    file_paths: Set[str] = {df.file_path for df in datafiles}
    assert path1 in file_paths and path2 in file_paths


def test_get_all_datafiles_all_snapshots(iceberg_table: Table, tmp_path: Path) -> None:
    mt = MaintenanceTable(iceberg_table)
    df1 = pa.Table.from_pylist([{"id": 1, "value": "A"}])
    path1 = str(tmp_path / "file1.parquet")
    pq.write_table(df1, path1)
    tx1 = mt.tbl.transaction()
    tx1.add_files([path1])
    tx1.commit_transaction()
    df2 = pa.Table.from_pylist([{"id": 2, "value": "B"}])
    path2 = str(tmp_path / "file2.parquet")
    pq.write_table(df2, path2)
    tx2 = mt.tbl.transaction()
    tx2.add_files([path2])
    tx2.commit_transaction()
    datafiles: List[DataFile] = mt._get_all_datafiles(scan_all_snapshots=True)
    file_paths: Set[str] = {df.file_path for df in datafiles}
    assert path1 in file_paths and path2 in file_paths


def test_deduplicate_data_files_removes_duplicates(iceberg_table: Table, tmp_path: Path) -> None:
    mt = MaintenanceTable(iceberg_table)
    df = pa.Table.from_pylist([{"id": 1, "value": "A"}])
    path = str(tmp_path / "dup.parquet")
    pq.write_table(df, path)

    tx1 = mt.tbl.transaction()
    tx1.add_files([path])
    tx1.commit_transaction()
    tx2 = mt.tbl.transaction()
    tx2.add_files([path])
    tx2.commit_transaction()

    all_datafiles: List[DataFile] = mt._get_all_datafiles(scan_all_snapshots=True)
    file_paths: List[str] = [df.file_path for df in all_datafiles]
    assert file_paths.count(path) > 1

    removed: List[DataFile] = mt.deduplicate_data_files(scan_all_partitions=True, scan_all_snapshots=True)

    all_datafiles_after: List[DataFile] = mt._get_all_datafiles(scan_all_snapshots=True)
    file_paths_after: List[str] = [df.file_path for df in all_datafiles_after]
    assert file_paths_after.count(path) == 1
    assert all(isinstance(df, DataFile) for df in removed)
