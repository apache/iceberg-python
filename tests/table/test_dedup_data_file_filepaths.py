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
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pyarrow import Table as pa_table

from pyiceberg.io.pyarrow import parquet_file_to_data_file
from pyiceberg.manifest import DataFile
from pyiceberg.table import Table
from pyiceberg.table.maintenance import MaintenanceTable
from tests.catalog.test_base import InMemoryCatalog


@pytest.fixture
def iceberg_catalog(tmp_path):
    catalog = InMemoryCatalog("test.in_memory.catalog", warehouse=tmp_path.absolute().as_posix())
    catalog.create_namespace("default")
    return catalog


def test_overwrite_removes_only_selected_datafile(iceberg_catalog, tmp_path):
    # Create a table and append two batches referencing the same file path
    identifier = "default.test_overwrite_removes_only_selected_datafile"
    try:
        iceberg_catalog.drop_table(identifier)
    except Exception:
        pass

    # Create Arrow schema and table
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

    # Write Arrow tables to Parquet files
    parquet_path_a = str(tmp_path / "file_a.parquet")
    parquet_path_b = str(tmp_path / "file_a.parquet")
    pq.write_table(df_a, parquet_path_a)
    pq.write_table(df_b, parquet_path_b)

    table: Table = iceberg_catalog.create_table(identifier, arrow_schema)

    # Add both files as DataFiles using add_files
    tx = table.transaction()
    tx.add_files([parquet_path_a], check_duplicate_files=False)
    tx.add_files([parquet_path_b], check_duplicate_files=False)

    # Find DataFile for file_b
    data_file_b = parquet_file_to_data_file(table.io, table.metadata, parquet_path_b)

    # Overwrite: Remove only the DataFile for file_b
    mt = MaintenanceTable(tbl=table)

    # Find: duplicate data files, across all partitions and snapshots
    mt.tbl.maintenance.deduplicate_data_files(scan_all_partitions=True, scan_all_snapshots=True)

    # Assert: only the row from file_a remains
    # Get all file paths in the current table
    file_paths = [chunk.as_py() for chunk in mt.tbl.inspect.data_files().to_pylist()]

    # Assert there are no duplicate file paths
    assert len(file_paths) == len(set(file_paths)), "Duplicate file paths found in the table"


def test_get_all_datafiles_current_snapshot(iceberg_table, tmp_path):
    mt = MaintenanceTable(iceberg_table)
    # Write two files
    df1 = pa.Table.from_pylist([{"id": 1, "value": "A"}])
    df2 = pa.Table.from_pylist([{"id": 2, "value": "B"}])
    path1 = str(tmp_path / "file1.parquet")
    path2 = str(tmp_path / "file2.parquet")
    pq.write_table(df1, path1)
    pq.write_table(df2, path2)
    mt.tbl.transaction().add_files([path1, path2]).commit_transaction()
    datafiles = mt._get_all_datafiles(scan_all_snapshots=False)
    file_paths = {df.file_path for df in datafiles}
    assert path1 in file_paths and path2 in file_paths


def test_get_all_datafiles_all_snapshots(iceberg_table, tmp_path):
    mt = MaintenanceTable(iceberg_table)
    # Write and add a file, then overwrite
    df1 = pa.Table.from_pylist([{"id": 1, "value": "A"}])
    path1 = str(tmp_path / "file1.parquet")
    pq.write_table(df1, path1)
    mt.tbl.transaction().add_files([path1]).commit_transaction()
    # Overwrite with a new file
    df2 = pa.Table.from_pylist([{"id": 2, "value": "B"}])
    path2 = str(tmp_path / "file2.parquet")
    pq.write_table(df2, path2)
    mt.tbl.transaction().add_files([path2]).commit()
    # Should find both files if scanning all snapshots
    datafiles = mt._get_all_datafiles(scan_all_snapshots=True)
    file_paths = {df.file_path for df in datafiles}
    assert path1 in file_paths and path2 in file_paths


def test_deduplicate_data_files_removes_duplicates(iceberg_table, tmp_path):
    mt = MaintenanceTable(iceberg_table)
    # Write and add the same file twice (simulate duplicate)
    df = pa.Table.from_pylist([{"id": 1, "value": "A"}])
    path = str(tmp_path / "dup.parquet")
    pq.write_table(df, path)

    # Add the same file twice to the table
    mt.tbl.transaction().add_files([path]).commit_transaction()
    mt.tbl.transaction().add_files([path]).commit_transaction()

    # There should be duplicates
    all_datafiles = mt._get_all_datafiles(scan_all_snapshots=True)
    file_paths = [df.file_path for df in all_datafiles]
    assert file_paths.count(path) > 1

    # Deduplicate
    removed = mt.deduplicate_data_files(scan_all_partitions=True, scan_all_snapshots=True)

    # After deduplication, only one should remain
    all_datafiles_after = mt._get_all_datafiles(scan_all_snapshots=True)
    file_paths_after = [df.file_path for df in all_datafiles_after]
    assert file_paths_after.count(path) == 1
    assert all(isinstance(df, DataFile) for df in removed)
