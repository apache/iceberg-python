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
import os
import uuid
from pathlib import Path
from typing import List, Set, Generator

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from pyiceberg.manifest import DataFile
from pyiceberg.table import Table
from pyiceberg.table.maintenance import MaintenanceTable
from tests.catalog.test_base import InMemoryCatalog


@pytest.fixture
def iceberg_catalog(tmp_path: Path) -> Generator[InMemoryCatalog, None, None]:
    catalog = InMemoryCatalog("test.in_memory.catalog", warehouse=tmp_path.absolute().as_posix())
    catalog.create_namespace("default")
    yield catalog
    # Clean up SQLAlchemy engine connections
    if hasattr(catalog, 'engine'):
        try:
            catalog.engine.dispose()
        except Exception:
            pass


@pytest.fixture
def dupe_data_file_path(tmp_path: Path) -> Path:
    unique_id = uuid.uuid4()
    return tmp_path / f"{unique_id}" / "file1.parquet"


@pytest.fixture
def prepopulated_table(iceberg_catalog: InMemoryCatalog, dupe_data_file_path: Path) -> Generator[Table, None, None]:
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
    tx.commit_transaction()
    tx2 = table.transaction()
    tx2.add_files([str(dupe_data_file_path)], check_duplicate_files=False)
    tx2.commit_transaction()

    yield table
    
    # Cleanup table's catalog connections
    if hasattr(table, '_catalog') and hasattr(table._catalog, 'engine'):
        try:
            table._catalog.engine.dispose()
        except Exception:
            pass


def test_overwrite_removes_only_selected_datafile(prepopulated_table: Table, dupe_data_file_path: Path) -> None:
    mt = MaintenanceTable(tbl=prepopulated_table)

    removed_files: List[DataFile] = mt.deduplicate_data_files()

    file_names_after: Set[str] = {df.file_path.split("/")[-1] for df in mt._get_all_datafiles()}
    # Only one file with the same name should remain after deduplication
    assert dupe_data_file_path.name in file_names_after, f"Expected {dupe_data_file_path.name} to remain in the table"
    assert len(file_names_after) == 1, "Expected only one unique file name to remain after deduplication"
    # All removed files should have the same file name
    assert all(df.file_path.split("/")[-1] == dupe_data_file_path.name for df in removed_files), (
        "All removed files should be duplicates by name"
    )


def test_get_all_datafiles_current_snapshot(prepopulated_table: Table, dupe_data_file_path: Path) -> None:
    mt = MaintenanceTable(tbl=prepopulated_table)

    datafiles: List[DataFile] = mt._get_all_datafiles()
    file_paths: Set[str] = {df.file_path.split("/")[-1] for df in datafiles}
    assert dupe_data_file_path.name in file_paths


def test_get_all_datafiles_all_snapshots(prepopulated_table: Table, dupe_data_file_path: Path) -> None:
    try:
        mt = MaintenanceTable(tbl=prepopulated_table)

        datafiles: List[DataFile] = mt._get_all_datafiles()
        file_paths: Set[str] = {df.file_path.split("/")[-1] for df in datafiles}
        assert dupe_data_file_path.name in file_paths
    finally:
        # Ensure catalog connections are properly closed
        if hasattr(prepopulated_table, '_catalog'):
            catalog = prepopulated_table._catalog
            if hasattr(catalog, '_connection') and catalog._connection is not None:
                try:
                    catalog._connection.close()
                except Exception:
                    pass


def test_deduplicate_data_files_removes_duplicates_in_current_snapshot(
    prepopulated_table: Table, dupe_data_file_path: Path
) -> None:
    try:
        mt = MaintenanceTable(tbl=prepopulated_table)

        all_datafiles: List[DataFile] = mt._get_all_datafiles()
        file_names: List[str] = [os.path.basename(df.file_path) for df in all_datafiles]
        # There should be more than one reference before deduplication
        assert file_names.count(dupe_data_file_path.name) > 1, (
            f"Expected multiple references to {dupe_data_file_path.name} before deduplication"
        )
        removed: List[DataFile] = mt.deduplicate_data_files()

        all_datafiles_after: List[DataFile] = mt._get_all_datafiles()
        file_names_after: List[str] = [os.path.basename(df.file_path) for df in all_datafiles_after]
        # Only one reference should remain after deduplication
        assert file_names_after.count(dupe_data_file_path.name) == 1
        assert all(isinstance(df, DataFile) for df in removed)
    finally:
        # Ensure we close the table's catalog connection
        if hasattr(prepopulated_table, "_catalog"):
            catalog = prepopulated_table._catalog
            if hasattr(catalog, "connection") and catalog.connection is not None:
                catalog.connection.close()
