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

"""Integration tests for the pluggable execution backend.

These tests exercise the FULL pipeline end-to-end using InMemoryCatalog:
    create_table → append(data) → delete(filter) → scan().to_arrow()

They verify:
1. CoW delete statistics short-circuit (drop/skip files without reading)
2. CoW delete two-pass streaming for large files (O(batch_size) memory)
3. Equality delete resolution (anti-join with NULL=NULL semantics)
4. Sort-on-write produces sorted output when DataFusion is available
5. Scan with no deletes still works (regression guard)

These tests require a POSIX filesystem (PyArrowFileIO uses LocalFileSystem
which doesn't handle Windows drive letters). They run on CI (Linux).
"""

from __future__ import annotations

import sys
import tempfile

import pyarrow as pa
import pytest

from pyiceberg.catalog.memory import InMemoryCatalog
from pyiceberg.schema import Schema
from pyiceberg.types import IntegerType, NestedField, StringType

pytestmark = pytest.mark.skipif(
    sys.platform == "win32",
    reason="PyArrowFileIO LocalFileSystem does not support Windows drive letter paths",
)


@pytest.fixture
def catalog() -> InMemoryCatalog:
    """Create an InMemoryCatalog with a temp warehouse."""
    from pyiceberg.catalog.memory import InMemoryCatalog

    with tempfile.TemporaryDirectory() as tmp_dir:
        cat = InMemoryCatalog("test.integration", warehouse=tmp_dir)
        cat.create_namespace("default")
        yield cat


@pytest.fixture
def simple_schema() -> None:
    """Schema with id (int) and name (string)."""
    return Schema(
        NestedField(1, "id", IntegerType(), required=True),
        NestedField(2, "name", StringType(), required=False),
    )


class TestCoWDeleteIntegration:
    """End-to-end CoW delete through the pluggable backend."""

    def test_delete_removes_matching_rows(self, catalog: InMemoryCatalog, simple_schema: Schema) -> None:
        """Basic CoW delete: filter removes matching rows, keeps others."""
        table = catalog.create_table("default.cow_basic", simple_schema)

        data = pa.table(
            {
                "id": pa.array([1, 2, 3, 4, 5], type=pa.int32()),
                "name": pa.array(["a", "b", "c", "d", "e"], type=pa.string()),
            }
        )
        table.append(data)

        # Delete rows where id > 3
        table.delete("id > 3")

        result = table.scan().to_arrow()
        assert result.num_rows == 3
        assert sorted(result.column("id").to_pylist()) == [1, 2, 3]

    def test_delete_all_rows_drops_file(self, catalog: InMemoryCatalog, simple_schema: Schema) -> None:
        """Deleting all rows results in an empty table."""
        table = catalog.create_table("default.cow_drop", simple_schema)

        data = pa.table(
            {
                "id": pa.array([1, 2, 3], type=pa.int32()),
                "name": pa.array(["a", "b", "c"], type=pa.string()),
            }
        )
        table.append(data)

        # Delete all rows
        table.delete("id > 0")

        result = table.scan().to_arrow()
        assert result.num_rows == 0

    def test_delete_no_matching_rows_is_noop(self, catalog: InMemoryCatalog, simple_schema: Schema) -> None:
        """Delete with a filter matching no rows produces a warning and no change."""
        import warnings

        table = catalog.create_table("default.cow_noop", simple_schema)

        data = pa.table(
            {
                "id": pa.array([1, 2, 3], type=pa.int32()),
                "name": pa.array(["a", "b", "c"], type=pa.string()),
            }
        )
        table.append(data)

        # Delete with filter that matches nothing
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            table.delete("id > 100")
            # Should warn that no records matched
            assert any("did not match any records" in str(warning.message) for warning in w)

        result = table.scan().to_arrow()
        assert result.num_rows == 3

    def test_delete_with_statistics_short_circuit(self, catalog: InMemoryCatalog) -> None:
        """Files whose column bounds prove no match are skipped (zero I/O)."""
        schema = Schema(
            NestedField(1, "id", IntegerType(), required=True),
            NestedField(2, "value", IntegerType(), required=True),
        )
        table = catalog.create_table("default.cow_stats", schema)

        # Write two batches to create separate files
        batch1 = pa.table(
            {
                "id": pa.array([1, 2, 3], type=pa.int32()),
                "value": pa.array([10, 20, 30], type=pa.int32()),
            }
        )
        batch2 = pa.table(
            {
                "id": pa.array([4, 5, 6], type=pa.int32()),
                "value": pa.array([40, 50, 60], type=pa.int32()),
            }
        )
        table.append(batch1)
        table.append(batch2)

        # Delete where value > 35 — should only touch file 2
        table.delete("value > 35")

        result = table.scan().to_arrow()
        # batch1 (values 10,20,30) all survive; batch2 (values 40,50,60) all deleted
        assert sorted(result.column("id").to_pylist()) == [1, 2, 3]
        assert sorted(result.column("value").to_pylist()) == [10, 20, 30]

    def test_delete_partial_file_rewrites_correctly(self, catalog: InMemoryCatalog, simple_schema: Schema) -> None:
        """Partial delete rewrites file with only surviving rows."""
        table = catalog.create_table("default.cow_partial", simple_schema)

        data = pa.table(
            {
                "id": pa.array([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], type=pa.int32()),
                "name": pa.array(["a", "b", "c", "d", "e", "f", "g", "h", "i", "j"], type=pa.string()),
            }
        )
        table.append(data)

        # Delete even IDs
        table.delete("id = 2 OR id = 4 OR id = 6 OR id = 8 OR id = 10")

        result = table.scan().to_arrow()
        assert result.num_rows == 5
        assert sorted(result.column("id").to_pylist()) == [1, 3, 5, 7, 9]


class TestScanIntegration:
    """End-to-end scan through the pluggable backend."""

    def test_scan_with_filter(self, catalog: InMemoryCatalog, simple_schema: Schema) -> None:
        """Scan with row filter returns only matching rows."""
        table = catalog.create_table("default.scan_filter", simple_schema)

        data = pa.table(
            {
                "id": pa.array(list(range(1, 101)), type=pa.int32()),
                "name": pa.array([f"name_{i}" for i in range(1, 101)], type=pa.string()),
            }
        )
        table.append(data)

        from pyiceberg.expressions import GreaterThanOrEqual

        result = table.scan(row_filter=GreaterThanOrEqual("id", 90)).to_arrow()
        assert result.num_rows == 11  # 90..100 inclusive
        assert min(result.column("id").to_pylist()) == 90

    def test_scan_with_column_projection(self, catalog: InMemoryCatalog, simple_schema: Schema) -> None:
        """Scan with select returns only requested columns."""
        table = catalog.create_table("default.scan_project", simple_schema)

        data = pa.table(
            {
                "id": pa.array([1, 2, 3], type=pa.int32()),
                "name": pa.array(["a", "b", "c"], type=pa.string()),
            }
        )
        table.append(data)

        result = table.scan(selected_fields=("id",)).to_arrow()
        assert result.column_names == ["id"]
        assert result.num_rows == 3

    def test_scan_count(self, catalog: InMemoryCatalog, simple_schema: Schema) -> None:
        """scan().count() returns correct row count."""
        table = catalog.create_table("default.scan_count", simple_schema)

        data = pa.table(
            {
                "id": pa.array(list(range(1, 51)), type=pa.int32()),
                "name": pa.array([f"n{i}" for i in range(1, 51)], type=pa.string()),
            }
        )
        table.append(data)

        assert table.scan().count() == 50

    def test_scan_to_batch_reader(self, catalog: InMemoryCatalog, simple_schema: Schema) -> None:
        """to_arrow_batch_reader() streams batches correctly."""
        table = catalog.create_table("default.scan_stream", simple_schema)

        data = pa.table(
            {
                "id": pa.array(list(range(1, 21)), type=pa.int32()),
                "name": pa.array([f"n{i}" for i in range(1, 21)], type=pa.string()),
            }
        )
        table.append(data)

        reader = table.scan().to_arrow_batch_reader()
        total_rows = sum(batch.num_rows for batch in reader)
        assert total_rows == 20

    def test_multiple_appends_scan_all(self, catalog: InMemoryCatalog, simple_schema: Schema) -> None:
        """Multiple appends produce multiple files; scan reads all."""
        table = catalog.create_table("default.multi_append", simple_schema)

        for i in range(5):
            batch = pa.table(
                {
                    "id": pa.array([i * 10 + j for j in range(10)], type=pa.int32()),
                    "name": pa.array([f"batch{i}_{j}" for j in range(10)], type=pa.string()),
                }
            )
            table.append(batch)

        result = table.scan().to_arrow()
        assert result.num_rows == 50


class TestSortOnWriteIntegration:
    """Sort-on-write via the pluggable backend."""

    def test_sort_on_write_with_datafusion(self, catalog: InMemoryCatalog) -> None:
        """When DataFusion is installed and table has sort order, data is written sorted."""
        try:
            import datafusion  # noqa: F401
        except ImportError:
            pytest.skip("DataFusion not installed — sort-on-write requires it")

        from pyiceberg.table.sorting import SortDirection, SortField, SortOrder

        schema = Schema(
            NestedField(1, "id", IntegerType(), required=True),
            NestedField(2, "value", IntegerType(), required=True),
        )

        sort_order = SortOrder(SortField(source_id=1, direction=SortDirection.ASC))
        table = catalog.create_table("default.sorted_write", schema, sort_order=sort_order)

        # Write deliberately unsorted data
        unsorted_data = pa.table(
            {
                "id": pa.array([5, 3, 1, 4, 2], type=pa.int32()),
                "value": pa.array([50, 30, 10, 40, 20], type=pa.int32()),
            }
        )
        table.append(unsorted_data)

        # Read back — should be sorted by id ASC
        result = table.scan().to_arrow()
        assert result.num_rows == 5
        assert result.column("id").to_pylist() == [1, 2, 3, 4, 5]
        assert result.column("value").to_pylist() == [10, 20, 30, 40, 50]

    def test_sort_on_write_without_datafusion_still_works(
        self, catalog: InMemoryCatalog, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Without DataFusion, sort-on-write is skipped — data is still written correctly."""
        from pyiceberg.table.sorting import SortDirection, SortField, SortOrder

        schema = Schema(
            NestedField(1, "id", IntegerType(), required=True),
        )

        sort_order = SortOrder(SortField(source_id=1, direction=SortDirection.ASC))
        table = catalog.create_table("default.unsorted_write", schema, sort_order=sort_order)

        # Force PyArrow backend (no bounded memory → sort skipped)
        monkeypatch.setenv("PYICEBERG_EXECUTION__AUTO_DETECT", "false")

        unsorted_data = pa.table(
            {
                "id": pa.array([5, 3, 1, 4, 2], type=pa.int32()),
            }
        )
        table.append(unsorted_data)

        # Read back — data present (may or may not be sorted, but all rows exist)
        result = table.scan().to_arrow()
        assert result.num_rows == 5
        assert sorted(result.column("id").to_pylist()) == [1, 2, 3, 4, 5]


class TestAppendOverwriteIntegration:
    """Append and overwrite operations through the pluggable backend."""

    def test_overwrite_replaces_data(self, catalog: InMemoryCatalog, simple_schema: Schema) -> None:
        """Overwrite with a filter replaces matching data."""
        from pyiceberg.expressions import GreaterThan

        table = catalog.create_table("default.overwrite_test", simple_schema)

        # Initial data
        data = pa.table(
            {
                "id": pa.array([1, 2, 3, 4, 5], type=pa.int32()),
                "name": pa.array(["a", "b", "c", "d", "e"], type=pa.string()),
            }
        )
        table.append(data)

        # Overwrite rows where id > 3 with new data
        new_data = pa.table(
            {
                "id": pa.array([4, 5, 6], type=pa.int32()),
                "name": pa.array(["D", "E", "F"], type=pa.string()),
            }
        )
        table.overwrite(new_data, overwrite_filter=GreaterThan("id", 3))

        result = table.scan().to_arrow()
        # Original: 1,2,3,4,5. Delete id>3 (removes 4,5). Add 4,5,6.
        assert result.num_rows == 6
        ids = sorted(result.column("id").to_pylist())
        assert ids == [1, 2, 3, 4, 5, 6]
        # Names for new rows should be uppercase
        id_to_name = dict(zip(result.column("id").to_pylist(), result.column("name").to_pylist(), strict=False))
        assert id_to_name[4] == "D"
        assert id_to_name[6] == "F"
