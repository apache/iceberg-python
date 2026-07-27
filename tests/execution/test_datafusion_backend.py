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

"""Unit tests for the DataFusion compute backend.

Skipped entirely when datafusion is not installed.
"""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

datafusion = pytest.importorskip("datafusion")

from pyiceberg.execution.backends.datafusion_backend import (  # noqa: E402
    DataFusionComputeBackend,
    DataFusionReadBackend,
)
from pyiceberg.expressions import (  # noqa: E402
    AlwaysTrue,
    GreaterThan,
    Reference,
)
from pyiceberg.schema import Schema  # noqa: E402
from pyiceberg.types import IntegerType, NestedField, StringType  # noqa: E402


@pytest.fixture
def compute() -> DataFusionComputeBackend:
    return DataFusionComputeBackend()


@pytest.fixture
def simple_schema() -> Schema:
    return Schema(
        NestedField(1, "id", IntegerType(), required=False),
        NestedField(2, "name", StringType(), required=False),
    )


@pytest.fixture
def sample_batches() -> list[pa.RecordBatch]:
    return [
        pa.record_batch({"id": [3, 1, 4], "name": ["c", "a", "d"]}),
        pa.record_batch({"id": [1, 5, 9], "name": ["a2", "e", "i"]}),
    ]


class TestSupportsBasicProperties:
    def test_supports_bounded_memory(self, compute: DataFusionComputeBackend) -> None:
        assert compute.supports_bounded_memory is True


class TestSort:
    def test_sort_ascending(self, compute: DataFusionComputeBackend, sample_batches: list[pa.RecordBatch]) -> None:
        result = list(compute.sort(iter(sample_batches), [("id", "ascending")]))
        table = pa.Table.from_batches(result)
        assert table.column("id").to_pylist() == [1, 1, 3, 4, 5, 9]

    def test_sort_descending(self, compute: DataFusionComputeBackend, sample_batches: list[pa.RecordBatch]) -> None:
        result = list(compute.sort(iter(sample_batches), [("id", "descending")]))
        table = pa.Table.from_batches(result)
        assert table.column("id").to_pylist() == [9, 5, 4, 3, 1, 1]

    def test_sort_empty(self, compute: DataFusionComputeBackend) -> None:
        result = list(compute.sort(iter([]), [("id", "ascending")]))
        assert result == []

    def test_sort_multi_key(self, compute: DataFusionComputeBackend) -> None:
        batches = [pa.record_batch({"a": [1, 1, 2, 2], "b": [4, 3, 2, 1]})]
        result = list(compute.sort(iter(batches), [("a", "ascending"), ("b", "descending")]))
        table = pa.Table.from_batches(result)
        assert table.column("a").to_pylist() == [1, 1, 2, 2]
        assert table.column("b").to_pylist() == [4, 3, 2, 1]


class TestSortFromFiles:
    def test_sort_from_single_file(self, compute: DataFusionComputeBackend, tmp_path: Path) -> None:
        table = pa.table({"id": [5, 2, 8, 1], "value": ["e", "b", "h", "a"]})
        path = str(tmp_path / "data.parquet")
        pq.write_table(table, path)

        result = list(compute.sort_from_files([path], [("id", "ascending")], {}))
        result_table = pa.Table.from_batches(result)
        assert result_table.column("id").to_pylist() == [1, 2, 5, 8]

    def test_sort_from_multiple_files(self, compute: DataFusionComputeBackend, tmp_path: Path) -> None:
        pq.write_table(pa.table({"id": [5, 3]}), str(tmp_path / "a.parquet"))
        pq.write_table(pa.table({"id": [1, 7]}), str(tmp_path / "b.parquet"))

        result = list(
            compute.sort_from_files(
                [str(tmp_path / "a.parquet"), str(tmp_path / "b.parquet")],
                [("id", "ascending")],
                {},
            )
        )
        table = pa.Table.from_batches(result)
        assert table.column("id").to_pylist() == [1, 3, 5, 7]

    def test_sort_from_empty_list(self, compute: DataFusionComputeBackend) -> None:
        result = list(compute.sort_from_files([], [("id", "ascending")], {}))
        assert result == []


class TestAntiJoin:
    def test_single_column(self, compute: DataFusionComputeBackend) -> None:
        left = [pa.record_batch({"id": [1, 2, 3, 4, 5]})]
        right = [pa.record_batch({"id": [2, 4]})]
        result = list(compute.anti_join(iter(left), iter(right), ["id"]))
        table = pa.Table.from_batches(result)
        assert sorted(table.column("id").to_pylist()) == [1, 3, 5]

    def test_null_equals_null(self, compute: DataFusionComputeBackend) -> None:
        """IS NOT DISTINCT FROM: NULL matches NULL."""
        left = [pa.record_batch({"id": pa.array([1, 2, None, 4], type=pa.int64())})]
        right = [pa.record_batch({"id": pa.array([None, 2], type=pa.int64())})]
        result = list(compute.anti_join(iter(left), iter(right), ["id"]))
        table = pa.Table.from_batches(result)
        assert sorted(table.column("id").to_pylist()) == [1, 4]

    def test_empty_left(self, compute: DataFusionComputeBackend) -> None:
        result = list(compute.anti_join(iter([]), iter([pa.record_batch({"id": [1]})]), ["id"]))
        assert result == []

    def test_empty_right(self, compute: DataFusionComputeBackend) -> None:
        left = [pa.record_batch({"id": [1, 2, 3]})]
        result = list(compute.anti_join(iter(left), iter([]), ["id"]))
        table = pa.Table.from_batches(result)
        assert table.column("id").to_pylist() == [1, 2, 3]

    def test_multi_column(self, compute: DataFusionComputeBackend) -> None:
        left = [pa.record_batch({"a": [1, 1, 2], "b": ["x", "y", "x"]})]
        right = [pa.record_batch({"a": [1], "b": ["x"]})]
        result = list(compute.anti_join(iter(left), iter(right), ["a", "b"]))
        table = pa.Table.from_batches(result)
        assert table.num_rows == 2


class TestAntiJoinFromFiles:
    def test_basic(self, compute: DataFusionComputeBackend, tmp_path: Path) -> None:
        pq.write_table(pa.table({"id": [1, 2, 3, 4, 5]}), str(tmp_path / "left.parquet"))
        pq.write_table(pa.table({"id": [2, 4]}), str(tmp_path / "right.parquet"))

        result = list(
            compute.anti_join_from_files(
                [str(tmp_path / "left.parquet")],
                [str(tmp_path / "right.parquet")],
                ["id"],
                {},
            )
        )
        table = pa.Table.from_batches(result)
        assert sorted(table.column("id").to_pylist()) == [1, 3, 5]

    def test_null_matching(self, compute: DataFusionComputeBackend, tmp_path: Path) -> None:
        """IS NOT DISTINCT FROM semantics from files."""
        pq.write_table(
            pa.table({"id": pa.array([1, 2, None, 4], type=pa.int64())}),
            str(tmp_path / "left.parquet"),
        )
        pq.write_table(
            pa.table({"id": pa.array([None, 2], type=pa.int64())}),
            str(tmp_path / "right.parquet"),
        )

        result = list(
            compute.anti_join_from_files(
                [str(tmp_path / "left.parquet")],
                [str(tmp_path / "right.parquet")],
                ["id"],
                {},
            )
        )
        table = pa.Table.from_batches(result)
        assert sorted(table.column("id").to_pylist()) == [1, 4]


class TestFilter:
    def test_basic_filter(self, compute: DataFusionComputeBackend, simple_schema: Schema) -> None:
        from pyiceberg.expressions.visitors import bind

        batches = [pa.record_batch({"id": [1, 2, 3, 4, 5], "name": ["a", "b", "c", "d", "e"]})]
        bound = bind(simple_schema, GreaterThan(Reference("id"), 3), case_sensitive=True)
        result = list(compute.filter(iter(batches), bound))
        table = pa.Table.from_batches(result)
        assert table.column("id").to_pylist() == [4, 5]

    def test_filter_all_excluded(self, compute: DataFusionComputeBackend, simple_schema: Schema) -> None:
        from pyiceberg.expressions.visitors import bind

        batches = [pa.record_batch({"id": [1, 2, 3], "name": ["a", "b", "c"]})]
        bound = bind(simple_schema, GreaterThan(Reference("id"), 100), case_sensitive=True)
        result = list(compute.filter(iter(batches), bound))
        assert result == []


class TestApplyPositionalDeletes:
    def test_basic_positional_delete(self, compute: DataFusionComputeBackend, simple_schema: Schema, tmp_path: Path) -> None:
        # Write data file
        data = pa.table({"id": [1, 2, 3, 4, 5], "name": ["a", "b", "c", "d", "e"]})
        data_path = str(tmp_path / "data.parquet")
        pq.write_table(data, data_path)

        # Write position delete file (delete rows at positions 1 and 3 → id=2, id=4)
        deletes = pa.table({"file_path": [data_path, data_path], "pos": [1, 3]})
        del_path = str(tmp_path / "deletes.parquet")
        pq.write_table(deletes, del_path)

        result = list(compute.apply_positional_deletes(data_path, [del_path], simple_schema, {}))
        table = pa.Table.from_batches(result)
        assert sorted(table.column("id").to_pylist()) == [1, 3, 5]

    def test_no_matching_positions(self, compute: DataFusionComputeBackend, simple_schema: Schema, tmp_path: Path) -> None:
        data = pa.table({"id": [1, 2, 3], "name": ["a", "b", "c"]})
        data_path = str(tmp_path / "data.parquet")
        pq.write_table(data, data_path)

        # Delete file references a different data file
        deletes = pa.table({"file_path": ["other/file.parquet", "other/file.parquet"], "pos": [0, 1]})
        del_path = str(tmp_path / "deletes.parquet")
        pq.write_table(deletes, del_path)

        result = list(compute.apply_positional_deletes(data_path, [del_path], simple_schema, {}))
        table = pa.Table.from_batches(result)
        assert table.column("id").to_pylist() == [1, 2, 3]


class TestReadBackend:
    def test_read_basic(self, simple_schema: Schema, tmp_path: Path) -> None:
        read = DataFusionReadBackend()
        table = pa.table({"id": [1, 2, 3], "name": ["a", "b", "c"]})
        path = str(tmp_path / "data.parquet")
        pq.write_table(table, path)

        result = list(read.read_parquet(path, simple_schema, AlwaysTrue(), {}))
        result_table = pa.Table.from_batches(result)
        assert result_table.num_rows == 3
        assert sorted(result_table.column("id").to_pylist()) == [1, 2, 3]

    def test_read_with_projection(self, tmp_path: Path) -> None:
        read = DataFusionReadBackend()
        table = pa.table({"id": [1, 2, 3], "name": ["a", "b", "c"], "value": [10, 20, 30]})
        path = str(tmp_path / "data.parquet")
        pq.write_table(table, path)

        # Project only id column
        projected = Schema(NestedField(1, "id", IntegerType(), required=False))
        result = list(read.read_parquet(path, projected, AlwaysTrue(), {}))
        result_table = pa.Table.from_batches(result)
        assert result_table.column_names == ["id"]
        assert result_table.num_rows == 3


class TestMemoryLimit:
    def test_sort_with_small_memory_limit(self, compute: DataFusionComputeBackend, tmp_path: Path) -> None:
        """Sort still works with a small memory limit (spills to disk)."""
        # 10K rows to create some memory pressure with a modest limit
        table = pa.table({"id": list(range(10000, 0, -1))})
        path = str(tmp_path / "data.parquet")
        pq.write_table(table, path)

        # 32 MB limit — small enough to demonstrate bounded execution, large enough
        # for DataFusion's sort spill reservation overhead.
        result = list(compute.sort_from_files([path], [("id", "ascending")], {}, memory_limit=32 * 1024 * 1024))
        result_table = pa.Table.from_batches(result)
        assert result_table.column("id").to_pylist() == list(range(1, 10001))
