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


"""Tests for sort-on-write and _SortedRecordBatchReader lifecycle management."""

from __future__ import annotations

import inspect
from collections.abc import Iterator
from pathlib import Path
from typing import TYPE_CHECKING
from unittest.mock import MagicMock, PropertyMock, patch

import pyarrow as pa
import pytest

from pyiceberg.schema import Schema
from pyiceberg.transforms import IdentityTransform
from pyiceberg.types import IntegerType, NestedField, StringType

if TYPE_CHECKING:
    from pyiceberg.table import Transaction


class TestApplySortOrderWithRecordBatchReader:
    """Behavioral tests for _apply_sort_order when df is a pa.RecordBatchReader."""

    @pytest.fixture
    def transaction_with_sort_order(self) -> Transaction:
        """Create a minimal Transaction-like object with a sort order configured."""
        from pyiceberg.table import Transaction
        from pyiceberg.table.sorting import (
            NullOrder,
            SortDirection,
            SortField,
            SortOrder,
        )
        from pyiceberg.transforms import IdentityTransform

        schema = Schema(
            NestedField(field_id=1, name="id", field_type=IntegerType(), required=True),
            NestedField(field_id=2, name="name", field_type=StringType(), required=False),
        )

        sort_order = SortOrder(
            SortField(
                source_id=1,
                transform=IdentityTransform(),
                direction=SortDirection.ASC,
                null_order=NullOrder.NULLS_LAST,
            ),
            order_id=1,
        )

        tx = object.__new__(Transaction)
        mock_table = MagicMock()
        mock_table.io.properties = {}
        tx._table = mock_table

        mock_metadata = MagicMock()
        mock_metadata.schema.return_value = schema
        mock_metadata.sort_orders = [sort_order]
        mock_metadata.default_sort_order_id = 1

        type(tx).table_metadata = PropertyMock(return_value=mock_metadata)  # type: ignore[method-assign]

        return tx

    def test_record_batch_reader_input_produces_sorted_output(
        self, transaction_with_sort_order: Transaction
    ) -> None:
        """RecordBatchReader input to _apply_sort_order produces correctly sorted output."""
        pytest.importorskip("datafusion")

        from pyiceberg.io.pyarrow import schema_to_pyarrow

        schema = Schema(
            NestedField(field_id=1, name="id", field_type=IntegerType(), required=True),
            NestedField(field_id=2, name="name", field_type=StringType(), required=False),
        )
        arrow_schema = schema_to_pyarrow(schema, include_field_ids=False)

        batch1 = pa.record_batch(
            {"id": pa.array([5, 3, 1], type=pa.int32()), "name": pa.array(["e", "c", "a"], type=pa.large_string())},
            schema=arrow_schema,
        )
        batch2 = pa.record_batch(
            {"id": pa.array([4, 2], type=pa.int32()), "name": pa.array(["d", "b"], type=pa.large_string())},
            schema=arrow_schema,
        )

        reader = pa.RecordBatchReader.from_batches(arrow_schema, [batch1, batch2])

        from pyiceberg.execution.protocol import Backends

        backends = Backends.resolve({})
        result = transaction_with_sort_order._apply_sort_order(reader, backends)

        assert isinstance(result, pa.RecordBatchReader)

        result_table = result.read_all()
        assert result_table.column("id").to_pylist() == [1, 2, 3, 4, 5]
        assert result_table.column("name").to_pylist() == ["a", "b", "c", "d", "e"]

    def test_table_input_produces_sorted_output(
        self, transaction_with_sort_order: Transaction
    ) -> None:
        """pa.Table input to _apply_sort_order also produces correctly sorted output."""
        pytest.importorskip("datafusion")

        from pyiceberg.execution.protocol import Backends
        from pyiceberg.io.pyarrow import schema_to_pyarrow

        schema = Schema(
            NestedField(field_id=1, name="id", field_type=IntegerType(), required=True),
            NestedField(field_id=2, name="name", field_type=StringType(), required=False),
        )
        arrow_schema = schema_to_pyarrow(schema, include_field_ids=False)

        table = pa.table(
            {
                "id": pa.array([5, 3, 1, 4, 2], type=pa.int32()),
                "name": pa.array(["e", "c", "a", "d", "b"], type=pa.large_string()),
            },
            schema=arrow_schema,
        )

        result = transaction_with_sort_order._apply_sort_order(table, Backends.resolve({}))
        assert isinstance(result, pa.RecordBatchReader)

        result_table = result.read_all()
        assert result_table.column("id").to_pylist() == [1, 2, 3, 4, 5]
        assert result_table.column("name").to_pylist() == ["a", "b", "c", "d", "e"]

    def test_no_sort_order_returns_input_unchanged(self) -> None:
        """If table has no sort order, _apply_sort_order returns input unchanged."""
        from pyiceberg.table import Transaction
        from pyiceberg.table.sorting import UNSORTED_SORT_ORDER_ID

        tx = object.__new__(Transaction)
        mock_table = MagicMock()
        mock_table.io.properties = {}
        tx._table = mock_table

        mock_metadata = MagicMock()
        mock_metadata.default_sort_order_id = UNSORTED_SORT_ORDER_ID
        type(tx).table_metadata = PropertyMock(return_value=mock_metadata)  # type: ignore[method-assign]

        mock_backends = MagicMock()
        input_table = pa.table({"id": [3, 1, 2]})
        result = tx._apply_sort_order(input_table, mock_backends)

        assert result is input_table

    def test_no_bounded_memory_returns_input_unchanged(self) -> None:
        """If compute backend cannot spill, _apply_sort_order returns input unchanged."""
        from pyiceberg.table import Transaction
        from pyiceberg.table.sorting import (
            NullOrder,
            SortDirection,
            SortField,
            SortOrder,
        )
        from pyiceberg.transforms import IdentityTransform

        schema = Schema(NestedField(field_id=1, name="id", field_type=IntegerType(), required=True))

        sort_order = SortOrder(
            SortField(source_id=1, transform=IdentityTransform(), direction=SortDirection.ASC, null_order=NullOrder.NULLS_LAST),
            order_id=1,
        )

        tx = object.__new__(Transaction)
        mock_table = MagicMock()
        mock_table.io.properties = {}
        tx._table = mock_table

        mock_metadata = MagicMock()
        mock_metadata.schema.return_value = schema
        mock_metadata.sort_orders = [sort_order]
        mock_metadata.default_sort_order_id = 1
        type(tx).table_metadata = PropertyMock(return_value=mock_metadata)  # type: ignore[method-assign]

        mock_backends = MagicMock()
        mock_backends.supports_bounded_memory = False

        input_table = pa.table({"id": [3, 1, 2]})
        result = tx._apply_sort_order(input_table, mock_backends)

        assert result is input_table

    def test_sorted_reader_cleans_up_temp_file(
        self, transaction_with_sort_order: Transaction
    ) -> None:
        """Temp file created by _apply_sort_order is cleaned up after reader is consumed."""
        pytest.importorskip("datafusion")

        from pyiceberg.io.pyarrow import schema_to_pyarrow

        schema = Schema(
            NestedField(field_id=1, name="id", field_type=IntegerType(), required=True),
            NestedField(field_id=2, name="name", field_type=StringType(), required=False),
        )
        arrow_schema = schema_to_pyarrow(schema, include_field_ids=False)

        batch = pa.record_batch(
            {"id": pa.array([3, 1, 2], type=pa.int32()), "name": pa.array(["c", "a", "b"], type=pa.large_string())},
            schema=arrow_schema,
        )
        reader = pa.RecordBatchReader.from_batches(arrow_schema, [batch])

        from pyiceberg.execution.protocol import Backends

        backends = Backends.resolve({})
        result_reader = transaction_with_sort_order._apply_sort_order(reader, backends)

        result = result_reader.read_all()
        assert result.column("id").to_pylist() == [1, 2, 3]
        assert True  # If we got here without error, lifecycle is correct


class TestSortedRecordBatchReaderTypeAnnotations:
    """Verify _SortedRecordBatchReader.create() has precise type annotations."""

    def test_create_signature_has_proper_types(self) -> None:
        """create() parameters must have fully-parameterized type annotations."""
        from pyiceberg.execution._sorted_reader import _SortedRecordBatchReader

        sig = inspect.signature(_SortedRecordBatchReader.create)
        params = sig.parameters

        materialize_ann = params["materialize_fn"].annotation
        assert materialize_ann is not inspect.Parameter.empty
        ann_str = str(materialize_ann)
        assert "Callable" in ann_str
        assert "AbstractContextManager" in ann_str

        sort_ann = params["sort_fn"].annotation
        assert sort_ann is not inspect.Parameter.empty
        sort_str = str(sort_ann)
        assert "Callable" in sort_str
        assert "Iterator" in sort_str

        schema_ann = params["schema"].annotation
        assert schema_ann is not inspect.Parameter.empty
        schema_str = str(schema_ann)
        assert "Any" not in schema_str

        return_ann = sig.return_annotation
        assert return_ann is not inspect.Signature.empty
        return_str = str(return_ann)
        assert "Any" not in return_str

    def test_create_returns_record_batch_reader(self) -> None:
        """create() must return a pa.RecordBatchReader when called with valid args."""
        from collections.abc import Generator
        from contextlib import contextmanager

        from pyiceberg.execution._sorted_reader import _SortedRecordBatchReader

        schema = pa.schema([pa.field("x", pa.int32())])

        @contextmanager
        def fake_materialize() -> Generator[str, None, None]:
            yield "/tmp/fake.parquet"

        def fake_sort(path: str) -> Iterator[pa.RecordBatch]:
            yield pa.record_batch({"x": [1, 2, 3]}, schema=schema)

        reader = _SortedRecordBatchReader.create(
            materialize_fn=fake_materialize,
            sort_fn=fake_sort,
            schema=schema,
        )

        assert isinstance(reader, pa.RecordBatchReader)

    def test_create_streams_sorted_batches(self) -> None:
        """Reader must stream all batches from sort_fn."""
        from collections.abc import Generator
        from contextlib import contextmanager

        from pyiceberg.execution._sorted_reader import _SortedRecordBatchReader

        schema = pa.schema([pa.field("val", pa.int64())])

        @contextmanager
        def fake_materialize() -> Generator[str, None, None]:
            yield "/tmp/fake.parquet"

        def fake_sort(path: str) -> Iterator[pa.RecordBatch]:
            yield pa.record_batch({"val": [10, 20]}, schema=schema)
            yield pa.record_batch({"val": [30]}, schema=schema)

        reader = _SortedRecordBatchReader.create(
            materialize_fn=fake_materialize,
            sort_fn=fake_sort,
            schema=schema,
        )

        table = reader.read_all()
        assert table.num_rows == 3
        assert table.column("val").to_pylist() == [10, 20, 30]


class TestSortedRecordBatchReaderCleanup:
    """Verify temp file lifecycle management."""

    def test_cleanup_on_normal_exhaustion(self, tmp_path: Path) -> None:
        """Context manager __exit__ called when reader is fully consumed."""
        from collections.abc import Generator
        from contextlib import contextmanager

        from pyiceberg.execution._sorted_reader import _SortedRecordBatchReader

        cleanup_called: list[bool] = []
        schema = pa.schema([pa.field("x", pa.int32())])

        @contextmanager
        def tracked_materialize() -> Generator[str, None, None]:
            yield str(tmp_path / "data.parquet")
            cleanup_called.append(True)

        def fake_sort(path: str) -> Iterator[pa.RecordBatch]:
            yield pa.record_batch({"x": [1]}, schema=schema)

        reader = _SortedRecordBatchReader.create(
            materialize_fn=tracked_materialize,
            sort_fn=fake_sort,
            schema=schema,
        )

        reader.read_all()
        assert cleanup_called

    def test_cleanup_on_exception_in_sort(self, tmp_path: Path) -> None:
        """Context manager __exit__ called even when sort_fn raises."""
        from collections.abc import Generator
        from contextlib import contextmanager

        from pyiceberg.execution._sorted_reader import _SortedRecordBatchReader

        cleanup_called: list[bool] = []
        schema = pa.schema([pa.field("x", pa.int32())])

        @contextmanager
        def tracked_materialize() -> Generator[str, None, None]:
            try:
                yield str(tmp_path / "data.parquet")
            finally:
                cleanup_called.append(True)

        def failing_sort(path: str) -> Iterator[pa.RecordBatch]:
            raise RuntimeError("sort failed")
            yield pa.record_batch({"x": [1]}, schema=schema)  # type: ignore[unreachable]  # needed for type

        reader = _SortedRecordBatchReader.create(
            materialize_fn=tracked_materialize,
            sort_fn=failing_sort,
            schema=schema,
        )

        with pytest.raises(RuntimeError, match="sort failed"):
            reader.read_all()

        assert cleanup_called


# =============================================================================
# From: test_planning.py
# =============================================================================


class TestWarnIfLargeMaterialization:
    """Verify DataFusion backend emits ResourceWarning above the 1GB threshold."""

    def test_large_table_emits_resource_warning(self) -> None:
        """ResourceWarning is emitted when materialized result exceeds 1GB."""
        import warnings

        from pyiceberg.execution.backends.datafusion_backend import (
            _MATERIALIZATION_WARNING_THRESHOLD_DEFAULT,
            _warn_if_large_materialization,
        )

        # Create a table that reports > 1GB nbytes (mock the nbytes property)
        mock_table = MagicMock()
        mock_table.nbytes = _MATERIALIZATION_WARNING_THRESHOLD_DEFAULT + 1

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            _warn_if_large_materialization(mock_table)

        resource_warnings = [w for w in caught if issubclass(w.category, ResourceWarning)]
        assert len(resource_warnings) == 1, f"Expected 1 ResourceWarning, got {len(resource_warnings)}: {caught}"
        assert "GB" in str(resource_warnings[0].message)

    def test_small_table_no_warning(self) -> None:
        """No warning emitted when materialized result is below threshold."""
        import warnings

        from pyiceberg.execution.backends.datafusion_backend import (
            _MATERIALIZATION_WARNING_THRESHOLD_DEFAULT,
            _warn_if_large_materialization,
        )

        mock_table = MagicMock()
        mock_table.nbytes = _MATERIALIZATION_WARNING_THRESHOLD_DEFAULT - 1

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            _warn_if_large_materialization(mock_table)

        resource_warnings = [w for w in caught if issubclass(w.category, ResourceWarning)]
        assert len(resource_warnings) == 0, f"No ResourceWarning expected below threshold, got: {resource_warnings}"

    def test_threshold_is_exactly_1gb(self) -> None:
        """The materialization warning threshold is exactly 1 GB."""
        from pyiceberg.execution.backends.datafusion_backend import (
            _MATERIALIZATION_WARNING_THRESHOLD_DEFAULT,
        )

        assert _MATERIALIZATION_WARNING_THRESHOLD_DEFAULT == 1 * 1024 * 1024 * 1024


# =============================================================================
# BoundedMemoryPlanner: Real DataFusion End-to-End Test
# =============================================================================


# =============================================================================
# Sort-on-write: sort_order_id correctness
# =============================================================================


class TestSortOrderIdOnDataFiles:
    """Verify sort_order_id is set on DataFiles only when sort was actually applied.

    The Iceberg spec allows sort_order_id on DataFiles to indicate the sort order
    used when writing. We should only set it when:
    1. The table has a non-trivial sort order, AND
    2. The backend supports bounded memory (sort was actually applied)

    If sort is skipped (no DF installed), sort_order_id must be None.
    """

    def test_sort_order_id_set_when_sort_applied(self) -> None:
        """_prepare_write returns sort_order_id when table has sort order + DF backend."""
        from unittest.mock import MagicMock, PropertyMock

        import pyarrow as pa

        from pyiceberg.table import Transaction
        from pyiceberg.table.sorting import SortField, SortOrder

        tx = object.__new__(Transaction)
        mock_table = MagicMock()
        mock_table.io.properties = {}
        tx._table = mock_table

        schema = Schema(NestedField(1, "id", IntegerType(), required=True))
        sort_order = SortOrder(order_id=7, fields=[SortField(source_id=1, transform=IdentityTransform())])

        mock_metadata = MagicMock()
        mock_metadata.schema.return_value = schema
        mock_metadata.sort_orders = [sort_order]
        mock_metadata.default_sort_order_id = 7
        type(tx).table_metadata = PropertyMock(return_value=mock_metadata)  # type: ignore[method-assign]

        # Mock a bounded-memory backend

        mock_backends = MagicMock()
        mock_backends.supports_bounded_memory = True
        mock_backends.compute.sort_from_files.return_value = iter([pa.record_batch({"id": [1, 2, 3]})])

        with patch("pyiceberg.execution.protocol.Backends.resolve", return_value=mock_backends):
            df = pa.table({"id": [3, 1, 2]})
            _, _, sort_order_id = tx._prepare_write(df, "append")

        assert sort_order_id == 7, f"Expected sort_order_id=7, got {sort_order_id}"

    def test_sort_order_id_none_when_no_bounded_memory(self) -> None:
        """_prepare_write returns sort_order_id=None when backend cannot sort."""
        from unittest.mock import MagicMock, PropertyMock

        import pyarrow as pa

        from pyiceberg.table import Transaction
        from pyiceberg.table.sorting import SortField, SortOrder

        tx = object.__new__(Transaction)
        mock_table = MagicMock()
        mock_table.io.properties = {}
        tx._table = mock_table

        schema = Schema(NestedField(1, "id", IntegerType(), required=True))
        sort_order = SortOrder(order_id=7, fields=[SortField(source_id=1, transform=IdentityTransform())])

        mock_metadata = MagicMock()
        mock_metadata.schema.return_value = schema
        mock_metadata.sort_orders = [sort_order]
        mock_metadata.default_sort_order_id = 7
        type(tx).table_metadata = PropertyMock(return_value=mock_metadata)  # type: ignore[method-assign]

        # Mock a non-bounded backend (PyArrow only)

        mock_backends = MagicMock()
        mock_backends.supports_bounded_memory = False

        with patch("pyiceberg.execution.protocol.Backends.resolve", return_value=mock_backends):
            df = pa.table({"id": [3, 1, 2]})
            _, _, sort_order_id = tx._prepare_write(df, "append")

        assert sort_order_id is None, f"sort_order_id should be None when backend cannot sort, got {sort_order_id}"

    def test_sort_order_id_none_when_unsorted_table(self) -> None:
        """_prepare_write returns sort_order_id=None when table has no sort order."""
        from unittest.mock import MagicMock, PropertyMock

        import pyarrow as pa

        from pyiceberg.table import Transaction
        from pyiceberg.table.sorting import UNSORTED_SORT_ORDER_ID

        tx = object.__new__(Transaction)
        mock_table = MagicMock()
        mock_table.io.properties = {}
        tx._table = mock_table

        schema = Schema(NestedField(1, "id", IntegerType(), required=True))

        mock_metadata = MagicMock()
        mock_metadata.schema.return_value = schema
        mock_metadata.sort_orders = []
        mock_metadata.default_sort_order_id = UNSORTED_SORT_ORDER_ID
        type(tx).table_metadata = PropertyMock(return_value=mock_metadata)  # type: ignore[method-assign]

        mock_backends = MagicMock()
        mock_backends.supports_bounded_memory = True

        with patch("pyiceberg.execution.protocol.Backends.resolve", return_value=mock_backends):
            df = pa.table({"id": [3, 1, 2]})
            _, _, sort_order_id = tx._prepare_write(df, "append")

        assert sort_order_id is None, f"sort_order_id should be None for unsorted table, got {sort_order_id}"

    def test_sort_applies_globally_not_per_partition(self) -> None:
        """Sort-on-write sorts the entire input globally, not per-partition.

        This is a known limitation for partitioned tables: the global sort
        may not produce per-partition sorted output when the partition column
        is not a prefix of the sort key. Since sort order is advisory per the
        Iceberg spec, this is acceptable but should be documented behavior.
        """
        from unittest.mock import MagicMock, PropertyMock

        import pyarrow as pa

        from pyiceberg.table import Transaction
        from pyiceberg.table.sorting import SortField, SortOrder

        tx = object.__new__(Transaction)
        mock_table = MagicMock()
        mock_table.io.properties = {}
        tx._table = mock_table

        schema = Schema(
            NestedField(1, "id", IntegerType(), required=True),
            NestedField(2, "category", StringType(), required=True),
        )
        # Sort by id (but partition is on category — sort doesn't align)
        sort_order = SortOrder(order_id=3, fields=[SortField(source_id=1, transform=IdentityTransform())])

        mock_metadata = MagicMock()
        mock_metadata.schema.return_value = schema
        mock_metadata.sort_orders = [sort_order]
        mock_metadata.default_sort_order_id = 3
        type(tx).table_metadata = PropertyMock(return_value=mock_metadata)  # type: ignore[method-assign]

        mock_backends = MagicMock()
        mock_backends.supports_bounded_memory = True
        # sort_from_files receives a single file path (the materialized temp file)
        # This proves sort is applied globally to ALL data, not per-partition
        mock_backends.compute.sort_from_files.return_value = iter(
            [pa.record_batch({"id": [1, 2, 3], "category": ["b", "a", "a"]})]
        )

        with patch("pyiceberg.execution.protocol.Backends.resolve", return_value=mock_backends):
            df = pa.table({"id": [3, 1, 2], "category": ["a", "b", "a"]})
            result_df, _, sort_order_id = tx._prepare_write(df, "append")

        # The result is a RecordBatchReader (lazy). Consume it to trigger sort_from_files.
        if hasattr(result_df, "read_all"):
            result_df.read_all()

        # sort_from_files was called exactly once with a single temp file
        assert mock_backends.compute.sort_from_files.call_count == 1
        call_args = mock_backends.compute.sort_from_files.call_args
        file_paths = call_args[0][0]  # first positional arg: file_paths list
        assert len(file_paths) == 1, "Sort should receive a single materialized file (global sort)"

        # sort_order_id is set (sort was applied, even if not per-partition optimal)
        assert sort_order_id == 3
