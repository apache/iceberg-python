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

"""Tests for equality delete resolution via the pluggable backend.

These tests create real Parquet files (data + equality delete) on the local
filesystem and verify the full anti-join logic produces correct results without
requiring Docker/Spark. They validate:

1. Basic equality delete: rows matching delete values are excluded
2. Multi-column equality delete: composite key anti-join
3. NULL matching: IS NOT DISTINCT FROM semantics (NULL == NULL per Iceberg spec)
4. No-op case: delete values not present in data file
5. All-rows-deleted case: empty result
6. equality_ids metadata propagation: anti-join uses the correct columns

Per Iceberg Spec v2 §5.5:
- Equality delete files contain only the columns referenced by equality_ids
- Rows match if ALL equality columns match (IS NOT DISTINCT FROM)
- Delete files apply to data files in the same partition with data_sequence_number <= delete_sequence_number

Additionally validates:
- ManifestGroupPlanner accepts equality deletes (previously raised ValueError)
- schema_to_pyarrow(..., include_field_ids=False) for user-facing output
- Sequence number gating: equality deletes require delete.seq > data.seq (STRICTLY GREATER)
- Position deletes use >= gating (different rule from equality)
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from pyiceberg.execution._orchestrate import orchestrate_scan
from pyiceberg.execution.backends.pyarrow_backend import PyArrowComputeBackend, PyArrowReadBackend
from pyiceberg.execution.protocol import Backends
from pyiceberg.expressions import AlwaysTrue
from pyiceberg.manifest import DataFileContent, ManifestEntry
from pyiceberg.schema import Schema
from pyiceberg.table.delete_file_index import DeleteFileIndex
from pyiceberg.typedef import Record
from pyiceberg.types import IntegerType, NestedField, StringType


def _write_parquet(path: str, table: pa.Table) -> None:
    """Write a PyArrow table to a Parquet file."""
    pq.write_table(table, path)


def _make_data_file_mock(
    file_path: str,
    content: DataFileContent,
    spec_id: int = 0,
    record_count: int = 0,
    equality_ids: list[int] | None = None,
    sequence_number: int = 1,
) -> MagicMock:
    """Create a mock DataFile object with required attributes."""
    mock = MagicMock()
    mock.file_path = file_path
    mock.content = content
    mock.spec_id = spec_id
    mock.record_count = record_count
    mock.equality_ids = equality_ids
    mock.file_size_in_bytes = 1024
    mock.partition = MagicMock()
    return mock


def _make_file_scan_task(data_path: str, delete_files: list, record_count: int = 10) -> MagicMock:
    """Create a mock FileScanTask with data file and delete files."""
    from pyiceberg.expressions import AlwaysTrue

    task = MagicMock()
    task.file = _make_data_file_mock(data_path, DataFileContent.DATA, record_count=record_count)
    task.delete_files = delete_files
    task.residual = AlwaysTrue()
    return task


def _make_table_metadata(schema: Schema) -> MagicMock:
    """Create a mock TableMetadata with the given schema."""
    mock = MagicMock()
    mock.schema.return_value = schema
    mock.format_version = 2
    mock.default_spec_id = 0
    mock.specs.return_value = {0: MagicMock(fields=[])}
    return mock


def _make_backends() -> Backends:
    """Create a Backends instance using PyArrow for all axes (local file tests)."""
    from pyiceberg.execution.backends.pyarrow_backend import PyArrowWriteBackend

    return Backends(
        read=PyArrowReadBackend(),
        write=PyArrowWriteBackend(),
        compute=PyArrowComputeBackend(),
        io_properties={},
    )


def _make_manifest_entry(
    file_path: str,
    content: DataFileContent,
    seq_num: int = 0,
    spec_id: int = 0,
    partition: None = None,
    sequence_number: int | None = None,
    equality_ids: list[int] | None = None,
) -> ManifestEntry:
    """Create a minimal ManifestEntry for testing.

    Supports both `seq_num` (positional) and `sequence_number` (keyword) for
    compatibility with tests from different source files.
    """
    effective_seq = sequence_number if sequence_number is not None else seq_num

    data_file = MagicMock()
    data_file.file_path = file_path
    data_file.content = content
    data_file.spec_id = spec_id
    data_file.partition = partition if partition is not None else Record()
    data_file.lower_bounds = None
    data_file.upper_bounds = None
    data_file.record_count = 100
    data_file.equality_ids = equality_ids

    entry = MagicMock(spec=ManifestEntry)
    entry.data_file = data_file
    entry.sequence_number = effective_seq
    return entry


def _make_data_file(file_path: str, spec_id: int = 0) -> MagicMock:
    """Create a mock DataFile for lookup."""
    df = MagicMock()
    df.file_path = file_path
    df.spec_id = spec_id
    df.partition = Record()
    return df


# =============================================================================
# Tests from test_equality_deletes.py -- Basic equality delete resolution
# =============================================================================


class TestEqualityDeleteBasic:
    """Basic equality delete: single-column anti-join excludes matching rows."""

    def test_single_column_equality_delete(self, tmp_path):
        """Rows where id matches equality delete values are excluded."""
        # Data file: ids [1, 2, 3, 4, 5]
        data_table = pa.table({"id": [1, 2, 3, 4, 5], "value": ["a", "b", "c", "d", "e"]})
        data_path = str(tmp_path / "data.parquet")
        _write_parquet(data_path, data_table)

        # Equality delete file: delete rows where id IN {2, 4}
        # Per spec: equality delete files contain ONLY the equality columns
        delete_table = pa.table({"id": [2, 4]})
        delete_path = str(tmp_path / "eq_delete.parquet")
        _write_parquet(delete_path, delete_table)

        schema = Schema(
            NestedField(1, "id", IntegerType(), required=True),
            NestedField(2, "value", StringType(), required=False),
        )
        table_metadata = _make_table_metadata(schema)

        delete_file = _make_data_file_mock(
            delete_path,
            DataFileContent.EQUALITY_DELETES,
            equality_ids=[1],  # field_id=1 is "id"
        )
        task = _make_file_scan_task(data_path, delete_files=[delete_file])

        backends = _make_backends()
        batches = list(
            orchestrate_scan(
                backends=backends,
                tasks=iter([task]),
                table_metadata=table_metadata,
                projected_schema=schema,
                row_filter=AlwaysTrue(),
            )
        )

        result = pa.Table.from_batches(batches)
        surviving_ids = sorted(result.column("id").to_pylist())
        assert surviving_ids == [1, 3, 5], f"Expected [1,3,5] after deleting ids 2,4. Got {surviving_ids}"

    def test_equality_delete_no_matches_returns_all(self, tmp_path):
        """When delete values don't match any data rows, all rows survive."""
        data_table = pa.table({"id": [1, 2, 3], "name": ["x", "y", "z"]})
        data_path = str(tmp_path / "data.parquet")
        _write_parquet(data_path, data_table)

        # Delete values not present in data
        delete_table = pa.table({"id": [99, 100]})
        delete_path = str(tmp_path / "eq_delete.parquet")
        _write_parquet(delete_path, delete_table)

        schema = Schema(
            NestedField(1, "id", IntegerType(), required=True),
            NestedField(2, "name", StringType(), required=False),
        )
        table_metadata = _make_table_metadata(schema)

        delete_file = _make_data_file_mock(
            delete_path,
            DataFileContent.EQUALITY_DELETES,
            equality_ids=[1],
        )
        task = _make_file_scan_task(data_path, delete_files=[delete_file])

        backends = _make_backends()
        batches = list(
            orchestrate_scan(
                backends=backends,
                tasks=iter([task]),
                table_metadata=table_metadata,
                projected_schema=schema,
                row_filter=AlwaysTrue(),
            )
        )

        result = pa.Table.from_batches(batches)
        assert sorted(result.column("id").to_pylist()) == [1, 2, 3]

    def test_equality_delete_all_rows_returns_empty(self, tmp_path):
        """When all data rows match the delete, result is empty."""
        data_table = pa.table({"id": [1, 2], "name": ["a", "b"]})
        data_path = str(tmp_path / "data.parquet")
        _write_parquet(data_path, data_table)

        delete_table = pa.table({"id": [1, 2]})
        delete_path = str(tmp_path / "eq_delete.parquet")
        _write_parquet(delete_path, delete_table)

        schema = Schema(
            NestedField(1, "id", IntegerType(), required=True),
            NestedField(2, "name", StringType(), required=False),
        )
        table_metadata = _make_table_metadata(schema)

        delete_file = _make_data_file_mock(
            delete_path,
            DataFileContent.EQUALITY_DELETES,
            equality_ids=[1],
        )
        task = _make_file_scan_task(data_path, delete_files=[delete_file])

        backends = _make_backends()
        batches = list(
            orchestrate_scan(
                backends=backends,
                tasks=iter([task]),
                table_metadata=table_metadata,
                projected_schema=schema,
                row_filter=AlwaysTrue(),
            )
        )

        if batches:
            result = pa.Table.from_batches(batches)
            assert result.num_rows == 0
        # else: no batches at all is also correct (empty result)


class TestEqualityDeleteNullSemantics:
    """IS NOT DISTINCT FROM: NULL in data matches NULL in delete file."""

    def test_null_matches_null_single_column(self, tmp_path):
        """Per Iceberg spec §5.5.2: NULL matches NULL in equality delete resolution."""
        # Data file: id=1, id=NULL, id=3
        data_table = pa.table(
            {
                "id": pa.array([1, None, 3], type=pa.int32()),
                "value": ["a", "b", "c"],
            }
        )
        data_path = str(tmp_path / "data.parquet")
        _write_parquet(data_path, data_table)

        # Delete file: delete where id IS NULL
        delete_table = pa.table({"id": pa.array([None], type=pa.int32())})
        delete_path = str(tmp_path / "eq_delete.parquet")
        _write_parquet(delete_path, delete_table)

        schema = Schema(
            NestedField(1, "id", IntegerType(), required=False),
            NestedField(2, "value", StringType(), required=False),
        )
        table_metadata = _make_table_metadata(schema)

        delete_file = _make_data_file_mock(
            delete_path,
            DataFileContent.EQUALITY_DELETES,
            equality_ids=[1],
        )
        task = _make_file_scan_task(data_path, delete_files=[delete_file])

        backends = _make_backends()
        batches = list(
            orchestrate_scan(
                backends=backends,
                tasks=iter([task]),
                table_metadata=table_metadata,
                projected_schema=schema,
                row_filter=AlwaysTrue(),
            )
        )

        result = pa.Table.from_batches(batches)
        surviving_ids = result.column("id").to_pylist()
        # NULL row deleted, 1 and 3 survive
        assert sorted([x for x in surviving_ids if x is not None]) == [1, 3]
        assert None not in surviving_ids


class TestEqualityDeleteMultiColumn:
    """Multi-column equality delete: composite key anti-join."""

    def test_two_column_composite_key(self, tmp_path):
        """Both columns must match for a row to be deleted (AND semantics)."""
        data_table = pa.table(
            {
                "id": [1, 2, 3, 4],
                "category": ["a", "b", "a", "b"],
                "value": [10, 20, 30, 40],
            }
        )
        data_path = str(tmp_path / "data.parquet")
        _write_parquet(data_path, data_table)

        # Delete where (id=2 AND category='b') -- only row 2 matches
        # Also (id=3 AND category='b') -- no match (row 3 has category='a')
        delete_table = pa.table({"id": [2, 3], "category": ["b", "b"]})
        delete_path = str(tmp_path / "eq_delete.parquet")
        _write_parquet(delete_path, delete_table)

        schema = Schema(
            NestedField(1, "id", IntegerType(), required=True),
            NestedField(2, "category", StringType(), required=True),
            NestedField(3, "value", IntegerType(), required=True),
        )
        table_metadata = _make_table_metadata(schema)

        delete_file = _make_data_file_mock(
            delete_path,
            DataFileContent.EQUALITY_DELETES,
            equality_ids=[1, 2],  # field_id=1 "id", field_id=2 "category"
        )
        task = _make_file_scan_task(data_path, delete_files=[delete_file])

        backends = _make_backends()
        batches = list(
            orchestrate_scan(
                backends=backends,
                tasks=iter([task]),
                table_metadata=table_metadata,
                projected_schema=schema,
                row_filter=AlwaysTrue(),
            )
        )

        result = pa.Table.from_batches(batches)
        surviving_ids = sorted(result.column("id").to_pylist())
        # Row 2 (id=2, category='b') deleted. Row 3 (id=3, category='a') survives.
        assert surviving_ids == [1, 3, 4], f"Expected [1,3,4], got {surviving_ids}"


class TestEqualityDeleteMissingEqualityIds:
    """When equality_ids is not set on delete files, a warning is emitted and data is returned as-is."""

    def test_missing_equality_ids_warns_and_returns_superset(self, tmp_path):
        """Delete files without equality_ids emit UserWarning and don't filter."""
        data_table = pa.table({"id": [1, 2, 3], "name": ["a", "b", "c"]})
        data_path = str(tmp_path / "data.parquet")
        _write_parquet(data_path, data_table)

        delete_table = pa.table({"id": [2]})
        delete_path = str(tmp_path / "eq_delete.parquet")
        _write_parquet(delete_path, delete_table)

        schema = Schema(
            NestedField(1, "id", IntegerType(), required=True),
            NestedField(2, "name", StringType(), required=False),
        )
        table_metadata = _make_table_metadata(schema)

        # No equality_ids set -- cannot determine which columns to join on
        delete_file = _make_data_file_mock(
            delete_path,
            DataFileContent.EQUALITY_DELETES,
            equality_ids=None,
        )
        task = _make_file_scan_task(data_path, delete_files=[delete_file])

        backends = _make_backends()
        with pytest.warns(UserWarning, match="equality_ids"):
            batches = list(
                orchestrate_scan(
                    backends=backends,
                    tasks=iter([task]),
                    table_metadata=table_metadata,
                    projected_schema=schema,
                    row_filter=AlwaysTrue(),
                )
            )

        result = pa.Table.from_batches(batches)
        # All rows returned (superset) because we can't determine join columns
        assert sorted(result.column("id").to_pylist()) == [1, 2, 3]


# =============================================================================
# Tests from test_equality_delete_support.py -- Regression risk items
# =============================================================================


class TestEqualityDeleteAcceptance:
    """ManifestGroupPlanner must accept equality delete entries (not raise ValueError).

    Previously, encountering an EQUALITY_DELETES entry raised:
        ValueError("PyIceberg does not yet support equality deletes...")

    The pluggable backend now handles equality deletes via the anti-join path in
    orchestrate_scan. ManifestGroupPlanner must pass them through to DeleteFileIndex
    so they can be assigned to FileScanTasks for the orchestrator to process.
    """

    def test_planner_does_not_raise_on_equality_deletes(self):
        """ManifestGroupPlanner must NOT raise ValueError for equality delete entries."""
        from unittest.mock import MagicMock, patch

        from pyiceberg.manifest import DataFileContent, ManifestEntry
        from pyiceberg.table import ManifestGroupPlanner

        planner = ManifestGroupPlanner(
            table_metadata=MagicMock(),
            io=MagicMock(),
            row_filter=MagicMock(),
            case_sensitive=True,
        )

        mock_entry = MagicMock(spec=ManifestEntry)
        mock_data_file = MagicMock()
        mock_data_file.content = DataFileContent.EQUALITY_DELETES
        mock_data_file.spec_id = 0
        mock_data_file.partition = MagicMock()
        mock_entry.data_file = mock_data_file

        # Should NOT raise ValueError
        with patch.object(planner, "plan_manifest_entries", return_value=[[mock_entry]]):
            try:
                list(planner.plan_files([MagicMock()]))
            except ValueError as e:
                if "equality deletes" in str(e).lower():
                    pytest.fail(f"Equality deletes should be accepted, got: {e}")
                raise


class TestIncludeFieldIdsFalseIsIntentional:
    """User-facing Arrow schemas should NOT include Iceberg field ID metadata.

    Field IDs are internal Iceberg bookkeeping (used for schema evolution, partition
    specs, and manifest entry correlation). They are NOT useful to end users consuming
    the RecordBatchReader or pa.Table output. Including them pollutes the schema
    metadata and confuses downstream tools (pandas, polars, DuckDB).

    The behavioral change from include_field_ids=True (old) to False (new) is
    INTENTIONAL and correct. Users care about column names and data types.
    """

    def test_to_arrow_batch_reader_schema_has_no_field_ids(self):
        """The batch reader output schema must NOT contain PARQUET:field_id metadata."""
        from pyiceberg.io.pyarrow import schema_to_pyarrow
        from pyiceberg.schema import Schema
        from pyiceberg.types import IntegerType, NestedField, StringType

        schema = Schema(
            NestedField(field_id=1, name="id", field_type=IntegerType(), required=True),
            NestedField(field_id=2, name="name", field_type=StringType(), required=False),
        )

        # This is what the batch reader uses for its output schema
        arrow_schema = schema_to_pyarrow(schema, include_field_ids=False)

        # Verify no PARQUET:field_id in metadata
        for field in arrow_schema:
            metadata = field.metadata or {}
            assert b"PARQUET:field_id" not in metadata, (
                f"Field '{field.name}' has PARQUET:field_id metadata. "
                f"User-facing output should not include internal Iceberg field IDs."
            )

    def test_output_schema_preserves_column_names(self):
        """Column names must be preserved in the user-facing schema."""
        from pyiceberg.io.pyarrow import schema_to_pyarrow
        from pyiceberg.schema import Schema
        from pyiceberg.types import IntegerType, NestedField, StringType

        schema = Schema(
            NestedField(field_id=1, name="user_id", field_type=IntegerType(), required=True),
            NestedField(field_id=2, name="email", field_type=StringType(), required=False),
        )

        arrow_schema = schema_to_pyarrow(schema, include_field_ids=False)
        assert arrow_schema.names == ["user_id", "email"]

    def test_output_schema_preserves_data_types(self):
        """Data types must be preserved in the user-facing schema."""
        from pyiceberg.io.pyarrow import schema_to_pyarrow
        from pyiceberg.schema import Schema
        from pyiceberg.types import DoubleType, IntegerType, NestedField, StringType

        schema = Schema(
            NestedField(field_id=1, name="id", field_type=IntegerType(), required=True),
            NestedField(field_id=2, name="name", field_type=StringType(), required=False),
            NestedField(field_id=3, name="score", field_type=DoubleType(), required=False),
        )

        arrow_schema = schema_to_pyarrow(schema, include_field_ids=False)
        assert arrow_schema.field("id").type == pa.int32()
        assert arrow_schema.field("name").type == pa.large_string()
        assert arrow_schema.field("score").type == pa.float64()

    def test_include_field_ids_true_does_include_metadata(self):
        """Verify that include_field_ids=True DOES include metadata (for internal use)."""
        from pyiceberg.io.pyarrow import schema_to_pyarrow
        from pyiceberg.schema import Schema
        from pyiceberg.types import IntegerType, NestedField

        schema = Schema(
            NestedField(field_id=42, name="x", field_type=IntegerType(), required=True),
        )

        arrow_schema_with_ids = schema_to_pyarrow(schema, include_field_ids=True)
        metadata = arrow_schema_with_ids.field("x").metadata or {}
        assert b"PARQUET:field_id" in metadata, (
            "include_field_ids=True should include PARQUET:field_id metadata. This confirms the False path correctly strips it."
        )


# =============================================================================
# Tests from test_equality_delete_seq_gating.py -- Sequence number gating
# =============================================================================


class TestEqualityDeleteSequenceNumberGating:
    """Equality deletes must use strictly-greater seq gating (delete.seq > data.seq)."""

    def test_equality_delete_same_seq_does_NOT_apply(self):
        """An equality delete with seq == data.seq must NOT apply to the data file.

        Per spec: equality deletes target only data written BEFORE them.
        Same-snapshot writes are not targets.
        """
        index = DeleteFileIndex()

        # Data file at sequence 5
        data_entry = _make_manifest_entry("data.parquet", DataFileContent.DATA, seq_num=5)

        # Equality delete at sequence 5 (SAME snapshot)
        eq_delete_entry = _make_manifest_entry("eq_del.parquet", DataFileContent.EQUALITY_DELETES, seq_num=5)
        index.add_delete_file(eq_delete_entry)

        # Query: which deletes apply to data file at seq 5?
        result = index.for_data_file(5, data_entry.data_file)
        assert len(result) == 0, (
            "Equality delete with same sequence number as data file must NOT apply. "
            "Spec §5.5.2: equality deletes require delete.seq > data.seq."
        )

    def test_equality_delete_greater_seq_DOES_apply(self):
        """An equality delete with seq > data.seq MUST apply to the data file."""
        index = DeleteFileIndex()

        data_entry = _make_manifest_entry("data.parquet", DataFileContent.DATA, seq_num=3)

        # Equality delete at sequence 5 (written AFTER data)
        eq_delete_entry = _make_manifest_entry("eq_del.parquet", DataFileContent.EQUALITY_DELETES, seq_num=5)
        index.add_delete_file(eq_delete_entry)

        result = index.for_data_file(3, data_entry.data_file)
        assert len(result) == 1
        assert list(result)[0].file_path == "eq_del.parquet"

    def test_equality_delete_lesser_seq_does_NOT_apply(self):
        """An equality delete with seq < data.seq must NOT apply."""
        index = DeleteFileIndex()

        data_entry = _make_manifest_entry("data.parquet", DataFileContent.DATA, seq_num=10)

        # Equality delete at sequence 3 (written BEFORE data -- already superseded)
        eq_delete_entry = _make_manifest_entry("eq_del.parquet", DataFileContent.EQUALITY_DELETES, seq_num=3)
        index.add_delete_file(eq_delete_entry)

        result = index.for_data_file(10, data_entry.data_file)
        assert len(result) == 0

    def test_position_delete_same_seq_DOES_apply(self):
        """A position delete with seq == data.seq MUST apply (different rule from equality).

        Position deletes use >= because they reference specific (file_path, pos) tuples
        that can only be generated after the data file exists.
        """
        index = DeleteFileIndex()

        data_entry = _make_manifest_entry("data.parquet", DataFileContent.DATA, seq_num=5)

        # Position delete at sequence 5 (SAME snapshot) -- should apply
        pos_delete_entry = _make_manifest_entry("pos_del.parquet", DataFileContent.POSITION_DELETES, seq_num=5)
        index.add_delete_file(pos_delete_entry)

        result = index.for_data_file(5, data_entry.data_file)
        assert len(result) == 1
        assert list(result)[0].file_path == "pos_del.parquet"

    def test_position_delete_greater_seq_DOES_apply(self):
        """A position delete with seq > data.seq MUST apply."""
        index = DeleteFileIndex()

        data_entry = _make_manifest_entry("data.parquet", DataFileContent.DATA, seq_num=3)

        pos_delete_entry = _make_manifest_entry("pos_del.parquet", DataFileContent.POSITION_DELETES, seq_num=7)
        index.add_delete_file(pos_delete_entry)

        result = index.for_data_file(3, data_entry.data_file)
        assert len(result) == 1

    def test_mixed_equality_and_position_with_same_seq(self):
        """With both delete types at same seq as data, only position applies."""
        index = DeleteFileIndex()

        data_entry = _make_manifest_entry("data.parquet", DataFileContent.DATA, seq_num=5)

        eq_delete_entry = _make_manifest_entry("eq_del.parquet", DataFileContent.EQUALITY_DELETES, seq_num=5)
        pos_delete_entry = _make_manifest_entry("pos_del.parquet", DataFileContent.POSITION_DELETES, seq_num=5)
        index.add_delete_file(eq_delete_entry)
        index.add_delete_file(pos_delete_entry)

        result = index.for_data_file(5, data_entry.data_file)

        # Position delete applies (>=), equality does NOT (requires >)
        paths = {d.file_path for d in result}
        assert "pos_del.parquet" in paths, "Position delete with same seq should apply"
        assert "eq_del.parquet" not in paths, "Equality delete with same seq must NOT apply"

    def test_multiple_equality_deletes_different_seqs(self):
        """Only equality deletes with seq STRICTLY GREATER than data apply."""
        index = DeleteFileIndex()

        data_entry = _make_manifest_entry("data.parquet", DataFileContent.DATA, seq_num=5)

        # Three equality deletes: seq 3, 5, 7
        for seq in [3, 5, 7]:
            entry = _make_manifest_entry(f"eq_del_{seq}.parquet", DataFileContent.EQUALITY_DELETES, seq_num=seq)
            index.add_delete_file(entry)

        result = index.for_data_file(5, data_entry.data_file)
        paths = {d.file_path for d in result}

        assert "eq_del_3.parquet" not in paths, "seq 3 < data seq 5 -- must NOT apply"
        assert "eq_del_5.parquet" not in paths, "seq 5 == data seq 5 -- must NOT apply (strictly greater required)"
        assert "eq_del_7.parquet" in paths, "seq 7 > data seq 5 -- MUST apply"


# =============================================================================
# Tests from test_sequence_number_gating.py -- DeleteFileIndex gating
# =============================================================================


class TestEqualityDeleteSequenceGating:
    """Verify equality deletes are ONLY assigned when del.seq > data.seq."""

    def test_equality_delete_same_sequence_NOT_assigned(self):
        """Equality delete with SAME sequence number as data MUST NOT apply.

        Per spec §5.5.2: "Equality delete files [...] apply to data files with
        a lower data sequence number."
        """
        index = DeleteFileIndex()

        # Equality delete at sequence 5
        eq_entry = _make_manifest_entry(
            "s3://bucket/eq_delete.parquet",
            DataFileContent.EQUALITY_DELETES,
            sequence_number=5,
            equality_ids=[1],
        )
        index.add_delete_file(eq_entry)

        # Data file also at sequence 5
        data_file = _make_data_file("s3://bucket/data.parquet")

        # for_data_file with seq_num=5: equality delete at seq=5 should NOT apply
        result = index.for_data_file(5, data_file)
        assert len(result) == 0, (
            "Equality delete at same sequence number should NOT be assigned. "
            "Per spec: equality deletes only apply to data with LOWER sequence number."
        )

    def test_equality_delete_higher_sequence_IS_assigned(self):
        """Equality delete with HIGHER sequence number than data MUST apply."""
        index = DeleteFileIndex()

        # Equality delete at sequence 6
        eq_entry = _make_manifest_entry(
            "s3://bucket/eq_delete.parquet",
            DataFileContent.EQUALITY_DELETES,
            sequence_number=6,
            equality_ids=[1],
        )
        index.add_delete_file(eq_entry)

        # Data file at sequence 5
        data_file = _make_data_file("s3://bucket/data.parquet")

        # for_data_file with seq_num=5: equality delete at seq=6 should apply
        result = index.for_data_file(5, data_file)
        assert len(result) == 1
        assert list(result)[0].file_path == "s3://bucket/eq_delete.parquet"

    def test_equality_delete_lower_sequence_NOT_assigned(self):
        """Equality delete with LOWER sequence number than data MUST NOT apply."""
        index = DeleteFileIndex()

        # Equality delete at sequence 3
        eq_entry = _make_manifest_entry(
            "s3://bucket/eq_delete.parquet",
            DataFileContent.EQUALITY_DELETES,
            sequence_number=3,
            equality_ids=[1],
        )
        index.add_delete_file(eq_entry)

        # Data file at sequence 5
        data_file = _make_data_file("s3://bucket/data.parquet")

        # for_data_file with seq_num=5: equality delete at seq=3 should NOT apply
        result = index.for_data_file(5, data_file)
        assert len(result) == 0


class TestPositionDeleteSequenceGating:
    """Verify position deletes use NON-STRICT (>=) gating."""

    def test_position_delete_same_sequence_IS_assigned(self):
        """Position delete with SAME sequence number as data MUST apply.

        Position deletes use >= (not strictly >).
        """
        index = DeleteFileIndex()

        # Position delete at sequence 5
        pos_entry = _make_manifest_entry(
            "s3://bucket/pos_delete.parquet",
            DataFileContent.POSITION_DELETES,
            sequence_number=5,
        )
        index.add_delete_file(pos_entry)

        # Data file at sequence 5
        data_file = _make_data_file("s3://bucket/data.parquet")

        result = index.for_data_file(5, data_file)
        assert len(result) == 1, "Position delete at same sequence number MUST be assigned (>=, not >)."

    def test_position_delete_higher_sequence_IS_assigned(self):
        """Position delete with HIGHER sequence number than data MUST apply."""
        index = DeleteFileIndex()

        pos_entry = _make_manifest_entry(
            "s3://bucket/pos_delete.parquet",
            DataFileContent.POSITION_DELETES,
            sequence_number=7,
        )
        index.add_delete_file(pos_entry)

        data_file = _make_data_file("s3://bucket/data.parquet")
        result = index.for_data_file(5, data_file)
        assert len(result) == 1

    def test_position_delete_lower_sequence_NOT_assigned(self):
        """Position delete with LOWER sequence number than data MUST NOT apply."""
        index = DeleteFileIndex()

        pos_entry = _make_manifest_entry(
            "s3://bucket/pos_delete.parquet",
            DataFileContent.POSITION_DELETES,
            sequence_number=3,
        )
        index.add_delete_file(pos_entry)

        data_file = _make_data_file("s3://bucket/data.parquet")
        result = index.for_data_file(5, data_file)
        assert len(result) == 0


class TestMixedDeleteTypes:
    """Verify correct gating when both position and equality deletes exist."""

    def test_mixed_at_same_sequence_only_position_applies(self):
        """At same seq: position delete applies, equality delete does NOT."""
        index = DeleteFileIndex()

        # Both at sequence 5
        pos_entry = _make_manifest_entry(
            "s3://bucket/pos_delete.parquet",
            DataFileContent.POSITION_DELETES,
            sequence_number=5,
        )
        eq_entry = _make_manifest_entry(
            "s3://bucket/eq_delete.parquet",
            DataFileContent.EQUALITY_DELETES,
            sequence_number=5,
            equality_ids=[1],
        )
        index.add_delete_file(pos_entry)
        index.add_delete_file(eq_entry)

        data_file = _make_data_file("s3://bucket/data.parquet")
        result = index.for_data_file(5, data_file)

        # Only position delete should be included (>= is satisfied)
        # Equality delete at same seq should NOT be included (> is NOT satisfied)
        result_paths = {d.file_path for d in result}
        assert "s3://bucket/pos_delete.parquet" in result_paths
        assert "s3://bucket/eq_delete.parquet" not in result_paths
