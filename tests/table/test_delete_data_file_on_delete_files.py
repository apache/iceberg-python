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
"""Regression tests for delete_data_file() on _DeleteFiles instances.

_DeleteFiles is primarily predicate-driven, but it inherits delete_data_file()
from _SnapshotProducer.  These tests verify that explicit file references are
honoured correctly in all combinations with predicate-based deletion.
"""

import pyarrow as pa
import pytest

from pyiceberg.catalog import Catalog
from pyiceberg.expressions import EqualTo
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.types import LongType, NestedField


@pytest.fixture()
def delete_table(catalog: Catalog) -> Table:
    """A table with a single data file containing [1, 2, 3]."""
    catalog.create_namespace_if_not_exists("default")
    identifier = "default.delete_explicit_test"
    try:
        catalog.drop_table(identifier)
    except Exception:
        pass
    table = catalog.create_table(
        identifier,
        Schema(NestedField(1, "x", LongType(), required=False)),
    )
    table.append(pa.table({"x": [1, 2, 3]}))
    return table


@pytest.fixture()
def multi_file_table(catalog: Catalog) -> Table:
    """A table with three separate data files, one row each."""
    catalog.create_namespace_if_not_exists("default")
    identifier = "default.delete_multi_file_test"
    try:
        catalog.drop_table(identifier)
    except Exception:
        pass
    table = catalog.create_table(
        identifier,
        Schema(NestedField(1, "x", LongType(), required=False)),
    )
    table.append(pa.table({"x": [1]}))
    table.append(pa.table({"x": [2]}))
    table.append(pa.table({"x": [3]}))
    return table


# ---------------------------------------------------------------------------
# Basic: explicit delete_data_file on _DeleteFiles
# ---------------------------------------------------------------------------


def test_explicit_delete_removes_single_file(delete_table: Table) -> None:
    """The most basic case: delete_data_file() should actually delete the file."""
    data_file = next(iter(delete_table.scan().plan_files())).file

    with delete_table.transaction() as tx:
        with tx.update_snapshot().delete() as delete_snapshot:
            delete_snapshot.delete_data_file(data_file)

    assert delete_table.scan().to_arrow()["x"].to_pylist() == []


def test_explicit_delete_only_removes_targeted_file(multi_file_table: Table) -> None:
    """Only the targeted file is removed; other files remain intact."""
    files = sorted(
        [task.file for task in multi_file_table.scan().plan_files()],
        key=lambda f: f.file_path,
    )
    assert len(files) == 3

    with multi_file_table.transaction() as tx:
        with tx.update_snapshot().delete() as delete_snapshot:
            delete_snapshot.delete_data_file(files[0])

    remaining = sorted(multi_file_table.scan().to_arrow()["x"].to_pylist())
    # One file removed (unknown which value), two remain
    assert len(remaining) == 2


def test_explicit_delete_multiple_files(multi_file_table: Table) -> None:
    """Multiple explicit delete_data_file() calls delete all targeted files."""
    files = [task.file for task in multi_file_table.scan().plan_files()]
    assert len(files) == 3

    with multi_file_table.transaction() as tx:
        with tx.update_snapshot().delete() as delete_snapshot:
            delete_snapshot.delete_data_file(files[0])
            delete_snapshot.delete_data_file(files[1])

    remaining = multi_file_table.scan().to_arrow()["x"].to_pylist()
    assert len(remaining) == 1


def test_explicit_delete_all_files(multi_file_table: Table) -> None:
    """Explicitly deleting every file leaves the table empty."""
    files = [task.file for task in multi_file_table.scan().plan_files()]

    with multi_file_table.transaction() as tx:
        with tx.update_snapshot().delete() as delete_snapshot:
            for f in files:
                delete_snapshot.delete_data_file(f)

    assert multi_file_table.scan().to_arrow()["x"].to_pylist() == []


# ---------------------------------------------------------------------------
# Combining explicit deletes with predicate-based deletes
# ---------------------------------------------------------------------------


def test_predicate_delete_still_works(multi_file_table: Table) -> None:
    """Predicate-based deletion continues to work (baseline, no regression).

    With single-row files, EqualTo matches the entire file strictly.
    """
    before_count = len(list(multi_file_table.scan().plan_files()))
    assert before_count == 3

    # EqualTo("x", 1) strictly matches the file containing only [1]
    with multi_file_table.transaction() as tx:
        with tx.update_snapshot().delete() as delete_snapshot:
            delete_snapshot.delete_by_predicate(EqualTo("x", 1))

    remaining = sorted(multi_file_table.scan().to_arrow()["x"].to_pylist())
    assert remaining == [2, 3]


def test_explicit_and_predicate_combined(multi_file_table: Table) -> None:
    """Both explicit file deletion and predicate-based deletion take effect.

    Uses multi_file_table which has three single-row files: [1], [2], [3].
    """
    files = [task.file for task in multi_file_table.scan().plan_files()]
    assert len(files) == 3

    # Read the value from one file so we know which one we're explicitly targeting
    # Use the first file and also a predicate for x=1 (which targets the file with [1])
    target_file = files[0]

    with multi_file_table.transaction() as tx:
        with tx.update_snapshot().delete() as delete_snapshot:
            delete_snapshot.delete_data_file(target_file)
            # Also delete via predicate — EqualTo("x", 1) strictly matches
            # the single-row file containing only [1]
            delete_snapshot.delete_by_predicate(EqualTo("x", 1))

    remaining = sorted(multi_file_table.scan().to_arrow()["x"].to_pylist())
    # At least the explicitly targeted file is gone, plus the file matching x=1.
    # If target_file IS the x=1 file, we remove 1 file. Otherwise, 2 files removed.
    assert len(remaining) <= 2
    assert 1 not in remaining  # the predicate guarantees x=1 is gone


# ---------------------------------------------------------------------------
# Snapshot summary correctness
# ---------------------------------------------------------------------------


def test_snapshot_summary_correct_after_explicit_delete(delete_table: Table) -> None:
    """The snapshot summary reflects the explicit deletion accurately."""
    data_file = next(iter(delete_table.scan().plan_files())).file

    with delete_table.transaction() as tx:
        with tx.update_snapshot().delete() as delete_snapshot:
            delete_snapshot.delete_data_file(data_file)

    snapshot = delete_table.current_snapshot()
    assert snapshot is not None
    assert snapshot.summary is not None
    assert snapshot.summary.additional_properties["total-data-files"] == "0"
    assert snapshot.summary.additional_properties["total-records"] == "0"


def test_snapshot_summary_correct_partial_delete(multi_file_table: Table) -> None:
    """Deleting one of multiple files yields correct summary counts."""
    files = [task.file for task in multi_file_table.scan().plan_files()]
    assert len(files) == 3

    with multi_file_table.transaction() as tx:
        with tx.update_snapshot().delete() as delete_snapshot:
            delete_snapshot.delete_data_file(files[0])

    snapshot = multi_file_table.current_snapshot()
    assert snapshot is not None
    assert snapshot.summary is not None
    assert snapshot.summary.additional_properties["total-data-files"] == "2"


# ---------------------------------------------------------------------------
# Edge cases and invariants
# ---------------------------------------------------------------------------


def test_no_commit_when_no_files_deleted(delete_table: Table) -> None:
    """If no files are targeted, no snapshot is produced."""
    snapshot_before = delete_table.current_snapshot()

    with delete_table.transaction() as tx:
        with tx.update_snapshot().delete() as _delete_snapshot:
            pass  # no delete_data_file, no predicate

    # No new snapshot should be produced
    assert delete_table.current_snapshot() == snapshot_before


def test_idempotent_delete_same_file_twice(delete_table: Table) -> None:
    """Calling delete_data_file twice with the same file is idempotent."""
    data_file = next(iter(delete_table.scan().plan_files())).file

    with delete_table.transaction() as tx:
        with tx.update_snapshot().delete() as delete_snapshot:
            delete_snapshot.delete_data_file(data_file)
            delete_snapshot.delete_data_file(data_file)  # duplicate

    assert delete_table.scan().to_arrow()["x"].to_pylist() == []
    snapshot = delete_table.current_snapshot()
    assert snapshot is not None
    assert snapshot.summary is not None
    assert snapshot.summary.additional_properties["total-data-files"] == "0"


def test_delete_preserves_other_snapshots_history(multi_file_table: Table) -> None:
    """Deleting files creates a new snapshot but doesn't alter history."""
    snapshot_before = multi_file_table.current_snapshot()
    assert snapshot_before is not None
    files = [task.file for task in multi_file_table.scan().plan_files()]

    with multi_file_table.transaction() as tx:
        with tx.update_snapshot().delete() as delete_snapshot:
            delete_snapshot.delete_data_file(files[0])

    snapshot_after = multi_file_table.current_snapshot()
    assert snapshot_after is not None
    assert snapshot_after.parent_snapshot_id == snapshot_before.snapshot_id
