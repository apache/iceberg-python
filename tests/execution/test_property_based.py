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

"""Property-based tests for execution backend correctness.

Uses Hypothesis to generate random inputs and verify invariants that must hold
for ALL possible data, not just hand-picked examples. This catches edge cases
that example-based tests miss (boundary values, empty sets, all-null columns,
large cardinalities, unicode, etc.).

Requires: pip install hypothesis (optional dev dependency, tests skipped if absent).
"""

from __future__ import annotations

import pyarrow as pa
import pytest

hypothesis = pytest.importorskip("hypothesis")

from hypothesis import HealthCheck, assume, given, settings
from hypothesis import strategies as st

from pyiceberg.execution.backends.pyarrow_backend import (
    PyArrowComputeBackend,
)

# =============================================================================
# Strategies: generate random Arrow data
# =============================================================================

# Strategy for nullable int64 values (includes None for NULL)
nullable_int64 = st.one_of(st.none(), st.integers(min_value=-(2**62), max_value=2**62))

# Strategy for nullable strings (includes None for NULL)
nullable_string = st.one_of(st.none(), st.text(min_size=0, max_size=50))


@st.composite
def int64_table(draw, min_rows=0, max_rows=100, columns=("key",)):
    """Generate a pa.Table with nullable int64 columns."""
    num_rows = draw(st.integers(min_value=min_rows, max_value=max_rows))
    data = {}
    for col in columns:
        values = draw(st.lists(nullable_int64, min_size=num_rows, max_size=num_rows))
        data[col] = pa.array(values, type=pa.int64())
    return pa.table(data)


@st.composite
def string_table(draw, min_rows=0, max_rows=50, columns=("key",)):
    """Generate a pa.Table with nullable string columns."""
    num_rows = draw(st.integers(min_value=min_rows, max_value=max_rows))
    data = {}
    for col in columns:
        values = draw(st.lists(nullable_string, min_size=num_rows, max_size=num_rows))
        data[col] = pa.array(values, type=pa.string())
    return pa.table(data)


# =============================================================================
# Property: Anti-Join Correctness
# =============================================================================


class TestAntiJoinProperties:
    """Property-based tests for anti_join: verify invariants hold for ALL inputs."""

    backend = PyArrowComputeBackend()

    @given(left=int64_table(min_rows=0, max_rows=80), right=int64_table(min_rows=0, max_rows=40))
    @settings(max_examples=200, suppress_health_check=[HealthCheck.too_slow], deadline=None)
    def test_anti_join_result_is_subset_of_left(self, left, right) -> None:
        """∀ left, right: anti_join(left, right) ⊆ left (result rows come from left only)."""
        batches = list(self.backend.anti_join(iter(left.to_batches()), iter(right.to_batches()), on=["key"]))
        if not batches:
            return  # Empty result is always a valid subset
        result = pa.Table.from_batches(batches)

        # Every row in result must exist in left
        result_keys = set(result.column("key").to_pylist())
        left_keys = set(left.column("key").to_pylist())
        assert result_keys.issubset(left_keys)

    @given(left=int64_table(min_rows=0, max_rows=80), right=int64_table(min_rows=0, max_rows=40))
    @settings(max_examples=200, suppress_health_check=[HealthCheck.too_slow], deadline=None)
    def test_anti_join_excludes_matching_keys(self, left, right) -> None:
        """∀ left, right: no row in result has key matching any right key (IS NOT DISTINCT FROM)."""
        batches = list(self.backend.anti_join(iter(left.to_batches()), iter(right.to_batches()), on=["key"]))
        if not batches:
            return
        result = pa.Table.from_batches(batches)

        right_keys = set(right.column("key").to_pylist())
        result_keys = result.column("key").to_pylist()

        # IS NOT DISTINCT FROM: None == None
        for key in result_keys:
            assert key not in right_keys, f"Result contains key {key!r} which exists in right side"

    @given(left=int64_table(min_rows=1, max_rows=50))
    @settings(max_examples=100, suppress_health_check=[HealthCheck.too_slow], deadline=None)
    def test_anti_join_empty_right_returns_all_left(self, left) -> None:
        """∀ left: anti_join(left, ∅) = left (empty right → all left rows survive)."""
        right = pa.table({"key": pa.array([], type=pa.int64())})
        batches = list(self.backend.anti_join(iter(left.to_batches()), iter(right.to_batches()), on=["key"]))
        if not batches:
            # left was empty after conversion
            assert left.num_rows == 0
            return
        result = pa.Table.from_batches(batches)
        assert result.num_rows == left.num_rows

    @given(data=int64_table(min_rows=1, max_rows=50))
    @settings(max_examples=100, suppress_health_check=[HealthCheck.too_slow], deadline=None)
    def test_anti_join_self_returns_empty(self, data) -> None:
        """∀ data: anti_join(data, data) = ∅ (every row matches itself)."""
        batches = list(self.backend.anti_join(iter(data.to_batches()), iter(data.to_batches()), on=["key"]))
        if batches:
            result = pa.Table.from_batches(batches)
            assert result.num_rows == 0
        # else: empty iterator is also correct

    @given(
        left=int64_table(min_rows=0, max_rows=50, columns=("a", "b")),
        right=int64_table(min_rows=0, max_rows=30, columns=("a", "b")),
    )
    @settings(max_examples=150, suppress_health_check=[HealthCheck.too_slow], deadline=None)
    def test_multi_column_anti_join_subset_invariant(self, left, right) -> None:
        """Multi-column anti-join result is always a subset of left."""
        batches = list(self.backend.anti_join(iter(left.to_batches()), iter(right.to_batches()), on=["a", "b"]))
        if not batches:
            return
        result = pa.Table.from_batches(batches)
        assert result.num_rows <= left.num_rows


# =============================================================================
# Property: Filter Correctness
# =============================================================================


class TestFilterProperties:
    """Property-based tests for filter: verify streaming filter preserves semantics."""

    backend = PyArrowComputeBackend()

    @given(data=int64_table(min_rows=0, max_rows=100, columns=("value",)))
    @settings(max_examples=200, suppress_health_check=[HealthCheck.too_slow], deadline=None)
    def test_filter_never_adds_rows(self, data) -> None:
        """∀ data, predicate: |filter(data)| ≤ |data| (filter only removes)."""

        # Use AlwaysTrue which should return all rows
        from pyiceberg.expressions import AlwaysTrue

        batches = list(self.backend.filter(iter(data.to_batches()), AlwaysTrue()))
        if not batches:
            # AlwaysTrue on empty data returns empty
            assert data.num_rows == 0 or all(b.num_rows == 0 for b in data.to_batches())
            return
        result_rows = sum(b.num_rows for b in batches)
        assert result_rows <= data.num_rows

    @given(data=int64_table(min_rows=0, max_rows=100, columns=("value",)))
    @settings(max_examples=100, suppress_health_check=[HealthCheck.too_slow], deadline=None)
    def test_filter_always_true_preserves_all(self, data) -> None:
        """∀ data: filter(data, AlwaysTrue) = data (identity filter)."""
        from pyiceberg.expressions import AlwaysTrue

        batches = list(self.backend.filter(iter(data.to_batches()), AlwaysTrue()))
        result_rows = sum(b.num_rows for b in batches)
        assert result_rows == data.num_rows

    @given(data=int64_table(min_rows=0, max_rows=100, columns=("value",)))
    @settings(max_examples=100, suppress_health_check=[HealthCheck.too_slow], deadline=None)
    def test_filter_always_false_returns_empty(self, data) -> None:
        """∀ data: filter(data, AlwaysFalse) = ∅ (nothing passes)."""
        from pyiceberg.expressions import AlwaysFalse

        batches = list(self.backend.filter(iter(data.to_batches()), AlwaysFalse()))
        result_rows = sum(b.num_rows for b in batches)
        assert result_rows == 0


# =============================================================================
# Property: Sort Correctness
# =============================================================================


class TestSortProperties:
    """Property-based tests for sort: verify output ordering invariants."""

    backend = PyArrowComputeBackend()

    @given(data=int64_table(min_rows=0, max_rows=100, columns=("key",)))
    @settings(max_examples=200, suppress_health_check=[HealthCheck.too_slow], deadline=None)
    def test_sort_preserves_multiset(self, data) -> None:
        """∀ data: sorted(data) has the same multiset of values as data."""
        batches = list(self.backend.sort(iter(data.to_batches()), sort_keys=[("key", "ascending")]))
        if not batches:
            assert data.num_rows == 0
            return
        result = pa.Table.from_batches(batches)
        # Same number of rows
        assert result.num_rows == data.num_rows
        # Same multiset of values (sorted vs original)
        assert sorted(result.column("key").to_pylist(), key=lambda x: (x is None, x)) == sorted(
            data.column("key").to_pylist(), key=lambda x: (x is None, x)
        )

    @given(data=int64_table(min_rows=0, max_rows=100, columns=("key",)))
    @settings(max_examples=200, suppress_health_check=[HealthCheck.too_slow], deadline=None)
    def test_sort_ascending_is_ordered(self, data) -> None:
        """∀ data: sort(data, ascending) produces non-decreasing key values."""
        assume(data.num_rows > 0)
        batches = list(self.backend.sort(iter(data.to_batches()), sort_keys=[("key", "ascending")]))
        if not batches:
            return
        result = pa.Table.from_batches(batches)
        keys = result.column("key").to_pylist()
        # Filter out Nones (nulls sort first in PyArrow by default)
        non_null = [k for k in keys if k is not None]
        assert non_null == sorted(non_null)

    @given(data=int64_table(min_rows=0, max_rows=100, columns=("key",)))
    @settings(max_examples=200, suppress_health_check=[HealthCheck.too_slow], deadline=None)
    def test_sort_descending_is_ordered(self, data) -> None:
        """∀ data: sort(data, descending) produces non-increasing key values."""
        assume(data.num_rows > 0)
        batches = list(self.backend.sort(iter(data.to_batches()), sort_keys=[("key", "descending")]))
        if not batches:
            return
        result = pa.Table.from_batches(batches)
        keys = result.column("key").to_pylist()
        non_null = [k for k in keys if k is not None]
        assert non_null == sorted(non_null, reverse=True)

    @given(data=int64_table(min_rows=0, max_rows=50, columns=("key",)))
    @settings(max_examples=100, suppress_health_check=[HealthCheck.too_slow], deadline=None)
    def test_sort_idempotent(self, data) -> None:
        """∀ data: sort(sort(data)) = sort(data) (sorting is idempotent)."""
        first_sort = list(self.backend.sort(iter(data.to_batches()), sort_keys=[("key", "ascending")]))
        if not first_sort:
            return
        second_sort = list(self.backend.sort(iter(first_sort), sort_keys=[("key", "ascending")]))
        first_table = pa.Table.from_batches(first_sort)
        second_table = pa.Table.from_batches(second_sort)
        assert first_table.equals(second_table)


# =============================================================================
# Property: Anti-Join NULL Semantics (IS NOT DISTINCT FROM)
# =============================================================================


# Strategy: generate tables with high NULL density for targeted NULL testing
@st.composite
def high_null_int64_table(draw, min_rows=1, max_rows=60, columns=("key",), null_probability=0.4):
    """Generate a pa.Table with high NULL density in join columns."""
    num_rows = draw(st.integers(min_value=min_rows, max_value=max_rows))
    data = {}
    for col in columns:
        values = []
        for _ in range(num_rows):
            if draw(st.floats(min_value=0, max_value=1)) < null_probability:
                values.append(None)
            else:
                values.append(draw(st.integers(min_value=-100, max_value=100)))
        data[col] = pa.array(values, type=pa.int64())
    return pa.table(data)


@st.composite
def high_null_multi_column_table(draw, min_rows=1, max_rows=40, null_probability=0.3) -> None:
    """Generate a multi-column table with independent NULLs per column."""
    num_rows = draw(st.integers(min_value=min_rows, max_value=max_rows))
    data = {}
    for col in ("a", "b"):
        values = []
        for _ in range(num_rows):
            if draw(st.floats(min_value=0, max_value=1)) < null_probability:
                values.append(None)
            else:
                values.append(draw(st.integers(min_value=-50, max_value=50)))
        data[col] = pa.array(values, type=pa.int64())
    # Add a non-join payload column to verify it's preserved
    data["payload"] = pa.array(draw(st.lists(st.integers(0, 999), min_size=num_rows, max_size=num_rows)), type=pa.int64())
    return pa.table(data)


class TestAntiJoinNullSemantics:
    """Property-based tests specifically for IS NOT DISTINCT FROM NULL handling.

    Iceberg spec §5.5 mandates that equality deletes use IS NOT DISTINCT FROM
    semantics: NULL matches NULL. This is NOT standard SQL equality (where
    NULL = NULL yields UNKNOWN/FALSE).

    These tests verify the invariant:
        ∀ left_row: if ∃ right_row where left_row.key IS NOT DISTINCT FROM right_row.key,
        then left_row is EXCLUDED from the result.

    Equivalently: NULL in left matches NULL in right.
    """

    backend = PyArrowComputeBackend()

    @given(
        left=high_null_int64_table(min_rows=1, max_rows=60),
        right=high_null_int64_table(min_rows=1, max_rows=30),
    )
    @settings(max_examples=300, suppress_health_check=[HealthCheck.too_slow], deadline=None)
    def test_null_in_left_excluded_when_null_in_right(self, left, right) -> None:
        """If right contains NULL, then ALL left NULLs are excluded (IS NOT DISTINCT FROM).

        This is the core NULL semantic: NULL is NOT DISTINCT FROM NULL → match → exclude.
        """
        right_keys = right.column("key").to_pylist()
        right_has_null = None in right_keys

        batches = list(self.backend.anti_join(iter(left.to_batches()), iter(right.to_batches()), on=["key"]))

        if not batches:
            # All left rows were matched — valid
            return

        result = pa.Table.from_batches(batches)
        result_keys = result.column("key").to_pylist()

        if right_has_null:
            # IS NOT DISTINCT FROM: NULL matches NULL → no NULLs in result
            assert None not in result_keys, (
                f"Right contains NULL → all left NULLs must be excluded from result. "
                f"But result contains {result_keys.count(None)} NULL(s)."
            )

    @given(
        left=high_null_int64_table(min_rows=1, max_rows=60),
        right=high_null_int64_table(min_rows=1, max_rows=30, null_probability=0.0),
    )
    @settings(max_examples=300, suppress_health_check=[HealthCheck.too_slow, HealthCheck.filter_too_much], deadline=None)
    def test_null_in_left_preserved_when_no_null_in_right(self, left, right) -> None:
        """If right has NO NULL, then left NULLs survive (no right row to match).

        This verifies the converse: NULLs are only excluded when there's a matching
        NULL on the right side. Without a right-side NULL, left NULLs have no match.
        """
        right_keys = right.column("key").to_pylist()
        assume(None not in right_keys)  # Safety check (null_probability=0.0 should guarantee this)

        left_keys = left.column("key").to_pylist()
        left_null_count = left_keys.count(None)

        if left_null_count == 0:
            return  # No NULLs to test

        batches = list(self.backend.anti_join(iter(left.to_batches()), iter(right.to_batches()), on=["key"]))

        if not batches:
            # All left rows matched by non-null values in right (unlikely but valid)
            return

        result = pa.Table.from_batches(batches)
        result_keys = result.column("key").to_pylist()
        result_null_count = result_keys.count(None)

        # All left NULLs must survive because right has no NULL to match them
        assert result_null_count == left_null_count, (
            f"Right has no NULLs → all {left_null_count} left NULLs must survive. But result has {result_null_count} NULL(s)."
        )

    @given(
        non_null_values=st.lists(st.integers(-50, 50), min_size=1, max_size=30),
        left_null_count=st.integers(min_value=1, max_value=10),
        right_null_count=st.integers(min_value=1, max_value=5),
    )
    @settings(max_examples=200, suppress_health_check=[HealthCheck.too_slow], deadline=None)
    def test_null_count_reduction_exact(self, non_null_values, left_null_count, right_null_count) -> None:
        """When both sides have NULLs, ALL left NULLs are removed (not just matching count).

        IS NOT DISTINCT FROM is a predicate (returns true/false), not a counting join.
        If right has 1 NULL, it matches ALL left NULLs (not just 1).
        """
        # Left: non_null_values + left_null_count NULLs
        left_values = non_null_values + [None] * left_null_count
        left = pa.table({"key": pa.array(left_values, type=pa.int64())})

        # Right: just NULLs (no non-null values to match the left data)
        right = pa.table({"key": pa.array([None] * right_null_count, type=pa.int64())})

        batches = list(self.backend.anti_join(iter(left.to_batches()), iter(right.to_batches()), on=["key"]))

        if not batches:
            # Only possible if all left values also matched (they shouldn't since right has only NULLs)
            raise AssertionError("Non-null left values should survive when right only has NULLs")

        result = pa.Table.from_batches(batches)
        result_keys = result.column("key").to_pylist()

        # All NULLs removed, all non-null values preserved
        assert None not in result_keys, f"Right has NULL → all {left_null_count} left NULLs must be excluded"
        assert sorted(result_keys) == sorted(non_null_values), (
            "Non-null left values must be preserved (right has only NULLs, no match)"
        )

    @given(
        left=high_null_multi_column_table(min_rows=1, max_rows=40),
        right=high_null_multi_column_table(min_rows=1, max_rows=20),
    )
    @settings(max_examples=200, suppress_health_check=[HealthCheck.too_slow], deadline=None)
    def test_multi_column_null_semantics(self, left, right) -> None:
        """Multi-column IS NOT DISTINCT FROM: (NULL, NULL) matches (NULL, NULL).

        For multi-column joins, IS NOT DISTINCT FROM applies independently per column:
        row (NULL, 5) matches (NULL, 5) but NOT (NULL, 6).
        """
        batches = list(self.backend.anti_join(iter(left.to_batches()), iter(right.to_batches()), on=["a", "b"]))

        if not batches:
            return

        result = pa.Table.from_batches(batches)

        # Build right key set using IS NOT DISTINCT FROM semantics
        right_key_set = set()
        for i in range(right.num_rows):
            a = right.column("a")[i].as_py()
            b = right.column("b")[i].as_py()
            # Use a sentinel for None to enable set membership (Python None is hashable)
            right_key_set.add((_null_sentinel(a), _null_sentinel(b)))

        # Verify no result row has a matching key in right
        for i in range(result.num_rows):
            a = result.column("a")[i].as_py()
            b = result.column("b")[i].as_py()
            result_key = (_null_sentinel(a), _null_sentinel(b))
            assert result_key not in right_key_set, f"Result row ({a}, {b}) matches right-side row via IS NOT DISTINCT FROM"

    @given(
        left=high_null_multi_column_table(min_rows=1, max_rows=30),
        right=high_null_multi_column_table(min_rows=1, max_rows=15),
    )
    @settings(max_examples=200, suppress_health_check=[HealthCheck.too_slow], deadline=None)
    def test_multi_column_partial_null_no_false_match(self, left, right) -> None:
        """(NULL, 5) does NOT match (NULL, 6) — partial NULL overlap is not a match.

        IS NOT DISTINCT FROM is applied per-column conjunctively:
        match iff col1_left IS NOT DISTINCT FROM col1_right
              AND col2_left IS NOT DISTINCT FROM col2_right
        """
        batches = list(self.backend.anti_join(iter(left.to_batches()), iter(right.to_batches()), on=["a", "b"]))

        if not batches:
            return

        result = pa.Table.from_batches(batches)

        # Result row count + excluded count must equal left row count
        # (anti-join is a partition: every left row is either in result or excluded)
        assert result.num_rows <= left.num_rows

        # Payload column must be preserved unchanged for surviving rows
        if result.num_rows > 0:
            assert "payload" in result.schema.names, "Non-join columns must be preserved in anti-join output"

    @given(
        values=st.lists(st.integers(-20, 20), min_size=2, max_size=20, unique=True),
        null_positions=st.lists(st.integers(0, 19), min_size=1, max_size=5, unique=True),
    )
    @settings(max_examples=150, suppress_health_check=[HealthCheck.too_slow], deadline=None)
    def test_null_exclusion_does_not_affect_non_null_rows(self, values, null_positions) -> None:
        """Excluding NULLs via anti-join must NOT accidentally exclude non-null rows.

        Regression guard: a buggy NULL handling implementation might overmatch
        (e.g., using is_null() incorrectly to build the exclusion mask).
        """
        # Build left with specific NULLs injected
        left_values = list(values[: min(len(values), 20)])
        for pos in null_positions:
            if pos < len(left_values):
                left_values[pos] = None

        left = pa.table({"key": pa.array(left_values, type=pa.int64())})
        # Right contains ONLY NULL — should only exclude left NULLs
        right = pa.table({"key": pa.array([None], type=pa.int64())})

        batches = list(self.backend.anti_join(iter(left.to_batches()), iter(right.to_batches()), on=["key"]))

        if not batches:
            # All values were None
            assert all(v is None for v in left_values)
            return

        result = pa.Table.from_batches(batches)
        result_keys = result.column("key").to_pylist()

        # No NULLs in result (they matched right's NULL)
        assert None not in result_keys

        # ALL non-null values from left must survive
        expected_non_null = [v for v in left_values if v is not None]
        assert sorted(result_keys) == sorted(expected_non_null), (
            f"Non-null values must not be affected by NULL exclusion. "
            f"Expected {sorted(expected_non_null)}, got {sorted(result_keys)}"
        )


def _null_sentinel(value) -> None:
    """Convert None to a hashable sentinel for set-based IS NOT DISTINCT FROM checks."""
    _SENTINEL = object()
    return _SENTINEL if value is None else value


# Use a module-level sentinel so all calls share the same object identity
_NULL_SENTINEL_OBJ = object()


def _null_sentinel(value) -> None:
    """Convert None to a hashable sentinel for set-based IS NOT DISTINCT FROM checks."""
    return _NULL_SENTINEL_OBJ if value is None else value
