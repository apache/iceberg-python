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
"""
Regression / investigation test for manifest pruning correctness under partition spec evolution.

Context
-------
PR #3011 (merged Feb 20 2026) added manifest pruning to _OverwriteFiles and _DeleteFiles
in pyiceberg/table/update/snapshot.py. The pruning builds a partition predicate from the
*current* partition spec and evaluates it against every manifest in the snapshot via a
KeyDefaultDict of per-spec evaluators.

The question this test file investigates:
  When a table has been through partition spec evolution, its snapshot may contain manifests
  written under *different* partition_spec_ids. Does the manifest evaluator correctly resolve
  each manifest's own spec before deciding whether to include or skip it?

If the answer is "no", the overwrite will silently skip manifests from the old spec, leaving
stale data files that should have been deleted -- a silent correctness bug.

How to run
----------
  pytest tests/integration/test_manifest_pruning_spec_evolution.py -v
"""

import tempfile
from typing import Any

import pyarrow as pa

from pyiceberg.catalog import Catalog, load_catalog
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import IdentityTransform
from pyiceberg.types import LongType, NestedField, StringType

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

SCHEMA = Schema(
    NestedField(field_id=1, name="category", field_type=StringType(), required=False),
    NestedField(field_id=2, name="region", field_type=StringType(), required=False),
    NestedField(field_id=3, name="value", field_type=LongType(), required=False),
)

# Spec 0: partitioned only by category
SPEC_V0 = PartitionSpec(PartitionField(source_id=1, field_id=1000, transform=IdentityTransform(), name="category"))


def make_catalog(warehouse: str) -> Catalog:
    """Spin up a local SQLite-backed catalog -- no services needed."""
    return load_catalog(
        "test",
        type="sql",
        uri=f"sqlite:///{warehouse}/catalog.db",
        warehouse=f"file://{warehouse}",
    )


def arrow_table(rows: list[dict[str, Any]]) -> pa.Table:
    return pa.Table.from_pylist(
        rows,
        schema=pa.schema(
            [
                pa.field("category", pa.string()),
                pa.field("region", pa.string()),
                pa.field("value", pa.int64()),
            ]
        ),
    )


# ---------------------------------------------------------------------------
# Test 1: Mixed spec snapshot -- overwrite partition present in both specs
# ---------------------------------------------------------------------------


def test_overwrite_after_partition_spec_evolution_correctness() -> None:
    """
    Verifies that dynamic_partition_overwrite correctly replaces ALL data files
    for the target partition, including those written under a previous partition spec.

    Setup:
      - Spec 0: partition by identity(category)
      - Write A(1,2,3) and B(10,11) under spec 0
      - Evolve to spec 1: add identity(region)
      - Write A(100,101) and B(200) under spec 1
      - Overwrite category=A with new rows (999, 888)

    Expected after overwrite:
      - Only new A rows: values [888, 999]
      - All B rows untouched: values [10, 11, 200]
      - Total: 5 rows

    Bug (pre-fix): spec-0 A manifests are skipped by the evaluator,
    leaving stale A rows (1, 2, 3) in the table -> 8 rows total.
    """
    with tempfile.TemporaryDirectory() as warehouse:
        catalog = make_catalog(warehouse)
        catalog.create_namespace("default")

        # --- Step 1: create table with spec 0 ---
        table = catalog.create_table(
            "default.test_spec_evolution_overwrite",
            schema=SCHEMA,
            partition_spec=SPEC_V0,
        )

        # --- Step 2: write data under spec 0 ---
        table.append(
            arrow_table(
                [
                    {"category": "A", "region": None, "value": 1},
                    {"category": "A", "region": None, "value": 2},
                    {"category": "A", "region": None, "value": 3},
                    {"category": "B", "region": None, "value": 10},
                    {"category": "B", "region": None, "value": 11},
                ]
            )
        )
        assert table.scan().to_arrow().num_rows == 5

        # --- Step 3: evolve partition spec -- add identity(region) ---
        with table.update_spec() as update:
            update.add_field(
                source_column_name="region",
                transform=IdentityTransform(),
                partition_field_name="region",
            )
        table = catalog.load_table("default.test_spec_evolution_overwrite")
        assert table.spec().spec_id == 1, f"Expected spec_id=1, got {table.spec().spec_id}"

        # --- Step 4: write data under spec 1 ---
        table.append(
            arrow_table(
                [
                    {"category": "A", "region": "us", "value": 100},
                    {"category": "A", "region": "eu", "value": 101},
                    {"category": "B", "region": "us", "value": 200},
                ]
            )
        )
        assert table.scan().to_arrow().num_rows == 8

        # Confirm mixed-spec snapshot is actually set up
        current_snapshot = table.current_snapshot()
        assert current_snapshot is not None
        manifests = current_snapshot.manifests(table.io)
        spec_ids_in_snapshot = {m.partition_spec_id for m in manifests}
        assert len(spec_ids_in_snapshot) > 1, f"Test setup failed: expected manifests from >1 spec, got {spec_ids_in_snapshot}"

        # --- Step 5: dynamic_partition_overwrite for category=A only ---
        table.dynamic_partition_overwrite(
            arrow_table(
                [
                    {"category": "A", "region": "us", "value": 999},
                    {"category": "A", "region": "eu", "value": 888},
                ]
            )
        )

        table = catalog.load_table("default.test_spec_evolution_overwrite")
        result = table.scan().to_arrow().to_pydict()

        categories = result["category"]
        values = result["value"]

        a_values = [v for c, v in zip(categories, values, strict=True) if c == "A"]
        b_values = [v for c, v in zip(categories, values, strict=True) if c == "B"]

        # Total rows: 2 new A + 3 B = 5
        assert len(a_values) + len(b_values) == 5, (
            f"Row count mismatch: expected 5, got {len(a_values) + len(b_values)}.\n"
            f"A values: {sorted(a_values)} -- stale values would be any of [1, 2, 3, 100, 101]\n"
            f"B values: {sorted(b_values)}"
        )

        # A rows must be only the new ones
        stale = [v for v in a_values if v in (1, 2, 3, 100, 101)]
        assert not stale, (
            f"Stale A rows found (should have been deleted): {stale}\n"
            f"spec-0 manifests were incorrectly skipped during manifest pruning."
        )
        assert sorted(a_values) == [888, 999], f"Expected A=[888,999], got {sorted(a_values)}"

        # B rows completely untouched
        assert sorted(b_values) == [10, 11, 200], f"Expected B=[10,11,200], got {sorted(b_values)}"


# ---------------------------------------------------------------------------
# Test 2: Overwrite partition that ONLY exists in spec-0 manifests
# This is the most dangerous case -- silent data duplication, no exception raised
# ---------------------------------------------------------------------------


def test_overwrite_partition_only_in_old_spec() -> None:
    """
    Sharpest form of the bug: the overwrite target (category=B) has data
    ONLY under spec-0. After spec evolution to spec-1, overwriting B should
    delete the old spec-0 B files and write new ones.

    Bug (pre-fix): the manifest evaluator, built against spec-1's predicate,
    finds zero matching manifests for B (because B only exists in spec-0
    manifests) -> UserWarning "did not match any records" -> old B rows survive
    -> silent data duplication: [999, 10, 11] instead of [999].
    """
    with tempfile.TemporaryDirectory() as warehouse:
        catalog = make_catalog(warehouse)
        catalog.create_namespace("default")

        table = catalog.create_table(
            "default.test_old_spec_only_overwrite",
            schema=SCHEMA,
            partition_spec=SPEC_V0,
        )

        # Write ONLY category=B under spec 0
        table.append(
            arrow_table(
                [
                    {"category": "B", "region": None, "value": 10},
                    {"category": "B", "region": None, "value": 11},
                ]
            )
        )

        # Evolve spec -- add identity(region)
        with table.update_spec() as update:
            update.add_field(
                source_column_name="region",
                transform=IdentityTransform(),
                partition_field_name="region",
            )
        table = catalog.load_table("default.test_old_spec_only_overwrite")

        # Write ONLY category=A under spec 1 (B has no spec-1 data)
        table.append(
            arrow_table(
                [
                    {"category": "A", "region": "us", "value": 100},
                ]
            )
        )

        # Overwrite category=B -- it only exists in spec-0 manifests
        table.dynamic_partition_overwrite(
            arrow_table(
                [
                    {"category": "B", "region": "us", "value": 999},
                ]
            )
        )

        table = catalog.load_table("default.test_old_spec_only_overwrite")
        result = table.scan().to_arrow().to_pydict()

        categories = result["category"]
        values = result["value"]

        b_values = [v for c, v in zip(categories, values, strict=True) if c == "B"]
        a_values = [v for c, v in zip(categories, values, strict=True) if c == "A"]

        assert b_values == [999], (
            f"Expected B=[999] only, got {b_values}.\n"
            f"Stale rows {[v for v in b_values if v != 999]} were not deleted -- "
            f"spec-0 manifests were incorrectly skipped."
        )
        assert a_values == [100], f"A data unexpectedly modified: {a_values}"
