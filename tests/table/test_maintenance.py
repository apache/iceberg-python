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
import random

import pyarrow as pa

from pyiceberg.catalog import Catalog
from pyiceberg.exceptions import NoSuchNamespaceError
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import IdentityTransform


def test_maintenance_compact(catalog: Catalog) -> None:
    # Setup Schema and specs
    from pyiceberg.types import LongType, NestedField, StringType

    schema = Schema(
        NestedField(1, "id", LongType()),
        NestedField(2, "category", StringType()),
        NestedField(3, "value", LongType()),
    )
    spec = PartitionSpec(PartitionField(source_id=2, field_id=1000, transform=IdentityTransform(), name="category"))

    # Create the namespace and table
    try:
        catalog.create_namespace("default")
    except NoSuchNamespaceError:
        pass
    table = catalog.create_table(
        "default.test_compaction",
        schema=schema,
        partition_spec=spec,
    )

    # Append many small data files
    categories = ["cat1", "cat2", "cat3"]
    for i in range(12):
        table.append(
            pa.table(
                {
                    "id": list(range(i * 10, (i + 1) * 10)),
                    "category": [categories[i % 3]] * 10,
                    "value": [random.randint(1, 100) for _ in range(10)],
                }
            )
        )

    # Verify state before compaction
    before_files = list(table.scan().plan_files())
    assert len(before_files) == 12
    arrow_table_before = table.scan().to_arrow()
    assert arrow_table_before.num_rows == 120

    # Execute Compaction
    table.maintenance.compact()

    # Verify state after compaction
    table.refresh()
    after_files = list(table.scan().plan_files())
    assert len(after_files) == 3  # Should be 1 optimized data file per partition

    arrow_table_after = table.scan().to_arrow()
    assert arrow_table_after.num_rows == 120
    assert arrow_table_before.column_names == arrow_table_after.column_names
    assert sorted(arrow_table_before.to_pylist(), key=lambda x: x["id"]) == sorted(
        arrow_table_after.to_pylist(), key=lambda x: x["id"]
    )

    # Ensure snapshot properties specify the replace-operation
    new_snapshot = table.current_snapshot()
    assert new_snapshot is not None
    assert new_snapshot.summary is not None
    assert new_snapshot.summary.get("snapshot-type") == "replace"
    assert new_snapshot.summary.get("replace-operation") == "compaction"


def test_maintenance_compact_empty_table(catalog: Catalog) -> None:
    from pyiceberg.types import LongType, NestedField, StringType

    schema = Schema(
        NestedField(1, "id", LongType()),
        NestedField(2, "category", StringType()),
    )

    try:
        catalog.create_namespace("default")
    except NoSuchNamespaceError:
        pass

    table = catalog.create_table("default.test_compaction_empty", schema=schema)
    before_snapshots = len(table.history())

    # Should safely return doing nothing
    table.maintenance.compact()

    table.refresh()
    after_snapshots = len(table.history())
    assert before_snapshots == after_snapshots  # No new snapshot should be made
