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

import pytest
import pyarrow as pa
from pyiceberg.catalog import Catalog
from pyiceberg.schema import Schema, NestedField
from pyiceberg.types import LongType, StringType
from pyiceberg.partitioning import PartitionSpec, UNPARTITIONED_PARTITION_SPEC
from pyiceberg.table import Table


def _create_table(
    catalog: Catalog,
    identifier: str,
    schema: Schema,
    partition_spec: PartitionSpec,
    properties: dict,
) -> Table:
    try:
        catalog.drop_table(identifier)
    except Exception:
        pass
    return catalog.create_table(identifier, schema, partition_spec=partition_spec, properties=properties)


@pytest.mark.integration
def test_deletes(session_catalog: Catalog):
    # Create a table.
    identifier = "default.test_deletes_table"
    schema = Schema(
        NestedField(1, "id", LongType()),
        NestedField(2, "data", StringType()),
    )
    table = _create_table(
        session_catalog,
        identifier,
        schema,
        UNPARTITIONED_PARTITION_SPEC,
        {"format-version": "3"}
    )

    assert table.metadata.next_row_id == 0

    # Create 30 rows.
    num_rows = 30
    pyarrow_table = pa.Table.from_pylist(
        [{"id": i, "data": f"row_{i}"} for i in range(num_rows)],
        schema=schema.as_arrow()
    )

    table.append(pyarrow_table)

    # Ensure that the current snapshot has claimed rows [0, 30)
    current_snapshot = table.current_snapshot()
    assert current_snapshot is not None
    assert current_snapshot.first_row_id == 0
    assert table.metadata.next_row_id == num_rows

    table.delete("id >= 0")

    # Deleting a file should create a new snapshot which should inherit last-row-id from the
    # previous metadata and not change last-row-id for this metadata.
    current_snapshot_after_delete = table.current_snapshot()
    assert current_snapshot_after_delete is not None
    assert current_snapshot_after_delete.first_row_id == num_rows
    assert current_snapshot.added_rows == 0
    assert table.metadata.next_row_id == num_rows

    # Clean up
    session_catalog.drop_table(identifier)