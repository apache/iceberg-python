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
# pylint:disable=redefined-outer-name

import pyarrow as pa
import pytest
from pytest_lazy_fixtures import lf

from pyiceberg.catalog import Catalog
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.io.pyarrow import _dataframe_to_data_files
from pyiceberg.schema import Schema
from pyiceberg.types import BooleanType, IntegerType, LongType, NestedField, StringType

TEST_NAMESPACE_IDENTIFIER = "TEST NS"


@pytest.mark.integration
@pytest.mark.parametrize("catalog", [lf("session_catalog")])
def test_namespace_exists(catalog: RestCatalog) -> None:
    if not catalog.namespace_exists(TEST_NAMESPACE_IDENTIFIER):
        catalog.create_namespace(TEST_NAMESPACE_IDENTIFIER)

    assert catalog.namespace_exists(TEST_NAMESPACE_IDENTIFIER)


@pytest.mark.integration
@pytest.mark.parametrize("catalog", [lf("session_catalog")])
def test_namespace_not_exists(catalog: RestCatalog) -> None:
    if catalog.namespace_exists(TEST_NAMESPACE_IDENTIFIER):
        catalog.drop_namespace(TEST_NAMESPACE_IDENTIFIER)

    assert not catalog.namespace_exists(TEST_NAMESPACE_IDENTIFIER)


@pytest.mark.integration
@pytest.mark.parametrize("catalog", [lf("session_catalog")])
def test_create_namespace_if_not_exists(catalog: RestCatalog) -> None:
    if catalog.namespace_exists(TEST_NAMESPACE_IDENTIFIER):
        catalog.drop_namespace(TEST_NAMESPACE_IDENTIFIER)

    catalog.create_namespace_if_not_exists(TEST_NAMESPACE_IDENTIFIER)

    assert catalog.namespace_exists(TEST_NAMESPACE_IDENTIFIER)


@pytest.mark.integration
@pytest.mark.parametrize("catalog", [lf("session_catalog")])
def test_create_namespace_if_already_existing(catalog: RestCatalog) -> None:
    if not catalog.namespace_exists(TEST_NAMESPACE_IDENTIFIER):
        catalog.create_namespace(TEST_NAMESPACE_IDENTIFIER)

    catalog.create_namespace_if_not_exists(TEST_NAMESPACE_IDENTIFIER)

    assert catalog.namespace_exists(TEST_NAMESPACE_IDENTIFIER)


@pytest.mark.integration
@pytest.mark.parametrize("catalog", [lf("session_catalog")])
@pytest.mark.parametrize("format_version", [1, 2])
def test_replace_table_transaction(catalog: Catalog, format_version: int) -> None:
    identifier = f"default.test_replace_table_txn_{catalog.name}_{format_version}"
    try:
        catalog.create_namespace("default")
    except Exception:
        pass
    try:
        catalog.drop_table(identifier)
    except NoSuchTableError:
        pass

    # Create a table with initial schema and write some data
    original_schema = Schema(
        NestedField(field_id=1, name="id", field_type=LongType(), required=False),
        NestedField(field_id=2, name="data", field_type=StringType(), required=False),
    )
    original = catalog.create_table(identifier, schema=original_schema, properties={"format-version": str(format_version)})
    original_uuid = original.metadata.table_uuid

    pa_table = pa.Table.from_pydict(
        {"id": [1, 2, 3], "data": ["a", "b", "c"]},
        schema=pa.schema([pa.field("id", pa.int64()), pa.field("data", pa.large_string())]),
    )

    with original.transaction() as txn:
        with txn.update_snapshot().fast_append() as snapshot_update:
            for data_file in _dataframe_to_data_files(table_metadata=txn.table_metadata, df=pa_table, io=original.io):
                snapshot_update.append_data_file(data_file)

    original.refresh()
    current_snapshot = original.current_snapshot()
    assert current_snapshot is not None
    original_snapshot_id = current_snapshot.snapshot_id

    # Replace with a new schema
    new_schema = Schema(
        NestedField(field_id=1, name="id", field_type=LongType(), required=False),
        NestedField(field_id=2, name="name", field_type=StringType(), required=False),
        NestedField(field_id=3, name="active", field_type=BooleanType(), required=False),
    )

    with catalog.replace_table_transaction(
        identifier, schema=new_schema, properties={"format-version": str(format_version)}
    ) as txn:
        pass  # just replace the schema, no data

    table = catalog.load_table(identifier)

    # UUID must be preserved
    assert table.metadata.table_uuid == original_uuid

    # Current snapshot should be cleared (main ref removed)
    assert table.current_snapshot() is None

    # Old snapshots should still exist in the metadata
    assert len(table.metadata.snapshots) >= 1
    assert any(s.snapshot_id == original_snapshot_id for s in table.metadata.snapshots)

    # New schema should be current, with field IDs reused for "id" (should still be 1)
    current_schema = table.schema()
    id_field = current_schema.find_field("id")
    assert id_field.field_id == 1  # reused from original schema

    name_field = current_schema.find_field("name")
    assert name_field is not None
    assert name_field.field_id >= 3  # "name" is new, must not reuse "data"'s ID (2)

    active_field = current_schema.find_field("active")
    assert active_field is not None
    assert active_field.field_id >= 4  # "active" is new

    # last_column_id must account for the new fields
    assert table.metadata.last_column_id >= 4

    # Old schemas should still exist — exactly 2 (original + replacement)
    assert len(table.metadata.schemas) == 2


@pytest.mark.integration
@pytest.mark.parametrize("catalog", [lf("session_catalog")])
def test_replace_table(catalog: Catalog) -> None:
    identifier = f"default.test_replace_table_{catalog.name}"
    try:
        catalog.create_namespace("default")
    except Exception:
        pass
    try:
        catalog.drop_table(identifier)
    except NoSuchTableError:
        pass

    original_schema = Schema(
        NestedField(field_id=1, name="id", field_type=LongType(), required=False),
        NestedField(field_id=2, name="data", field_type=StringType(), required=False),
    )
    original = catalog.create_table(identifier, schema=original_schema)
    original_uuid = original.metadata.table_uuid

    new_schema = Schema(
        NestedField(field_id=1, name="x", field_type=IntegerType(), required=False),
        NestedField(field_id=2, name="y", field_type=IntegerType(), required=False),
    )

    result = catalog.replace_table(identifier, schema=new_schema)

    # UUID preserved
    assert result.metadata.table_uuid == original_uuid
    # New schema applied — "x" and "y" are entirely new names, so they get fresh IDs >= 3
    x_field = result.schema().find_field("x")
    y_field = result.schema().find_field("y")
    assert x_field is not None
    assert y_field is not None
    assert x_field.field_id >= 3
    assert y_field.field_id >= 4
    assert result.metadata.last_column_id >= 4


@pytest.mark.integration
@pytest.mark.parametrize("catalog", [lf("session_catalog")])
def test_replace_table_not_found(catalog: Catalog) -> None:
    with pytest.raises(NoSuchTableError):
        catalog.replace_table(
            "default.does_not_exist_for_replace",
            schema=Schema(NestedField(field_id=1, name="id", field_type=LongType(), required=False)),
        )


@pytest.mark.integration
@pytest.mark.parametrize("catalog", [lf("session_catalog")])
def test_replace_table_same_schema_no_duplication(catalog: Catalog) -> None:
    """Replacing a table with the exact same schema should succeed without adding duplicates.

    The code detects that the schema already exists and skips AddSchemaUpdate,
    matching Java's reuseOrCreateNewSchemaId behavior.
    """
    identifier = f"default.test_replace_same_schema_{catalog.name}"
    try:
        catalog.create_namespace("default")
    except Exception:
        pass
    try:
        catalog.drop_table(identifier)
    except NoSuchTableError:
        pass

    schema_a = Schema(
        NestedField(field_id=1, name="id", field_type=LongType(), required=False),
        NestedField(field_id=2, name="data", field_type=StringType(), required=False),
    )
    catalog.create_table(identifier, schema=schema_a)

    # Replace with the SAME schema — should succeed without adding a duplicate
    catalog.replace_table(identifier, schema=schema_a)

    table = catalog.load_table(identifier)
    # Should still have exactly 1 schema (no duplicate added)
    assert len(table.metadata.schemas) == 1
    # Current schema ID should remain 0 (the original)
    assert table.metadata.current_schema_id == 0
    # Schema should be unchanged
    assert len(table.schema().fields) == 2
    assert table.schema().find_field("id") is not None
    assert table.schema().find_field("data") is not None


@pytest.mark.integration
@pytest.mark.parametrize("catalog", [lf("session_catalog")])
def test_replace_table_back_to_previous_schema(catalog: Catalog) -> None:
    """Replacing A -> B -> A where A and B have disjoint fields.

    Since field IDs are reused from the current schema only (matching Java),
    "data" gets a new field ID when replacing back from B (which doesn't have "data").
    The resulting schema is structurally different from the original, so a 3rd schema
    is created. This matches Java's behavior.
    """
    identifier = f"default.test_replace_back_to_prev_schema_{catalog.name}"
    try:
        catalog.create_namespace("default")
    except Exception:
        pass
    try:
        catalog.drop_table(identifier)
    except NoSuchTableError:
        pass

    schema_a = Schema(
        NestedField(field_id=1, name="id", field_type=LongType(), required=False),
        NestedField(field_id=2, name="data", field_type=StringType(), required=False),
    )
    catalog.create_table(identifier, schema=schema_a)

    # Step 2: Replace with schema B (disjoint fields: "name" and "active" instead of "data")
    schema_b = Schema(
        NestedField(field_id=1, name="id", field_type=LongType(), required=False),
        NestedField(field_id=2, name="name", field_type=StringType(), required=False),
        NestedField(field_id=3, name="active", field_type=BooleanType(), required=False),
    )
    catalog.replace_table(identifier, schema=schema_b)

    table_after_b = catalog.load_table(identifier)
    assert len(table_after_b.metadata.schemas) == 2

    # Step 3: Replace BACK to schema A
    catalog.replace_table(identifier, schema=schema_a)

    table = catalog.load_table(identifier)
    # "data" gets a new field ID (not reused from historical schema A), so a 3rd schema is created
    assert len(table.metadata.schemas) == 3
    # last_column_id must be monotonically non-decreasing
    assert table.metadata.last_column_id >= table_after_b.metadata.last_column_id


@pytest.mark.integration
@pytest.mark.parametrize("catalog", [lf("session_catalog")])
def test_replace_table_last_column_id_monotonic(catalog: Catalog) -> None:
    """last_column_id must never decrease, even when replacing with fewer columns."""
    identifier = f"default.test_replace_last_col_id_{catalog.name}"
    try:
        catalog.create_namespace("default")
    except Exception:
        pass
    try:
        catalog.drop_table(identifier)
    except NoSuchTableError:
        pass

    # Create table with 5 columns
    schema_5col = Schema(
        NestedField(field_id=1, name="a", field_type=LongType(), required=False),
        NestedField(field_id=2, name="b", field_type=StringType(), required=False),
        NestedField(field_id=3, name="c", field_type=StringType(), required=False),
        NestedField(field_id=4, name="d", field_type=StringType(), required=False),
        NestedField(field_id=5, name="e", field_type=StringType(), required=False),
    )
    catalog.create_table(identifier, schema=schema_5col)

    table = catalog.load_table(identifier)
    initial_last_col_id = table.metadata.last_column_id
    assert initial_last_col_id >= 5, f"Initial last_column_id should be >= 5, got {initial_last_col_id}"

    # Replace with only 2 columns (subset)
    schema_2col = Schema(
        NestedField(field_id=1, name="a", field_type=LongType(), required=False),
        NestedField(field_id=2, name="b", field_type=StringType(), required=False),
    )
    catalog.replace_table(identifier, schema=schema_2col)

    table = catalog.load_table(identifier)
    after_shrink_last_col_id = table.metadata.last_column_id
    # last_column_id must NOT decrease
    assert after_shrink_last_col_id >= initial_last_col_id, (
        f"last_column_id decreased from {initial_last_col_id} to {after_shrink_last_col_id} "
        f"after replacing with fewer columns. It must be monotonically non-decreasing."
    )

    # Replace with 3 columns (2 existing + 1 new)
    schema_3col = Schema(
        NestedField(field_id=1, name="a", field_type=LongType(), required=False),
        NestedField(field_id=2, name="b", field_type=StringType(), required=False),
        NestedField(field_id=3, name="f", field_type=BooleanType(), required=False),  # new column
    )
    catalog.replace_table(identifier, schema=schema_3col)

    table = catalog.load_table(identifier)
    after_grow_last_col_id = table.metadata.last_column_id
    # New column should get an ID > previous last_column_id, so last_column_id should grow
    assert after_grow_last_col_id >= initial_last_col_id + 1, (
        f"last_column_id should be >= {initial_last_col_id + 1} after adding a new column, got {after_grow_last_col_id}"
    )

    # Verify the new column "f" got an ID > initial_last_col_id (not reusing a dropped column's ID)
    f_field = table.schema().find_field("f")
    assert f_field.field_id > initial_last_col_id, (
        f"New field 'f' got field_id={f_field.field_id}, but it should be > {initial_last_col_id} to maintain monotonicity"
    )


@pytest.mark.integration
@pytest.mark.parametrize("catalog", [lf("session_catalog")])
def test_replace_table_metadata_log_grows(catalog: Catalog) -> None:
    """After replacing a table, the metadata_log should contain the pre-replace metadata."""
    identifier = f"default.test_replace_metadata_log_{catalog.name}"
    try:
        catalog.create_namespace("default")
    except Exception:
        pass
    try:
        catalog.drop_table(identifier)
    except NoSuchTableError:
        pass

    schema_a = Schema(
        NestedField(field_id=1, name="id", field_type=LongType(), required=False),
        NestedField(field_id=2, name="data", field_type=StringType(), required=False),
    )
    catalog.create_table(identifier, schema=schema_a)

    table_before = catalog.load_table(identifier)
    metadata_log_before = len(table_before.metadata.metadata_log)

    # Replace with a different schema
    schema_b = Schema(
        NestedField(field_id=1, name="x", field_type=IntegerType(), required=False),
        NestedField(field_id=2, name="y", field_type=IntegerType(), required=False),
    )
    catalog.replace_table(identifier, schema=schema_b)

    table_after = catalog.load_table(identifier)
    metadata_log_after = len(table_after.metadata.metadata_log)

    # The metadata_log should have grown by at least 1 entry
    assert metadata_log_after > metadata_log_before, (
        f"metadata_log did not grow after replace_table. "
        f"Before: {metadata_log_before} entries, After: {metadata_log_after} entries. "
        f"The pre-replace metadata location should have been appended."
    )


@pytest.mark.integration
@pytest.mark.parametrize("catalog", [lf("session_catalog")])
def test_replace_table_snapshot_preserved_after_replace(catalog: Catalog) -> None:
    """Snapshots are preserved but current snapshot is cleared after replace."""
    identifier = f"default.test_replace_snapshot_preserved_{catalog.name}"
    try:
        catalog.create_namespace("default")
    except Exception:
        pass
    try:
        catalog.drop_table(identifier)
    except NoSuchTableError:
        pass

    schema = Schema(
        NestedField(field_id=1, name="id", field_type=LongType(), required=False),
        NestedField(field_id=2, name="data", field_type=StringType(), required=False),
    )
    table = catalog.create_table(identifier, schema=schema)

    # Write data to create a snapshot
    pa_table = pa.Table.from_pydict(
        {"id": [1, 2], "data": ["a", "b"]},
        schema=pa.schema([pa.field("id", pa.int64()), pa.field("data", pa.large_string())]),
    )
    with table.transaction() as txn:
        with txn.update_snapshot().fast_append() as snapshot_update:
            for data_file in _dataframe_to_data_files(table_metadata=txn.table_metadata, df=pa_table, io=table.io):
                snapshot_update.append_data_file(data_file)

    table.refresh()
    current_snapshot = table.current_snapshot()
    assert current_snapshot is not None
    original_snapshot_id = current_snapshot.snapshot_id
    original_snapshot_log_len = len(table.metadata.snapshot_log)

    # Replace with new schema
    new_schema = Schema(
        NestedField(field_id=1, name="id", field_type=LongType(), required=False),
        NestedField(field_id=2, name="value", field_type=StringType(), required=False),
    )
    catalog.replace_table(identifier, schema=new_schema)

    replaced = catalog.load_table(identifier)

    # Current snapshot cleared (main ref removed)
    assert replaced.current_snapshot() is None

    # Old snapshot still in metadata
    assert any(s.snapshot_id == original_snapshot_id for s in replaced.metadata.snapshots)

    # Snapshot log preserved
    assert len(replaced.metadata.snapshot_log) >= original_snapshot_log_len


@pytest.mark.integration
@pytest.mark.parametrize("catalog", [lf("session_catalog")])
def test_replace_table_with_partition_spec(catalog: Catalog) -> None:
    """Replace table with a new partition spec preserves old spec in metadata."""
    from pyiceberg.partitioning import PartitionField, PartitionSpec
    from pyiceberg.transforms import IdentityTransform

    identifier = f"default.test_replace_with_spec_{catalog.name}"
    try:
        catalog.create_namespace("default")
    except Exception:
        pass
    try:
        catalog.drop_table(identifier)
    except NoSuchTableError:
        pass

    schema = Schema(
        NestedField(field_id=1, name="id", field_type=LongType(), required=False),
        NestedField(field_id=2, name="data", field_type=StringType(), required=False),
    )
    catalog.create_table(identifier, schema=schema)

    # Replace with a partition spec on "id"
    new_spec = PartitionSpec(
        PartitionField(source_id=1, field_id=1000, transform=IdentityTransform(), name="id"),
        spec_id=0,
    )
    catalog.replace_table(identifier, schema=schema, partition_spec=new_spec)

    table = catalog.load_table(identifier)

    # New spec is the default
    current_spec = table.metadata.spec()
    assert len(current_spec.fields) == 1
    assert current_spec.fields[0].name == "id"

    # Old unpartitioned spec still in metadata
    assert len(table.metadata.partition_specs) >= 2

    # last_partition_id should be correct
    assert table.metadata.last_partition_id is not None and table.metadata.last_partition_id >= 1000


@pytest.mark.integration
@pytest.mark.parametrize("catalog", [lf("session_catalog")])
def test_replace_table_sequential_replaces(catalog: Catalog) -> None:
    """Multiple sequential replaces: schemas grow, last_column_id is monotonic, metadata_log grows."""
    identifier = f"default.test_replace_sequential_{catalog.name}"
    try:
        catalog.create_namespace("default")
    except Exception:
        pass
    try:
        catalog.drop_table(identifier)
    except NoSuchTableError:
        pass

    schema_a = Schema(
        NestedField(field_id=1, name="a", field_type=LongType(), required=False),
        NestedField(field_id=2, name="b", field_type=StringType(), required=False),
    )
    catalog.create_table(identifier, schema=schema_a)

    table = catalog.load_table(identifier)
    prev_last_col_id = table.metadata.last_column_id
    prev_metadata_log_len = len(table.metadata.metadata_log)

    # Replace 1: A -> B (completely different fields)
    schema_b = Schema(
        NestedField(field_id=1, name="x", field_type=IntegerType(), required=False),
        NestedField(field_id=2, name="y", field_type=IntegerType(), required=False),
        NestedField(field_id=3, name="z", field_type=BooleanType(), required=False),
    )
    catalog.replace_table(identifier, schema=schema_b)

    table = catalog.load_table(identifier)
    assert table.metadata.last_column_id >= prev_last_col_id
    assert len(table.metadata.schemas) == 2
    assert len(table.metadata.metadata_log) > prev_metadata_log_len
    prev_last_col_id = table.metadata.last_column_id
    prev_metadata_log_len = len(table.metadata.metadata_log)

    # Replace 2: B -> C (again different)
    schema_c = Schema(
        NestedField(field_id=1, name="p", field_type=StringType(), required=False),
        NestedField(field_id=2, name="q", field_type=LongType(), required=False),
    )
    catalog.replace_table(identifier, schema=schema_c)

    table = catalog.load_table(identifier)
    assert table.metadata.last_column_id >= prev_last_col_id
    assert len(table.metadata.schemas) == 3  # A, B, C all have different fields
    assert len(table.metadata.metadata_log) > prev_metadata_log_len
