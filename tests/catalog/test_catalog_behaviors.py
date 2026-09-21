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
Consolidated behavior tests for InMemoryCatalog and SqlCatalog.
"""

import os
from collections.abc import Generator
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest
from pydantic_core import ValidationError
from pytest_lazy_fixtures import lf
from sqlalchemy.exc import IntegrityError

from pyiceberg.catalog import Catalog
from pyiceberg.exceptions import (
    CommitFailedException,
    NamespaceAlreadyExistsError,
    NamespaceNotEmptyError,
    NoSuchNamespaceError,
    NoSuchTableError,
    TableAlreadyExistsError,
)
from pyiceberg.io.pyarrow import _dataframe_to_data_files, schema_to_pyarrow
from pyiceberg.partitioning import UNPARTITIONED_PARTITION_SPEC, PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table import Table, TableProperties
from pyiceberg.table.snapshots import Operation
from pyiceberg.table.sorting import NullOrder, SortDirection, SortField, SortOrder
from pyiceberg.table.update import AddSchemaUpdate, SetCurrentSchemaUpdate
from pyiceberg.transforms import IdentityTransform, VoidTransform
from pyiceberg.typedef import Identifier
from pyiceberg.types import BooleanType, IntegerType, LongType, NestedField, StringType


# Name parsing tests
def test_namespace_from_tuple() -> None:
    identifier = ("com", "organization", "department", "my_table")
    namespace_from = Catalog.namespace_from(identifier)
    assert namespace_from == ("com", "organization", "department")


def test_namespace_from_str() -> None:
    identifier = "com.organization.department.my_table"
    namespace_from = Catalog.namespace_from(identifier)
    assert namespace_from == ("com", "organization", "department")


def test_name_from_tuple() -> None:
    identifier = ("com", "organization", "department", "my_table")
    name_from = Catalog.table_name_from(identifier)
    assert name_from == "my_table"


def test_name_from_str() -> None:
    identifier = "com.organization.department.my_table"
    name_from = Catalog.table_name_from(identifier)
    assert name_from == "my_table"


# Create table tests
def test_create_table(catalog: Catalog, test_table_identifier: Identifier, table_schema_simple: Schema) -> None:
    namespace = Catalog.namespace_from(test_table_identifier)
    catalog.create_namespace(namespace)
    table = catalog.create_table(test_table_identifier, table_schema_simple)
    loaded = catalog.load_table(test_table_identifier)
    assert loaded.name() == table.name()
    assert loaded.metadata_location == table.metadata_location


def test_create_table_if_not_exists_duplicated_table(
    catalog: Catalog, table_schema_nested: Schema, test_table_identifier: Identifier
) -> None:
    namespace = Catalog.namespace_from(test_table_identifier)
    catalog.create_namespace(namespace)
    table1 = catalog.create_table(test_table_identifier, table_schema_nested)
    table2 = catalog.create_table_if_not_exists(test_table_identifier, table_schema_nested)
    assert table1.name() == table2.name()


def test_create_table_raises_error_when_table_already_exists(
    catalog: Catalog, test_table_identifier: Identifier, table_schema_nested: Schema
) -> None:
    namespace = Catalog.namespace_from(test_table_identifier)
    catalog.create_namespace(namespace)
    catalog.create_table(test_table_identifier, table_schema_nested)
    with pytest.raises(TableAlreadyExistsError):
        catalog.create_table(test_table_identifier, table_schema_nested)


def test_table_exists(catalog: Catalog, test_table_identifier: Identifier, table_schema_nested: Schema) -> None:
    namespace = Catalog.namespace_from(test_table_identifier)
    catalog.create_namespace(namespace)
    catalog.create_table(test_table_identifier, table_schema_nested, properties={"format-version": "2"})
    assert catalog.table_exists(test_table_identifier)


def test_table_exists_on_table_not_found(catalog: Catalog, test_table_identifier: Identifier) -> None:
    assert not catalog.table_exists(test_table_identifier)


def test_create_table_raises_error_when_namespace_does_not_exist(catalog: Catalog, table_schema_simple: Schema) -> None:
    with pytest.raises(NoSuchNamespaceError):
        catalog.create_table(("non_existent_ns", "table"), table_schema_simple)


def test_table_raises_error_on_table_not_found(catalog: Catalog, test_table_identifier: Identifier) -> None:
    identifier_str = ".".join(test_table_identifier)
    with pytest.raises(NoSuchTableError, match=f"Table does not exist: {identifier_str}"):
        catalog.load_table(test_table_identifier)


def test_create_table_without_namespace(catalog: Catalog, table_schema_nested: Schema, table_name: str) -> None:
    with pytest.raises(NoSuchNamespaceError):
        catalog.create_table(table_name, table_schema_nested)


@pytest.mark.parametrize("format_version", [1, 2])
def test_create_table_transaction(catalog: Catalog, format_version: int) -> None:
    identifier = f"default.arrow_create_table_transaction_{catalog.name}_{format_version}"
    try:
        catalog.create_namespace("default")
    except NamespaceAlreadyExistsError:
        pass

    try:
        catalog.drop_table(identifier=identifier)
    except NoSuchTableError:
        pass

    pa_table = pa.Table.from_pydict(
        {
            "foo": ["a", None, "z"],
        },
        schema=pa.schema([pa.field("foo", pa.large_string(), nullable=True)]),
    )

    pa_table_with_column = pa.Table.from_pydict(
        {
            "foo": ["a", None, "z"],
            "bar": [19, None, 25],
        },
        schema=pa.schema(
            [
                pa.field("foo", pa.large_string(), nullable=True),
                pa.field("bar", pa.int32(), nullable=True),
            ]
        ),
    )

    with catalog.create_table_transaction(
        identifier=identifier, schema=pa_table.schema, properties={"format-version": str(format_version)}
    ) as txn:
        with txn.update_snapshot().fast_append() as snapshot_update:
            for data_file in _dataframe_to_data_files(table_metadata=txn.table_metadata, df=pa_table, io=txn._table.io):
                snapshot_update.append_data_file(data_file)

        with txn.update_schema() as schema_txn:
            schema_txn.union_by_name(pa_table_with_column.schema)

        with txn.update_snapshot().fast_append() as snapshot_update:
            for data_file in _dataframe_to_data_files(
                table_metadata=txn.table_metadata, df=pa_table_with_column, io=txn._table.io
            ):
                snapshot_update.append_data_file(data_file)

    tbl = catalog.load_table(identifier=identifier)
    assert tbl.format_version == format_version
    assert len(tbl.scan().to_arrow()) == 6


def test_create_table_default_sort_order(
    catalog: Catalog, table_schema_nested: Schema, test_table_identifier: Identifier
) -> None:
    namespace = Catalog.namespace_from(test_table_identifier)
    catalog.create_namespace(namespace)
    table = catalog.create_table(test_table_identifier, table_schema_nested)
    assert table.sort_order().order_id == 0, "Order ID must match"
    assert table.sort_order().is_unsorted is True, "Order must be unsorted"
    catalog.drop_table(test_table_identifier)


def test_create_v1_table(catalog: Catalog, table_schema_nested: Schema, test_table_identifier: Identifier) -> None:
    namespace = Catalog.namespace_from(test_table_identifier)
    catalog.create_namespace(namespace)
    table = catalog.create_table(test_table_identifier, table_schema_nested, properties={"format-version": "1"})
    assert table.sort_order().order_id == 0, "Order ID must match"
    assert table.sort_order().is_unsorted is True, "Order must be unsorted"
    assert table.format_version == 1
    assert table.spec() == UNPARTITIONED_PARTITION_SPEC
    catalog.drop_table(test_table_identifier)


def test_create_table_custom_sort_order(catalog: Catalog, table_schema_nested: Schema, test_table_identifier: Identifier) -> None:
    namespace = Catalog.namespace_from(test_table_identifier)
    catalog.create_namespace(namespace)
    order = SortOrder(SortField(source_id=2, transform=IdentityTransform(), null_order=NullOrder.NULLS_FIRST))
    table = catalog.create_table(test_table_identifier, table_schema_nested, sort_order=order)
    given_sort_order = table.sort_order()
    assert given_sort_order.order_id == 1, "Order ID must match"
    assert len(given_sort_order.fields) == 1, "Order must have 1 field"
    assert given_sort_order.fields[0].direction == SortDirection.ASC, "Direction must match"
    assert given_sort_order.fields[0].null_order == NullOrder.NULLS_FIRST, "Null order must match"
    assert isinstance(given_sort_order.fields[0].transform, IdentityTransform), "Transform must match"
    catalog.drop_table(test_table_identifier)


def test_create_table_with_default_warehouse_location(
    warehouse: Path, catalog_with_warehouse: Catalog, table_schema_nested: Schema, test_table_identifier: Identifier
) -> None:
    identifier_tuple = Catalog.identifier_to_tuple(test_table_identifier)
    namespace = Catalog.namespace_from(test_table_identifier)
    catalog_with_warehouse.create_namespace(namespace)
    catalog_with_warehouse.create_table(test_table_identifier, table_schema_nested)
    table = catalog_with_warehouse.load_table(test_table_identifier)
    assert table.name() == identifier_tuple
    assert table.metadata_location.startswith(f"file://{warehouse}")
    assert os.path.exists(table.metadata_location[len("file://") :])
    catalog_with_warehouse.drop_table(test_table_identifier)


def test_create_table_location_override(
    catalog: Catalog,
    tmp_path: Path,
    table_schema_nested: Schema,
    test_table_identifier: Identifier,
    test_table_properties: dict[str, str],
) -> None:
    test_partition_spec = PartitionSpec(PartitionField(name="x", transform=IdentityTransform(), source_id=1, field_id=1000))
    new_location = f"file://{tmp_path}/new_location"
    namespace = Catalog.namespace_from(test_table_identifier)
    catalog.create_namespace(namespace)
    table = catalog.create_table(
        identifier=test_table_identifier,
        schema=table_schema_nested,
        location=new_location,
        partition_spec=test_partition_spec,
        properties=test_table_properties,
    )
    assert catalog.load_table(test_table_identifier) == table
    assert table.location() == new_location


def test_create_table_removes_trailing_slash_from_location(
    warehouse: Path, catalog: Catalog, table_schema_nested: Schema, test_table_identifier: Identifier
) -> None:
    identifier_tuple = Catalog.identifier_to_tuple(test_table_identifier)
    namespace = Catalog.namespace_from(test_table_identifier)
    table_name = Catalog.table_name_from(identifier_tuple)
    location = f"file://{warehouse}/{catalog.name}/{table_name}-given"
    catalog.create_namespace(namespace)
    catalog.create_table(test_table_identifier, table_schema_nested, location=f"{location}/")
    table = catalog.load_table(test_table_identifier)
    assert table.name() == identifier_tuple
    assert table.metadata_location.startswith(f"file://{warehouse}")
    assert os.path.exists(table.metadata_location[len("file://") :])
    assert table.location() == location
    catalog.drop_table(test_table_identifier)


def test_create_tables_idempotency(catalog: Catalog) -> None:
    # Second initialization should not fail even if tables are already created
    catalog.create_tables()  # type: ignore[attr-defined]
    catalog.create_tables()  # type: ignore[attr-defined]


def test_create_table_pyarrow_schema(
    catalog: Catalog,
    pyarrow_schema_simple_without_ids: pa.Schema,
    test_table_identifier: Identifier,
    test_table_properties: dict[str, str],
) -> None:
    namespace = Catalog.namespace_from(test_table_identifier)
    catalog.create_namespace(namespace)
    table = catalog.create_table(
        identifier=test_table_identifier,
        schema=pyarrow_schema_simple_without_ids,
        properties=test_table_properties,
    )
    assert catalog.load_table(test_table_identifier) == table


def test_write_pyarrow_schema(catalog: Catalog, test_table_identifier: Identifier) -> None:
    pyarrow_table = pa.Table.from_arrays(
        [
            pa.array([None, "A", "B", "C"]),  # 'foo' column
            pa.array([1, 2, 3, 4]),  # 'bar' column
            pa.array([True, None, False, True]),  # 'baz' column
            pa.array([None, "A", "B", "C"]),  # 'large' column
        ],
        schema=pa.schema(
            [
                pa.field("foo", pa.large_string(), nullable=True),
                pa.field("bar", pa.int32(), nullable=False),
                pa.field("baz", pa.bool_(), nullable=True),
                pa.field("large", pa.large_string(), nullable=True),
            ]
        ),
    )
    namespace = Catalog.namespace_from(test_table_identifier)
    catalog.create_namespace(namespace)
    table = catalog.create_table(test_table_identifier, pyarrow_table.schema)
    table.append(pyarrow_table)


@pytest.mark.parametrize(
    "schema,expected",
    [
        (lf("pyarrow_schema_simple_without_ids"), lf("iceberg_schema_simple_no_ids")),
        (lf("table_schema_simple"), lf("table_schema_simple")),
        (lf("table_schema_nested"), lf("table_schema_nested")),
        (lf("pyarrow_schema_nested_without_ids"), lf("iceberg_schema_nested_no_ids")),
    ],
)
def test_convert_schema_if_needed(
    schema: Schema | pa.Schema,
    expected: Schema,
    catalog: Catalog,
) -> None:
    assert expected == catalog._convert_schema_if_needed(schema)


# Register table tests


def test_register_table(catalog: Catalog, test_table_identifier: Identifier, metadata_location: str) -> None:
    identifier_tuple = Catalog.identifier_to_tuple(test_table_identifier)
    namespace = Catalog.namespace_from(test_table_identifier)
    catalog.create_namespace(namespace)
    table = catalog.register_table(test_table_identifier, metadata_location)
    assert table.name() == identifier_tuple
    assert table.metadata_location == metadata_location
    assert os.path.exists(metadata_location)
    catalog.drop_table(test_table_identifier)


def test_register_existing_table(catalog: Catalog, test_table_identifier: Identifier, metadata_location: str) -> None:
    namespace = Catalog.namespace_from(test_table_identifier)
    catalog.create_namespace(namespace)
    catalog.register_table(test_table_identifier, metadata_location)
    with pytest.raises(TableAlreadyExistsError):
        catalog.register_table(test_table_identifier, metadata_location)


# Load table tests


def test_load_table(catalog: Catalog, table_schema_nested: Schema, test_table_identifier: Identifier) -> None:
    namespace = Catalog.namespace_from(test_table_identifier)
    catalog.create_namespace(namespace)
    table = catalog.create_table(test_table_identifier, table_schema_nested)
    loaded_table = catalog.load_table(test_table_identifier)
    assert table.name() == loaded_table.name()
    assert table.metadata_location == loaded_table.metadata_location
    assert table.metadata == loaded_table.metadata


def test_load_table_from_self_identifier(
    catalog: Catalog, table_schema_nested: Schema, test_table_identifier: Identifier
) -> None:
    identifier_tuple = Catalog.identifier_to_tuple(test_table_identifier)
    namespace = Catalog.namespace_from(test_table_identifier)
    catalog.create_namespace(namespace)
    table = catalog.create_table(test_table_identifier, table_schema_nested)
    intermediate = catalog.load_table(test_table_identifier)
    assert intermediate.name() == identifier_tuple
    loaded_table = catalog.load_table(intermediate.name())
    assert table.name() == loaded_table.name()
    assert table.metadata_location == loaded_table.metadata_location
    assert table.metadata == loaded_table.metadata


_SIMPLE_SCHEMA = Schema(
    NestedField(field_id=1, name="id", field_type=LongType(), required=False),
    NestedField(field_id=2, name="data", field_type=StringType(), required=False),
)


def _create_simple_table(
    catalog: Catalog,
    identifier: Identifier,
    *,
    schema: Schema = _SIMPLE_SCHEMA,
    format_version: int = 2,
    partition_spec: PartitionSpec = UNPARTITIONED_PARTITION_SPEC,
    properties: dict[str, str] | None = None,
) -> tuple[Identifier, Schema]:
    namespace = Catalog.namespace_from(identifier)
    catalog.create_namespace_if_not_exists(namespace)
    merged_properties = {"format-version": str(format_version), **(properties or {})}
    catalog.create_table(identifier, schema=schema, partition_spec=partition_spec, properties=merged_properties)
    return identifier, schema


def _simple_data(num_rows: int = 2) -> pa.Table:
    return pa.Table.from_pydict(
        {"id": list(range(num_rows)), "data": [chr(ord("a") + i) for i in range(num_rows)]},
        schema=pa.schema([pa.field("id", pa.int64()), pa.field("data", pa.large_string())]),
    )


_REPLACE_SCHEMA = Schema(
    NestedField(field_id=1, name="id", field_type=LongType(), required=False),
    NestedField(field_id=2, name="data", field_type=StringType(), required=False),
    NestedField(field_id=3, name="extra", field_type=BooleanType(), required=False),
)


def test_replace_transaction(catalog: Catalog, test_table_identifier: Identifier) -> None:
    _, original_schema = _create_simple_table(catalog, test_table_identifier)
    original = catalog.load_table(test_table_identifier)
    original.append(_simple_data())
    original = catalog.load_table(test_table_identifier)
    old_snapshot_id = original.current_snapshot().snapshot_id  # type: ignore[union-attr]
    snapshot_log_before = list(original.metadata.snapshot_log)
    assert len(snapshot_log_before) == 1

    catalog.replace_table_transaction(test_table_identifier, schema=_REPLACE_SCHEMA).commit_transaction()
    replaced = catalog.load_table(test_table_identifier)

    # UUID + history preserved, current snapshot cleared, current schema swapped.
    assert replaced.metadata.table_uuid == original.metadata.table_uuid
    assert replaced.metadata.current_snapshot_id is None
    assert {f.name for f in replaced.schema().fields} == {"id", "data", "extra"}
    # Old snapshot kept by identity (not just count), and snapshot_log entries from before survive
    # in order at the front of the log.
    assert any(s.snapshot_id == old_snapshot_id for s in replaced.metadata.snapshots)
    assert replaced.metadata.snapshot_log[: len(snapshot_log_before)] == snapshot_log_before
    # Old schema is still in the schemas list alongside the new one.
    schema_ids = sorted(s.schema_id for s in replaced.metadata.schemas)
    assert schema_ids == [0, 1]
    assert replaced.metadata.current_schema_id == 1
    # Time-travel back to the pre-replace snapshot returns the rows that were there before.
    assert replaced.scan(snapshot_id=old_snapshot_id).to_arrow().equals(_simple_data())


@dataclass
class _ReplaceFixture:
    """State produced by `_run_complete_replace`: the table before/after the replace plus
    the inputs needed to assert on the result."""

    original: Table
    replaced: Table
    new_sort: SortOrder
    original_data: pa.Table
    old_snapshot_id: int


def _run_complete_replace(catalog: Catalog, identifier: Identifier, tmp_path: Path) -> _ReplaceFixture:
    """Set up a table, run a full-six-args RTAS replace, and return the handles needed for assertions."""
    _create_simple_table(catalog, identifier, properties={"keep": "yes", "override": "old"})
    catalog.load_table(identifier).append(_simple_data())
    original = catalog.load_table(identifier)
    old_snapshot_id = original.current_snapshot().snapshot_id  # type: ignore[union-attr]
    original_data = original.scan().to_arrow()

    new_location = f"file://{tmp_path}/replaced"
    new_schema = Schema(
        NestedField(field_id=1, name="id", field_type=LongType(), required=False),
        NestedField(field_id=2, name="data", field_type=StringType(), required=False),
        NestedField(field_id=3, name="extra", field_type=BooleanType(), required=False),
    )
    new_spec = PartitionSpec(PartitionField(source_id=1, field_id=1000, name="id_part", transform=IdentityTransform()))
    new_sort = SortOrder(SortField(source_id=1, transform=IdentityTransform(), direction=SortDirection.ASC))
    new_data = pa.Table.from_pydict(
        {"id": [10, 20], "data": ["alice", "bob"], "extra": [True, False]},
        schema=pa.schema([pa.field("id", pa.int64()), pa.field("data", pa.large_string()), pa.field("extra", pa.bool_())]),
    )

    with catalog.replace_table_transaction(
        identifier,
        schema=new_schema,
        partition_spec=new_spec,
        sort_order=new_sort,
        location=new_location,
        properties={"override": "new", "added": "v"},
    ) as txn:
        txn.append(new_data)

    return _ReplaceFixture(
        original=original,
        replaced=catalog.load_table(identifier),
        new_sort=new_sort,
        original_data=original_data,
        old_snapshot_id=old_snapshot_id,
    )


def test_complete_replace_transaction_applies_new_schema_spec_and_sort(
    catalog: Catalog, test_table_identifier: Identifier, tmp_path: Path
) -> None:
    fx = _run_complete_replace(catalog, test_table_identifier, tmp_path)
    # Identity invariants.
    assert fx.replaced.metadata.table_uuid == fx.original.metadata.table_uuid
    assert fx.replaced.metadata.location == f"file://{tmp_path}/replaced"
    # New schema / spec / sort applied; old entries retained in history.
    assert {f.name for f in fx.replaced.schema().fields} == {"id", "data", "extra"}
    assert sorted(s.schema_id for s in fx.replaced.metadata.schemas) == [0, 1]
    assert fx.replaced.spec().fields[0].source_id == 1
    assert isinstance(fx.replaced.spec().fields[0].transform, IdentityTransform)
    assert {s.spec_id for s in fx.replaced.metadata.partition_specs} == {0, 1}
    assert fx.replaced.sort_order().fields == fx.new_sort.fields
    assert {s.order_id for s in fx.replaced.metadata.sort_orders} == {0, fx.replaced.metadata.default_sort_order_id}


def test_complete_replace_transaction_merges_properties(
    catalog: Catalog, test_table_identifier: Identifier, tmp_path: Path
) -> None:
    fx = _run_complete_replace(catalog, test_table_identifier, tmp_path)
    # `keep` is preserved, `override` is updated, `added` is new, and `format-version` does not leak.
    assert fx.replaced.properties["keep"] == "yes"
    assert fx.replaced.properties["override"] == "new"
    assert fx.replaced.properties["added"] == "v"
    assert "format-version" not in fx.replaced.properties


def test_complete_replace_transaction_rtas_preserves_old_snapshot(
    catalog: Catalog, test_table_identifier: Identifier, tmp_path: Path
) -> None:
    fx = _run_complete_replace(catalog, test_table_identifier, tmp_path)
    # New snapshot exists, has no parent (fresh start), old snapshot is still in the snapshot list.
    new_snapshot = fx.replaced.current_snapshot()
    assert new_snapshot is not None
    assert new_snapshot.snapshot_id != fx.old_snapshot_id
    assert new_snapshot.parent_snapshot_id is None
    assert any(s.snapshot_id == fx.old_snapshot_id for s in fx.replaced.metadata.snapshots)
    assert fx.replaced.scan().to_arrow().num_rows == 2
    # Time-travel back to before the replace returns the original rows from the old schema.
    time_travel = fx.replaced.scan(snapshot_id=fx.old_snapshot_id).to_arrow()
    assert time_travel.num_rows == fx.original_data.num_rows
    assert time_travel.column("id").to_pylist() == fx.original_data.column("id").to_pylist()


def test_replace_transaction_requires_table_exists(catalog: Catalog, test_table_identifier: Identifier) -> None:
    schema = Schema(NestedField(field_id=1, name="id", field_type=LongType(), required=False))
    with pytest.raises(NoSuchTableError):
        catalog.replace_table_transaction(test_table_identifier, schema=schema)


def test_replace_table_reuses_schema_id_when_identical(catalog: Catalog, test_table_identifier: Identifier) -> None:
    _, base_schema = _create_simple_table(catalog, test_table_identifier)
    catalog.replace_table_transaction(test_table_identifier, schema=base_schema).commit_transaction()
    replaced = catalog.load_table(test_table_identifier)
    # Identical shape -> no new schema appended, current points back at id 0.
    assert [s.schema_id for s in replaced.metadata.schemas] == [0]
    assert replaced.metadata.current_schema_id == 0
    assert replaced.metadata.last_column_id == 2


def test_replace_table_reuses_partition_spec_and_sort_order_when_identical(
    catalog: Catalog, test_table_identifier: Identifier
) -> None:
    spec = PartitionSpec(PartitionField(source_id=1, field_id=1000, name="id_part", transform=IdentityTransform()))
    sort = SortOrder(SortField(source_id=1, transform=IdentityTransform(), direction=SortDirection.ASC))
    _, schema = _create_simple_table(catalog, test_table_identifier, partition_spec=spec)
    # Introduce a sort order then replay both spec and sort — neither should append a new entry.
    catalog.replace_table_transaction(
        test_table_identifier, schema=schema, partition_spec=spec, sort_order=sort
    ).commit_transaction()
    sorted_first = catalog.load_table(test_table_identifier)
    sorted_order_id = sorted_first.metadata.default_sort_order_id
    assert sorted_order_id != 0

    catalog.replace_table_transaction(
        test_table_identifier, schema=schema, partition_spec=spec, sort_order=sort
    ).commit_transaction()
    replayed = catalog.load_table(test_table_identifier)
    assert [s.spec_id for s in replayed.metadata.partition_specs] == [0]
    assert replayed.metadata.default_spec_id == 0
    assert replayed.metadata.default_sort_order_id == sorted_order_id

    # Dropping the sort order falls back to the unsorted order_id 0 (also reused, not appended).
    catalog.replace_table_transaction(test_table_identifier, schema=schema, partition_spec=spec).commit_transaction()
    unsorted = catalog.load_table(test_table_identifier)
    assert unsorted.sort_order().is_unsorted
    assert unsorted.metadata.default_sort_order_id == 0


@pytest.mark.parametrize("keep_identifier", [True, False], ids=["preserves", "drops"])
def test_replace_table_identifier_field_ids(catalog: Catalog, test_table_identifier: Identifier, keep_identifier: bool) -> None:
    schema = Schema(
        NestedField(field_id=1, name="id", field_type=LongType(), required=True),
        NestedField(field_id=2, name="data", field_type=StringType(), required=False),
        identifier_field_ids=[1],
    )
    _create_simple_table(catalog, test_table_identifier, schema=schema)
    new_schema = (
        Schema(
            NestedField(field_id=1, name="id", field_type=LongType(), required=True),
            NestedField(field_id=2, name="data", field_type=StringType(), required=False),
            NestedField(field_id=3, name="extra", field_type=BooleanType(), required=False),
            identifier_field_ids=[1],
        )
        if keep_identifier
        else Schema(
            NestedField(field_id=1, name="id", field_type=LongType(), required=False),
            NestedField(field_id=2, name="data", field_type=StringType(), required=False),
        )
    )
    catalog.replace_table_transaction(test_table_identifier, schema=new_schema).commit_transaction()
    replaced = catalog.load_table(test_table_identifier)
    expected = [1] if keep_identifier else []
    assert list(replaced.schema().identifier_field_ids) == expected


@pytest.mark.parametrize(
    "format_version, expect_void_carry_forward",
    [(1, True), (2, False)],
    ids=["v1-carries-forward", "v2-drops"],
)
def test_replace_table_partition_field_carry_forward(
    catalog: Catalog,
    test_table_identifier: Identifier,
    format_version: int,
    expect_void_carry_forward: bool,
) -> None:
    spec = PartitionSpec(PartitionField(source_id=1, field_id=1000, name="id_part", transform=IdentityTransform()))
    _, schema = _create_simple_table(catalog, test_table_identifier, partition_spec=spec, format_version=format_version)
    catalog.replace_table_transaction(test_table_identifier, schema=schema).commit_transaction()
    replaced = catalog.load_table(test_table_identifier)
    new_spec = replaced.spec()
    if expect_void_carry_forward:
        void_field = next(f for f in new_spec.fields if f.field_id == 1000)
        assert isinstance(void_field.transform, VoidTransform)
        assert void_field.source_id == 1
        assert void_field.name == "id_part"
    else:
        assert new_spec.is_unpartitioned()


def test_replace_table_upgrades_format_version(catalog: Catalog, test_table_identifier: Identifier) -> None:
    _, schema = _create_simple_table(catalog, test_table_identifier, format_version=1)
    assert catalog.load_table(test_table_identifier).format_version == 1

    catalog.replace_table_transaction(
        test_table_identifier, schema=schema, properties={"format-version": "2"}
    ).commit_transaction()
    upgraded = catalog.load_table(test_table_identifier)
    assert upgraded.format_version == 2
    # `format-version` is a control input, not a persisted property.
    assert "format-version" not in upgraded.properties


def test_replace_table_keeps_upgraded_format_version_on_subsequent_replace(
    catalog: Catalog, test_table_identifier: Identifier
) -> None:
    _, schema = _create_simple_table(catalog, test_table_identifier, format_version=1)
    catalog.replace_table_transaction(
        test_table_identifier, schema=schema, properties={"format-version": "2"}
    ).commit_transaction()
    new_schema = Schema(*schema.fields, NestedField(field_id=3, name="extra", field_type=BooleanType(), required=False))
    catalog.replace_table_transaction(test_table_identifier, schema=new_schema).commit_transaction()
    replayed = catalog.load_table(test_table_identifier)
    assert replayed.format_version == 2
    assert {f.name for f in replayed.schema().fields} == {"id", "data", "extra"}


@pytest.mark.parametrize(
    "properties, location, expected_match",
    [
        pytest.param({"format-version": "1"}, None, "Cannot downgrade format-version", id="format-version-downgrade"),
        pytest.param({"format-version": "two"}, None, "Invalid format-version property", id="non-numeric-format-version"),
        pytest.param({}, "/", "location must not be empty", id="empty-location-after-rstrip"),
    ],
)
def test_replace_table_rejects_invalid_inputs(
    catalog: Catalog,
    test_table_identifier: Identifier,
    properties: dict[str, str],
    location: str | None,
    expected_match: str,
) -> None:
    _, schema = _create_simple_table(catalog, test_table_identifier, format_version=2)
    with pytest.raises(ValueError, match=expected_match):
        catalog.replace_table_transaction(test_table_identifier, schema=schema, properties=properties, location=location)


def test_replace_table_inherits_existing_location(catalog: Catalog, test_table_identifier: Identifier) -> None:
    _, schema = _create_simple_table(catalog, test_table_identifier)
    existing = catalog.load_table(test_table_identifier).metadata.location
    catalog.replace_table_transaction(test_table_identifier, schema=schema).commit_transaction()
    assert catalog.load_table(test_table_identifier).metadata.location == existing


def test_replace_table_uses_explicit_location(catalog: Catalog, test_table_identifier: Identifier, tmp_path: Path) -> None:
    _, schema = _create_simple_table(catalog, test_table_identifier)
    new_location = f"file://{tmp_path}/relocated"
    catalog.replace_table_transaction(test_table_identifier, schema=schema, location=new_location).commit_transaction()
    assert catalog.load_table(test_table_identifier).metadata.location == new_location


def test_replace_table_strips_trailing_slash_from_location(
    catalog: Catalog, test_table_identifier: Identifier, tmp_path: Path
) -> None:
    _, schema = _create_simple_table(catalog, test_table_identifier)
    bare = f"file://{tmp_path}/relocated"
    catalog.replace_table_transaction(test_table_identifier, schema=schema, location=bare + "/").commit_transaction()
    assert catalog.load_table(test_table_identifier).metadata.location == bare


def test_replace_table_transaction_rolls_back_on_failure(catalog: Catalog, test_table_identifier: Identifier) -> None:
    _create_simple_table(catalog, test_table_identifier)
    catalog.load_table(test_table_identifier).append(_simple_data())
    before = catalog.load_table(test_table_identifier).metadata

    def run_failing_replace() -> None:
        with catalog.replace_table_transaction(test_table_identifier, schema=_REPLACE_SCHEMA):
            raise RuntimeError("simulated failure inside replace transaction")

    with pytest.raises(RuntimeError, match="simulated failure inside replace transaction"):
        run_failing_replace()

    after = catalog.load_table(test_table_identifier).metadata
    assert after.table_uuid == before.table_uuid
    assert after.current_snapshot_id == before.current_snapshot_id
    assert after.current_schema_id == before.current_schema_id
    assert len(after.schemas) == len(before.schemas)


def test_concurrent_replace_transaction_schema_conflict(catalog: Catalog, test_table_identifier: Identifier) -> None:
    _create_simple_table(catalog, test_table_identifier)
    txn_a = catalog.replace_table_transaction(test_table_identifier, schema=_REPLACE_SCHEMA)
    txn_b = catalog.replace_table_transaction(test_table_identifier, schema=_REPLACE_SCHEMA)

    txn_a.commit_transaction()
    after_a = catalog.load_table(test_table_identifier).metadata
    with pytest.raises(CommitFailedException, match="last assigned field id"):
        txn_b.commit_transaction()
    # The failed commit must be a true no-op: no metadata advanced past where `txn_a` left things.
    assert catalog.load_table(test_table_identifier).metadata.last_column_id == after_a.last_column_id


def test_concurrent_replace_transaction_partition_spec_conflict(catalog: Catalog, test_table_identifier: Identifier) -> None:
    _, schema = _create_simple_table(catalog, test_table_identifier)
    new_spec = PartitionSpec(PartitionField(source_id=1, field_id=1000, name="id_part", transform=IdentityTransform()))
    txn_a = catalog.replace_table_transaction(test_table_identifier, schema=schema, partition_spec=new_spec)
    txn_b = catalog.replace_table_transaction(test_table_identifier, schema=schema, partition_spec=new_spec)

    txn_a.commit_transaction()
    after_a = catalog.load_table(test_table_identifier).metadata
    with pytest.raises(CommitFailedException, match="last assigned partition id"):
        txn_b.commit_transaction()
    # The failed commit must be a true no-op: no metadata advanced past where `txn_a` left things.
    assert catalog.load_table(test_table_identifier).metadata.last_partition_id == after_a.last_partition_id


# Rename table tests


def test_rename_table(
    catalog: Catalog, table_schema_nested: Schema, test_table_identifier: Identifier, another_table_identifier: Identifier
) -> None:
    from_namespace = Catalog.namespace_from(test_table_identifier)
    to_namespace = Catalog.namespace_from(another_table_identifier)
    catalog.create_namespace(from_namespace)
    catalog.create_namespace(to_namespace)
    table = catalog.create_table(test_table_identifier, table_schema_nested)
    assert table.name() == test_table_identifier
    catalog.rename_table(test_table_identifier, another_table_identifier)
    new_table = catalog.load_table(another_table_identifier)
    assert new_table.name() == another_table_identifier
    assert new_table.metadata_location == table.metadata_location
    with pytest.raises(NoSuchTableError):
        catalog.load_table(test_table_identifier)


def test_rename_table_from_self_identifier(
    catalog: Catalog, table_schema_nested: Schema, test_table_identifier: Identifier, another_table_identifier: Identifier
) -> None:
    from_namespace = Catalog.namespace_from(test_table_identifier)
    to_namespace = Catalog.namespace_from(another_table_identifier)
    catalog.create_namespace(from_namespace)
    catalog.create_namespace(to_namespace)
    table = catalog.create_table(test_table_identifier, table_schema_nested)
    assert table.name() == test_table_identifier
    catalog.rename_table(table.name(), another_table_identifier)
    new_table = catalog.load_table(another_table_identifier)
    assert new_table.name() == another_table_identifier
    assert new_table.metadata_location == table.metadata_location
    with pytest.raises(NoSuchTableError):
        catalog.load_table(table.name())
    with pytest.raises(NoSuchTableError):
        catalog.load_table(test_table_identifier)


def test_rename_table_to_existing_one(
    catalog: Catalog, table_schema_nested: Schema, test_table_identifier: Identifier, another_table_identifier: Identifier
) -> None:
    from_namespace = Catalog.namespace_from(test_table_identifier)
    to_namespace = Catalog.namespace_from(another_table_identifier)
    catalog.create_namespace(from_namespace)
    catalog.create_namespace(to_namespace)
    table = catalog.create_table(test_table_identifier, table_schema_nested)
    assert table.name() == test_table_identifier
    new_table = catalog.create_table(another_table_identifier, table_schema_nested)
    assert new_table.name() == another_table_identifier
    with pytest.raises(TableAlreadyExistsError):
        catalog.rename_table(test_table_identifier, another_table_identifier)


def test_rename_missing_table(catalog: Catalog, test_table_identifier: Identifier, another_table_identifier: Identifier) -> None:
    from_namespace = Catalog.namespace_from(test_table_identifier)
    to_namespace = Catalog.namespace_from(another_table_identifier)
    catalog.create_namespace(from_namespace)
    catalog.create_namespace(to_namespace)
    with pytest.raises(NoSuchTableError):
        catalog.rename_table(test_table_identifier, another_table_identifier)


def test_rename_table_to_missing_namespace(
    catalog: Catalog, table_schema_nested: Schema, test_table_identifier: Identifier, another_table_identifier: Identifier
) -> None:
    from_namespace = Catalog.namespace_from(test_table_identifier)
    catalog.create_namespace(from_namespace)
    table = catalog.create_table(test_table_identifier, table_schema_nested)
    assert table.name() == test_table_identifier
    with pytest.raises(NoSuchNamespaceError):
        catalog.rename_table(test_table_identifier, another_table_identifier)


# Drop table tests


def test_drop_table(catalog: Catalog, table_schema_nested: Schema, test_table_identifier: Identifier) -> None:
    identifier_tuple = Catalog.identifier_to_tuple(test_table_identifier)
    namespace = Catalog.namespace_from(test_table_identifier)
    catalog.create_namespace(namespace)
    table = catalog.create_table(test_table_identifier, table_schema_nested)
    assert table.name() == identifier_tuple
    catalog.drop_table(test_table_identifier)
    with pytest.raises(NoSuchTableError):
        catalog.load_table(test_table_identifier)


def test_drop_table_from_self_identifier(
    catalog: Catalog, table_schema_nested: Schema, test_table_identifier: Identifier
) -> None:
    identifier_tuple = Catalog.identifier_to_tuple(test_table_identifier)
    namespace = Catalog.namespace_from(test_table_identifier)
    catalog.create_namespace(namespace)
    table = catalog.create_table(test_table_identifier, table_schema_nested)
    assert table.name() == identifier_tuple
    catalog.drop_table(table.name())
    with pytest.raises(NoSuchTableError):
        catalog.load_table(table.name())
    with pytest.raises(NoSuchTableError):
        catalog.load_table(test_table_identifier)


def test_drop_table_that_does_not_exist_raise_error(catalog: Catalog, test_table_identifier: Identifier) -> None:
    with pytest.raises(NoSuchTableError):
        catalog.drop_table(test_table_identifier)


def test_purge_table(catalog: Catalog, table_schema_simple: Schema, test_table_identifier: Identifier) -> None:
    namespace = Catalog.namespace_from(test_table_identifier)
    catalog.create_namespace(namespace)
    catalog.create_table(test_table_identifier, table_schema_simple)
    catalog.purge_table(test_table_identifier)
    with pytest.raises(NoSuchTableError, match=f"Table does not exist: {'.'.join(test_table_identifier)}"):
        catalog.load_table(test_table_identifier)


# List tables tests


def test_list_tables(
    catalog: Catalog, table_schema_nested: Schema, test_table_identifier: Identifier, another_table_identifier: Identifier
) -> None:
    namespace_1 = Catalog.namespace_from(test_table_identifier)
    namespace_2 = Catalog.namespace_from(another_table_identifier)
    catalog.create_namespace(namespace_1)
    catalog.create_namespace(namespace_2)
    catalog.create_table(test_table_identifier, table_schema_nested)
    catalog.create_table(another_table_identifier, table_schema_nested)
    identifier_list = catalog.list_tables(namespace_1)
    assert len(identifier_list) == 1
    assert test_table_identifier in identifier_list

    identifier_list = catalog.list_tables(namespace_2)
    assert len(identifier_list) == 1
    assert another_table_identifier in identifier_list


def test_list_tables_under_a_namespace(catalog: Catalog, table_schema_nested: Schema, test_table_identifier: Identifier) -> None:
    namespace = Catalog.namespace_from(test_table_identifier)
    catalog.create_namespace(namespace)
    catalog.create_table(test_table_identifier, table_schema_nested)
    new_namespace = ("new", "namespace")
    catalog.create_namespace(new_namespace)
    all_tables = catalog.list_tables(namespace=namespace)
    new_namespace_tables = catalog.list_tables(new_namespace)
    assert all_tables
    assert test_table_identifier in all_tables
    assert new_namespace_tables == []


def test_list_tables_when_missing_namespace(catalog: Catalog, test_namespace: Identifier) -> None:
    with pytest.raises(NoSuchNamespaceError):
        catalog.list_tables(test_namespace)


# Commit table tests
def test_commit_table(catalog: Catalog, table_schema_nested: Schema, test_table_identifier: Identifier) -> None:
    namespace = Catalog.namespace_from(test_table_identifier)
    catalog.create_namespace(namespace)
    table = catalog.create_table(test_table_identifier, table_schema_nested)
    last_updated_ms = table.metadata.last_updated_ms
    original_table_metadata_location = table.metadata_location
    original_table_last_updated_ms = table.metadata.last_updated_ms

    assert catalog._parse_metadata_version(table.metadata_location) == 0  # type: ignore[attr-defined]
    assert table.metadata.current_schema_id == 0

    transaction = table.transaction()
    update = transaction.update_schema()
    update.add_column(path="b", field_type=IntegerType())
    update.commit()
    transaction.commit_transaction()

    updated_table_metadata = table.metadata

    assert catalog._parse_metadata_version(table.metadata_location) == 1  # type: ignore[attr-defined]
    assert updated_table_metadata.current_schema_id == 1
    assert len(updated_table_metadata.schemas) == 2
    new_schema = next(schema for schema in updated_table_metadata.schemas if schema.schema_id == 1)
    assert new_schema
    assert new_schema == update._apply()
    assert new_schema.find_field("b").field_type == IntegerType()
    assert updated_table_metadata.last_updated_ms > last_updated_ms
    assert len(updated_table_metadata.metadata_log) == 1
    assert updated_table_metadata.metadata_log[0].metadata_file == original_table_metadata_location
    assert updated_table_metadata.metadata_log[0].timestamp_ms == original_table_last_updated_ms


def test_catalog_commit_table_applies_schema_updates(
    catalog: Catalog,
    table_schema_nested: Schema,
    test_table_identifier: Identifier,
) -> None:
    namespace = Catalog.namespace_from(test_table_identifier)
    catalog.create_namespace(namespace)
    table = catalog.create_table(test_table_identifier, table_schema_nested)

    new_schema = Schema(
        NestedField(1, "x", LongType()),
        NestedField(2, "y", LongType(), doc="comment"),
        NestedField(3, "z", LongType()),
        NestedField(4, "add", LongType()),
    )

    response = table.catalog.commit_table(
        table,
        updates=(
            AddSchemaUpdate(schema=new_schema),
            SetCurrentSchemaUpdate(),
        ),
        requirements=(),
    )
    assert response.metadata.table_uuid == table.metadata.table_uuid
    assert len(response.metadata.schemas) == 2
    assert response.metadata.schemas[1] == new_schema
    assert response.metadata.current_schema_id == new_schema.schema_id


def test_concurrent_commit_table(catalog: Catalog, table_schema_nested: Schema, test_table_identifier: Identifier) -> None:
    namespace = Catalog.namespace_from(test_table_identifier)
    catalog.create_namespace(namespace)
    table_a = catalog.create_table(test_table_identifier, table_schema_nested)
    table_b = catalog.load_table(test_table_identifier)

    with table_a.update_schema() as update:
        update.add_column(path="b", field_type=IntegerType())

    with pytest.raises(CommitFailedException, match="Requirement failed: current schema id has changed: expected 0, found 1"):
        # This one should fail since it already has been updated
        with table_b.update_schema() as update:
            update.add_column(path="c", field_type=IntegerType())


def test_delete_metadata_multiple(catalog: Catalog, table_schema_nested: Schema, random_table_identifier: Identifier) -> None:
    namespace = Catalog.namespace_from(random_table_identifier)
    catalog.create_namespace(namespace)
    table = catalog.create_table(random_table_identifier, table_schema_nested)

    original_metadata_location = table.metadata_location

    for i in range(5):
        with table.transaction() as transaction:
            with transaction.update_schema() as update:
                update.add_column(path=f"new_column_{i}", field_type=IntegerType())

    assert len(table.metadata.metadata_log) == 5
    assert os.path.exists(original_metadata_location[len("file://") :])

    # Set the max versions property to 2, and delete after commit
    new_property = {
        TableProperties.METADATA_PREVIOUS_VERSIONS_MAX: "2",
        TableProperties.METADATA_DELETE_AFTER_COMMIT_ENABLED: "true",
    }

    with table.transaction() as transaction:
        transaction.set_properties(properties=new_property)

    # Verify that only the most recent metadata files are kept
    assert len(table.metadata.metadata_log) == 2
    updated_metadata_1, updated_metadata_2 = table.metadata.metadata_log

    # new metadata log was added, so earlier metadata logs are removed.
    with table.transaction() as transaction:
        with transaction.update_schema() as update:
            update.add_column(path="new_column_x", field_type=IntegerType())

    assert len(table.metadata.metadata_log) == 2
    assert not os.path.exists(original_metadata_location[len("file://") :])
    assert not os.path.exists(updated_metadata_1.metadata_file[len("file://") :])
    assert os.path.exists(updated_metadata_2.metadata_file[len("file://") :])


# Table properties tests


def test_table_properties_int_value(catalog: Catalog, table_schema_simple: Schema, test_table_identifier: Identifier) -> None:
    # table properties can be set to int, but still serialized to string
    namespace = Catalog.namespace_from(test_table_identifier)
    catalog.create_namespace(namespace)
    property_with_int = {"property_name": 42}
    table = catalog.create_table(test_table_identifier, table_schema_simple, properties=property_with_int)
    assert isinstance(table.properties["property_name"], str)


def test_table_properties_raise_for_none_value(
    catalog: Catalog, table_schema_simple: Schema, test_table_identifier: Identifier
) -> None:
    namespace = Catalog.namespace_from(test_table_identifier)
    catalog.create_namespace(namespace)
    property_with_none = {"property_name": None}
    with pytest.raises(ValidationError) as exc_info:
        _ = catalog.create_table(test_table_identifier, table_schema_simple, properties=property_with_none)
    assert "None type is not a supported value in properties: property_name" in str(exc_info.value)


# Append table


def test_append_table(catalog: Catalog, table_schema_simple: Schema, test_table_identifier: Identifier) -> None:
    namespace = Catalog.namespace_from(test_table_identifier)
    catalog.create_namespace(namespace)
    table = catalog.create_table(test_table_identifier, table_schema_simple)

    df = pa.Table.from_pydict(
        {
            "foo": ["a"],
            "bar": [1],
            "baz": [True],
        },
        schema=schema_to_pyarrow(table_schema_simple),
    )

    table.append(df)

    # new snapshot is written in APPEND mode
    assert len(table.metadata.snapshots) == 1
    assert table.metadata.snapshots[0].snapshot_id == table.metadata.current_snapshot_id
    assert table.metadata.snapshots[0].parent_snapshot_id is None
    assert table.metadata.snapshots[0].sequence_number == 1
    assert table.metadata.snapshots[0].summary is not None
    assert table.metadata.snapshots[0].summary.operation == Operation.APPEND
    assert table.metadata.snapshots[0].summary["added-data-files"] == "1"
    assert table.metadata.snapshots[0].summary["added-records"] == "1"
    assert table.metadata.snapshots[0].summary["total-data-files"] == "1"
    assert table.metadata.snapshots[0].summary["total-records"] == "1"
    assert len(table.metadata.metadata_log) == 1

    # read back the data
    assert df == table.scan().to_arrow()


# Test writes
def test_table_writes_metadata_to_custom_location(
    catalog: Catalog,
    test_table_identifier: Identifier,
    table_schema_simple: Schema,
    warehouse: Path,
) -> None:
    namespace = Catalog.namespace_from(test_table_identifier)
    metadata_path = f"file://{warehouse}/custom/path"
    catalog.create_namespace(namespace)
    table = catalog.create_table(
        identifier=test_table_identifier,
        schema=table_schema_simple,
        properties={TableProperties.WRITE_METADATA_PATH: metadata_path},
    )
    df = pa.Table.from_pydict(
        {"foo": ["a"], "bar": [1], "baz": [True]},
        schema=schema_to_pyarrow(table_schema_simple),
    )
    table.append(df)
    snapshot = table.current_snapshot()
    assert snapshot is not None
    manifests = snapshot.manifests(table.io)
    location_provider = table.location_provider()

    assert location_provider.new_metadata_location("").startswith(metadata_path)
    assert manifests[0].manifest_path.startswith(metadata_path)
    assert table.location() != metadata_path
    assert table.metadata_location.startswith(metadata_path)


def test_table_writes_metadata_to_default_path(
    catalog: Catalog,
    test_table_identifier: Identifier,
    table_schema_simple: Schema,
    test_table_properties: dict[str, str],
) -> None:
    namespace = Catalog.namespace_from(test_table_identifier)
    catalog.create_namespace(namespace)
    table = catalog.create_table(
        identifier=test_table_identifier,
        schema=table_schema_simple,
        properties=test_table_properties,
    )
    metadata_path = f"{table.location()}/metadata"
    df = pa.Table.from_pydict(
        {"foo": ["a"], "bar": [1], "baz": [True]},
        schema=schema_to_pyarrow(table_schema_simple),
    )
    table.append(df)
    snapshot = table.current_snapshot()
    assert snapshot is not None
    manifests = snapshot.manifests(table.io)
    location_provider = table.location_provider()

    assert location_provider.new_metadata_location("").startswith(metadata_path)
    assert manifests[0].manifest_path.startswith(metadata_path)
    assert table.metadata_location.startswith(metadata_path)


def test_table_metadata_writes_reflect_latest_path(
    catalog: Catalog,
    test_table_identifier: Identifier,
    table_schema_simple: Schema,
    warehouse: Path,
) -> None:
    namespace = Catalog.namespace_from(test_table_identifier)
    catalog.create_namespace(namespace)
    table = catalog.create_table(
        identifier=test_table_identifier,
        schema=table_schema_simple,
    )

    initial_metadata_path = f"{table.location()}/metadata"
    assert table.location_provider().new_metadata_location("metadata.json") == f"{initial_metadata_path}/metadata.json"

    # update table with new path for metadata
    new_metadata_path = f"file://{warehouse}/custom/path"
    table.transaction().set_properties({TableProperties.WRITE_METADATA_PATH: new_metadata_path}).commit_transaction()

    assert table.location_provider().new_metadata_location("metadata.json") == f"{new_metadata_path}/metadata.json"


@pytest.mark.parametrize("format_version", [1, 2])
def test_write_and_evolve(catalog: Catalog, format_version: int) -> None:
    identifier = f"default.arrow_write_data_and_evolve_schema_v{format_version}"

    try:
        catalog.create_namespace("default")
    except NamespaceAlreadyExistsError:
        pass

    try:
        catalog.drop_table(identifier=identifier)
    except NoSuchTableError:
        pass

    pa_table = pa.Table.from_pydict(
        {
            "foo": ["a", None, "z"],
        },
        schema=pa.schema([pa.field("foo", pa.large_string(), nullable=True)]),
    )

    tbl = catalog.create_table(identifier=identifier, schema=pa_table.schema, properties={"format-version": str(format_version)})

    pa_table_with_column = pa.Table.from_pydict(
        {
            "foo": ["a", None, "z"],
            "bar": [19, None, 25],
        },
        schema=pa.schema(
            [
                pa.field("foo", pa.large_string(), nullable=True),
                pa.field("bar", pa.int32(), nullable=True),
            ]
        ),
    )

    with tbl.transaction() as txn:
        with txn.update_schema() as schema_txn:
            schema_txn.union_by_name(pa_table_with_column.schema)

        txn.append(pa_table_with_column)
        txn.overwrite(pa_table_with_column)
        txn.delete("foo = 'a'")


# Merge manifests
@pytest.mark.parametrize("format_version", [1, 2])
def test_merge_manifests_local_file_system(catalog: Catalog, arrow_table_with_null: pa.Table, format_version: int) -> None:
    # To catch manifest file name collision bug during merge:
    # https://github.com/apache/iceberg-python/pull/363#discussion_r1660691918
    catalog.create_namespace_if_not_exists("default")
    try:
        catalog.drop_table("default.test_merge_manifest")
    except NoSuchTableError:
        pass
    tbl = catalog.create_table(
        "default.test_merge_manifest",
        arrow_table_with_null.schema,
        properties={
            "commit.manifest-merge.enabled": "true",
            "commit.manifest.min-count-to-merge": "2",
            "format-version": format_version,
        },
    )

    for _ in range(5):
        tbl.append(arrow_table_with_null)

    assert len(tbl.scan().to_arrow()) == 5 * len(arrow_table_with_null)
    current_snapshot = tbl.current_snapshot()
    assert current_snapshot
    manifests = current_snapshot.manifests(tbl.io)
    assert len(manifests) == 1


# Add column to table


def test_add_column(catalog: Catalog, table_schema_simple: Schema, random_table_identifier: Identifier) -> None:
    namespace = Catalog.namespace_from(random_table_identifier)
    catalog.create_namespace(namespace)
    table = catalog.create_table(random_table_identifier, table_schema_simple)
    table.update_schema().add_column(path="new_column1", field_type=IntegerType()).commit()
    assert table.schema() == Schema(
        NestedField(field_id=1, name="foo", field_type=StringType(), required=False),
        NestedField(field_id=2, name="bar", field_type=IntegerType(), required=True),
        NestedField(field_id=3, name="baz", field_type=BooleanType(), required=False),
        NestedField(field_id=4, name="new_column1", field_type=IntegerType(), required=False),
        schema_id=1,
        identifier_field_ids=[2],
    )
    assert table.schema().schema_id == 1

    transaction = table.transaction()
    transaction.update_schema().add_column(path="new_column2", field_type=IntegerType(), doc="doc").commit()
    transaction.commit_transaction()

    assert table.schema() == Schema(
        NestedField(field_id=1, name="foo", field_type=StringType(), required=False),
        NestedField(field_id=2, name="bar", field_type=IntegerType(), required=True),
        NestedField(field_id=3, name="baz", field_type=BooleanType(), required=False),
        NestedField(field_id=4, name="new_column1", field_type=IntegerType(), required=False),
        NestedField(field_id=5, name="new_column2", field_type=IntegerType(), required=False, doc="doc"),
        identifier_field_ids=[2],
    )
    assert table.schema().schema_id == 2


def test_add_column_with_statement(catalog: Catalog, table_schema_simple: Schema, random_table_identifier: Identifier) -> None:
    namespace = Catalog.namespace_from(random_table_identifier)
    catalog.create_namespace(namespace)
    table = catalog.create_table(random_table_identifier, table_schema_simple)

    with table.update_schema() as tx:
        tx.add_column(path="new_column1", field_type=IntegerType())

    assert table.schema() == Schema(
        NestedField(field_id=1, name="foo", field_type=StringType(), required=False),
        NestedField(field_id=2, name="bar", field_type=IntegerType(), required=True),
        NestedField(field_id=3, name="baz", field_type=BooleanType(), required=False),
        NestedField(field_id=4, name="new_column1", field_type=IntegerType(), required=False),
        identifier_field_ids=[2],
    )
    assert table.schema().schema_id == 1

    with table.transaction() as tx:
        tx.update_schema().add_column(path="new_column2", field_type=IntegerType(), doc="doc").commit()

    assert table.schema() == Schema(
        NestedField(field_id=1, name="foo", field_type=StringType(), required=False),
        NestedField(field_id=2, name="bar", field_type=IntegerType(), required=True),
        NestedField(field_id=3, name="baz", field_type=BooleanType(), required=False),
        NestedField(field_id=4, name="new_column1", field_type=IntegerType(), required=False),
        NestedField(field_id=5, name="new_column2", field_type=IntegerType(), required=False, doc="doc"),
        identifier_field_ids=[2],
    )
    assert table.schema().schema_id == 2


def test_update_schema_with_statement_does_not_commit_on_exception(
    catalog: Catalog, table_schema_simple: Schema, random_table_identifier: Identifier
) -> None:
    namespace = Catalog.namespace_from(random_table_identifier)
    catalog.create_namespace(namespace)
    table = catalog.create_table(random_table_identifier, table_schema_simple)

    with pytest.raises(ValueError):
        with table.update_schema() as tx:
            tx.add_column(path="should_not_commit", field_type=IntegerType())
            int("boom")

    assert table.schema() == table_schema_simple
    assert table.schema().schema_id == 0

    reloaded = catalog.load_table(random_table_identifier)
    assert reloaded.schema() == table_schema_simple
    assert reloaded.schema().schema_id == 0


# Namespace tests


def test_create_namespace(catalog: Catalog, test_namespace: Identifier, test_table_properties: dict[str, str]) -> None:
    catalog.create_namespace(test_namespace, test_table_properties)
    assert catalog.namespace_exists(test_namespace)
    assert (Catalog.identifier_to_tuple(test_namespace)[:1]) in catalog.list_namespaces()
    assert test_table_properties == catalog.load_namespace_properties(test_namespace)


def test_create_namespace_raises_error_on_existing_namespace(
    catalog: Catalog, test_namespace: Identifier, test_table_properties: dict[str, str]
) -> None:
    catalog.create_namespace(test_namespace, test_table_properties)
    with pytest.raises(NamespaceAlreadyExistsError):
        catalog.create_namespace(test_namespace, test_table_properties)


def test_create_namespace_if_not_exists(catalog: Catalog, database_name: str) -> None:
    catalog.create_namespace(database_name)
    assert (database_name,) in catalog.list_namespaces()
    catalog.create_namespace_if_not_exists(database_name)
    assert (database_name,) in catalog.list_namespaces()


def test_create_namespaces_sharing_same_prefix(catalog: Catalog, test_namespace: Identifier) -> None:
    child_namespace = test_namespace + ("child",)
    # Parent first
    catalog.create_namespace(test_namespace)
    # Then child
    catalog.create_namespace(child_namespace)


def test_create_namespace_with_comment_and_location(catalog: Catalog, test_namespace: Identifier) -> None:
    test_location = "/test/location"
    test_properties = {
        "comment": "this is a test description",
        "location": test_location,
    }
    catalog.create_namespace(namespace=test_namespace, properties=test_properties)
    loaded_database_list = catalog.list_namespaces()
    assert Catalog.identifier_to_tuple(test_namespace)[:1] in loaded_database_list
    properties = catalog.load_namespace_properties(test_namespace)
    assert properties["comment"] == "this is a test description"
    assert properties["location"] == test_location


@pytest.mark.filterwarnings("ignore")
def test_create_namespace_with_null_properties(catalog: Catalog, test_namespace: Identifier) -> None:
    with pytest.raises(IntegrityError):
        catalog.create_namespace(namespace=test_namespace, properties={None: "value"})  # type: ignore

    with pytest.raises(IntegrityError):
        catalog.create_namespace(namespace=test_namespace, properties={"key": None})


@pytest.mark.parametrize("empty_namespace", ["", (), (""), ("", ""), " ", (" ")])
def test_create_namespace_with_empty_identifier(catalog: Catalog, empty_namespace: Any) -> None:
    with pytest.raises(NoSuchNamespaceError):
        catalog.create_namespace(empty_namespace)


# Get namespace tests


def test_get_namespace_metadata_raises_error_when_namespace_does_not_exist(catalog: Catalog, test_namespace: Identifier) -> None:
    namespace = ".".join(test_namespace)
    with pytest.raises(NoSuchNamespaceError, match=f"Namespace {namespace} does not exists"):
        catalog.load_namespace_properties(test_namespace)


def test_namespace_exists(catalog: Catalog) -> None:
    for ns in [("db1",), ("db1", "ns1"), ("db2", "ns1"), ("db3", "ns1", "ns2")]:
        catalog.create_namespace(ns)
        assert catalog.namespace_exists(ns)

    # `db2` exists because `db2.ns1` exists
    assert catalog.namespace_exists("db2")
    # `db3.ns1` exists because `db3.ns1.ns2` exists
    assert catalog.namespace_exists("db3.ns1")
    # make sure '_' is escaped in the query
    assert not catalog.namespace_exists("db_")
    # make sure '%' is escaped in the query
    assert not catalog.namespace_exists("db%")


# Namespace properties


def test_load_namespace_properties(catalog: Catalog, test_namespace: Identifier) -> None:
    warehouse_location = "/test/location"
    test_properties = {
        "comment": "this is a test description",
        "location": f"{warehouse_location}/{test_namespace}",
        "test_property1": "1",
        "test_property2": "2",
        "test_property3": "3",
    }

    catalog.create_namespace(test_namespace, test_properties)
    listed_properties = catalog.load_namespace_properties(test_namespace)
    for k, v in listed_properties.items():
        assert k in test_properties
        assert v == test_properties[k]


def test_load_namespace_properties_non_existing_namespace(catalog: Catalog) -> None:
    with pytest.raises(NoSuchNamespaceError):
        catalog.load_namespace_properties("does_not_exist")


def test_load_empty_namespace_properties(catalog: Catalog, test_namespace: Identifier) -> None:
    catalog.create_namespace(test_namespace)
    listed_properties = catalog.load_namespace_properties(test_namespace)
    assert listed_properties == {"exists": "true"}


# List namespaces tests


def test_list_namespaces(catalog: Catalog) -> None:
    namespace_list = ["db", "db.ns1", "db.ns1.ns2", "db.ns2", "db2", "db2.ns1", "db%"]
    for namespace in namespace_list:
        if not catalog.namespace_exists(namespace):
            catalog.create_namespace(namespace)

    ns_list = catalog.list_namespaces()
    for ns in [("db",), ("db%",), ("db2",)]:
        assert ns in ns_list

    ns_list = catalog.list_namespaces("db")
    assert sorted(ns_list) == [("db", "ns1"), ("db", "ns2")]

    ns_list = catalog.list_namespaces("db.ns1")
    assert sorted(ns_list) == [("db", "ns1", "ns2")]

    ns_list = catalog.list_namespaces("db.ns1.ns2")
    assert len(ns_list) == 0


def test_list_namespaces_fuzzy_match(catalog: Catalog) -> None:
    namespace_list = ["db.ns1", "db.ns1.ns2", "db.ns2", "db.ns1X.ns3", "db_.ns1.ns2", "db2.ns1.ns2"]
    for namespace in namespace_list:
        if not catalog.namespace_exists(namespace):
            catalog.create_namespace(namespace)

    assert catalog.list_namespaces("db.ns1") == [("db", "ns1", "ns2")]

    assert catalog.list_namespaces("db_.ns1") == [("db_", "ns1", "ns2")]


def test_list_non_existing_namespaces(catalog: Catalog) -> None:
    with pytest.raises(NoSuchNamespaceError):
        catalog.list_namespaces("does_not_exist")


# Update namespace properties tests


def test_update_namespace_properties(catalog: Catalog, test_namespace: Identifier) -> None:
    warehouse_location = "/test/location"
    test_properties = {
        "comment": "this is a test description",
        "location": f"{warehouse_location}/{test_namespace}",
        "test_property1": "1",
        "test_property2": "2",
        "test_property3": "3",
    }
    removals = {"test_property1", "test_property2", "test_property3", "should_not_removed"}
    updates = {"test_property4": "4", "test_property5": "5", "comment": "updated test description"}
    catalog.create_namespace(test_namespace, test_properties)
    update_report = catalog.update_namespace_properties(test_namespace, removals, updates)
    for k in updates.keys():
        assert k in update_report.updated
    for k in removals:
        if k == "should_not_removed":
            assert k in update_report.missing
        else:
            assert k in update_report.removed
    assert catalog.load_namespace_properties(test_namespace) == {
        "comment": "updated test description",
        "test_property4": "4",
        "test_property5": "5",
        "location": f"{warehouse_location}/{test_namespace}",
    }


def test_update_namespace_metadata_raises_error_when_namespace_does_not_exist(
    catalog: Catalog, test_namespace: Identifier, test_table_properties: dict[str, str]
) -> None:
    namespace = ".".join(test_namespace)
    with pytest.raises(NoSuchNamespaceError, match=f"Namespace {namespace} does not exists"):
        catalog.update_namespace_properties(test_namespace, updates=test_table_properties)


def test_update_namespace_metadata(catalog: Catalog, test_namespace: Identifier, test_table_properties: dict[str, str]) -> None:
    catalog.create_namespace(test_namespace, test_table_properties)
    new_metadata = {"key3": "value3", "key4": "value4"}
    summary = catalog.update_namespace_properties(test_namespace, updates=new_metadata)
    assert catalog.namespace_exists(test_namespace)
    assert new_metadata.items() <= catalog.load_namespace_properties(test_namespace).items()
    assert summary.removed == []
    assert sorted(summary.updated) == ["key3", "key4"]
    assert summary.missing == []


def test_update_namespace_metadata_removals(
    catalog: Catalog, test_namespace: Identifier, test_table_properties: dict[str, str]
) -> None:
    catalog.create_namespace(test_namespace, test_table_properties)
    new_metadata = {"key3": "value3", "key4": "value4"}
    remove_metadata = {"key1"}
    summary = catalog.update_namespace_properties(test_namespace, remove_metadata, new_metadata)
    assert catalog.namespace_exists(test_namespace)
    assert new_metadata.items() <= catalog.load_namespace_properties(test_namespace).items()
    assert remove_metadata.isdisjoint(catalog.load_namespace_properties(test_namespace).keys())
    assert summary.removed == ["key1"]
    assert sorted(summary.updated) == ["key3", "key4"]
    assert summary.missing == []


# Drop namespace tests


def test_drop_namespace(catalog: Catalog, table_schema_nested: Schema, test_table_identifier: Identifier) -> None:
    namespace = Catalog.namespace_from(test_table_identifier)
    catalog.create_namespace(namespace)
    assert catalog.namespace_exists(namespace)
    catalog.create_table(test_table_identifier, table_schema_nested)
    with pytest.raises(NamespaceNotEmptyError):
        catalog.drop_namespace(namespace)
    catalog.drop_table(test_table_identifier)
    catalog.drop_namespace(namespace)
    assert not catalog.namespace_exists(namespace)


def test_drop_namespace_raises_error_when_namespace_does_not_exist(catalog: Catalog) -> None:
    with pytest.raises(NoSuchNamespaceError):
        catalog.drop_namespace("does_not_exist")


def test_drop_namespace_raises_error_when_namespace_not_empty(
    catalog: Catalog, table_schema_nested: Schema, test_table_identifier: Identifier
) -> None:
    namespace = Catalog.namespace_from(test_table_identifier)
    catalog.create_namespace(namespace)
    catalog.create_table(test_table_identifier, table_schema_nested)
    with pytest.raises(NamespaceNotEmptyError, match=f"Namespace {'.'.join(namespace)} is not empty"):
        catalog.drop_namespace(namespace)


# RecordBatchReader streaming append/overwrite tests
#
# Streaming writes accept a pa.RecordBatchReader and microbatch it into target-sized
# Parquet files instead of materialising the full Arrow Table in memory. Tracks
# https://github.com/apache/iceberg-python/issues/2152.


def _simple_arrow_table() -> pa.Table:
    return pa.Table.from_pydict(
        {"foo": ["a", None, "z"]},
        schema=pa.schema([pa.field("foo", pa.large_string(), nullable=True)]),
    )


def _simple_record_batch_reader(num_batches: int = 3) -> tuple[pa.RecordBatchReader, int]:
    """Build an N-batch reader of the simple schema. Returns (reader, total_rows)."""
    pa_table = _simple_arrow_table()
    batches = pa_table.to_batches() * num_batches
    reader = pa.RecordBatchReader.from_batches(pa_table.schema, iter(batches))
    return reader, sum(b.num_rows for b in batches)


def test_append_record_batch_reader(catalog: Catalog) -> None:
    catalog.create_namespace("default")
    identifier = f"default.append_record_batch_reader_{catalog.name}"
    reader, total_rows = _simple_record_batch_reader(num_batches=3)
    tbl = catalog.create_table(identifier=identifier, schema=reader.schema)

    tbl.append(reader)

    assert len(tbl.scan().to_arrow()) == total_rows


def test_append_record_batch_reader_microbatched(catalog: Catalog) -> None:
    """A reader bigger than the per-file target produces multiple Parquet files
    in a single snapshot — verifying the byte-budget microbatching path."""
    catalog.create_namespace("default")
    identifier = f"default.append_record_batch_reader_microbatch_{catalog.name}"
    reader, total_rows = _simple_record_batch_reader(num_batches=8)
    # Force every batch to roll a new file by setting an absurdly small target size.
    tbl = catalog.create_table(
        identifier=identifier,
        schema=reader.schema,
        properties={TableProperties.WRITE_TARGET_FILE_SIZE_BYTES: "1"},
    )

    tbl.append(reader)

    snapshot = tbl.metadata.current_snapshot()
    assert snapshot is not None
    assert snapshot.summary is not None
    added_files = snapshot.summary["added-data-files"]
    assert added_files is not None and int(added_files) > 1, snapshot.summary
    assert len(tbl.scan().to_arrow()) == total_rows


def test_append_record_batch_reader_empty(catalog: Catalog) -> None:
    catalog.create_namespace("default")
    identifier = f"default.append_record_batch_reader_empty_{catalog.name}"
    schema = _simple_arrow_table().schema
    reader = pa.RecordBatchReader.from_batches(schema, iter([]))
    tbl = catalog.create_table(identifier=identifier, schema=schema)

    tbl.append(reader)

    assert len(tbl.scan().to_arrow()) == 0


def test_overwrite_record_batch_reader(catalog: Catalog) -> None:
    catalog.create_namespace("default")
    identifier = f"default.overwrite_record_batch_reader_{catalog.name}"
    pa_table = _simple_arrow_table()
    tbl = catalog.create_table(identifier=identifier, schema=pa_table.schema)
    tbl.append(pa_table)
    assert len(tbl.scan().to_arrow()) == pa_table.num_rows

    reader, total_rows = _simple_record_batch_reader(num_batches=2)
    tbl.overwrite(reader)

    assert len(tbl.scan().to_arrow()) == total_rows


def test_append_record_batch_reader_to_partitioned_table_raises(catalog: Catalog) -> None:
    catalog.create_namespace("default")
    identifier = f"default.append_record_batch_reader_partitioned_{catalog.name}"
    iceberg_schema = Schema(
        NestedField(1, "id", IntegerType(), required=False),
        NestedField(2, "bucket", StringType(), required=False),
    )
    partition_spec = PartitionSpec(
        PartitionField(source_id=2, field_id=1000, transform=IdentityTransform(), name="bucket"),
    )
    tbl = catalog.create_table(identifier=identifier, schema=iceberg_schema, partition_spec=partition_spec)

    arrow_schema = schema_to_pyarrow(iceberg_schema)
    reader = pa.RecordBatchReader.from_batches(arrow_schema, iter([]))
    with pytest.raises(NotImplementedError, match="partitioned table"):
        tbl.append(reader)


def test_append_invalid_input_type_raises(catalog: Catalog) -> None:
    catalog.create_namespace("default")
    identifier = f"default.append_invalid_input_{catalog.name}"
    pa_table = _simple_arrow_table()
    tbl = catalog.create_table(identifier=identifier, schema=pa_table.schema)
    with pytest.raises(ValueError, match="Expected pa.Table or pa.RecordBatchReader"):
        tbl.append("not an arrow object")


def test_record_batch_reader_consumed_exactly_once(catalog: Catalog) -> None:
    """The streaming path must consume the underlying generator exactly once.
    A regression that drained the reader twice (e.g. an extra .schema access
    that materialised the iterator, or a retry-loop without a fresh reader)
    would silently lose data — the second pass is empty.
    """
    catalog.create_namespace("default")
    identifier = f"default.record_batch_reader_consumed_once_{catalog.name}"
    pa_table = _simple_arrow_table()
    consumed_batches = 0

    def tracking_batches() -> Generator[pa.RecordBatch, None, None]:
        nonlocal consumed_batches
        for batch in pa_table.to_batches() * 3:
            consumed_batches += 1
            yield batch

    reader = pa.RecordBatchReader.from_batches(pa_table.schema, tracking_batches())
    tbl = catalog.create_table(identifier=identifier, schema=pa_table.schema)

    tbl.append(reader)

    # The generator should have been driven to exhaustion exactly once: 3 batches.
    assert consumed_batches == 3
    assert len(tbl.scan().to_arrow()) == pa_table.num_rows * 3


def test_record_batch_reader_schema_mismatch_writes_no_files(catalog: Catalog) -> None:
    """A schema mismatch must fail before any data files are written. Otherwise
    we'd leak orphan parquet files in storage (and a partial commit that picks
    them up later via add_files would be a correctness disaster).
    """
    catalog.create_namespace("default")
    identifier = f"default.record_batch_reader_schema_mismatch_{catalog.name}"
    iceberg_schema = Schema(NestedField(1, "foo", StringType(), required=False))
    tbl = catalog.create_table(identifier=identifier, schema=iceberg_schema)

    bad_schema = pa.schema([pa.field("foo", pa.int64(), nullable=True)])
    bad_reader = pa.RecordBatchReader.from_batches(
        bad_schema,
        iter([pa.RecordBatch.from_pylist([{"foo": 1}], schema=bad_schema)]),
    )

    with pytest.raises(ValueError):
        tbl.append(bad_reader)

    # No snapshot should have been produced: the schema check runs before
    # _append_snapshot_producer opens.
    assert tbl.metadata.current_snapshot() is None
    assert len(tbl.scan().to_arrow()) == 0
