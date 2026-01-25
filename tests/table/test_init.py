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
import json
import uuid
from copy import copy
from typing import Any

import pytest
from pydantic import BaseModel, ValidationError

from pyiceberg.catalog.noop import NoopCatalog
from pyiceberg.exceptions import CommitFailedException
from pyiceberg.expressions import (
    AlwaysTrue,
    And,
    EqualTo,
    In,
)
from pyiceberg.io import PY_IO_IMPL, load_file_io
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table import (
    CommitTableRequest,
    StaticTable,
    Table,
    TableIdentifier,
)
from pyiceberg.table.metadata import TableMetadataUtil, TableMetadataV2, _generate_snapshot_id
from pyiceberg.table.refs import MAIN_BRANCH, SnapshotRef, SnapshotRefType
from pyiceberg.table.snapshots import (
    MetadataLogEntry,
    Operation,
    Snapshot,
    SnapshotLogEntry,
    Summary,
)
from pyiceberg.table.sorting import (
    NullOrder,
    SortDirection,
    SortField,
    SortOrder,
)
from pyiceberg.table.statistics import BlobMetadata, PartitionStatisticsFile, StatisticsFile
from pyiceberg.table.update import (
    AddPartitionSpecUpdate,
    AddSnapshotUpdate,
    AddSortOrderUpdate,
    AssertCreate,
    AssertCurrentSchemaId,
    AssertDefaultSortOrderId,
    AssertDefaultSpecId,
    AssertLastAssignedFieldId,
    AssertLastAssignedPartitionId,
    AssertRefSnapshotId,
    AssertTableUUID,
    RemovePartitionSpecsUpdate,
    RemovePartitionStatisticsUpdate,
    RemovePropertiesUpdate,
    RemoveSchemasUpdate,
    RemoveSnapshotRefUpdate,
    RemoveSnapshotsUpdate,
    RemoveStatisticsUpdate,
    SetDefaultSortOrderUpdate,
    SetPartitionStatisticsUpdate,
    SetPropertiesUpdate,
    SetSnapshotRefUpdate,
    SetStatisticsUpdate,
    _apply_table_update,
    _TableMetadataUpdateContext,
    update_table_metadata,
)
from pyiceberg.table.update.schema import UpdateSchema
from pyiceberg.transforms import (
    BucketTransform,
    IdentityTransform,
)
from pyiceberg.types import (
    BinaryType,
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    FixedType,
    FloatType,
    IntegerType,
    ListType,
    LongType,
    MapType,
    NestedField,
    PrimitiveType,
    StringType,
    StructType,
    TimestampType,
    TimestamptzType,
    TimeType,
    UUIDType,
)


def test_schema(table_v2: Table) -> None:
    assert table_v2.schema() == Schema(
        NestedField(field_id=1, name="x", field_type=LongType(), required=True),
        NestedField(field_id=2, name="y", field_type=LongType(), required=True, doc="comment"),
        NestedField(field_id=3, name="z", field_type=LongType(), required=True),
        identifier_field_ids=[1, 2],
    )
    assert table_v2.schema().schema_id == 1


def test_schemas(table_v2: Table) -> None:
    assert table_v2.schemas() == {
        0: Schema(
            NestedField(field_id=1, name="x", field_type=LongType(), required=True),
            identifier_field_ids=[],
        ),
        1: Schema(
            NestedField(field_id=1, name="x", field_type=LongType(), required=True),
            NestedField(field_id=2, name="y", field_type=LongType(), required=True, doc="comment"),
            NestedField(field_id=3, name="z", field_type=LongType(), required=True),
            identifier_field_ids=[1, 2],
        ),
    }
    assert table_v2.schemas()[0].schema_id == 0
    assert table_v2.schemas()[1].schema_id == 1


def test_spec(table_v2: Table) -> None:
    assert table_v2.spec() == PartitionSpec(
        PartitionField(source_id=1, field_id=1000, transform=IdentityTransform(), name="x"), spec_id=0
    )


def test_specs(table_v2: Table) -> None:
    assert table_v2.specs() == {
        0: PartitionSpec(PartitionField(source_id=1, field_id=1000, transform=IdentityTransform(), name="x"), spec_id=0)
    }


def test_sort_order(table_v2: Table) -> None:
    assert table_v2.sort_order() == SortOrder(
        SortField(source_id=2, transform=IdentityTransform(), direction=SortDirection.ASC, null_order=NullOrder.NULLS_FIRST),
        SortField(
            source_id=3,
            transform=BucketTransform(num_buckets=4),
            direction=SortDirection.DESC,
            null_order=NullOrder.NULLS_LAST,
        ),
        order_id=3,
    )


def test_sort_orders(table_v2: Table) -> None:
    assert table_v2.sort_orders() == {
        3: SortOrder(
            SortField(source_id=2, transform=IdentityTransform(), direction=SortDirection.ASC, null_order=NullOrder.NULLS_FIRST),
            SortField(
                source_id=3,
                transform=BucketTransform(num_buckets=4),
                direction=SortDirection.DESC,
                null_order=NullOrder.NULLS_LAST,
            ),
            order_id=3,
        )
    }


def test_location(table_v2: Table) -> None:
    assert table_v2.location() == "s3://bucket/test/location"


def test_current_snapshot(table_v2: Table) -> None:
    assert table_v2.current_snapshot() == Snapshot(
        snapshot_id=3055729675574597004,
        parent_snapshot_id=3051729675574597004,
        sequence_number=1,
        timestamp_ms=1555100955770,
        manifest_list="s3://a/b/2.avro",
        summary=Summary(operation=Operation.APPEND),
        schema_id=1,
    )


def test_snapshot_by_id(table_v2: Table) -> None:
    assert table_v2.snapshot_by_id(3055729675574597004) == Snapshot(
        snapshot_id=3055729675574597004,
        parent_snapshot_id=3051729675574597004,
        sequence_number=1,
        timestamp_ms=1555100955770,
        manifest_list="s3://a/b/2.avro",
        summary=Summary(operation=Operation.APPEND),
        schema_id=1,
    )


def test_snapshot_by_timestamp(table_v2: Table) -> None:
    assert table_v2.snapshot_as_of_timestamp(1515100955770) == Snapshot(
        snapshot_id=3051729675574597004,
        parent_snapshot_id=None,
        sequence_number=0,
        timestamp_ms=1515100955770,
        manifest_list="s3://a/b/1.avro",
        summary=Summary(Operation.APPEND),
        schema_id=None,
    )
    assert table_v2.snapshot_as_of_timestamp(1515100955770, inclusive=False) is None


def test_snapshot_by_id_does_not_exist(table_v2: Table) -> None:
    assert table_v2.snapshot_by_id(-1) is None


def test_snapshot_by_name(table_v2: Table) -> None:
    assert table_v2.snapshot_by_name("test") == Snapshot(
        snapshot_id=3051729675574597004,
        parent_snapshot_id=None,
        sequence_number=0,
        timestamp_ms=1515100955770,
        manifest_list="s3://a/b/1.avro",
        summary=Summary(operation=Operation.APPEND),
        schema_id=None,
    )


def test_snapshot_by_name_does_not_exist(table_v2: Table) -> None:
    assert table_v2.snapshot_by_name("doesnotexist") is None


def test_repr(table_v2: Table) -> None:
    expected = """table(
  1: x: required long,
  2: y: required long (comment),
  3: z: required long
),
partition by: [x],
sort order: [2 ASC NULLS FIRST, bucket[4](3) DESC NULLS LAST],
snapshot: Operation.APPEND: id=3055729675574597004, parent_id=3051729675574597004, schema_id=1"""
    assert repr(table_v2) == expected


def test_history(table_v2: Table) -> None:
    assert table_v2.history() == [
        SnapshotLogEntry(snapshot_id=3051729675574597004, timestamp_ms=1515100955770),
        SnapshotLogEntry(snapshot_id=3055729675574597004, timestamp_ms=1555100955770),
    ]


@pytest.mark.parametrize(
    "table_fixture",
    [
        pytest.param(pytest.lazy_fixture("table_v2"), id="parquet"),
        pytest.param(pytest.lazy_fixture("table_v2_orc"), id="orc"),
    ],
)
def test_table_scan_select(table_fixture: Table) -> None:
    scan = table_fixture.scan()
    assert scan.selected_fields == ("*",)
    assert scan.select("a", "b").selected_fields == ("a", "b")
    assert scan.select("a", "c").select("a").selected_fields == ("a",)


def test_table_scan_row_filter(table_v2: Table) -> None:
    scan = table_v2.scan()
    assert scan.row_filter == AlwaysTrue()
    assert scan.filter(EqualTo("x", 10)).row_filter == EqualTo("x", 10)
    assert scan.filter(EqualTo("x", 10)).filter(In("y", (10, 11))).row_filter == And(EqualTo("x", 10), In("y", (10, 11)))


def test_table_scan_ref(table_v2: Table) -> None:
    scan = table_v2.scan()
    assert scan.use_ref("test").snapshot_id == 3051729675574597004


def test_table_scan_ref_does_not_exists(table_v2: Table) -> None:
    scan = table_v2.scan()

    with pytest.raises(ValueError) as exc_info:
        _ = scan.use_ref("boom")

    assert "Cannot scan unknown ref=boom" in str(exc_info.value)


def test_table_scan_projection_full_schema(table_v2: Table) -> None:
    scan = table_v2.scan()
    projection_schema = scan.select("x", "y", "z").projection()
    assert projection_schema == Schema(
        NestedField(field_id=1, name="x", field_type=LongType(), required=True),
        NestedField(field_id=2, name="y", field_type=LongType(), required=True, doc="comment"),
        NestedField(field_id=3, name="z", field_type=LongType(), required=True),
        identifier_field_ids=[1, 2],
    )
    assert projection_schema.schema_id == 1


def test_table_scan_projection_single_column(table_v2: Table) -> None:
    scan = table_v2.scan()
    projection_schema = scan.select("y").projection()
    assert projection_schema == Schema(
        NestedField(field_id=2, name="y", field_type=LongType(), required=True, doc="comment"),
        identifier_field_ids=[2],
    )
    assert projection_schema.schema_id == 1


def test_table_scan_projection_single_column_case_sensitive(table_v2: Table) -> None:
    scan = table_v2.scan()
    projection_schema = scan.with_case_sensitive(False).select("Y").projection()
    assert projection_schema == Schema(
        NestedField(field_id=2, name="y", field_type=LongType(), required=True, doc="comment"),
        identifier_field_ids=[2],
    )
    assert projection_schema.schema_id == 1


def test_table_scan_projection_unknown_column(table_v2: Table) -> None:
    scan = table_v2.scan()

    with pytest.raises(ValueError) as exc_info:
        _ = scan.select("a").projection()

    assert "Could not find column: 'a'" in str(exc_info.value)


def test_static_table_same_as_table(table_v2: Table, metadata_location: str) -> None:
    static_table = StaticTable.from_metadata(metadata_location)
    assert isinstance(static_table, Table)
    assert static_table.metadata == table_v2.metadata


def test_static_table_gz_same_as_table(table_v2: Table, metadata_location_gz: str) -> None:
    static_table = StaticTable.from_metadata(metadata_location_gz)
    assert isinstance(static_table, Table)
    assert static_table.metadata == table_v2.metadata


def test_static_table_version_hint_same_as_table(
    table_v2: Table,
    table_location_with_version_hint_full: str,
    table_location_with_version_hint_numeric: str,
    table_location_with_version_hint_non_numeric: str,
) -> None:
    for table_location in [
        table_location_with_version_hint_full,
        table_location_with_version_hint_numeric,
        table_location_with_version_hint_non_numeric,
    ]:
        static_table = StaticTable.from_metadata(table_location)
        assert isinstance(static_table, Table)
        assert static_table.metadata == table_v2.metadata


def test_static_table_io_does_not_exist(metadata_location: str) -> None:
    with pytest.raises(ValueError):
        StaticTable.from_metadata(metadata_location, {PY_IO_IMPL: "pyiceberg.does.not.exist.FileIO"})


def test_serialize_set_properties_updates() -> None:
    assert (
        SetPropertiesUpdate(updates={"abc": "🤪"}).model_dump_json() == """{"action":"set-properties","updates":{"abc":"🤪"}}"""
    )


def test_add_column(table_v2: Table) -> None:
    update = UpdateSchema(transaction=table_v2.transaction())
    update.add_column(path="b", field_type=IntegerType())
    apply_schema: Schema = update._apply()  # pylint: disable=W0212
    assert len(apply_schema.fields) == 4

    assert apply_schema == Schema(
        NestedField(field_id=1, name="x", field_type=LongType(), required=True),
        NestedField(field_id=2, name="y", field_type=LongType(), required=True, doc="comment"),
        NestedField(field_id=3, name="z", field_type=LongType(), required=True),
        NestedField(field_id=4, name="b", field_type=IntegerType(), required=False),
        identifier_field_ids=[1, 2],
    )
    assert apply_schema.schema_id == 2
    assert apply_schema.highest_field_id == 4


def test_update_column(table_v1: Table, table_v2: Table) -> None:
    """
    Table should be able to update existing property `doc`
    Table should also be able to update property `required`, if the field is not an identifier field.
    """
    COMMENT2 = "comment2"
    for table in [table_v1, table_v2]:
        original_schema = table.schema()
        # update existing doc to a new doc
        assert original_schema.find_field("y").doc == "comment"
        new_schema = table.transaction().update_schema().update_column("y", doc=COMMENT2)._apply()
        assert new_schema.find_field("y").doc == COMMENT2, "failed to update existing field doc"

        # update existing doc to an empty string
        assert new_schema.find_field("y").doc == COMMENT2
        new_schema2 = table.transaction().update_schema().update_column("y", doc="")._apply()
        assert new_schema2.find_field("y").doc == "", "failed to remove existing field doc"

        # update required to False
        assert original_schema.find_field("z").required is True
        new_schema3 = table.transaction().update_schema().update_column("z", required=False)._apply()
        assert new_schema3.find_field("z").required is False, "failed to update existing field required"

        # assert the above two updates also works with union_by_name
        assert table.update_schema().union_by_name(new_schema)._apply() == new_schema, (
            "failed to update existing field doc with union_by_name"
        )
        assert table.update_schema().union_by_name(new_schema2)._apply() == new_schema2, (
            "failed to remove existing field doc with union_by_name"
        )
        assert table.update_schema().union_by_name(new_schema3)._apply() == new_schema3, (
            "failed to update existing field required with union_by_name"
        )


def test_add_primitive_type_column(table_v2: Table) -> None:
    primitive_type: dict[str, PrimitiveType] = {
        "boolean": BooleanType(),
        "int": IntegerType(),
        "long": LongType(),
        "float": FloatType(),
        "double": DoubleType(),
        "date": DateType(),
        "time": TimeType(),
        "timestamp": TimestampType(),
        "timestamptz": TimestamptzType(),
        "string": StringType(),
        "uuid": UUIDType(),
        "binary": BinaryType(),
    }

    for name, type_ in primitive_type.items():
        field_name = f"new_column_{name}"
        update = UpdateSchema(transaction=table_v2.transaction())
        update.add_column(path=field_name, field_type=type_, doc=f"new_column_{name}")
        new_schema = update._apply()  # pylint: disable=W0212

        field: NestedField = new_schema.find_field(field_name)
        assert field.field_type == type_
        assert field.doc == f"new_column_{name}"


def test_add_nested_type_column(table_v2: Table) -> None:
    # add struct type column
    field_name = "new_column_struct"
    update = UpdateSchema(transaction=table_v2.transaction())
    struct_ = StructType(
        NestedField(1, "lat", DoubleType()),
        NestedField(2, "long", DoubleType()),
    )
    update.add_column(path=field_name, field_type=struct_)
    schema_ = update._apply()  # pylint: disable=W0212
    field: NestedField = schema_.find_field(field_name)
    assert field.field_type == StructType(
        NestedField(5, "lat", DoubleType()),
        NestedField(6, "long", DoubleType()),
    )
    assert schema_.highest_field_id == 6


def test_add_nested_map_type_column(table_v2: Table) -> None:
    # add map type column
    field_name = "new_column_map"
    update = UpdateSchema(transaction=table_v2.transaction())
    map_ = MapType(1, StringType(), 2, IntegerType(), False)
    update.add_column(path=field_name, field_type=map_)
    new_schema = update._apply()  # pylint: disable=W0212
    field: NestedField = new_schema.find_field(field_name)
    assert field.field_type == MapType(5, StringType(), 6, IntegerType(), False)
    assert new_schema.highest_field_id == 6


def test_add_nested_list_type_column(table_v2: Table) -> None:
    # add list type column
    field_name = "new_column_list"
    update = UpdateSchema(transaction=table_v2.transaction())
    list_ = ListType(
        element_id=101,
        element_type=StructType(
            NestedField(102, "lat", DoubleType()),
            NestedField(103, "long", DoubleType()),
        ),
        element_required=False,
    )
    update.add_column(path=field_name, field_type=list_)
    new_schema = update._apply()  # pylint: disable=W0212
    field: NestedField = new_schema.find_field(field_name)
    assert field.field_type == ListType(
        element_id=5,
        element_type=StructType(
            NestedField(6, "lat", DoubleType()),
            NestedField(7, "long", DoubleType()),
        ),
        element_required=False,
    )
    assert new_schema.highest_field_id == 7


def test_update_list_element_required(table_v2: Table) -> None:
    """Test that update_column can change list element's required property."""
    # Add a list column with optional elements
    update = UpdateSchema(transaction=table_v2.transaction())
    update.add_column(path="tags", field_type=ListType(element_id=1, element_type=StringType(), element_required=False))
    schema_with_list = update._apply()

    # Verify initial state
    field = schema_with_list.find_field("tags")
    assert isinstance(field.field_type, ListType)
    assert field.field_type.element_required is False

    # Update element to required
    update2 = UpdateSchema(transaction=table_v2.transaction(), allow_incompatible_changes=True, schema=schema_with_list)
    new_schema = update2.update_column(("tags", "element"), required=True)._apply()

    # Verify the update
    field = new_schema.find_field("tags")
    assert isinstance(field.field_type, ListType)
    assert field.field_type.element_required is True


def test_update_map_value_required(table_v2: Table) -> None:
    """Test that update_column can change map value's required property."""
    # Add a map column with optional values
    update = UpdateSchema(transaction=table_v2.transaction())
    update.add_column(
        path="metadata",
        field_type=MapType(key_id=1, key_type=StringType(), value_id=2, value_type=IntegerType(), value_required=False),
    )
    schema_with_map = update._apply()

    # Verify initial state
    field = schema_with_map.find_field("metadata")
    assert isinstance(field.field_type, MapType)
    assert field.field_type.value_required is False

    # Update value to required
    update2 = UpdateSchema(transaction=table_v2.transaction(), allow_incompatible_changes=True, schema=schema_with_map)
    new_schema = update2.update_column(("metadata", "value"), required=True)._apply()

    # Verify the update
    field = new_schema.find_field("metadata")
    assert isinstance(field.field_type, MapType)
    assert field.field_type.value_required is True


def test_update_list_element_required_to_optional(table_v2: Table) -> None:
    """Test that update_column can change list element from required to optional (safe direction)."""
    # Add a list column with required elements
    update = UpdateSchema(transaction=table_v2.transaction())
    update.add_column(path="tags", field_type=ListType(element_id=1, element_type=StringType(), element_required=True))
    schema_with_list = update._apply()

    # Verify initial state
    field = schema_with_list.find_field("tags")
    assert isinstance(field.field_type, ListType)
    assert field.field_type.element_required is True

    # Update element to optional - should work without allow_incompatible_changes
    update2 = UpdateSchema(transaction=table_v2.transaction(), schema=schema_with_list)
    new_schema = update2.update_column(("tags", "element"), required=False)._apply()

    # Verify the update
    field = new_schema.find_field("tags")
    assert isinstance(field.field_type, ListType)
    assert field.field_type.element_required is False


def test_update_list_element_required_fails_without_allow_incompatible(table_v2: Table) -> None:
    """Test that optional -> required fails without allow_incompatible_changes."""
    # Add a list column with optional elements
    update = UpdateSchema(transaction=table_v2.transaction())
    update.add_column(path="tags", field_type=ListType(element_id=1, element_type=StringType(), element_required=False))
    schema_with_list = update._apply()

    # Try to update element to required without allow_incompatible_changes - should fail
    update2 = UpdateSchema(transaction=table_v2.transaction(), schema=schema_with_list)
    with pytest.raises(ValueError, match="Cannot change column nullability"):
        update2.update_column(("tags", "element"), required=True)._apply()


def test_update_map_value_required_fails_without_allow_incompatible(table_v2: Table) -> None:
    """Test that optional -> required for map value fails without allow_incompatible_changes."""
    # Add a map column with optional values
    update = UpdateSchema(transaction=table_v2.transaction())
    update.add_column(
        path="metadata",
        field_type=MapType(key_id=1, key_type=StringType(), value_id=2, value_type=IntegerType(), value_required=False),
    )
    schema_with_map = update._apply()

    # Try to update value to required without allow_incompatible_changes - should fail
    update2 = UpdateSchema(transaction=table_v2.transaction(), schema=schema_with_map)
    with pytest.raises(ValueError, match="Cannot change column nullability"):
        update2.update_column(("metadata", "value"), required=True)._apply()


def test_update_map_key_fails(table_v2: Table) -> None:
    """Test that updating map keys is not allowed."""
    # Add a map column
    update = UpdateSchema(transaction=table_v2.transaction())
    update.add_column(
        path="metadata",
        field_type=MapType(key_id=1, key_type=StringType(), value_id=2, value_type=IntegerType(), value_required=False),
    )
    schema_with_map = update._apply()

    # Try to update the key - should fail even with allow_incompatible_changes
    update2 = UpdateSchema(transaction=table_v2.transaction(), allow_incompatible_changes=True, schema=schema_with_map)
    with pytest.raises(ValueError, match="Cannot update map keys"):
        update2.update_column(("metadata", "key"), required=False)._apply()


def test_update_map_value_required_to_optional(table_v2: Table) -> None:
    """Test that update_column can change map value from required to optional (safe direction)."""
    # Add a map column with required values
    update = UpdateSchema(transaction=table_v2.transaction())
    update.add_column(
        path="metadata",
        field_type=MapType(key_id=1, key_type=StringType(), value_id=2, value_type=IntegerType(), value_required=True),
    )
    schema_with_map = update._apply()

    # Verify initial state
    field = schema_with_map.find_field("metadata")
    assert isinstance(field.field_type, MapType)
    assert field.field_type.value_required is True

    # Update value to optional - should work without allow_incompatible_changes
    update2 = UpdateSchema(transaction=table_v2.transaction(), schema=schema_with_map)
    new_schema = update2.update_column(("metadata", "value"), required=False)._apply()

    # Verify the update
    field = new_schema.find_field("metadata")
    assert isinstance(field.field_type, MapType)
    assert field.field_type.value_required is False


def test_update_list_and_map_in_single_schema_change(table_v2: Table) -> None:
    """Test updating both list element and map value required properties in a single schema change."""
    # Add both a list and a map column with optional elements/values
    update = UpdateSchema(transaction=table_v2.transaction())
    update.add_column(path="tags", field_type=ListType(element_id=1, element_type=StringType(), element_required=False))
    update.add_column(
        path="metadata",
        field_type=MapType(key_id=1, key_type=StringType(), value_id=2, value_type=IntegerType(), value_required=False),
    )
    schema_with_both = update._apply()

    # Verify initial state
    list_field = schema_with_both.find_field("tags")
    assert isinstance(list_field.field_type, ListType)
    assert list_field.field_type.element_required is False

    map_field = schema_with_both.find_field("metadata")
    assert isinstance(map_field.field_type, MapType)
    assert map_field.field_type.value_required is False

    # Update both in a single schema change
    update2 = UpdateSchema(transaction=table_v2.transaction(), allow_incompatible_changes=True, schema=schema_with_both)
    update2.update_column(("tags", "element"), required=True)
    update2.update_column(("metadata", "value"), required=True)
    new_schema = update2._apply()

    # Verify both updates
    list_field = new_schema.find_field("tags")
    assert isinstance(list_field.field_type, ListType)
    assert list_field.field_type.element_required is True

    map_field = new_schema.find_field("metadata")
    assert isinstance(map_field.field_type, MapType)
    assert map_field.field_type.value_required is True


def test_update_nested_list_in_struct_required(table_v2: Table) -> None:
    """Test updating nested list element required property inside a struct."""

    # Add a struct column containing a list
    update = UpdateSchema(transaction=table_v2.transaction())
    struct_type = StructType(
        NestedField(
            field_id=1,
            name="coordinates",
            field_type=ListType(element_id=2, element_type=DoubleType(), element_required=False),
            required=False,
        )
    )
    update.add_column(path="location", field_type=struct_type)
    schema_with_nested = update._apply()

    # Verify initial state
    field = schema_with_nested.find_field("location")
    assert isinstance(field.field_type, StructType)
    nested_list = field.field_type.fields[0].field_type
    assert isinstance(nested_list, ListType)
    assert nested_list.element_required is False

    # Update nested list element to required
    update2 = UpdateSchema(transaction=table_v2.transaction(), allow_incompatible_changes=True, schema=schema_with_nested)
    new_schema = update2.update_column(("location", "coordinates", "element"), required=True)._apply()

    # Verify the update
    field = new_schema.find_field("location")
    nested_list = field.field_type.fields[0].field_type
    assert isinstance(nested_list, ListType)
    assert nested_list.element_required is True


def test_apply_set_properties_update(table_v2: Table) -> None:
    base_metadata = table_v2.metadata

    new_metadata_no_update = update_table_metadata(base_metadata, (SetPropertiesUpdate(updates={}),))
    assert new_metadata_no_update == base_metadata

    new_metadata = update_table_metadata(
        base_metadata, (SetPropertiesUpdate(updates={"read.split.target.size": "123", "test_a": "test_a", "test_b": "test_b"}),)
    )

    assert base_metadata.properties == {"read.split.target.size": "134217728"}
    assert new_metadata.properties == {"read.split.target.size": "123", "test_a": "test_a", "test_b": "test_b"}

    new_metadata_add_only = update_table_metadata(new_metadata, (SetPropertiesUpdate(updates={"test_c": "test_c"}),))

    assert new_metadata_add_only.properties == {
        "read.split.target.size": "123",
        "test_a": "test_a",
        "test_b": "test_b",
        "test_c": "test_c",
    }
    assert new_metadata_add_only.last_updated_ms > base_metadata.last_updated_ms


def test_apply_remove_properties_update(table_v2: Table) -> None:
    base_metadata = update_table_metadata(
        table_v2.metadata,
        (SetPropertiesUpdate(updates={"test_a": "test_a", "test_b": "test_b", "test_c": "test_c", "test_d": "test_d"}),),
    )

    new_metadata_no_removal = update_table_metadata(base_metadata, (RemovePropertiesUpdate(removals=[]),))

    assert base_metadata == new_metadata_no_removal

    new_metadata = update_table_metadata(base_metadata, (RemovePropertiesUpdate(removals=["test_a", "test_c"]),))

    assert base_metadata.properties == {
        "read.split.target.size": "134217728",
        "test_a": "test_a",
        "test_b": "test_b",
        "test_c": "test_c",
        "test_d": "test_d",
    }
    assert new_metadata.properties == {"read.split.target.size": "134217728", "test_b": "test_b", "test_d": "test_d"}


def test_apply_add_schema_update(table_v2: Table) -> None:
    transaction = table_v2.transaction()
    update = transaction.update_schema()
    update.add_column(path="b", field_type=IntegerType())
    update.commit()

    test_context = _TableMetadataUpdateContext()

    new_table_metadata = _apply_table_update(transaction._updates[0], base_metadata=table_v2.metadata, context=test_context)  # pylint: disable=W0212
    assert len(new_table_metadata.schemas) == 3
    assert new_table_metadata.current_schema_id == 1
    assert len(test_context._updates) == 1
    assert test_context._updates[0] == transaction._updates[0]  # pylint: disable=W0212
    assert test_context.is_added_schema(2)

    new_table_metadata = _apply_table_update(transaction._updates[1], base_metadata=new_table_metadata, context=test_context)  # pylint: disable=W0212
    assert len(new_table_metadata.schemas) == 3
    assert new_table_metadata.current_schema_id == 2
    assert len(test_context._updates) == 2
    assert test_context._updates[1] == transaction._updates[1]  # pylint: disable=W0212
    assert test_context.is_added_schema(2)


def test_update_metadata_table_schema(table_v2: Table) -> None:
    transaction = table_v2.transaction()
    update = transaction.update_schema()
    update.add_column(path="b", field_type=IntegerType())
    update.commit()
    new_metadata = update_table_metadata(table_v2.metadata, transaction._updates)  # pylint: disable=W0212
    apply_schema: Schema = next(schema for schema in new_metadata.schemas if schema.schema_id == 2)
    assert len(apply_schema.fields) == 4

    assert apply_schema == Schema(
        NestedField(field_id=1, name="x", field_type=LongType(), required=True),
        NestedField(field_id=2, name="y", field_type=LongType(), required=True, doc="comment"),
        NestedField(field_id=3, name="z", field_type=LongType(), required=True),
        NestedField(field_id=4, name="b", field_type=IntegerType(), required=False),
        identifier_field_ids=[1, 2],
    )
    assert apply_schema.schema_id == 2
    assert apply_schema.highest_field_id == 4

    assert new_metadata.current_schema_id == 2


def test_update_metadata_add_snapshot(table_v2: Table) -> None:
    new_snapshot = Snapshot(
        snapshot_id=25,
        parent_snapshot_id=19,
        sequence_number=200,
        timestamp_ms=1602638593590,
        manifest_list="s3:/a/b/c.avro",
        summary=Summary(Operation.APPEND),
        schema_id=3,
    )

    new_metadata = update_table_metadata(table_v2.metadata, (AddSnapshotUpdate(snapshot=new_snapshot),))
    assert len(new_metadata.snapshots) == 3
    assert new_metadata.snapshots[-1] == new_snapshot
    assert new_metadata.last_sequence_number == new_snapshot.sequence_number
    assert new_metadata.last_updated_ms == new_snapshot.timestamp_ms


def test_update_metadata_set_ref_snapshot(table_v2: Table) -> None:
    update, _ = table_v2.transaction()._set_ref_snapshot(
        snapshot_id=3051729675574597004,
        ref_name="main",
        type="branch",
        max_ref_age_ms=123123123,
        max_snapshot_age_ms=12312312312,
        min_snapshots_to_keep=1,
    )

    new_metadata = update_table_metadata(table_v2.metadata, update)
    assert len(new_metadata.snapshot_log) == 3
    assert new_metadata.snapshot_log[2].snapshot_id == 3051729675574597004
    assert new_metadata.current_snapshot_id == 3051729675574597004
    assert new_metadata.last_updated_ms > table_v2.metadata.last_updated_ms
    assert new_metadata.refs["main"] == SnapshotRef(
        snapshot_id=3051729675574597004,
        snapshot_ref_type="branch",
        min_snapshots_to_keep=1,
        max_snapshot_age_ms=12312312312,
        max_ref_age_ms=123123123,
    )


def test_update_metadata_set_snapshot_ref(table_v2: Table) -> None:
    update = SetSnapshotRefUpdate(
        ref_name="main",
        type="branch",
        snapshot_id=3051729675574597004,
        max_ref_age_ms=123123123,
        max_snapshot_age_ms=12312312312,
        min_snapshots_to_keep=1,
    )

    new_metadata = update_table_metadata(table_v2.metadata, (update,))
    assert len(new_metadata.snapshot_log) == 3
    assert new_metadata.snapshot_log[2].snapshot_id == 3051729675574597004
    assert new_metadata.current_snapshot_id == 3051729675574597004
    assert new_metadata.last_updated_ms > table_v2.metadata.last_updated_ms
    assert new_metadata.refs[update.ref_name] == SnapshotRef(
        snapshot_id=3051729675574597004,
        snapshot_ref_type="branch",
        min_snapshots_to_keep=1,
        max_snapshot_age_ms=12312312312,
        max_ref_age_ms=123123123,
    )


def test_update_remove_snapshots(table_v2: Table) -> None:
    REMOVE_SNAPSHOT = 3051729675574597004
    KEEP_SNAPSHOT = 3055729675574597004
    # assert fixture data to easily understand the test assumptions
    assert len(table_v2.metadata.snapshots) == 2
    assert len(table_v2.metadata.snapshot_log) == 2
    assert len(table_v2.metadata.refs) == 2
    update = RemoveSnapshotsUpdate(snapshot_ids=[REMOVE_SNAPSHOT])
    new_metadata = update_table_metadata(table_v2.metadata, (update,))
    assert len(new_metadata.snapshots) == 1
    assert new_metadata.snapshots[0].snapshot_id == KEEP_SNAPSHOT
    assert new_metadata.snapshots[0].parent_snapshot_id is None
    assert new_metadata.current_snapshot_id == KEEP_SNAPSHOT
    assert new_metadata.last_updated_ms > table_v2.metadata.last_updated_ms
    assert len(new_metadata.snapshot_log) == 1
    assert new_metadata.snapshot_log[0].snapshot_id == KEEP_SNAPSHOT
    assert len(new_metadata.refs) == 1
    assert new_metadata.refs["main"].snapshot_id == KEEP_SNAPSHOT


def test_update_remove_snapshots_doesnt_exist(table_v2: Table) -> None:
    update = RemoveSnapshotsUpdate(
        snapshot_ids=[123],
    )
    with pytest.raises(ValueError, match="Snapshot with snapshot id 123 does not exist"):
        update_table_metadata(table_v2.metadata, (update,))


def test_update_remove_snapshots_remove_current_snapshot_id(table_v2: Table) -> None:
    update = RemoveSnapshotsUpdate(snapshot_ids=[3055729675574597004])
    new_metadata = update_table_metadata(table_v2.metadata, (update,))
    assert len(new_metadata.refs) == 1
    assert new_metadata.refs["test"].snapshot_id == 3051729675574597004
    assert new_metadata.current_snapshot_id is None


def test_update_remove_snapshot_ref(table_v2: Table) -> None:
    # assert fixture data to easily understand the test assumptions
    assert len(table_v2.metadata.refs) == 2
    update = RemoveSnapshotRefUpdate(ref_name="test")
    new_metadata = update_table_metadata(table_v2.metadata, (update,))
    assert len(new_metadata.refs) == 1
    assert new_metadata.refs["main"].snapshot_id == 3055729675574597004


def test_update_metadata_add_update_sort_order(table_v2: Table) -> None:
    new_sort_order = SortOrder(order_id=table_v2.sort_order().order_id + 1)
    new_metadata = update_table_metadata(
        table_v2.metadata,
        (AddSortOrderUpdate(sort_order=new_sort_order), SetDefaultSortOrderUpdate(sort_order_id=-1)),
    )
    assert len(new_metadata.sort_orders) == 2
    assert new_metadata.sort_orders[-1] == new_sort_order
    assert new_metadata.default_sort_order_id == new_sort_order.order_id
    assert new_metadata.last_updated_ms > table_v2.metadata.last_updated_ms


def test_update_metadata_update_sort_order_invalid(table_v2: Table) -> None:
    with pytest.raises(ValueError, match="Cannot set current sort order to the last added one when no sort order has been added"):
        update_table_metadata(table_v2.metadata, (SetDefaultSortOrderUpdate(sort_order_id=-1),))

    invalid_order_id = 10
    with pytest.raises(ValueError, match=f"Sort order with id {invalid_order_id} does not exist"):
        update_table_metadata(table_v2.metadata, (SetDefaultSortOrderUpdate(sort_order_id=invalid_order_id),))


def test_update_metadata_with_multiple_updates(table_v1: Table) -> None:
    base_metadata = table_v1.metadata
    transaction = table_v1.transaction()
    transaction.upgrade_table_version(format_version=2)

    schema_update_1 = transaction.update_schema()
    schema_update_1.add_column(path="b", field_type=IntegerType())
    schema_update_1.commit()

    transaction.set_properties(owner="test", test_a="test_a", test_b="test_b", test_c="test_c")

    test_updates = transaction._updates  # pylint: disable=W0212

    new_snapshot = Snapshot(
        snapshot_id=25,
        parent_snapshot_id=19,
        sequence_number=200,
        timestamp_ms=1602638573590,
        manifest_list="s3:/a/b/c.avro",
        summary=Summary(Operation.APPEND),
        schema_id=3,
    )

    test_updates += (
        AddSnapshotUpdate(snapshot=new_snapshot),
        SetPropertiesUpdate(updates={"test_a": "test_a1"}),
        SetSnapshotRefUpdate(
            ref_name="main",
            type="branch",
            snapshot_id=25,
            max_ref_age_ms=123123123,
            max_snapshot_age_ms=12312312312,
            min_snapshots_to_keep=1,
        ),
        RemovePropertiesUpdate(removals=["test_c", "test_b"]),
    )

    new_metadata = update_table_metadata(base_metadata, test_updates)
    # rebuild the metadata to trigger validation
    new_metadata = TableMetadataUtil.parse_obj(copy(new_metadata.model_dump()))

    # UpgradeFormatVersionUpdate
    assert new_metadata.format_version == 2
    assert isinstance(new_metadata, TableMetadataV2)

    # UpdateSchema
    assert len(new_metadata.schemas) == 2
    assert new_metadata.current_schema_id == 1
    assert new_metadata.schema_by_id(new_metadata.current_schema_id).highest_field_id == 4  # type: ignore

    # AddSchemaUpdate
    assert len(new_metadata.snapshots) == 2
    assert new_metadata.snapshots[-1] == new_snapshot
    assert new_metadata.last_sequence_number == new_snapshot.sequence_number
    assert new_metadata.last_updated_ms == new_snapshot.timestamp_ms

    # SetSnapshotRefUpdate
    assert len(new_metadata.snapshot_log) == 1
    assert new_metadata.snapshot_log[0].snapshot_id == 25
    assert new_metadata.current_snapshot_id == 25
    assert new_metadata.last_updated_ms == 1602638573590
    assert new_metadata.refs["main"] == SnapshotRef(
        snapshot_id=25,
        snapshot_ref_type="branch",
        min_snapshots_to_keep=1,
        max_snapshot_age_ms=12312312312,
        max_ref_age_ms=123123123,
    )

    # Set/RemovePropertiesUpdate
    assert new_metadata.properties == {"owner": "test", "test_a": "test_a1"}


def test_update_metadata_schema_immutability(
    table_v2_with_fixed_and_decimal_types: TableMetadataV2,
) -> None:
    update = SetSnapshotRefUpdate(
        ref_name="main",
        type="branch",
        snapshot_id=3051729675574597004,
        max_ref_age_ms=123123123,
        max_snapshot_age_ms=12312312312,
        min_snapshots_to_keep=1,
    )

    new_metadata = update_table_metadata(
        table_v2_with_fixed_and_decimal_types.metadata,
        (update,),
    )

    assert new_metadata.schemas[0].fields == (
        NestedField(field_id=1, name="x", field_type=LongType(), required=True),
        NestedField(field_id=4, name="a", field_type=DecimalType(precision=16, scale=2), required=True),
        NestedField(field_id=5, name="b", field_type=DecimalType(precision=16, scale=8), required=True),
        NestedField(field_id=6, name="c", field_type=FixedType(length=16), required=True),
        NestedField(field_id=7, name="d", field_type=FixedType(length=18), required=True),
    )


def test_metadata_isolation_from_illegal_updates(table_v1: Table) -> None:
    base_metadata = table_v1.metadata
    base_metadata_backup = base_metadata.model_copy(deep=True)

    # Apply legal updates on the table metadata
    transaction = table_v1.transaction()
    schema_update_1 = transaction.update_schema()
    schema_update_1.add_column(path="b", field_type=IntegerType())
    schema_update_1.commit()
    test_updates = transaction._updates  # pylint: disable=W0212
    new_snapshot = Snapshot(
        snapshot_id=25,
        parent_snapshot_id=19,
        sequence_number=200,
        timestamp_ms=1602638573590,
        manifest_list="s3:/a/b/c.avro",
        summary=Summary(Operation.APPEND),
        schema_id=3,
    )
    test_updates += (
        AddSnapshotUpdate(snapshot=new_snapshot),
        SetSnapshotRefUpdate(
            ref_name="main",
            type="branch",
            snapshot_id=25,
            max_ref_age_ms=123123123,
            max_snapshot_age_ms=12312312312,
            min_snapshots_to_keep=1,
        ),
    )
    new_metadata = update_table_metadata(base_metadata, test_updates)

    # Check that the original metadata is not modified
    assert base_metadata == base_metadata_backup

    # Perform illegal update on the new metadata:
    # TableMetadata should be immutable, but the pydantic's frozen config cannot prevent
    # operations such as list append.
    new_metadata.partition_specs.append(PartitionSpec(spec_id=0))
    assert len(new_metadata.partition_specs) == 2

    # The original metadata should not be affected by the illegal update on the new metadata
    assert len(base_metadata.partition_specs) == 1


def test_generate_snapshot_id(table_v2: Table) -> None:
    assert isinstance(_generate_snapshot_id(), int)
    assert isinstance(table_v2.metadata.new_snapshot_id(), int)


def test_assert_create(table_v2: Table) -> None:
    AssertCreate().validate(None)

    with pytest.raises(CommitFailedException, match="Table already exists"):
        AssertCreate().validate(table_v2.metadata)


def test_assert_table_uuid(table_v2: Table) -> None:
    base_metadata = table_v2.metadata
    AssertTableUUID(uuid=base_metadata.table_uuid).validate(base_metadata)

    with pytest.raises(CommitFailedException, match="Requirement failed: current table metadata is missing"):
        AssertTableUUID(uuid=uuid.UUID("9c12d441-03fe-4693-9a96-a0705ddf69c2")).validate(None)

    with pytest.raises(
        CommitFailedException,
        match="Table UUID does not match: 9c12d441-03fe-4693-9a96-a0705ddf69c2 != 9c12d441-03fe-4693-9a96-a0705ddf69c1",
    ):
        AssertTableUUID(uuid=uuid.UUID("9c12d441-03fe-4693-9a96-a0705ddf69c2")).validate(base_metadata)


def test_assert_ref_snapshot_id(table_v2: Table) -> None:
    base_metadata = table_v2.metadata
    AssertRefSnapshotId(ref=MAIN_BRANCH, snapshot_id=base_metadata.current_snapshot_id).validate(base_metadata)

    with pytest.raises(CommitFailedException, match="Requirement failed: current table metadata is missing"):
        AssertRefSnapshotId(ref=MAIN_BRANCH, snapshot_id=1).validate(None)

    with pytest.raises(
        CommitFailedException,
        match=f"Requirement failed: branch {MAIN_BRANCH} was created concurrently",
    ):
        AssertRefSnapshotId(ref=MAIN_BRANCH, snapshot_id=None).validate(base_metadata)

    with pytest.raises(
        CommitFailedException,
        match=f"Requirement failed: branch {MAIN_BRANCH} has changed: expected id 1, found 3055729675574597004",
    ):
        AssertRefSnapshotId(ref=MAIN_BRANCH, snapshot_id=1).validate(base_metadata)

    non_existing_ref = "not_exist_branch_or_tag"
    assert table_v2.refs().get("not_exist_branch_or_tag") is None

    with pytest.raises(
        CommitFailedException,
        match=f"Requirement failed: branch or tag {non_existing_ref} is missing, expected 1",
    ):
        AssertRefSnapshotId(ref=non_existing_ref, snapshot_id=1).validate(base_metadata)

    # existing Tag in metadata: test
    ref_tag = table_v2.refs().get("test")
    assert ref_tag is not None
    assert ref_tag.snapshot_ref_type == SnapshotRefType.TAG, "TAG test should be present in table to be tested"

    with pytest.raises(
        CommitFailedException,
        match="Requirement failed: tag test has changed: expected id 3055729675574597004, found 3051729675574597004",
    ):
        AssertRefSnapshotId(ref="test", snapshot_id=3055729675574597004).validate(base_metadata)

    expected_json = '{"type":"assert-ref-snapshot-id","ref":"main","snapshot-id":null}'
    assert AssertRefSnapshotId(ref="main").model_dump_json() == expected_json


def test_assert_last_assigned_field_id(table_v2: Table) -> None:
    base_metadata = table_v2.metadata
    AssertLastAssignedFieldId(last_assigned_field_id=base_metadata.last_column_id).validate(base_metadata)

    with pytest.raises(CommitFailedException, match="Requirement failed: current table metadata is missing"):
        AssertLastAssignedFieldId(last_assigned_field_id=1).validate(None)

    with pytest.raises(
        CommitFailedException,
        match="Requirement failed: last assigned field id has changed: expected 1, found 3",
    ):
        AssertLastAssignedFieldId(last_assigned_field_id=1).validate(base_metadata)


def test_assert_current_schema_id(table_v2: Table) -> None:
    base_metadata = table_v2.metadata
    AssertCurrentSchemaId(current_schema_id=base_metadata.current_schema_id).validate(base_metadata)

    with pytest.raises(CommitFailedException, match="Requirement failed: current table metadata is missing"):
        AssertCurrentSchemaId(current_schema_id=1).validate(None)

    with pytest.raises(
        CommitFailedException,
        match="Requirement failed: current schema id has changed: expected 2, found 1",
    ):
        AssertCurrentSchemaId(current_schema_id=2).validate(base_metadata)


def test_last_assigned_partition_id(table_v2: Table) -> None:
    base_metadata = table_v2.metadata
    AssertLastAssignedPartitionId(last_assigned_partition_id=base_metadata.last_partition_id).validate(base_metadata)

    with pytest.raises(CommitFailedException, match="Requirement failed: current table metadata is missing"):
        AssertLastAssignedPartitionId(last_assigned_partition_id=1).validate(None)

    with pytest.raises(
        CommitFailedException,
        match="Requirement failed: last assigned partition id has changed: expected 1, found 1000",
    ):
        AssertLastAssignedPartitionId(last_assigned_partition_id=1).validate(base_metadata)


def test_assert_default_spec_id(table_v2: Table) -> None:
    base_metadata = table_v2.metadata
    AssertDefaultSpecId(default_spec_id=base_metadata.default_spec_id).validate(base_metadata)

    with pytest.raises(CommitFailedException, match="Requirement failed: current table metadata is missing"):
        AssertDefaultSpecId(default_spec_id=1).validate(None)

    with pytest.raises(
        CommitFailedException,
        match="Requirement failed: default spec id has changed: expected 1, found 0",
    ):
        AssertDefaultSpecId(default_spec_id=1).validate(base_metadata)


def test_assert_default_sort_order_id(table_v2: Table) -> None:
    base_metadata = table_v2.metadata
    AssertDefaultSortOrderId(default_sort_order_id=base_metadata.default_sort_order_id).validate(base_metadata)

    with pytest.raises(CommitFailedException, match="Requirement failed: current table metadata is missing"):
        AssertDefaultSortOrderId(default_sort_order_id=1).validate(None)

    with pytest.raises(
        CommitFailedException,
        match="Requirement failed: default sort order id has changed: expected 1, found 3",
    ):
        AssertDefaultSortOrderId(default_sort_order_id=1).validate(base_metadata)


def test_correct_schema() -> None:
    table_metadata = TableMetadataV2(
        **{
            "format-version": 2,
            "table-uuid": "9c12d441-03fe-4693-9a96-a0705ddf69c1",
            "location": "s3://bucket/test/location",
            "last-sequence-number": 34,
            "last-updated-ms": 1602638573590,
            "last-column-id": 3,
            "current-schema-id": 1,
            "schemas": [
                {"type": "struct", "schema-id": 0, "fields": [{"id": 1, "name": "x", "required": True, "type": "long"}]},
                {
                    "type": "struct",
                    "schema-id": 1,
                    "identifier-field-ids": [1, 2],
                    "fields": [
                        {"id": 1, "name": "x", "required": True, "type": "long"},
                        {"id": 2, "name": "y", "required": True, "type": "long"},
                        {"id": 3, "name": "z", "required": True, "type": "long"},
                    ],
                },
            ],
            "default-spec-id": 0,
            "partition-specs": [
                {"spec-id": 0, "fields": [{"name": "x", "transform": "identity", "source-id": 1, "field-id": 1000}]}
            ],
            "last-partition-id": 1000,
            "default-sort-order-id": 0,
            "sort-orders": [],
            "current-snapshot-id": 123,
            "snapshots": [
                {
                    "snapshot-id": 234,
                    "timestamp-ms": 1515100955770,
                    "sequence-number": 0,
                    "summary": {"operation": "append"},
                    "manifest-list": "s3://a/b/1.avro",
                    "schema-id": 10,
                },
                {
                    "snapshot-id": 123,
                    "timestamp-ms": 1515100955770,
                    "sequence-number": 0,
                    "summary": {"operation": "append"},
                    "manifest-list": "s3://a/b/1.avro",
                    "schema-id": 0,
                },
            ],
        }
    )

    t = Table(
        identifier=("default", "t1"),
        metadata=table_metadata,
        metadata_location="s3://../..",
        io=load_file_io(),
        catalog=NoopCatalog("NoopCatalog"),
    )

    # Should use the current schema, instead the one from the snapshot
    projection_schema = t.scan().projection()
    assert projection_schema == Schema(
        NestedField(field_id=1, name="x", field_type=LongType(), required=True),
        NestedField(field_id=2, name="y", field_type=LongType(), required=True),
        NestedField(field_id=3, name="z", field_type=LongType(), required=True),
        identifier_field_ids=[1, 2],
    )
    assert projection_schema.schema_id == 1

    # When we explicitly filter on the commit, we want to have the schema that's linked to the snapshot
    projection_schema = t.scan(snapshot_id=123).projection()
    assert projection_schema == Schema(
        NestedField(field_id=1, name="x", field_type=LongType(), required=True),
        identifier_field_ids=[],
    )
    assert projection_schema.schema_id == 0

    with pytest.warns(UserWarning, match="Metadata does not contain schema with id: 10"):
        t.scan(snapshot_id=234).projection()

    # Invalid snapshot
    with pytest.raises(ValueError) as exc_info:
        _ = t.scan(snapshot_id=-1).projection()

    assert "Snapshot not found: -1" in str(exc_info.value)


def test_table_properties(example_table_metadata_v2: dict[str, Any]) -> None:
    # metadata properties are all strings
    for k, v in example_table_metadata_v2["properties"].items():
        assert isinstance(k, str)
        assert isinstance(v, str)
    metadata = TableMetadataV2(**example_table_metadata_v2)
    for k, v in metadata.properties.items():
        assert isinstance(k, str)
        assert isinstance(v, str)

    # property can be set to int, but still serialized as string
    property_with_int = {"property_name": 42}
    new_example_table_metadata_v2 = {**example_table_metadata_v2, "properties": property_with_int}
    assert isinstance(new_example_table_metadata_v2["properties"]["property_name"], int)
    new_metadata = TableMetadataV2(**new_example_table_metadata_v2)
    assert isinstance(new_metadata.properties["property_name"], str)


def test_table_properties_raise_for_none_value(example_table_metadata_v2: dict[str, Any]) -> None:
    property_with_none = {"property_name": None}
    example_table_metadata_v2 = {**example_table_metadata_v2, "properties": property_with_none}
    with pytest.raises(ValidationError) as exc_info:
        TableMetadataV2(**example_table_metadata_v2)
    assert "None type is not a supported value in properties: property_name" in str(exc_info.value)


def test_serialize_commit_table_request() -> None:
    request = CommitTableRequest(
        requirements=(AssertTableUUID(uuid="4bfd18a3-74c6-478e-98b1-71c4c32f4163"),),
        identifier=TableIdentifier(namespace=["a"], name="b"),
    )

    deserialized_request = CommitTableRequest.model_validate_json(request.model_dump_json())
    assert request == deserialized_request


def test_update_metadata_log(table_v2: Table) -> None:
    new_snapshot = Snapshot(
        snapshot_id=25,
        parent_snapshot_id=19,
        sequence_number=200,
        timestamp_ms=1602638593590,
        manifest_list="s3:/a/b/c.avro",
        summary=Summary(Operation.APPEND),
        schema_id=3,
    )

    new_metadata = update_table_metadata(
        table_v2.metadata, (AddSnapshotUpdate(snapshot=new_snapshot),), False, table_v2.metadata_location
    )
    assert len(new_metadata.metadata_log) == 2


def test_update_metadata_log_overflow(table_v2: Table) -> None:
    metadata_log = [
        MetadataLogEntry(
            timestamp_ms=1602638593590 + i,
            metadata_file=f"/path/to/metadata/{i}.json",
        )
        for i in range(10)
    ]
    table_v2.metadata = table_v2.metadata.model_copy(update={"metadata_log": metadata_log, "last_updated_ms": 1602638593600})
    table_v2.metadata_location = "/path/to/metadata/10.json"
    assert len(table_v2.metadata.metadata_log) == 10

    base_metadata = table_v2.metadata
    new_metadata = update_table_metadata(
        base_metadata,
        (SetPropertiesUpdate(updates={"write.metadata.previous-versions-max": "5"}),),
        False,
        table_v2.metadata_location,
    )
    assert len(new_metadata.metadata_log) == 5
    assert new_metadata.metadata_log[-1].metadata_file == "/path/to/metadata/10.json"

    # check invalid value of write.metadata.previous-versions-max
    new_metadata = update_table_metadata(
        base_metadata,
        (SetPropertiesUpdate(updates={"write.metadata.previous-versions-max": "0"}),),
        False,
        table_v2.metadata_location,
    )
    assert len(new_metadata.metadata_log) == 1


def test_remove_partition_spec_update(table_v2: Table) -> None:
    base_metadata = table_v2.metadata
    new_spec = PartitionSpec(PartitionField(source_id=2, field_id=1001, transform=IdentityTransform(), name="y"), spec_id=1)
    metadata_with_new_spec = update_table_metadata(base_metadata, (AddPartitionSpecUpdate(spec=new_spec),))

    assert len(metadata_with_new_spec.partition_specs) == 2

    update = RemovePartitionSpecsUpdate(spec_ids=[1])
    updated_metadata = update_table_metadata(
        metadata_with_new_spec,
        (update,),
    )

    assert len(updated_metadata.partition_specs) == 1


def test_remove_partition_spec_update_spec_does_not_exist(table_v2: Table) -> None:
    update = RemovePartitionSpecsUpdate(
        spec_ids=[123],
    )
    with pytest.raises(ValueError, match="Partition spec with id 123 does not exist"):
        update_table_metadata(table_v2.metadata, (update,))


def test_remove_partition_spec_update_default_spec(table_v2: Table) -> None:
    update = RemovePartitionSpecsUpdate(
        spec_ids=[0],
    )
    with pytest.raises(ValueError, match="Cannot remove default partition spec: 0"):
        update_table_metadata(table_v2.metadata, (update,))


def test_remove_schemas_update(table_v2: Table) -> None:
    base_metadata = table_v2.metadata
    assert len(base_metadata.schemas) == 2

    update = RemoveSchemasUpdate(schema_ids=[0])
    updated_metadata = update_table_metadata(
        base_metadata,
        (update,),
    )

    assert len(updated_metadata.schemas) == 1


def test_remove_schemas_update_schema_does_not_exist(table_v2: Table) -> None:
    update = RemoveSchemasUpdate(
        schema_ids=[123],
    )
    with pytest.raises(ValueError, match="Schema with schema id 123 does not exist"):
        update_table_metadata(table_v2.metadata, (update,))


def test_remove_schemas_update_current_schema(table_v2: Table) -> None:
    update = RemoveSchemasUpdate(
        schema_ids=[1],
    )
    with pytest.raises(ValueError, match="Cannot remove current schema with id 1"):
        update_table_metadata(table_v2.metadata, (update,))


def test_set_statistics_update(table_v2_with_statistics: Table) -> None:
    snapshot_id = table_v2_with_statistics.metadata.current_snapshot_id

    blob_metadata = BlobMetadata(
        type="apache-datasketches-theta-v1",
        snapshot_id=snapshot_id,
        sequence_number=2,
        fields=[1],
        properties={"prop-key": "prop-value"},
    )

    statistics_file = StatisticsFile(
        snapshot_id=snapshot_id,
        statistics_path="s3://bucket/warehouse/stats.puffin",
        file_size_in_bytes=124,
        file_footer_size_in_bytes=27,
        blob_metadata=[blob_metadata],
    )

    update = SetStatisticsUpdate(
        snapshot_id=snapshot_id,
        statistics=statistics_file,
    )

    assert model_roundtrips(update)

    new_metadata = update_table_metadata(
        table_v2_with_statistics.metadata,
        (update,),
    )

    expected = """
    {
      "snapshot-id": 3055729675574597004,
      "statistics-path": "s3://bucket/warehouse/stats.puffin",
      "file-size-in-bytes": 124,
      "file-footer-size-in-bytes": 27,
      "blob-metadata": [
        {
          "type": "apache-datasketches-theta-v1",
          "snapshot-id": 3055729675574597004,
          "sequence-number": 2,
          "fields": [
            1
          ],
          "properties": {
            "prop-key": "prop-value"
          }
        }
      ]
    }"""

    assert len(new_metadata.statistics) == 2

    updated_statistics = [stat for stat in new_metadata.statistics if stat.snapshot_id == snapshot_id]

    assert len(updated_statistics) == 1
    assert json.loads(updated_statistics[0].model_dump_json()) == json.loads(expected)


def test_set_statistics_update_handles_deprecated_snapshot_id(table_v2_with_statistics: Table) -> None:
    snapshot_id = table_v2_with_statistics.metadata.current_snapshot_id

    blob_metadata = BlobMetadata(
        type="apache-datasketches-theta-v1",
        snapshot_id=snapshot_id,
        sequence_number=2,
        fields=[1],
        properties={"prop-key": "prop-value"},
    )

    statistics_file = StatisticsFile(
        snapshot_id=snapshot_id,
        statistics_path="s3://bucket/warehouse/stats.puffin",
        file_size_in_bytes=124,
        file_footer_size_in_bytes=27,
        blob_metadata=[blob_metadata],
    )
    update_with_model = SetStatisticsUpdate(statistics=statistics_file)
    assert model_roundtrips(update_with_model)
    assert update_with_model.snapshot_id == snapshot_id

    update_with_dict = SetStatisticsUpdate.model_validate({"statistics": statistics_file.model_dump()})
    assert model_roundtrips(update_with_dict)
    assert update_with_dict.snapshot_id == snapshot_id

    update_json = """
        {
            "statistics":
                 {
                     "snapshot-id": 3055729675574597004,
                     "statistics-path": "s3://a/b/stats.puffin",
                     "file-size-in-bytes": 413,
                     "file-footer-size-in-bytes": 42,
                     "blob-metadata": [
                         {
                             "type": "apache-datasketches-theta-v1",
                             "snapshot-id": 3055729675574597004,
                             "sequence-number": 1,
                             "fields": [1]
                         }
                     ]
                 }
        }
    """

    update_with_json = SetStatisticsUpdate.model_validate_json(update_json)
    assert model_roundtrips(update_with_json)
    assert update_with_json.snapshot_id == snapshot_id


def test_remove_statistics_update(table_v2_with_statistics: Table) -> None:
    update = RemoveStatisticsUpdate(
        snapshot_id=3055729675574597004,
    )

    remove_metadata = update_table_metadata(
        table_v2_with_statistics.metadata,
        (update,),
    )

    assert len(remove_metadata.statistics) == 1

    with pytest.raises(
        ValueError,
        match="Statistics with snapshot id 123456789 does not exist",
    ):
        update_table_metadata(
            table_v2_with_statistics.metadata,
            (RemoveStatisticsUpdate(snapshot_id=123456789),),
        )


def test_set_partition_statistics_update(table_v2_with_statistics: Table) -> None:
    snapshot_id = table_v2_with_statistics.metadata.current_snapshot_id

    partition_statistics_file = PartitionStatisticsFile(
        snapshot_id=snapshot_id,
        statistics_path="s3://bucket/warehouse/stats.puffin",
        file_size_in_bytes=124,
    )

    update = SetPartitionStatisticsUpdate(
        partition_statistics=partition_statistics_file,
    )

    new_metadata = update_table_metadata(
        table_v2_with_statistics.metadata,
        (update,),
    )

    expected = """
    {
      "snapshot-id": 3055729675574597004,
      "statistics-path": "s3://bucket/warehouse/stats.puffin",
      "file-size-in-bytes": 124
    }"""

    assert len(new_metadata.partition_statistics) == 1

    updated_statistics = [stat for stat in new_metadata.partition_statistics if stat.snapshot_id == snapshot_id]

    assert len(updated_statistics) == 1
    assert json.loads(updated_statistics[0].model_dump_json()) == json.loads(expected)


def test_remove_partition_statistics_update(table_v2_with_statistics: Table) -> None:
    # Add partition statistics file.
    snapshot_id = table_v2_with_statistics.metadata.current_snapshot_id

    partition_statistics_file = PartitionStatisticsFile(
        snapshot_id=snapshot_id,
        statistics_path="s3://bucket/warehouse/stats.puffin",
        file_size_in_bytes=124,
    )

    update = SetPartitionStatisticsUpdate(
        partition_statistics=partition_statistics_file,
    )

    new_metadata = update_table_metadata(
        table_v2_with_statistics.metadata,
        (update,),
    )
    assert len(new_metadata.partition_statistics) == 1

    # Remove the same partition statistics file.
    remove_update = RemovePartitionStatisticsUpdate(snapshot_id=snapshot_id)

    remove_metadata = update_table_metadata(
        new_metadata,
        (remove_update,),
    )

    assert len(remove_metadata.partition_statistics) == 0


def test_remove_partition_statistics_update_with_invalid_snapshot_id(table_v2_with_statistics: Table) -> None:
    # Remove the same partition statistics file.
    with pytest.raises(
        ValueError,
        match="Partition Statistics with snapshot id 123456789 does not exist",
    ):
        update_table_metadata(
            table_v2_with_statistics.metadata,
            (RemovePartitionStatisticsUpdate(snapshot_id=123456789),),
        )


def test_add_snapshot_update_fails_without_first_row_id(table_v3: Table) -> None:
    new_snapshot = Snapshot(
        snapshot_id=25,
        parent_snapshot_id=19,
        sequence_number=200,
        timestamp_ms=1602638593590,
        manifest_list="s3:/a/b/c.avro",
        summary=Summary(Operation.APPEND),
        schema_id=3,
    )

    with pytest.raises(
        ValueError,
        match="Cannot add snapshot without first row id",
    ):
        update_table_metadata(table_v3.metadata, (AddSnapshotUpdate(snapshot=new_snapshot),))


def test_add_snapshot_update_fails_with_smaller_first_row_id(table_v3: Table) -> None:
    new_snapshot = Snapshot(
        snapshot_id=25,
        parent_snapshot_id=19,
        sequence_number=200,
        timestamp_ms=1602638593590,
        manifest_list="s3:/a/b/c.avro",
        summary=Summary(Operation.APPEND),
        schema_id=3,
        first_row_id=0,
    )

    with pytest.raises(
        ValueError,
        match="Cannot add a snapshot with first row id smaller than the table's next-row-id",
    ):
        update_table_metadata(table_v3.metadata, (AddSnapshotUpdate(snapshot=new_snapshot),))


def test_add_snapshot_update_updates_next_row_id(table_v3: Table) -> None:
    new_snapshot = Snapshot(
        snapshot_id=25,
        parent_snapshot_id=19,
        sequence_number=200,
        timestamp_ms=1602638593590,
        manifest_list="s3:/a/b/c.avro",
        summary=Summary(Operation.APPEND),
        schema_id=3,
        first_row_id=2,
        added_rows=10,
    )

    new_metadata = update_table_metadata(table_v3.metadata, (AddSnapshotUpdate(snapshot=new_snapshot),))
    assert new_metadata.next_row_id == 11


def model_roundtrips(model: BaseModel) -> bool:
    """Helper assertion that tests if a pydantic model roundtrips
    successfully.
    """
    __tracebackhide__ = True
    model_data = model.model_dump()
    if model != type(model).model_validate(model_data):
        pytest.fail(f"model {type(model)} did not roundtrip successfully")
    return True


def test_check_uuid_raises_when_mismatch(table_v2: Table, example_table_metadata_v2: dict[str, Any]) -> None:
    different_uuid = "550e8400-e29b-41d4-a716-446655440000"
    metadata_with_different_uuid = {**example_table_metadata_v2, "table-uuid": different_uuid}
    new_metadata = TableMetadataV2(**metadata_with_different_uuid)

    with pytest.raises(ValueError) as exc_info:
        Table._check_uuid(table_v2.metadata, new_metadata)

    assert "Table UUID does not match" in str(exc_info.value)
    assert different_uuid in str(exc_info.value)


def test_check_uuid_passes_when_match(table_v2: Table, example_table_metadata_v2: dict[str, Any]) -> None:
    new_metadata = TableMetadataV2(**example_table_metadata_v2)
    # Should not raise with same uuid
    Table._check_uuid(table_v2.metadata, new_metadata)
