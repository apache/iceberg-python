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
from typing import Dict

import pytest
from sortedcontainers import SortedList

from pyiceberg.expressions import (
    AlwaysTrue,
    And,
    EqualTo,
    In,
)
from pyiceberg.io import PY_IO_IMPL
from pyiceberg.manifest import (
    DataFile,
    DataFileContent,
    FileFormat,
    ManifestEntry,
    ManifestEntryStatus,
)
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table import (
    AddSnapshotUpdate,
    SetPropertiesUpdate,
    SetSnapshotRefUpdate,
    SnapshotRef,
    StaticTable,
    Table,
    TableMetadataUpdateContext,
    UpdateSchema,
    _generate_snapshot_id,
    _match_deletes_to_datafile,
    apply_table_update,
    update_table_metadata,
)
from pyiceberg.table.metadata import INITIAL_SEQUENCE_NUMBER
from pyiceberg.table.snapshots import (
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
from pyiceberg.transforms import BucketTransform, IdentityTransform
from pyiceberg.types import (
    BinaryType,
    BooleanType,
    DateType,
    DoubleType,
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
        schema_id=1,
        identifier_field_ids=[1, 2],
    )


def test_schemas(table_v2: Table) -> None:
    assert table_v2.schemas() == {
        0: Schema(
            NestedField(field_id=1, name="x", field_type=LongType(), required=True),
            schema_id=0,
            identifier_field_ids=[],
        ),
        1: Schema(
            NestedField(field_id=1, name="x", field_type=LongType(), required=True),
            NestedField(field_id=2, name="y", field_type=LongType(), required=True, doc="comment"),
            NestedField(field_id=3, name="z", field_type=LongType(), required=True),
            schema_id=1,
            identifier_field_ids=[1, 2],
        ),
    }


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


def test_table_scan_select(table_v2: Table) -> None:
    scan = table_v2.scan()
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
    assert scan.select("x", "y", "z").projection() == Schema(
        NestedField(field_id=1, name="x", field_type=LongType(), required=True),
        NestedField(field_id=2, name="y", field_type=LongType(), required=True, doc="comment"),
        NestedField(field_id=3, name="z", field_type=LongType(), required=True),
        schema_id=1,
        identifier_field_ids=[1, 2],
    )


def test_table_scan_projection_single_column(table_v2: Table) -> None:
    scan = table_v2.scan()
    assert scan.select("y").projection() == Schema(
        NestedField(field_id=2, name="y", field_type=LongType(), required=True, doc="comment"),
        schema_id=1,
        identifier_field_ids=[2],
    )


def test_table_scan_projection_single_column_case_sensitive(table_v2: Table) -> None:
    scan = table_v2.scan()
    assert scan.with_case_sensitive(False).select("Y").projection() == Schema(
        NestedField(field_id=2, name="y", field_type=LongType(), required=True, doc="comment"),
        schema_id=1,
        identifier_field_ids=[2],
    )


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


def test_static_table_io_does_not_exist(metadata_location: str) -> None:
    with pytest.raises(ValueError):
        StaticTable.from_metadata(metadata_location, {PY_IO_IMPL: "pyiceberg.does.not.exist.FileIO"})


def test_match_deletes_to_datafile() -> None:
    data_entry = ManifestEntry(
        status=ManifestEntryStatus.ADDED,
        sequence_number=1,
        data_file=DataFile(
            content=DataFileContent.DATA,
            file_path="s3://bucket/0000.parquet",
            file_format=FileFormat.PARQUET,
            partition={},
            record_count=3,
            file_size_in_bytes=3,
        ),
    )
    delete_entry_1 = ManifestEntry(
        status=ManifestEntryStatus.ADDED,
        sequence_number=0,  # Older than the data
        data_file=DataFile(
            content=DataFileContent.POSITION_DELETES,
            file_path="s3://bucket/0001-delete.parquet",
            file_format=FileFormat.PARQUET,
            partition={},
            record_count=3,
            file_size_in_bytes=3,
        ),
    )
    delete_entry_2 = ManifestEntry(
        status=ManifestEntryStatus.ADDED,
        sequence_number=3,
        data_file=DataFile(
            content=DataFileContent.POSITION_DELETES,
            file_path="s3://bucket/0002-delete.parquet",
            file_format=FileFormat.PARQUET,
            partition={},
            record_count=3,
            file_size_in_bytes=3,
            # We don't really care about the tests here
            value_counts={},
            null_value_counts={},
            nan_value_counts={},
            lower_bounds={},
            upper_bounds={},
        ),
    )
    assert _match_deletes_to_datafile(
        data_entry,
        SortedList(iterable=[delete_entry_1, delete_entry_2], key=lambda entry: entry.sequence_number or INITIAL_SEQUENCE_NUMBER),
    ) == {
        delete_entry_2.data_file,
    }


def test_match_deletes_to_datafile_duplicate_number() -> None:
    data_entry = ManifestEntry(
        status=ManifestEntryStatus.ADDED,
        sequence_number=1,
        data_file=DataFile(
            content=DataFileContent.DATA,
            file_path="s3://bucket/0000.parquet",
            file_format=FileFormat.PARQUET,
            partition={},
            record_count=3,
            file_size_in_bytes=3,
        ),
    )
    delete_entry_1 = ManifestEntry(
        status=ManifestEntryStatus.ADDED,
        sequence_number=3,
        data_file=DataFile(
            content=DataFileContent.POSITION_DELETES,
            file_path="s3://bucket/0001-delete.parquet",
            file_format=FileFormat.PARQUET,
            partition={},
            record_count=3,
            file_size_in_bytes=3,
            # We don't really care about the tests here
            value_counts={},
            null_value_counts={},
            nan_value_counts={},
            lower_bounds={},
            upper_bounds={},
        ),
    )
    delete_entry_2 = ManifestEntry(
        status=ManifestEntryStatus.ADDED,
        sequence_number=3,
        data_file=DataFile(
            content=DataFileContent.POSITION_DELETES,
            file_path="s3://bucket/0002-delete.parquet",
            file_format=FileFormat.PARQUET,
            partition={},
            record_count=3,
            file_size_in_bytes=3,
            # We don't really care about the tests here
            value_counts={},
            null_value_counts={},
            nan_value_counts={},
            lower_bounds={},
            upper_bounds={},
        ),
    )
    assert _match_deletes_to_datafile(
        data_entry,
        SortedList(iterable=[delete_entry_1, delete_entry_2], key=lambda entry: entry.sequence_number or INITIAL_SEQUENCE_NUMBER),
    ) == {
        delete_entry_1.data_file,
        delete_entry_2.data_file,
    }


def test_serialize_set_properties_updates() -> None:
    assert SetPropertiesUpdate(updates={"abc": "🤪"}).model_dump_json() == """{"action":"set-properties","updates":{"abc":"🤪"}}"""


def test_add_column(table_v2: Table) -> None:
    update = UpdateSchema(table_v2)
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


def test_add_primitive_type_column(table_v2: Table) -> None:
    primitive_type: Dict[str, PrimitiveType] = {
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
        update = UpdateSchema(table_v2)
        update.add_column(path=field_name, field_type=type_, doc=f"new_column_{name}")
        new_schema = update._apply()  # pylint: disable=W0212

        field: NestedField = new_schema.find_field(field_name)
        assert field.field_type == type_
        assert field.doc == f"new_column_{name}"


def test_add_nested_type_column(table_v2: Table) -> None:
    # add struct type column
    field_name = "new_column_struct"
    update = UpdateSchema(table_v2)
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
    update = UpdateSchema(table_v2)
    map_ = MapType(1, StringType(), 2, IntegerType(), False)
    update.add_column(path=field_name, field_type=map_)
    new_schema = update._apply()  # pylint: disable=W0212
    field: NestedField = new_schema.find_field(field_name)
    assert field.field_type == MapType(5, StringType(), 6, IntegerType(), False)
    assert new_schema.highest_field_id == 6


def test_add_nested_list_type_column(table_v2: Table) -> None:
    # add list type column
    field_name = "new_column_list"
    update = UpdateSchema(table_v2)
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


def test_apply_add_schema_update(table_v2: Table) -> None:
    transaction = table_v2.transaction()
    update = transaction.update_schema()
    update.add_column(path="b", field_type=IntegerType())
    update.commit()

    test_context = TableMetadataUpdateContext()

    new_table_metadata = apply_table_update(
        transaction._updates[0], base_metadata=table_v2.metadata, context=test_context
    )  # pylint: disable=W0212
    assert len(new_table_metadata.schemas) == 3
    assert new_table_metadata.current_schema_id == 1
    assert len(test_context.updates) == 1
    assert test_context.updates[0] == transaction._updates[0]  # pylint: disable=W0212
    assert test_context.last_added_schema_id == 2

    new_table_metadata = apply_table_update(
        transaction._updates[1], base_metadata=new_table_metadata, context=test_context
    )  # pylint: disable=W0212
    assert len(new_table_metadata.schemas) == 3
    assert new_table_metadata.current_schema_id == 2
    assert len(test_context.updates) == 2
    assert test_context.updates[1] == transaction._updates[1]  # pylint: disable=W0212
    assert test_context.last_added_schema_id == 2


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
        timestamp_ms=1602638573590,
        manifest_list="s3:/a/b/c.avro",
        summary=Summary(Operation.APPEND),
        schema_id=3,
    )

    new_metadata = update_table_metadata(table_v2.metadata, (AddSnapshotUpdate(snapshot=new_snapshot),))
    assert len(new_metadata.snapshots) == 3
    assert new_metadata.snapshots[-1] == new_snapshot
    assert new_metadata.last_sequence_number == new_snapshot.sequence_number
    assert new_metadata.last_updated_ms == new_snapshot.timestamp_ms


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


def test_update_metadata_with_multiple_updates(table_v1: Table) -> None:
    base_metadata = table_v1.metadata
    transaction = table_v1.transaction()
    transaction.upgrade_table_version(format_version=2)

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

    # UpgradeFormatVersionUpdate
    assert new_metadata.format_version == 2

    # UpdateSchema
    assert len(new_metadata.schemas) == 2
    assert new_metadata.current_schema_id == 1
    assert new_metadata.schemas_by_id[new_metadata.current_schema_id].highest_field_id == 4

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


def test_generate_snapshot_id(table_v2: Table) -> None:
    assert isinstance(_generate_snapshot_id(), int)
    assert isinstance(table_v2.new_snapshot_id(), int)
