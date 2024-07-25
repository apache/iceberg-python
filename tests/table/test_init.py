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
import uuid
from copy import copy
from typing import Any, Dict

import pytest
from pydantic import ValidationError
from sortedcontainers import SortedList

from pyiceberg.catalog.noop import NoopCatalog
from pyiceberg.exceptions import CommitFailedException
from pyiceberg.expressions import (
    AlwaysTrue,
    And,
    EqualTo,
    In,
)
from pyiceberg.io import PY_IO_IMPL, load_file_io
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
    AddSortOrderUpdate,
    AssertCreate,
    AssertCurrentSchemaId,
    AssertDefaultSortOrderId,
    AssertDefaultSpecId,
    AssertLastAssignedFieldId,
    AssertLastAssignedPartitionId,
    AssertRefSnapshotId,
    AssertTableUUID,
    CommitTableRequest,
    RemovePropertiesUpdate,
    SetDefaultSortOrderUpdate,
    SetPropertiesUpdate,
    SetSnapshotRefUpdate,
    StaticTable,
    Table,
    TableIdentifier,
    UpdateSchema,
    _apply_table_update,
    _match_deletes_to_data_file,
    _TableMetadataUpdateContext,
    update_table_metadata,
)
from pyiceberg.table.metadata import INITIAL_SEQUENCE_NUMBER, TableMetadataUtil, TableMetadataV2, _generate_snapshot_id
from pyiceberg.table.refs import SnapshotRef
from pyiceberg.table.snapshots import (
    Operation,
    Snapshot,
    SnapshotLogEntry,
    Summary,
    ancestors_of,
)
from pyiceberg.table.sorting import (
    NullOrder,
    SortDirection,
    SortField,
    SortOrder,
)
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


def test_ancestors_of(table_v2: Table) -> None:
    assert list(ancestors_of(table_v2.current_snapshot(), table_v2.metadata)) == [
        Snapshot(
            snapshot_id=3055729675574597004,
            parent_snapshot_id=3051729675574597004,
            sequence_number=1,
            timestamp_ms=1555100955770,
            manifest_list="s3://a/b/2.avro",
            summary=Summary(Operation.APPEND),
            schema_id=1,
        ),
        Snapshot(
            snapshot_id=3051729675574597004,
            parent_snapshot_id=None,
            sequence_number=0,
            timestamp_ms=1515100955770,
            manifest_list="s3://a/b/1.avro",
            summary=Summary(Operation.APPEND),
            schema_id=None,
        ),
    ]


def test_ancestors_of_recursive_error(table_v2_with_extensive_snapshots: Table) -> None:
    # Test RecursionError: maximum recursion depth exceeded
    assert (
        len(
            list(
                ancestors_of(
                    table_v2_with_extensive_snapshots.current_snapshot(),
                    table_v2_with_extensive_snapshots.metadata,
                )
            )
        )
        == 2000
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
    assert _match_deletes_to_data_file(
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
    assert _match_deletes_to_data_file(
        data_entry,
        SortedList(iterable=[delete_entry_1, delete_entry_2], key=lambda entry: entry.sequence_number or INITIAL_SEQUENCE_NUMBER),
    ) == {
        delete_entry_1.data_file,
        delete_entry_2.data_file,
    }


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
    AssertRefSnapshotId(ref="main", snapshot_id=base_metadata.current_snapshot_id).validate(base_metadata)

    with pytest.raises(CommitFailedException, match="Requirement failed: current table metadata is missing"):
        AssertRefSnapshotId(ref="main", snapshot_id=1).validate(None)

    with pytest.raises(
        CommitFailedException,
        match="Requirement failed: branch main was created concurrently",
    ):
        AssertRefSnapshotId(ref="main", snapshot_id=None).validate(base_metadata)

    with pytest.raises(
        CommitFailedException,
        match="Requirement failed: branch main has changed: expected id 1, found 3055729675574597004",
    ):
        AssertRefSnapshotId(ref="main", snapshot_id=1).validate(base_metadata)

    with pytest.raises(
        CommitFailedException,
        match="Requirement failed: branch or tag not_exist is missing, expected 1",
    ):
        AssertRefSnapshotId(ref="not_exist", snapshot_id=1).validate(base_metadata)


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
    table_metadata = TableMetadataV2(**{
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
        "partition-specs": [{"spec-id": 0, "fields": [{"name": "x", "transform": "identity", "source-id": 1, "field-id": 1000}]}],
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
    })

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


def test_table_properties(example_table_metadata_v2: Dict[str, Any]) -> None:
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


def test_table_properties_raise_for_none_value(example_table_metadata_v2: Dict[str, Any]) -> None:
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
