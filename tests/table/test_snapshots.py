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
# pylint:disable=redefined-outer-name,eval-used
from typing import cast

import pytest

from pyiceberg.manifest import DataFile, DataFileContent, ManifestContent, ManifestFile
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.table.snapshots import (
    Operation,
    Snapshot,
    SnapshotSummaryCollector,
    Summary,
    ancestors_between,
    ancestors_of,
    latest_ancestor_before_timestamp,
    update_snapshot_summaries,
)
from pyiceberg.transforms import IdentityTransform
from pyiceberg.typedef import Record
from pyiceberg.types import (
    BooleanType,
    IntegerType,
    NestedField,
    StringType,
)


@pytest.fixture
def snapshot() -> Snapshot:
    return Snapshot(
        snapshot_id=25,
        parent_snapshot_id=19,
        sequence_number=200,
        timestamp_ms=1602638573590,
        manifest_list="s3:/a/b/c.avro",
        summary=Summary(Operation.APPEND),
        schema_id=3,
    )


@pytest.fixture
def snapshot_with_properties() -> Snapshot:
    return Snapshot(
        snapshot_id=25,
        parent_snapshot_id=19,
        sequence_number=200,
        timestamp_ms=1602638573590,
        manifest_list="s3:/a/b/c.avro",
        summary=Summary(Operation.APPEND, foo="bar"),
        schema_id=3,
    )


def test_serialize_summary() -> None:
    assert Summary(Operation.APPEND).model_dump_json() == """{"operation":"append"}"""


def test_serialize_summary_with_properties() -> None:
    summary = Summary(Operation.APPEND, property="yes")
    assert summary.model_dump_json() == """{"operation":"append","property":"yes"}"""


def test_serialize_snapshot(snapshot: Snapshot) -> None:
    assert snapshot.model_dump_json() == (
        '{"snapshot-id":25,"parent-snapshot-id":19,"sequence-number":200,"timestamp-ms":1602638573590,'
        '"manifest-list":"s3:/a/b/c.avro","summary":{"operation":"append"},"schema-id":3}'
    )


def test_serialize_snapshot_without_sequence_number() -> None:
    snapshot = Snapshot(
        snapshot_id=25,
        parent_snapshot_id=19,
        sequence_number=None,
        timestamp_ms=1602638573590,
        manifest_list="s3:/a/b/c.avro",
        summary=Summary(Operation.APPEND),
        schema_id=3,
    )
    actual = snapshot.model_dump_json()
    expected = (
        '{"snapshot-id":25,"parent-snapshot-id":19,"timestamp-ms":1602638573590,'
        '"manifest-list":"s3:/a/b/c.avro","summary":{"operation":"append"},"schema-id":3}'
    )
    assert actual == expected


def test_serialize_snapshot_with_properties(snapshot_with_properties: Snapshot) -> None:
    assert snapshot_with_properties.model_dump_json() == (
        '{"snapshot-id":25,"parent-snapshot-id":19,"sequence-number":200,"timestamp-ms":1602638573590,'
        '"manifest-list":"s3:/a/b/c.avro","summary":{"operation":"append","foo":"bar"},"schema-id":3}'
    )


def test_deserialize_summary() -> None:
    summary = Summary.model_validate_json("""{"operation": "append"}""")
    assert summary.operation == Operation.APPEND


def test_deserialize_summary_with_properties() -> None:
    summary = Summary.model_validate_json("""{"operation": "append", "property": "yes"}""")
    assert summary.operation == Operation.APPEND
    assert summary.additional_properties == {"property": "yes"}


def test_deserialize_snapshot(snapshot: Snapshot) -> None:
    payload = (
        '{"snapshot-id": 25, "parent-snapshot-id": 19, "sequence-number": 200, "timestamp-ms": 1602638573590, '
        '"manifest-list": "s3:/a/b/c.avro", "summary": {"operation": "append"}, "schema-id": 3}'
    )
    actual = Snapshot.model_validate_json(payload)
    assert actual == snapshot


def test_deserialize_snapshot_without_operation(snapshot: Snapshot) -> None:
    payload = (
        '{"snapshot-id": 25, "parent-snapshot-id": 19, "sequence-number": 200, "timestamp-ms": 1602638573590, '
        '"manifest-list": "s3:/a/b/c.avro", "summary": {}, "schema-id": 3}'
    )
    with pytest.warns(UserWarning, match="Encountered invalid snapshot summary: operation is missing, defaulting to overwrite"):
        actual = Snapshot.model_validate_json(payload)
    assert actual.summary.operation == Operation.OVERWRITE


def test_deserialize_snapshot_with_properties(snapshot_with_properties: Snapshot) -> None:
    payload = (
        '{"snapshot-id":25,"parent-snapshot-id":19,"sequence-number":200,"timestamp-ms":1602638573590,'
        '"manifest-list":"s3:/a/b/c.avro","summary":{"operation":"append","foo":"bar"},"schema-id":3}'
    )
    snapshot = Snapshot.model_validate_json(payload)
    assert snapshot == snapshot_with_properties


def test_snapshot_repr(snapshot: Snapshot) -> None:
    assert repr(snapshot) == (
        "Snapshot(snapshot_id=25, parent_snapshot_id=19, sequence_number=200, timestamp_ms=1602638573590, "
        "manifest_list='s3:/a/b/c.avro', summary=Summary(Operation.APPEND), schema_id=3)"
    )
    assert snapshot == eval(repr(snapshot))


def test_snapshot_with_properties_repr(snapshot_with_properties: Snapshot) -> None:
    assert repr(snapshot_with_properties) == (
        "Snapshot(snapshot_id=25, parent_snapshot_id=19, sequence_number=200, timestamp_ms=1602638573590, "
        "manifest_list='s3:/a/b/c.avro', summary=Summary(Operation.APPEND, **{'foo': 'bar'}), schema_id=3)"
    )
    assert snapshot_with_properties == eval(repr(snapshot_with_properties))


@pytest.fixture
def manifest_file() -> ManifestFile:
    return ManifestFile.from_args(
        content=ManifestContent.DATA,
        manifest_length=100,
        added_files_count=1,
        existing_files_count=2,
        deleted_files_count=3,
        added_rows_count=100,
        existing_rows_count=110,
        deleted_rows_count=120,
    )


@pytest.mark.integration
def test_snapshot_summary_collector(table_schema_simple: Schema) -> None:
    ssc = SnapshotSummaryCollector()

    assert ssc.build() == {}
    data_file = DataFile.from_args(content=DataFileContent.DATA, record_count=100, file_size_in_bytes=1234, partition=Record())
    ssc.add_file(data_file, schema=table_schema_simple)

    assert ssc.build() == {
        "added-data-files": "1",
        "added-files-size": "1234",
        "added-records": "100",
    }


@pytest.mark.integration
def test_snapshot_summary_collector_with_partition() -> None:
    # Given

    ssc = SnapshotSummaryCollector()

    assert ssc.build() == {}
    schema = Schema(
        NestedField(field_id=1, name="bool_field", field_type=BooleanType(), required=False),
        NestedField(field_id=2, name="string_field", field_type=StringType(), required=False),
        NestedField(field_id=3, name="int_field", field_type=IntegerType(), required=False),
    )
    spec = PartitionSpec(PartitionField(source_id=3, field_id=1001, transform=IdentityTransform(), name="int_field"))
    data_file_1 = DataFile.from_args(content=DataFileContent.DATA, record_count=100, file_size_in_bytes=1234, partition=Record(1))
    data_file_2 = DataFile.from_args(content=DataFileContent.DATA, record_count=200, file_size_in_bytes=4321, partition=Record(2))
    # When
    ssc.add_file(data_file=data_file_1, schema=schema, partition_spec=spec)
    ssc.remove_file(data_file=data_file_1, schema=schema, partition_spec=spec)
    ssc.remove_file(data_file=data_file_2, schema=schema, partition_spec=spec)

    # Then
    assert ssc.build() == {
        "added-files-size": "1234",
        "removed-files-size": "5555",
        "added-data-files": "1",
        "deleted-data-files": "2",
        "added-records": "100",
        "deleted-records": "300",
        "changed-partition-count": "2",
    }

    # When
    ssc.set_partition_summary_limit(10)

    # Then
    assert ssc.build() == {
        "added-files-size": "1234",
        "removed-files-size": "5555",
        "added-data-files": "1",
        "deleted-data-files": "2",
        "added-records": "100",
        "deleted-records": "300",
        "changed-partition-count": "2",
        "partition-summaries-included": "true",
        "partitions.int_field=1": (
            "added-files-size=1234,removed-files-size=1234,added-data-files=1,"
            "deleted-data-files=1,added-records=100,deleted-records=100"
        ),
        "partitions.int_field=2": "removed-files-size=4321,deleted-data-files=1,deleted-records=200",
    }


@pytest.mark.integration
def test_snapshot_summary_collector_with_partition_limit_in_constructor() -> None:
    # Given
    partition_summary_limit = 10
    ssc = SnapshotSummaryCollector(partition_summary_limit=partition_summary_limit)

    assert ssc.build() == {}
    schema = Schema(
        NestedField(field_id=1, name="bool_field", field_type=BooleanType(), required=False),
        NestedField(field_id=2, name="string_field", field_type=StringType(), required=False),
        NestedField(field_id=3, name="int_field", field_type=IntegerType(), required=False),
    )
    spec = PartitionSpec(PartitionField(source_id=3, field_id=1001, transform=IdentityTransform(), name="int_field"))
    data_file_1 = DataFile.from_args(content=DataFileContent.DATA, record_count=100, file_size_in_bytes=1234, partition=Record(1))
    data_file_2 = DataFile.from_args(content=DataFileContent.DATA, record_count=200, file_size_in_bytes=4321, partition=Record(2))

    # When
    ssc.add_file(data_file=data_file_1, schema=schema, partition_spec=spec)
    ssc.remove_file(data_file=data_file_1, schema=schema, partition_spec=spec)
    ssc.remove_file(data_file=data_file_2, schema=schema, partition_spec=spec)

    # Then
    assert ssc.build() == {
        "added-files-size": "1234",
        "removed-files-size": "5555",
        "added-data-files": "1",
        "deleted-data-files": "2",
        "added-records": "100",
        "deleted-records": "300",
        "changed-partition-count": "2",
        "partition-summaries-included": "true",
        "partitions.int_field=1": (
            "added-files-size=1234,removed-files-size=1234,added-data-files=1,"
            "deleted-data-files=1,added-records=100,deleted-records=100"
        ),
        "partitions.int_field=2": "removed-files-size=4321,deleted-data-files=1,deleted-records=200",
    }


@pytest.mark.integration
def test_partition_summaries_included_not_set_when_no_change() -> None:
    ssc = SnapshotSummaryCollector()
    # No files added, so no partition_metrics
    ssc.set_partition_summary_limit(10)
    result = ssc.build()
    assert "partition-summaries-included" not in result
    assert result == {}  # Should be empty dict


@pytest.mark.integration
def test_partition_summaries_included_not_set_when_unpartitioned_files(table_schema_simple: Schema) -> None:
    ssc = SnapshotSummaryCollector()
    data_file = DataFile.from_args(content=DataFileContent.DATA, record_count=100, file_size_in_bytes=1234, partition=Record())
    ssc.add_file(data_file, schema=table_schema_simple)
    ssc.set_partition_summary_limit(10)
    result = ssc.build()
    assert "partition-summaries-included" not in result


def test_merge_snapshot_summaries_empty() -> None:
    assert update_snapshot_summaries(Summary(Operation.APPEND)) == Summary(
        operation=Operation.APPEND,
        **{
            "total-data-files": "0",
            "total-delete-files": "0",
            "total-records": "0",
            "total-files-size": "0",
            "total-position-deletes": "0",
            "total-equality-deletes": "0",
        },
    )


def test_merge_snapshot_summaries_new_summary() -> None:
    actual = update_snapshot_summaries(
        summary=Summary(
            operation=Operation.APPEND,
            **{
                "added-data-files": "1",
                "added-delete-files": "2",
                "added-equality-deletes": "3",
                "added-files-size": "4",
                "added-position-deletes": "5",
                "added-records": "6",
            },
        )
    )

    expected = Summary(
        operation=Operation.APPEND,
        **{
            "added-data-files": "1",
            "added-delete-files": "2",
            "added-equality-deletes": "3",
            "added-files-size": "4",
            "added-position-deletes": "5",
            "added-records": "6",
            "total-data-files": "1",
            "total-delete-files": "2",
            "total-records": "6",
            "total-files-size": "4",
            "total-position-deletes": "5",
            "total-equality-deletes": "3",
        },
    )

    assert actual == expected


def test_merge_snapshot_summaries_overwrite_summary() -> None:
    actual = update_snapshot_summaries(
        summary=Summary(
            operation=Operation.OVERWRITE,
            **{
                "added-data-files": "1",
                "added-delete-files": "2",
                "added-equality-deletes": "3",
                "added-files-size": "4",
                "added-position-deletes": "5",
                "added-records": "6",
            },
        ),
        previous_summary={
            "total-data-files": "1",
            "total-delete-files": "1",
            "total-equality-deletes": "1",
            "total-files-size": "1",
            "total-position-deletes": "1",
            "total-records": "1",
        },
    )

    expected = {
        "added-data-files": "1",
        "added-delete-files": "2",
        "added-equality-deletes": "3",
        "added-files-size": "4",
        "added-position-deletes": "5",
        "added-records": "6",
        "total-data-files": "2",
        "total-delete-files": "3",
        "total-records": "7",
        "total-files-size": "5",
        "total-position-deletes": "6",
        "total-equality-deletes": "4",
    }

    assert actual.additional_properties == expected


def test_invalid_operation() -> None:
    with pytest.raises(ValueError) as e:
        update_snapshot_summaries(summary=Summary(Operation.REPLACE))
    assert "Operation not implemented: Operation.REPLACE" in str(e.value)


def test_invalid_type() -> None:
    with pytest.raises(ValueError) as e:
        update_snapshot_summaries(
            summary=Summary(
                operation=Operation.OVERWRITE,
                **{
                    "added-data-files": "1",
                    "added-delete-files": "2",
                    "added-equality-deletes": "3",
                    "added-files-size": "4",
                    "added-position-deletes": "5",
                    "added-records": "6",
                },
            ),
            previous_summary={"total-data-files": "abc"},  # should be a number
        )

    assert "Could not parse summary property total-data-files to an int: abc" in str(e.value)


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


def test_ancestors_between(table_v2_with_extensive_snapshots: Table) -> None:
    oldest_snapshot = table_v2_with_extensive_snapshots.snapshots()[0]
    current_snapshot = cast(Snapshot, table_v2_with_extensive_snapshots.current_snapshot())
    assert (
        len(
            list(
                ancestors_between(
                    oldest_snapshot,
                    current_snapshot,
                    table_v2_with_extensive_snapshots.metadata,
                )
            )
        )
        == 2000
    )


def test_latest_ancestor_before_timestamp() -> None:
    from pyiceberg.table.metadata import TableMetadataV2

    # Create metadata with 4 snapshots at ordered timestamps
    metadata = TableMetadataV2(
        **{
            "format-version": 2,
            "table-uuid": "9c12d441-03fe-4693-9a96-a0705ddf69c1",
            "location": "s3://bucket/test/location",
            "last-sequence-number": 4,
            "last-updated-ms": 1602638573590,
            "last-column-id": 1,
            "current-schema-id": 0,
            "schemas": [{"type": "struct", "schema-id": 0, "fields": [{"id": 1, "name": "x", "required": True, "type": "long"}]}],
            "default-spec-id": 0,
            "partition-specs": [{"spec-id": 0, "fields": []}],
            "last-partition-id": 999,
            "default-sort-order-id": 0,
            "sort-orders": [{"order-id": 0, "fields": []}],
            "current-snapshot-id": 4,
            "snapshots": [
                {
                    "snapshot-id": 1,
                    "timestamp-ms": 1000,
                    "sequence-number": 1,
                    "summary": {"operation": "append"},
                    "manifest-list": "s3://a/1.avro",
                },
                {
                    "snapshot-id": 2,
                    "parent-snapshot-id": 1,
                    "timestamp-ms": 2000,
                    "sequence-number": 2,
                    "summary": {"operation": "append"},
                    "manifest-list": "s3://a/2.avro",
                },
                {
                    "snapshot-id": 3,
                    "parent-snapshot-id": 2,
                    "timestamp-ms": 3000,
                    "sequence-number": 3,
                    "summary": {"operation": "append"},
                    "manifest-list": "s3://a/3.avro",
                },
                {
                    "snapshot-id": 4,
                    "parent-snapshot-id": 3,
                    "timestamp-ms": 4000,
                    "sequence-number": 4,
                    "summary": {"operation": "append"},
                    "manifest-list": "s3://a/4.avro",
                },
            ],
        }
    )

    result = latest_ancestor_before_timestamp(metadata, 3500)
    assert result is not None
    assert result.snapshot_id == 3

    result = latest_ancestor_before_timestamp(metadata, 2500)
    assert result is not None
    assert result.snapshot_id == 2

    result = latest_ancestor_before_timestamp(metadata, 5000)
    assert result is not None
    assert result.snapshot_id == 4

    result = latest_ancestor_before_timestamp(metadata, 3000)
    assert result is not None
    assert result.snapshot_id == 2

    result = latest_ancestor_before_timestamp(metadata, 1000)
    assert result is None
