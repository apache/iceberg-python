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
import pytest

from pyiceberg.manifest import DataFile, DataFileContent, ManifestContent, ManifestFile
from pyiceberg.table.snapshots import Operation, Snapshot, SnapshotSummaryCollector, Summary, update_snapshot_summaries


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
    assert (
        snapshot.model_dump_json()
        == """{"snapshot-id":25,"parent-snapshot-id":19,"sequence-number":200,"timestamp-ms":1602638573590,"manifest-list":"s3:/a/b/c.avro","summary":{"operation":"append"},"schema-id":3}"""
    )


def test_serialize_snapshot_without_sequence_number() -> None:
    snapshot = Snapshot(
        snapshot_id=25,
        parent_snapshot_id=19,
        timestamp_ms=1602638573590,
        manifest_list="s3:/a/b/c.avro",
        summary=Summary(Operation.APPEND),
        schema_id=3,
    )
    actual = snapshot.model_dump_json()
    expected = """{"snapshot-id":25,"parent-snapshot-id":19,"timestamp-ms":1602638573590,"manifest-list":"s3:/a/b/c.avro","summary":{"operation":"append"},"schema-id":3}"""
    assert actual == expected


def test_serialize_snapshot_with_properties(snapshot_with_properties: Snapshot) -> None:
    assert (
        snapshot_with_properties.model_dump_json()
        == """{"snapshot-id":25,"parent-snapshot-id":19,"sequence-number":200,"timestamp-ms":1602638573590,"manifest-list":"s3:/a/b/c.avro","summary":{"operation":"append","foo":"bar"},"schema-id":3}"""
    )


def test_deserialize_summary() -> None:
    summary = Summary.model_validate_json("""{"operation": "append"}""")
    assert summary.operation == Operation.APPEND


def test_deserialize_summary_with_properties() -> None:
    summary = Summary.model_validate_json("""{"operation": "append", "property": "yes"}""")
    assert summary.operation == Operation.APPEND
    assert summary.additional_properties == {"property": "yes"}


def test_deserialize_snapshot(snapshot: Snapshot) -> None:
    payload = """{"snapshot-id": 25, "parent-snapshot-id": 19, "sequence-number": 200, "timestamp-ms": 1602638573590, "manifest-list": "s3:/a/b/c.avro", "summary": {"operation": "append"}, "schema-id": 3}"""
    actual = Snapshot.model_validate_json(payload)
    assert actual == snapshot


def test_deserialize_snapshot_with_properties(snapshot_with_properties: Snapshot) -> None:
    payload = """{"snapshot-id":25,"parent-snapshot-id":19,"sequence-number":200,"timestamp-ms":1602638573590,"manifest-list":"s3:/a/b/c.avro","summary":{"operation":"append","foo":"bar"},"schema-id":3}"""
    snapshot = Snapshot.model_validate_json(payload)
    assert snapshot == snapshot_with_properties


def test_snapshot_repr(snapshot: Snapshot) -> None:
    assert (
        repr(snapshot)
        == """Snapshot(snapshot_id=25, parent_snapshot_id=19, sequence_number=200, timestamp_ms=1602638573590, manifest_list='s3:/a/b/c.avro', summary=Summary(Operation.APPEND), schema_id=3)"""
    )
    assert snapshot == eval(repr(snapshot))


def test_snapshot_with_properties_repr(snapshot_with_properties: Snapshot) -> None:
    assert (
        repr(snapshot_with_properties)
        == """Snapshot(snapshot_id=25, parent_snapshot_id=19, sequence_number=200, timestamp_ms=1602638573590, manifest_list='s3:/a/b/c.avro', summary=Summary(Operation.APPEND, **{'foo': 'bar'}), schema_id=3)"""
    )
    assert snapshot_with_properties == eval(repr(snapshot_with_properties))


@pytest.fixture
def manifest_file() -> ManifestFile:
    return ManifestFile(
        content=ManifestContent.DATA,
        manifest_length=100,
        added_files_count=1,
        existing_files_count=2,
        deleted_files_count=3,
        added_rows_count=100,
        existing_rows_count=110,
        deleted_rows_count=120,
    )


@pytest.fixture
def data_file() -> DataFile:
    return DataFile(
        content=DataFileContent.DATA,
        record_count=100,
        file_size_in_bytes=1234,
    )


def test_snapshot_summary_collector(data_file: DataFile) -> None:
    ssc = SnapshotSummaryCollector()

    assert ssc.build() == {}

    ssc.add_file(data_file)

    assert ssc.build() == {
        'added-data-files': '1',
        'added-files-size': '1234',
        'added-records': '100',
    }


def test_merge_snapshot_summaries_empty() -> None:
    assert update_snapshot_summaries(Summary(Operation.APPEND)) == Summary(
        operation=Operation.APPEND,
        **{
            'total-data-files': '0',
            'total-delete-files': '0',
            'total-records': '0',
            'total-files-size': '0',
            'total-position-deletes': '0',
            'total-equality-deletes': '0',
        },
    )


def test_merge_snapshot_summaries_new_summary() -> None:
    actual = update_snapshot_summaries(
        summary=Summary(
            operation=Operation.APPEND,
            **{
                'added-data-files': '1',
                'added-delete-files': '2',
                'added-equality-deletes': '3',
                'added-files-size': '4',
                'added-position-deletes': '5',
                'added-records': '6',
            },
        )
    )

    expected = Summary(
        operation=Operation.APPEND,
        **{
            'added-data-files': '1',
            'added-delete-files': '2',
            'added-equality-deletes': '3',
            'added-files-size': '4',
            'added-position-deletes': '5',
            'added-records': '6',
            'total-data-files': '1',
            'total-delete-files': '2',
            'total-records': '6',
            'total-files-size': '4',
            'total-position-deletes': '5',
            'total-equality-deletes': '3',
        },
    )

    assert actual == expected


def test_merge_snapshot_summaries_overwrite_summary() -> None:
    actual = update_snapshot_summaries(
        summary=Summary(
            operation=Operation.OVERWRITE,
            **{
                'added-data-files': '1',
                'added-delete-files': '2',
                'added-equality-deletes': '3',
                'added-files-size': '4',
                'added-position-deletes': '5',
                'added-records': '6',
            },
        ),
        previous_summary={
            'total-data-files': '1',
            'total-delete-files': '1',
            'total-equality-deletes': '1',
            'total-files-size': '1',
            'total-position-deletes': '1',
            'total-records': '1',
        },
        truncate_full_table=True,
    )

    expected = {
        'added-data-files': '1',
        'added-delete-files': '2',
        'added-equality-deletes': '3',
        'added-files-size': '4',
        'added-position-deletes': '5',
        'added-records': '6',
        'total-data-files': '1',
        'total-records': '6',
        'total-delete-files': '2',
        'total-equality-deletes': '3',
        'total-files-size': '4',
        'total-position-deletes': '5',
        'deleted-data-files': '1',
        'removed-delete-files': '1',
        'deleted-records': '1',
        'removed-files-size': '1',
        'removed-position-deletes': '1',
        'removed-equality-deletes': '1',
    }

    assert actual.additional_properties == expected


def test_invalid_operation() -> None:
    with pytest.raises(ValueError) as e:
        update_snapshot_summaries(summary=Summary(Operation.REPLACE))
    assert "Operation not implemented: Operation.REPLACE" in str(e.value)

    with pytest.raises(ValueError) as e:
        update_snapshot_summaries(summary=Summary(Operation.DELETE))
    assert "Operation not implemented: Operation.DELETE" in str(e.value)


def test_invalid_type() -> None:
    with pytest.raises(ValueError) as e:
        update_snapshot_summaries(
            summary=Summary(
                operation=Operation.OVERWRITE,
                **{
                    'added-data-files': '1',
                    'added-delete-files': '2',
                    'added-equality-deletes': '3',
                    'added-files-size': '4',
                    'added-position-deletes': '5',
                    'added-records': '6',
                },
            ),
            previous_summary={'total-data-files': 'abc'},  # should be a number
            truncate_full_table=True,
        )

    assert "Could not parse summary property total-data-files to an int: abc" in str(e.value)
