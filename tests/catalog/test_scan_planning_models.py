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
from typing import Any

import pytest
from pydantic import TypeAdapter, ValidationError

from pyiceberg.catalog.rest.scan_planning import (
    CountMap,
    FetchScanTasksRequest,
    PlanCancelled,
    PlanCompleted,
    PlanningResponse,
    PlanSubmitted,
    PlanTableScanRequest,
    RESTDataFile,
    RESTDeleteFile,
    RESTEqualityDeleteFile,
    RESTFileScanTask,
    RESTPositionDeleteFile,
    ScanTasks,
    StorageCredential,
    ValueMap,
)
from pyiceberg.expressions import AlwaysTrue, EqualTo, Reference
from pyiceberg.manifest import FileFormat


def test_count_map_valid() -> None:
    cm = CountMap(keys=[1, 2, 3], values=[100, 200, 300])
    assert cm.to_dict() == {1: 100, 2: 200, 3: 300}


def test_count_map_empty() -> None:
    cm = CountMap()
    assert cm.to_dict() == {}


def test_count_map_length_mismatch() -> None:
    with pytest.raises(ValidationError) as exc_info:
        CountMap(keys=[1, 2, 3], values=[100, 200])
    assert "must have same length" in str(exc_info.value)


def test_value_map_mixed_types() -> None:
    vm = ValueMap(keys=[1, 2, 3], values=[True, 42, "val"])
    assert vm.to_dict() == {1: True, 2: 42, 3: "val"}


def test_data_file_parsing() -> None:
    data = {
        "spec-id": 0,
        "content": "data",
        "file-path": "s3://bucket/table/file.parquet",
        "file-format": "parquet",
        "file-size-in-bytes": 1024,
        "record-count": 100,
    }
    df = RESTDataFile.model_validate(data)
    assert df.content == "data"
    assert df.file_path == "s3://bucket/table/file.parquet"
    assert df.file_format == FileFormat.PARQUET
    assert df.file_size_in_bytes == 1024


def test_data_file_with_stats() -> None:
    data = {
        "spec-id": 0,
        "content": "data",
        "file-path": "s3://bucket/table/file.parquet",
        "file-format": "parquet",
        "file-size-in-bytes": 1024,
        "record-count": 100,
        "column-sizes": {"keys": [1, 2], "values": [500, 524]},
        "value-counts": {"keys": [1, 2], "values": [100, 100]},
    }
    df = RESTDataFile.model_validate(data)
    assert df.column_sizes is not None
    assert df.column_sizes.to_dict() == {1: 500, 2: 524}


def test_position_delete_file() -> None:
    data = {
        "spec-id": 0,
        "content": "position-deletes",
        "file-path": "s3://bucket/table/delete.parquet",
        "file-format": "parquet",
        "file-size-in-bytes": 512,
        "record-count": 10,
        "content-offset": 100,
        "content-size-in-bytes": 200,
    }
    pdf = RESTPositionDeleteFile.model_validate(data)
    assert pdf.content == "position-deletes"
    assert pdf.content_offset == 100
    assert pdf.content_size_in_bytes == 200


def test_equality_delete_file() -> None:
    data = {
        "spec-id": 0,
        "content": "equality-deletes",
        "file-path": "s3://bucket/table/eq-delete.parquet",
        "file-format": "parquet",
        "file-size-in-bytes": 256,
        "record-count": 5,
        "equality-ids": [1, 2],
    }
    edf = RESTEqualityDeleteFile.model_validate(data)
    assert edf.content == "equality-deletes"
    assert edf.equality_ids == [1, 2]


def test_file_format_case_insensitive() -> None:
    for fmt in ["parquet", "PARQUET", "Parquet"]:
        data = {
            "spec-id": 0,
            "content": "data",
            "file-path": "/path",
            "file-format": fmt,
            "file-size-in-bytes": 100,
            "record-count": 10,
        }
        df = RESTDataFile.model_validate(data)
        assert df.file_format == FileFormat.PARQUET


@pytest.mark.parametrize(
    "format_str,expected",
    [
        ("parquet", FileFormat.PARQUET),
        ("avro", FileFormat.AVRO),
        ("orc", FileFormat.ORC),
    ],
)
def test_file_formats(format_str: str, expected: FileFormat) -> None:
    data = {
        "spec-id": 0,
        "content": "data",
        "file-path": f"s3://bucket/table/path/file.{format_str}",
        "file-format": format_str,
        "file-size-in-bytes": 1024,
        "record-count": 100,
    }
    df = RESTDataFile.model_validate(data)
    assert df.file_format == expected


def test_delete_file_discriminator_position() -> None:
    data = {
        "spec-id": 0,
        "content": "position-deletes",
        "file-path": "s3://bucket/table/delete.parquet",
        "file-format": "parquet",
        "file-size-in-bytes": 256,
        "record-count": 5,
    }
    result = TypeAdapter(RESTDeleteFile).validate_python(data)
    assert isinstance(result, RESTPositionDeleteFile)


def test_delete_file_discriminator_equality() -> None:
    data = {
        "spec-id": 0,
        "content": "equality-deletes",
        "file-path": "s3://bucket/table/delete.parquet",
        "file-format": "parquet",
        "file-size-in-bytes": 256,
        "record-count": 5,
        "equality-ids": [1],
    }
    result = TypeAdapter(RESTDeleteFile).validate_python(data)
    assert isinstance(result, RESTEqualityDeleteFile)


def test_basic_scan_task() -> None:
    data = {
        "data-file": {
            "spec-id": 0,
            "content": "data",
            "file-path": "s3://bucket/table/file.parquet",
            "file-format": "parquet",
            "file-size-in-bytes": 1024,
            "record-count": 100,
        }
    }
    task = RESTFileScanTask.model_validate(data)
    assert task.data_file.file_path == "s3://bucket/table/file.parquet"
    assert task.delete_file_references is None
    assert task.residual_filter is None


def test_scan_task_with_delete_references() -> None:
    data = {
        "data-file": {
            "spec-id": 0,
            "content": "data",
            "file-path": "s3://bucket/table/file.parquet",
            "file-format": "parquet",
            "file-size-in-bytes": 1024,
            "record-count": 100,
        },
        "delete-file-references": [0, 1, 2],
    }
    task = RESTFileScanTask.model_validate(data)
    assert task.delete_file_references == [0, 1, 2]


def test_scan_task_with_residual_filter_true() -> None:
    data = {
        "data-file": {
            "spec-id": 0,
            "content": "data",
            "file-path": "s3://bucket/table/file.parquet",
            "file-format": "parquet",
            "file-size-in-bytes": 1024,
            "record-count": 100,
        },
        "residual-filter": True,
    }
    task = RESTFileScanTask.model_validate(data)
    assert isinstance(task.residual_filter, AlwaysTrue)


def test_empty_scan_tasks() -> None:
    data: dict[str, Any] = {
        "delete-files": [],
        "file-scan-tasks": [],
        "plan-tasks": [],
    }
    scan_tasks = ScanTasks.model_validate(data)
    assert len(scan_tasks.file_scan_tasks) == 0
    assert len(scan_tasks.delete_files) == 0
    assert len(scan_tasks.plan_tasks) == 0


def test_scan_tasks_with_files() -> None:
    data = {
        "delete-files": [
            {
                "spec-id": 0,
                "content": "position-deletes",
                "file-path": "s3://bucket/table/delete.parquet",
                "file-format": "parquet",
                "file-size-in-bytes": 256,
                "record-count": 5,
            }
        ],
        "file-scan-tasks": [
            {
                "data-file": {
                    "spec-id": 0,
                    "content": "data",
                    "file-path": "s3://bucket/table/data.parquet",
                    "file-format": "parquet",
                    "file-size-in-bytes": 1024,
                    "record-count": 100,
                },
                "delete-file-references": [0],
            }
        ],
        "plan-tasks": ["token-1", "token-2"],
    }
    scan_tasks = ScanTasks.model_validate(data)
    assert len(scan_tasks.delete_files) == 1
    assert len(scan_tasks.file_scan_tasks) == 1
    assert len(scan_tasks.plan_tasks) == 2


def test_invalid_delete_file_reference() -> None:
    data = {
        "delete-files": [],
        "file-scan-tasks": [
            {
                "data-file": {
                    "spec-id": 0,
                    "content": "data",
                    "file-path": "s3://bucket/table/data.parquet",
                    "file-format": "parquet",
                    "file-size-in-bytes": 1024,
                    "record-count": 100,
                },
                "delete-file-references": [0],
            }
        ],
        "plan-tasks": [],
    }
    with pytest.raises(ValidationError) as exc_info:
        ScanTasks.model_validate(data)
    assert "Invalid delete file reference" in str(exc_info.value)


def test_delete_files_require_file_scan_tasks() -> None:
    data = {
        "delete-files": [
            {
                "spec-id": 0,
                "content": "position-deletes",
                "file-path": "s3://bucket/table/delete.parquet",
                "file-format": "parquet",
                "file-size-in-bytes": 256,
                "record-count": 5,
            }
        ],
        "file-scan-tasks": [],
        "plan-tasks": [],
    }
    with pytest.raises(ValidationError) as exc_info:
        ScanTasks.model_validate(data)
    assert "deleteFiles should only be returned with fileScanTasks" in str(exc_info.value)


def test_minimal_request() -> None:
    request = PlanTableScanRequest()
    dumped = request.model_dump(by_alias=True, exclude_none=True)
    assert dumped == {"case-sensitive": True, "use-snapshot-schema": False}


def test_request_with_snapshot_id() -> None:
    request = PlanTableScanRequest(snapshot_id=12345)
    dumped = request.model_dump(by_alias=True, exclude_none=True)
    assert dumped["snapshot-id"] == 12345


def test_request_with_select_and_filter() -> None:
    request = PlanTableScanRequest(
        select=["id", "name"],
        filter=EqualTo(Reference("id"), 42),
    )
    dumped = request.model_dump(by_alias=True, exclude_none=True)
    assert dumped["select"] == ["id", "name"]
    assert "filter" in dumped


def test_incremental_scan_request() -> None:
    request = PlanTableScanRequest(
        start_snapshot_id=100,
        end_snapshot_id=200,
    )
    dumped = request.model_dump(by_alias=True, exclude_none=True)
    assert dumped["start-snapshot-id"] == 100
    assert dumped["end-snapshot-id"] == 200


def test_start_snapshot_requires_end_snapshot() -> None:
    with pytest.raises(ValidationError) as exc_info:
        PlanTableScanRequest(start_snapshot_id=100)
    assert "end-snapshot-id is required" in str(exc_info.value)


def test_snapshot_id_conflicts_with_start_snapshot() -> None:
    with pytest.raises(ValidationError) as exc_info:
        PlanTableScanRequest(snapshot_id=50, start_snapshot_id=100, end_snapshot_id=200)
    assert "Cannot specify both" in str(exc_info.value)


def test_fetch_scan_tasks_request() -> None:
    request = FetchScanTasksRequest(plan_task="token-abc-123")
    dumped = request.model_dump(by_alias=True)
    assert dumped == {"plan-task": "token-abc-123"}


def test_completed_response() -> None:
    data = {
        "status": "completed",
        "plan-id": "plan-123",
        "delete-files": [],
        "file-scan-tasks": [],
        "plan-tasks": [],
    }
    result = TypeAdapter(PlanningResponse).validate_python(data)
    assert isinstance(result, PlanCompleted)
    assert result.plan_id == "plan-123"


def test_completed_response_without_plan_id() -> None:
    data = {
        "status": "completed",
        "delete-files": [],
        "file-scan-tasks": [],
        "plan-tasks": [],
    }
    result = TypeAdapter(PlanningResponse).validate_python(data)
    assert isinstance(result, PlanCompleted)
    assert result.plan_id is None


def test_completed_response_with_credentials() -> None:
    data = {
        "status": "completed",
        "delete-files": [],
        "file-scan-tasks": [],
        "plan-tasks": [],
        "storage-credentials": [
            {"prefix": "s3://bucket/", "config": {}},
        ],
    }
    result = TypeAdapter(PlanningResponse).validate_python(data)
    assert isinstance(result, PlanCompleted)
    assert result.storage_credentials is not None
    assert len(result.storage_credentials) == 1


def test_submitted_response() -> None:
    data = {
        "status": "submitted",
        "plan-id": "drus-plan",
    }
    result = TypeAdapter(PlanningResponse).validate_python(data)
    assert isinstance(result, PlanSubmitted)
    assert result.plan_id == "drus-plan"


def test_submitted_response_without_plan_id() -> None:
    data = {"status": "submitted"}
    result = TypeAdapter(PlanningResponse).validate_python(data)
    assert isinstance(result, PlanSubmitted)


def test_cancelled_response() -> None:
    data = {"status": "cancelled"}
    result = TypeAdapter(PlanningResponse).validate_python(data)
    assert isinstance(result, PlanCancelled)


def test_storage_credential_parsing() -> None:
    data = {
        "prefix": "s3://bucket/path/",
        "config": {
            "s3.access-key-id": "key",
            "s3.secret-access-key": "secret",
        },
    }
    cred = StorageCredential.model_validate(data)
    assert cred.prefix == "s3://bucket/path/"
    assert cred.config["s3.access-key-id"] == "key"
