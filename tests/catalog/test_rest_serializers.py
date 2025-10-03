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
import json

from pyiceberg.catalog.rest.expression import (
    AndOrExpression,
    Expression,
    LiteralExpression,
    Term,
)
from pyiceberg.catalog.rest.planning_models import (
    AsyncPlanningResult,
    CancelledPlanningResult,
    CompletedPlanningWithIDResult,
    DataFile,
    DeleteFile,
    EqualityDeleteFile,
    FailedPlanningResult,
    FileScanTask,
    PlanTableScanRequest,
    PlanTableScanResult,
    PositionDeleteFile,
    ScanTasks,
)
from pyiceberg.catalog.rest.response import ErrorResponseMessage


def test_serialize_plan_table_scan_request() -> None:
    """Test serializing a PlanTableScanRequest to a dict"""
    expression = AndOrExpression(
        type="and",
        left=Expression(
            root=AndOrExpression(
                type="or",
                left=Expression(
                    root=AndOrExpression(
                        type="and",
                        left=Expression(
                            root=LiteralExpression(type="lt", term=Term(root="a"), value={"type": "integer", "value": 1})
                        ),
                        right=Expression(
                            root=LiteralExpression(type="lt-eq", term=Term(root="b"), value={"type": "integer", "value": 2})
                        ),
                    )
                ),
                right=Expression(root=LiteralExpression(type="eq", term=Term(root="c"), value={"type": "integer", "value": 3})),
            )
        ),
        right=Expression(root=LiteralExpression(type="gt", term=Term(root="d"), value={"type": "integer", "value": 4})),
    )
    request = PlanTableScanRequest(
        snapshot_id=1,
        select=["a", "b", "c"],
        filter=Expression(root=expression),
        case_sensitive=True,
    )
    # Assert that JSON matches.
    assert request.model_dump_json(exclude_none=True) == snapshot_json_for_plan_table_scan_request()


def test_deserialize_plan_table_scan_request() -> None:
    """Test deserializing a dict to a PlanTableScanRequest"""
    model = PlanTableScanRequest.model_validate_json(snapshot_json_for_plan_table_scan_request())
    expression = AndOrExpression(
        type="and",
        left=Expression(
            root=AndOrExpression(
                type="or",
                left=Expression(
                    root=AndOrExpression(
                        type="and",
                        left=Expression(
                            root=LiteralExpression(type="lt", term=Term(root="a"), value={"type": "integer", "value": 1})
                        ),
                        right=Expression(
                            root=LiteralExpression(type="lt-eq", term=Term(root="b"), value={"type": "integer", "value": 2})
                        ),
                    )
                ),
                right=Expression(root=LiteralExpression(type="eq", term=Term(root="c"), value={"type": "integer", "value": 3})),
            )
        ),
        right=Expression(root=LiteralExpression(type="gt", term=Term(root="d"), value={"type": "integer", "value": 4})),
    )
    expected = PlanTableScanRequest(
        snapshot_id=1,
        select=["a", "b", "c"],
        filter=Expression(root=expression),
        case_sensitive=True,
    )

    # Assert that deserialized dict == Python object
    assert model == expected


def test_deserialize_scan_tasks() -> None:
    """Test deserializing dict to ScanTasks"""
    scan_tasks = ScanTasks.model_validate_json(snapshot_json_for_scan_tasks())

    # Assert JSON fields match expected.
    assert len(scan_tasks.file_scan_tasks) == 1
    assert len(scan_tasks.delete_files) == 2
    assert scan_tasks.file_scan_tasks[0].data_file.file_path == "/path/to/data-a.parquet"
    assert scan_tasks.delete_files[0].root.file_path == "/path/to/delete-a.parquet"
    assert scan_tasks.delete_files[1].root.file_path == "/path/to/delete-b.parquet"


def test_serialize_scan_tasks() -> None:
    """Test serializing a ScanTasks to a dict"""
    scan_tasks = ScanTasks(
        file_scan_tasks=[
            FileScanTask(
                data_file=DataFile(
                    content="data",
                    file_path="/path/to/data-a.parquet",
                    file_format="parquet",
                    partition=[],
                    record_count=56,
                    file_size_in_bytes=1024,
                    spec_id=0,
                ),
                delete_file_references=[0, 1],
            )
        ],
        delete_files=[
            DeleteFile(
                root=PositionDeleteFile(
                    content="position-deletes",
                    file_path="/path/to/delete-a.parquet",
                    file_format="parquet",
                    partition=[],
                    record_count=10,
                    file_size_in_bytes=256,
                    spec_id=0,
                )
            ),
            DeleteFile(
                root=EqualityDeleteFile(
                    content="equality-deletes",
                    file_path="/path/to/delete-b.parquet",
                    file_format="parquet",
                    partition=[],
                    record_count=10,
                    file_size_in_bytes=256,
                    spec_id=0,
                    equality_ids=[1, 2],
                )
            ),
        ],
    )

    # Assert that JSON matches.
    assert scan_tasks.model_dump_json(exclude_none=True) == snapshot_json_for_scan_tasks()


def snapshot_json_for_plan_table_scan_request() -> str:
    return """{"snapshot-id":1,"select":["a","b","c"],"filter":{"type":"and","left":{"type":"or","left":{"type":"and","left":{"type":"lt","term":"a","value":{"type":"integer","value":1}},"right":{"type":"lt-eq","term":"b","value":{"type":"integer","value":2}}},"right":{"type":"eq","term":"c","value":{"type":"integer","value":3}}},"right":{"type":"gt","term":"d","value":{"type":"integer","value":4}}},"case-sensitive":true,"use-snapshot-schema":false}"""


def snapshot_json_for_scan_tasks() -> str:
    return """{"delete-files":[{"content":"position-deletes","file-path":"/path/to/delete-a.parquet","file-format":"parquet","spec-id":0,"partition":[],"file-size-in-bytes":256,"record-count":10},{"content":"equality-deletes","file-path":"/path/to/delete-b.parquet","file-format":"parquet","spec-id":0,"partition":[],"file-size-in-bytes":256,"record-count":10,"equality-ids":[1,2]}],"file-scan-tasks":[{"data-file":{"content":"data","file-path":"/path/to/data-a.parquet","file-format":"parquet","spec-id":0,"partition":[],"file-size-in-bytes":1024,"record-count":56},"delete-file-references":[0,1]}]}"""


def test_deserialize_async_planning_result() -> None:
    """Test deserializing a dict to an AsyncPlanningResult"""
    result = PlanTableScanResult.model_validate_json(snapshot_json_for_async_planning_result())
    expected = AsyncPlanningResult(status="submitted", plan_id="plan-123")
    # Assert that deserialized dict == Python object
    assert result.root == expected


def test_serialize_async_planning_result() -> None:
    """Test serializing an AsyncPlanningResult to a dict"""
    result = PlanTableScanResult(root=AsyncPlanningResult(status="submitted", plan_id="plan-123"))
    # Assert that JSON matches
    assert json.loads(result.model_dump_json(by_alias=True)) == json.loads(snapshot_json_for_async_planning_result())


def snapshot_json_for_async_planning_result() -> str:
    return """{"status":"submitted","plan-id":"plan-123"}"""


def test_deserialize_failed_planning_result() -> None:
    """Test deserializing a dict to a FailedPlanningResult"""
    result = PlanTableScanResult.model_validate_json(snapshot_json_for_failed_planning_result())
    expected = FailedPlanningResult(
        status="failed",
        error=ErrorResponseMessage(
            message="The plan is invalid",
            type="NoSuchPlanException",
            code=404,
        ),
    )
    # Assert that deserialized dict == Python object
    assert result.root == expected


def test_serialize_failed_planning_result() -> None:
    """Test serializing a FailedPlanningResult to a dict"""
    result = PlanTableScanResult(
        root=FailedPlanningResult(
            status="failed",
            error=ErrorResponseMessage(
                message="The plan is invalid",
                type="NoSuchPlanException",
                code=404,
            ),
        )
    )
    # Assert that JSON matches
    assert json.loads(result.model_dump_json(by_alias=True, exclude_none=True)) == json.loads(
        snapshot_json_for_failed_planning_result()
    )


def snapshot_json_for_failed_planning_result() -> str:
    return """{"status":"failed","error":{"message":"The plan is invalid","type":"NoSuchPlanException","code":404}}"""


def test_deserialize_cancelled_planning_result() -> None:
    """Test deserializing a dict to an CancelledPlanningResult"""
    result = PlanTableScanResult.model_validate_json(snapshot_json_for_cancelled_planning_result())
    expected = CancelledPlanningResult(status="cancelled")
    # Assert that deserialized dict == Python object
    assert result.root == expected


def test_serialize_cancelled_planning_result() -> None:
    """Test serializing an CancelledPlanningResult to a dict"""
    result = PlanTableScanResult(root=CancelledPlanningResult(status="cancelled"))
    # Assert that JSON matches
    assert json.loads(result.model_dump_json(by_alias=True)) == json.loads(snapshot_json_for_cancelled_planning_result())


def snapshot_json_for_cancelled_planning_result() -> str:
    return """{"status":"cancelled"}"""


def test_deserialize_completed_planning_with_id_result() -> None:
    """Test deserializing a dict to a CompletedPlanningWithIDResult"""
    scan_tasks_dict = json.loads(snapshot_json_for_scan_tasks())
    scan_tasks_dict["status"] = "completed"
    scan_tasks_dict["plan-id"] = "plan-456"
    json_str = json.dumps(scan_tasks_dict)

    result = PlanTableScanResult.model_validate_json(json_str)
    expected_scan_tasks = ScanTasks.model_validate_json(snapshot_json_for_scan_tasks())

    expected = CompletedPlanningWithIDResult(
        status="completed",
        plan_id="plan-456",
        file_scan_tasks=expected_scan_tasks.file_scan_tasks,
        delete_files=expected_scan_tasks.delete_files,
    )
    # Assert that deserialized dict == Python object
    assert result.root == expected


def test_serialize_completed_planning_with_id_result() -> None:
    """Test serializing a CompletedPlanningWithIDResult to a dict"""
    expected_scan_tasks = ScanTasks.model_validate_json(snapshot_json_for_scan_tasks())
    result = PlanTableScanResult(
        root=CompletedPlanningWithIDResult(
            status="completed",
            plan_id="plan-456",
            file_scan_tasks=expected_scan_tasks.file_scan_tasks,
            delete_files=expected_scan_tasks.delete_files,
        )
    )

    scan_tasks_dict = json.loads(snapshot_json_for_scan_tasks())
    scan_tasks_dict["status"] = "completed"
    scan_tasks_dict["plan-id"] = "plan-456"

    # Assert that JSON matches
    assert json.loads(result.model_dump_json(exclude_none=True, by_alias=True)) == scan_tasks_dict
