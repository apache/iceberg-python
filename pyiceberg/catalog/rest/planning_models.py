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

from datetime import date
from typing import List, Literal, Optional, Union
from uuid import UUID

from pydantic import Field

from pyiceberg.catalog.rest.expression import Expression
from pyiceberg.catalog.rest.response import ErrorResponse as IcebergErrorResponse
from pyiceberg.typedef import IcebergBaseModel, IcebergRootModel


class FieldName(IcebergRootModel[str]):
    root: str = Field(
        ...,
        description="A full field name (including parent field names), such as those passed in APIs like Java `Schema#findField(String name)`.\nThe nested field name follows these rules - Nested struct fields are named by concatenating field names at each struct level using dot (`.`) delimiter, e.g. employer.contact_info.address.zip_code - Nested fields in a map key are named using the keyword `key`, e.g. employee_address_map.key.first_name - Nested fields in a map value are named using the keyword `value`, e.g. employee_address_map.value.zip_code - Nested fields in a list are named using the keyword `element`, e.g. employees.element.first_name",
    )


class BooleanTypeValue(IcebergRootModel[bool]):
    root: bool = Field(..., json_schema_extra={"example": True})


class IntegerTypeValue(IcebergRootModel[int]):
    root: int = Field(..., json_schema_extra={"example": 42})


class LongTypeValue(IcebergRootModel[int]):
    root: int = Field(..., json_schema_extra={"example": 9223372036854775807})


class FloatTypeValue(IcebergRootModel[float]):
    root: float = Field(..., json_schema_extra={"example": 3.14})


class DoubleTypeValue(IcebergRootModel[float]):
    root: float = Field(..., json_schema_extra={"example": 123.456})


class DecimalTypeValue(IcebergRootModel[str]):
    root: str = Field(
        ...,
        description="Decimal type values are serialized as strings. Decimals with a positive scale serialize as numeric plain text, while decimals with a negative scale use scientific notation and the exponent will be equal to the negated scale. For instance, a decimal with a positive scale is '123.4500', with zero scale is '2', and with a negative scale is '2E+20'",
        json_schema_extra={"example": "123.4500"},
    )


class StringTypeValue(IcebergRootModel[str]):
    root: str = Field(..., json_schema_extra={"example": "hello"})


class UUIDTypeValue(IcebergRootModel[UUID]):
    root: UUID = Field(
        ...,
        description="UUID type values are serialized as a 36-character lowercase string in standard UUID format as specified by RFC-4122",
        json_schema_extra={"example": "eb26bdb1-a1d8-4aa6-990e-da940875492c"},
        max_length=36,
        min_length=36,
        pattern="^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
    )


class DateTypeValue(IcebergRootModel[date]):
    root: date = Field(
        ...,
        description="Date type values follow the 'YYYY-MM-DD' ISO-8601 standard date format",
        json_schema_extra={"example": "2007-12-03"},
    )


class TimeTypeValue(IcebergRootModel[str]):
    root: str = Field(
        ...,
        description="Time type values follow the 'HH:MM:SS.ssssss' ISO-8601 format with microsecond precision",
        json_schema_extra={"example": "22:31:08.123456"},
    )


class TimestampTypeValue(IcebergRootModel[str]):
    root: str = Field(
        ...,
        description="Timestamp type values follow the 'YYYY-MM-DDTHH:MM:SS.ssssss' ISO-8601 format with microsecond precision",
        json_schema_extra={"example": "2007-12-03T10:15:30.123456"},
    )


class TimestampTzTypeValue(IcebergRootModel[str]):
    root: str = Field(
        ...,
        description="TimestampTz type values follow the 'YYYY-MM-DDTHH:MM:SS.ssssss+00:00' ISO-8601 format with microsecond precision, and a timezone offset (+00:00 for UTC)",
        json_schema_extra={"example": "2007-12-03T10:15:30.123456+00:00"},
    )


class TimestampNanoTypeValue(IcebergRootModel[str]):
    root: str = Field(
        ...,
        description="Timestamp_ns type values follow the 'YYYY-MM-DDTHH:MM:SS.sssssssss' ISO-8601 format with nanosecond precision",
        json_schema_extra={"example": "2007-12-03T10:15:30.123456789"},
    )


class TimestampTzNanoTypeValue(IcebergRootModel[str]):
    root: str = Field(
        ...,
        description="Timestamp_ns type values follow the 'YYYY-MM-DDTHH:MM:SS.sssssssss+00:00' ISO-8601 format with nanosecond precision, and a timezone offset (+00:00 for UTC)",
        json_schema_extra={"example": "2007-12-03T10:15:30.123456789+00:00"},
    )


class FixedTypeValue(IcebergRootModel[str]):
    root: str = Field(
        ...,
        description="Fixed length type values are stored and serialized as an uppercase hexadecimal string preserving the fixed length",
        json_schema_extra={"example": "78797A"},
    )


class BinaryTypeValue(IcebergRootModel[str]):
    root: str = Field(
        ...,
        description="Binary type values are stored and serialized as an uppercase hexadecimal string",
        json_schema_extra={"example": "78797A"},
    )


class CountMap(IcebergBaseModel):
    keys: Optional[List[IntegerTypeValue]] = Field(None, description="List of integer column ids for each corresponding value")
    values: Optional[List[LongTypeValue]] = Field(None, description="List of Long values, matched to 'keys' by index")


class PrimitiveTypeValue(
    IcebergRootModel[
        Union[
            BooleanTypeValue,
            IntegerTypeValue,
            LongTypeValue,
            FloatTypeValue,
            DoubleTypeValue,
            DecimalTypeValue,
            StringTypeValue,
            UUIDTypeValue,
            DateTypeValue,
            TimeTypeValue,
            TimestampTypeValue,
            TimestampTzTypeValue,
            TimestampNanoTypeValue,
            TimestampTzNanoTypeValue,
            FixedTypeValue,
            BinaryTypeValue,
        ]
    ]
):
    root: Union[
        BooleanTypeValue,
        IntegerTypeValue,
        LongTypeValue,
        FloatTypeValue,
        DoubleTypeValue,
        DecimalTypeValue,
        StringTypeValue,
        UUIDTypeValue,
        DateTypeValue,
        TimeTypeValue,
        TimestampTypeValue,
        TimestampTzTypeValue,
        TimestampNanoTypeValue,
        TimestampTzNanoTypeValue,
        FixedTypeValue,
        BinaryTypeValue,
    ]


class ValueMap(IcebergBaseModel):
    keys: Optional[List[IntegerTypeValue]] = Field(None, description="List of integer column ids for each corresponding value")
    values: Optional[List[PrimitiveTypeValue]] = Field(
        None, description="List of primitive type values, matched to 'keys' by index"
    )


class PlanTableScanRequest(IcebergBaseModel):
    snapshot_id: Optional[int] = Field(
        None,
        alias="snapshot-id",
        description="Identifier for the snapshot to scan in a point-in-time scan",
    )
    select: Optional[List[FieldName]] = Field(None, description="List of selected schema fields")
    filter: Optional[Expression] = Field(None, description="Expression used to filter the table data")
    case_sensitive: Optional[bool] = Field(
        True,
        alias="case-sensitive",
        description="Enables case sensitive field matching for filter and select",
    )
    use_snapshot_schema: Optional[bool] = Field(
        False,
        alias="use-snapshot-schema",
        description="Whether to use the schema at the time the snapshot was written.\nWhen time travelling, the snapshot schema should be used (true). When scanning a branch, the table schema should be used (false).",
    )
    start_snapshot_id: Optional[int] = Field(
        None,
        alias="start-snapshot-id",
        description="Starting snapshot ID for an incremental scan (exclusive)",
    )
    end_snapshot_id: Optional[int] = Field(
        None,
        alias="end-snapshot-id",
        description="Ending snapshot ID for an incremental scan (inclusive).\nRequired when start-snapshot-id is specified.",
    )
    stats_fields: Optional[List[FieldName]] = Field(
        None,
        alias="stats-fields",
        description="List of fields for which the service should send column stats.",
    )


class ContentFile(IcebergBaseModel):
    content: str
    file_path: str = Field(..., alias="file-path")
    file_format: Literal["avro", "orc", "parquet", "puffin"] = Field(..., alias="file-format")
    spec_id: int = Field(..., alias="spec-id")
    partition: List[PrimitiveTypeValue] = Field(
        ...,
        description="A list of partition field values ordered based on the fields of the partition spec specified by the `spec-id`",
        json_schema_extra={"example": [1, "bar"]},
    )
    file_size_in_bytes: int = Field(..., alias="file-size-in-bytes", description="Total file size in bytes")
    record_count: int = Field(..., alias="record-count", description="Number of records in the file")
    key_metadata: Optional[BinaryTypeValue] = Field(None, alias="key-metadata", description="Encryption key metadata blob")
    split_offsets: Optional[List[int]] = Field(None, alias="split-offsets", description="List of splittable offsets")
    sort_order_id: Optional[int] = Field(None, alias="sort-order-id")


class PositionDeleteFile(ContentFile):
    content: Literal["position-deletes"] = "position-deletes"
    content_offset: Optional[int] = Field(
        None,
        alias="content-offset",
        description="Offset within the delete file of delete content",
    )
    content_size_in_bytes: Optional[int] = Field(
        None,
        alias="content-size-in-bytes",
        description="Length, in bytes, of the delete content; required if content-offset is present",
    )


class EqualityDeleteFile(ContentFile):
    content: Literal["equality-deletes"] = "equality-deletes"
    equality_ids: Optional[List[int]] = Field(None, alias="equality-ids", description="List of equality field IDs")


class DeleteFile(IcebergRootModel[Union[PositionDeleteFile, EqualityDeleteFile]]):
    root: Union[PositionDeleteFile, EqualityDeleteFile] = Field(..., discriminator="content")


class DataFile(ContentFile):
    content: Literal["data"] = "data"
    first_row_id: Optional[int] = Field(
        None,
        alias="first-row-id",
        description="The first row ID assigned to the first row in the data file",
    )
    column_sizes: Optional[CountMap] = Field(
        None,
        alias="column-sizes",
        description="Map of column id to total count, including null and NaN",
    )
    value_counts: Optional[CountMap] = Field(None, alias="value-counts", description="Map of column id to null value count")
    null_value_counts: Optional[CountMap] = Field(
        None,
        alias="null-value-counts",
        description="Map of column id to null value count",
    )
    nan_value_counts: Optional[CountMap] = Field(
        None,
        alias="nan-value-counts",
        description="Map of column id to number of NaN values in the column",
    )
    lower_bounds: Optional[ValueMap] = Field(
        None,
        alias="lower-bounds",
        description="Map of column id to lower bound primitive type values",
    )
    upper_bounds: Optional[ValueMap] = Field(
        None,
        alias="upper-bounds",
        description="Map of column id to upper bound primitive type values",
    )


class FileScanTask(IcebergBaseModel):
    data_file: DataFile = Field(..., alias="data-file")
    delete_file_references: Optional[List[int]] = Field(
        None,
        alias="delete-file-references",
        description="A list of indices in the delete files array (0-based)",
    )
    residual_filter: Optional[Expression] = Field(
        None,
        alias="residual-filter",
        description="An optional filter to be applied to rows in this file scan task.\nIf the residual is not present, the client must produce the residual or use the original filter.",
    )


class PlanTask(IcebergRootModel[str]):
    root: str = Field(
        ...,
        description="An opaque string provided by the REST server that represents a unit of work to produce file scan tasks for scan planning. This allows clients to fetch tasks across multiple requests to accommodate large result sets.",
    )


class ScanTasks(IcebergBaseModel):
    """
    Scan and planning tasks for server-side scan planning.

    - `plan-tasks` contains opaque units of planning work
    - `file-scan-tasks` contains a partial or complete list of table scan tasks
    - `delete-files` contains delete files referenced by file scan tasks

    Each plan task must be passed to the fetchScanTasks endpoint to fetch the file scan tasks for the plan task.

    The list of delete files must contain all delete files referenced by the file scan tasks.

    """

    delete_files: Optional[List[DeleteFile]] = Field(
        None,
        alias="delete-files",
        description="Delete files referenced by file scan tasks",
    )
    file_scan_tasks: Optional[List[FileScanTask]] = Field(None, alias="file-scan-tasks")
    plan_tasks: Optional[List[PlanTask]] = Field(None, alias="plan-tasks")


class FailedPlanningResult(IcebergErrorResponse):
    """Failed server-side planning result."""

    status: Literal["failed"]


class AsyncPlanningResult(IcebergBaseModel):
    status: Literal["submitted"]
    plan_id: str = Field(..., alias="plan-id", description="ID used to track a planning request")


class CancelledPlanningResult(IcebergBaseModel):
    """A cancelled planning result."""

    status: Literal["cancelled"]


class CompletedPlanningWithIDResult(ScanTasks):
    """Completed server-side planning result."""

    status: Literal["completed"]
    plan_id: Optional[str] = Field(None, alias="plan-id", description="ID used to track a planning request")


class PlanTableScanResult(
    IcebergRootModel[Union[CompletedPlanningWithIDResult, FailedPlanningResult, AsyncPlanningResult, CancelledPlanningResult]]
):
    """Result of server-side scan planning for planTableScan."""

    root: Union[CompletedPlanningWithIDResult, FailedPlanningResult, AsyncPlanningResult, CancelledPlanningResult] = Field(
        ..., discriminator="status"
    )
