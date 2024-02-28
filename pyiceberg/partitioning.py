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
from __future__ import annotations

from abc import ABC, abstractmethod
from functools import cached_property, singledispatch
from typing import Any, Dict, Generic, List, Optional, Tuple, TypeVar

from pydantic import (
    BeforeValidator,
    Field,
    PlainSerializer,
    WithJsonSchema,
)
from typing_extensions import Annotated

from pyiceberg.schema import Schema
from pyiceberg.transforms import (
    BucketTransform,
    DayTransform,
    HourTransform,
    IdentityTransform,
    Transform,
    TruncateTransform,
    UnknownTransform,
    VoidTransform,
    YearTransform,
    parse_transform,
)
from pyiceberg.typedef import IcebergBaseModel
from pyiceberg.types import NestedField, StructType

INITIAL_PARTITION_SPEC_ID = 0
PARTITION_FIELD_ID_START: int = 1000


class PartitionField(IcebergBaseModel):
    """PartitionField represents how one partition value is derived from the source column via transformation.

    Attributes:
        source_id(int): The source column id of table's schema.
        field_id(int): The partition field id across all the table partition specs.
        transform(Transform): The transform used to produce partition values from source column.
        name(str): The name of this partition field.
    """

    source_id: int = Field(alias="source-id")
    field_id: int = Field(alias="field-id")
    transform: Annotated[  # type: ignore
        Transform,
        BeforeValidator(parse_transform),
        PlainSerializer(lambda c: str(c), return_type=str),  # pylint: disable=W0108
        WithJsonSchema({"type": "string"}, mode="serialization"),
    ] = Field()
    name: str = Field()

    def __init__(
        self,
        source_id: Optional[int] = None,
        field_id: Optional[int] = None,
        transform: Optional[Transform[Any, Any]] = None,
        name: Optional[str] = None,
        **data: Any,
    ):
        if source_id is not None:
            data["source-id"] = source_id
        if field_id is not None:
            data["field-id"] = field_id
        if transform is not None:
            data["transform"] = transform
        if name is not None:
            data["name"] = name

        super().__init__(**data)

    def __str__(self) -> str:
        """Return the string representation of the PartitionField class."""
        return f"{self.field_id}: {self.name}: {self.transform}({self.source_id})"


class PartitionSpec(IcebergBaseModel):
    """
    PartitionSpec captures the transformation from table data to partition values.

    Attributes:
        spec_id(int): any change to PartitionSpec will produce a new specId.
        fields(Tuple[PartitionField): list of partition fields to produce partition values.
    """

    spec_id: int = Field(alias="spec-id", default=INITIAL_PARTITION_SPEC_ID)
    fields: Tuple[PartitionField, ...] = Field(default_factory=tuple)

    def __init__(
        self,
        *fields: PartitionField,
        **data: Any,
    ):
        if fields:
            data["fields"] = tuple(fields)
        super().__init__(**data)

    def __eq__(self, other: Any) -> bool:
        """
        Produce a boolean to return True if two objects are considered equal.

        Note:
            Equality of PartitionSpec is determined by spec_id and partition fields only.
        """
        if not isinstance(other, PartitionSpec):
            return False
        return self.spec_id == other.spec_id and self.fields == other.fields

    def __str__(self) -> str:
        """
        Produce a human-readable string representation of PartitionSpec.

        Note:
            Only include list of partition fields in the PartitionSpec's string representation.
        """
        result_str = "["
        if self.fields:
            result_str += "\n  " + "\n  ".join([str(field) for field in self.fields]) + "\n"
        result_str += "]"
        return result_str

    def __repr__(self) -> str:
        """Return the string representation of the PartitionSpec class."""
        fields = f"{', '.join(repr(column) for column in self.fields)}, " if self.fields else ""
        return f"PartitionSpec({fields}spec_id={self.spec_id})"

    def is_unpartitioned(self) -> bool:
        return not self.fields

    @property
    def last_assigned_field_id(self) -> int:
        if self.fields:
            return max(pf.field_id for pf in self.fields)
        return PARTITION_FIELD_ID_START - 1

    @cached_property
    def source_id_to_fields_map(self) -> Dict[int, List[PartitionField]]:
        source_id_to_fields_map: Dict[int, List[PartitionField]] = {}
        for partition_field in self.fields:
            existing = source_id_to_fields_map.get(partition_field.source_id, [])
            existing.append(partition_field)
            source_id_to_fields_map[partition_field.source_id] = existing
        return source_id_to_fields_map

    def fields_by_source_id(self, field_id: int) -> List[PartitionField]:
        return self.source_id_to_fields_map.get(field_id, [])

    def compatible_with(self, other: PartitionSpec) -> bool:
        """Produce a boolean to return True if two PartitionSpec are considered compatible."""
        if self == other:
            return True
        if len(self.fields) != len(other.fields):
            return False
        return all(
            this_field.source_id == that_field.source_id
            and this_field.transform == that_field.transform
            and this_field.name == that_field.name
            for this_field, that_field in zip(self.fields, other.fields)
        )

    def partition_type(self, schema: Schema) -> StructType:
        """Produce a struct of the PartitionSpec.

        The partition fields should be optional:

        - All partition transforms are required to produce null if the input value is null, so it can
          happen when the source column is optional.
        - Partition fields may be added later, in which case not all files would have the result field,
          and it may be null.

        There is a case where we can guarantee that a partition field in the first and only partition spec
        that uses a required source column will never be null, but it doesn't seem worth tracking this case.

        :param schema: The schema to bind to.
        :return: A StructType that represents the PartitionSpec, with a NestedField for each PartitionField.
        """
        nested_fields = []
        for field in self.fields:
            source_type = schema.find_type(field.source_id)
            result_type = field.transform.result_type(source_type)
            nested_fields.append(NestedField(field.field_id, field.name, result_type, required=False))
        return StructType(*nested_fields)


UNPARTITIONED_PARTITION_SPEC = PartitionSpec(spec_id=0)


def assign_fresh_partition_spec_ids(spec: PartitionSpec, old_schema: Schema, fresh_schema: Schema) -> PartitionSpec:
    partition_fields = []
    for pos, field in enumerate(spec.fields):
        original_column_name = old_schema.find_column_name(field.source_id)
        if original_column_name is None:
            raise ValueError(f"Could not find in old schema: {field}")
        fresh_field = fresh_schema.find_field(original_column_name)
        if fresh_field is None:
            raise ValueError(f"Could not find field in fresh schema: {original_column_name}")
        partition_fields.append(
            PartitionField(
                name=field.name,
                source_id=fresh_field.field_id,
                field_id=PARTITION_FIELD_ID_START + pos,
                transform=field.transform,
            )
        )
    return PartitionSpec(*partition_fields, spec_id=INITIAL_PARTITION_SPEC_ID)


T = TypeVar("T")


class PartitionSpecVisitor(Generic[T], ABC):
    @abstractmethod
    def identity(self, field_id: int, source_name: str, source_id: int) -> T:
        """Visit identity partition field."""

    @abstractmethod
    def bucket(self, field_id: int, source_name: str, source_id: int, num_buckets: int) -> T:
        """Visit bucket partition field."""

    @abstractmethod
    def truncate(self, field_id: int, source_name: str, source_id: int, width: int) -> T:
        """Visit truncate partition field."""

    @abstractmethod
    def year(self, field_id: int, source_name: str, source_id: int) -> T:
        """Visit year partition field."""

    @abstractmethod
    def month(self, field_id: int, source_name: str, source_id: int) -> T:
        """Visit month partition field."""

    @abstractmethod
    def day(self, field_id: int, source_name: str, source_id: int) -> T:
        """Visit day partition field."""

    @abstractmethod
    def hour(self, field_id: int, source_name: str, source_id: int) -> T:
        """Visit hour partition field."""

    @abstractmethod
    def always_null(self, field_id: int, source_name: str, source_id: int) -> T:
        """Visit void partition field."""

    @abstractmethod
    def unknown(self, field_id: int, source_name: str, source_id: int, transform: str) -> T:
        """Visit unknown partition field."""
        raise ValueError(f"Unknown transform is not supported: {transform}")


class _PartitionNameGenerator(PartitionSpecVisitor[str]):
    def identity(self, field_id: int, source_name: str, source_id: int) -> str:
        return source_name

    def bucket(self, field_id: int, source_name: str, source_id: int, num_buckets: int) -> str:
        return f"{source_name}_bucket_{num_buckets}"

    def truncate(self, field_id: int, source_name: str, source_id: int, width: int) -> str:
        return source_name + "_trunc_" + str(width)

    def year(self, field_id: int, source_name: str, source_id: int) -> str:
        return source_name + "_year"

    def month(self, field_id: int, source_name: str, source_id: int) -> str:
        return source_name + "_month"

    def day(self, field_id: int, source_name: str, source_id: int) -> str:
        return source_name + "_day"

    def hour(self, field_id: int, source_name: str, source_id: int) -> str:
        return source_name + "_hour"

    def always_null(self, field_id: int, source_name: str, source_id: int) -> str:
        return source_name + "_null"

    def unknown(self, field_id: int, source_name: str, source_id: int, transform: str) -> str:
        return super().unknown(field_id, source_name, source_id, transform)


R = TypeVar("R")


@singledispatch
def _visit(spec: PartitionSpec, schema: Schema, visitor: PartitionSpecVisitor[R]) -> List[R]:
    return [_visit_partition_field(schema, field, visitor) for field in spec.fields]


def _visit_partition_field(schema: Schema, field: PartitionField, visitor: PartitionSpecVisitor[R]) -> R:
    source_name = schema.find_column_name(field.source_id)
    if not source_name:
        raise ValueError(f"Could not find field with id {field.source_id}")

    transform = field.transform
    if isinstance(transform, IdentityTransform):
        return visitor.identity(field.field_id, source_name, field.source_id)
    elif isinstance(transform, BucketTransform):
        return visitor.bucket(field.field_id, source_name, field.source_id, transform.num_buckets)
    elif isinstance(transform, TruncateTransform):
        return visitor.truncate(field.field_id, source_name, field.source_id, transform.width)
    elif isinstance(transform, DayTransform):
        return visitor.day(field.field_id, source_name, field.source_id)
    elif isinstance(transform, HourTransform):
        return visitor.hour(field.field_id, source_name, field.source_id)
    elif isinstance(transform, YearTransform):
        return visitor.year(field.field_id, source_name, field.source_id)
    elif isinstance(transform, VoidTransform):
        return visitor.always_null(field.field_id, source_name, field.source_id)
    elif isinstance(transform, UnknownTransform):
        return visitor.unknown(field.field_id, source_name, field.source_id, repr(transform))
    else:
        raise ValueError(f"Unknown transform {transform}")
