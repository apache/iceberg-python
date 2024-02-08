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
Contains everything around the name mapping.

More information can be found on here:
https://iceberg.apache.org/spec/#name-mapping-serialization
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections import ChainMap
from functools import cached_property, singledispatch
from typing import Any, Dict, Generic, List, TypeVar, Union

from pydantic import Field, conlist, field_validator, model_serializer

from pyiceberg.schema import Schema, SchemaVisitor, visit
from pyiceberg.typedef import IcebergBaseModel, IcebergRootModel
from pyiceberg.types import ListType, MapType, NestedField, PrimitiveType, StructType


class MappedField(IcebergBaseModel):
    field_id: int = Field(alias="field-id")
    names: List[str] = conlist(str, min_length=1)
    fields: List[MappedField] = Field(default_factory=list)

    @field_validator('fields', mode='before')
    @classmethod
    def convert_null_to_empty_List(cls, v: Any) -> Any:
        return v or []

    @model_serializer
    def ser_model(self) -> Dict[str, Any]:
        """Set custom serializer to leave out the field when it is empty."""
        fields = {'fields': self.fields} if len(self.fields) > 0 else {}
        return {
            'field-id': self.field_id,
            'names': self.names,
            **fields,
        }

    def __len__(self) -> int:
        """Return the number of fields."""
        return len(self.fields)

    def __str__(self) -> str:
        """Convert the mapped-field into a nicely formatted string."""
        # Otherwise the UTs fail because the order of the set can change
        fields_str = ", ".join([str(e) for e in self.fields]) or ""
        fields_str = " " + fields_str if fields_str else ""
        return "([" + ", ".join(self.names) + "] -> " + (str(self.field_id) or "?") + fields_str + ")"


class NameMapping(IcebergRootModel[List[MappedField]]):
    root: List[MappedField]

    @cached_property
    def _field_by_name(self) -> Dict[str, MappedField]:
        return visit_name_mapping(self, _IndexByName())

    def find(self, *names: str) -> MappedField:
        name = '.'.join(names)
        try:
            return self._field_by_name[name]
        except KeyError as e:
            raise ValueError(f"Could not find field with name: {name}") from e

    def __len__(self) -> int:
        """Return the number of mappings."""
        return len(self.root)

    def __str__(self) -> str:
        """Convert the name-mapping into a nicely formatted string."""
        if len(self.root) == 0:
            return "[]"
        else:
            return "[\n  " + "\n  ".join([str(e) for e in self.root]) + "\n]"


T = TypeVar("T")


class NameMappingVisitor(Generic[T], ABC):
    @abstractmethod
    def mapping(self, nm: NameMapping, field_results: T) -> T:
        """Visit a NameMapping."""

    @abstractmethod
    def fields(self, struct: List[MappedField], field_results: List[T]) -> T:
        """Visit a List[MappedField]."""

    @abstractmethod
    def field(self, field: MappedField, field_result: T) -> T:
        """Visit a MappedField."""


class _IndexByName(NameMappingVisitor[Dict[str, MappedField]]):
    def mapping(self, nm: NameMapping, field_results: Dict[str, MappedField]) -> Dict[str, MappedField]:
        return field_results

    def fields(self, struct: List[MappedField], field_results: List[Dict[str, MappedField]]) -> Dict[str, MappedField]:
        return dict(ChainMap(*field_results))

    def field(self, field: MappedField, field_result: Dict[str, MappedField]) -> Dict[str, MappedField]:
        result: Dict[str, MappedField] = {
            f"{field_name}.{key}": result_field for key, result_field in field_result.items() for field_name in field.names
        }

        for name in field.names:
            result[name] = field

        return result


@singledispatch
def visit_name_mapping(obj: Union[NameMapping, List[MappedField], MappedField], visitor: NameMappingVisitor[T]) -> T:
    """Traverse the name mapping in post-order traversal."""
    raise NotImplementedError(f"Cannot visit non-type: {obj}")


@visit_name_mapping.register(NameMapping)
def _(obj: NameMapping, visitor: NameMappingVisitor[T]) -> T:
    return visitor.mapping(obj, visit_name_mapping(obj.root, visitor))


@visit_name_mapping.register(list)
def _(fields: List[MappedField], visitor: NameMappingVisitor[T]) -> T:
    results = [visitor.field(field, visit_name_mapping(field.fields, visitor)) for field in fields]
    return visitor.fields(fields, results)


def parse_mapping_from_json(mapping: str) -> NameMapping:
    return NameMapping.model_validate_json(mapping)


class _CreateMapping(SchemaVisitor[List[MappedField]]):
    def schema(self, schema: Schema, struct_result: List[MappedField]) -> List[MappedField]:
        return struct_result

    def struct(self, struct: StructType, field_results: List[List[MappedField]]) -> List[MappedField]:
        return [
            MappedField(field_id=field.field_id, names=[field.name], fields=result)
            for field, result in zip(struct.fields, field_results)
        ]

    def field(self, field: NestedField, field_result: List[MappedField]) -> List[MappedField]:
        return field_result

    def list(self, list_type: ListType, element_result: List[MappedField]) -> List[MappedField]:
        return [MappedField(field_id=list_type.element_id, names=["element"], fields=element_result)]

    def map(self, map_type: MapType, key_result: List[MappedField], value_result: List[MappedField]) -> List[MappedField]:
        return [
            MappedField(field_id=map_type.key_id, names=["key"], fields=key_result),
            MappedField(field_id=map_type.value_id, names=["value"], fields=value_result),
        ]

    def primitive(self, primitive: PrimitiveType) -> List[MappedField]:
        return []


def create_mapping_from_schema(schema: Schema) -> NameMapping:
    return NameMapping(visit(schema, _CreateMapping()))
