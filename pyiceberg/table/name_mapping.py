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
from pyiceberg.types import IcebergType, ListType, MapType, NestedField, PrimitiveType, StructType

LIST_ELEMENT_NAME = "element"
MAP_KEY_NAME = "key"
MAP_VALUE_NAME = "value"


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


def apply_name_mapping(schema_or_type: Union[Schema, IcebergType], name_mapping: NameMapping) -> Schema:
    """Traverses the schema, and sets new IDs."""
    return visit(schema_or_type, _ApplyNameMapping(name_mapping=name_mapping))


class _ApplyNameMapping(SchemaVisitor[IcebergType]):
    """Traverses the schema and applies the IDs from provided name_mapping."""

    field_names: List[str]

    def __init__(self, name_mapping: NameMapping) -> None:
        self.field_names = []
        self.name_mapping = name_mapping

    def _current_path(self) -> str:
        return ".".join(self.field_names)

    def _path(self, name: str) -> str:
        return ".".join(self.field_names + [name])

    def schema(self, schema: Schema, struct_result: StructType) -> Schema:
        return Schema(*struct_result.fields)

    def struct(self, struct: StructType, field_results: List[IcebergType]) -> StructType:
        return StructType(*field_results)

    def field(self, field: NestedField, field_result: IcebergType | None) -> NestedField:
        return NestedField(
            field_id=self.name_mapping.id(self._path(field.name)),
            name=field.name,
            field_type=field_result,
            required=field.required,
            doc=field.doc,
        )

    def list(self, list_type: ListType, element_result: IcebergType | None) -> ListType:
        return ListType(
            element_id=self.name_mapping.id(self._current_path() + "." + LIST_ELEMENT_NAME),
            element=element_result,
            element_required=list_type.element_required,
        )

    def map(self, map_type: MapType, key_result: IcebergType | None, value_result: IcebergType | None) -> MapType:
        return MapType(
            key_id=self.name_mapping.id(self._current_path() + "." + MAP_KEY_NAME),
            key_type=key_result,
            value_id=self.name_mapping.id(self._current_path() + "." + MAP_VALUE_NAME),
            value_type=value_result,
            value_required=map_type.value_required,
        )

    def primitive(self, primitive: PrimitiveType) -> PrimitiveType:
        return primitive

    def before_field(self, field: NestedField) -> None:
        self.field_names.append(field.name)

    def after_field(self, field: NestedField) -> None:
        self.field_names.pop()

    def before_list_element(self, element: NestedField) -> None:
        self.field_names.append(LIST_ELEMENT_NAME)

    def before_map_key(self, key: NestedField) -> None:
        self.field_names.append(MAP_KEY_NAME)

    def before_map_value(self, value: NestedField) -> None:
        self.field_names.append(MAP_VALUE_NAME)
