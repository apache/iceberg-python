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
"""Metadata columns that are not part of a table schema, but can be selected in a scan."""

from __future__ import annotations

from collections.abc import Mapping
from types import MappingProxyType

from pyiceberg.schema import Schema
from pyiceberg.types import LongType, NestedField

ROW_ID = NestedField(
    field_id=2147483540,
    name="_row_id",
    field_type=LongType(),
    required=False,
    doc="Implicit row ID that is automatically assigned",
)
LAST_UPDATED_SEQUENCE_NUMBER = NestedField(
    field_id=2147483539,
    name="_last_updated_sequence_number",
    field_type=LongType(),
    required=False,
    doc="Sequence number when the row was last updated",
)

METADATA_COLUMNS: Mapping[str, NestedField] = MappingProxyType(
    {field.name: field for field in (ROW_ID, LAST_UPDATED_SEQUENCE_NUMBER)}
)


def _find_metadata_column(name: str, case_sensitive: bool) -> NestedField | None:
    if case_sensitive:
        return METADATA_COLUMNS.get(name)
    return next((field for field in METADATA_COLUMNS.values() if field.name.lower() == name.lower()), None)


def _has_field(schema: Schema, name: str, case_sensitive: bool) -> bool:
    try:
        schema.find_field(name, case_sensitive=case_sensitive)
        return True
    except ValueError:
        return False


def split_selected_fields(
    schema: Schema, selected_fields: tuple[str, ...], case_sensitive: bool
) -> tuple[tuple[str, ...], tuple[NestedField, ...]]:
    """Split selected names into schema columns and metadata columns.

    A name that resolves in the schema is a schema column, even when it matches a metadata column name.
    """
    schema_names: list[str] = []
    metadata_fields: list[NestedField] = []
    for name in selected_fields:
        if name != "*" and not _has_field(schema, name, case_sensitive):
            if (field := _find_metadata_column(name, case_sensitive)) is not None:
                if field not in metadata_fields:
                    metadata_fields.append(field)
                continue
        schema_names.append(name)
    return tuple(schema_names), tuple(metadata_fields)


def project_schema(schema: Schema, selected_fields: tuple[str, ...], case_sensitive: bool, format_version: int) -> Schema:
    """Prune the schema to the selected names, and append any selected metadata columns."""
    schema_names, metadata_fields = split_selected_fields(schema, selected_fields, case_sensitive)

    if metadata_fields and format_version < 3:
        names = ", ".join(field.name for field in metadata_fields)
        raise ValueError(
            f"Cannot select {names}: row lineage requires format version 3, but the table has format version {format_version}"
        )

    projected = schema if "*" in schema_names else schema.select(*schema_names, case_sensitive=case_sensitive)
    if not metadata_fields:
        return projected

    return Schema(
        *projected.fields,
        *metadata_fields,
        schema_id=projected.schema_id,
        identifier_field_ids=projected.identifier_field_ids,
    )
