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

from typing import Dict, List, Literal, Optional

from pydantic import Field, field_validator

from pyiceberg.schema import Schema
from pyiceberg.typedef import IcebergBaseModel, Properties
from pyiceberg.types import transform_dict_value_to_str


class SQLViewRepresentation(IcebergBaseModel):
    """Represents the SQL query that defines the view."""

    type: Literal["sql"] = Field()
    """A string that indicates the type of representation. Must be `sql`"""
    sql: str = Field()
    """A string that contains the SQL text of the view definition."""
    dialect: str = Field()
    """The dialect of the SQL, e.g. `spark`, `trino`, `presto`."""


class ViewVersion(IcebergBaseModel):
    """A version of the view definition."""

    version_id: int = Field(alias="version-id")
    """ID for the version"""
    schema_id: int = Field(alias="schema-id")
    """ID of the schema for the view version"""
    timestamp_ms: int = Field(alias="timestamp-ms")
    """Timestamp when the version was created (ms from epoch)"""
    summary: Dict[str, str] = Field()
    """A string to string map of summary metadata about the version"""
    representations: List[SQLViewRepresentation] = Field()
    """A list of representations for the view definition"""
    default_catalog: Optional[str] = Field(alias="default-catalog", default=None)
    """Catalog name to use when a reference in the SELECT does not contain a catalog"""
    default_namespace: Namespace = Field(alias="default-namespace")
    """Namespace to use when a reference in the SELECT is a single identifier"""


class ViewVersionLogEntry(IcebergBaseModel):
    """A log entry of a view version change."""

    timestamp_ms: int = Field(alias="timestamp-ms")
    """Timestamp when the version was created (ms from epoch)"""
    version_id: int = Field(alias="version-id")
    """ID for the version"""


class ViewMetadata(IcebergBaseModel):
    """The metadata for a view."""

    view_uuid: str = Field(alias="view-uuid")
    """A UUID that identifies the view, generated when the view is created."""
    format_version: int = Field(alias='format-version', ge=1, le=1)
    """An integer version number for the view format; must be 1"""
    location: str = Field()
    """The view's base location; used to create metadata file locations"""
    schemas: List[Schema] = Field()
    """A list of known schemas"""
    current_version_id: int = Field(alias="current-version-id")
    """ID of the current version of the view (version-id)"""
    versions: List[ViewVersion] = Field()
    """A list of known versions of the view"""
    version_log: List[ViewVersionLogEntry] = Field(alias="version-log")
    """A list of version log entries"""
    properties: Properties = Field(default_factory=dict)
    """A string to string map of view properties"""

    @field_validator("properties", mode="before")
    def transform_properties_dict_value_to_str(cls, properties: Properties) -> Dict[str, str]:
        return transform_dict_value_to_str(properties)
