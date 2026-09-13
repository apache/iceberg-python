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

from typing import Any
from uuid import UUID

from pyiceberg.schema import Schema
from pyiceberg.typedef import Identifier
from pyiceberg.view.metadata import SQLViewRepresentation, ViewHistoryEntry, ViewMetadata, ViewVersion

__all__ = [
    "View",
    "ViewMetadata",
    "ViewVersion",
    "ViewHistoryEntry",
    "SQLViewRepresentation",
]


class View:
    """An Iceberg view."""

    _identifier: Identifier
    metadata: ViewMetadata

    def __init__(
        self,
        identifier: Identifier,
        metadata: ViewMetadata,
    ) -> None:
        self._identifier = identifier
        self.metadata = metadata

    def name(self) -> Identifier:
        """Return the identifier of this view."""
        return self._identifier

    def schema(self) -> Schema:
        """Return the schema for this view."""
        return next(schema for schema in self.metadata.schemas if schema.schema_id == self.current_version().schema_id)

    def schemas(self) -> dict[int, Schema]:
        """Return the schemas for this view."""
        return {schema.schema_id: schema for schema in self.metadata.schemas}

    def current_version(self) -> ViewVersion:
        """Get the version of this view."""
        return next(version for version in self.metadata.versions if version.version_id == self.metadata.current_version_id)

    @property
    def versions(self) -> list[ViewVersion]:
        """Get the versions of this view."""
        return self.metadata.versions

    def version(self, version_id: int) -> ViewVersion | None:
        """Get the version in this view by ID, or None if the ID cannot be found."""
        return next((version for version in self.metadata.versions if version.version_id == version_id), None)

    def history(self) -> list[ViewHistoryEntry]:
        """Get the version of this history view."""
        return self.metadata.version_log

    @property
    def properties(self) -> dict[str, str]:
        """Return a map of string properties for this view."""
        return self.metadata.properties

    def location(self) -> str:
        """Return the view's base location."""
        return self.metadata.location

    def uuid(self) -> UUID:
        """Return the view's UUID."""
        return UUID(self.metadata.view_uuid)

    def sql_for(self, dialect: str) -> SQLViewRepresentation | None:
        """Return the view representation for the sql dialect, or None if no representation could be resolved."""
        return next(
            (repr.root for repr in self.current_version().representations if repr.root.dialect.casefold() == dialect.casefold()),
            None,
        )

    def __eq__(self, other: Any) -> bool:
        """Return the equality of two instances of the View class."""
        return self.name() == other.name() and self.metadata == other.metadata if isinstance(other, View) else False
