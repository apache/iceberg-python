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
"""Filesystem-only catalog compatible with the Java HadoopCatalog."""

from __future__ import annotations

import re
from typing import TYPE_CHECKING

import pyarrow as pa
from pyarrow.fs import FileSelector, FileSystem, FileType

from pyiceberg.catalog import (
    WAREHOUSE_LOCATION,
    Catalog,
    MetastoreCatalog,
    PropertiesUpdateSummary,
)
from pyiceberg.exceptions import (
    CommitFailedException,
    NamespaceAlreadyExistsError,
    NamespaceNotEmptyError,
    NoSuchNamespaceError,
    NoSuchTableError,
    NoSuchViewError,
    TableAlreadyExistsError,
)
from pyiceberg.io import FileIO
from pyiceberg.io.pyarrow import PyArrowFileIO
from pyiceberg.partitioning import UNPARTITIONED_PARTITION_SPEC, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.serializers import FromInputFile, ToOutputFile
from pyiceberg.table import CommitTableResponse, CreateTableTransaction, StagedTable, Table, TableProperties
from pyiceberg.table.metadata import TableMetadata, new_table_metadata
from pyiceberg.table.sorting import UNSORTED_SORT_ORDER, SortOrder
from pyiceberg.table.update import TableRequirement, TableUpdate
from pyiceberg.typedef import EMPTY_DICT, Identifier, Properties

if TYPE_CHECKING:
    from pyiceberg.view import View

VERSION_HINT_FILE = "version-hint.text"

_METADATA_FILE_PATTERN = re.compile(
    r"""
    ^v(\d+)\.metadata\.json$      # v1.metadata.json
    |
    ^(\d+)-.+\.metadata\.json$    # 00001-<uuid>.metadata.json
    """,
    re.X,
)


class HadoopCatalog(MetastoreCatalog):
    """A filesystem-only catalog that stores tables under a warehouse directory.

    Table metadata lives at ``<warehouse>/<namespace>/<table>/metadata`` and the current
    version is tracked in ``version-hint.text``. This mirrors the Java HadoopCatalog. Unlike
    the Java implementation, the version hint is only a hint: when it is missing or stale the
    latest metadata file is found by listing the metadata directory.

    Namespaces map to directories and properties are not stored, the same as the Java catalog.
    """

    _warehouse: str
    _io: FileIO

    def __init__(self, name: str, **properties: str) -> None:
        super().__init__(name, **properties)
        warehouse = properties.get(WAREHOUSE_LOCATION)
        if not warehouse:
            raise ValueError("Cannot initialize HadoopCatalog because warehouse must not be null or empty")
        self._warehouse = warehouse.rstrip("/")
        self._io = self._load_file_io(location=self._warehouse)

    @property
    def warehouse(self) -> str:
        """Return the warehouse location that holds every table in this catalog."""
        return self._warehouse

    def _table_location(self, identifier: Identifier) -> str:
        return "/".join((self._warehouse, *identifier))

    def _namespace_location(self, namespace: str | Identifier) -> str:
        levels = Catalog.identifier_to_tuple(namespace)
        return "/".join((self._warehouse, *levels)) if levels else self._warehouse

    def _fs_and_path(self, location: str) -> tuple[FileSystem, str]:
        if not isinstance(self._io, PyArrowFileIO):
            raise NotImplementedError("HadoopCatalog requires the PyArrowFileIO implementation for directory listing")
        scheme, netloc, path = self._io.parse_location(location, self.properties)
        return self._io.fs_by_scheme(scheme, netloc), path

    def _is_directory(self, location: str) -> bool:
        fs, path = self._fs_and_path(location)
        return fs.get_file_info(path).type == FileType.Directory

    def _list_entries(self, location: str) -> list[str]:
        fs, path = self._fs_and_path(location)
        infos = fs.get_file_info(FileSelector(path, recursive=False, allow_not_found=True))
        return [info.base_name for info in infos if info.type != FileType.NotFound]

    def _list_directories(self, location: str) -> list[str]:
        fs, path = self._fs_and_path(location)
        infos = fs.get_file_info(FileSelector(path, recursive=False, allow_not_found=True))
        return [info.base_name for info in infos if info.type == FileType.Directory]

    def _metadata_dir(self, location: str) -> str:
        return f"{location.rstrip('/')}/metadata"

    def _is_table_dir(self, location: str) -> bool:
        fs, path = self._fs_and_path(self._metadata_dir(location))
        infos = fs.get_file_info(FileSelector(path, recursive=False, allow_not_found=True))
        return any(info.type == FileType.File and info.base_name.endswith(".metadata.json") for info in infos)

    def _metadata_versions(self, location: str) -> list[tuple[int, str]]:
        metadata_dir = self._metadata_dir(location)
        fs, path = self._fs_and_path(metadata_dir)
        infos = fs.get_file_info(FileSelector(path, recursive=False, allow_not_found=True))
        versions = []
        for info in infos:
            if info.type != FileType.File:
                continue
            match = _METADATA_FILE_PATTERN.match(info.base_name)
            if match:
                versions.append((int(match.group(1) or match.group(2)), f"{metadata_dir}/{info.base_name}"))
        return versions

    def _latest_metadata_location(self, location: str) -> str | None:
        metadata_dir = self._metadata_dir(location)
        hint_location = f"{metadata_dir}/{VERSION_HINT_FILE}"

        if self._io.new_input(hint_location).exists():
            with self._io.new_input(hint_location).open() as stream:
                hint = stream.read().decode("utf-8").strip()
            if hint.endswith(".metadata.json"):
                hinted_location = f"{metadata_dir}/{hint}"
            elif hint.isdigit():
                hinted_location = f"{metadata_dir}/v{hint}.metadata.json"
            else:
                hinted_location = f"{metadata_dir}/{hint}.metadata.json"
            if self._io.new_input(hinted_location).exists():
                return hinted_location

        versions = self._metadata_versions(location)
        if not versions:
            return None
        _, latest = max(versions)
        return latest

    def _next_metadata_version(self, location: str) -> int:
        return max((version for version, _ in self._metadata_versions(location)), default=0) + 1

    def _write_metadata(self, metadata: TableMetadata, metadata_location: str, version: int) -> None:
        try:
            ToOutputFile.table_metadata(metadata, self._io.new_output(metadata_location), overwrite=False)
        except FileExistsError as e:
            raise CommitFailedException(f"Metadata file already exists: {metadata_location}") from e

        hint_location = f"{metadata_location.rsplit('/', 1)[0]}/{VERSION_HINT_FILE}"
        with self._io.new_output(hint_location).create(overwrite=True) as stream:
            stream.write(str(version).encode("utf-8"))

    def _parse_identifier(self, identifier: str | Identifier, err: type[Exception] = ValueError) -> Identifier:
        levels = Catalog.identifier_to_tuple(identifier)
        if len(levels) < 2:
            raise err(f"Invalid table identifier: {identifier}")
        return tuple(level.strip() for level in levels)

    def _new_table_metadata(
        self,
        identifier: Identifier,
        schema: Schema | pa.Schema,
        location: str | None,
        partition_spec: PartitionSpec,
        sort_order: SortOrder,
        properties: Properties,
    ) -> TableMetadata:
        table_location = self._table_location(identifier)
        if location is not None and location.rstrip("/") != table_location:
            raise ValueError(f"Cannot set a custom location for a path-based table, expected {table_location}")
        schema = self._convert_schema_if_needed(
            schema, int(properties.get(TableProperties.FORMAT_VERSION, TableProperties.DEFAULT_FORMAT_VERSION))
        )
        return new_table_metadata(
            location=table_location,
            schema=schema,
            partition_spec=partition_spec,
            sort_order=sort_order,
            properties=properties,
        )

    def create_table(
        self,
        identifier: str | Identifier,
        schema: Schema | pa.Schema,
        location: str | None = None,
        partition_spec: PartitionSpec = UNPARTITIONED_PARTITION_SPEC,
        sort_order: SortOrder = UNSORTED_SORT_ORDER,
        properties: Properties = EMPTY_DICT,
    ) -> Table:
        table_identifier = self._parse_identifier(identifier)
        if self._latest_metadata_location(self._table_location(table_identifier)) is not None:
            raise TableAlreadyExistsError(f"Table {identifier} already exists")

        metadata = self._new_table_metadata(table_identifier, schema, location, partition_spec, sort_order, properties)
        self._write_metadata(metadata, f"{self._metadata_dir(metadata.location)}/v1.metadata.json", 1)
        return self.load_table(identifier)

    def create_table_transaction(
        self,
        identifier: str | Identifier,
        schema: Schema | pa.Schema,
        location: str | None = None,
        partition_spec: PartitionSpec = UNPARTITIONED_PARTITION_SPEC,
        sort_order: SortOrder = UNSORTED_SORT_ORDER,
        properties: Properties = EMPTY_DICT,
    ) -> CreateTableTransaction:
        table_identifier = self._parse_identifier(identifier)
        metadata = self._new_table_metadata(table_identifier, schema, location, partition_spec, sort_order, properties)
        metadata_location = f"{self._metadata_dir(metadata.location)}/v1.metadata.json"
        return CreateTableTransaction(
            StagedTable(
                identifier=table_identifier,
                metadata=metadata,
                metadata_location=metadata_location,
                io=self._load_file_io(location=metadata_location, table_properties=metadata.properties),
                catalog=self,
            )
        )

    def load_table(self, identifier: str | Identifier) -> Table:
        table_identifier = self._parse_identifier(identifier, err=NoSuchTableError)
        location = self._table_location(table_identifier)
        metadata_location = self._latest_metadata_location(location)
        if metadata_location is None:
            raise NoSuchTableError(f"Table does not exist: {identifier}")

        metadata = FromInputFile.table_metadata(self._io.new_input(metadata_location))
        return Table(
            identifier=table_identifier,
            metadata=metadata,
            metadata_location=metadata_location,
            io=self._load_file_io(location=metadata_location, table_properties=metadata.properties),
            catalog=self,
        )

    def commit_table(
        self, table: Table, requirements: tuple[TableRequirement, ...], updates: tuple[TableUpdate, ...]
    ) -> CommitTableResponse:
        current_table: Table | None
        try:
            current_table = self.load_table(table.name())
        except NoSuchTableError:
            current_table = None

        staged = self._update_and_stage_table(current_table, table.name(), requirements, updates)
        if current_table is not None and staged.metadata == current_table.metadata:
            return CommitTableResponse(metadata=current_table.metadata, metadata_location=current_table.metadata_location)

        version = self._next_metadata_version(staged.metadata.location)
        metadata_location = f"{self._metadata_dir(staged.metadata.location)}/v{version}.metadata.json"
        self._write_metadata(staged.metadata, metadata_location, version)
        return CommitTableResponse(metadata=staged.metadata, metadata_location=metadata_location)

    def register_table(self, identifier: str | Identifier, metadata_location: str) -> Table:
        raise NotImplementedError("Cannot register tables in HadoopCatalog")

    def drop_table(self, identifier: str | Identifier) -> None:
        table = self.load_table(identifier)
        fs, path = self._fs_and_path(table.metadata.location)
        fs.delete_dir(path)

    def rename_table(self, from_identifier: str | Identifier, to_identifier: str | Identifier) -> Table:
        raise NotImplementedError("Cannot rename Hadoop tables")

    def create_namespace(self, namespace: str | Identifier, properties: Properties = EMPTY_DICT) -> None:
        levels = Catalog.identifier_to_tuple(namespace)
        if not levels:
            raise ValueError("Cannot create namespace with an empty name")
        if properties:
            raise NotImplementedError(f"Cannot create namespace {namespace}: metadata is not supported")

        location = self._namespace_location(namespace)
        if self._is_directory(location):
            raise NamespaceAlreadyExistsError(f"Namespace already exists: {namespace}")

        fs, path = self._fs_and_path(location)
        fs.create_dir(path, recursive=True)

    def drop_namespace(self, namespace: str | Identifier) -> None:
        levels = Catalog.identifier_to_tuple(namespace)
        location = self._namespace_location(namespace)
        if not levels or not self._is_directory(location):
            raise NoSuchNamespaceError(f"Namespace does not exist: {namespace}")
        if self._list_entries(location):
            raise NamespaceNotEmptyError(f"Namespace {namespace} is not empty")

        fs, path = self._fs_and_path(location)
        fs.delete_dir(path)

    def list_tables(self, namespace: str | Identifier) -> list[Identifier]:
        levels = Catalog.identifier_to_tuple(namespace)
        if not levels:
            raise ValueError(f"Missing namespace in table identifier: {namespace}")

        location = self._namespace_location(namespace)
        if not self._is_directory(location):
            raise NoSuchNamespaceError(f"Namespace does not exist: {namespace}")

        return [(*levels, name) for name in sorted(self._list_directories(location)) if self._is_table_dir(f"{location}/{name}")]

    def list_namespaces(self, namespace: str | Identifier = ()) -> list[Identifier]:
        levels = Catalog.identifier_to_tuple(namespace)
        location = self._namespace_location(namespace)
        if not self._is_directory(location):
            raise NoSuchNamespaceError(f"Namespace does not exist: {namespace}")

        return [
            (*levels, name) for name in sorted(self._list_directories(location)) if not self._is_table_dir(f"{location}/{name}")
        ]

    def load_namespace_properties(self, namespace: str | Identifier) -> Properties:
        levels = Catalog.identifier_to_tuple(namespace)
        location = self._namespace_location(namespace)
        if not levels or not self._is_directory(location):
            raise NoSuchNamespaceError(f"Namespace does not exist: {namespace}")
        if self._is_table_dir(location):
            raise NoSuchNamespaceError(f"Namespace does not exist: {namespace}")
        return {"location": location}

    def update_namespace_properties(
        self, namespace: str | Identifier, removals: set[str] | None = None, updates: Properties = EMPTY_DICT
    ) -> PropertiesUpdateSummary:
        raise NotImplementedError(f"Cannot set namespace properties {namespace}: not supported")

    def view_exists(self, identifier: str | Identifier) -> bool:
        return False

    def list_views(self, namespace: str | Identifier) -> list[Identifier]:
        return []

    def load_view(self, identifier: str | Identifier) -> View:
        raise NoSuchViewError(f"View does not exist: {identifier}")

    def register_view(self, identifier: str | Identifier, metadata_location: str) -> View:
        raise NotImplementedError("Views are not supported by HadoopCatalog")

    def drop_view(self, identifier: str | Identifier) -> None:
        raise NotImplementedError("Views are not supported by HadoopCatalog")

    def rename_view(self, from_identifier: str | Identifier, to_identifier: str | Identifier) -> None:
        raise NotImplementedError("Views are not supported by HadoopCatalog")
