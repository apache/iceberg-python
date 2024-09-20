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

import itertools
import uuid
import warnings
from abc import ABC, abstractmethod
from dataclasses import dataclass
from functools import cached_property
from itertools import chain
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    Optional,
    Set,
    Tuple,
    TypeVar,
    Union,
)

from pydantic import Field
from sortedcontainers import SortedList

import pyiceberg.expressions.parser as parser
from pyiceberg.expressions import (
    AlwaysTrue,
    And,
    BooleanExpression,
    EqualTo,
)
from pyiceberg.expressions.visitors import (
    _InclusiveMetricsEvaluator,
    bind,
    expression_evaluator,
    inclusive_projection,
    manifest_evaluator,
)
from pyiceberg.io import FileIO, load_file_io
from pyiceberg.manifest import (
    POSITIONAL_DELETE_SCHEMA,
    DataFile,
    DataFileContent,
    ManifestContent,
    ManifestEntry,
    ManifestFile,
)
from pyiceberg.partitioning import (
    PARTITION_FIELD_ID_START,
    UNPARTITIONED_PARTITION_SPEC,
    PartitionKey,
    PartitionSpec,
)
from pyiceberg.schema import Schema
from pyiceberg.table.inspect import InspectTable
from pyiceberg.table.metadata import (
    INITIAL_SEQUENCE_NUMBER,
    TableMetadata,
)
from pyiceberg.table.name_mapping import (
    NameMapping,
)
from pyiceberg.table.refs import SnapshotRef
from pyiceberg.table.snapshots import (
    Snapshot,
    SnapshotLogEntry,
)
from pyiceberg.table.sorting import UNSORTED_SORT_ORDER, SortOrder
from pyiceberg.table.update import (
    AddPartitionSpecUpdate,
    AddSchemaUpdate,
    AddSnapshotUpdate,
    AddSortOrderUpdate,
    AssertCreate,
    AssertRefSnapshotId,
    AssertTableUUID,
    AssignUUIDUpdate,
    RemovePropertiesUpdate,
    SetCurrentSchemaUpdate,
    SetDefaultSortOrderUpdate,
    SetDefaultSpecUpdate,
    SetLocationUpdate,
    SetPropertiesUpdate,
    SetSnapshotRefUpdate,
    TableRequirement,
    TableUpdate,
    UpdatesAndRequirements,
    UpgradeFormatVersionUpdate,
    update_table_metadata,
)
from pyiceberg.table.update.schema import UpdateSchema, _Move, _MoveOperation
from pyiceberg.table.update.snapshot import (
    ManageSnapshots,
    UpdateSnapshot,
    _DeleteFiles,
    _FastAppendFiles,
    _MergeAppendFiles,
    _OverwriteFiles,
)
from pyiceberg.table.update.spec import UpdateSpec
from pyiceberg.typedef import (
    EMPTY_DICT,
    IcebergBaseModel,
    IcebergRootModel,
    Identifier,
    KeyDefaultDict,
    Properties,
    Record,
    TableVersion,
)
from pyiceberg.types import (
    strtobool,
)
from pyiceberg.utils.concurrent import ExecutorFactory
from pyiceberg.utils.config import Config
from pyiceberg.utils.deprecated import deprecated, deprecation_message
from pyiceberg.utils.properties import property_as_bool

if TYPE_CHECKING:
    import daft
    import pandas as pd
    import pyarrow as pa
    import ray
    from duckdb import DuckDBPyConnection

    from pyiceberg.catalog import Catalog

ALWAYS_TRUE = AlwaysTrue()
DOWNCAST_NS_TIMESTAMP_TO_US_ON_WRITE = "downcast-ns-timestamp-to-us-on-write"


class TableProperties:
    PARQUET_ROW_GROUP_SIZE_BYTES = "write.parquet.row-group-size-bytes"
    PARQUET_ROW_GROUP_SIZE_BYTES_DEFAULT = 128 * 1024 * 1024  # 128 MB

    PARQUET_ROW_GROUP_LIMIT = "write.parquet.row-group-limit"
    PARQUET_ROW_GROUP_LIMIT_DEFAULT = 1048576

    PARQUET_PAGE_SIZE_BYTES = "write.parquet.page-size-bytes"
    PARQUET_PAGE_SIZE_BYTES_DEFAULT = 1024 * 1024  # 1 MB

    PARQUET_PAGE_ROW_LIMIT = "write.parquet.page-row-limit"
    PARQUET_PAGE_ROW_LIMIT_DEFAULT = 20000

    PARQUET_DICT_SIZE_BYTES = "write.parquet.dict-size-bytes"
    PARQUET_DICT_SIZE_BYTES_DEFAULT = 2 * 1024 * 1024  # 2 MB

    PARQUET_COMPRESSION = "write.parquet.compression-codec"
    PARQUET_COMPRESSION_DEFAULT = "zstd"

    PARQUET_COMPRESSION_LEVEL = "write.parquet.compression-level"
    PARQUET_COMPRESSION_LEVEL_DEFAULT = None

    PARQUET_BLOOM_FILTER_MAX_BYTES = "write.parquet.bloom-filter-max-bytes"
    PARQUET_BLOOM_FILTER_MAX_BYTES_DEFAULT = 1024 * 1024

    PARQUET_BLOOM_FILTER_COLUMN_ENABLED_PREFIX = "write.parquet.bloom-filter-enabled.column"

    WRITE_TARGET_FILE_SIZE_BYTES = "write.target-file-size-bytes"
    WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT = 512 * 1024 * 1024  # 512 MB

    DEFAULT_WRITE_METRICS_MODE = "write.metadata.metrics.default"
    DEFAULT_WRITE_METRICS_MODE_DEFAULT = "truncate(16)"

    METRICS_MODE_COLUMN_CONF_PREFIX = "write.metadata.metrics.column"

    WRITE_PARTITION_SUMMARY_LIMIT = "write.summary.partition-limit"
    WRITE_PARTITION_SUMMARY_LIMIT_DEFAULT = 0

    DELETE_MODE = "write.delete.mode"
    DELETE_MODE_COPY_ON_WRITE = "copy-on-write"
    DELETE_MODE_MERGE_ON_READ = "merge-on-read"
    DELETE_MODE_DEFAULT = DELETE_MODE_COPY_ON_WRITE

    DEFAULT_NAME_MAPPING = "schema.name-mapping.default"
    FORMAT_VERSION = "format-version"
    DEFAULT_FORMAT_VERSION = 2

    MANIFEST_TARGET_SIZE_BYTES = "commit.manifest.target-size-bytes"
    MANIFEST_TARGET_SIZE_BYTES_DEFAULT = 8 * 1024 * 1024  # 8 MB

    MANIFEST_MIN_MERGE_COUNT = "commit.manifest.min-count-to-merge"
    MANIFEST_MIN_MERGE_COUNT_DEFAULT = 100

    MANIFEST_MERGE_ENABLED = "commit.manifest-merge.enabled"
    MANIFEST_MERGE_ENABLED_DEFAULT = False

    METADATA_PREVIOUS_VERSIONS_MAX = "write.metadata.previous-versions-max"
    METADATA_PREVIOUS_VERSIONS_MAX_DEFAULT = 100


class Transaction:
    _table: Table
    table_metadata: TableMetadata
    _autocommit: bool
    _updates: Tuple[TableUpdate, ...]
    _requirements: Tuple[TableRequirement, ...]

    def __init__(self, table: Table, autocommit: bool = False):
        """Open a transaction to stage and commit changes to a table.

        Args:
            table: The table that will be altered.
            autocommit: Option to automatically commit the changes when they are staged.
        """
        self.table_metadata = table.metadata
        self._table = table
        self._autocommit = autocommit
        self._updates = ()
        self._requirements = ()

    def __enter__(self) -> Transaction:
        """Start a transaction to update the table."""
        return self

    def __exit__(self, _: Any, value: Any, traceback: Any) -> None:
        """Close and commit the transaction."""
        self.commit_transaction()

    def _apply(self, updates: Tuple[TableUpdate, ...], requirements: Tuple[TableRequirement, ...] = ()) -> Transaction:
        """Check if the requirements are met, and applies the updates to the metadata."""
        for requirement in requirements:
            requirement.validate(self.table_metadata)

        self._updates += updates

        # For the requirements, it does not make sense to add a requirement more than once
        # For example, you cannot assert that the current schema has two different IDs
        existing_requirements = {type(requirement) for requirement in self._requirements}
        for new_requirement in requirements:
            if type(new_requirement) not in existing_requirements:
                self._requirements = self._requirements + requirements

        self.table_metadata = update_table_metadata(self.table_metadata, updates)

        if self._autocommit:
            self.commit_transaction()
            self._updates = ()
            self._requirements = ()

        return self

    def _scan(self, row_filter: Union[str, BooleanExpression] = ALWAYS_TRUE) -> DataScan:
        """Minimal data scan of the table with the current state of the transaction."""
        return DataScan(
            table_metadata=self.table_metadata,
            io=self._table.io,
            row_filter=row_filter,
        )

    def upgrade_table_version(self, format_version: TableVersion) -> Transaction:
        """Set the table to a certain version.

        Args:
            format_version: The newly set version.

        Returns:
            The alter table builder.
        """
        if format_version not in {1, 2}:
            raise ValueError(f"Unsupported table format version: {format_version}")

        if format_version < self.table_metadata.format_version:
            raise ValueError(f"Cannot downgrade v{self.table_metadata.format_version} table to v{format_version}")

        if format_version > self.table_metadata.format_version:
            return self._apply((UpgradeFormatVersionUpdate(format_version=format_version),))

        return self

    def set_properties(self, properties: Properties = EMPTY_DICT, **kwargs: Any) -> Transaction:
        """Set properties.

        When a property is already set, it will be overwritten.

        Args:
            properties: The properties set on the table.
            kwargs: properties can also be pass as kwargs.

        Returns:
            The alter table builder.
        """
        if properties and kwargs:
            raise ValueError("Cannot pass both properties and kwargs")
        updates = properties or kwargs
        return self._apply((SetPropertiesUpdate(updates=updates),))

    @deprecated(
        deprecated_in="0.7.0",
        removed_in="0.8.0",
        help_message="Please use one of the functions in ManageSnapshots instead",
    )
    def add_snapshot(self, snapshot: Snapshot) -> Transaction:
        """Add a new snapshot to the table.

        Returns:
            The transaction with the add-snapshot staged.
        """
        updates = (AddSnapshotUpdate(snapshot=snapshot),)

        return self._apply(updates, ())

    @deprecated(
        deprecated_in="0.7.0",
        removed_in="0.8.0",
        help_message="Please use one of the functions in ManageSnapshots instead",
    )
    def set_ref_snapshot(
        self,
        snapshot_id: int,
        parent_snapshot_id: Optional[int],
        ref_name: str,
        type: str,
        max_ref_age_ms: Optional[int] = None,
        max_snapshot_age_ms: Optional[int] = None,
        min_snapshots_to_keep: Optional[int] = None,
    ) -> Transaction:
        """Update a ref to a snapshot.

        Returns:
            The transaction with the set-snapshot-ref staged
        """
        updates = (
            SetSnapshotRefUpdate(
                snapshot_id=snapshot_id,
                ref_name=ref_name,
                type=type,
                max_ref_age_ms=max_ref_age_ms,
                max_snapshot_age_ms=max_snapshot_age_ms,
                min_snapshots_to_keep=min_snapshots_to_keep,
            ),
        )

        requirements = (AssertRefSnapshotId(snapshot_id=parent_snapshot_id, ref="main"),)
        return self._apply(updates, requirements)

    def _set_ref_snapshot(
        self,
        snapshot_id: int,
        ref_name: str,
        type: str,
        max_ref_age_ms: Optional[int] = None,
        max_snapshot_age_ms: Optional[int] = None,
        min_snapshots_to_keep: Optional[int] = None,
    ) -> UpdatesAndRequirements:
        """Update a ref to a snapshot.

        Returns:
            The updates and requirements for the set-snapshot-ref staged
        """
        updates = (
            SetSnapshotRefUpdate(
                snapshot_id=snapshot_id,
                ref_name=ref_name,
                type=type,
                max_ref_age_ms=max_ref_age_ms,
                max_snapshot_age_ms=max_snapshot_age_ms,
                min_snapshots_to_keep=min_snapshots_to_keep,
            ),
        )
        requirements = (
            AssertRefSnapshotId(
                snapshot_id=self.table_metadata.refs[ref_name].snapshot_id if ref_name in self.table_metadata.refs else None,
                ref=ref_name,
            ),
        )

        return updates, requirements

    def update_schema(self, allow_incompatible_changes: bool = False, case_sensitive: bool = True) -> UpdateSchema:
        """Create a new UpdateSchema to alter the columns of this table.

        Args:
            allow_incompatible_changes: If changes are allowed that might break downstream consumers.
            case_sensitive: If field names are case-sensitive.

        Returns:
            A new UpdateSchema.
        """
        return UpdateSchema(
            self,
            allow_incompatible_changes=allow_incompatible_changes,
            case_sensitive=case_sensitive,
            name_mapping=self.table_metadata.name_mapping(),
        )

    def update_snapshot(self, snapshot_properties: Dict[str, str] = EMPTY_DICT) -> UpdateSnapshot:
        """Create a new UpdateSnapshot to produce a new snapshot for the table.

        Returns:
            A new UpdateSnapshot
        """
        return UpdateSnapshot(self, io=self._table.io, snapshot_properties=snapshot_properties)

    def append(self, df: pa.Table, snapshot_properties: Dict[str, str] = EMPTY_DICT) -> None:
        """
        Shorthand API for appending a PyArrow table to a table transaction.

        Args:
            df: The Arrow dataframe that will be appended to overwrite the table
            snapshot_properties: Custom properties to be added to the snapshot summary
        """
        try:
            import pyarrow as pa
        except ModuleNotFoundError as e:
            raise ModuleNotFoundError("For writes PyArrow needs to be installed") from e

        from pyiceberg.io.pyarrow import _check_pyarrow_schema_compatible, _dataframe_to_data_files

        if not isinstance(df, pa.Table):
            raise ValueError(f"Expected PyArrow table, got: {df}")

        if unsupported_partitions := [
            field for field in self.table_metadata.spec().fields if not field.transform.supports_pyarrow_transform
        ]:
            raise ValueError(
                f"Not all partition types are supported for writes. Following partitions cannot be written using pyarrow: {unsupported_partitions}."
            )
        downcast_ns_timestamp_to_us = Config().get_bool(DOWNCAST_NS_TIMESTAMP_TO_US_ON_WRITE) or False
        _check_pyarrow_schema_compatible(
            self.table_metadata.schema(), provided_schema=df.schema, downcast_ns_timestamp_to_us=downcast_ns_timestamp_to_us
        )

        manifest_merge_enabled = property_as_bool(
            self.table_metadata.properties,
            TableProperties.MANIFEST_MERGE_ENABLED,
            TableProperties.MANIFEST_MERGE_ENABLED_DEFAULT,
        )
        update_snapshot = self.update_snapshot(snapshot_properties=snapshot_properties)
        append_method = update_snapshot.merge_append if manifest_merge_enabled else update_snapshot.fast_append

        with append_method() as append_files:
            # skip writing data files if the dataframe is empty
            if df.shape[0] > 0:
                data_files = _dataframe_to_data_files(
                    table_metadata=self.table_metadata, write_uuid=append_files.commit_uuid, df=df, io=self._table.io
                )
                for data_file in data_files:
                    append_files.append_data_file(data_file)

    def overwrite(
        self,
        df: pa.Table,
        overwrite_filter: Union[BooleanExpression, str] = ALWAYS_TRUE,
        snapshot_properties: Dict[str, str] = EMPTY_DICT,
    ) -> None:
        """
        Shorthand for adding a table overwrite with a PyArrow table to the transaction.

        An overwrite may produce zero or more snapshots based on the operation:

            - DELETE: In case existing Parquet files can be dropped completely.
            - REPLACE: In case existing Parquet files need to be rewritten.
            - APPEND: In case new data is being inserted into the table.

        Args:
            df: The Arrow dataframe that will be used to overwrite the table
            overwrite_filter: ALWAYS_TRUE when you overwrite all the data,
                              or a boolean expression in case of a partial overwrite
            snapshot_properties: Custom properties to be added to the snapshot summary
        """
        try:
            import pyarrow as pa
        except ModuleNotFoundError as e:
            raise ModuleNotFoundError("For writes PyArrow needs to be installed") from e

        from pyiceberg.io.pyarrow import _check_pyarrow_schema_compatible, _dataframe_to_data_files

        if not isinstance(df, pa.Table):
            raise ValueError(f"Expected PyArrow table, got: {df}")

        if unsupported_partitions := [
            field for field in self.table_metadata.spec().fields if not field.transform.supports_pyarrow_transform
        ]:
            raise ValueError(
                f"Not all partition types are supported for writes. Following partitions cannot be written using pyarrow: {unsupported_partitions}."
            )
        downcast_ns_timestamp_to_us = Config().get_bool(DOWNCAST_NS_TIMESTAMP_TO_US_ON_WRITE) or False
        _check_pyarrow_schema_compatible(
            self.table_metadata.schema(), provided_schema=df.schema, downcast_ns_timestamp_to_us=downcast_ns_timestamp_to_us
        )

        self.delete(delete_filter=overwrite_filter, snapshot_properties=snapshot_properties)

        with self.update_snapshot(snapshot_properties=snapshot_properties).fast_append() as update_snapshot:
            # skip writing data files if the dataframe is empty
            if df.shape[0] > 0:
                data_files = _dataframe_to_data_files(
                    table_metadata=self.table_metadata, write_uuid=update_snapshot.commit_uuid, df=df, io=self._table.io
                )
                for data_file in data_files:
                    update_snapshot.append_data_file(data_file)

    def delete(self, delete_filter: Union[str, BooleanExpression], snapshot_properties: Dict[str, str] = EMPTY_DICT) -> None:
        """
        Shorthand for deleting record from a table.

        An deletee may produce zero or more snapshots based on the operation:

            - DELETE: In case existing Parquet files can be dropped completely.
            - REPLACE: In case existing Parquet files need to be rewritten

        Args:
            delete_filter: A boolean expression to delete rows from a table
            snapshot_properties: Custom properties to be added to the snapshot summary
        """
        from pyiceberg.io.pyarrow import (
            ArrowScan,
            _dataframe_to_data_files,
            _expression_to_complementary_pyarrow,
        )

        if (
            self.table_metadata.properties.get(TableProperties.DELETE_MODE, TableProperties.DELETE_MODE_DEFAULT)
            == TableProperties.DELETE_MODE_MERGE_ON_READ
        ):
            warnings.warn("Merge on read is not yet supported, falling back to copy-on-write")

        if isinstance(delete_filter, str):
            delete_filter = _parse_row_filter(delete_filter)

        with self.update_snapshot(snapshot_properties=snapshot_properties).delete() as delete_snapshot:
            delete_snapshot.delete_by_predicate(delete_filter)

        # Check if there are any files that require an actual rewrite of a data file
        if delete_snapshot.rewrites_needed is True:
            bound_delete_filter = bind(self.table_metadata.schema(), delete_filter, case_sensitive=True)
            preserve_row_filter = _expression_to_complementary_pyarrow(bound_delete_filter)

            files = self._scan(row_filter=delete_filter).plan_files()

            commit_uuid = uuid.uuid4()
            counter = itertools.count(0)

            replaced_files: List[Tuple[DataFile, List[DataFile]]] = []
            # This will load the Parquet file into memory, including:
            #   - Filter out the rows based on the delete filter
            #   - Projecting it to the current schema
            #   - Applying the positional deletes if they are there
            # When writing
            #   - Apply the latest partition-spec
            #   - And sort order when added
            for original_file in files:
                df = ArrowScan(
                    table_metadata=self.table_metadata,
                    io=self._table.io,
                    projected_schema=self.table_metadata.schema(),
                    row_filter=AlwaysTrue(),
                ).to_table(tasks=[original_file])
                filtered_df = df.filter(preserve_row_filter)

                # Only rewrite if there are records being deleted
                if len(filtered_df) == 0:
                    replaced_files.append((original_file.file, []))
                elif len(df) != len(filtered_df):
                    replaced_files.append((
                        original_file.file,
                        list(
                            _dataframe_to_data_files(
                                io=self._table.io,
                                df=filtered_df,
                                table_metadata=self.table_metadata,
                                write_uuid=commit_uuid,
                                counter=counter,
                            )
                        ),
                    ))

            if len(replaced_files) > 0:
                with self.update_snapshot(snapshot_properties=snapshot_properties).overwrite(
                    commit_uuid=commit_uuid
                ) as overwrite_snapshot:
                    for original_data_file, replaced_data_files in replaced_files:
                        overwrite_snapshot.delete_data_file(original_data_file)
                        for replaced_data_file in replaced_data_files:
                            overwrite_snapshot.append_data_file(replaced_data_file)

        if not delete_snapshot.files_affected and not delete_snapshot.rewrites_needed:
            warnings.warn("Delete operation did not match any records")

    def add_files(
        self, file_paths: List[str], snapshot_properties: Dict[str, str] = EMPTY_DICT, check_duplicate_files: bool = True
    ) -> None:
        """
        Shorthand API for adding files as data files to the table transaction.

        Args:
            file_paths: The list of full file paths to be added as data files to the table

        Raises:
            FileNotFoundError: If the file does not exist.
            ValueError: Raises a ValueError given file_paths contains duplicate files
            ValueError: Raises a ValueError given file_paths already referenced by table
        """
        if len(file_paths) != len(set(file_paths)):
            raise ValueError("File paths must be unique")

        if check_duplicate_files:
            import pyarrow.compute as pc

            expr = pc.field("file_path").isin(file_paths)
            referenced_files = [file["file_path"] for file in self._table.inspect.files().filter(expr).to_pylist()]

            if referenced_files:
                raise ValueError(f"Cannot add files that are already referenced by table, files: {', '.join(referenced_files)}")

        if self.table_metadata.name_mapping() is None:
            self.set_properties(**{
                TableProperties.DEFAULT_NAME_MAPPING: self.table_metadata.schema().name_mapping.model_dump_json()
            })
        with self.update_snapshot(snapshot_properties=snapshot_properties).fast_append() as update_snapshot:
            data_files = _parquet_files_to_data_files(
                table_metadata=self.table_metadata, file_paths=file_paths, io=self._table.io
            )
            for data_file in data_files:
                update_snapshot.append_data_file(data_file)

    def update_spec(self) -> UpdateSpec:
        """Create a new UpdateSpec to update the partitioning of the table.

        Returns:
            A new UpdateSpec.
        """
        return UpdateSpec(self)

    def remove_properties(self, *removals: str) -> Transaction:
        """Remove properties.

        Args:
            removals: Properties to be removed.

        Returns:
            The alter table builder.
        """
        return self._apply((RemovePropertiesUpdate(removals=removals),))

    def update_location(self, location: str) -> Transaction:
        """Set the new table location.

        Args:
            location: The new location of the table.

        Returns:
            The alter table builder.
        """
        raise NotImplementedError("Not yet implemented")

    def commit_transaction(self) -> Table:
        """Commit the changes to the catalog.

        Returns:
            The table with the updates applied.
        """
        if len(self._updates) > 0:
            self._requirements += (AssertTableUUID(uuid=self.table_metadata.table_uuid),)
            self._table._do_commit(  # pylint: disable=W0212
                updates=self._updates,
                requirements=self._requirements,
            )
            return self._table
        else:
            return self._table


class CreateTableTransaction(Transaction):
    """A transaction that involves the creation of a a new table."""

    def _initial_changes(self, table_metadata: TableMetadata) -> None:
        """Set the initial changes that can reconstruct the initial table metadata when creating the CreateTableTransaction."""
        self._updates += (
            AssignUUIDUpdate(uuid=table_metadata.table_uuid),
            UpgradeFormatVersionUpdate(format_version=table_metadata.format_version),
        )

        schema: Schema = table_metadata.schema()
        self._updates += (
            AddSchemaUpdate(schema_=schema, last_column_id=schema.highest_field_id, initial_change=True),
            SetCurrentSchemaUpdate(schema_id=-1),
        )

        spec: PartitionSpec = table_metadata.spec()
        if spec.is_unpartitioned():
            self._updates += (AddPartitionSpecUpdate(spec=UNPARTITIONED_PARTITION_SPEC, initial_change=True),)
        else:
            self._updates += (AddPartitionSpecUpdate(spec=spec, initial_change=True),)
        self._updates += (SetDefaultSpecUpdate(spec_id=-1),)

        sort_order: Optional[SortOrder] = table_metadata.sort_order_by_id(table_metadata.default_sort_order_id)
        if sort_order is None or sort_order.is_unsorted:
            self._updates += (AddSortOrderUpdate(sort_order=UNSORTED_SORT_ORDER, initial_change=True),)
        else:
            self._updates += (AddSortOrderUpdate(sort_order=sort_order, initial_change=True),)
        self._updates += (SetDefaultSortOrderUpdate(sort_order_id=-1),)

        self._updates += (
            SetLocationUpdate(location=table_metadata.location),
            SetPropertiesUpdate(updates=table_metadata.properties),
        )

    def __init__(self, table: StagedTable):
        super().__init__(table, autocommit=False)
        self._initial_changes(table.metadata)

    def commit_transaction(self) -> Table:
        """Commit the changes to the catalog.

        In the case of a CreateTableTransaction, the only requirement is AssertCreate.
        Returns:
            The table with the updates applied.
        """
        self._requirements = (AssertCreate(),)
        self._table._do_commit(  # pylint: disable=W0212
            updates=self._updates,
            requirements=self._requirements,
        )
        return self._table


class Namespace(IcebergRootModel[List[str]]):
    """Reference to one or more levels of a namespace."""

    root: List[str] = Field(
        ...,
        description="Reference to one or more levels of a namespace",
    )


class TableIdentifier(IcebergBaseModel):
    """Fully Qualified identifier to a table."""

    namespace: Namespace
    name: str


class CommitTableRequest(IcebergBaseModel):
    """A pydantic BaseModel for a table commit request."""

    identifier: TableIdentifier = Field()
    requirements: Tuple[TableRequirement, ...] = Field(default_factory=tuple)
    updates: Tuple[TableUpdate, ...] = Field(default_factory=tuple)


class CommitTableResponse(IcebergBaseModel):
    """A pydantic BaseModel for a table commit response."""

    metadata: TableMetadata
    metadata_location: str = Field(alias="metadata-location")


class Table:
    """An Iceberg table."""

    _identifier: Identifier = Field()
    metadata: TableMetadata
    metadata_location: str = Field()
    io: FileIO
    catalog: Catalog

    def __init__(
        self, identifier: Identifier, metadata: TableMetadata, metadata_location: str, io: FileIO, catalog: Catalog
    ) -> None:
        self._identifier = identifier
        self.metadata = metadata
        self.metadata_location = metadata_location
        self.io = io
        self.catalog = catalog

    def transaction(self) -> Transaction:
        """Create a new transaction object to first stage the changes, and then commit them to the catalog.

        Returns:
            The transaction object
        """
        return Transaction(self)

    @property
    def inspect(self) -> InspectTable:
        """Return the InspectTable object to browse the table metadata.

        Returns:
            InspectTable object based on this Table.
        """
        return InspectTable(self)

    def refresh(self) -> Table:
        """Refresh the current table metadata.

        Returns:
            An updated instance of the same Iceberg table
        """
        fresh = self.catalog.load_table(self._identifier)
        self.metadata = fresh.metadata
        self.io = fresh.io
        self.metadata_location = fresh.metadata_location
        return self

    @property
    def identifier(self) -> Identifier:
        """Return the identifier of this table.

        Returns:
            An Identifier tuple of the table name
        """
        deprecation_message(
            deprecated_in="0.8.0",
            removed_in="0.9.0",
            help_message="Table.identifier property is deprecated. Please use Table.name() function instead.",
        )
        return (self.catalog.name,) + self._identifier

    def name(self) -> Identifier:
        """Return the identifier of this table.

        Returns:
            An Identifier tuple of the table name
        """
        return self.identifier

    def scan(
        self,
        row_filter: Union[str, BooleanExpression] = ALWAYS_TRUE,
        selected_fields: Tuple[str, ...] = ("*",),
        case_sensitive: bool = True,
        snapshot_id: Optional[int] = None,
        options: Properties = EMPTY_DICT,
        limit: Optional[int] = None,
    ) -> DataScan:
        """Fetch a DataScan based on the table's current metadata.

            The data scan can be used to project the table's data
            that matches the provided row_filter onto the table's
            current schema.

        Args:
            row_filter:
                A string or BooleanExpression that decsribes the
                desired rows
            selected_fileds:
                A tuple of strings representing the column names
                to return in the output dataframe.
            case_sensitive:
                If True column matching is case sensitive
            snapshot_id:
                Optional Snapshot ID to time travel to. If None,
                scans the table as of the current snapshot ID.
            options:
                Additional Table properties as a dictionary of
                string key value pairs to use for this scan.
            limit:
                An integer representing the number of rows to
                return in the scan result. If None, fetches all
                matching rows.

        Returns:
            A DataScan based on the table's current metadata.
        """
        return DataScan(
            table_metadata=self.metadata,
            io=self.io,
            row_filter=row_filter,
            selected_fields=selected_fields,
            case_sensitive=case_sensitive,
            snapshot_id=snapshot_id,
            options=options,
            limit=limit,
        )

    @property
    def format_version(self) -> TableVersion:
        return self.metadata.format_version

    def schema(self) -> Schema:
        """Return the schema for this table."""
        return next(schema for schema in self.metadata.schemas if schema.schema_id == self.metadata.current_schema_id)

    def schemas(self) -> Dict[int, Schema]:
        """Return a dict of the schema of this table."""
        return {schema.schema_id: schema for schema in self.metadata.schemas}

    def spec(self) -> PartitionSpec:
        """Return the partition spec of this table."""
        return next(spec for spec in self.metadata.partition_specs if spec.spec_id == self.metadata.default_spec_id)

    def specs(self) -> Dict[int, PartitionSpec]:
        """Return a dict the partition specs this table."""
        return {spec.spec_id: spec for spec in self.metadata.partition_specs}

    def sort_order(self) -> SortOrder:
        """Return the sort order of this table."""
        return next(
            sort_order for sort_order in self.metadata.sort_orders if sort_order.order_id == self.metadata.default_sort_order_id
        )

    def sort_orders(self) -> Dict[int, SortOrder]:
        """Return a dict of the sort orders of this table."""
        return {sort_order.order_id: sort_order for sort_order in self.metadata.sort_orders}

    def last_partition_id(self) -> int:
        """Return the highest assigned partition field ID across all specs or 999 if only the unpartitioned spec exists."""
        if self.metadata.last_partition_id:
            return self.metadata.last_partition_id
        return PARTITION_FIELD_ID_START - 1

    @property
    def properties(self) -> Dict[str, str]:
        """Properties of the table."""
        return self.metadata.properties

    def location(self) -> str:
        """Return the table's base location."""
        return self.metadata.location

    @property
    def last_sequence_number(self) -> int:
        return self.metadata.last_sequence_number

    def current_snapshot(self) -> Optional[Snapshot]:
        """Get the current snapshot for this table, or None if there is no current snapshot."""
        if self.metadata.current_snapshot_id is not None:
            return self.snapshot_by_id(self.metadata.current_snapshot_id)
        return None

    def snapshots(self) -> List[Snapshot]:
        return self.metadata.snapshots

    def snapshot_by_id(self, snapshot_id: int) -> Optional[Snapshot]:
        """Get the snapshot of this table with the given id, or None if there is no matching snapshot."""
        return self.metadata.snapshot_by_id(snapshot_id)

    def snapshot_by_name(self, name: str) -> Optional[Snapshot]:
        """Return the snapshot referenced by the given name or null if no such reference exists."""
        if ref := self.metadata.refs.get(name):
            return self.snapshot_by_id(ref.snapshot_id)
        return None

    def snapshot_as_of_timestamp(self, timestamp_ms: int, inclusive: bool = True) -> Optional[Snapshot]:
        """Get the snapshot that was current as of or right before the given timestamp, or None if there is no matching snapshot.

        Args:
            timestamp_ms: Find snapshot that was current at/before this timestamp
            inclusive: Includes timestamp_ms in search when True. Excludes timestamp_ms when False
        """
        for log_entry in reversed(self.history()):
            if (inclusive and log_entry.timestamp_ms <= timestamp_ms) or log_entry.timestamp_ms < timestamp_ms:
                return self.snapshot_by_id(log_entry.snapshot_id)
        return None

    def history(self) -> List[SnapshotLogEntry]:
        """Get the snapshot history of this table."""
        return self.metadata.snapshot_log

    def manage_snapshots(self) -> ManageSnapshots:
        """
        Shorthand to run snapshot management operations like create branch, create tag, etc.

        Use table.manage_snapshots().<operation>().commit() to run a specific operation.
        Use table.manage_snapshots().<operation-one>().<operation-two>().commit() to run multiple operations.
        Pending changes are applied on commit.

        We can also use context managers to make more changes. For example,

        with table.manage_snapshots() as ms:
           ms.create_tag(snapshot_id1, "Tag_A").create_tag(snapshot_id2, "Tag_B")
        """
        return ManageSnapshots(transaction=Transaction(self, autocommit=True))

    def update_schema(self, allow_incompatible_changes: bool = False, case_sensitive: bool = True) -> UpdateSchema:
        """Create a new UpdateSchema to alter the columns of this table.

        Args:
            allow_incompatible_changes: If changes are allowed that might break downstream consumers.
            case_sensitive: If field names are case-sensitive.

        Returns:
            A new UpdateSchema.
        """
        return UpdateSchema(
            transaction=Transaction(self, autocommit=True),
            allow_incompatible_changes=allow_incompatible_changes,
            case_sensitive=case_sensitive,
            name_mapping=self.name_mapping(),
        )

    def name_mapping(self) -> Optional[NameMapping]:
        """Return the table's field-id NameMapping."""
        return self.metadata.name_mapping()

    def append(self, df: pa.Table, snapshot_properties: Dict[str, str] = EMPTY_DICT) -> None:
        """
        Shorthand API for appending a PyArrow table to the table.

        Args:
            df: The Arrow dataframe that will be appended to overwrite the table
            snapshot_properties: Custom properties to be added to the snapshot summary
        """
        with self.transaction() as tx:
            tx.append(df=df, snapshot_properties=snapshot_properties)

    def overwrite(
        self,
        df: pa.Table,
        overwrite_filter: Union[BooleanExpression, str] = ALWAYS_TRUE,
        snapshot_properties: Dict[str, str] = EMPTY_DICT,
    ) -> None:
        """
        Shorthand for overwriting the table with a PyArrow table.

        An overwrite may produce zero or more snapshots based on the operation:

            - DELETE: In case existing Parquet files can be dropped completely.
            - REPLACE: In case existing Parquet files need to be rewritten.
            - APPEND: In case new data is being inserted into the table.

        Args:
            df: The Arrow dataframe that will be used to overwrite the table
            overwrite_filter: ALWAYS_TRUE when you overwrite all the data,
                              or a boolean expression in case of a partial overwrite
            snapshot_properties: Custom properties to be added to the snapshot summary
        """
        with self.transaction() as tx:
            tx.overwrite(df=df, overwrite_filter=overwrite_filter, snapshot_properties=snapshot_properties)

    def delete(
        self, delete_filter: Union[BooleanExpression, str] = ALWAYS_TRUE, snapshot_properties: Dict[str, str] = EMPTY_DICT
    ) -> None:
        """
        Shorthand for deleting rows from the table.

        Args:
            delete_filter: The predicate that used to remove rows
            snapshot_properties: Custom properties to be added to the snapshot summary
        """
        with self.transaction() as tx:
            tx.delete(delete_filter=delete_filter, snapshot_properties=snapshot_properties)

    def add_files(
        self, file_paths: List[str], snapshot_properties: Dict[str, str] = EMPTY_DICT, check_duplicate_files: bool = True
    ) -> None:
        """
        Shorthand API for adding files as data files to the table.

        Args:
            file_paths: The list of full file paths to be added as data files to the table

        Raises:
            FileNotFoundError: If the file does not exist.
        """
        with self.transaction() as tx:
            tx.add_files(
                file_paths=file_paths, snapshot_properties=snapshot_properties, check_duplicate_files=check_duplicate_files
            )

    def update_spec(self, case_sensitive: bool = True) -> UpdateSpec:
        return UpdateSpec(Transaction(self, autocommit=True), case_sensitive=case_sensitive)

    def refs(self) -> Dict[str, SnapshotRef]:
        """Return the snapshot references in the table."""
        return self.metadata.refs

    def _do_commit(self, updates: Tuple[TableUpdate, ...], requirements: Tuple[TableRequirement, ...]) -> None:
        response = self.catalog.commit_table(self, requirements, updates)
        self.metadata = response.metadata
        self.metadata_location = response.metadata_location

    def __eq__(self, other: Any) -> bool:
        """Return the equality of two instances of the Table class."""
        return (
            self.name() == other.name() and self.metadata == other.metadata and self.metadata_location == other.metadata_location
            if isinstance(other, Table)
            else False
        )

    def __repr__(self) -> str:
        """Return the string representation of the Table class."""
        table_name = self.catalog.table_name_from(self._identifier)
        schema_str = ",\n  ".join(str(column) for column in self.schema().columns if self.schema())
        partition_str = f"partition by: [{', '.join(field.name for field in self.spec().fields if self.spec())}]"
        sort_order_str = f"sort order: [{', '.join(str(field) for field in self.sort_order().fields if self.sort_order())}]"
        snapshot_str = f"snapshot: {str(self.current_snapshot()) if self.current_snapshot() else 'null'}"
        result_str = f"{table_name}(\n  {schema_str}\n),\n{partition_str},\n{sort_order_str},\n{snapshot_str}"
        return result_str

    def to_daft(self) -> daft.DataFrame:
        """Read a Daft DataFrame lazily from this Iceberg table.

        Returns:
            daft.DataFrame: Unmaterialized Daft Dataframe created from the Iceberg table
        """
        import daft

        return daft.read_iceberg(self)


class StaticTable(Table):
    """Load a table directly from a metadata file (i.e., without using a catalog)."""

    def refresh(self) -> Table:
        """Refresh the current table metadata."""
        raise NotImplementedError("To be implemented")

    @classmethod
    def from_metadata(cls, metadata_location: str, properties: Properties = EMPTY_DICT) -> StaticTable:
        io = load_file_io(properties=properties, location=metadata_location)
        file = io.new_input(metadata_location)

        from pyiceberg.serializers import FromInputFile

        metadata = FromInputFile.table_metadata(file)

        from pyiceberg.catalog.noop import NoopCatalog

        return cls(
            identifier=("static-table", metadata_location),
            metadata_location=metadata_location,
            metadata=metadata,
            io=load_file_io({**properties, **metadata.properties}),
            catalog=NoopCatalog("static-table"),
        )


class StagedTable(Table):
    def refresh(self) -> Table:
        raise ValueError("Cannot refresh a staged table")

    def scan(
        self,
        row_filter: Union[str, BooleanExpression] = ALWAYS_TRUE,
        selected_fields: Tuple[str, ...] = ("*",),
        case_sensitive: bool = True,
        snapshot_id: Optional[int] = None,
        options: Properties = EMPTY_DICT,
        limit: Optional[int] = None,
    ) -> DataScan:
        raise ValueError("Cannot scan a staged table")

    def to_daft(self) -> daft.DataFrame:
        raise ValueError("Cannot convert a staged table to a Daft DataFrame")


def _parse_row_filter(expr: Union[str, BooleanExpression]) -> BooleanExpression:
    """Accept an expression in the form of a BooleanExpression or a string.

    In the case of a string, it will be converted into a unbound BooleanExpression.

    Args:
        expr: Expression as a BooleanExpression or a string.

    Returns: An unbound BooleanExpression.
    """
    return parser.parse(expr) if isinstance(expr, str) else expr


S = TypeVar("S", bound="TableScan", covariant=True)


class TableScan(ABC):
    table_metadata: TableMetadata
    io: FileIO
    row_filter: BooleanExpression
    selected_fields: Tuple[str, ...]
    case_sensitive: bool
    snapshot_id: Optional[int]
    options: Properties
    limit: Optional[int]

    def __init__(
        self,
        table_metadata: TableMetadata,
        io: FileIO,
        row_filter: Union[str, BooleanExpression] = ALWAYS_TRUE,
        selected_fields: Tuple[str, ...] = ("*",),
        case_sensitive: bool = True,
        snapshot_id: Optional[int] = None,
        options: Properties = EMPTY_DICT,
        limit: Optional[int] = None,
    ):
        self.table_metadata = table_metadata
        self.io = io
        self.row_filter = _parse_row_filter(row_filter)
        self.selected_fields = selected_fields
        self.case_sensitive = case_sensitive
        self.snapshot_id = snapshot_id
        self.options = options
        self.limit = limit

    def snapshot(self) -> Optional[Snapshot]:
        if self.snapshot_id:
            return self.table_metadata.snapshot_by_id(self.snapshot_id)
        return self.table_metadata.current_snapshot()

    def projection(self) -> Schema:
        current_schema = self.table_metadata.schema()
        if self.snapshot_id is not None:
            snapshot = self.table_metadata.snapshot_by_id(self.snapshot_id)
            if snapshot is not None:
                if snapshot.schema_id is not None:
                    try:
                        current_schema = next(
                            schema for schema in self.table_metadata.schemas if schema.schema_id == snapshot.schema_id
                        )
                    except StopIteration:
                        warnings.warn(f"Metadata does not contain schema with id: {snapshot.schema_id}")
            else:
                raise ValueError(f"Snapshot not found: {self.snapshot_id}")

        if "*" in self.selected_fields:
            return current_schema

        return current_schema.select(*self.selected_fields, case_sensitive=self.case_sensitive)

    @abstractmethod
    def plan_files(self) -> Iterable[ScanTask]: ...

    @abstractmethod
    def to_arrow(self) -> pa.Table: ...

    @abstractmethod
    def to_pandas(self, **kwargs: Any) -> pd.DataFrame: ...

    def update(self: S, **overrides: Any) -> S:
        """Create a copy of this table scan with updated fields."""
        return type(self)(**{**self.__dict__, **overrides})

    def use_ref(self: S, name: str) -> S:
        if self.snapshot_id:
            raise ValueError(f"Cannot override ref, already set snapshot id={self.snapshot_id}")
        if snapshot := self.table_metadata.snapshot_by_name(name):
            return self.update(snapshot_id=snapshot.snapshot_id)

        raise ValueError(f"Cannot scan unknown ref={name}")

    def select(self: S, *field_names: str) -> S:
        if "*" in self.selected_fields:
            return self.update(selected_fields=field_names)
        return self.update(selected_fields=tuple(set(self.selected_fields).intersection(set(field_names))))

    def filter(self: S, expr: Union[str, BooleanExpression]) -> S:
        return self.update(row_filter=And(self.row_filter, _parse_row_filter(expr)))

    def with_case_sensitive(self: S, case_sensitive: bool = True) -> S:
        return self.update(case_sensitive=case_sensitive)


class ScanTask(ABC):
    pass


@dataclass(init=False)
class FileScanTask(ScanTask):
    """Task representing a data file and its corresponding delete files."""

    file: DataFile
    delete_files: Set[DataFile]
    start: int
    length: int

    def __init__(
        self,
        data_file: DataFile,
        delete_files: Optional[Set[DataFile]] = None,
        start: Optional[int] = None,
        length: Optional[int] = None,
    ) -> None:
        self.file = data_file
        self.delete_files = delete_files or set()
        self.start = start or 0
        self.length = length or data_file.file_size_in_bytes


def _open_manifest(
    io: FileIO,
    manifest: ManifestFile,
    partition_filter: Callable[[DataFile], bool],
    metrics_evaluator: Callable[[DataFile], bool],
) -> List[ManifestEntry]:
    """Open a manifest file and return matching manifest entries.

    Returns:
        A list of ManifestEntry that matches the provided filters.
    """
    return [
        manifest_entry
        for manifest_entry in manifest.fetch_manifest_entry(io, discard_deleted=True)
        if partition_filter(manifest_entry.data_file) and metrics_evaluator(manifest_entry.data_file)
    ]


def _min_sequence_number(manifests: List[ManifestFile]) -> int:
    try:
        return min(
            manifest.min_sequence_number or INITIAL_SEQUENCE_NUMBER
            for manifest in manifests
            if manifest.content == ManifestContent.DATA
        )
    except ValueError:
        # In case of an empty iterator
        return INITIAL_SEQUENCE_NUMBER


def _match_deletes_to_data_file(data_entry: ManifestEntry, positional_delete_entries: SortedList[ManifestEntry]) -> Set[DataFile]:
    """Check if the delete file is relevant for the data file.

    Using the column metrics to see if the filename is in the lower and upper bound.

    Args:
        data_entry (ManifestEntry): The manifest entry path of the datafile.
        positional_delete_entries (List[ManifestEntry]): All the candidate positional deletes manifest entries.

    Returns:
        A set of files that are relevant for the data file.
    """
    relevant_entries = positional_delete_entries[positional_delete_entries.bisect_right(data_entry) :]

    if len(relevant_entries) > 0:
        evaluator = _InclusiveMetricsEvaluator(POSITIONAL_DELETE_SCHEMA, EqualTo("file_path", data_entry.data_file.file_path))
        return {
            positional_delete_entry.data_file
            for positional_delete_entry in relevant_entries
            if evaluator.eval(positional_delete_entry.data_file)
        }
    else:
        return set()


class DataScan(TableScan):
    def _build_partition_projection(self, spec_id: int) -> BooleanExpression:
        project = inclusive_projection(self.table_metadata.schema(), self.table_metadata.specs()[spec_id])
        return project(self.row_filter)

    @cached_property
    def partition_filters(self) -> KeyDefaultDict[int, BooleanExpression]:
        return KeyDefaultDict(self._build_partition_projection)

    def _build_manifest_evaluator(self, spec_id: int) -> Callable[[ManifestFile], bool]:
        spec = self.table_metadata.specs()[spec_id]
        return manifest_evaluator(spec, self.table_metadata.schema(), self.partition_filters[spec_id], self.case_sensitive)

    def _build_partition_evaluator(self, spec_id: int) -> Callable[[DataFile], bool]:
        spec = self.table_metadata.specs()[spec_id]
        partition_type = spec.partition_type(self.table_metadata.schema())
        partition_schema = Schema(*partition_type.fields)
        partition_expr = self.partition_filters[spec_id]

        # The lambda created here is run in multiple threads.
        # So we avoid creating _EvaluatorExpression methods bound to a single
        # shared instance across multiple threads.
        return lambda data_file: expression_evaluator(partition_schema, partition_expr, self.case_sensitive)(data_file.partition)

    def _check_sequence_number(self, min_sequence_number: int, manifest: ManifestFile) -> bool:
        """Ensure that no manifests are loaded that contain deletes that are older than the data.

        Args:
            min_sequence_number (int): The minimal sequence number.
            manifest (ManifestFile): A ManifestFile that can be either data or deletes.

        Returns:
            Boolean indicating if it is either a data file, or a relevant delete file.
        """
        return manifest.content == ManifestContent.DATA or (
            # Not interested in deletes that are older than the data
            manifest.content == ManifestContent.DELETES
            and (manifest.sequence_number or INITIAL_SEQUENCE_NUMBER) >= min_sequence_number
        )

    def plan_files(self) -> Iterable[FileScanTask]:
        """Plans the relevant files by filtering on the PartitionSpecs.

        Returns:
            List of FileScanTasks that contain both data and delete files.
        """
        snapshot = self.snapshot()
        if not snapshot:
            return iter([])

        # step 1: filter manifests using partition summaries
        # the filter depends on the partition spec used to write the manifest file, so create a cache of filters for each spec id

        manifest_evaluators: Dict[int, Callable[[ManifestFile], bool]] = KeyDefaultDict(self._build_manifest_evaluator)

        manifests = [
            manifest_file
            for manifest_file in snapshot.manifests(self.io)
            if manifest_evaluators[manifest_file.partition_spec_id](manifest_file)
        ]

        # step 2: filter the data files in each manifest
        # this filter depends on the partition spec used to write the manifest file

        partition_evaluators: Dict[int, Callable[[DataFile], bool]] = KeyDefaultDict(self._build_partition_evaluator)
        metrics_evaluator = _InclusiveMetricsEvaluator(
            self.table_metadata.schema(),
            self.row_filter,
            self.case_sensitive,
            strtobool(self.options.get("include_empty_files", "false")),
        ).eval

        min_sequence_number = _min_sequence_number(manifests)

        data_entries: List[ManifestEntry] = []
        positional_delete_entries = SortedList(key=lambda entry: entry.sequence_number or INITIAL_SEQUENCE_NUMBER)

        executor = ExecutorFactory.get_or_create()
        for manifest_entry in chain(
            *executor.map(
                lambda args: _open_manifest(*args),
                [
                    (
                        self.io,
                        manifest,
                        partition_evaluators[manifest.partition_spec_id],
                        metrics_evaluator,
                    )
                    for manifest in manifests
                    if self._check_sequence_number(min_sequence_number, manifest)
                ],
            )
        ):
            data_file = manifest_entry.data_file
            if data_file.content == DataFileContent.DATA:
                data_entries.append(manifest_entry)
            elif data_file.content == DataFileContent.POSITION_DELETES:
                positional_delete_entries.add(manifest_entry)
            elif data_file.content == DataFileContent.EQUALITY_DELETES:
                raise ValueError("PyIceberg does not yet support equality deletes: https://github.com/apache/iceberg/issues/6568")
            else:
                raise ValueError(f"Unknown DataFileContent ({data_file.content}): {manifest_entry}")

        return [
            FileScanTask(
                data_entry.data_file,
                delete_files=_match_deletes_to_data_file(
                    data_entry,
                    positional_delete_entries,
                ),
            )
            for data_entry in data_entries
        ]

    def to_arrow(self) -> pa.Table:
        """Read an Arrow table eagerly from this DataScan.

        All rows will be loaded into memory at once.

        Returns:
            pa.Table: Materialized Arrow Table from the Iceberg table's DataScan
        """
        from pyiceberg.io.pyarrow import ArrowScan

        return ArrowScan(
            self.table_metadata, self.io, self.projection(), self.row_filter, self.case_sensitive, self.limit
        ).to_table(self.plan_files())

    def to_arrow_batch_reader(self) -> pa.RecordBatchReader:
        """Return an Arrow RecordBatchReader from this DataScan.

        For large results, using a RecordBatchReader requires less memory than
        loading an Arrow Table for the same DataScan, because a RecordBatch
        is read one at a time.

        Returns:
            pa.RecordBatchReader: Arrow RecordBatchReader from the Iceberg table's DataScan
                which can be used to read a stream of record batches one by one.
        """
        import pyarrow as pa

        from pyiceberg.io.pyarrow import ArrowScan, schema_to_pyarrow

        target_schema = schema_to_pyarrow(self.projection())
        batches = ArrowScan(
            self.table_metadata, self.io, self.projection(), self.row_filter, self.case_sensitive, self.limit
        ).to_record_batches(self.plan_files())

        return pa.RecordBatchReader.from_batches(
            target_schema,
            batches,
        )

    def to_pandas(self, **kwargs: Any) -> pd.DataFrame:
        """Read a Pandas DataFrame eagerly from this Iceberg table.

        Returns:
            pd.DataFrame: Materialized Pandas Dataframe from the Iceberg table
        """
        return self.to_arrow().to_pandas(**kwargs)

    def to_duckdb(self, table_name: str, connection: Optional[DuckDBPyConnection] = None) -> DuckDBPyConnection:
        """Shorthand for loading the Iceberg Table in DuckDB.

        Returns:
            DuckDBPyConnection: In memory DuckDB connection with the Iceberg table.
        """
        import duckdb

        con = connection or duckdb.connect(database=":memory:")
        con.register(table_name, self.to_arrow())

        return con

    def to_ray(self) -> ray.data.dataset.Dataset:
        """Read a Ray Dataset eagerly from this Iceberg table.

        Returns:
            ray.data.dataset.Dataset: Materialized Ray Dataset from the Iceberg table
        """
        import ray

        return ray.data.from_arrow(self.to_arrow())


@dataclass(frozen=True)
class WriteTask:
    """Task with the parameters for writing a DataFile."""

    write_uuid: uuid.UUID
    task_id: int
    schema: Schema
    record_batches: List[pa.RecordBatch]
    sort_order_id: Optional[int] = None
    partition_key: Optional[PartitionKey] = None

    def generate_data_file_filename(self, extension: str) -> str:
        # Mimics the behavior in the Java API:
        # https://github.com/apache/iceberg/blob/a582968975dd30ff4917fbbe999f1be903efac02/core/src/main/java/org/apache/iceberg/io/OutputFileFactory.java#L92-L101
        return f"00000-{self.task_id}-{self.write_uuid}.{extension}"

    def generate_data_file_path(self, extension: str) -> str:
        if self.partition_key:
            file_path = f"{self.partition_key.to_path()}/{self.generate_data_file_filename(extension)}"
            return file_path
        else:
            return self.generate_data_file_filename(extension)


@dataclass(frozen=True)
class AddFileTask:
    """Task with the parameters for adding a Parquet file as a DataFile."""

    file_path: str
    partition_field_value: Record


def _parquet_files_to_data_files(table_metadata: TableMetadata, file_paths: List[str], io: FileIO) -> Iterable[DataFile]:
    """Convert a list files into DataFiles.

    Returns:
        An iterable that supplies DataFiles that describe the parquet files.
    """
    from pyiceberg.io.pyarrow import parquet_files_to_data_files

    yield from parquet_files_to_data_files(io=io, table_metadata=table_metadata, file_paths=iter(file_paths))


@deprecated(
    deprecated_in="0.8.0",
    removed_in="0.9.0",
    help_message="pyiceberg.table.Move has been changed to private class pyiceberg.table.update.schema._Move",
)
def Move(*args: Any, **kwargs: Any) -> _Move:
    return _Move(*args, **kwargs)


@deprecated(
    deprecated_in="0.8.0",
    removed_in="0.9.0",
    help_message="pyiceberg.table.MoveOperation has been changed to private class pyiceberg.table.update.schema._MoveOperation",
)
def MoveOperation(*args: Any, **kwargs: Any) -> _MoveOperation:
    return _MoveOperation(*args, **kwargs)


@deprecated(
    deprecated_in="0.8.0",
    removed_in="0.9.0",
    help_message="pyiceberg.table.DeleteFiles has been changed to private class pyiceberg.table.update.snapshot._DeleteFiles",
)
def DeleteFiles(*args: Any, **kwargs: Any) -> _DeleteFiles:
    return _DeleteFiles(*args, **kwargs)


@deprecated(
    deprecated_in="0.8.0",
    removed_in="0.9.0",
    help_message="pyiceberg.table.FastAppendFiles has been changed to private class pyiceberg.table.update.snapshot._FastAppendFiles",
)
def FastAppendFiles(*args: Any, **kwargs: Any) -> _FastAppendFiles:
    return _FastAppendFiles(*args, **kwargs)


@deprecated(
    deprecated_in="0.8.0",
    removed_in="0.9.0",
    help_message="pyiceberg.table.MergeAppendFiles has been changed to private class pyiceberg.table.update.snapshot._MergeAppendFiles",
)
def MergeAppendFiles(*args: Any, **kwargs: Any) -> _MergeAppendFiles:
    return _MergeAppendFiles(*args, **kwargs)


@deprecated(
    deprecated_in="0.8.0",
    removed_in="0.9.0",
    help_message="pyiceberg.table.OverwriteFiles has been changed to private class pyiceberg.table.update.snapshot._OverwriteFiles",
)
def OverwriteFiles(*args: Any, **kwargs: Any) -> _OverwriteFiles:
    return _OverwriteFiles(*args, **kwargs)
