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

import concurrent
import itertools
import uuid
import warnings
from abc import ABC, abstractmethod
from collections import defaultdict
from concurrent.futures import Future
from dataclasses import dataclass
from functools import cached_property
from itertools import chain
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Generic,
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
    AlwaysFalse,
    AlwaysTrue,
    And,
    BooleanExpression,
    EqualTo,
    Or,
    Reference,
)
from pyiceberg.expressions.visitors import (
    ROWS_MIGHT_NOT_MATCH,
    ROWS_MUST_MATCH,
    _InclusiveMetricsEvaluator,
    _StrictMetricsEvaluator,
    bind,
    expression_evaluator,
    inclusive_projection,
    manifest_evaluator,
)
from pyiceberg.io import FileIO, OutputFile, load_file_io
from pyiceberg.manifest import (
    POSITIONAL_DELETE_SCHEMA,
    DataFile,
    DataFileContent,
    ManifestContent,
    ManifestEntry,
    ManifestEntryStatus,
    ManifestFile,
    ManifestWriter,
    write_manifest,
    write_manifest_list,
)
from pyiceberg.partitioning import (
    INITIAL_PARTITION_SPEC_ID,
    PARTITION_FIELD_ID_START,
    UNPARTITIONED_PARTITION_SPEC,
    PartitionField,
    PartitionKey,
    PartitionSpec,
    _PartitionNameGenerator,
    _visit_partition_field,
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
    Operation,
    Snapshot,
    SnapshotLogEntry,
    SnapshotSummaryCollector,
    Summary,
    update_snapshot_summaries,
)
from pyiceberg.table.sorting import UNSORTED_SORT_ORDER, SortOrder
from pyiceberg.table.update import (
    AddPartitionSpecUpdate,
    AddSchemaUpdate,
    AddSnapshotUpdate,
    AddSortOrderUpdate,
    AssertCreate,
    AssertLastAssignedPartitionId,
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
    U,
    UpdatesAndRequirements,
    UpdateTableMetadata,
    UpgradeFormatVersionUpdate,
    update_table_metadata,
)
from pyiceberg.table.update.schema import UpdateSchema
from pyiceberg.transforms import IdentityTransform, TimeTransform, Transform, VoidTransform
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
from pyiceberg.utils.bin_packing import ListPacker
from pyiceberg.utils.concurrent import ExecutorFactory
from pyiceberg.utils.config import Config
from pyiceberg.utils.deprecated import deprecated, deprecation_message
from pyiceberg.utils.properties import property_as_bool, property_as_int

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
        """Minimal data scan the table with the current state of the transaction."""
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
            _dataframe_to_data_files,
            _expression_to_complementary_pyarrow,
            project_table,
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
                df = project_table(
                    tasks=[original_file],
                    table_metadata=self.table_metadata,
                    io=self._table.io,
                    row_filter=AlwaysTrue(),
                    projected_schema=self.table_metadata.schema(),
                )
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
    identifier: TableIdentifier = Field()
    requirements: Tuple[TableRequirement, ...] = Field(default_factory=tuple)
    updates: Tuple[TableUpdate, ...] = Field(default_factory=tuple)


class CommitTableResponse(IcebergBaseModel):
    metadata: TableMetadata
    metadata_location: str = Field(alias="metadata-location")


class Table:
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
        """Return the InspectTable object to browse the table metadata."""
        return InspectTable(self)

    def refresh(self) -> Table:
        """Refresh the current table metadata."""
        fresh = self.catalog.load_table(self._identifier)
        self.metadata = fresh.metadata
        self.io = fresh.io
        self.metadata_location = fresh.metadata_location
        return self

    @property
    def identifier(self) -> Identifier:
        """Return the identifier of this table."""
        deprecation_message(
            deprecated_in="0.8.0",
            removed_in="0.9.0",
            help_message="Table.identifier property is deprecated. Please use Table.name() function instead.",
        )
        return (self.catalog.name,) + self._identifier

    def name(self) -> Identifier:
        """Return the identifier of this table."""
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
        from pyiceberg.io.pyarrow import ArrowScan

        return ArrowScan(
            self.table_metadata, self.io, self.projection(), self.row_filter, self.case_sensitive, self.limit
        ).to_table(self.plan_files())

    def to_arrow_batch_reader(self) -> pa.RecordBatchReader:
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
        return self.to_arrow().to_pandas(**kwargs)

    def to_duckdb(self, table_name: str, connection: Optional[DuckDBPyConnection] = None) -> DuckDBPyConnection:
        import duckdb

        con = connection or duckdb.connect(database=":memory:")
        con.register(table_name, self.to_arrow())

        return con

    def to_ray(self) -> ray.data.dataset.Dataset:
        import ray

        return ray.data.from_arrow(self.to_arrow())


class ManageSnapshots(UpdateTableMetadata["ManageSnapshots"]):
    """
    Run snapshot management operations using APIs.

    APIs include create branch, create tag, etc.

    Use table.manage_snapshots().<operation>().commit() to run a specific operation.
    Use table.manage_snapshots().<operation-one>().<operation-two>().commit() to run multiple operations.
    Pending changes are applied on commit.

    We can also use context managers to make more changes. For example,

    with table.manage_snapshots() as ms:
       ms.create_tag(snapshot_id1, "Tag_A").create_tag(snapshot_id2, "Tag_B")
    """

    _updates: Tuple[TableUpdate, ...] = ()
    _requirements: Tuple[TableRequirement, ...] = ()

    def _commit(self) -> UpdatesAndRequirements:
        """Apply the pending changes and commit."""
        return self._updates, self._requirements

    def create_tag(self, snapshot_id: int, tag_name: str, max_ref_age_ms: Optional[int] = None) -> ManageSnapshots:
        """
        Create a new tag pointing to the given snapshot id.

        Args:
            snapshot_id (int): snapshot id of the existing snapshot to tag
            tag_name (str): name of the tag
            max_ref_age_ms (Optional[int]): max ref age in milliseconds

        Returns:
            This for method chaining
        """
        update, requirement = self._transaction._set_ref_snapshot(
            snapshot_id=snapshot_id,
            ref_name=tag_name,
            type="tag",
            max_ref_age_ms=max_ref_age_ms,
        )
        self._updates += update
        self._requirements += requirement
        return self

    def create_branch(
        self,
        snapshot_id: int,
        branch_name: str,
        max_ref_age_ms: Optional[int] = None,
        max_snapshot_age_ms: Optional[int] = None,
        min_snapshots_to_keep: Optional[int] = None,
    ) -> ManageSnapshots:
        """
        Create a new branch pointing to the given snapshot id.

        Args:
            snapshot_id (int): snapshot id of existing snapshot at which the branch is created.
            branch_name (str): name of the new branch
            max_ref_age_ms (Optional[int]): max ref age in milliseconds
            max_snapshot_age_ms (Optional[int]): max age of snapshots to keep in milliseconds
            min_snapshots_to_keep (Optional[int]): min number of snapshots to keep in milliseconds
        Returns:
            This for method chaining
        """
        update, requirement = self._transaction._set_ref_snapshot(
            snapshot_id=snapshot_id,
            ref_name=branch_name,
            type="branch",
            max_ref_age_ms=max_ref_age_ms,
            max_snapshot_age_ms=max_snapshot_age_ms,
            min_snapshots_to_keep=min_snapshots_to_keep,
        )
        self._updates += update
        self._requirements += requirement
        return self


@dataclass(frozen=True)
class WriteTask:
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
    file_path: str
    partition_field_value: Record


def _new_manifest_path(location: str, num: int, commit_uuid: uuid.UUID) -> str:
    return f"{location}/metadata/{commit_uuid}-m{num}.avro"


def _generate_manifest_list_path(location: str, snapshot_id: int, attempt: int, commit_uuid: uuid.UUID) -> str:
    # Mimics the behavior in Java:
    # https://github.com/apache/iceberg/blob/c862b9177af8e2d83122220764a056f3b96fd00c/core/src/main/java/org/apache/iceberg/SnapshotProducer.java#L491
    return f"{location}/metadata/snap-{snapshot_id}-{attempt}-{commit_uuid}.avro"


def _parquet_files_to_data_files(table_metadata: TableMetadata, file_paths: List[str], io: FileIO) -> Iterable[DataFile]:
    """Convert a list files into DataFiles.

    Returns:
        An iterable that supplies DataFiles that describe the parquet files.
    """
    from pyiceberg.io.pyarrow import parquet_files_to_data_files

    yield from parquet_files_to_data_files(io=io, table_metadata=table_metadata, file_paths=iter(file_paths))


class _SnapshotProducer(UpdateTableMetadata[U], Generic[U]):
    commit_uuid: uuid.UUID
    _io: FileIO
    _operation: Operation
    _snapshot_id: int
    _parent_snapshot_id: Optional[int]
    _added_data_files: List[DataFile]
    _manifest_num_counter: itertools.count[int]
    _deleted_data_files: Set[DataFile]

    def __init__(
        self,
        operation: Operation,
        transaction: Transaction,
        io: FileIO,
        commit_uuid: Optional[uuid.UUID] = None,
        snapshot_properties: Dict[str, str] = EMPTY_DICT,
    ) -> None:
        super().__init__(transaction)
        self.commit_uuid = commit_uuid or uuid.uuid4()
        self._io = io
        self._operation = operation
        self._snapshot_id = self._transaction.table_metadata.new_snapshot_id()
        # Since we only support the main branch for now
        self._parent_snapshot_id = (
            snapshot.snapshot_id if (snapshot := self._transaction.table_metadata.current_snapshot()) else None
        )
        self._added_data_files = []
        self._deleted_data_files = set()
        self.snapshot_properties = snapshot_properties
        self._manifest_num_counter = itertools.count(0)

    def append_data_file(self, data_file: DataFile) -> _SnapshotProducer[U]:
        self._added_data_files.append(data_file)
        return self

    def delete_data_file(self, data_file: DataFile) -> _SnapshotProducer[U]:
        self._deleted_data_files.add(data_file)
        return self

    @abstractmethod
    def _deleted_entries(self) -> List[ManifestEntry]: ...

    @abstractmethod
    def _existing_manifests(self) -> List[ManifestFile]: ...

    def _process_manifests(self, manifests: List[ManifestFile]) -> List[ManifestFile]:
        """To perform any post-processing on the manifests before writing them to the new snapshot."""
        return manifests

    def _manifests(self) -> List[ManifestFile]:
        def _write_added_manifest() -> List[ManifestFile]:
            if self._added_data_files:
                with write_manifest(
                    format_version=self._transaction.table_metadata.format_version,
                    spec=self._transaction.table_metadata.spec(),
                    schema=self._transaction.table_metadata.schema(),
                    output_file=self.new_manifest_output(),
                    snapshot_id=self._snapshot_id,
                ) as writer:
                    for data_file in self._added_data_files:
                        writer.add(
                            ManifestEntry(
                                status=ManifestEntryStatus.ADDED,
                                snapshot_id=self._snapshot_id,
                                sequence_number=None,
                                file_sequence_number=None,
                                data_file=data_file,
                            )
                        )
                return [writer.to_manifest_file()]
            else:
                return []

        def _write_delete_manifest() -> List[ManifestFile]:
            # Check if we need to mark the files as deleted
            deleted_entries = self._deleted_entries()
            if len(deleted_entries) > 0:
                deleted_manifests = []
                partition_groups: Dict[int, List[ManifestEntry]] = defaultdict(list)
                for deleted_entry in deleted_entries:
                    partition_groups[deleted_entry.data_file.spec_id].append(deleted_entry)
                for spec_id, entries in partition_groups.items():
                    with write_manifest(
                        format_version=self._transaction.table_metadata.format_version,
                        spec=self._transaction.table_metadata.specs()[spec_id],
                        schema=self._transaction.table_metadata.schema(),
                        output_file=self.new_manifest_output(),
                        snapshot_id=self._snapshot_id,
                    ) as writer:
                        for entry in entries:
                            writer.add_entry(entry)
                    deleted_manifests.append(writer.to_manifest_file())
                return deleted_manifests
            else:
                return []

        executor = ExecutorFactory.get_or_create()

        added_manifests = executor.submit(_write_added_manifest)
        delete_manifests = executor.submit(_write_delete_manifest)
        existing_manifests = executor.submit(self._existing_manifests)

        return self._process_manifests(added_manifests.result() + delete_manifests.result() + existing_manifests.result())

    def _summary(self, snapshot_properties: Dict[str, str] = EMPTY_DICT) -> Summary:
        ssc = SnapshotSummaryCollector()
        partition_summary_limit = int(
            self._transaction.table_metadata.properties.get(
                TableProperties.WRITE_PARTITION_SUMMARY_LIMIT, TableProperties.WRITE_PARTITION_SUMMARY_LIMIT_DEFAULT
            )
        )
        ssc.set_partition_summary_limit(partition_summary_limit)

        for data_file in self._added_data_files:
            ssc.add_file(
                data_file=data_file,
                partition_spec=self._transaction.table_metadata.spec(),
                schema=self._transaction.table_metadata.schema(),
            )

        if len(self._deleted_data_files) > 0:
            specs = self._transaction.table_metadata.specs()
            for data_file in self._deleted_data_files:
                ssc.remove_file(
                    data_file=data_file,
                    partition_spec=specs[data_file.spec_id],
                    schema=self._transaction.table_metadata.schema(),
                )

        previous_snapshot = (
            self._transaction.table_metadata.snapshot_by_id(self._parent_snapshot_id)
            if self._parent_snapshot_id is not None
            else None
        )

        return update_snapshot_summaries(
            summary=Summary(operation=self._operation, **ssc.build(), **snapshot_properties),
            previous_summary=previous_snapshot.summary if previous_snapshot is not None else None,
            truncate_full_table=self._operation == Operation.OVERWRITE,
        )

    def _commit(self) -> UpdatesAndRequirements:
        new_manifests = self._manifests()
        next_sequence_number = self._transaction.table_metadata.next_sequence_number()

        summary = self._summary(self.snapshot_properties)

        manifest_list_file_path = _generate_manifest_list_path(
            location=self._transaction.table_metadata.location,
            snapshot_id=self._snapshot_id,
            attempt=0,
            commit_uuid=self.commit_uuid,
        )
        with write_manifest_list(
            format_version=self._transaction.table_metadata.format_version,
            output_file=self._io.new_output(manifest_list_file_path),
            snapshot_id=self._snapshot_id,
            parent_snapshot_id=self._parent_snapshot_id,
            sequence_number=next_sequence_number,
        ) as writer:
            writer.add_manifests(new_manifests)

        snapshot = Snapshot(
            snapshot_id=self._snapshot_id,
            parent_snapshot_id=self._parent_snapshot_id,
            manifest_list=manifest_list_file_path,
            sequence_number=next_sequence_number,
            summary=summary,
            schema_id=self._transaction.table_metadata.current_schema_id,
        )

        return (
            (
                AddSnapshotUpdate(snapshot=snapshot),
                SetSnapshotRefUpdate(
                    snapshot_id=self._snapshot_id, parent_snapshot_id=self._parent_snapshot_id, ref_name="main", type="branch"
                ),
            ),
            (AssertRefSnapshotId(snapshot_id=self._transaction.table_metadata.current_snapshot_id, ref="main"),),
        )

    @property
    def snapshot_id(self) -> int:
        return self._snapshot_id

    def spec(self, spec_id: int) -> PartitionSpec:
        return self._transaction.table_metadata.specs()[spec_id]

    def new_manifest_writer(self, spec: PartitionSpec) -> ManifestWriter:
        return write_manifest(
            format_version=self._transaction.table_metadata.format_version,
            spec=spec,
            schema=self._transaction.table_metadata.schema(),
            output_file=self.new_manifest_output(),
            snapshot_id=self._snapshot_id,
        )

    def new_manifest_output(self) -> OutputFile:
        return self._io.new_output(
            _new_manifest_path(
                location=self._transaction.table_metadata.location,
                num=next(self._manifest_num_counter),
                commit_uuid=self.commit_uuid,
            )
        )

    def fetch_manifest_entry(self, manifest: ManifestFile, discard_deleted: bool = True) -> List[ManifestEntry]:
        return manifest.fetch_manifest_entry(io=self._io, discard_deleted=discard_deleted)


class DeleteFiles(_SnapshotProducer["DeleteFiles"]):
    """Will delete manifest entries from the current snapshot based on the predicate.

    This will produce a DELETE snapshot:
        Data files were removed and their contents logically deleted and/or delete
        files were added to delete rows.

    From the specification
    """

    _predicate: BooleanExpression

    def __init__(
        self,
        operation: Operation,
        transaction: Transaction,
        io: FileIO,
        commit_uuid: Optional[uuid.UUID] = None,
        snapshot_properties: Dict[str, str] = EMPTY_DICT,
    ):
        super().__init__(operation, transaction, io, commit_uuid, snapshot_properties)
        self._predicate = AlwaysFalse()

    def _commit(self) -> UpdatesAndRequirements:
        # Only produce a commit when there is something to delete
        if self.files_affected:
            return super()._commit()
        else:
            return (), ()

    def _build_partition_projection(self, spec_id: int) -> BooleanExpression:
        schema = self._transaction.table_metadata.schema()
        spec = self._transaction.table_metadata.specs()[spec_id]
        project = inclusive_projection(schema, spec)
        return project(self._predicate)

    @cached_property
    def partition_filters(self) -> KeyDefaultDict[int, BooleanExpression]:
        return KeyDefaultDict(self._build_partition_projection)

    def _build_manifest_evaluator(self, spec_id: int) -> Callable[[ManifestFile], bool]:
        schema = self._transaction.table_metadata.schema()
        spec = self._transaction.table_metadata.specs()[spec_id]
        return manifest_evaluator(spec, schema, self.partition_filters[spec_id], case_sensitive=True)

    def delete_by_predicate(self, predicate: BooleanExpression) -> None:
        self._predicate = Or(self._predicate, predicate)

    @cached_property
    def _compute_deletes(self) -> Tuple[List[ManifestFile], List[ManifestEntry], bool]:
        """Computes all the delete operation and cache it when nothing changes.

        Returns:
            - List of existing manifests that are not affected by the delete operation.
            - The manifest-entries that are deleted based on the metadata.
            - Flag indicating that rewrites of data-files are needed.
        """
        schema = self._transaction.table_metadata.schema()

        def _copy_with_new_status(entry: ManifestEntry, status: ManifestEntryStatus) -> ManifestEntry:
            return ManifestEntry(
                status=status,
                snapshot_id=entry.snapshot_id,
                sequence_number=entry.sequence_number,
                file_sequence_number=entry.file_sequence_number,
                data_file=entry.data_file,
            )

        manifest_evaluators: Dict[int, Callable[[ManifestFile], bool]] = KeyDefaultDict(self._build_manifest_evaluator)
        strict_metrics_evaluator = _StrictMetricsEvaluator(schema, self._predicate, case_sensitive=True).eval
        inclusive_metrics_evaluator = _InclusiveMetricsEvaluator(schema, self._predicate, case_sensitive=True).eval

        existing_manifests = []
        total_deleted_entries = []
        partial_rewrites_needed = False
        self._deleted_data_files = set()
        if snapshot := self._transaction.table_metadata.current_snapshot():
            for manifest_file in snapshot.manifests(io=self._io):
                if manifest_file.content == ManifestContent.DATA:
                    if not manifest_evaluators[manifest_file.partition_spec_id](manifest_file):
                        # If the manifest isn't relevant, we can just keep it in the manifest-list
                        existing_manifests.append(manifest_file)
                    else:
                        # It is relevant, let's check out the content
                        deleted_entries = []
                        existing_entries = []
                        for entry in manifest_file.fetch_manifest_entry(io=self._io, discard_deleted=True):
                            if strict_metrics_evaluator(entry.data_file) == ROWS_MUST_MATCH:
                                # Based on the metadata, it can be dropped right away
                                deleted_entries.append(_copy_with_new_status(entry, ManifestEntryStatus.DELETED))
                                self._deleted_data_files.add(entry.data_file)
                            else:
                                # Based on the metadata, we cannot determine if it can be deleted
                                existing_entries.append(_copy_with_new_status(entry, ManifestEntryStatus.EXISTING))
                                if inclusive_metrics_evaluator(entry.data_file) != ROWS_MIGHT_NOT_MATCH:
                                    partial_rewrites_needed = True

                        if len(deleted_entries) > 0:
                            total_deleted_entries += deleted_entries

                            # Rewrite the manifest
                            if len(existing_entries) > 0:
                                with write_manifest(
                                    format_version=self._transaction.table_metadata.format_version,
                                    spec=self._transaction.table_metadata.specs()[manifest_file.partition_spec_id],
                                    schema=self._transaction.table_metadata.schema(),
                                    output_file=self.new_manifest_output(),
                                    snapshot_id=self._snapshot_id,
                                ) as writer:
                                    for existing_entry in existing_entries:
                                        writer.add_entry(existing_entry)
                                existing_manifests.append(writer.to_manifest_file())
                        else:
                            existing_manifests.append(manifest_file)
                else:
                    existing_manifests.append(manifest_file)

        return existing_manifests, total_deleted_entries, partial_rewrites_needed

    def _existing_manifests(self) -> List[ManifestFile]:
        return self._compute_deletes[0]

    def _deleted_entries(self) -> List[ManifestEntry]:
        return self._compute_deletes[1]

    @property
    def rewrites_needed(self) -> bool:
        """Indicate if data files need to be rewritten."""
        return self._compute_deletes[2]

    @property
    def files_affected(self) -> bool:
        """Indicate if any manifest-entries can be dropped."""
        return len(self._deleted_entries()) > 0


class FastAppendFiles(_SnapshotProducer["FastAppendFiles"]):
    def _existing_manifests(self) -> List[ManifestFile]:
        """To determine if there are any existing manifest files.

        A fast append will add another ManifestFile to the ManifestList.
        All the existing manifest files are considered existing.
        """
        existing_manifests = []

        if self._parent_snapshot_id is not None:
            previous_snapshot = self._transaction.table_metadata.snapshot_by_id(self._parent_snapshot_id)

            if previous_snapshot is None:
                raise ValueError(f"Snapshot could not be found: {self._parent_snapshot_id}")

            for manifest in previous_snapshot.manifests(io=self._io):
                if manifest.has_added_files() or manifest.has_existing_files() or manifest.added_snapshot_id == self._snapshot_id:
                    existing_manifests.append(manifest)

        return existing_manifests

    def _deleted_entries(self) -> List[ManifestEntry]:
        """To determine if we need to record any deleted manifest entries.

        In case of an append, nothing is deleted.
        """
        return []


class MergeAppendFiles(FastAppendFiles):
    _target_size_bytes: int
    _min_count_to_merge: int
    _merge_enabled: bool

    def __init__(
        self,
        operation: Operation,
        transaction: Transaction,
        io: FileIO,
        commit_uuid: Optional[uuid.UUID] = None,
        snapshot_properties: Dict[str, str] = EMPTY_DICT,
    ) -> None:
        super().__init__(operation, transaction, io, commit_uuid, snapshot_properties)
        self._target_size_bytes = property_as_int(
            self._transaction.table_metadata.properties,
            TableProperties.MANIFEST_TARGET_SIZE_BYTES,
            TableProperties.MANIFEST_TARGET_SIZE_BYTES_DEFAULT,
        )  # type: ignore
        self._min_count_to_merge = property_as_int(
            self._transaction.table_metadata.properties,
            TableProperties.MANIFEST_MIN_MERGE_COUNT,
            TableProperties.MANIFEST_MIN_MERGE_COUNT_DEFAULT,
        )  # type: ignore
        self._merge_enabled = property_as_bool(
            self._transaction.table_metadata.properties,
            TableProperties.MANIFEST_MERGE_ENABLED,
            TableProperties.MANIFEST_MERGE_ENABLED_DEFAULT,
        )

    def _process_manifests(self, manifests: List[ManifestFile]) -> List[ManifestFile]:
        """To perform any post-processing on the manifests before writing them to the new snapshot.

        In MergeAppendFiles, we merge manifests based on the target size and the minimum count to merge
        if automatic merge is enabled.
        """
        unmerged_data_manifests = [manifest for manifest in manifests if manifest.content == ManifestContent.DATA]
        unmerged_deletes_manifests = [manifest for manifest in manifests if manifest.content == ManifestContent.DELETES]

        data_manifest_merge_manager = _ManifestMergeManager(
            target_size_bytes=self._target_size_bytes,
            min_count_to_merge=self._min_count_to_merge,
            merge_enabled=self._merge_enabled,
            snapshot_producer=self,
        )

        return data_manifest_merge_manager.merge_manifests(unmerged_data_manifests) + unmerged_deletes_manifests


class OverwriteFiles(_SnapshotProducer["OverwriteFiles"]):
    """Overwrites data from the table. This will produce an OVERWRITE snapshot.

    Data and delete files were added and removed in a logical overwrite operation.
    """

    def _existing_manifests(self) -> List[ManifestFile]:
        """Determine if there are any existing manifest files."""
        existing_files = []

        if snapshot := self._transaction.table_metadata.current_snapshot():
            for manifest_file in snapshot.manifests(io=self._io):
                entries = manifest_file.fetch_manifest_entry(io=self._io, discard_deleted=True)
                found_deleted_data_files = [entry.data_file for entry in entries if entry.data_file in self._deleted_data_files]

                if len(found_deleted_data_files) == 0:
                    existing_files.append(manifest_file)
                else:
                    # We have to rewrite the manifest file without the deleted data files
                    if any(entry.data_file not in found_deleted_data_files for entry in entries):
                        with write_manifest(
                            format_version=self._transaction.table_metadata.format_version,
                            spec=self._transaction.table_metadata.spec(),
                            schema=self._transaction.table_metadata.schema(),
                            output_file=self.new_manifest_output(),
                            snapshot_id=self._snapshot_id,
                        ) as writer:
                            [
                                writer.add_entry(
                                    ManifestEntry(
                                        status=ManifestEntryStatus.EXISTING,
                                        snapshot_id=entry.snapshot_id,
                                        sequence_number=entry.sequence_number,
                                        file_sequence_number=entry.file_sequence_number,
                                        data_file=entry.data_file,
                                    )
                                )
                                for entry in entries
                                if entry.data_file not in found_deleted_data_files
                            ]
                        existing_files.append(writer.to_manifest_file())
        return existing_files

    def _deleted_entries(self) -> List[ManifestEntry]:
        """To determine if we need to record any deleted entries.

        With a full overwrite all the entries are considered deleted.
        With partial overwrites we have to use the predicate to evaluate
        which entries are affected.
        """
        if self._parent_snapshot_id is not None:
            previous_snapshot = self._transaction.table_metadata.snapshot_by_id(self._parent_snapshot_id)
            if previous_snapshot is None:
                # This should never happen since you cannot overwrite an empty table
                raise ValueError(f"Could not find the previous snapshot: {self._parent_snapshot_id}")

            executor = ExecutorFactory.get_or_create()

            def _get_entries(manifest: ManifestFile) -> List[ManifestEntry]:
                return [
                    ManifestEntry(
                        status=ManifestEntryStatus.DELETED,
                        snapshot_id=entry.snapshot_id,
                        sequence_number=entry.sequence_number,
                        file_sequence_number=entry.file_sequence_number,
                        data_file=entry.data_file,
                    )
                    for entry in manifest.fetch_manifest_entry(self._io, discard_deleted=True)
                    if entry.data_file.content == DataFileContent.DATA and entry.data_file in self._deleted_data_files
                ]

            list_of_entries = executor.map(_get_entries, previous_snapshot.manifests(self._io))
            return list(chain(*list_of_entries))
        else:
            return []


class UpdateSnapshot:
    _transaction: Transaction
    _io: FileIO
    _snapshot_properties: Dict[str, str]

    def __init__(self, transaction: Transaction, io: FileIO, snapshot_properties: Dict[str, str] = EMPTY_DICT) -> None:
        self._transaction = transaction
        self._io = io
        self._snapshot_properties = snapshot_properties

    def fast_append(self) -> FastAppendFiles:
        return FastAppendFiles(
            operation=Operation.APPEND, transaction=self._transaction, io=self._io, snapshot_properties=self._snapshot_properties
        )

    def merge_append(self) -> MergeAppendFiles:
        return MergeAppendFiles(
            operation=Operation.APPEND, transaction=self._transaction, io=self._io, snapshot_properties=self._snapshot_properties
        )

    def overwrite(self, commit_uuid: Optional[uuid.UUID] = None) -> OverwriteFiles:
        return OverwriteFiles(
            commit_uuid=commit_uuid,
            operation=Operation.OVERWRITE
            if self._transaction.table_metadata.current_snapshot() is not None
            else Operation.APPEND,
            transaction=self._transaction,
            io=self._io,
            snapshot_properties=self._snapshot_properties,
        )

    def delete(self) -> DeleteFiles:
        return DeleteFiles(
            operation=Operation.DELETE,
            transaction=self._transaction,
            io=self._io,
            snapshot_properties=self._snapshot_properties,
        )


class UpdateSpec(UpdateTableMetadata["UpdateSpec"]):
    _transaction: Transaction
    _name_to_field: Dict[str, PartitionField] = {}
    _name_to_added_field: Dict[str, PartitionField] = {}
    _transform_to_field: Dict[Tuple[int, str], PartitionField] = {}
    _transform_to_added_field: Dict[Tuple[int, str], PartitionField] = {}
    _renames: Dict[str, str] = {}
    _added_time_fields: Dict[int, PartitionField] = {}
    _case_sensitive: bool
    _adds: List[PartitionField]
    _deletes: Set[int]
    _last_assigned_partition_id: int

    def __init__(self, transaction: Transaction, case_sensitive: bool = True) -> None:
        super().__init__(transaction)
        self._name_to_field = {field.name: field for field in transaction.table_metadata.spec().fields}
        self._name_to_added_field = {}
        self._transform_to_field = {
            (field.source_id, repr(field.transform)): field for field in transaction.table_metadata.spec().fields
        }
        self._transform_to_added_field = {}
        self._adds = []
        self._deletes = set()
        self._last_assigned_partition_id = transaction.table_metadata.last_partition_id or PARTITION_FIELD_ID_START - 1
        self._renames = {}
        self._transaction = transaction
        self._case_sensitive = case_sensitive
        self._added_time_fields = {}

    def add_field(
        self,
        source_column_name: str,
        transform: Transform[Any, Any],
        partition_field_name: Optional[str] = None,
    ) -> UpdateSpec:
        ref = Reference(source_column_name)
        bound_ref = ref.bind(self._transaction.table_metadata.schema(), self._case_sensitive)
        # verify transform can actually bind it
        output_type = bound_ref.field.field_type
        if not transform.can_transform(output_type):
            raise ValueError(f"{transform} cannot transform {output_type} values from {bound_ref.field.name}")

        transform_key = (bound_ref.field.field_id, repr(transform))
        existing_partition_field = self._transform_to_field.get(transform_key)
        if existing_partition_field and self._is_duplicate_partition(transform, existing_partition_field):
            raise ValueError(f"Duplicate partition field for ${ref.name}=${ref}, ${existing_partition_field} already exists")

        added = self._transform_to_added_field.get(transform_key)
        if added:
            raise ValueError(f"Already added partition: {added.name}")

        new_field = self._partition_field((bound_ref.field.field_id, transform), partition_field_name)
        if new_field.name in self._name_to_added_field:
            raise ValueError(f"Already added partition field with name: {new_field.name}")

        if isinstance(new_field.transform, TimeTransform):
            existing_time_field = self._added_time_fields.get(new_field.source_id)
            if existing_time_field:
                raise ValueError(f"Cannot add time partition field: {new_field.name} conflicts with {existing_time_field.name}")
            self._added_time_fields[new_field.source_id] = new_field
        self._transform_to_added_field[transform_key] = new_field

        existing_partition_field = self._name_to_field.get(new_field.name)
        if existing_partition_field and new_field.field_id not in self._deletes:
            if isinstance(existing_partition_field.transform, VoidTransform):
                self.rename_field(
                    existing_partition_field.name, existing_partition_field.name + "_" + str(existing_partition_field.field_id)
                )
            else:
                raise ValueError(f"Cannot add duplicate partition field name: {existing_partition_field.name}")

        self._name_to_added_field[new_field.name] = new_field
        self._adds.append(new_field)
        return self

    def add_identity(self, source_column_name: str) -> UpdateSpec:
        return self.add_field(source_column_name, IdentityTransform(), None)

    def remove_field(self, name: str) -> UpdateSpec:
        added = self._name_to_added_field.get(name)
        if added:
            raise ValueError(f"Cannot delete newly added field {name}")
        renamed = self._renames.get(name)
        if renamed:
            raise ValueError(f"Cannot rename and delete field {name}")
        field = self._name_to_field.get(name)
        if not field:
            raise ValueError(f"No such partition field: {name}")

        self._deletes.add(field.field_id)
        return self

    def rename_field(self, name: str, new_name: str) -> UpdateSpec:
        existing_field = self._name_to_field.get(new_name)
        if existing_field and isinstance(existing_field.transform, VoidTransform):
            return self.rename_field(name, name + "_" + str(existing_field.field_id))
        added = self._name_to_added_field.get(name)
        if added:
            raise ValueError("Cannot rename recently added partitions")
        field = self._name_to_field.get(name)
        if not field:
            raise ValueError(f"Cannot find partition field {name}")
        if field.field_id in self._deletes:
            raise ValueError(f"Cannot delete and rename partition field {name}")
        self._renames[name] = new_name
        return self

    def _commit(self) -> UpdatesAndRequirements:
        new_spec = self._apply()
        updates: Tuple[TableUpdate, ...] = ()
        requirements: Tuple[TableRequirement, ...] = ()

        if self._transaction.table_metadata.default_spec_id != new_spec.spec_id:
            if new_spec.spec_id not in self._transaction.table_metadata.specs():
                updates = (
                    AddPartitionSpecUpdate(spec=new_spec),
                    SetDefaultSpecUpdate(spec_id=-1),
                )
            else:
                updates = (SetDefaultSpecUpdate(spec_id=new_spec.spec_id),)

            required_last_assigned_partitioned_id = self._transaction.table_metadata.last_partition_id
            requirements = (AssertLastAssignedPartitionId(last_assigned_partition_id=required_last_assigned_partitioned_id),)

        return updates, requirements

    def _apply(self) -> PartitionSpec:
        def _check_and_add_partition_name(schema: Schema, name: str, source_id: int, partition_names: Set[str]) -> None:
            try:
                field = schema.find_field(name)
            except ValueError:
                field = None

            if source_id is not None and field is not None and field.field_id != source_id:
                raise ValueError(f"Cannot create identity partition from a different field in the schema {name}")
            elif field is not None and source_id != field.field_id:
                raise ValueError(f"Cannot create partition from name that exists in schema {name}")
            if not name:
                raise ValueError("Undefined name")
            if name in partition_names:
                raise ValueError(f"Partition name has to be unique: {name}")
            partition_names.add(name)

        def _add_new_field(
            schema: Schema, source_id: int, field_id: int, name: str, transform: Transform[Any, Any], partition_names: Set[str]
        ) -> PartitionField:
            _check_and_add_partition_name(schema, name, source_id, partition_names)
            return PartitionField(source_id, field_id, transform, name)

        partition_fields = []
        partition_names: Set[str] = set()
        for field in self._transaction.table_metadata.spec().fields:
            if field.field_id not in self._deletes:
                renamed = self._renames.get(field.name)
                if renamed:
                    new_field = _add_new_field(
                        self._transaction.table_metadata.schema(),
                        field.source_id,
                        field.field_id,
                        renamed,
                        field.transform,
                        partition_names,
                    )
                else:
                    new_field = _add_new_field(
                        self._transaction.table_metadata.schema(),
                        field.source_id,
                        field.field_id,
                        field.name,
                        field.transform,
                        partition_names,
                    )
                partition_fields.append(new_field)
            elif self._transaction.table_metadata.format_version == 1:
                renamed = self._renames.get(field.name)
                if renamed:
                    new_field = _add_new_field(
                        self._transaction.table_metadata.schema(),
                        field.source_id,
                        field.field_id,
                        renamed,
                        VoidTransform(),
                        partition_names,
                    )
                else:
                    new_field = _add_new_field(
                        self._transaction.table_metadata.schema(),
                        field.source_id,
                        field.field_id,
                        field.name,
                        VoidTransform(),
                        partition_names,
                    )

                partition_fields.append(new_field)

        for added_field in self._adds:
            new_field = PartitionField(
                source_id=added_field.source_id,
                field_id=added_field.field_id,
                transform=added_field.transform,
                name=added_field.name,
            )
            partition_fields.append(new_field)

        # Reuse spec id or create a new one.
        new_spec = PartitionSpec(*partition_fields)
        new_spec_id = INITIAL_PARTITION_SPEC_ID
        for spec in self._transaction.table_metadata.specs().values():
            if new_spec.compatible_with(spec):
                new_spec_id = spec.spec_id
                break
            elif new_spec_id <= spec.spec_id:
                new_spec_id = spec.spec_id + 1
        return PartitionSpec(*partition_fields, spec_id=new_spec_id)

    def _partition_field(self, transform_key: Tuple[int, Transform[Any, Any]], name: Optional[str]) -> PartitionField:
        if self._transaction.table_metadata.format_version == 2:
            source_id, transform = transform_key
            historical_fields = []
            for spec in self._transaction.table_metadata.specs().values():
                for field in spec.fields:
                    historical_fields.append((field.source_id, field.field_id, repr(field.transform), field.name))

            for field_key in historical_fields:
                if field_key[0] == source_id and field_key[2] == repr(transform):
                    if name is None or field_key[3] == name:
                        return PartitionField(source_id, field_key[1], transform, name)

        new_field_id = self._new_field_id()
        if name is None:
            tmp_field = PartitionField(transform_key[0], new_field_id, transform_key[1], "unassigned_field_name")
            name = _visit_partition_field(self._transaction.table_metadata.schema(), tmp_field, _PartitionNameGenerator())
        return PartitionField(transform_key[0], new_field_id, transform_key[1], name)

    def _new_field_id(self) -> int:
        self._last_assigned_partition_id += 1
        return self._last_assigned_partition_id

    def _is_duplicate_partition(self, transform: Transform[Any, Any], partition_field: PartitionField) -> bool:
        return partition_field.field_id not in self._deletes and partition_field.transform == transform


class _ManifestMergeManager(Generic[U]):
    _target_size_bytes: int
    _min_count_to_merge: int
    _merge_enabled: bool
    _snapshot_producer: _SnapshotProducer[U]

    def __init__(
        self, target_size_bytes: int, min_count_to_merge: int, merge_enabled: bool, snapshot_producer: _SnapshotProducer[U]
    ) -> None:
        self._target_size_bytes = target_size_bytes
        self._min_count_to_merge = min_count_to_merge
        self._merge_enabled = merge_enabled
        self._snapshot_producer = snapshot_producer

    def _group_by_spec(self, manifests: List[ManifestFile]) -> Dict[int, List[ManifestFile]]:
        groups = defaultdict(list)
        for manifest in manifests:
            groups[manifest.partition_spec_id].append(manifest)
        return groups

    def _create_manifest(self, spec_id: int, manifest_bin: List[ManifestFile]) -> ManifestFile:
        with self._snapshot_producer.new_manifest_writer(spec=self._snapshot_producer.spec(spec_id)) as writer:
            for manifest in manifest_bin:
                for entry in self._snapshot_producer.fetch_manifest_entry(manifest=manifest, discard_deleted=False):
                    if entry.status == ManifestEntryStatus.DELETED and entry.snapshot_id == self._snapshot_producer.snapshot_id:
                        #  only files deleted by this snapshot should be added to the new manifest
                        writer.delete(entry)
                    elif entry.status == ManifestEntryStatus.ADDED and entry.snapshot_id == self._snapshot_producer.snapshot_id:
                        # added entries from this snapshot are still added, otherwise they should be existing
                        writer.add(entry)
                    elif entry.status != ManifestEntryStatus.DELETED:
                        # add all non-deleted files from the old manifest as existing files
                        writer.existing(entry)

        return writer.to_manifest_file()

    def _merge_group(self, first_manifest: ManifestFile, spec_id: int, manifests: List[ManifestFile]) -> List[ManifestFile]:
        packer: ListPacker[ManifestFile] = ListPacker(target_weight=self._target_size_bytes, lookback=1, largest_bin_first=False)
        bins: List[List[ManifestFile]] = packer.pack_end(manifests, lambda m: m.manifest_length)

        def merge_bin(manifest_bin: List[ManifestFile]) -> List[ManifestFile]:
            output_manifests = []
            if len(manifest_bin) == 1:
                output_manifests.append(manifest_bin[0])
            elif first_manifest in manifest_bin and len(manifest_bin) < self._min_count_to_merge:
                #  if the bin has the first manifest (the new data files or an appended manifest file) then only
                #  merge it if the number of manifests is above the minimum count. this is applied only to bins
                #  with an in-memory manifest so that large manifests don't prevent merging older groups.
                output_manifests.extend(manifest_bin)
            else:
                output_manifests.append(self._create_manifest(spec_id, manifest_bin))

            return output_manifests

        executor = ExecutorFactory.get_or_create()
        futures = [executor.submit(merge_bin, b) for b in bins]

        # for consistent ordering, we need to maintain future order
        futures_index = {f: i for i, f in enumerate(futures)}
        completed_futures: SortedList[Future[List[ManifestFile]]] = SortedList(iterable=[], key=lambda f: futures_index[f])
        for future in concurrent.futures.as_completed(futures):
            completed_futures.add(future)

        bin_results: List[List[ManifestFile]] = [f.result() for f in completed_futures if f.result()]

        return [manifest for bin_result in bin_results for manifest in bin_result]

    def merge_manifests(self, manifests: List[ManifestFile]) -> List[ManifestFile]:
        if not self._merge_enabled or len(manifests) == 0:
            return manifests

        first_manifest = manifests[0]
        groups = self._group_by_spec(manifests)

        merged_manifests = []
        for spec_id in reversed(groups.keys()):
            merged_manifests.extend(self._merge_group(first_manifest, spec_id, groups[spec_id]))

        return merged_manifests
