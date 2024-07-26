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
from copy import copy
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from functools import cached_property, singledispatch
from itertools import chain
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Generic,
    Iterable,
    List,
    Literal,
    Optional,
    Set,
    Tuple,
    TypeVar,
    Union,
)

from pydantic import Field, field_validator
from sortedcontainers import SortedList
from typing_extensions import Annotated

import pyiceberg.expressions.parser as parser
from pyiceberg.conversions import from_bytes
from pyiceberg.exceptions import CommitFailedException, ResolveError, ValidationError
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
    ROWS_CANNOT_MATCH,
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
    PartitionFieldSummary,
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
from pyiceberg.schema import (
    PartnerAccessor,
    Schema,
    SchemaVisitor,
    SchemaWithPartnerVisitor,
    assign_fresh_schema_ids,
    promote,
    visit,
    visit_with_partner,
)
from pyiceberg.table.metadata import (
    INITIAL_SEQUENCE_NUMBER,
    SUPPORTED_TABLE_FORMAT_VERSION,
    TableMetadata,
    TableMetadataUtil,
)
from pyiceberg.table.name_mapping import (
    NameMapping,
    update_mapping,
)
from pyiceberg.table.refs import MAIN_BRANCH, SnapshotRef
from pyiceberg.table.snapshots import (
    Operation,
    Snapshot,
    SnapshotLogEntry,
    SnapshotSummaryCollector,
    Summary,
    ancestors_of,
    update_snapshot_summaries,
)
from pyiceberg.table.sorting import UNSORTED_SORT_ORDER, SortOrder
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
    IcebergType,
    ListType,
    MapType,
    NestedField,
    PrimitiveType,
    StructType,
    transform_dict_value_to_str,
)
from pyiceberg.utils.bin_packing import ListPacker
from pyiceberg.utils.concurrent import ExecutorFactory
from pyiceberg.utils.config import Config
from pyiceberg.utils.datetime import datetime_to_millis
from pyiceberg.utils.deprecated import deprecated
from pyiceberg.utils.singleton import _convert_to_hashable_type

if TYPE_CHECKING:
    import daft
    import pandas as pd
    import pyarrow as pa
    import ray
    from duckdb import DuckDBPyConnection

    from pyiceberg.catalog import Catalog

ALWAYS_TRUE = AlwaysTrue()
TABLE_ROOT_ID = -1
_JAVA_LONG_MAX = 9223372036854775807
DOWNCAST_NS_TIMESTAMP_TO_US_ON_WRITE = "downcast-ns-timestamp-to-us-on-write"


class TableProperties:
    PARQUET_ROW_GROUP_SIZE_BYTES = "write.parquet.row-group-size-bytes"
    PARQUET_ROW_GROUP_SIZE_BYTES_DEFAULT = 128 * 1024 * 1024  # 128 MB

    PARQUET_ROW_GROUP_LIMIT = "write.parquet.row-group-limit"
    PARQUET_ROW_GROUP_LIMIT_DEFAULT = 128 * 1024 * 1024  # 128 MB

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


class PropertyUtil:
    @staticmethod
    def property_as_int(properties: Dict[str, str], property_name: str, default: Optional[int] = None) -> Optional[int]:
        if value := properties.get(property_name):
            try:
                return int(value)
            except ValueError as e:
                raise ValueError(f"Could not parse table property {property_name} to an integer: {value}") from e
        else:
            return default

    @staticmethod
    def property_as_float(properties: Dict[str, str], property_name: str, default: Optional[float] = None) -> Optional[float]:
        if value := properties.get(property_name):
            try:
                return float(value)
            except ValueError as e:
                raise ValueError(f"Could not parse table property {property_name} to a float: {value}") from e
        else:
            return default

    @staticmethod
    def property_as_bool(properties: Dict[str, str], property_name: str, default: bool) -> bool:
        if value := properties.get(property_name):
            return value.lower() == "true"
        return default

    @staticmethod
    def get_first_property_value(properties: Properties, *property_names: str) -> Optional[Any]:
        for property_name in property_names:
            if property_value := properties.get(property_name):
                return property_value
        return None


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

        if format_version < self._table.metadata.format_version:
            raise ValueError(f"Cannot downgrade v{self._table.metadata.format_version} table to v{format_version}")

        if format_version > self._table.metadata.format_version:
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
            name_mapping=self._table.name_mapping(),
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
            self._table.schema(), provided_schema=df.schema, downcast_ns_timestamp_to_us=downcast_ns_timestamp_to_us
        )

        manifest_merge_enabled = PropertyUtil.property_as_bool(
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
                    table_metadata=self._table.metadata, write_uuid=append_files.commit_uuid, df=df, io=self._table.io
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
            self._table.schema(), provided_schema=df.schema, downcast_ns_timestamp_to_us=downcast_ns_timestamp_to_us
        )

        self.delete(delete_filter=overwrite_filter, snapshot_properties=snapshot_properties)

        with self.update_snapshot(snapshot_properties=snapshot_properties).fast_append() as update_snapshot:
            # skip writing data files if the dataframe is empty
            if df.shape[0] > 0:
                data_files = _dataframe_to_data_files(
                    table_metadata=self._table.metadata, write_uuid=update_snapshot.commit_uuid, df=df, io=self._table.io
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
            bound_delete_filter = bind(self._table.schema(), delete_filter, case_sensitive=True)
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
                    table_metadata=self._table.metadata,
                    io=self._table.io,
                    row_filter=AlwaysTrue(),
                    projected_schema=self.table_metadata.schema(),
                )
                filtered_df = df.filter(preserve_row_filter)

                # Only rewrite if there are records being deleted
                if len(df) != len(filtered_df):
                    replaced_files.append((
                        original_file.file,
                        list(
                            _dataframe_to_data_files(
                                io=self._table.io,
                                df=filtered_df,
                                table_metadata=self._table.metadata,
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

    def add_files(self, file_paths: List[str], snapshot_properties: Dict[str, str] = EMPTY_DICT) -> None:
        """
        Shorthand API for adding files as data files to the table transaction.

        Args:
            file_paths: The list of full file paths to be added as data files to the table

        Raises:
            FileNotFoundError: If the file does not exist.
        """
        if self._table.name_mapping() is None:
            self.set_properties(**{TableProperties.DEFAULT_NAME_MAPPING: self._table.schema().name_mapping.model_dump_json()})
        with self.update_snapshot(snapshot_properties=snapshot_properties).fast_append() as update_snapshot:
            data_files = _parquet_files_to_data_files(
                table_metadata=self._table.metadata, file_paths=file_paths, io=self._table.io
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


class AssignUUIDUpdate(IcebergBaseModel):
    action: Literal["assign-uuid"] = Field(default="assign-uuid")
    uuid: uuid.UUID


class UpgradeFormatVersionUpdate(IcebergBaseModel):
    action: Literal["upgrade-format-version"] = Field(default="upgrade-format-version")
    format_version: int = Field(alias="format-version")


class AddSchemaUpdate(IcebergBaseModel):
    action: Literal["add-schema"] = Field(default="add-schema")
    schema_: Schema = Field(alias="schema")
    # This field is required: https://github.com/apache/iceberg/pull/7445
    last_column_id: int = Field(alias="last-column-id")

    initial_change: bool = Field(default=False, exclude=True)


class SetCurrentSchemaUpdate(IcebergBaseModel):
    action: Literal["set-current-schema"] = Field(default="set-current-schema")
    schema_id: int = Field(
        alias="schema-id", description="Schema ID to set as current, or -1 to set last added schema", default=-1
    )


class AddPartitionSpecUpdate(IcebergBaseModel):
    action: Literal["add-spec"] = Field(default="add-spec")
    spec: PartitionSpec

    initial_change: bool = Field(default=False, exclude=True)


class SetDefaultSpecUpdate(IcebergBaseModel):
    action: Literal["set-default-spec"] = Field(default="set-default-spec")
    spec_id: int = Field(
        alias="spec-id", description="Partition spec ID to set as the default, or -1 to set last added spec", default=-1
    )


class AddSortOrderUpdate(IcebergBaseModel):
    action: Literal["add-sort-order"] = Field(default="add-sort-order")
    sort_order: SortOrder = Field(alias="sort-order")

    initial_change: bool = Field(default=False, exclude=True)


class SetDefaultSortOrderUpdate(IcebergBaseModel):
    action: Literal["set-default-sort-order"] = Field(default="set-default-sort-order")
    sort_order_id: int = Field(
        alias="sort-order-id", description="Sort order ID to set as the default, or -1 to set last added sort order", default=-1
    )


class AddSnapshotUpdate(IcebergBaseModel):
    action: Literal["add-snapshot"] = Field(default="add-snapshot")
    snapshot: Snapshot


class SetSnapshotRefUpdate(IcebergBaseModel):
    action: Literal["set-snapshot-ref"] = Field(default="set-snapshot-ref")
    ref_name: str = Field(alias="ref-name")
    type: Literal["tag", "branch"]
    snapshot_id: int = Field(alias="snapshot-id")
    max_ref_age_ms: Annotated[Optional[int], Field(alias="max-ref-age-ms", default=None)]
    max_snapshot_age_ms: Annotated[Optional[int], Field(alias="max-snapshot-age-ms", default=None)]
    min_snapshots_to_keep: Annotated[Optional[int], Field(alias="min-snapshots-to-keep", default=None)]


class RemoveSnapshotsUpdate(IcebergBaseModel):
    action: Literal["remove-snapshots"] = Field(default="remove-snapshots")
    snapshot_ids: List[int] = Field(alias="snapshot-ids")


class RemoveSnapshotRefUpdate(IcebergBaseModel):
    action: Literal["remove-snapshot-ref"] = Field(default="remove-snapshot-ref")
    ref_name: str = Field(alias="ref-name")


class SetLocationUpdate(IcebergBaseModel):
    action: Literal["set-location"] = Field(default="set-location")
    location: str


class SetPropertiesUpdate(IcebergBaseModel):
    action: Literal["set-properties"] = Field(default="set-properties")
    updates: Dict[str, str]

    @field_validator("updates", mode="before")
    def transform_properties_dict_value_to_str(cls, properties: Properties) -> Dict[str, str]:
        return transform_dict_value_to_str(properties)


class RemovePropertiesUpdate(IcebergBaseModel):
    action: Literal["remove-properties"] = Field(default="remove-properties")
    removals: List[str]


TableUpdate = Annotated[
    Union[
        AssignUUIDUpdate,
        UpgradeFormatVersionUpdate,
        AddSchemaUpdate,
        SetCurrentSchemaUpdate,
        AddPartitionSpecUpdate,
        SetDefaultSpecUpdate,
        AddSortOrderUpdate,
        SetDefaultSortOrderUpdate,
        AddSnapshotUpdate,
        SetSnapshotRefUpdate,
        RemoveSnapshotsUpdate,
        RemoveSnapshotRefUpdate,
        SetLocationUpdate,
        SetPropertiesUpdate,
        RemovePropertiesUpdate,
    ],
    Field(discriminator="action"),
]


class _TableMetadataUpdateContext:
    _updates: List[TableUpdate]

    def __init__(self) -> None:
        self._updates = []

    def add_update(self, update: TableUpdate) -> None:
        self._updates.append(update)

    def is_added_snapshot(self, snapshot_id: int) -> bool:
        return any(
            update.snapshot.snapshot_id == snapshot_id for update in self._updates if isinstance(update, AddSnapshotUpdate)
        )

    def is_added_schema(self, schema_id: int) -> bool:
        return any(update.schema_.schema_id == schema_id for update in self._updates if isinstance(update, AddSchemaUpdate))

    def is_added_partition_spec(self, spec_id: int) -> bool:
        return any(update.spec.spec_id == spec_id for update in self._updates if isinstance(update, AddPartitionSpecUpdate))

    def is_added_sort_order(self, sort_order_id: int) -> bool:
        return any(
            update.sort_order.order_id == sort_order_id for update in self._updates if isinstance(update, AddSortOrderUpdate)
        )

    def has_changes(self) -> bool:
        return len(self._updates) > 0


@singledispatch
def _apply_table_update(update: TableUpdate, base_metadata: TableMetadata, context: _TableMetadataUpdateContext) -> TableMetadata:
    """Apply a table update to the table metadata.

    Args:
        update: The update to be applied.
        base_metadata: The base metadata to be updated.
        context: Contains previous updates and other change tracking information in the current transaction.

    Returns:
        The updated metadata.

    """
    raise NotImplementedError(f"Unsupported table update: {update}")


@_apply_table_update.register(AssignUUIDUpdate)
def _(update: AssignUUIDUpdate, base_metadata: TableMetadata, context: _TableMetadataUpdateContext) -> TableMetadata:
    if update.uuid == base_metadata.table_uuid:
        return base_metadata

    context.add_update(update)
    return base_metadata.model_copy(update={"table_uuid": update.uuid})


@_apply_table_update.register(SetLocationUpdate)
def _(update: SetLocationUpdate, base_metadata: TableMetadata, context: _TableMetadataUpdateContext) -> TableMetadata:
    context.add_update(update)
    return base_metadata.model_copy(update={"location": update.location})


@_apply_table_update.register(UpgradeFormatVersionUpdate)
def _(
    update: UpgradeFormatVersionUpdate,
    base_metadata: TableMetadata,
    context: _TableMetadataUpdateContext,
) -> TableMetadata:
    if update.format_version > SUPPORTED_TABLE_FORMAT_VERSION:
        raise ValueError(f"Unsupported table format version: {update.format_version}")
    elif update.format_version < base_metadata.format_version:
        raise ValueError(f"Cannot downgrade v{base_metadata.format_version} table to v{update.format_version}")
    elif update.format_version == base_metadata.format_version:
        return base_metadata

    updated_metadata_data = copy(base_metadata.model_dump())
    updated_metadata_data["format-version"] = update.format_version

    context.add_update(update)
    return TableMetadataUtil.parse_obj(updated_metadata_data)


@_apply_table_update.register(SetPropertiesUpdate)
def _(update: SetPropertiesUpdate, base_metadata: TableMetadata, context: _TableMetadataUpdateContext) -> TableMetadata:
    if len(update.updates) == 0:
        return base_metadata

    properties = dict(base_metadata.properties)
    properties.update(update.updates)

    context.add_update(update)
    return base_metadata.model_copy(update={"properties": properties})


@_apply_table_update.register(RemovePropertiesUpdate)
def _(update: RemovePropertiesUpdate, base_metadata: TableMetadata, context: _TableMetadataUpdateContext) -> TableMetadata:
    if len(update.removals) == 0:
        return base_metadata

    properties = dict(base_metadata.properties)
    for key in update.removals:
        properties.pop(key)

    context.add_update(update)
    return base_metadata.model_copy(update={"properties": properties})


@_apply_table_update.register(AddSchemaUpdate)
def _(update: AddSchemaUpdate, base_metadata: TableMetadata, context: _TableMetadataUpdateContext) -> TableMetadata:
    if update.last_column_id < base_metadata.last_column_id:
        raise ValueError(f"Invalid last column id {update.last_column_id}, must be >= {base_metadata.last_column_id}")

    metadata_updates: Dict[str, Any] = {
        "last_column_id": update.last_column_id,
        "schemas": [update.schema_] if update.initial_change else base_metadata.schemas + [update.schema_],
    }

    context.add_update(update)
    return base_metadata.model_copy(update=metadata_updates)


@_apply_table_update.register(SetCurrentSchemaUpdate)
def _(update: SetCurrentSchemaUpdate, base_metadata: TableMetadata, context: _TableMetadataUpdateContext) -> TableMetadata:
    new_schema_id = update.schema_id
    if new_schema_id == -1:
        # The last added schema should be in base_metadata.schemas at this point
        new_schema_id = max(schema.schema_id for schema in base_metadata.schemas)
        if not context.is_added_schema(new_schema_id):
            raise ValueError("Cannot set current schema to last added schema when no schema has been added")

    if new_schema_id == base_metadata.current_schema_id:
        return base_metadata

    schema = base_metadata.schema_by_id(new_schema_id)
    if schema is None:
        raise ValueError(f"Schema with id {new_schema_id} does not exist")

    context.add_update(update)
    return base_metadata.model_copy(update={"current_schema_id": new_schema_id})


@_apply_table_update.register(AddPartitionSpecUpdate)
def _(update: AddPartitionSpecUpdate, base_metadata: TableMetadata, context: _TableMetadataUpdateContext) -> TableMetadata:
    for spec in base_metadata.partition_specs:
        if spec.spec_id == update.spec.spec_id and not update.initial_change:
            raise ValueError(f"Partition spec with id {spec.spec_id} already exists: {spec}")

    metadata_updates: Dict[str, Any] = {
        "partition_specs": [update.spec] if update.initial_change else base_metadata.partition_specs + [update.spec],
        "last_partition_id": max(
            max([field.field_id for field in update.spec.fields], default=0),
            base_metadata.last_partition_id or PARTITION_FIELD_ID_START - 1,
        ),
    }

    context.add_update(update)
    return base_metadata.model_copy(update=metadata_updates)


@_apply_table_update.register(SetDefaultSpecUpdate)
def _(update: SetDefaultSpecUpdate, base_metadata: TableMetadata, context: _TableMetadataUpdateContext) -> TableMetadata:
    new_spec_id = update.spec_id
    if new_spec_id == -1:
        new_spec_id = max(spec.spec_id for spec in base_metadata.partition_specs)
        if not context.is_added_partition_spec(new_spec_id):
            raise ValueError("Cannot set current partition spec to last added one when no partition spec has been added")
    if new_spec_id == base_metadata.default_spec_id:
        return base_metadata
    found_spec_id = False
    for spec in base_metadata.partition_specs:
        found_spec_id = spec.spec_id == new_spec_id
        if found_spec_id:
            break

    if not found_spec_id:
        raise ValueError(f"Failed to find spec with id {new_spec_id}")

    context.add_update(update)
    return base_metadata.model_copy(update={"default_spec_id": new_spec_id})


@_apply_table_update.register(AddSnapshotUpdate)
def _(update: AddSnapshotUpdate, base_metadata: TableMetadata, context: _TableMetadataUpdateContext) -> TableMetadata:
    if len(base_metadata.schemas) == 0:
        raise ValueError("Attempting to add a snapshot before a schema is added")
    elif len(base_metadata.partition_specs) == 0:
        raise ValueError("Attempting to add a snapshot before a partition spec is added")
    elif len(base_metadata.sort_orders) == 0:
        raise ValueError("Attempting to add a snapshot before a sort order is added")
    elif base_metadata.snapshot_by_id(update.snapshot.snapshot_id) is not None:
        raise ValueError(f"Snapshot with id {update.snapshot.snapshot_id} already exists")
    elif (
        base_metadata.format_version == 2
        and update.snapshot.sequence_number is not None
        and update.snapshot.sequence_number <= base_metadata.last_sequence_number
        and update.snapshot.parent_snapshot_id is not None
    ):
        raise ValueError(
            f"Cannot add snapshot with sequence number {update.snapshot.sequence_number} "
            f"older than last sequence number {base_metadata.last_sequence_number}"
        )

    context.add_update(update)
    return base_metadata.model_copy(
        update={
            "last_updated_ms": update.snapshot.timestamp_ms,
            "last_sequence_number": update.snapshot.sequence_number,
            "snapshots": base_metadata.snapshots + [update.snapshot],
        }
    )


@_apply_table_update.register(SetSnapshotRefUpdate)
def _(update: SetSnapshotRefUpdate, base_metadata: TableMetadata, context: _TableMetadataUpdateContext) -> TableMetadata:
    snapshot_ref = SnapshotRef(
        snapshot_id=update.snapshot_id,
        snapshot_ref_type=update.type,
        min_snapshots_to_keep=update.min_snapshots_to_keep,
        max_snapshot_age_ms=update.max_snapshot_age_ms,
        max_ref_age_ms=update.max_ref_age_ms,
    )

    existing_ref = base_metadata.refs.get(update.ref_name)
    if existing_ref is not None and existing_ref == snapshot_ref:
        return base_metadata

    snapshot = base_metadata.snapshot_by_id(snapshot_ref.snapshot_id)
    if snapshot is None:
        raise ValueError(f"Cannot set {update.ref_name} to unknown snapshot {snapshot_ref.snapshot_id}")

    metadata_updates: Dict[str, Any] = {}
    if context.is_added_snapshot(snapshot_ref.snapshot_id):
        metadata_updates["last_updated_ms"] = snapshot.timestamp_ms

    if update.ref_name == MAIN_BRANCH:
        metadata_updates["current_snapshot_id"] = snapshot_ref.snapshot_id
        if "last_updated_ms" not in metadata_updates:
            metadata_updates["last_updated_ms"] = datetime_to_millis(datetime.now().astimezone())

        metadata_updates["snapshot_log"] = base_metadata.snapshot_log + [
            SnapshotLogEntry(
                snapshot_id=snapshot_ref.snapshot_id,
                timestamp_ms=metadata_updates["last_updated_ms"],
            )
        ]

    metadata_updates["refs"] = {**base_metadata.refs, update.ref_name: snapshot_ref}
    context.add_update(update)
    return base_metadata.model_copy(update=metadata_updates)


@_apply_table_update.register(AddSortOrderUpdate)
def _(update: AddSortOrderUpdate, base_metadata: TableMetadata, context: _TableMetadataUpdateContext) -> TableMetadata:
    context.add_update(update)
    return base_metadata.model_copy(
        update={
            "sort_orders": [update.sort_order] if update.initial_change else base_metadata.sort_orders + [update.sort_order],
        }
    )


@_apply_table_update.register(SetDefaultSortOrderUpdate)
def _(
    update: SetDefaultSortOrderUpdate,
    base_metadata: TableMetadata,
    context: _TableMetadataUpdateContext,
) -> TableMetadata:
    new_sort_order_id = update.sort_order_id
    if new_sort_order_id == -1:
        # The last added sort order should be in base_metadata.sort_orders at this point
        new_sort_order_id = max(sort_order.order_id for sort_order in base_metadata.sort_orders)
        if not context.is_added_sort_order(new_sort_order_id):
            raise ValueError("Cannot set current sort order to the last added one when no sort order has been added")

    if new_sort_order_id == base_metadata.default_sort_order_id:
        return base_metadata

    sort_order = base_metadata.sort_order_by_id(new_sort_order_id)
    if sort_order is None:
        raise ValueError(f"Sort order with id {new_sort_order_id} does not exist")

    context.add_update(update)
    return base_metadata.model_copy(update={"default_sort_order_id": new_sort_order_id})


def update_table_metadata(
    base_metadata: TableMetadata, updates: Tuple[TableUpdate, ...], enforce_validation: bool = False
) -> TableMetadata:
    """Update the table metadata with the given updates in one transaction.

    Args:
        base_metadata: The base metadata to be updated.
        updates: The updates in one transaction.
        enforce_validation: Whether to trigger validation after applying the updates.

    Returns:
        The metadata with the updates applied.
    """
    context = _TableMetadataUpdateContext()
    new_metadata = base_metadata

    for update in updates:
        new_metadata = _apply_table_update(update, new_metadata, context)

    # Update last_updated_ms if it was not updated by update operations
    if context.has_changes() and base_metadata.last_updated_ms == new_metadata.last_updated_ms:
        new_metadata = new_metadata.model_copy(update={"last_updated_ms": datetime_to_millis(datetime.now().astimezone())})

    if enforce_validation:
        return TableMetadataUtil.parse_obj(new_metadata.model_dump())
    else:
        return new_metadata.model_copy(deep=True)


class ValidatableTableRequirement(IcebergBaseModel):
    type: str

    @abstractmethod
    def validate(self, base_metadata: Optional[TableMetadata]) -> None:
        """Validate the requirement against the base metadata.

        Args:
            base_metadata: The base metadata to be validated against.

        Raises:
            CommitFailedException: When the requirement is not met.
        """
        ...


class AssertCreate(ValidatableTableRequirement):
    """The table must not already exist; used for create transactions."""

    type: Literal["assert-create"] = Field(default="assert-create")

    def validate(self, base_metadata: Optional[TableMetadata]) -> None:
        if base_metadata is not None:
            raise CommitFailedException("Table already exists")


class AssertTableUUID(ValidatableTableRequirement):
    """The table UUID must match the requirement's `uuid`."""

    type: Literal["assert-table-uuid"] = Field(default="assert-table-uuid")
    uuid: uuid.UUID

    def validate(self, base_metadata: Optional[TableMetadata]) -> None:
        if base_metadata is None:
            raise CommitFailedException("Requirement failed: current table metadata is missing")
        elif self.uuid != base_metadata.table_uuid:
            raise CommitFailedException(f"Table UUID does not match: {self.uuid} != {base_metadata.table_uuid}")


class AssertRefSnapshotId(ValidatableTableRequirement):
    """The table branch or tag identified by the requirement's `ref` must reference the requirement's `snapshot-id`.

    if `snapshot-id` is `null` or missing, the ref must not already exist.
    """

    type: Literal["assert-ref-snapshot-id"] = Field(default="assert-ref-snapshot-id")
    ref: str = Field(...)
    snapshot_id: Optional[int] = Field(default=None, alias="snapshot-id")

    def validate(self, base_metadata: Optional[TableMetadata]) -> None:
        if base_metadata is None:
            raise CommitFailedException("Requirement failed: current table metadata is missing")
        elif snapshot_ref := base_metadata.refs.get(self.ref):
            ref_type = snapshot_ref.snapshot_ref_type
            if self.snapshot_id is None:
                raise CommitFailedException(f"Requirement failed: {ref_type} {self.ref} was created concurrently")
            elif self.snapshot_id != snapshot_ref.snapshot_id:
                raise CommitFailedException(
                    f"Requirement failed: {ref_type} {self.ref} has changed: expected id {self.snapshot_id}, found {snapshot_ref.snapshot_id}"
                )
        elif self.snapshot_id is not None:
            raise CommitFailedException(f"Requirement failed: branch or tag {self.ref} is missing, expected {self.snapshot_id}")


class AssertLastAssignedFieldId(ValidatableTableRequirement):
    """The table's last assigned column id must match the requirement's `last-assigned-field-id`."""

    type: Literal["assert-last-assigned-field-id"] = Field(default="assert-last-assigned-field-id")
    last_assigned_field_id: int = Field(..., alias="last-assigned-field-id")

    def validate(self, base_metadata: Optional[TableMetadata]) -> None:
        if base_metadata is None:
            raise CommitFailedException("Requirement failed: current table metadata is missing")
        elif base_metadata.last_column_id != self.last_assigned_field_id:
            raise CommitFailedException(
                f"Requirement failed: last assigned field id has changed: expected {self.last_assigned_field_id}, found {base_metadata.last_column_id}"
            )


class AssertCurrentSchemaId(ValidatableTableRequirement):
    """The table's current schema id must match the requirement's `current-schema-id`."""

    type: Literal["assert-current-schema-id"] = Field(default="assert-current-schema-id")
    current_schema_id: int = Field(..., alias="current-schema-id")

    def validate(self, base_metadata: Optional[TableMetadata]) -> None:
        if base_metadata is None:
            raise CommitFailedException("Requirement failed: current table metadata is missing")
        elif self.current_schema_id != base_metadata.current_schema_id:
            raise CommitFailedException(
                f"Requirement failed: current schema id has changed: expected {self.current_schema_id}, found {base_metadata.current_schema_id}"
            )


class AssertLastAssignedPartitionId(ValidatableTableRequirement):
    """The table's last assigned partition id must match the requirement's `last-assigned-partition-id`."""

    type: Literal["assert-last-assigned-partition-id"] = Field(default="assert-last-assigned-partition-id")
    last_assigned_partition_id: Optional[int] = Field(..., alias="last-assigned-partition-id")

    def validate(self, base_metadata: Optional[TableMetadata]) -> None:
        if base_metadata is None:
            raise CommitFailedException("Requirement failed: current table metadata is missing")
        elif base_metadata.last_partition_id != self.last_assigned_partition_id:
            raise CommitFailedException(
                f"Requirement failed: last assigned partition id has changed: expected {self.last_assigned_partition_id}, found {base_metadata.last_partition_id}"
            )


class AssertDefaultSpecId(ValidatableTableRequirement):
    """The table's default spec id must match the requirement's `default-spec-id`."""

    type: Literal["assert-default-spec-id"] = Field(default="assert-default-spec-id")
    default_spec_id: int = Field(..., alias="default-spec-id")

    def validate(self, base_metadata: Optional[TableMetadata]) -> None:
        if base_metadata is None:
            raise CommitFailedException("Requirement failed: current table metadata is missing")
        elif self.default_spec_id != base_metadata.default_spec_id:
            raise CommitFailedException(
                f"Requirement failed: default spec id has changed: expected {self.default_spec_id}, found {base_metadata.default_spec_id}"
            )


class AssertDefaultSortOrderId(ValidatableTableRequirement):
    """The table's default sort order id must match the requirement's `default-sort-order-id`."""

    type: Literal["assert-default-sort-order-id"] = Field(default="assert-default-sort-order-id")
    default_sort_order_id: int = Field(..., alias="default-sort-order-id")

    def validate(self, base_metadata: Optional[TableMetadata]) -> None:
        if base_metadata is None:
            raise CommitFailedException("Requirement failed: current table metadata is missing")
        elif self.default_sort_order_id != base_metadata.default_sort_order_id:
            raise CommitFailedException(
                f"Requirement failed: default sort order id has changed: expected {self.default_sort_order_id}, found {base_metadata.default_sort_order_id}"
            )


TableRequirement = Annotated[
    Union[
        AssertCreate,
        AssertTableUUID,
        AssertRefSnapshotId,
        AssertLastAssignedFieldId,
        AssertCurrentSchemaId,
        AssertLastAssignedPartitionId,
        AssertDefaultSpecId,
        AssertDefaultSortOrderId,
    ],
    Field(discriminator="type"),
]

UpdatesAndRequirements = Tuple[Tuple[TableUpdate, ...], Tuple[TableRequirement, ...]]


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
    identifier: Identifier = Field()
    metadata: TableMetadata
    metadata_location: str = Field()
    io: FileIO
    catalog: Catalog

    def __init__(
        self, identifier: Identifier, metadata: TableMetadata, metadata_location: str, io: FileIO, catalog: Catalog
    ) -> None:
        self.identifier = identifier
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
        fresh = self.catalog.load_table(self.identifier[1:])
        self.metadata = fresh.metadata
        self.io = fresh.io
        self.metadata_location = fresh.metadata_location
        return self

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

    def add_files(self, file_paths: List[str], snapshot_properties: Dict[str, str] = EMPTY_DICT) -> None:
        """
        Shorthand API for adding files as data files to the table.

        Args:
            file_paths: The list of full file paths to be added as data files to the table

        Raises:
            FileNotFoundError: If the file does not exist.
        """
        with self.transaction() as tx:
            tx.add_files(file_paths=file_paths, snapshot_properties=snapshot_properties)

    def update_spec(self, case_sensitive: bool = True) -> UpdateSpec:
        return UpdateSpec(Transaction(self, autocommit=True), case_sensitive=case_sensitive)

    def refs(self) -> Dict[str, SnapshotRef]:
        """Return the snapshot references in the table."""
        return self.metadata.refs

    def _do_commit(self, updates: Tuple[TableUpdate, ...], requirements: Tuple[TableRequirement, ...]) -> None:
        response = self.catalog._commit_table(  # pylint: disable=W0212
            CommitTableRequest(
                identifier=TableIdentifier(namespace=self.identifier[:-1], name=self.identifier[-1]),
                updates=updates,
                requirements=requirements,
            )
        )  # pylint: disable=W0212
        self.metadata = response.metadata
        self.metadata_location = response.metadata_location

    def __eq__(self, other: Any) -> bool:
        """Return the equality of two instances of the Table class."""
        return (
            self.identifier == other.identifier
            and self.metadata == other.metadata
            and self.metadata_location == other.metadata_location
            if isinstance(other, Table)
            else False
        )

    def __repr__(self) -> str:
        """Return the string representation of the Table class."""
        table_name = self.catalog.table_name_from(self.identifier)
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
            self.table_metadata.schema(), self.row_filter, self.case_sensitive, self.options.get("include_empty_files") == "true"
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
        from pyiceberg.io.pyarrow import project_table

        return project_table(
            self.plan_files(),
            self.table_metadata,
            self.io,
            self.row_filter,
            self.projection(),
            case_sensitive=self.case_sensitive,
            limit=self.limit,
        )

    def to_arrow_batch_reader(self) -> pa.RecordBatchReader:
        import pyarrow as pa

        from pyiceberg.io.pyarrow import project_batches, schema_to_pyarrow

        target_schema = schema_to_pyarrow(self.projection())
        return pa.RecordBatchReader.from_batches(
            target_schema,
            project_batches(
                self.plan_files(),
                self.table_metadata,
                self.io,
                self.row_filter,
                self.projection(),
                case_sensitive=self.case_sensitive,
                limit=self.limit,
            ),
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


class MoveOperation(Enum):
    First = 1
    Before = 2
    After = 3


@dataclass
class Move:
    field_id: int
    full_name: str
    op: MoveOperation
    other_field_id: Optional[int] = None


U = TypeVar("U")


class UpdateTableMetadata(ABC, Generic[U]):
    _transaction: Transaction

    def __init__(self, transaction: Transaction) -> None:
        self._transaction = transaction

    @abstractmethod
    def _commit(self) -> UpdatesAndRequirements: ...

    def commit(self) -> None:
        self._transaction._apply(*self._commit())

    def __exit__(self, _: Any, value: Any, traceback: Any) -> None:
        """Close and commit the change."""
        self.commit()

    def __enter__(self) -> U:
        """Update the table."""
        return self  # type: ignore


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


class UpdateSchema(UpdateTableMetadata["UpdateSchema"]):
    _schema: Schema
    _last_column_id: itertools.count[int]
    _identifier_field_names: Set[str]

    _adds: Dict[int, List[NestedField]] = {}
    _updates: Dict[int, NestedField] = {}
    _deletes: Set[int] = set()
    _moves: Dict[int, List[Move]] = {}

    _added_name_to_id: Dict[str, int] = {}
    # Part of https://github.com/apache/iceberg/pull/8393
    _id_to_parent: Dict[int, str] = {}
    _allow_incompatible_changes: bool
    _case_sensitive: bool

    def __init__(
        self,
        transaction: Transaction,
        allow_incompatible_changes: bool = False,
        case_sensitive: bool = True,
        schema: Optional[Schema] = None,
        name_mapping: Optional[NameMapping] = None,
    ) -> None:
        super().__init__(transaction)

        if isinstance(schema, Schema):
            self._schema = schema
            self._last_column_id = itertools.count(1 + schema.highest_field_id)
        else:
            self._schema = self._transaction.table_metadata.schema()
            self._last_column_id = itertools.count(1 + self._transaction.table_metadata.last_column_id)

        self._name_mapping = name_mapping
        self._identifier_field_names = self._schema.identifier_field_names()

        self._adds = {}
        self._updates = {}
        self._deletes = set()
        self._moves = {}

        self._added_name_to_id = {}

        def get_column_name(field_id: int) -> str:
            column_name = self._schema.find_column_name(column_id=field_id)
            if column_name is None:
                raise ValueError(f"Could not find field-id: {field_id}")
            return column_name

        self._id_to_parent = {
            field_id: get_column_name(parent_field_id) for field_id, parent_field_id in self._schema._lazy_id_to_parent.items()
        }

        self._allow_incompatible_changes = allow_incompatible_changes
        self._case_sensitive = case_sensitive
        self._transaction = transaction

    def case_sensitive(self, case_sensitive: bool) -> UpdateSchema:
        """Determine if the case of schema needs to be considered when comparing column names.

        Args:
            case_sensitive: When false case is not considered in column name comparisons.

        Returns:
            This for method chaining
        """
        self._case_sensitive = case_sensitive
        return self

    def union_by_name(self, new_schema: Union[Schema, "pa.Schema"]) -> UpdateSchema:
        from pyiceberg.catalog import Catalog

        visit_with_partner(
            Catalog._convert_schema_if_needed(new_schema),
            -1,
            UnionByNameVisitor(update_schema=self, existing_schema=self._schema, case_sensitive=self._case_sensitive),  # type: ignore
            PartnerIdByNameAccessor(partner_schema=self._schema, case_sensitive=self._case_sensitive),
        )
        return self

    def add_column(
        self, path: Union[str, Tuple[str, ...]], field_type: IcebergType, doc: Optional[str] = None, required: bool = False
    ) -> UpdateSchema:
        """Add a new column to a nested struct or Add a new top-level column.

        Because "." may be interpreted as a column path separator or may be used in field names, it
        is not allowed to add nested column by passing in a string. To add to nested structures or
        to add fields with names that contain "." use a tuple instead to indicate the path.

        If type is a nested type, its field IDs are reassigned when added to the existing schema.

        Args:
            path: Name for the new column.
            field_type: Type for the new column.
            doc: Documentation string for the new column.
            required: Whether the new column is required.

        Returns:
            This for method chaining.
        """
        if isinstance(path, str):
            if "." in path:
                raise ValueError(f"Cannot add column with ambiguous name: {path}, provide a tuple instead")
            path = (path,)

        if required and not self._allow_incompatible_changes:
            # Table format version 1 and 2 cannot add required column because there is no initial value
            raise ValueError(f'Incompatible change: cannot add required column: {".".join(path)}')

        name = path[-1]
        parent = path[:-1]

        full_name = ".".join(path)
        parent_full_path = ".".join(parent)
        parent_id: int = TABLE_ROOT_ID

        if len(parent) > 0:
            parent_field = self._schema.find_field(parent_full_path, self._case_sensitive)
            parent_type = parent_field.field_type
            if isinstance(parent_type, MapType):
                parent_field = parent_type.value_field
            elif isinstance(parent_type, ListType):
                parent_field = parent_type.element_field

            if not parent_field.field_type.is_struct:
                raise ValueError(f"Cannot add column '{name}' to non-struct type: {parent_full_path}")

            parent_id = parent_field.field_id

        existing_field = None
        try:
            existing_field = self._schema.find_field(full_name, self._case_sensitive)
        except ValueError:
            pass

        if existing_field is not None and existing_field.field_id not in self._deletes:
            raise ValueError(f"Cannot add column, name already exists: {full_name}")

        # assign new IDs in order
        new_id = self.assign_new_column_id()

        # update tracking for moves
        self._added_name_to_id[full_name] = new_id
        self._id_to_parent[new_id] = parent_full_path

        new_type = assign_fresh_schema_ids(field_type, self.assign_new_column_id)
        field = NestedField(field_id=new_id, name=name, field_type=new_type, required=required, doc=doc)

        if parent_id in self._adds:
            self._adds[parent_id].append(field)
        else:
            self._adds[parent_id] = [field]

        return self

    def delete_column(self, path: Union[str, Tuple[str, ...]]) -> UpdateSchema:
        """Delete a column from a table.

        Args:
            path: The path to the column.

        Returns:
            The UpdateSchema with the delete operation staged.
        """
        name = (path,) if isinstance(path, str) else path
        full_name = ".".join(name)

        field = self._schema.find_field(full_name, case_sensitive=self._case_sensitive)

        if field.field_id in self._adds:
            raise ValueError(f"Cannot delete a column that has additions: {full_name}")
        if field.field_id in self._updates:
            raise ValueError(f"Cannot delete a column that has updates: {full_name}")

        self._deletes.add(field.field_id)

        return self

    def rename_column(self, path_from: Union[str, Tuple[str, ...]], new_name: str) -> UpdateSchema:
        """Update the name of a column.

        Args:
            path_from: The path to the column to be renamed.
            new_name: The new path of the column.

        Returns:
            The UpdateSchema with the rename operation staged.
        """
        path_from = ".".join(path_from) if isinstance(path_from, tuple) else path_from
        field_from = self._schema.find_field(path_from, self._case_sensitive)

        if field_from.field_id in self._deletes:
            raise ValueError(f"Cannot rename a column that will be deleted: {path_from}")

        if updated := self._updates.get(field_from.field_id):
            self._updates[field_from.field_id] = NestedField(
                field_id=updated.field_id,
                name=new_name,
                field_type=updated.field_type,
                doc=updated.doc,
                required=updated.required,
            )
        else:
            self._updates[field_from.field_id] = NestedField(
                field_id=field_from.field_id,
                name=new_name,
                field_type=field_from.field_type,
                doc=field_from.doc,
                required=field_from.required,
            )

        # Lookup the field because of casing
        from_field_correct_casing = self._schema.find_column_name(field_from.field_id)
        if from_field_correct_casing in self._identifier_field_names:
            self._identifier_field_names.remove(from_field_correct_casing)
            new_identifier_path = f"{from_field_correct_casing[: -len(field_from.name)]}{new_name}"
            self._identifier_field_names.add(new_identifier_path)

        return self

    def make_column_optional(self, path: Union[str, Tuple[str, ...]]) -> UpdateSchema:
        """Make a column optional.

        Args:
            path: The path to the field.

        Returns:
            The UpdateSchema with the requirement change staged.
        """
        self._set_column_requirement(path, required=False)
        return self

    def set_identifier_fields(self, *fields: str) -> None:
        self._identifier_field_names = set(fields)

    def _set_column_requirement(self, path: Union[str, Tuple[str, ...]], required: bool) -> None:
        path = (path,) if isinstance(path, str) else path
        name = ".".join(path)

        field = self._schema.find_field(name, self._case_sensitive)

        if (field.required and required) or (field.optional and not required):
            # if the change is a noop, allow it even if allowIncompatibleChanges is false
            return

        if not self._allow_incompatible_changes and required:
            raise ValueError(f"Cannot change column nullability: {name}: optional -> required")

        if field.field_id in self._deletes:
            raise ValueError(f"Cannot update a column that will be deleted: {name}")

        if updated := self._updates.get(field.field_id):
            self._updates[field.field_id] = NestedField(
                field_id=updated.field_id,
                name=updated.name,
                field_type=updated.field_type,
                doc=updated.doc,
                required=required,
            )
        else:
            self._updates[field.field_id] = NestedField(
                field_id=field.field_id,
                name=field.name,
                field_type=field.field_type,
                doc=field.doc,
                required=required,
            )

    def update_column(
        self,
        path: Union[str, Tuple[str, ...]],
        field_type: Optional[IcebergType] = None,
        required: Optional[bool] = None,
        doc: Optional[str] = None,
    ) -> UpdateSchema:
        """Update the type of column.

        Args:
            path: The path to the field.
            field_type: The new type
            required: If the field should be required
            doc: Documentation describing the column

        Returns:
            The UpdateSchema with the type update staged.
        """
        path = (path,) if isinstance(path, str) else path
        full_name = ".".join(path)

        if field_type is None and required is None and doc is None:
            return self

        field = self._schema.find_field(full_name, self._case_sensitive)

        if field.field_id in self._deletes:
            raise ValueError(f"Cannot update a column that will be deleted: {full_name}")

        if field_type is not None:
            if not field.field_type.is_primitive:
                raise ValidationError(f"Cannot change column type: {field.field_type} is not a primitive")

            if not self._allow_incompatible_changes and field.field_type != field_type:
                try:
                    promote(field.field_type, field_type)
                except ResolveError as e:
                    raise ValidationError(f"Cannot change column type: {full_name}: {field.field_type} -> {field_type}") from e

        if updated := self._updates.get(field.field_id):
            self._updates[field.field_id] = NestedField(
                field_id=updated.field_id,
                name=updated.name,
                field_type=field_type or updated.field_type,
                doc=doc or updated.doc,
                required=updated.required,
            )
        else:
            self._updates[field.field_id] = NestedField(
                field_id=field.field_id,
                name=field.name,
                field_type=field_type or field.field_type,
                doc=doc or field.doc,
                required=field.required,
            )

        if required is not None:
            self._set_column_requirement(path, required=required)

        return self

    def _find_for_move(self, name: str) -> Optional[int]:
        try:
            return self._schema.find_field(name, self._case_sensitive).field_id
        except ValueError:
            pass

        return self._added_name_to_id.get(name)

    def _move(self, move: Move) -> None:
        if parent_name := self._id_to_parent.get(move.field_id):
            parent_field = self._schema.find_field(parent_name, case_sensitive=self._case_sensitive)
            if not parent_field.field_type.is_struct:
                raise ValueError(f"Cannot move fields in non-struct type: {parent_field.field_type}")

            if move.op == MoveOperation.After or move.op == MoveOperation.Before:
                if move.other_field_id is None:
                    raise ValueError("Expected other field when performing before/after move")

                if self._id_to_parent.get(move.field_id) != self._id_to_parent.get(move.other_field_id):
                    raise ValueError(f"Cannot move field {move.full_name} to a different struct")

            self._moves[parent_field.field_id] = self._moves.get(parent_field.field_id, []) + [move]
        else:
            # In the top level field
            if move.op == MoveOperation.After or move.op == MoveOperation.Before:
                if move.other_field_id is None:
                    raise ValueError("Expected other field when performing before/after move")

                if other_struct := self._id_to_parent.get(move.other_field_id):
                    raise ValueError(f"Cannot move field {move.full_name} to a different struct: {other_struct}")

            self._moves[TABLE_ROOT_ID] = self._moves.get(TABLE_ROOT_ID, []) + [move]

    def move_first(self, path: Union[str, Tuple[str, ...]]) -> UpdateSchema:
        """Move the field to the first position of the parent struct.

        Args:
            path: The path to the field.

        Returns:
            The UpdateSchema with the move operation staged.
        """
        full_name = ".".join(path) if isinstance(path, tuple) else path

        field_id = self._find_for_move(full_name)

        if field_id is None:
            raise ValueError(f"Cannot move missing column: {full_name}")

        self._move(Move(field_id=field_id, full_name=full_name, op=MoveOperation.First))

        return self

    def move_before(self, path: Union[str, Tuple[str, ...]], before_path: Union[str, Tuple[str, ...]]) -> UpdateSchema:
        """Move the field to before another field.

        Args:
            path: The path to the field.

        Returns:
            The UpdateSchema with the move operation staged.
        """
        full_name = ".".join(path) if isinstance(path, tuple) else path
        field_id = self._find_for_move(full_name)

        if field_id is None:
            raise ValueError(f"Cannot move missing column: {full_name}")

        before_full_name = (
            ".".join(
                before_path,
            )
            if isinstance(before_path, tuple)
            else before_path
        )
        before_field_id = self._find_for_move(before_full_name)

        if before_field_id is None:
            raise ValueError(f"Cannot move {full_name} before missing column: {before_full_name}")

        if field_id == before_field_id:
            raise ValueError(f"Cannot move {full_name} before itself")

        self._move(Move(field_id=field_id, full_name=full_name, other_field_id=before_field_id, op=MoveOperation.Before))

        return self

    def move_after(self, path: Union[str, Tuple[str, ...]], after_name: Union[str, Tuple[str, ...]]) -> UpdateSchema:
        """Move the field to after another field.

        Args:
            path: The path to the field.

        Returns:
            The UpdateSchema with the move operation staged.
        """
        full_name = ".".join(path) if isinstance(path, tuple) else path

        field_id = self._find_for_move(full_name)

        if field_id is None:
            raise ValueError(f"Cannot move missing column: {full_name}")

        after_path = ".".join(after_name) if isinstance(after_name, tuple) else after_name
        after_field_id = self._find_for_move(after_path)

        if after_field_id is None:
            raise ValueError(f"Cannot move {full_name} after missing column: {after_path}")

        if field_id == after_field_id:
            raise ValueError(f"Cannot move {full_name} after itself")

        self._move(Move(field_id=field_id, full_name=full_name, other_field_id=after_field_id, op=MoveOperation.After))

        return self

    def _commit(self) -> UpdatesAndRequirements:
        """Apply the pending changes and commit."""
        new_schema = self._apply()

        existing_schema_id = next(
            (schema.schema_id for schema in self._transaction.table_metadata.schemas if schema == new_schema), None
        )

        requirements: Tuple[TableRequirement, ...] = ()
        updates: Tuple[TableUpdate, ...] = ()

        # Check if it is different current schema ID
        if existing_schema_id != self._schema.schema_id:
            requirements += (AssertCurrentSchemaId(current_schema_id=self._schema.schema_id),)
            if existing_schema_id is None:
                last_column_id = max(self._transaction.table_metadata.last_column_id, new_schema.highest_field_id)
                updates += (
                    AddSchemaUpdate(schema=new_schema, last_column_id=last_column_id),
                    SetCurrentSchemaUpdate(schema_id=-1),
                )
            else:
                updates += (SetCurrentSchemaUpdate(schema_id=existing_schema_id),)

            if name_mapping := self._name_mapping:
                updated_name_mapping = update_mapping(name_mapping, self._updates, self._adds)
                updates += (
                    SetPropertiesUpdate(updates={TableProperties.DEFAULT_NAME_MAPPING: updated_name_mapping.model_dump_json()}),
                )

        return updates, requirements

    def _apply(self) -> Schema:
        """Apply the pending changes to the original schema and returns the result.

        Returns:
            the result Schema when all pending updates are applied
        """
        struct = visit(self._schema, _ApplyChanges(self._adds, self._updates, self._deletes, self._moves))
        if struct is None:
            # Should never happen
            raise ValueError("Could not apply changes")

        # Check the field-ids
        new_schema = Schema(*struct.fields)
        field_ids = set()
        for name in self._identifier_field_names:
            try:
                field = new_schema.find_field(name, case_sensitive=self._case_sensitive)
            except ValueError as e:
                raise ValueError(
                    f"Cannot find identifier field {name}. In case of deletion, update the identifier fields first."
                ) from e

            field_ids.add(field.field_id)

        if txn := self._transaction:
            next_schema_id = 1 + (
                max(schema.schema_id for schema in txn.table_metadata.schemas) if txn.table_metadata is not None else 0
            )
        else:
            next_schema_id = 0

        return Schema(*struct.fields, schema_id=next_schema_id, identifier_field_ids=field_ids)

    def assign_new_column_id(self) -> int:
        return next(self._last_column_id)


class _ApplyChanges(SchemaVisitor[Optional[IcebergType]]):
    _adds: Dict[int, List[NestedField]]
    _updates: Dict[int, NestedField]
    _deletes: Set[int]
    _moves: Dict[int, List[Move]]

    def __init__(
        self, adds: Dict[int, List[NestedField]], updates: Dict[int, NestedField], deletes: Set[int], moves: Dict[int, List[Move]]
    ) -> None:
        self._adds = adds
        self._updates = updates
        self._deletes = deletes
        self._moves = moves

    def schema(self, schema: Schema, struct_result: Optional[IcebergType]) -> Optional[IcebergType]:
        added = self._adds.get(TABLE_ROOT_ID)
        moves = self._moves.get(TABLE_ROOT_ID)

        if added is not None or moves is not None:
            if not isinstance(struct_result, StructType):
                raise ValueError(f"Cannot add fields to non-struct: {struct_result}")

            if new_fields := _add_and_move_fields(struct_result.fields, added or [], moves or []):
                return StructType(*new_fields)

        return struct_result

    def struct(self, struct: StructType, field_results: List[Optional[IcebergType]]) -> Optional[IcebergType]:
        has_changes = False
        new_fields = []

        for idx, result_type in enumerate(field_results):
            result_type = field_results[idx]

            # Has been deleted
            if result_type is None:
                has_changes = True
                continue

            field = struct.fields[idx]

            name = field.name
            doc = field.doc
            required = field.required

            # There is an update
            if update := self._updates.get(field.field_id):
                name = update.name
                doc = update.doc
                required = update.required

            if field.name == name and field.field_type == result_type and field.required == required and field.doc == doc:
                new_fields.append(field)
            else:
                has_changes = True
                new_fields.append(
                    NestedField(field_id=field.field_id, name=name, field_type=result_type, required=required, doc=doc)
                )

        if has_changes:
            return StructType(*new_fields)

        return struct

    def field(self, field: NestedField, field_result: Optional[IcebergType]) -> Optional[IcebergType]:
        # the API validates deletes, updates, and additions don't conflict handle deletes
        if field.field_id in self._deletes:
            return None

        # handle updates
        if (update := self._updates.get(field.field_id)) and field.field_type != update.field_type:
            return update.field_type

        if isinstance(field_result, StructType):
            # handle add & moves
            added = self._adds.get(field.field_id)
            moves = self._moves.get(field.field_id)
            if added is not None or moves is not None:
                if not isinstance(field.field_type, StructType):
                    raise ValueError(f"Cannot add fields to non-struct: {field}")

                if new_fields := _add_and_move_fields(field_result.fields, added or [], moves or []):
                    return StructType(*new_fields)

        return field_result

    def list(self, list_type: ListType, element_result: Optional[IcebergType]) -> Optional[IcebergType]:
        element_type = self.field(list_type.element_field, element_result)
        if element_type is None:
            raise ValueError(f"Cannot delete element type from list: {element_result}")

        return ListType(element_id=list_type.element_id, element=element_type, element_required=list_type.element_required)

    def map(
        self, map_type: MapType, key_result: Optional[IcebergType], value_result: Optional[IcebergType]
    ) -> Optional[IcebergType]:
        key_id: int = map_type.key_field.field_id

        if key_id in self._deletes:
            raise ValueError(f"Cannot delete map keys: {map_type}")

        if key_id in self._updates:
            raise ValueError(f"Cannot update map keys: {map_type}")

        if key_id in self._adds:
            raise ValueError(f"Cannot add fields to map keys: {map_type}")

        if map_type.key_type != key_result:
            raise ValueError(f"Cannot alter map keys: {map_type}")

        value_field: NestedField = map_type.value_field
        value_type = self.field(value_field, value_result)
        if value_type is None:
            raise ValueError(f"Cannot delete value type from map: {value_field}")

        return MapType(
            key_id=map_type.key_id,
            key_type=map_type.key_type,
            value_id=map_type.value_id,
            value_type=value_type,
            value_required=map_type.value_required,
        )

    def primitive(self, primitive: PrimitiveType) -> Optional[IcebergType]:
        return primitive


class UnionByNameVisitor(SchemaWithPartnerVisitor[int, bool]):
    update_schema: UpdateSchema
    existing_schema: Schema
    case_sensitive: bool

    def __init__(self, update_schema: UpdateSchema, existing_schema: Schema, case_sensitive: bool) -> None:
        self.update_schema = update_schema
        self.existing_schema = existing_schema
        self.case_sensitive = case_sensitive

    def schema(self, schema: Schema, partner_id: Optional[int], struct_result: bool) -> bool:
        return struct_result

    def struct(self, struct: StructType, partner_id: Optional[int], missing_positions: List[bool]) -> bool:
        if partner_id is None:
            return True

        fields = struct.fields
        partner_struct = self._find_field_type(partner_id)

        if not partner_struct.is_struct:
            raise ValueError(f"Expected a struct, got: {partner_struct}")

        for pos, missing in enumerate(missing_positions):
            if missing:
                self._add_column(partner_id, fields[pos])
            else:
                field = fields[pos]
                if nested_field := partner_struct.field_by_name(field.name, case_sensitive=self.case_sensitive):
                    self._update_column(field, nested_field)

        return False

    def _add_column(self, parent_id: int, field: NestedField) -> None:
        if parent_name := self.existing_schema.find_column_name(parent_id):
            path: Tuple[str, ...] = (parent_name, field.name)
        else:
            path = (field.name,)

        self.update_schema.add_column(path=path, field_type=field.field_type, required=field.required, doc=field.doc)

    def _update_column(self, field: NestedField, existing_field: NestedField) -> None:
        full_name = self.existing_schema.find_column_name(existing_field.field_id)

        if full_name is None:
            raise ValueError(f"Could not find field: {existing_field}")

        if field.optional and existing_field.required:
            self.update_schema.make_column_optional(full_name)

        if field.field_type.is_primitive and field.field_type != existing_field.field_type:
            self.update_schema.update_column(full_name, field_type=field.field_type)

        if field.doc is not None and not field.doc != existing_field.doc:
            self.update_schema.update_column(full_name, doc=field.doc)

    def _find_field_type(self, field_id: int) -> IcebergType:
        if field_id == -1:
            return self.existing_schema.as_struct()
        else:
            return self.existing_schema.find_field(field_id).field_type

    def field(self, field: NestedField, partner_id: Optional[int], field_result: bool) -> bool:
        return partner_id is None

    def list(self, list_type: ListType, list_partner_id: Optional[int], element_missing: bool) -> bool:
        if list_partner_id is None:
            return True

        if element_missing:
            raise ValueError("Error traversing schemas: element is missing, but list is present")

        partner_list_type = self._find_field_type(list_partner_id)
        if not isinstance(partner_list_type, ListType):
            raise ValueError(f"Expected list-type, got: {partner_list_type}")

        self._update_column(list_type.element_field, partner_list_type.element_field)

        return False

    def map(self, map_type: MapType, map_partner_id: Optional[int], key_missing: bool, value_missing: bool) -> bool:
        if map_partner_id is None:
            return True

        if key_missing:
            raise ValueError("Error traversing schemas: key is missing, but map is present")

        if value_missing:
            raise ValueError("Error traversing schemas: value is missing, but map is present")

        partner_map_type = self._find_field_type(map_partner_id)
        if not isinstance(partner_map_type, MapType):
            raise ValueError(f"Expected map-type, got: {partner_map_type}")

        self._update_column(map_type.key_field, partner_map_type.key_field)
        self._update_column(map_type.value_field, partner_map_type.value_field)

        return False

    def primitive(self, primitive: PrimitiveType, primitive_partner_id: Optional[int]) -> bool:
        return primitive_partner_id is None


class PartnerIdByNameAccessor(PartnerAccessor[int]):
    partner_schema: Schema
    case_sensitive: bool

    def __init__(self, partner_schema: Schema, case_sensitive: bool) -> None:
        self.partner_schema = partner_schema
        self.case_sensitive = case_sensitive

    def schema_partner(self, partner: Optional[int]) -> Optional[int]:
        return -1

    def field_partner(self, partner_field_id: Optional[int], field_id: int, field_name: str) -> Optional[int]:
        if partner_field_id is not None:
            if partner_field_id == -1:
                struct = self.partner_schema.as_struct()
            else:
                struct = self.partner_schema.find_field(partner_field_id).field_type
                if not struct.is_struct:
                    raise ValueError(f"Expected StructType: {struct}")

            if field := struct.field_by_name(name=field_name, case_sensitive=self.case_sensitive):
                return field.field_id

        return None

    def list_element_partner(self, partner_list_id: Optional[int]) -> Optional[int]:
        if partner_list_id is not None and (field := self.partner_schema.find_field(partner_list_id)):
            if not isinstance(field.field_type, ListType):
                raise ValueError(f"Expected ListType: {field}")
            return field.field_type.element_field.field_id
        else:
            return None

    def map_key_partner(self, partner_map_id: Optional[int]) -> Optional[int]:
        if partner_map_id is not None and (field := self.partner_schema.find_field(partner_map_id)):
            if not isinstance(field.field_type, MapType):
                raise ValueError(f"Expected MapType: {field}")
            return field.field_type.key_field.field_id
        else:
            return None

    def map_value_partner(self, partner_map_id: Optional[int]) -> Optional[int]:
        if partner_map_id is not None and (field := self.partner_schema.find_field(partner_map_id)):
            if not isinstance(field.field_type, MapType):
                raise ValueError(f"Expected MapType: {field}")
            return field.field_type.value_field.field_id
        else:
            return None


def _add_fields(fields: Tuple[NestedField, ...], adds: Optional[List[NestedField]]) -> Tuple[NestedField, ...]:
    adds = adds or []
    return fields + tuple(adds)


def _move_fields(fields: Tuple[NestedField, ...], moves: List[Move]) -> Tuple[NestedField, ...]:
    reordered = list(copy(fields))
    for move in moves:
        # Find the field that we're about to move
        field = next(field for field in reordered if field.field_id == move.field_id)
        # Remove the field that we're about to move from the list
        reordered = [field for field in reordered if field.field_id != move.field_id]

        if move.op == MoveOperation.First:
            reordered = [field] + reordered
        elif move.op == MoveOperation.Before or move.op == MoveOperation.After:
            other_field_id = move.other_field_id
            other_field_pos = next(i for i, field in enumerate(reordered) if field.field_id == other_field_id)
            if move.op == MoveOperation.Before:
                reordered.insert(other_field_pos, field)
            else:
                reordered.insert(other_field_pos + 1, field)
        else:
            raise ValueError(f"Unknown operation: {move.op}")

    return tuple(reordered)


def _add_and_move_fields(
    fields: Tuple[NestedField, ...], adds: List[NestedField], moves: List[Move]
) -> Optional[Tuple[NestedField, ...]]:
    if len(adds) > 0:
        # always apply adds first so that added fields can be moved
        added = _add_fields(fields, adds)
        if len(moves) > 0:
            return _move_fields(added, moves)
        else:
            return added
    elif len(moves) > 0:
        return _move_fields(fields, moves)
    return None if len(adds) == 0 else tuple(*fields, *adds)


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
                with write_manifest(
                    format_version=self._transaction.table_metadata.format_version,
                    spec=self._transaction.table_metadata.spec(),
                    schema=self._transaction.table_metadata.schema(),
                    output_file=self.new_manifest_output(),
                    snapshot_id=self._snapshot_id,
                ) as writer:
                    for delete_entry in deleted_entries:
                        writer.add_entry(delete_entry)
                return [writer.to_manifest_file()]
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
                                deleted_entries.append(_copy_with_new_status(entry, ManifestEntryStatus.DELETED))
                                self._deleted_data_files.add(entry.data_file)
                            elif inclusive_metrics_evaluator(entry.data_file) == ROWS_CANNOT_MATCH:
                                existing_entries.append(_copy_with_new_status(entry, ManifestEntryStatus.EXISTING))
                            else:
                                # Based on the metadata, it is unsure to say if the file can be deleted
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
                            # else:
                            # deleted_manifests.append()
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
        self._target_size_bytes = PropertyUtil.property_as_int(
            self._transaction.table_metadata.properties,
            TableProperties.MANIFEST_TARGET_SIZE_BYTES,
            TableProperties.MANIFEST_TARGET_SIZE_BYTES_DEFAULT,
        )  # type: ignore
        self._min_count_to_merge = PropertyUtil.property_as_int(
            self._transaction.table_metadata.properties,
            TableProperties.MANIFEST_MIN_MERGE_COUNT,
            TableProperties.MANIFEST_MIN_MERGE_COUNT_DEFAULT,
        )  # type: ignore
        self._merge_enabled = PropertyUtil.property_as_bool(
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


class InspectTable:
    tbl: Table

    def __init__(self, tbl: Table) -> None:
        self.tbl = tbl

        try:
            import pyarrow as pa  # noqa
        except ModuleNotFoundError as e:
            raise ModuleNotFoundError("For metadata operations PyArrow needs to be installed") from e

    def _get_snapshot(self, snapshot_id: Optional[int] = None) -> Snapshot:
        if snapshot_id is not None:
            if snapshot := self.tbl.metadata.snapshot_by_id(snapshot_id):
                return snapshot
            else:
                raise ValueError(f"Cannot find snapshot with ID {snapshot_id}")

        if snapshot := self.tbl.metadata.current_snapshot():
            return snapshot
        else:
            raise ValueError("Cannot get a snapshot as the table does not have any.")

    def snapshots(self) -> "pa.Table":
        import pyarrow as pa

        snapshots_schema = pa.schema([
            pa.field("committed_at", pa.timestamp(unit="ms"), nullable=False),
            pa.field("snapshot_id", pa.int64(), nullable=False),
            pa.field("parent_id", pa.int64(), nullable=True),
            pa.field("operation", pa.string(), nullable=True),
            pa.field("manifest_list", pa.string(), nullable=False),
            pa.field("summary", pa.map_(pa.string(), pa.string()), nullable=True),
        ])
        snapshots = []
        for snapshot in self.tbl.metadata.snapshots:
            if summary := snapshot.summary:
                operation = summary.operation.value
                additional_properties = snapshot.summary.additional_properties
            else:
                operation = None
                additional_properties = None

            snapshots.append({
                "committed_at": datetime.utcfromtimestamp(snapshot.timestamp_ms / 1000.0),
                "snapshot_id": snapshot.snapshot_id,
                "parent_id": snapshot.parent_snapshot_id,
                "operation": str(operation),
                "manifest_list": snapshot.manifest_list,
                "summary": additional_properties,
            })

        return pa.Table.from_pylist(
            snapshots,
            schema=snapshots_schema,
        )

    def entries(self, snapshot_id: Optional[int] = None) -> "pa.Table":
        import pyarrow as pa

        from pyiceberg.io.pyarrow import schema_to_pyarrow

        schema = self.tbl.metadata.schema()

        readable_metrics_struct = []

        def _readable_metrics_struct(bound_type: PrimitiveType) -> pa.StructType:
            pa_bound_type = schema_to_pyarrow(bound_type)
            return pa.struct([
                pa.field("column_size", pa.int64(), nullable=True),
                pa.field("value_count", pa.int64(), nullable=True),
                pa.field("null_value_count", pa.int64(), nullable=True),
                pa.field("nan_value_count", pa.int64(), nullable=True),
                pa.field("lower_bound", pa_bound_type, nullable=True),
                pa.field("upper_bound", pa_bound_type, nullable=True),
            ])

        for field in self.tbl.metadata.schema().fields:
            readable_metrics_struct.append(
                pa.field(schema.find_column_name(field.field_id), _readable_metrics_struct(field.field_type), nullable=False)
            )

        partition_record = self.tbl.metadata.specs_struct()
        pa_record_struct = schema_to_pyarrow(partition_record)

        entries_schema = pa.schema([
            pa.field("status", pa.int8(), nullable=False),
            pa.field("snapshot_id", pa.int64(), nullable=False),
            pa.field("sequence_number", pa.int64(), nullable=False),
            pa.field("file_sequence_number", pa.int64(), nullable=False),
            pa.field(
                "data_file",
                pa.struct([
                    pa.field("content", pa.int8(), nullable=False),
                    pa.field("file_path", pa.string(), nullable=False),
                    pa.field("file_format", pa.string(), nullable=False),
                    pa.field("partition", pa_record_struct, nullable=False),
                    pa.field("record_count", pa.int64(), nullable=False),
                    pa.field("file_size_in_bytes", pa.int64(), nullable=False),
                    pa.field("column_sizes", pa.map_(pa.int32(), pa.int64()), nullable=True),
                    pa.field("value_counts", pa.map_(pa.int32(), pa.int64()), nullable=True),
                    pa.field("null_value_counts", pa.map_(pa.int32(), pa.int64()), nullable=True),
                    pa.field("nan_value_counts", pa.map_(pa.int32(), pa.int64()), nullable=True),
                    pa.field("lower_bounds", pa.map_(pa.int32(), pa.binary()), nullable=True),
                    pa.field("upper_bounds", pa.map_(pa.int32(), pa.binary()), nullable=True),
                    pa.field("key_metadata", pa.binary(), nullable=True),
                    pa.field("split_offsets", pa.list_(pa.int64()), nullable=True),
                    pa.field("equality_ids", pa.list_(pa.int32()), nullable=True),
                    pa.field("sort_order_id", pa.int32(), nullable=True),
                ]),
                nullable=False,
            ),
            pa.field("readable_metrics", pa.struct(readable_metrics_struct), nullable=True),
        ])

        entries = []
        snapshot = self._get_snapshot(snapshot_id)
        for manifest in snapshot.manifests(self.tbl.io):
            for entry in manifest.fetch_manifest_entry(io=self.tbl.io):
                column_sizes = entry.data_file.column_sizes or {}
                value_counts = entry.data_file.value_counts or {}
                null_value_counts = entry.data_file.null_value_counts or {}
                nan_value_counts = entry.data_file.nan_value_counts or {}
                lower_bounds = entry.data_file.lower_bounds or {}
                upper_bounds = entry.data_file.upper_bounds or {}
                readable_metrics = {
                    schema.find_column_name(field.field_id): {
                        "column_size": column_sizes.get(field.field_id),
                        "value_count": value_counts.get(field.field_id),
                        "null_value_count": null_value_counts.get(field.field_id),
                        "nan_value_count": nan_value_counts.get(field.field_id),
                        # Makes them readable
                        "lower_bound": from_bytes(field.field_type, lower_bound)
                        if (lower_bound := lower_bounds.get(field.field_id))
                        else None,
                        "upper_bound": from_bytes(field.field_type, upper_bound)
                        if (upper_bound := upper_bounds.get(field.field_id))
                        else None,
                    }
                    for field in self.tbl.metadata.schema().fields
                }

                partition = entry.data_file.partition
                partition_record_dict = {
                    field.name: partition[pos]
                    for pos, field in enumerate(self.tbl.metadata.specs()[manifest.partition_spec_id].fields)
                }

                entries.append({
                    "status": entry.status.value,
                    "snapshot_id": entry.snapshot_id,
                    "sequence_number": entry.sequence_number,
                    "file_sequence_number": entry.file_sequence_number,
                    "data_file": {
                        "content": entry.data_file.content,
                        "file_path": entry.data_file.file_path,
                        "file_format": entry.data_file.file_format,
                        "partition": partition_record_dict,
                        "record_count": entry.data_file.record_count,
                        "file_size_in_bytes": entry.data_file.file_size_in_bytes,
                        "column_sizes": dict(entry.data_file.column_sizes),
                        "value_counts": dict(entry.data_file.value_counts),
                        "null_value_counts": dict(entry.data_file.null_value_counts),
                        "nan_value_counts": entry.data_file.nan_value_counts,
                        "lower_bounds": entry.data_file.lower_bounds,
                        "upper_bounds": entry.data_file.upper_bounds,
                        "key_metadata": entry.data_file.key_metadata,
                        "split_offsets": entry.data_file.split_offsets,
                        "equality_ids": entry.data_file.equality_ids,
                        "sort_order_id": entry.data_file.sort_order_id,
                        "spec_id": entry.data_file.spec_id,
                    },
                    "readable_metrics": readable_metrics,
                })

        return pa.Table.from_pylist(
            entries,
            schema=entries_schema,
        )

    def refs(self) -> "pa.Table":
        import pyarrow as pa

        ref_schema = pa.schema([
            pa.field("name", pa.string(), nullable=False),
            pa.field("type", pa.dictionary(pa.int32(), pa.string()), nullable=False),
            pa.field("snapshot_id", pa.int64(), nullable=False),
            pa.field("max_reference_age_in_ms", pa.int64(), nullable=True),
            pa.field("min_snapshots_to_keep", pa.int32(), nullable=True),
            pa.field("max_snapshot_age_in_ms", pa.int64(), nullable=True),
        ])

        ref_results = []
        for ref in self.tbl.metadata.refs:
            if snapshot_ref := self.tbl.metadata.refs.get(ref):
                ref_results.append({
                    "name": ref,
                    "type": snapshot_ref.snapshot_ref_type.upper(),
                    "snapshot_id": snapshot_ref.snapshot_id,
                    "max_reference_age_in_ms": snapshot_ref.max_ref_age_ms,
                    "min_snapshots_to_keep": snapshot_ref.min_snapshots_to_keep,
                    "max_snapshot_age_in_ms": snapshot_ref.max_snapshot_age_ms,
                })

        return pa.Table.from_pylist(ref_results, schema=ref_schema)

    def partitions(self, snapshot_id: Optional[int] = None) -> "pa.Table":
        import pyarrow as pa

        from pyiceberg.io.pyarrow import schema_to_pyarrow

        table_schema = pa.schema([
            pa.field("record_count", pa.int64(), nullable=False),
            pa.field("file_count", pa.int32(), nullable=False),
            pa.field("total_data_file_size_in_bytes", pa.int64(), nullable=False),
            pa.field("position_delete_record_count", pa.int64(), nullable=False),
            pa.field("position_delete_file_count", pa.int32(), nullable=False),
            pa.field("equality_delete_record_count", pa.int64(), nullable=False),
            pa.field("equality_delete_file_count", pa.int32(), nullable=False),
            pa.field("last_updated_at", pa.timestamp(unit="ms"), nullable=True),
            pa.field("last_updated_snapshot_id", pa.int64(), nullable=True),
        ])

        partition_record = self.tbl.metadata.specs_struct()
        has_partitions = len(partition_record.fields) > 0

        if has_partitions:
            pa_record_struct = schema_to_pyarrow(partition_record)
            partitions_schema = pa.schema([
                pa.field("partition", pa_record_struct, nullable=False),
                pa.field("spec_id", pa.int32(), nullable=False),
            ])

            table_schema = pa.unify_schemas([partitions_schema, table_schema])

        def update_partitions_map(
            partitions_map: Dict[Tuple[str, Any], Any],
            file: DataFile,
            partition_record_dict: Dict[str, Any],
            snapshot: Optional[Snapshot],
        ) -> None:
            partition_record_key = _convert_to_hashable_type(partition_record_dict)
            if partition_record_key not in partitions_map:
                partitions_map[partition_record_key] = {
                    "partition": partition_record_dict,
                    "spec_id": file.spec_id,
                    "record_count": 0,
                    "file_count": 0,
                    "total_data_file_size_in_bytes": 0,
                    "position_delete_record_count": 0,
                    "position_delete_file_count": 0,
                    "equality_delete_record_count": 0,
                    "equality_delete_file_count": 0,
                    "last_updated_at": snapshot.timestamp_ms if snapshot else None,
                    "last_updated_snapshot_id": snapshot.snapshot_id if snapshot else None,
                }

            partition_row = partitions_map[partition_record_key]

            if snapshot is not None:
                if partition_row["last_updated_at"] is None or partition_row["last_updated_snapshot_id"] < snapshot.timestamp_ms:
                    partition_row["last_updated_at"] = snapshot.timestamp_ms
                    partition_row["last_updated_snapshot_id"] = snapshot.snapshot_id

            if file.content == DataFileContent.DATA:
                partition_row["record_count"] += file.record_count
                partition_row["file_count"] += 1
                partition_row["total_data_file_size_in_bytes"] += file.file_size_in_bytes
            elif file.content == DataFileContent.POSITION_DELETES:
                partition_row["position_delete_record_count"] += file.record_count
                partition_row["position_delete_file_count"] += 1
            elif file.content == DataFileContent.EQUALITY_DELETES:
                partition_row["equality_delete_record_count"] += file.record_count
                partition_row["equality_delete_file_count"] += 1
            else:
                raise ValueError(f"Unknown DataFileContent ({file.content})")

        partitions_map: Dict[Tuple[str, Any], Any] = {}
        snapshot = self._get_snapshot(snapshot_id)
        for manifest in snapshot.manifests(self.tbl.io):
            for entry in manifest.fetch_manifest_entry(io=self.tbl.io):
                partition = entry.data_file.partition
                partition_record_dict = {
                    field.name: partition[pos]
                    for pos, field in enumerate(self.tbl.metadata.specs()[manifest.partition_spec_id].fields)
                }
                entry_snapshot = self.tbl.snapshot_by_id(entry.snapshot_id) if entry.snapshot_id is not None else None
                update_partitions_map(partitions_map, entry.data_file, partition_record_dict, entry_snapshot)

        return pa.Table.from_pylist(
            partitions_map.values(),
            schema=table_schema,
        )

    def manifests(self) -> "pa.Table":
        import pyarrow as pa

        from pyiceberg.conversions import from_bytes

        partition_summary_schema = pa.struct([
            pa.field("contains_null", pa.bool_(), nullable=False),
            pa.field("contains_nan", pa.bool_(), nullable=True),
            pa.field("lower_bound", pa.string(), nullable=True),
            pa.field("upper_bound", pa.string(), nullable=True),
        ])

        manifest_schema = pa.schema([
            pa.field("content", pa.int8(), nullable=False),
            pa.field("path", pa.string(), nullable=False),
            pa.field("length", pa.int64(), nullable=False),
            pa.field("partition_spec_id", pa.int32(), nullable=False),
            pa.field("added_snapshot_id", pa.int64(), nullable=False),
            pa.field("added_data_files_count", pa.int32(), nullable=False),
            pa.field("existing_data_files_count", pa.int32(), nullable=False),
            pa.field("deleted_data_files_count", pa.int32(), nullable=False),
            pa.field("added_delete_files_count", pa.int32(), nullable=False),
            pa.field("existing_delete_files_count", pa.int32(), nullable=False),
            pa.field("deleted_delete_files_count", pa.int32(), nullable=False),
            pa.field("partition_summaries", pa.list_(partition_summary_schema), nullable=False),
        ])

        def _partition_summaries_to_rows(
            spec: PartitionSpec, partition_summaries: List[PartitionFieldSummary]
        ) -> List[Dict[str, Any]]:
            rows = []
            for i, field_summary in enumerate(partition_summaries):
                field = spec.fields[i]
                partition_field_type = spec.partition_type(self.tbl.schema()).fields[i].field_type
                lower_bound = (
                    (
                        field.transform.to_human_string(
                            partition_field_type, from_bytes(partition_field_type, field_summary.lower_bound)
                        )
                    )
                    if field_summary.lower_bound
                    else None
                )
                upper_bound = (
                    (
                        field.transform.to_human_string(
                            partition_field_type, from_bytes(partition_field_type, field_summary.upper_bound)
                        )
                    )
                    if field_summary.upper_bound
                    else None
                )
                rows.append({
                    "contains_null": field_summary.contains_null,
                    "contains_nan": field_summary.contains_nan,
                    "lower_bound": lower_bound,
                    "upper_bound": upper_bound,
                })
            return rows

        specs = self.tbl.metadata.specs()
        manifests = []
        if snapshot := self.tbl.metadata.current_snapshot():
            for manifest in snapshot.manifests(self.tbl.io):
                is_data_file = manifest.content == ManifestContent.DATA
                is_delete_file = manifest.content == ManifestContent.DELETES
                manifests.append({
                    "content": manifest.content,
                    "path": manifest.manifest_path,
                    "length": manifest.manifest_length,
                    "partition_spec_id": manifest.partition_spec_id,
                    "added_snapshot_id": manifest.added_snapshot_id,
                    "added_data_files_count": manifest.added_files_count if is_data_file else 0,
                    "existing_data_files_count": manifest.existing_files_count if is_data_file else 0,
                    "deleted_data_files_count": manifest.deleted_files_count if is_data_file else 0,
                    "added_delete_files_count": manifest.added_files_count if is_delete_file else 0,
                    "existing_delete_files_count": manifest.existing_files_count if is_delete_file else 0,
                    "deleted_delete_files_count": manifest.deleted_files_count if is_delete_file else 0,
                    "partition_summaries": _partition_summaries_to_rows(specs[manifest.partition_spec_id], manifest.partitions)
                    if manifest.partitions
                    else [],
                })

        return pa.Table.from_pylist(
            manifests,
            schema=manifest_schema,
        )

    def metadata_log_entries(self) -> "pa.Table":
        import pyarrow as pa

        from pyiceberg.table.snapshots import MetadataLogEntry

        table_schema = pa.schema([
            pa.field("timestamp", pa.timestamp(unit="ms"), nullable=False),
            pa.field("file", pa.string(), nullable=False),
            pa.field("latest_snapshot_id", pa.int64(), nullable=True),
            pa.field("latest_schema_id", pa.int32(), nullable=True),
            pa.field("latest_sequence_number", pa.int64(), nullable=True),
        ])

        def metadata_log_entry_to_row(metadata_entry: MetadataLogEntry) -> Dict[str, Any]:
            latest_snapshot = self.tbl.snapshot_as_of_timestamp(metadata_entry.timestamp_ms)
            return {
                "timestamp": metadata_entry.timestamp_ms,
                "file": metadata_entry.metadata_file,
                "latest_snapshot_id": latest_snapshot.snapshot_id if latest_snapshot else None,
                "latest_schema_id": latest_snapshot.schema_id if latest_snapshot else None,
                "latest_sequence_number": latest_snapshot.sequence_number if latest_snapshot else None,
            }

        # similar to MetadataLogEntriesTable in Java
        # https://github.com/apache/iceberg/blob/8a70fe0ff5f241aec8856f8091c77fdce35ad256/core/src/main/java/org/apache/iceberg/MetadataLogEntriesTable.java#L62-L66
        metadata_log_entries = self.tbl.metadata.metadata_log + [
            MetadataLogEntry(metadata_file=self.tbl.metadata_location, timestamp_ms=self.tbl.metadata.last_updated_ms)
        ]

        return pa.Table.from_pylist(
            [metadata_log_entry_to_row(entry) for entry in metadata_log_entries],
            schema=table_schema,
        )

    def history(self) -> "pa.Table":
        import pyarrow as pa

        history_schema = pa.schema([
            pa.field("made_current_at", pa.timestamp(unit="ms"), nullable=False),
            pa.field("snapshot_id", pa.int64(), nullable=False),
            pa.field("parent_id", pa.int64(), nullable=True),
            pa.field("is_current_ancestor", pa.bool_(), nullable=False),
        ])

        ancestors_ids = {snapshot.snapshot_id for snapshot in ancestors_of(self.tbl.current_snapshot(), self.tbl.metadata)}

        history = []
        metadata = self.tbl.metadata

        for snapshot_entry in metadata.snapshot_log:
            snapshot = metadata.snapshot_by_id(snapshot_entry.snapshot_id)

            history.append({
                "made_current_at": datetime.utcfromtimestamp(snapshot_entry.timestamp_ms / 1000.0),
                "snapshot_id": snapshot_entry.snapshot_id,
                "parent_id": snapshot.parent_snapshot_id if snapshot else None,
                "is_current_ancestor": snapshot_entry.snapshot_id in ancestors_ids,
            })

        return pa.Table.from_pylist(history, schema=history_schema)

    def files(self, snapshot_id: Optional[int] = None) -> "pa.Table":
        import pyarrow as pa

        from pyiceberg.io.pyarrow import schema_to_pyarrow

        schema = self.tbl.metadata.schema()
        readable_metrics_struct = []

        def _readable_metrics_struct(bound_type: PrimitiveType) -> pa.StructType:
            pa_bound_type = schema_to_pyarrow(bound_type)
            return pa.struct([
                pa.field("column_size", pa.int64(), nullable=True),
                pa.field("value_count", pa.int64(), nullable=True),
                pa.field("null_value_count", pa.int64(), nullable=True),
                pa.field("nan_value_count", pa.int64(), nullable=True),
                pa.field("lower_bound", pa_bound_type, nullable=True),
                pa.field("upper_bound", pa_bound_type, nullable=True),
            ])

        for field in self.tbl.metadata.schema().fields:
            readable_metrics_struct.append(
                pa.field(schema.find_column_name(field.field_id), _readable_metrics_struct(field.field_type), nullable=False)
            )

        files_schema = pa.schema([
            pa.field("content", pa.int8(), nullable=False),
            pa.field("file_path", pa.string(), nullable=False),
            pa.field("file_format", pa.dictionary(pa.int32(), pa.string()), nullable=False),
            pa.field("spec_id", pa.int32(), nullable=False),
            pa.field("record_count", pa.int64(), nullable=False),
            pa.field("file_size_in_bytes", pa.int64(), nullable=False),
            pa.field("column_sizes", pa.map_(pa.int32(), pa.int64()), nullable=True),
            pa.field("value_counts", pa.map_(pa.int32(), pa.int64()), nullable=True),
            pa.field("null_value_counts", pa.map_(pa.int32(), pa.int64()), nullable=True),
            pa.field("nan_value_counts", pa.map_(pa.int32(), pa.int64()), nullable=True),
            pa.field("lower_bounds", pa.map_(pa.int32(), pa.binary()), nullable=True),
            pa.field("upper_bounds", pa.map_(pa.int32(), pa.binary()), nullable=True),
            pa.field("key_metadata", pa.binary(), nullable=True),
            pa.field("split_offsets", pa.list_(pa.int64()), nullable=True),
            pa.field("equality_ids", pa.list_(pa.int32()), nullable=True),
            pa.field("sort_order_id", pa.int32(), nullable=True),
            pa.field("readable_metrics", pa.struct(readable_metrics_struct), nullable=True),
        ])

        files: list[dict[str, Any]] = []

        if not snapshot_id and not self.tbl.metadata.current_snapshot():
            return pa.Table.from_pylist(
                files,
                schema=files_schema,
            )
        snapshot = self._get_snapshot(snapshot_id)

        io = self.tbl.io
        for manifest_list in snapshot.manifests(io):
            for manifest_entry in manifest_list.fetch_manifest_entry(io):
                data_file = manifest_entry.data_file
                column_sizes = data_file.column_sizes or {}
                value_counts = data_file.value_counts or {}
                null_value_counts = data_file.null_value_counts or {}
                nan_value_counts = data_file.nan_value_counts or {}
                lower_bounds = data_file.lower_bounds or {}
                upper_bounds = data_file.upper_bounds or {}
                readable_metrics = {
                    schema.find_column_name(field.field_id): {
                        "column_size": column_sizes.get(field.field_id),
                        "value_count": value_counts.get(field.field_id),
                        "null_value_count": null_value_counts.get(field.field_id),
                        "nan_value_count": nan_value_counts.get(field.field_id),
                        "lower_bound": from_bytes(field.field_type, lower_bound)
                        if (lower_bound := lower_bounds.get(field.field_id))
                        else None,
                        "upper_bound": from_bytes(field.field_type, upper_bound)
                        if (upper_bound := upper_bounds.get(field.field_id))
                        else None,
                    }
                    for field in self.tbl.metadata.schema().fields
                }
                files.append({
                    "content": data_file.content,
                    "file_path": data_file.file_path,
                    "file_format": data_file.file_format,
                    "spec_id": data_file.spec_id,
                    "record_count": data_file.record_count,
                    "file_size_in_bytes": data_file.file_size_in_bytes,
                    "column_sizes": dict(data_file.column_sizes),
                    "value_counts": dict(data_file.value_counts),
                    "null_value_counts": dict(data_file.null_value_counts),
                    "nan_value_counts": dict(data_file.nan_value_counts),
                    "lower_bounds": dict(data_file.lower_bounds),
                    "upper_bounds": dict(data_file.upper_bounds),
                    "key_metadata": data_file.key_metadata,
                    "split_offsets": data_file.split_offsets,
                    "equality_ids": data_file.equality_ids,
                    "sort_order_id": data_file.sort_order_id,
                    "readable_metrics": readable_metrics,
                })

        return pa.Table.from_pylist(
            files,
            schema=files_schema,
        )


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
