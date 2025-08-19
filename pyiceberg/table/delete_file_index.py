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
from bisect import bisect_left
from typing import Any, Dict, List, Optional, Tuple, Union

from pyiceberg.conversions import from_bytes
from pyiceberg.expressions import EqualTo
from pyiceberg.expressions.visitors import _InclusiveMetricsEvaluator
from pyiceberg.manifest import POSITIONAL_DELETE_SCHEMA, DataFile, DataFileContent, FileFormat, ManifestEntry
from pyiceberg.partitioning import PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.typedef import Record
from pyiceberg.types import NestedField
from pyiceberg.utils.partition_map import PartitionMap

PATH_FIELD_ID = 2147483546


class EqualityDeleteFileWrapper:
    """Stores the equality delete file along with the sequence number."""

    def __init__(self, manifest_entry: ManifestEntry, schema: Schema) -> None:
        """Initialize a new EqualityDeleteFileWrapper.

        Args:
            manifest_entry: The manifest entry containing the delete file
            schema: The table schema for field lookups
        """
        self.delete_file = manifest_entry.data_file
        self.schema = schema
        self.apply_sequence_number = (manifest_entry.sequence_number or 0) - 1
        self._converted_lower_bounds: Optional[Dict[int, Any]] = None
        self._converted_upper_bounds: Optional[Dict[int, Any]] = None
        self._equality_fields: Optional[List[NestedField]] = None

    def equality_fields(self) -> List[NestedField]:
        """Get equality fields for current delete file.

        Returns:
            List of NestedField objects representing the equality fields
        """
        if self._equality_fields is None:
            fields = []
            for field_id in self.delete_file.equality_ids or []:
                field = self.schema.find_field(field_id)
                if field:
                    fields.append(field)
            self._equality_fields = fields
        return self._equality_fields

    def lower_bound(self, field_id: int) -> Optional[Any]:
        """Convert or get lower bound for a field.

        Args:
            field_id: The field ID to get the bound for

        Returns:
            The converted lower bound value or None if not available
        """
        if self._converted_lower_bounds is None:
            self._converted_lower_bounds = self._convert_bounds(self.delete_file.lower_bounds)
        return self._converted_lower_bounds.get(field_id)

    def upper_bound(self, field_id: int) -> Optional[Any]:
        """Convert or get upper bound for a field.

        Args:
            field_id: The field ID to get the bound for

        Returns:
            The converted upper bound value or None if not available
        """
        if self._converted_upper_bounds is None:
            self._converted_upper_bounds = self._convert_bounds(self.delete_file.upper_bounds)
        return self._converted_upper_bounds.get(field_id)

    def _convert_bounds(self, bounds: Dict[int, bytes]) -> Dict[int, Any]:
        """Convert byte bounds to their proper types.

        Args:
            bounds: Dictionary mapping field IDs to byte bounds

        Returns:
            Dictionary mapping field IDs to converted bound values
        """
        if not bounds:
            return {}

        converted = {}
        for field in self.equality_fields():
            field_id = field.field_id
            bound = bounds.get(field_id)
            if bound is not None:
                # Use the field type to convert the bound
                converted[field_id] = from_bytes(field.field_type, bound)
        return converted


class PositionalDeleteFileWrapper:
    """Stores the position delete file along with the sequence number for filtering."""

    def __init__(self, manifest_entry: ManifestEntry):
        """Initialize a new PositionalDeleteFileWrapper.

        Args:
            manifest_entry: The manifest entry containing the delete file
        """
        self.delete_file = manifest_entry.data_file
        self.apply_sequence_number = manifest_entry.sequence_number or 0


class DeletesGroup:
    """Base class for managing collections of delete files with lazy sorting and binary search.

    Provides O(1) insertion with deferred O(n log n) sorting and O(log n + k) filtering
    where k is the number of matching delete files.
    """

    def __init__(self) -> None:
        """Initialize a new DeletesGroup."""
        self._buffer: Optional[List[Any]] = []
        self._sorted: bool = False  # Lazy sorting flag
        self._seqs: Optional[List[int]] = None
        self._files: Optional[List[Any]] = None

    def add(self, wrapper: Any) -> None:
        """Add a delete file wrapper to the group.

        Args:
            wrapper: The delete file wrapper to add

        Raises:
            ValueError: If attempting to add files after indexing
        """
        if self._buffer is None:
            raise ValueError("Can't add files to group after indexing")
        self._buffer.append(wrapper)
        self._sorted = False

    def _index_if_needed(self) -> None:
        """Sort wrappers by apply_sequence_number if not already sorted.

        This method implements lazy sorting to avoid unnecessary work when
        files are only added but not queried.
        """
        if not self._sorted:
            self._files = sorted(self._buffer, key=lambda f: f.apply_sequence_number)  # type: ignore
            self._seqs = [f.apply_sequence_number for f in self._files]
            self._buffer = None
            self._sorted = True

    def _get_candidates(self, seq: int) -> List[Any]:
        """Get delete files with apply_sequence_number >= seq using binary search.

        Args:
            seq: The sequence number to filter by

        Returns:
            List of delete file wrappers with sequence number >= seq
        """
        self._index_if_needed()

        if not self._files or not self._seqs:
            return []

        start_idx = bisect_left(self._seqs, seq)

        if start_idx >= len(self._files):
            return []

        return self._files[start_idx:]


class EqualityDeletesGroup(DeletesGroup):
    """Extends the base DeletesGroup with equality-specific filtering logic.

    Uses file statistics and bounds to eliminate impossible matches before expensive operations.
    This optimization significantly reduces the number of delete files that need to be processed
    during scan planning.
    """

    def filter(self, seq: int, data_file: DataFile) -> List[DataFile]:
        """Find equality deletes that could apply to the data file.

        Args:
            seq: The sequence number to filter by
            data_file: The data file to check against

        Returns:
            List of delete files that may apply to the data file
        """
        candidates = self._get_candidates(seq)

        matching_files = []
        for wrapper in candidates:
            if self._can_contain_eq_deletes_for_file(data_file, wrapper):
                matching_files.append(wrapper.delete_file)

        return matching_files

    def _can_contain_eq_deletes_for_file(self, data_file: DataFile, delete_wrapper: EqualityDeleteFileWrapper) -> bool:
        """Check if a data file might contain rows deleted by an equality delete file.

        This method uses statistics (bounds and null counts) to determine if a delete file
        could possibly match any rows in a data file, avoiding unnecessary processing.

        Args:
            data_file: The data file to check
            delete_wrapper: The equality delete file wrapper

        Returns:
            True if the delete file might apply to the data file, False otherwise
        """
        data_lowers = data_file.lower_bounds
        data_uppers = data_file.upper_bounds
        delete_file = delete_wrapper.delete_file

        # Check bounds and null counts if available
        data_null_counts = data_file.null_value_counts or {}
        data_value_counts = data_file.value_counts or {}
        delete_null_counts = delete_file.null_value_counts or {}
        delete_value_counts = delete_file.value_counts or {}

        # Check each equality field
        for field in delete_wrapper.equality_fields():
            if not field.field_type.is_primitive:
                continue
            field_id = field.field_id

            # Check null values
            if not field.required:
                data_has_nulls = data_null_counts.get(field_id, 0) > 0
                delete_has_nulls = delete_null_counts.get(field_id, 0) > 0
                if data_has_nulls and delete_has_nulls:
                    continue

                # If data is all nulls but delete doesn't delete nulls, no match
                data_all_nulls = data_null_counts.get(field_id, 0) == data_value_counts.get(field_id, 0)
                if data_all_nulls and not delete_has_nulls:
                    return False

                # If delete is all nulls but data has no nulls, no match
                delete_all_nulls = delete_null_counts.get(field_id, 0) == delete_value_counts.get(field_id, 0)
                if delete_all_nulls and not data_has_nulls:
                    return False

            # Check bounds overlap if available
            if (
                data_lowers is not None
                and data_uppers is not None
                and delete_file.lower_bounds is not None
                and delete_file.upper_bounds is not None
            ):
                data_lower_bytes = data_lowers.get(field_id)
                data_upper_bytes = data_uppers.get(field_id)
                delete_lower = delete_wrapper.lower_bound(field_id)
                delete_upper = delete_wrapper.upper_bound(field_id)

                # If any bound is missing, assume they overlap
                if data_lower_bytes is None or data_upper_bytes is None or delete_lower is None or delete_upper is None:
                    continue

                # converting data file bounds
                data_lower = from_bytes(field.field_type, data_lower_bytes)
                data_upper = from_bytes(field.field_type, data_upper_bytes)

                # Check if bounds don't overlap
                if data_lower > delete_upper or data_upper < delete_lower:
                    return False

        return True


class PositionalDeletesGroup(DeletesGroup):
    """Extends the base DeletesGroup with positional-specific filtering.

    Uses file path evaluation to determine which deletes apply to which data files.
    This class handles both path-specific position deletes and partition-level position deletes.
    """

    def _is_file_targeted_by_delete(self, delete_file: DataFile, data_file: DataFile) -> bool:
        """Check if a position delete file targets a specific data file.

        Args:
            delete_file: The position delete file to check
            data_file: The data file to check against

        Returns:
            True if the delete file targets the data file, False otherwise
        """
        has_path_bounds = (
            delete_file.lower_bounds
            and delete_file.upper_bounds
            and PATH_FIELD_ID in delete_file.lower_bounds
            and PATH_FIELD_ID in delete_file.upper_bounds
        )

        if not has_path_bounds:
            # applies to all files in the partition
            return True

        try:
            lower_path = delete_file.lower_bounds[PATH_FIELD_ID].decode("utf-8")
            upper_path = delete_file.upper_bounds[PATH_FIELD_ID].decode("utf-8")

            if lower_path == upper_path and lower_path == data_file.file_path:
                return True
        except (UnicodeDecodeError, AttributeError):
            # If we can't decode the path bounds, fall back to the metrics evaluator
            pass

        # Use metrics evaluator for more complex path matching
        evaluator = _InclusiveMetricsEvaluator(POSITIONAL_DELETE_SCHEMA, EqualTo("file_path", data_file.file_path))
        return evaluator.eval(delete_file)

    def filter(self, seq: int, data_file: DataFile) -> List[DataFile]:
        """Filter positional delete files that apply to the given sequence number and data file.

        Args:
            seq: The sequence number to filter by
            data_file: The data file to check against

        Returns:
            List of delete files that apply to the data file
        """
        candidates = self._get_candidates(seq)

        matching_files = []
        for wrapper in candidates:
            delete_file = wrapper.delete_file
            if self._is_file_targeted_by_delete(delete_file, data_file):
                matching_files.append(delete_file)

        return matching_files


class DeleteFileIndex:
    """Index that organizes delete files by partition for efficient lookup during scan planning.

    This class indexes delete files by type (equality or positional), partition, and path
    to enable efficient lookup of delete files that apply to a given data file.
    """

    def __init__(self, table_schema: Schema, partition_specs: Optional[Dict[int, PartitionSpec]] = None) -> None:
        """Initialize a DeleteFileIndex.

        Args:
            table_schema: The table schema for field lookups
            partition_specs: Dictionary mapping spec IDs to PartitionSpec objects
        """
        self.table_schema = table_schema
        self.partition_specs = partition_specs or {}

        # Global deletes
        self.global_eq_deletes = EqualityDeletesGroup()

        # Partition-specific deletes
        self.eq_deletes_by_partition: PartitionMap[EqualityDeletesGroup] = PartitionMap(self.partition_specs)
        self.pos_deletes_by_partition: PartitionMap[PositionalDeletesGroup] = PartitionMap(self.partition_specs)

        # Path-specific deletes
        self.pos_deletes_by_path: Dict[str, PositionalDeletesGroup] = {}
        self.dv: Dict[str, Tuple[DataFile, int]] = {}
        self.dv_values: Optional[List[Tuple[DataFile, int]]] = None
        self.dv_sorted: bool = False

    def add_delete_file(self, manifest_entry: ManifestEntry, partition_key: Optional[Record] = None) -> None:
        """Add delete file to the appropriate partition group based on its type.

        Args:
            manifest_entry: The manifest entry containing the delete file
            partition_key: The partition key for the delete file, if applicable

        Raises:
            ValueError: If attempting to add multiple deletion vectors for the same data file
        """
        delete_file = manifest_entry.data_file

        if delete_file.content == DataFileContent.EQUALITY_DELETES:
            # Skip equality deletes without equality_ids
            if not delete_file.equality_ids or len(delete_file.equality_ids) == 0:
                return

            wrapper = EqualityDeleteFileWrapper(manifest_entry, self.table_schema)

            # Check if the partition spec is unpartitioned
            is_unpartitioned = False
            spec_id = delete_file.spec_id or 0

            if spec_id in self.partition_specs:
                spec = self.partition_specs[spec_id]
                # A spec is unpartitioned when it has no partition fields
                is_unpartitioned = spec.is_unpartitioned()

            if is_unpartitioned:
                # If the spec is unpartitioned, add to global deletes
                self._add_to_partition_group(wrapper, None)
            else:
                # Otherwise, add to partition-specific deletes
                self._add_to_partition_group(wrapper, partition_key)

        elif delete_file.content == DataFileContent.POSITION_DELETES:
            # Check if this is a deletion vector (Puffin format)
            if delete_file.file_format == FileFormat.PUFFIN:
                sequence_number = manifest_entry.sequence_number or 0
                path = delete_file.file_path
                if path not in self.dv:
                    self.dv[path] = (delete_file, sequence_number)
                else:
                    raise ValueError(f"Can't index multiple DVs for {path}: {self.dv[path]} and {(delete_file, sequence_number)}")
            else:
                pos_wrapper = PositionalDeleteFileWrapper(manifest_entry)

                target_path = self._get_referenced_data_file(delete_file)
                if target_path:
                    # Index by target file path
                    self.pos_deletes_by_path.setdefault(target_path, PositionalDeletesGroup()).add(pos_wrapper)
                else:
                    # Index by partition
                    self._add_to_partition_group(pos_wrapper, partition_key)

    def _get_referenced_data_file(self, data_file: DataFile) -> Optional[str]:
        """Extract the target data file path from a position delete file.

        Args:
            data_file: The position delete file

        Returns:
            The referenced data file path or None if not available
        """
        if data_file.content != DataFileContent.POSITION_DELETES or not (data_file.lower_bounds and data_file.upper_bounds):
            return None

        lower_bound = data_file.lower_bounds.get(PATH_FIELD_ID)
        upper_bound = data_file.upper_bounds.get(PATH_FIELD_ID)

        if lower_bound and upper_bound and lower_bound == upper_bound:
            try:
                return lower_bound.decode("utf-8")
            except (UnicodeDecodeError, AttributeError):
                pass

        return None

    def _add_to_partition_group(
        self, wrapper: Union[EqualityDeleteFileWrapper, PositionalDeleteFileWrapper], partition_key: Optional[Record]
    ) -> None:
        """Add wrapper to the appropriate partition group based on wrapper type.

        Args:
            wrapper: The delete file wrapper to add
            partition_key: The partition key for the delete file, if applicable
        """
        # Get spec_id from the delete file if available, otherwise use default spec_id 0
        spec_id = wrapper.delete_file.spec_id or 0

        if isinstance(wrapper, EqualityDeleteFileWrapper):
            if partition_key is None:
                # Global equality deletes
                self.global_eq_deletes.add(wrapper)
            else:
                # Partition-specific equality deletes
                group_eq = self.eq_deletes_by_partition.compute_if_absent(spec_id, partition_key, lambda: EqualityDeletesGroup())
                group_eq.add(wrapper)
        else:
            # Position deletes - both partitioned and unpartitioned deletes
            group_pos = self.pos_deletes_by_partition.compute_if_absent(spec_id, partition_key, lambda: PositionalDeletesGroup())
            group_pos.add(wrapper)

    def for_data_file(self, seq: int, data_file: DataFile, partition_key: Optional[Record] = None) -> List[DataFile]:
        """Find all delete files that apply to the given data file.

        This method combines global deletes, partition-specific deletes, and path-specific deletes
        to determine all delete files that apply to a given data file.

        Args:
            seq: The sequence number of the data file
            data_file: The data file to find deletes for
            partition_key: The partition key for the data file, if applicable

        Returns:
            List of delete files that apply to the data file

        """
        deletes = []

        # Global equality deletes
        deletes.extend(self.global_eq_deletes.filter(seq, data_file))

        # Partition-specific equality deletes
        spec_id = data_file.spec_id or 0
        if partition_key is not None:
            eq_group: Optional[EqualityDeletesGroup] = self.eq_deletes_by_partition.get(spec_id, partition_key)
            if eq_group:
                deletes.extend(eq_group.filter(seq, data_file))

        # Check for deletion vector
        if self.dv:
            if not self.dv_sorted:
                self.dv_values = sorted(self.dv.values(), key=lambda x: x[1])
                self.dv_sorted = True

            if self.dv_values is not None:
                start_idx = bisect_left([item[1] for item in self.dv_values], seq)
                deletes.extend([item[0] for item in self.dv_values[start_idx:]])

        # Add position deletes
        pos_group: Optional[PositionalDeletesGroup] = self.pos_deletes_by_partition.get(spec_id, partition_key)
        if pos_group:
            deletes.extend(pos_group.filter(seq, data_file))

        # Path-specific positional deletes
        file_path = data_file.file_path
        if file_path in self.pos_deletes_by_path:
            deletes.extend(self.pos_deletes_by_path[file_path].filter(seq, data_file))

        return deletes
