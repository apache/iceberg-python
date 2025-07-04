from bisect import bisect_left
from typing import Any, Dict, List, Optional

from pyiceberg.expressions import EqualTo
from pyiceberg.expressions.visitors import _InclusiveMetricsEvaluator
from pyiceberg.manifest import POSITIONAL_DELETE_SCHEMA, DataFile, DataFileContent, ManifestEntry
from pyiceberg.partitioning import PartitionSpec
from pyiceberg.types import NestedField
from pyiceberg.utils.partition_map import PartitionMap

PATH_FIELD_ID = 2147483546


class EqualityDeleteFileWrapper:
    """Stores the equality delete file along with the sequence number."""

    def __init__(self, manifest_entry: ManifestEntry, schema: Any) -> None:
        self.delete_file = manifest_entry.data_file
        self.schema = schema
        self.apply_sequence_number = (manifest_entry.sequence_number or 0) - 1

    def equality_fields(self) -> List[NestedField]:
        """Get equality fields for current delete file."""
        fields = []
        for field_id in self.delete_file.equality_ids or []:
            field = self.schema.find_field(field_id)
            if field:
                fields.append(field)
        return fields


class PositionalDeleteFileWrapper:
    """Stores the position delete file along with the sequence number for filtering."""

    def __init__(self, manifest_entry: ManifestEntry):
        self.delete_file = manifest_entry.data_file
        self.apply_sequence_number = manifest_entry.sequence_number or 0


class DeletesGroup:
    """Base class for managing collections of delete files with lazy sorting and binary search.

    Provides O(1) insertion with deferred O(n log n) sorting and O(log n + k) filtering
    where k is the number of matching delete files.
    """

    def __init__(self) -> None:
        self._buffer: List[Any] = []
        self._sorted: bool = False  # Lazy sorting flag
        self._seqs: Optional[List[int]] = None
        self._files: Optional[List[Any]] = None

    def add(self, wrapper: Any) -> None:
        """Add a delete file wrapper to the group."""
        self._buffer.append(wrapper)
        self._sorted = False

    def _index_if_needed(self) -> None:
        """Sort wrappers by apply_sequence_number if not already sorted."""
        if not self._sorted:
            self._files = sorted(self._buffer, key=lambda f: f.apply_sequence_number)
            self._seqs = [f.apply_sequence_number for f in self._files]
            self._sorted = True

    def _get_candidates(self, seq: int) -> List[Any]:
        """Get delete files with apply_sequence_number >= seq using binary search."""
        self._index_if_needed()

        if not self._files or not self._seqs:
            return []

        start_idx = bisect_left(self._seqs, seq)

        if start_idx >= len(self._files):
            return []

        return self._files[start_idx:]


class EqualityDeletesGroup(DeletesGroup):
    """Extends the base DeletesGroup with equality-specific filtering logic that uses file statistics and bounds to eliminate impossible matches before expensive operations."""

    def filter(self, seq: int, data_file: DataFile) -> List[DataFile]:
        """Find equality deletes that could apply to the data file."""
        candidates = self._get_candidates(seq)

        matching_files = []
        for wrapper in candidates:
            if self._can_contain_eq_deletes_for_file(data_file, wrapper):
                matching_files.append(wrapper.delete_file)

        return matching_files

    def _can_contain_eq_deletes_for_file(self, data_file: DataFile, delete_wrapper: EqualityDeleteFileWrapper) -> bool:
        """Check if a data file might contain rows deleted by an equality delete file."""
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
                data_lower = data_lowers.get(field_id)
                data_upper = data_uppers.get(field_id)
                delete_lower = delete_file.lower_bounds.get(field_id)
                delete_upper = delete_file.upper_bounds.get(field_id)

                # If any bound is missing, assume they overlap
                if data_lower is None or data_upper is None or delete_lower is None or delete_upper is None:
                    continue

                # Check if bounds don't overlap
                if data_lower > delete_upper or data_upper < delete_lower:
                    return False

        return True


class PositionalDeletesGroup(DeletesGroup):
    """Extends the base DeletesGroup with positional-specific filtering that uses file path evaluation to determine which deletes apply to which data files."""

    def _is_file_targeted_by_delete(self, delete_file: DataFile, data_file: DataFile) -> bool:
        """Check if a position delete file targets a specific data file. The method determines if a position delete file applies to a data file based on path bounds."""
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
        """Filter positional delete files that apply to the given sequence number and data file."""
        candidates = self._get_candidates(seq)

        matching_files = []
        for wrapper in candidates:
            delete_file = wrapper.delete_file
            if self._is_file_targeted_by_delete(delete_file, data_file):
                matching_files.append(delete_file)

        return matching_files


class DeleteFileIndex:
    """Main index that organizes delete files by partition for efficient lookup during scan planning."""

    def __init__(self, table_schema: Any, partition_specs: Optional[Dict[int, PartitionSpec]] = None) -> None:
        """Initialize a DeleteFileIndex."""
        self.table_schema = table_schema
        self.partition_specs = partition_specs or {}

        # Global deletes
        self.global_eq_deletes = EqualityDeletesGroup()
        self.global_pos_deletes = PositionalDeletesGroup()

        # Partition-specific deletes
        self.eq_deletes_by_partition: PartitionMap[EqualityDeletesGroup] = PartitionMap(self.partition_specs)
        self.pos_deletes_by_partition: PartitionMap[PositionalDeletesGroup] = PartitionMap(self.partition_specs)

        # Path-specific deletes
        self.pos_deletes_by_path: Dict[str, PositionalDeletesGroup] = {}

    def add_delete_file(self, manifest_entry: ManifestEntry, partition_key: Optional[Any] = None) -> None:
        """Add delete file to the appropriate partition group based on its type."""
        data_file = manifest_entry.data_file

        if data_file.content == DataFileContent.EQUALITY_DELETES:
            # Skip equality deletes without equality_ids
            if not data_file.equality_ids or len(data_file.equality_ids) == 0:
                return

            wrapper = EqualityDeleteFileWrapper(manifest_entry, self.table_schema)

            # Check if the partition spec is unpartitioned
            is_unpartitioned = False

            # Try to get spec_id if available
            spec_id = getattr(data_file, "spec_id", None)
            if spec_id is not None and spec_id in self.partition_specs:
                spec = self.partition_specs[spec_id]
                # A spec is unpartitioned when it has no partition fields
                is_unpartitioned = len(spec.fields) == 0

            if is_unpartitioned:
                # If the spec is unpartitioned, add to global deletes
                self.global_eq_deletes.add(wrapper)
            else:
                # Otherwise, add to partition-specific deletes
                self._add_to_partition_group(wrapper, partition_key)

        elif data_file.content == DataFileContent.POSITION_DELETES:
            pos_wrapper = PositionalDeleteFileWrapper(manifest_entry)

            target_path = self.get_referenced_data_file(data_file)
            if target_path:
                # Index by target file path
                self.pos_deletes_by_path.setdefault(target_path, PositionalDeletesGroup()).add(pos_wrapper)
            else:
                # Index by partition
                self._add_to_partition_group(pos_wrapper, partition_key)

    def get_referenced_data_file(self, data_file: DataFile) -> Optional[str]:
        """Extract the target data file path from a position delete file."""
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

    def _add_to_partition_group(self, wrapper: Any, partition_key: Optional[Any]) -> None:
        """Add wrapper to the appropriate partition group based on wrapper type."""
        if partition_key is None:
            # Global deletes
            if isinstance(wrapper, EqualityDeleteFileWrapper):
                self.global_eq_deletes.add(wrapper)
            else:
                self.global_pos_deletes.add(wrapper)
            return

        # Get spec_id from the delete file if available, otherwise use default spec_id 0
        spec_id = getattr(wrapper.delete_file, "spec_id", 0)

        # Add to partition-specific deletes
        if isinstance(wrapper, EqualityDeleteFileWrapper):
            group_eq = self.eq_deletes_by_partition.compute_if_absent(spec_id, partition_key, lambda: EqualityDeletesGroup())
            group_eq.add(wrapper)
        else:
            group_pos = self.pos_deletes_by_partition.compute_if_absent(spec_id, partition_key, lambda: PositionalDeletesGroup())
            group_pos.add(wrapper)

    def for_data_file(self, seq: int, data_file: DataFile, partition_key: Optional[Any] = None) -> List[DataFile]:
        """Find all delete files that apply to the given data file."""
        deletes = []

        # Global equality deletes (apply to all partitions)
        deletes.extend(self.global_eq_deletes.filter(seq, data_file))

        # Partition-specific equality deletes
        if partition_key is not None:
            spec_id = getattr(data_file, "spec_id", 0)
            eq_group = self.eq_deletes_by_partition.get(spec_id, partition_key)
            if eq_group:
                deletes.extend(eq_group.filter(seq, data_file))

        # Global positional deletes (apply to all partitions)
        deletes.extend(self.global_pos_deletes.filter(seq, data_file))

        # Partition-specific positional deletes
        if partition_key is not None:
            spec_id = getattr(data_file, "spec_id", 0)
            pos_group = self.pos_deletes_by_partition.get(spec_id, partition_key)
            if pos_group:
                deletes.extend(pos_group.filter(seq, data_file))

        # Path-specific positional deletes
        file_path = data_file.file_path
        if file_path in self.pos_deletes_by_path:
            deletes.extend(self.pos_deletes_by_path[file_path].filter(seq, data_file))

        return deletes
