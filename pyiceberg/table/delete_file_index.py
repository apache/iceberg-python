from typing import List, Dict, Optional, Any
from bisect import bisect_left, bisect_right
from pyiceberg.table import DataFile
from pyiceberg.manifest import ManifestEntry, DataFileContent, POSITIONAL_DELETE_SCHEMA
from pyiceberg.types import NestedField
from pyiceberg.expressions import EqualTo
from pyiceberg.expressions.visitors import _InclusiveMetricsEvaluator


class EqualityDeleteFileWrapper:
    def __init__(self, spec_id: int, manifest_entry: ManifestEntry, schema):
        self.spec_id = spec_id
        self.delete_file = manifest_entry.data_file
        self.schema = schema
        self.apply_sequence_number = (manifest_entry.sequence_number or 0) - 1

    def equality_fields(self) -> List[NestedField]:
        """Get equality fields for current delete file"""
        fields = []
        for field_id in self.delete_file.equality_ids or []:
            field = self.schema.find_field(field_id)
            if field:
                fields.append(field)
        return fields

class PositionalDeleteFileWrapper:
    def __init__(self, spec_id: int, manifest_entry: ManifestEntry, schema):
        self.delete_file = manifest_entry.data_file
        self.apply_sequence_number = manifest_entry.sequence_number or 0

class EqualityDeletesGroup:
    def __init__(self):
        self._buffer: List[EqualityDeleteFileWrapper] = []
        self._sorted: bool = False
        self._seqs: Optional[List[int]] = None
        self._files: Optional[List[EqualityDeleteFileWrapper]] = None

    def add(self, wrapper: EqualityDeleteFileWrapper):
        """Add an equality delete file wrapper to the group"""
        self._buffer.append(wrapper)
        self._sorted = False

    def _index_if_needed(self):
        if not self._sorted:
            # Sort by apply sequence number
            self._files = sorted(self._buffer, key=lambda f: f.apply_sequence_number)
            self._seqs = [f.apply_sequence_number for f in self._files]
            self._sorted = True

    def filter(self, seq: int, data_file: DataFile) -> List[DataFile]:
        """Filter delete files that apply to the given data file"""
        self._index_if_needed()

        if not self._files:
            return []

        # Find the first delete file with apply_sequence_number >= seq
        start_idx = bisect_left(self._seqs, seq)

        if start_idx >= len(self._files):
            return []

        matching_files = []
        for wrapper in self._files[start_idx:]:
            if self._can_contain_eq_deletes_for_file(data_file, wrapper):
                matching_files.append(wrapper.delete_file)

        return matching_files

    def _can_contain_eq_deletes_for_file(self, data_file: DataFile, delete_wrapper: EqualityDeleteFileWrapper) -> bool:
        """Check if a data file might contain rows deleted by an equality delete file"""
        data_lowers = data_file.lower_bounds
        data_uppers = data_file.upper_bounds
        delete_file = delete_wrapper.delete_file

        data_null_counts = data_file.null_value_counts or {}
        data_value_counts = data_file.value_counts or {}
        delete_null_counts = delete_file.null_value_counts or {}
        delete_value_counts = delete_file.value_counts or {}

        for field in delete_wrapper.equality_fields():
            if not field.field_type.is_primitive:
                continue

            field_id = field.field_id

            # Check for null value patterns
            data_has_nulls = field_id in data_null_counts and data_null_counts[field_id] > 0
            delete_has_nulls = field_id in delete_null_counts and delete_null_counts[field_id] > 0

            # If both have nulls, they might overlap
            if data_has_nulls and delete_has_nulls:
                continue

            # Check if data file contains only nulls but delete file has no null deletes
            data_null_count = data_null_counts.get(field_id, 0)
            data_value_count = data_value_counts.get(field_id, 0)
            if data_null_count == data_value_count and data_value_count > 0 and not delete_has_nulls:
                return False

            # Check if delete file removes only nulls but data file has no nulls
            delete_null_count = delete_null_counts.get(field_id, 0)
            delete_value_count = delete_value_counts.get(field_id, 0)
            if delete_null_count == delete_value_count and delete_value_count > 0 and not data_has_nulls:
                return False

            # Check bounds overlap if available
            if data_lowers is not None and data_uppers is not None and delete_file.lower_bounds is not None and delete_file.upper_bounds is not None:
                data_lower = data_lowers.get(field_id)
                data_upper = data_uppers.get(field_id)
                delete_lower = delete_file.lower_bounds.get(field_id)
                delete_upper = delete_file.upper_bounds.get(field_id)

                # Skip if any bound is missing
                if not (data_lower and data_upper and delete_lower and delete_upper):
                    continue

                # Check ranges
                if data_upper < delete_lower or data_lower > delete_upper:
                    return False

        return True

class PositionalDeletesGroup:
    def __init__(self):
        self._buffer: List[PositionalDeleteFileWrapper] = []
        self._sorted: bool = False
        self._seqs: Optional[List[int]] = None
        self._files: Optional[List[PositionalDeleteFileWrapper]] = None

    def add(self, wrapper: PositionalDeleteFileWrapper):
        # Add a positional delete file wrapper to the group
        self._buffer.append(wrapper)
        self._sorted = False

    def _index_if_needed(self):
        if not self._sorted:
            self._files = sorted(self._buffer, key=lambda f: f.apply_sequence_number)
            self._seqs = [f.apply_sequence_number for f in self._files]
            self._sorted = True

    def filter(self, seq: int, data_file: DataFile) -> List[DataFile]:
        """Filter positional delete files that apply to the given sequence number and data file"""
        self._index_if_needed()

        if not self._files:
            return []

        start_idx = bisect_left(self._seqs, seq)

        if start_idx >= len(self._files):
            return []

        evaluator = _InclusiveMetricsEvaluator(POSITIONAL_DELETE_SCHEMA, EqualTo("file_path", data_file.file_path))
        
        matching_files = []
        for wrapper in self._files[start_idx:]:
            if evaluator.eval(wrapper.delete_file):
                matching_files.append(wrapper.delete_file)

        return matching_files

class DeleteFileIndex:
    def __init__(self, table_schema):
        self.table_schema = table_schema
        # Equality deletes
        self.global_eq_deletes = EqualityDeletesGroup()
        self.eq_deletes_by_partition: Dict[str, EqualityDeletesGroup] = {}
        # Positional deletes
        self.global_pos_deletes = PositionalDeletesGroup()
        self.pos_deletes_by_partition: Dict[str, PositionalDeletesGroup] = {}
        self.pos_deletes_by_path: Dict[str, PositionalDeletesGroup] = {}

    def _partition_key_to_string(self, partition_key: Optional[Any]) -> Optional[str]:
        """Convert partition key to string representation"""
        if partition_key is None:
            return None

        if hasattr(partition_key, '_data'):
            partition_values = []
            for i in range(len(partition_key._data)):
                partition_values.append(str(partition_key._data[i]))
            return "/".join(partition_values) if partition_values else "unpartitioned"
        else:
            return str(partition_key) if partition_key else "unpartitioned"

    def add_delete_file(self, spec_id: int, manifest_entry: ManifestEntry, partition_key: Optional[Any] = None):
        """Add delete file to the index"""
        data_file = manifest_entry.data_file
        
        if data_file.content == DataFileContent.EQUALITY_DELETES:
            self._add_equality_delete(spec_id, manifest_entry, partition_key)
        elif data_file.content == DataFileContent.POSITION_DELETES:
            self._add_positional_delete(spec_id, manifest_entry, partition_key)

    def _add_equality_delete(self, spec_id: int, manifest_entry: ManifestEntry, partition_key: Optional[Any] = None):
        """Add an equality delete to the index"""
        if (manifest_entry.data_file.content != DataFileContent.EQUALITY_DELETES or
            not manifest_entry.data_file.equality_ids):
            return
        wrapper = EqualityDeleteFileWrapper(spec_id, manifest_entry, self.table_schema)
        partition_str = self._partition_key_to_string(partition_key)
        if partition_str is None:
            # Global delete
            self.global_eq_deletes.add(wrapper)
        else:
            # Partition-specific delete
            if partition_str not in self.eq_deletes_by_partition:
                self.eq_deletes_by_partition[partition_str] = EqualityDeletesGroup()
            self.eq_deletes_by_partition[partition_str].add(wrapper)

    def _add_positional_delete(self, spec_id: int, manifest_entry: ManifestEntry, partition_key: Optional[Any] = None):
        # Add a positional delete to the index
        wrapper = PositionalDeleteFileWrapper(spec_id, manifest_entry, self.table_schema)
        
        # For positional deletes, we organize them by partition
        partition_str = self._partition_key_to_string(partition_key)
        if partition_str is None:
            # Global positional delete
            self.global_pos_deletes.add(wrapper)
        else:
            # Partition-specific positional delete
            if partition_str not in self.pos_deletes_by_partition:
                self.pos_deletes_by_partition[partition_str] = PositionalDeletesGroup()
            self.pos_deletes_by_partition[partition_str].add(wrapper)

    def for_data_file(self, seq: int, data_file: DataFile, partition_key: Optional[Any] = None) -> List[DataFile]:
        """Find delete files that apply to the given data file"""
        deletes = []

        # Equality deletes
        deletes.extend(self.global_eq_deletes.filter(seq, data_file))

        # Adding partition-specific equality deletes
        partition_str = self._partition_key_to_string(partition_key)
        if partition_str and partition_str in self.eq_deletes_by_partition:
            deletes.extend(
                self.eq_deletes_by_partition[partition_str].filter(seq, data_file)
            )

        # Positional deletes
        # Global positional deletes
        deletes.extend(self.global_pos_deletes.filter(seq, data_file))
        
        # Partition-specific positional deletes
        if partition_str and partition_str in self.pos_deletes_by_partition:
            deletes.extend(self.pos_deletes_by_partition[partition_str].filter(seq, data_file))

        return deletes
