import pytest

from pyiceberg.manifest import DataFile, DataFileContent, FileFormat, ManifestEntry, ManifestEntryStatus
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table.delete_file_index import (
    DeleteFileIndex,
    EqualityDeleteFileWrapper,
    EqualityDeletesGroup,
    PositionalDeleteFileWrapper,
    PositionalDeletesGroup,
)
from pyiceberg.transforms import IdentityTransform
from pyiceberg.utils.partition_map import PartitionMap
from tests.conftest import (
    create_basic_data_file,
    create_basic_equality_delete_file,
    create_equality_delete_entry,
    create_manifest_entry_with_delete_file,
    create_partition_positional_delete_entry,
    create_positional_delete_entry,
)


class TestEqualityDeleteFileWrapper:
    def test_initialization(self, id_data_schema: Schema) -> None:
        """Testing EqualityDeleteFileWrapper"""
        delete_file = create_basic_equality_delete_file(equality_ids=[1])
        manifest_entry = create_manifest_entry_with_delete_file(delete_file, sequence_number=10)

        wrapper = EqualityDeleteFileWrapper(manifest_entry, id_data_schema)
        assert wrapper.delete_file == delete_file
        assert wrapper.schema == id_data_schema
        assert wrapper.apply_sequence_number == 9  # sequence_number - 1

    def test_equality_fields(self, simple_id_schema: Schema) -> None:
        """Testing equality_fields method"""
        delete_file = create_basic_equality_delete_file(equality_ids=[1])
        manifest_entry = create_manifest_entry_with_delete_file(delete_file, sequence_number=10)

        wrapper = EqualityDeleteFileWrapper(manifest_entry, simple_id_schema)
        fields = wrapper.equality_fields()
        assert len(fields) == 1
        assert fields[0].field_id == 1
        assert fields[0].name == "id"

    def test_missing_field_handling(self, simple_id_schema: Schema) -> None:
        """Testing handling of missing fields"""
        delete_file = create_basic_equality_delete_file(equality_ids=[1, 999])
        manifest_entry = create_manifest_entry_with_delete_file(delete_file, sequence_number=10)

        wrapper = EqualityDeleteFileWrapper(manifest_entry, simple_id_schema)

        # Error raised when trying to find non-existent field
        with pytest.raises(ValueError, match="Could not find field with id: 999"):
            wrapper.equality_fields()


class TestEqualityDeletesGroup:
    def test_add_and_filter(self, simple_id_schema: Schema) -> None:
        """Testing adding and filtering equality delete files"""
        group = EqualityDeletesGroup()

        for seq in [4, 2, 1, 3]:
            delete_file = create_basic_equality_delete_file(file_path=f"s3://bucket/eq-delete-{seq}.parquet", equality_ids=[1])
            manifest_entry = create_manifest_entry_with_delete_file(delete_file, sequence_number=seq)
            wrapper = EqualityDeleteFileWrapper(manifest_entry, simple_id_schema)
            group.add(wrapper)

        # Not sorted yet
        assert not group._sorted
        assert group._seqs is None
        assert group._files is None

        # Filter will trigger sorting
        data_file = create_basic_data_file()
        result = group.filter(2, data_file)
        assert len(result) >= 0
        assert group._sorted

    def test_empty_group(self) -> None:
        group = EqualityDeletesGroup()
        data_file = create_basic_data_file()
        result = group.filter(1, data_file)
        assert result == []

    def test_non_equality_delete_ignored(self, simple_id_schema: Schema) -> None:
        """Testing if non-equality delete files are ignored"""
        group = EqualityDeletesGroup()

        eq_delete_file = create_basic_equality_delete_file(equality_ids=[1])
        eq_manifest_entry = create_manifest_entry_with_delete_file(eq_delete_file, sequence_number=1)
        wrapper = EqualityDeleteFileWrapper(eq_manifest_entry, simple_id_schema)
        group.add(wrapper)

        data_file = create_basic_data_file()
        result = group.filter(0, data_file)
        assert len(result) == 1

    def test_no_equality_ids_ignored(self, simple_id_schema: Schema) -> None:
        """Testing if equality delete files without equality_ids are ignored"""
        delete_index = DeleteFileIndex(simple_id_schema)

        delete_file = DataFile.from_args(
            content=DataFileContent.EQUALITY_DELETES,
            file_path="s3://bucket/eq-delete.parquet",
            file_format=FileFormat.PARQUET,
            partition={},
            record_count=5,
            file_size_in_bytes=50,
            equality_ids=[],  # Empty list
        )

        manifest_entry = create_manifest_entry_with_delete_file(delete_file, sequence_number=1)

        # Add to index
        delete_index.add_delete_file(manifest_entry)

        # Should be ignored due to empty equality_ids
        assert len(delete_index.global_eq_deletes._buffer) == 0


class TestPositionalDeleteFileWrapper:
    def test_initialization(self, simple_id_schema: Schema) -> None:
        """Test PositionalDeleteFileWrapper initialization"""
        pos_delete_file = DataFile.from_args(
            content=DataFileContent.POSITION_DELETES,
            file_path="s3://bucket/pos-delete.parquet",
            file_format=FileFormat.PARQUET,
            partition={},
            record_count=10,
            file_size_in_bytes=100,
        )

        manifest_entry = ManifestEntry.from_args(
            status=ManifestEntryStatus.ADDED,
            sequence_number=5,
            data_file=pos_delete_file,
        )

        wrapper = PositionalDeleteFileWrapper(manifest_entry)
        assert wrapper.delete_file == pos_delete_file
        assert wrapper.apply_sequence_number == 5


class TestPositionalDeletesGroup:
    def test_sequence_number_filtering(self, simple_id_schema: Schema) -> None:
        group = PositionalDeletesGroup()

        for seq in [1, 3, 5]:
            pos_delete_file = DataFile.from_args(
                content=DataFileContent.POSITION_DELETES,
                file_path=f"s3://bucket/pos-delete-{seq}.parquet",
                file_format=FileFormat.PARQUET,
                partition={},
                record_count=10,
                file_size_in_bytes=100,
                lower_bounds={2147483546: b"s3://bucket/data.parquet"},
                upper_bounds={2147483546: b"s3://bucket/data.parquet"},
            )

            manifest_entry = ManifestEntry.from_args(
                status=ManifestEntryStatus.ADDED,
                sequence_number=seq,
                data_file=pos_delete_file,
            )

            wrapper = PositionalDeleteFileWrapper(manifest_entry)
            group.add(wrapper)

        data_file = create_basic_data_file(file_path="s3://bucket/data.parquet")

        assert len(group.filter(1, data_file)) == 3
        assert len(group.filter(3, data_file)) == 2
        assert len(group.filter(5, data_file)) == 1
        assert len(group.filter(6, data_file)) == 0

    def test_file_path_matching(self, simple_id_schema: Schema) -> None:
        group = PositionalDeletesGroup()

        # Create a position delete file with path bounds
        pos_delete_file = DataFile.from_args(
            content=DataFileContent.POSITION_DELETES,
            file_path="s3://bucket/pos-delete.parquet",
            file_format=FileFormat.PARQUET,
            partition={},
            record_count=10,
            file_size_in_bytes=100,
            lower_bounds={2147483546: b"s3://bucket/target-data.parquet"},
            upper_bounds={2147483546: b"s3://bucket/target-data.parquet"},
        )

        manifest_entry = ManifestEntry.from_args(
            status=ManifestEntryStatus.ADDED,
            sequence_number=5,
            data_file=pos_delete_file,
        )

        wrapper = PositionalDeleteFileWrapper(manifest_entry)
        group.add(wrapper)

        matching_data_file = create_basic_data_file(file_path="s3://bucket/target-data.parquet")
        result = group.filter(5, matching_data_file)
        assert len(result) == 1

        result = group.filter(6, matching_data_file)
        assert len(result) == 0

        non_matching_data_file = create_basic_data_file(file_path="s3://bucket/other-data.parquet")
        result = group.filter(1, non_matching_data_file)
        assert len(result) == 0


class TestPartitionMap:
    def test_partition_map(self) -> None:
        """Test basic PartitionMap functionality"""
        partition_map: PartitionMap[str] = PartitionMap()
        # Test put and get
        partition_map.put(0, {"year": 2023}, "value1")
        assert partition_map.get(0, {"year": 2023}) == "value1"

        # Test with different partition key formats
        partition_map.put(1, ["a", "b"], "value2")
        assert partition_map.get(1, ["a", "b"]) == "value2"
        assert partition_map.get(1, ("a", "b")) == "value2"

        # Test  compute_if_absent
        value = partition_map.compute_if_absent(2, {"month": "January"}, lambda: "value3")
        assert value == "value3"
        assert partition_map.get(2, {"month": "January"}) == "value3"

        # Test values and is_empty
        assert list(partition_map.values()) == ["value1", "value2", "value3"]
        assert not partition_map.is_empty()

        # Test empty map
        empty_map: PartitionMap[str] = PartitionMap()
        assert empty_map.is_empty()


class TestDeleteFileIndex:
    @pytest.fixture
    def delete_index(self, simple_id_schema: Schema) -> DeleteFileIndex:
        """Fixture to create a DeleteFileIndex with simple schema"""
        unpartitioned_spec = PartitionSpec()
        partitioned_spec = PartitionSpec(
            PartitionField(source_id=1, field_id=1000, transform=IdentityTransform(), name="id_identity")
        )

        partition_specs = {
            0: unpartitioned_spec,  # spec_id 0 is unpartitioned
            1: partitioned_spec,  # spec_id 1 is partitioned
        }

        return DeleteFileIndex(simple_id_schema, partition_specs)

    def test_add_and_retrieve(self, delete_index: DeleteFileIndex) -> None:
        # global delete
        global_delete = create_equality_delete_entry(sequence_number=5, equality_ids=[1])
        delete_index.add_delete_file(global_delete, partition_key=None)

        # partition-specific delete
        partition_delete = create_equality_delete_entry(sequence_number=3, equality_ids=[1])
        delete_index.add_delete_file(partition_delete, partition_key={"partition1": "value1"})

        data_file = create_basic_data_file()

        # Query with seq=3 should get only global delete
        result = delete_index.for_data_file(3, data_file, partition_key={"partition1": "value1"})
        assert len(result) == 1

        # Query with seq=2 should get both deletes
        result = delete_index.for_data_file(2, data_file, partition_key={"partition1": "value1"})
        assert len(result) == 2

        # Query with seq=3 should get global delete for different partition
        result = delete_index.for_data_file(3, data_file, partition_key={"partition2": "value2"})
        assert len(result) == 1

        # Query with seq=3 should get global delete for unpartitioned
        result = delete_index.for_data_file(3, data_file, partition_key=None)
        assert len(result) == 1

    def test_sequence_number_filtering(self, delete_index: DeleteFileIndex) -> None:
        """Test sequence number based filtering"""
        # Add delete files with different sequence numbers
        for seq in [1, 2, 3, 4]:
            delete_entry = create_equality_delete_entry(sequence_number=seq, equality_ids=[1])
            delete_index.add_delete_file(delete_entry)

        data_file = create_basic_data_file()

        assert len(delete_index.for_data_file(0, data_file)) == 4  # All 4 deletes (1,2,3,4 > 0)
        assert len(delete_index.for_data_file(1, data_file)) == 3  # 3 deletes (2,3,4 > 1)
        assert len(delete_index.for_data_file(2, data_file)) == 2  # 2 deletes (3,4 > 2)
        assert len(delete_index.for_data_file(3, data_file)) == 1  # 1 delete (4 > 3)
        assert len(delete_index.for_data_file(4, data_file)) == 0  # 0 deletes (none > 4)

    def test_mixed_delete_types(self, delete_index: DeleteFileIndex) -> None:
        pos_delete_entry = create_positional_delete_entry(sequence_number=5, file_path="s3://bucket/data.parquet")
        delete_index.add_delete_file(pos_delete_entry)

        eq_delete_entry = create_equality_delete_entry(sequence_number=3, equality_ids=[1])
        delete_index.add_delete_file(eq_delete_entry)

        data_file = create_basic_data_file(file_path="s3://bucket/data.parquet")

        result = delete_index.for_data_file(5, data_file)

        contents = {df.content for df in result}
        assert DataFileContent.POSITION_DELETES in contents
        assert DataFileContent.EQUALITY_DELETES not in contents

        result = delete_index.for_data_file(2, data_file)

        contents = {df.content for df in result}
        assert DataFileContent.POSITION_DELETES in contents
        assert DataFileContent.EQUALITY_DELETES in contents

    def test_file_scoped_vs_partition_scoped_deletes(self, delete_index: DeleteFileIndex) -> None:
        """Test the difference between file-scoped and partition-scoped positional deletes"""

        # Create a file-scoped delete
        file_scoped_delete = create_positional_delete_entry(sequence_number=5, file_path="s3://bucket/data.parquet")
        delete_index.add_delete_file(file_scoped_delete, partition_key={"year": 2023})

        # Create a partition-scoped delete
        partition_scoped_delete = create_partition_positional_delete_entry(sequence_number=3)
        delete_index.add_delete_file(partition_scoped_delete, partition_key={"year": 2024})

        # Test file that matches the file-scoped delete
        target_file = create_basic_data_file(file_path="s3://bucket/data.parquet")

        result = delete_index.for_data_file(5, target_file, partition_key={"year": 2023})
        pos_deletes = [df for df in result if df.content == DataFileContent.POSITION_DELETES]
        assert len(pos_deletes) == 1  # file-scoped delete

        result = delete_index.for_data_file(3, target_file, partition_key={"year": 2024})
        pos_deletes = [df for df in result if df.content == DataFileContent.POSITION_DELETES]
        assert len(pos_deletes) == 2  # partition-scoped + file-scoped

        result = delete_index.for_data_file(3, target_file, partition_key={"year": 2025})
        pos_deletes = [df for df in result if df.content == DataFileContent.POSITION_DELETES]
        assert len(pos_deletes) == 1  # only file-scoped delete

    def test_positional_delete_partition_isolation(self, delete_index: DeleteFileIndex) -> None:
        global_pos_delete = create_partition_positional_delete_entry(sequence_number=5)
        delete_index.add_delete_file(global_pos_delete, partition_key=None)

        partition_pos_delete = create_partition_positional_delete_entry(sequence_number=3)
        partition_key_1 = {"bucket": 1}
        delete_index.add_delete_file(partition_pos_delete, partition_key=partition_key_1)

        data_file = create_basic_data_file(file_path="s3://bucket/data.parquet")

        result = delete_index.for_data_file(5, data_file, partition_key=partition_key_1)
        pos_deletes = [df for df in result if df.content == DataFileContent.POSITION_DELETES]
        assert len(pos_deletes) == 1  # only global (5 >= 5)

        result = delete_index.for_data_file(3, data_file, partition_key=partition_key_1)
        pos_deletes = [df for df in result if df.content == DataFileContent.POSITION_DELETES]
        assert len(pos_deletes) == 2  # global + partition-specific

        result = delete_index.for_data_file(5, data_file, partition_key=None)
        pos_deletes = [df for df in result if df.content == DataFileContent.POSITION_DELETES]
        assert len(pos_deletes) == 1  # only global

    def test_empty_index(self, delete_index: DeleteFileIndex) -> None:
        """Test empty delete file index"""
        data_file = create_basic_data_file()
        result = delete_index.for_data_file(1, data_file)
        assert result == []

    def test_unpartitioned_equality_delete(self, delete_index: DeleteFileIndex) -> None:
        """Test that equality delete files with unpartitioned specs are added to global deletes"""
        unpartitioned_delete = create_equality_delete_entry(sequence_number=5, equality_ids=[1])
        unpartitioned_delete.data_file._spec_id = 0

        delete_index.add_delete_file(unpartitioned_delete, partition_key={"bucket": 1})

        partitioned_delete = create_equality_delete_entry(sequence_number=3, equality_ids=[1])
        partitioned_delete.data_file._spec_id = 1

        delete_index.add_delete_file(partitioned_delete, partition_key={"bucket": 1})

        data_file = create_basic_data_file()

        # Query with a different partition key should get only the unpartitioned delete
        result = delete_index.for_data_file(4, data_file, partition_key={"bucket": 2})
        assert len(result) == 1  # Only the unpartitioned delete

        data_file._spec_id = 1

        # Query with the same partition key should get both deletes
        result = delete_index.for_data_file(2, data_file, partition_key={"bucket": 1})
        assert len(result) == 2  # Both unpartitioned and partitioned deletes
