import pytest

from pyiceberg.manifest import DataFile, DataFileContent, FileFormat, ManifestEntry, ManifestEntryStatus
from pyiceberg.schema import Schema
from pyiceberg.table.delete_file_index import DeleteFileIndex, EqualityDeleteFileWrapper, EqualityDeletesGroup, PositionalDeleteFileWrapper, PositionalDeletesGroup
from tests.conftest import (
    create_equality_delete_entry,
    create_basic_equality_delete_file,
    create_basic_data_file,
    create_manifest_entry_with_delete_file,
    create_positional_delete_entry,
)

class TestEqualityDeleteFileWrapper:
    def test_initialization(self, id_data_schema: Schema) -> None:
        """Testing EqualityDeleteFileWrapper"""
        delete_file = create_basic_equality_delete_file(equality_ids=[1, 2])
        manifest_entry = create_manifest_entry_with_delete_file(delete_file, sequence_number=10)

        wrapper = EqualityDeleteFileWrapper(1, manifest_entry, id_data_schema)

        assert wrapper.spec_id == 1
        assert wrapper.delete_file == delete_file
        assert wrapper.apply_sequence_number == 9  # sequence_number - 1

    def test_equality_fields(self, id_data_schema: Schema) -> None:
        delete_file = create_basic_equality_delete_file(equality_ids=[1, 2])
        manifest_entry = create_manifest_entry_with_delete_file(delete_file, sequence_number=10)

        wrapper = EqualityDeleteFileWrapper(1, manifest_entry, id_data_schema)
        fields = wrapper.equality_fields()

        assert len(fields) == 2
        assert fields[0].field_id == 1
        assert fields[1].field_id == 2

    def test_missing_field_handling(self, simple_id_schema: Schema) -> None:
        """Testing wrapper with non-existent field ID"""
        delete_file = create_basic_equality_delete_file(equality_ids=[1, 999])  # 999 doesn't exist
        manifest_entry = create_manifest_entry_with_delete_file(delete_file, sequence_number=10)

        wrapper = EqualityDeleteFileWrapper(1, manifest_entry, simple_id_schema)
        
        # Error raised when trying to find non-existent field
        with pytest.raises(ValueError, match="Could not find field with id: 999"):
            wrapper.equality_fields()


class TestEqualityDeletesGroup:
    def test_add_and_filter(self, simple_id_schema: Schema) -> None:
        """Testing adding files and filtering"""
        group = EqualityDeletesGroup()

        for seq in [4, 2, 1, 3]:
            delete_file = create_basic_equality_delete_file(
                file_path=f"s3://bucket/eq-delete-{seq}.parquet",
                equality_ids=[1]
            )
            manifest_entry = create_manifest_entry_with_delete_file(delete_file, sequence_number=seq)
            wrapper = EqualityDeleteFileWrapper(1, manifest_entry, simple_id_schema)
            group.add(wrapper)

        # Test that files are not sorted initially
        assert not group._sorted

        data_file = create_basic_data_file()

        # Test filtering - triggers indexing
        result = group.filter(2, data_file)

        # Check if sorted now
        assert group._sorted
        assert group._seqs == [0, 1, 2, 3]
        assert len(result) == 2

    def test_empty_group(self) -> None:
        group = EqualityDeletesGroup()
        data_file = create_basic_data_file()

        result = group.filter(5, data_file)
        assert result == []
        assert group._sorted

    def test_non_equality_delete_ignored(self, simple_id_schema: Schema) -> None:
        """Testing if non-equality delete files are ignored"""
        group = EqualityDeletesGroup()

        # Try to add a positional delete file
        pos_delete_file = DataFile.from_args(
            content=DataFileContent.POSITION_DELETES,
            file_path="s3://bucket/pos-delete.parquet",
            file_format=FileFormat.PARQUET,
            partition={},
            record_count=5,
            file_size_in_bytes=50,
        )

        manifest_entry = create_manifest_entry_with_delete_file(pos_delete_file, sequence_number=1)
        eq_delete_file = create_basic_equality_delete_file(equality_ids=[1])
        eq_manifest_entry = create_manifest_entry_with_delete_file(eq_delete_file, sequence_number=1)
        wrapper = EqualityDeleteFileWrapper(1, eq_manifest_entry, simple_id_schema)
        group.add(wrapper)
        assert len(group._buffer) == 1

    def test_no_equality_ids_ignored(self, simple_id_schema: Schema) -> None:
        """Test that delete files without equality_ids are handled at the index level"""
        delete_index = DeleteFileIndex(simple_id_schema)

        delete_file = DataFile.from_args(
            content=DataFileContent.EQUALITY_DELETES,
            file_path="s3://bucket/eq-delete.parquet",
            file_format=FileFormat.PARQUET,
            partition={},
            record_count=5,
            file_size_in_bytes=50,
            equality_ids=None,
        )

        manifest_entry = create_manifest_entry_with_delete_file(delete_file, sequence_number=1)
        delete_index.add_delete_file(1, manifest_entry)

        # Should be ignored due to missing equality_ids
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

        wrapper = PositionalDeleteFileWrapper(1, manifest_entry, simple_id_schema)

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
                lower_bounds={2147483546: "s3://bucket/data.parquet".encode()},
                upper_bounds={2147483546: "s3://bucket/data.parquet".encode()},
            )
            
            manifest_entry = ManifestEntry.from_args(
                status=ManifestEntryStatus.ADDED,
                sequence_number=seq,
                data_file=pos_delete_file,
            )
            
            wrapper = PositionalDeleteFileWrapper(1, manifest_entry, simple_id_schema)
            group.add(wrapper)

        data_file = create_basic_data_file(file_path="s3://bucket/data.parquet")

        assert len(group.filter(0, data_file)) == 3
        assert len(group.filter(2, data_file)) == 2
        assert len(group.filter(6, data_file)) == 0

    def test_file_path_matching(self, simple_id_schema: Schema) -> None:
        group = PositionalDeletesGroup()

        pos_delete_file = DataFile.from_args(
            content=DataFileContent.POSITION_DELETES,
            file_path="s3://bucket/pos-delete.parquet",
            file_format=FileFormat.PARQUET,
            partition={},
            record_count=10,
            file_size_in_bytes=100,
            lower_bounds={2147483546: "s3://bucket/target-data.parquet".encode()},
            upper_bounds={2147483546: "s3://bucket/target-data.parquet".encode()},
        )
        
        manifest_entry = ManifestEntry.from_args(
            status=ManifestEntryStatus.ADDED,
            sequence_number=5,
            data_file=pos_delete_file,
        )
        
        wrapper = PositionalDeleteFileWrapper(1, manifest_entry, simple_id_schema)
        group.add(wrapper)

        matching_data_file = create_basic_data_file(file_path="s3://bucket/target-data.parquet")
        result = group.filter(1, matching_data_file)
        assert len(result) == 1

        non_matching_data_file = create_basic_data_file(file_path="s3://bucket/other-data.parquet")
        result = group.filter(1, non_matching_data_file)
        assert len(result) == 0

class TestDeleteFileIndex:
    @pytest.fixture
    def delete_index(self, simple_id_schema: Schema) -> DeleteFileIndex:
        """Fixture to create a DeleteFileIndex with simple schema"""
        return DeleteFileIndex(simple_id_schema)

    def test_partition_key_to_string(self, delete_index: DeleteFileIndex) -> None:
        assert delete_index._partition_key_to_string(None) is None

        class MockPartitionKey:
            def __init__(self, data):
                self._data = data

        mock_key = MockPartitionKey([])
        assert delete_index._partition_key_to_string(mock_key) == "unpartitioned"

        mock_key = MockPartitionKey(["value1", "value2"])
        assert delete_index._partition_key_to_string(mock_key) == "value1/value2"

        simple_key = "simple_partition"
        assert delete_index._partition_key_to_string(simple_key) == "simple_partition"

    def test_add_and_retrieve(self, delete_index: DeleteFileIndex) -> None:
        # global delete
        global_delete = create_equality_delete_entry(sequence_number=5, equality_ids=[1])
        delete_index.add_delete_file(1, global_delete, partition_key=None)

        # partition-specific delete
        partition_delete = create_equality_delete_entry(sequence_number=3, equality_ids=[1])
        delete_index.add_delete_file(1, partition_delete, partition_key="partition1")

        data_file = create_basic_data_file()

        # both global and partition deletes for sequence 2 are applied
        result = delete_index.for_data_file(2, data_file, partition_key="partition1")
        assert len(result) == 2

        # only global delete for different partition applied
        result = delete_index.for_data_file(2, data_file, partition_key="partition2")
        assert len(result) == 1

        # should get global delete for unpartitioned
        result = delete_index.for_data_file(2, data_file, partition_key=None)
        assert len(result) == 1

    def test_sequence_number_filtering(self, delete_index: DeleteFileIndex) -> None:
        """Test sequence number based filtering"""
        # Add delete files with different sequence numbers
        for seq in [1, 2, 3, 4]:
            delete_entry = create_equality_delete_entry(sequence_number=seq, equality_ids=[1])
            delete_index.add_delete_file(1, delete_entry)

        data_file = create_basic_data_file()

        assert len(delete_index.for_data_file(0, data_file)) == 4
        assert len(delete_index.for_data_file(1, data_file)) == 3
        assert len(delete_index.for_data_file(2, data_file)) == 2
        assert len(delete_index.for_data_file(3, data_file)) == 1
        assert len(delete_index.for_data_file(4, data_file)) == 0

    def test_mixed_delete_types(self, delete_index: DeleteFileIndex) -> None:
        pos_delete_entry = create_positional_delete_entry(sequence_number=5, file_path="s3://bucket/data.parquet")
        delete_index.add_delete_file(1, pos_delete_entry)

        eq_delete_entry = create_equality_delete_entry(sequence_number=3, equality_ids=[1])
        delete_index.add_delete_file(1, eq_delete_entry)

        data_file = create_basic_data_file(file_path="s3://bucket/data.parquet")

        result = delete_index.for_data_file(2, data_file)
        
        # Should have both positional and equality deletes in contents
        contents = {df.content for df in result}
        assert DataFileContent.POSITION_DELETES in contents
        assert DataFileContent.EQUALITY_DELETES in contents

    def test_positional_delete_partition_isolation(self, delete_index: DeleteFileIndex) -> None:
        global_pos_delete = create_positional_delete_entry(sequence_number=5, file_path="s3://bucket/data.parquet")
        delete_index.add_delete_file(1, global_pos_delete, partition_key=None)

        partition_pos_delete = create_positional_delete_entry(sequence_number=3, file_path="s3://bucket/data.parquet")
        partition_key_1 = {"bucket": 1}
        delete_index.add_delete_file(1, partition_pos_delete, partition_key=partition_key_1)

        data_file = create_basic_data_file(file_path="s3://bucket/data.parquet")

        result = delete_index.for_data_file(1, data_file, partition_key=partition_key_1)
        pos_deletes = [df for df in result if df.content == DataFileContent.POSITION_DELETES]
        assert len(pos_deletes) == 2

        result = delete_index.for_data_file(1, data_file, partition_key=None)
        pos_deletes = [df for df in result if df.content == DataFileContent.POSITION_DELETES]
        assert len(pos_deletes) == 1

    def test_empty_index(self, delete_index: DeleteFileIndex) -> None:
        """Test empty delete file index"""
        data_file = create_basic_data_file()
        result = delete_index.for_data_file(1, data_file)
        assert result == []
