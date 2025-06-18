import pytest

from pyiceberg.manifest import DataFileContent, FileFormat
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
from pyiceberg.typedef import Record
from pyiceberg.types import IntegerType, NestedField, StringType
from tests.conftest import (
    create_basic_data_file,
    create_basic_equality_delete_file,
    create_deletion_vector_entry,
    create_equality_delete_entry,
    create_manifest_entry_with_delete_file,
    create_partition_positional_delete_entry,
    create_positional_delete_entry,
)


@pytest.fixture
def id_data_schema() -> Schema:
    return Schema(
        NestedField(1, "id", IntegerType(), required=True),
        NestedField(2, "data", StringType(), required=True),
    )


@pytest.fixture
def delete_index(id_data_schema: Schema) -> DeleteFileIndex:
    """Create a DeleteFileIndex with the id_data_schema."""
    return DeleteFileIndex(id_data_schema)


class TestDeleteFileIndex:
    """Tests for the DeleteFileIndex class."""

    def test_empty_index(self, delete_index: DeleteFileIndex) -> None:
        """Test that an empty index returns no delete files."""
        data_file = create_basic_data_file()
        assert len(delete_index.for_data_file(1, data_file)) == 0

    def test_min_sequence_number_filtering(self, id_data_schema: Schema) -> None:
        """Test filtering delete files by minimum sequence number."""
        part_spec = PartitionSpec()

        # Create delete files with different sequence numbers
        eq_delete_1 = create_basic_equality_delete_file(equality_ids=[1])
        eq_delete_1._spec_id = 0
        eq_delete_entry_1 = create_manifest_entry_with_delete_file(eq_delete_1, sequence_number=4)

        eq_delete_2 = create_basic_equality_delete_file(equality_ids=[1])
        eq_delete_2._spec_id = 0
        eq_delete_entry_2 = create_manifest_entry_with_delete_file(eq_delete_2, sequence_number=6)

        # Create a delete index with a minimum sequence number filter
        delete_index = DeleteFileIndex(id_data_schema, {0: part_spec})
        delete_index.add_delete_file(eq_delete_entry_1)
        delete_index.add_delete_file(eq_delete_entry_2)

        data_file = create_basic_data_file()
        data_file._spec_id = 0

        # Only one delete file should apply with sequence number > 4
        result = delete_index.for_data_file(4, data_file)
        assert len(result) == 1
        assert result[0].file_path == eq_delete_2.file_path

    def test_unpartitioned_deletes(self, id_data_schema: Schema) -> None:
        """Test unpartitioned delete files with different sequence numbers."""
        part_spec = PartitionSpec()

        # Unpartitioned equality delete files
        eq_delete_1 = create_basic_equality_delete_file(equality_ids=[1])
        eq_delete_1._spec_id = 0
        eq_delete_entry_1 = create_manifest_entry_with_delete_file(eq_delete_1, sequence_number=4)

        eq_delete_2 = create_basic_equality_delete_file(equality_ids=[1])
        eq_delete_2._spec_id = 0
        eq_delete_entry_2 = create_manifest_entry_with_delete_file(eq_delete_2, sequence_number=6)

        # Path specific position delete files
        pos_delete_1 = create_positional_delete_entry(sequence_number=5, spec_id=0)
        pos_delete_2 = create_positional_delete_entry(sequence_number=6, spec_id=0)

        # Create delete index
        delete_index = DeleteFileIndex(id_data_schema, {0: part_spec, 1: PartitionSpec()})
        delete_index.add_delete_file(eq_delete_entry_1)
        delete_index.add_delete_file(eq_delete_entry_2)
        delete_index.add_delete_file(pos_delete_1)
        delete_index.add_delete_file(pos_delete_2)

        #  Unpartitioned data file
        data_file = create_basic_data_file()
        data_file._spec_id = 0

        # All deletes should apply
        result = delete_index.for_data_file(0, data_file)
        assert len(result) == 4

        # All deletes should apply
        result = delete_index.for_data_file(3, data_file)
        assert len(result) == 4

        # Only last 3 deletes should apply
        result = delete_index.for_data_file(4, data_file)
        assert len(result) == 3

        # Last 2 deletes should apply
        result = delete_index.for_data_file(5, data_file)
        assert len(result) == 3

        # Only last delete should apply
        result = delete_index.for_data_file(6, data_file)
        assert len(result) == 1

        # No deletes applied
        result = delete_index.for_data_file(7, data_file)
        assert len(result) == 0

        # Global equality deletes and path specific position deletes should apply to partitioned file
        partitioned_file = create_basic_data_file(partition={"id": 1})
        partitioned_file._spec_id = 1

        result = delete_index.for_data_file(0, partitioned_file)
        assert len(result) == 4

    def test_partitioned_delete_index(self, id_data_schema: Schema) -> None:
        """Test partitioned delete files with different sequence numbers."""
        part_spec = PartitionSpec(PartitionField(source_id=1, field_id=1000, transform=IdentityTransform(), name="id_partition"))

        # Partitioned equality delete files
        partition_key = Record(1)
        eq_delete_1 = create_basic_equality_delete_file(equality_ids=[1], partition=partition_key)
        eq_delete_1._spec_id = 0
        eq_delete_entry_1 = create_manifest_entry_with_delete_file(eq_delete_1, sequence_number=4)

        eq_delete_2 = create_basic_equality_delete_file(equality_ids=[1], partition=partition_key)
        eq_delete_2._spec_id = 0
        eq_delete_entry_2 = create_manifest_entry_with_delete_file(eq_delete_2, sequence_number=6)

        # Position delete files with partition
        pos_delete_1 = create_partition_positional_delete_entry(sequence_number=5, spec_id=0, partition=partition_key)
        pos_delete_2 = create_partition_positional_delete_entry(sequence_number=6, spec_id=0, partition=partition_key)

        # Create delete index
        delete_index = DeleteFileIndex(id_data_schema, {0: part_spec, 1: PartitionSpec()})
        delete_index.add_delete_file(eq_delete_entry_1, partition_key=partition_key)
        delete_index.add_delete_file(eq_delete_entry_2, partition_key=partition_key)
        delete_index.add_delete_file(pos_delete_1, partition_key=partition_key)
        delete_index.add_delete_file(pos_delete_2, partition_key=partition_key)

        # Data file with same partition
        data_file_a = create_basic_data_file(partition={"id": 1})
        data_file_a._spec_id = 0

        result = delete_index.for_data_file(0, data_file_a, partition_key=partition_key)
        assert len(result) == 4

        result = delete_index.for_data_file(3, data_file_a, partition_key=partition_key)
        assert len(result) == 4

        result = delete_index.for_data_file(4, data_file_a, partition_key=partition_key)
        assert len(result) == 3

        result = delete_index.for_data_file(5, data_file_a, partition_key=partition_key)
        assert len(result) == 3

        result = delete_index.for_data_file(6, data_file_a, partition_key=partition_key)
        assert len(result) == 1

        # No deletes should apply to seq 7
        result = delete_index.for_data_file(7, data_file_a, partition_key=partition_key)
        assert len(result) == 0

        # Test with file in different partition
        data_file_b = create_basic_data_file(partition={"id": 2})
        data_file_b._spec_id = 0
        different_partition_key = Record(2)

        # No deletes should apply to file in different partition
        result = delete_index.for_data_file(0, data_file_b, partition_key=different_partition_key)
        assert len(result) == 0

        # Test with unpartitioned file
        unpartitioned_file = create_basic_data_file()
        unpartitioned_file._spec_id = 1

        # No partition deletes should apply to unpartitioned file
        result = delete_index.for_data_file(0, unpartitioned_file)
        assert len(result) == 0

    def test_partitioned_table_scan_with_global_deletes(self, id_data_schema: Schema) -> None:
        """Test that global equality deletes apply to partitioned files."""
        # Create partitioned spec
        part_spec = PartitionSpec(PartitionField(source_id=1, field_id=1000, transform=IdentityTransform(), name="id_partition"))

        # Create partitioned data file
        partition_key = Record(1)
        data_file = create_basic_data_file(partition={"id": 1})
        data_file._spec_id = 0

        # Create unpartitioned equality delete file (global)
        unpart_eq_delete = create_basic_equality_delete_file(equality_ids=[1])
        unpart_eq_delete._spec_id = 1  # Unpartitioned spec
        unpart_eq_delete_entry = create_manifest_entry_with_delete_file(unpart_eq_delete, sequence_number=5)

        # Create unpartitioned position delete file
        unpart_pos_delete = create_partition_positional_delete_entry(sequence_number=5, spec_id=1)

        # Create delete index
        delete_index = DeleteFileIndex(id_data_schema, {0: part_spec, 1: PartitionSpec()})
        delete_index.add_delete_file(unpart_eq_delete_entry)
        delete_index.add_delete_file(unpart_pos_delete)

        # Test that only global equality deletes apply to partitioned file
        result = delete_index.for_data_file(0, data_file, partition_key=partition_key)
        assert len(result) == 1
        assert result[0].content == DataFileContent.EQUALITY_DELETES
        assert result[0].file_path == unpart_eq_delete.file_path

    def test_partitioned_table_scan_with_global_and_partition_deletes(self, id_data_schema: Schema) -> None:
        """Test that both global and partition-specific deletes apply to partitioned files."""
        part_spec = PartitionSpec(PartitionField(source_id=1, field_id=1000, transform=IdentityTransform(), name="id_partition"))

        # Partitioned data file
        partition_key = Record(1)
        data_file = create_basic_data_file(partition={"id": 1})
        data_file._spec_id = 0

        # Partitioned equality delete file
        part_eq_delete = create_basic_equality_delete_file(equality_ids=[1], partition=partition_key)
        part_eq_delete._spec_id = 0
        part_eq_delete_entry = create_manifest_entry_with_delete_file(part_eq_delete, sequence_number=4)

        # Unpartitioned equality delete file (global)
        unpart_eq_delete = create_basic_equality_delete_file(equality_ids=[1])
        unpart_eq_delete._spec_id = 1  # Unpartitioned spec
        unpart_eq_delete_entry = create_manifest_entry_with_delete_file(unpart_eq_delete, sequence_number=5)

        # Unpartitioned position delete file
        unpart_pos_delete = create_partition_positional_delete_entry(sequence_number=5, spec_id=1)
        part_pos_delete = create_partition_positional_delete_entry(sequence_number=5, spec_id=0, partition=partition_key)

        delete_index = DeleteFileIndex(id_data_schema, {0: part_spec, 1: PartitionSpec()})
        delete_index.add_delete_file(part_eq_delete_entry, partition_key=partition_key)
        delete_index.add_delete_file(unpart_eq_delete_entry)
        delete_index.add_delete_file(unpart_pos_delete)
        delete_index.add_delete_file(part_pos_delete, partition_key=partition_key)

        # Test that both partition-specific deletes and global equality deletes apply
        result = delete_index.for_data_file(0, data_file, partition_key=partition_key)
        assert len(result) == 3

        file_paths = {d.file_path for d in result}
        assert part_eq_delete.file_path in file_paths
        assert unpart_eq_delete.file_path in file_paths

    def test_partitioned_table_sequence_numbers(self, id_data_schema: Schema) -> None:
        """Test sequence number handling in partitioned tables."""
        data_file = create_basic_data_file(file_path="s3://bucket/data.parquet", partition={"id": 1})
        data_file._spec_id = 0

        eq_delete = create_basic_equality_delete_file(
            file_path="s3://bucket/eq-delete.parquet", equality_ids=[1], partition=Record(1)
        )
        eq_delete._spec_id = 0
        eq_delete_entry = create_manifest_entry_with_delete_file(eq_delete, sequence_number=5)

        pos_delete = create_positional_delete_entry(sequence_number=5, file_path="s3://bucket/data.parquet", spec_id=0)

        part_spec = PartitionSpec(PartitionField(source_id=1, field_id=1000, transform=IdentityTransform(), name="id_partition"))
        delete_index = DeleteFileIndex(id_data_schema, {0: part_spec})
        delete_index.add_delete_file(eq_delete_entry, partition_key=Record(1))
        delete_index.add_delete_file(pos_delete, partition_key=Record(1))

        # Position deletes apply to data file with same sequence number
        result = delete_index.for_data_file(5, data_file, partition_key=Record(1))

        # Only position deletes should apply to files with the same sequence number
        pos_deletes = [d for d in result if d.content == DataFileContent.POSITION_DELETES]
        eq_deletes = [d for d in result if d.content == DataFileContent.EQUALITY_DELETES]

        assert len(pos_deletes) == 1
        assert len(eq_deletes) == 0

    def test_unpartitioned_table_sequence_numbers(self, id_data_schema: Schema) -> None:
        """Test sequence number handling in unpartitioned tables."""
        data_file = create_basic_data_file(file_path="s3://bucket/data.parquet")
        data_file._spec_id = 0

        eq_delete = create_basic_equality_delete_file(file_path="s3://bucket/eq-delete.parquet", equality_ids=[1])
        eq_delete._spec_id = 0
        eq_delete_entry = create_manifest_entry_with_delete_file(eq_delete, sequence_number=5)

        pos_delete = create_positional_delete_entry(sequence_number=5, file_path="s3://bucket/data.parquet", spec_id=0)
        delete_index = DeleteFileIndex(id_data_schema, {0: PartitionSpec()})
        delete_index.add_delete_file(eq_delete_entry)
        delete_index.add_delete_file(pos_delete)

        # Position deletes apply to data file with same sequence number
        result = delete_index.for_data_file(5, data_file)

        # Only position deletes should apply to files with the same sequence number
        pos_deletes = [d for d in result if d.content == DataFileContent.POSITION_DELETES]
        eq_deletes = [d for d in result if d.content == DataFileContent.EQUALITY_DELETES]

        assert len(pos_deletes) == 1
        assert len(eq_deletes) == 0

    def test_position_deletes_group(self) -> None:
        """Test the PositionalDeletesGroup class."""
        # Create position delete files with different sequence numbers
        pos_delete_1 = create_positional_delete_entry(sequence_number=1).data_file
        pos_delete_2 = create_positional_delete_entry(sequence_number=2).data_file
        pos_delete_3 = create_positional_delete_entry(sequence_number=3).data_file
        pos_delete_4 = create_positional_delete_entry(sequence_number=4).data_file

        # PositionalDeletesGroup
        group = PositionalDeletesGroup()
        group.add(PositionalDeleteFileWrapper(create_manifest_entry_with_delete_file(pos_delete_4, sequence_number=4)))
        group.add(PositionalDeleteFileWrapper(create_manifest_entry_with_delete_file(pos_delete_2, sequence_number=2)))
        group.add(PositionalDeleteFileWrapper(create_manifest_entry_with_delete_file(pos_delete_1, sequence_number=1)))
        group.add(PositionalDeleteFileWrapper(create_manifest_entry_with_delete_file(pos_delete_3, sequence_number=3)))

        # Test filtering by sequence number
        result_0 = group.filter(0, create_basic_data_file())
        assert len(result_0) == 4

        result_1 = group.filter(1, create_basic_data_file())
        assert len(result_1) == 4

        result_2 = group.filter(2, create_basic_data_file())
        assert len(result_2) == 3

        result_3 = group.filter(3, create_basic_data_file())
        assert len(result_3) == 2

        result_4 = group.filter(4, create_basic_data_file())
        assert len(result_4) == 1

        result_5 = group.filter(5, create_basic_data_file())
        assert len(result_5) == 0

        # Test that adding files after indexing raises an error
        group._index_if_needed()
        with pytest.raises(ValueError, match="Can't add files to group after indexing"):
            group.add(PositionalDeleteFileWrapper(create_manifest_entry_with_delete_file(pos_delete_1, sequence_number=1)))

    def test_equality_deletes_group(self, id_data_schema: Schema) -> None:
        """Test the EqualityDeletesGroup class."""
        # Create equality delete files with different sequence numbers
        eq_delete_1 = create_basic_equality_delete_file(equality_ids=[1])
        eq_delete_2 = create_basic_equality_delete_file(equality_ids=[1])
        eq_delete_3 = create_basic_equality_delete_file(equality_ids=[1])
        eq_delete_4 = create_basic_equality_delete_file(equality_ids=[1])

        # EqualityDeletesGroup
        group = EqualityDeletesGroup()
        group.add(
            EqualityDeleteFileWrapper(create_manifest_entry_with_delete_file(eq_delete_4, sequence_number=4), id_data_schema)
        )
        group.add(
            EqualityDeleteFileWrapper(create_manifest_entry_with_delete_file(eq_delete_2, sequence_number=2), id_data_schema)
        )
        group.add(
            EqualityDeleteFileWrapper(create_manifest_entry_with_delete_file(eq_delete_1, sequence_number=1), id_data_schema)
        )
        group.add(
            EqualityDeleteFileWrapper(create_manifest_entry_with_delete_file(eq_delete_3, sequence_number=3), id_data_schema)
        )

        data_file = create_basic_data_file()

        # Test filtering by sequence number
        result_0 = group.filter(0, data_file)
        assert len(result_0) == 4

        result_1 = group.filter(1, data_file)
        assert len(result_1) == 3

        result_2 = group.filter(2, data_file)
        assert len(result_2) == 2

        result_3 = group.filter(3, data_file)
        assert len(result_3) == 1

        result_4 = group.filter(4, data_file)
        assert len(result_4) == 0

        # Adding files after indexing raises an error
        group._index_if_needed()
        with pytest.raises(ValueError, match="Can't add files to group after indexing"):
            group.add(
                EqualityDeleteFileWrapper(create_manifest_entry_with_delete_file(eq_delete_1, sequence_number=1), id_data_schema)
            )

    def test_mix_delete_files_and_dvs(self, id_data_schema: Schema) -> None:
        """Test mixing regular delete files and deletion vectors."""
        data_file_a = create_basic_data_file(file_path="s3://bucket/data-a.parquet", partition={"id": 1})
        data_file_a._spec_id = 0

        data_file_b = create_basic_data_file(file_path="s3://bucket/data-b.parquet", partition={"id": 2})
        data_file_b._spec_id = 0

        # Position delete for file A
        pos_delete_a = create_positional_delete_entry(sequence_number=1, file_path="s3://bucket/data-a.parquet", spec_id=0)

        # Deletion vector for file A
        dv_a = create_deletion_vector_entry(sequence_number=2, file_path="s3://bucket/data-a.parquet", spec_id=0)

        # Position deletes for file B
        pos_delete_b1 = create_positional_delete_entry(sequence_number=1, file_path="s3://bucket/data-b.parquet", spec_id=0)
        pos_delete_b2 = create_positional_delete_entry(sequence_number=2, file_path="s3://bucket/data-b.parquet", spec_id=0)

        # Partitioned spec
        part_spec = PartitionSpec(PartitionField(source_id=1, field_id=1000, transform=IdentityTransform(), name="id_partition"))

        delete_index = DeleteFileIndex(id_data_schema, {0: part_spec})
        delete_index.add_delete_file(pos_delete_a)
        delete_index.add_delete_file(dv_a)
        delete_index.add_delete_file(pos_delete_b1)
        delete_index.add_delete_file(pos_delete_b2)

        # Test file A - DV and positional deletes will be added for file A
        result_a = delete_index.for_data_file(0, data_file_a)
        assert len(result_a) == 2

        # Test file B - both position deletes for file B apply and DV
        result_b = delete_index.for_data_file(0, data_file_b)
        assert len(result_b) == 3
        assert all(d.content == DataFileContent.POSITION_DELETES for d in result_b)

    def test_equality_delete_bounds_filtering(self, id_data_schema: Schema) -> None:
        """Test that equality deletes use bounds to filter out impossible matches."""
        # Create data file with bounds
        data_file = create_basic_data_file(
            lower_bounds={1: b"\x05\x00\x00\x00"},  # id >= 5
            upper_bounds={1: b"\x0a\x00\x00\x00"},  # id <= 10
        )

        # With non-overlapping bounds
        delete_index1 = DeleteFileIndex(id_data_schema)
        eq_delete_file = create_basic_equality_delete_file(
            equality_ids=[1],
            lower_bounds={1: b"\x0f\x00\x00\x00"},  # id >= 15
            upper_bounds={1: b"\x14\x00\x00\x00"},  # id <= 20
        )
        eq_delete_entry = create_manifest_entry_with_delete_file(eq_delete_file)
        delete_index1.add_delete_file(eq_delete_entry)

        # Should not apply because bounds don't overlap
        assert len(delete_index1.for_data_file(0, data_file)) == 0

        # Overlapping bounds
        delete_index2 = DeleteFileIndex(id_data_schema)
        eq_delete_file2 = create_basic_equality_delete_file(
            equality_ids=[1],
            lower_bounds={1: b"\x08\x00\x00\x00"},  # id >= 8
            upper_bounds={1: b"\x0f\x00\x00\x00"},  # id <= 15
        )
        eq_delete_entry2 = create_manifest_entry_with_delete_file(eq_delete_file2)
        delete_index2.add_delete_file(eq_delete_entry2)

        # Should apply because bounds overlap
        assert len(delete_index2.for_data_file(0, data_file)) == 1

    def test_equality_delete_null_filtering(self) -> None:
        """Test that equality deletes use null counts to filter out impossible matches."""
        schema = Schema(
            NestedField(1, "id", IntegerType(), required=False),
            NestedField(2, "data", StringType(), required=False),
        )

        data_file = create_basic_data_file(
            value_counts={1: 100, 2: 100},
            null_value_counts={1: 100, 2: 0},  # All values in field 1 are null
        )

        delete_index1 = DeleteFileIndex(schema)
        eq_delete_file = create_basic_equality_delete_file(
            equality_ids=[1],
            value_counts={1: 10},
            null_value_counts={1: 0},  # No nulls in delete file
        )
        eq_delete_entry = create_manifest_entry_with_delete_file(eq_delete_file)
        delete_index1.add_delete_file(eq_delete_entry)

        # Should not apply because data is all nulls but delete doesn't delete nulls
        assert len(delete_index1.for_data_file(0, data_file)) == 0

        delete_index2 = DeleteFileIndex(schema)
        eq_delete_file2 = create_basic_equality_delete_file(
            equality_ids=[1],
            value_counts={1: 10},
            null_value_counts={1: 5},  # Has nulls in delete file
        )
        eq_delete_entry2 = create_manifest_entry_with_delete_file(eq_delete_file2)
        delete_index2.add_delete_file(eq_delete_entry2)

        # Should apply because delete file has nulls
        assert len(delete_index2.for_data_file(0, data_file)) == 1

    def test_all_delete_types(self, id_data_schema: Schema) -> None:
        """Test that when all three delete types target the same file."""
        file_path = "s3://bucket/data.parquet"

        delete_index = DeleteFileIndex(id_data_schema)

        # Add an equality delete
        eq_delete_entry = create_equality_delete_entry(sequence_number=5, equality_ids=[1])
        delete_index.add_delete_file(eq_delete_entry)

        # Add a position delete
        pos_delete_entry = create_positional_delete_entry(sequence_number=5, file_path=file_path)
        delete_index.add_delete_file(pos_delete_entry)

        # Add a deletion vector
        dv_entry = create_deletion_vector_entry(sequence_number=5, file_path=file_path)
        delete_index.add_delete_file(dv_entry)

        data_file = create_basic_data_file(file_path=file_path)
        deletes = delete_index.for_data_file(4, data_file)

        # Should all deletes
        assert len(deletes) == 3

        eq_deletes = [d for d in deletes if d.content == DataFileContent.EQUALITY_DELETES]
        assert len(eq_deletes) == 1

        dv_deletes = [d for d in deletes if d.file_format == FileFormat.PUFFIN]
        assert len(dv_deletes) == 1

        # Verify that no position deletes are included
        pos_deletes = [d for d in deletes if d.content == DataFileContent.POSITION_DELETES and d.file_format != FileFormat.PUFFIN]
        assert len(pos_deletes) == 1
