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
import pytest

from pyiceberg.table.partition_statistics import (
    PartitionStatistics,
    partition_statistics_schema,
)
from pyiceberg.typedef import Record
from pyiceberg.types import IntegerType, NestedField, StringType, StructType


def test_partition_statistics_creation() -> None:
    """Test creating PartitionStatistics record."""
    partition = Record("2023-01-01")
    stats = PartitionStatistics(
        partition,  # partition
        1,  # spec_id
        100,  # data_record_count
        5,  # data_file_count
        1024,  # total_data_file_size_in_bytes
        10,  # position_delete_record_count
        2,  # position_delete_file_count
        5,  # equality_delete_record_count
        1,  # equality_delete_file_count
        95,  # total_record_count
        1672531200000,  # last_updated_at
        123,  # last_updated_snapshot_id
        0,  # dv_count
    )

    assert stats.spec_id == 1
    assert stats.data_record_count == 100
    assert stats.data_file_count == 5
    assert stats.total_data_file_size_in_bytes == 1024
    assert stats.position_delete_record_count == 10
    assert stats.position_delete_file_count == 2
    assert stats.equality_delete_record_count == 5
    assert stats.equality_delete_file_count == 1
    assert stats.total_record_count == 95
    assert stats.last_updated_at == 1672531200000
    assert stats.last_updated_snapshot_id == 123
    assert stats.dv_count == 0


def test_partition_statistics_v2_schema() -> None:
    """Test v2 schema generation (optional delete fields)."""
    partition_type = StructType(
        NestedField(field_id=1000, name="year", field_type=IntegerType(), required=True),
        NestedField(field_id=1001, name="month", field_type=IntegerType(), required=True),
    )

    schema = partition_statistics_schema(partition_type, format_version=2)

    assert len(schema.fields) == 12
    assert schema.fields[0].name == "partition"
    assert schema.fields[0].field_type == partition_type
    assert schema.fields[1].name == "spec_id"
    assert schema.fields[2].name == "data_record_count"
    assert schema.fields[5].name == "position_delete_record_count"
    assert not schema.fields[5].required  # optional in v2


def test_partition_statistics_v3_schema() -> None:
    """Test v3 schema generation (required delete fields + dv_count)."""
    partition_type = StructType(
        NestedField(field_id=1000, name="date", field_type=StringType(), required=True),
    )

    schema = partition_statistics_schema(partition_type, format_version=3)

    assert len(schema.fields) == 13
    assert schema.fields[0].name == "partition"
    assert schema.fields[5].name == "position_delete_record_count"
    assert schema.fields[5].required  # required in v3
    assert schema.fields[12].name == "dv_count"
    assert schema.fields[12].required
    assert schema.fields[12].initial_default == 0


def test_partition_statistics_unpartitioned_table() -> None:
    """Test that unpartitioned table raises error."""
    empty_partition_type = StructType()

    with pytest.raises(ValueError, match="Table must be partitioned"):
        partition_statistics_schema(empty_partition_type, format_version=2)


def test_partition_statistics_invalid_format_version() -> None:
    """Test invalid format version raises error."""
    partition_type = StructType(
        NestedField(field_id=1000, name="year", field_type=IntegerType(), required=True),
    )

    with pytest.raises(ValueError, match="Invalid format version"):
        partition_statistics_schema(partition_type, format_version=0)


def test_partition_statistics_set_method() -> None:
    """Test updating stats fields using set method."""
    partition = Record("2023-01-01")
    stats = PartitionStatistics(
        partition,
        1,  # spec_id
        100,  # data_record_count
        5,  # data_file_count
        1024,  # total_data_file_size_in_bytes
        0,
        0,
        0,
        0,
        None,
        None,
        None,
        0,
    )

    # Update using set
    stats.set(2, 200)  # data_record_count
    stats.set(3, 10)  # data_file_count

    assert stats.data_record_count == 200
    assert stats.data_file_count == 10


def test_write_partition_statistics_file(tmp_path: str) -> None:
    """Test writing partition statistics to file."""
    from pyiceberg.io.pyarrow import PyArrowFileIO
    from pyiceberg.partitioning import PartitionField, PartitionSpec
    from pyiceberg.schema import Schema
    from pyiceberg.table.metadata import TableMetadataV2
    from pyiceberg.table.partition_statistics import write_partition_statistics_file
    from pyiceberg.transforms import IdentityTransform

    # Create minimal table metadata with partition spec
    schema = Schema(
        NestedField(field_id=1, name="year", field_type=IntegerType(), required=True),
        NestedField(field_id=2, name="data", field_type=StringType(), required=True),
    )

    partition_spec = PartitionSpec(
        PartitionField(source_id=1, field_id=1000, transform=IdentityTransform(), name="year"),
        spec_id=0,
    )

    table_metadata = TableMetadataV2(
        location=str(tmp_path),
        last_column_id=2,
        schemas=[schema],
        current_schema_id=0,
        partition_specs=[partition_spec],
        default_spec_id=0,
        last_partition_id=1000,
    )

    # Create test statistics
    partition1 = Record(2023)
    stats1 = PartitionStatistics(
        partition1,
        0,  # spec_id
        100,  # data_record_count
        5,  # data_file_count
        1024,  # total_data_file_size_in_bytes
        0,
        0,
        0,
        0,
        None,
        None,
        None,
        0,
    )

    partition2 = Record(2024)
    stats2 = PartitionStatistics(
        partition2,
        0,  # spec_id
        200,  # data_record_count
        10,  # data_file_count
        2048,  # total_data_file_size_in_bytes
        0,
        0,
        0,
        0,
        None,
        None,
        None,
        0,
    )

    # Write to file
    io = PyArrowFileIO()
    stats_file = write_partition_statistics_file(
        io=io,
        table_metadata=table_metadata,
        snapshot_id=123,
        statistics=[stats1, stats2],
    )

    # Verify metadata
    assert stats_file.snapshot_id == 123
    assert stats_file.file_size_in_bytes > 0
    assert "partition-stats-123-" in stats_file.statistics_path
    assert stats_file.statistics_path.endswith(".parquet")


def test_write_and_read_partition_statistics(tmp_path: str) -> None:
    """Test writing and reading partition statistics roundtrip."""
    from pyiceberg.io.pyarrow import PyArrowFileIO
    from pyiceberg.partitioning import PartitionField, PartitionSpec
    from pyiceberg.schema import Schema
    from pyiceberg.table.metadata import TableMetadataV2
    from pyiceberg.table.partition_statistics import (
        PartitionStatisticsScanArrow,
        write_partition_statistics_file,
    )
    from pyiceberg.transforms import IdentityTransform

    # Create table metadata
    schema = Schema(
        NestedField(field_id=1, name="year", field_type=IntegerType(), required=True),
        NestedField(field_id=2, name="data", field_type=StringType(), required=True),
    )

    partition_spec = PartitionSpec(
        PartitionField(source_id=1, field_id=1000, transform=IdentityTransform(), name="year"),
        spec_id=0,
    )

    table_metadata = TableMetadataV2(
        location=str(tmp_path),
        last_column_id=2,
        schemas=[schema],
        current_schema_id=0,
        partition_specs=[partition_spec],
        default_spec_id=0,
        last_partition_id=1000,
    )

    # Create test statistics
    original_stats = [
        PartitionStatistics(Record(2023), 0, 100, 5, 1024, 0, 0, 0, 0, None, None, None, 0),
        PartitionStatistics(Record(2024), 0, 200, 10, 2048, 0, 0, 0, 0, None, None, None, 0),
    ]

    # Write to file
    io = PyArrowFileIO()
    stats_file = write_partition_statistics_file(
        io=io,
        table_metadata=table_metadata,
        snapshot_id=123,
        statistics=original_stats,
    )

    # Update metadata with stats file
    table_metadata.partition_statistics.append(stats_file)

    # Read back
    scan = PartitionStatisticsScanArrow(
        table_metadata=table_metadata,
        io=io,
        snapshot_id=123,
    )

    read_stats = list(scan.scan())

    # Verify roundtrip
    assert len(read_stats) == 2
    assert read_stats[0].spec_id == 0
    assert read_stats[0].data_record_count == 100
    assert read_stats[0].data_file_count == 5
    assert read_stats[1].data_record_count == 200
    assert read_stats[1].data_file_count == 10
