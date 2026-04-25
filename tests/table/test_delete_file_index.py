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

from pyiceberg.conversions import to_bytes
from pyiceberg.manifest import DataFile, DataFileContent, FileFormat, ManifestEntry, ManifestEntryStatus
from pyiceberg.schema import Schema
from pyiceberg.table.delete_file_index import PATH_FIELD_ID, DeleteFileIndex, PositionDeletes
from pyiceberg.typedef import Record
from pyiceberg.types import IntegerType, NestedField


def _create_data_file(
    file_path: str = "s3://bucket/data.parquet",
    spec_id: int = 0,
    lower_bounds: dict[int, bytes] | None = None,
    upper_bounds: dict[int, bytes] | None = None,
) -> DataFile:
    data_file = DataFile.from_args(
        content=DataFileContent.DATA,
        file_path=file_path,
        file_format=FileFormat.PARQUET,
        partition=Record(),
        record_count=100,
        file_size_in_bytes=1000,
        lower_bounds=lower_bounds,
        upper_bounds=upper_bounds,
    )
    data_file._spec_id = spec_id
    return data_file


def _create_positional_delete(
    sequence_number: int = 1, file_path: str = "s3://bucket/data.parquet", spec_id: int = 0
) -> ManifestEntry:
    delete_file = DataFile.from_args(
        content=DataFileContent.POSITION_DELETES,
        file_path=f"s3://bucket/pos-delete-{sequence_number}.parquet",
        file_format=FileFormat.PARQUET,
        partition=Record(),
        record_count=10,
        file_size_in_bytes=100,
        lower_bounds={PATH_FIELD_ID: file_path.encode()},
        upper_bounds={PATH_FIELD_ID: file_path.encode()},
    )
    delete_file._spec_id = spec_id
    return ManifestEntry.from_args(status=ManifestEntryStatus.ADDED, sequence_number=sequence_number, data_file=delete_file)


def _create_partition_delete(sequence_number: int = 1, spec_id: int = 0, partition: Record | None = None) -> ManifestEntry:
    delete_file = DataFile.from_args(
        content=DataFileContent.POSITION_DELETES,
        file_path=f"s3://bucket/pos-delete-{sequence_number}.parquet",
        file_format=FileFormat.PARQUET,
        partition=partition or Record(),
        record_count=10,
        file_size_in_bytes=100,
    )
    delete_file._spec_id = spec_id
    return ManifestEntry.from_args(status=ManifestEntryStatus.ADDED, sequence_number=sequence_number, data_file=delete_file)


def _create_deletion_vector(
    sequence_number: int = 1, file_path: str = "s3://bucket/data.parquet", spec_id: int = 0
) -> ManifestEntry:
    delete_file = DataFile.from_args(
        content=DataFileContent.POSITION_DELETES,
        file_path=f"s3://bucket/deletion-vector-{sequence_number}.puffin",
        file_format=FileFormat.PUFFIN,
        partition=Record(),
        record_count=10,
        file_size_in_bytes=100,
        lower_bounds={PATH_FIELD_ID: file_path.encode()},
        upper_bounds={PATH_FIELD_ID: file_path.encode()},
    )
    delete_file._spec_id = spec_id
    return ManifestEntry.from_args(status=ManifestEntryStatus.ADDED, sequence_number=sequence_number, data_file=delete_file)


def _create_equality_delete(
    sequence_number: int = 1,
    spec_id: int = 0,
    lower_bounds: dict[int, bytes] | None = None,
    upper_bounds: dict[int, bytes] | None = None,
) -> ManifestEntry:
    delete_file = DataFile.from_args(
        content=DataFileContent.EQUALITY_DELETES,
        file_path=f"s3://bucket/eq-delete-{sequence_number}.parquet",
        file_format=FileFormat.PARQUET,
        partition=Record(),
        record_count=10,
        file_size_in_bytes=100,
        equality_ids=[1],
        lower_bounds=lower_bounds,
        upper_bounds=upper_bounds,
    )
    delete_file._spec_id = spec_id
    return ManifestEntry.from_args(status=ManifestEntryStatus.ADDED, sequence_number=sequence_number, data_file=delete_file)


def test_empty_index() -> None:
    index = DeleteFileIndex()
    data_file = _create_data_file()
    assert index.for_data_file(1, data_file) == set()


def test_sequence_number_filtering() -> None:
    index = DeleteFileIndex()

    index.add_delete_file(_create_positional_delete(sequence_number=2))
    index.add_delete_file(_create_positional_delete(sequence_number=4))
    index.add_delete_file(_create_positional_delete(sequence_number=6))

    data_file = _create_data_file()

    assert len(index.for_data_file(1, data_file)) == 3
    assert len(index.for_data_file(2, data_file)) == 3
    assert len(index.for_data_file(3, data_file)) == 2
    assert len(index.for_data_file(5, data_file)) == 1
    assert len(index.for_data_file(7, data_file)) == 0


def test_path_specific_deletes() -> None:
    index = DeleteFileIndex()

    index.add_delete_file(_create_positional_delete(sequence_number=2, file_path="s3://bucket/a.parquet"))
    index.add_delete_file(_create_positional_delete(sequence_number=2, file_path="s3://bucket/b.parquet"))

    file_a = _create_data_file(file_path="s3://bucket/a.parquet")
    file_b = _create_data_file(file_path="s3://bucket/b.parquet")
    file_c = _create_data_file(file_path="s3://bucket/c.parquet")

    assert len(index.for_data_file(1, file_a)) == 1
    assert len(index.for_data_file(1, file_b)) == 1
    assert len(index.for_data_file(1, file_c)) == 0


def test_partitioned_deletes() -> None:
    index = DeleteFileIndex()

    partition_1 = Record(1)
    partition_2 = Record(2)

    index.add_delete_file(_create_partition_delete(sequence_number=2, spec_id=0, partition=partition_1), partition_1)
    index.add_delete_file(_create_partition_delete(sequence_number=2, spec_id=0, partition=partition_2), partition_2)

    data_file = _create_data_file()

    assert len(index.for_data_file(1, data_file, partition_1)) == 1
    assert len(index.for_data_file(1, data_file, partition_2)) == 1
    assert len(index.for_data_file(1, data_file, Record(3))) == 0


def test_mix_path_and_partition_deletes() -> None:
    index = DeleteFileIndex()

    partition = Record(1)

    index.add_delete_file(_create_positional_delete(sequence_number=2, file_path="s3://bucket/a.parquet"))
    index.add_delete_file(_create_partition_delete(sequence_number=3, spec_id=0, partition=partition), partition)

    data_file = _create_data_file(file_path="s3://bucket/a.parquet")

    result = index.for_data_file(1, data_file, partition)
    assert len(result) == 2


def test_dvs_treated_as_position_deletes() -> None:
    index = DeleteFileIndex()

    index.add_delete_file(_create_positional_delete(sequence_number=2, file_path="s3://bucket/a.parquet"))
    index.add_delete_file(_create_deletion_vector(sequence_number=3, file_path="s3://bucket/a.parquet"))

    data_file = _create_data_file(file_path="s3://bucket/a.parquet")

    result = index.for_data_file(1, data_file)
    assert len(result) == 2
    assert all(d.content == DataFileContent.POSITION_DELETES for d in result)


def test_cannot_add_after_indexing() -> None:
    group = PositionDeletes()
    group.add(_create_positional_delete(sequence_number=1).data_file, 1)

    group.filter_by_seq(0)

    with pytest.raises(ValueError, match="Cannot add files after indexing"):
        group.add(_create_positional_delete(sequence_number=2).data_file, 2)


def test_record_equality_for_partition_lookup() -> None:
    index = DeleteFileIndex()

    partition_a = Record(1, "foo")
    partition_b = Record(1, "foo")
    partition_c = Record(1, "bar")

    assert partition_a == partition_b
    assert partition_a != partition_c

    index.add_delete_file(_create_partition_delete(sequence_number=2, spec_id=0, partition=partition_a), partition_a)

    data_file = _create_data_file()

    assert len(index.for_data_file(1, data_file, partition_b)) == 1
    assert len(index.for_data_file(1, data_file, partition_c)) == 0


def test_equality_delete_sequence_number_filtering() -> None:
    index = DeleteFileIndex()

    # Equality delete with sequence number 2
    index.add_delete_file(_create_equality_delete(sequence_number=2))

    data_file = _create_data_file()

    # Data file with sequence number 1 should be affected by equality delete with sequence number 2
    assert len(index.for_data_file(1, data_file)) == 1

    # Data file with sequence number 2 should NOT be affected by equality delete with sequence number 2
    # Equality deletes apply only to data files added in strictly earlier snapshots (seq - 1)
    assert len(index.for_data_file(2, data_file)) == 0

    # Data file with sequence number 3 should NOT be affected
    assert len(index.for_data_file(3, data_file)) == 0


def test_global_equality_deletes() -> None:
    index = DeleteFileIndex()

    # Global equality delete (unpartitioned)
    index.add_delete_file(_create_equality_delete(sequence_number=10))

    partition_1 = Record(1)
    partition_2 = Record(2)

    # Partitioned equality delete for partition 1
    index.add_delete_file(_create_equality_delete(sequence_number=20), partition_1)

    file_1 = _create_data_file(file_path="s3://bucket/file_1.parquet")
    file_2 = _create_data_file(file_path="s3://bucket/file_2.parquet")

    # Partition 1 should have 2 equality deletes (1 global, 1 partitioned)
    assert len(index.for_data_file(1, file_1, partition_1)) == 2
    # Partition 2 should have 1 equality delete (1 global)
    assert len(index.for_data_file(1, file_2, partition_2)) == 1


def test_equality_delete_metrics_filtering() -> None:
    schema = Schema(NestedField(1, "id", IntegerType(), required=True))
    index = DeleteFileIndex(schema=schema)

    # Equality delete for rows where id is between 10 and 20
    index.add_delete_file(
        _create_equality_delete(
            sequence_number=100,
            lower_bounds={1: to_bytes(IntegerType(), 10)},
            upper_bounds={1: to_bytes(IntegerType(), 20)},
        )
    )

    # Data file with id between 0 and 5 (no overlap)
    file_no_overlap = _create_data_file(
        "s3://bucket/no_overlap.parquet",
        lower_bounds={1: to_bytes(IntegerType(), 0)},
        upper_bounds={1: to_bytes(IntegerType(), 5)},
    )
    assert len(index.for_data_file(1, file_no_overlap)) == 0

    # Data file with id between 15 and 25 (overlap)
    file_overlap = _create_data_file(
        "s3://bucket/overlap.parquet",
        lower_bounds={1: to_bytes(IntegerType(), 15)},
        upper_bounds={1: to_bytes(IntegerType(), 25)},
    )
    assert len(index.for_data_file(1, file_overlap)) == 1

    # Data file with id between 25 and 30 (no overlap)
    file_no_overlap_2 = _create_data_file(
        "s3://bucket/no_overlap_2.parquet",
        lower_bounds={1: to_bytes(IntegerType(), 25)},
        upper_bounds={1: to_bytes(IntegerType(), 30)},
    )
    assert len(index.for_data_file(1, file_no_overlap_2)) == 0
