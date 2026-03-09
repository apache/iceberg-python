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

from pyiceberg.manifest import DataFile, DataFileContent, FileFormat, ManifestEntry, ManifestEntryStatus
from pyiceberg.table.delete_file_index import PATH_FIELD_ID, DeleteFileIndex, PositionDeletes
from pyiceberg.typedef import Record


def _create_data_file(file_path: str = "s3://bucket/data.parquet", spec_id: int = 0) -> DataFile:
    data_file = DataFile.from_args(
        content=DataFileContent.DATA,
        file_path=file_path,
        file_format=FileFormat.PARQUET,
        partition=Record(),
        record_count=100,
        file_size_in_bytes=1000,
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
