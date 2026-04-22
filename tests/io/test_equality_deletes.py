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
import os
import tempfile

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from pyiceberg.catalog import Catalog
from pyiceberg.expressions import AlwaysTrue
from pyiceberg.io.pyarrow import ArrowScan, PyArrowFileIO, _task_to_record_batches
from pyiceberg.manifest import DataFile, DataFileContent, FileFormat
from pyiceberg.partitioning import PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table import FileScanTask, TableProperties
from pyiceberg.table.metadata import TableMetadataV2
from pyiceberg.typedef import Record
from pyiceberg.types import IntegerType, NestedField, StringType


@pytest.fixture
def table_schema() -> Schema:
    return Schema(
        NestedField(1, "id", IntegerType()),
        NestedField(2, "data", StringType()),
    )


def test_task_to_record_batches_with_equality_deletes(table_schema: Schema) -> None:
    with tempfile.TemporaryDirectory() as temp_dir:
        # 1. Create data file
        data_file_path = os.path.join(temp_dir, "data.parquet")
        data_table = pa.Table.from_arrays([pa.array([1, 2, 3]), pa.array(["a", "b", "c"])], names=["id", "data"])
        # Add field IDs to schema metadata
        data_table = data_table.cast(
            pa.schema(
                [
                    pa.field("id", pa.int32(), metadata={b"PARQUET:field_id": b"1"}),
                    pa.field("data", pa.string(), metadata={b"PARQUET:field_id": b"2"}),
                ]
            )
        )
        pq.write_table(data_table, data_file_path)

        # 2. Create equality delete file (deleting id=2)
        delete_file_path = os.path.join(temp_dir, "delete.parquet")
        delete_table = pa.Table.from_arrays([pa.array([2], type=pa.int32())], names=["id"])
        delete_table = delete_table.cast(
            pa.schema(
                [
                    pa.field("id", pa.int32(), metadata={b"PARQUET:field_id": b"1"}),
                ]
            )
        )
        pq.write_table(delete_table, delete_file_path)

        # 3. Set up Iceberg metadata
        io = PyArrowFileIO()
        data_file = DataFile.from_args(
            content=DataFileContent.DATA,
            file_path=data_file_path,
            file_format=FileFormat.PARQUET,
            partition=Record(),
            record_count=3,
            file_size_in_bytes=os.path.getsize(data_file_path),
        )
        data_file.spec_id = 0
        delete_file = DataFile.from_args(
            content=DataFileContent.EQUALITY_DELETES,
            file_path=delete_file_path,
            file_format=FileFormat.PARQUET,
            partition=Record(),
            record_count=1,
            file_size_in_bytes=os.path.getsize(delete_file_path),
            equality_ids=[1],
        )
        delete_file.spec_id = 0

        task = FileScanTask(
            data_file=data_file,
            delete_files={delete_file},
        )

        # 4. Run _task_to_record_batches
        # We need to pass the equality delete table already loaded
        eq_deletes = [({1}, delete_table)]

        batches = list(
            _task_to_record_batches(
                io=io,
                task=task,
                bound_row_filter=AlwaysTrue(),
                projected_schema=table_schema,
                table_schema=table_schema,
                projected_field_ids={1, 2},
                positional_deletes=None,
                equality_deletes=eq_deletes,
                case_sensitive=True,
            )
        )

        # 5. Verify results
        result_table = pa.Table.from_batches(batches)
        assert result_table.to_pydict() == {"id": [1, 3], "data": ["a", "c"]}


def test_task_to_record_batches_with_multiple_equality_deletes(table_schema: Schema) -> None:
    with tempfile.TemporaryDirectory() as temp_dir:
        # 1. Create data file
        data_file_path = os.path.join(temp_dir, "data.parquet")
        data_table = pa.Table.from_arrays([pa.array([1, 2, 3, 4]), pa.array(["a", "b", "c", "d"])], names=["id", "data"])
        data_table = data_table.cast(
            pa.schema(
                [
                    pa.field("id", pa.int32(), metadata={b"PARQUET:field_id": b"1"}),
                    pa.field("data", pa.string(), metadata={b"PARQUET:field_id": b"2"}),
                ]
            )
        )
        pq.write_table(data_table, data_file_path)

        # 2. Create equality delete file 1 (deleting id=2)
        delete_file_path1 = os.path.join(temp_dir, "delete1.parquet")
        delete_table1 = pa.Table.from_arrays([pa.array([2], type=pa.int32())], names=["id"])
        delete_table1 = delete_table1.cast(
            pa.schema(
                [
                    pa.field("id", pa.int32(), metadata={b"PARQUET:field_id": b"1"}),
                ]
            )
        )
        pq.write_table(delete_table1, delete_file_path1)

        # 3. Create equality delete file 2 (deleting data='c')
        delete_file_path2 = os.path.join(temp_dir, "delete2.parquet")
        delete_table2 = pa.Table.from_arrays([pa.array(["c"])], names=["data"])
        delete_table2 = delete_table2.cast(
            pa.schema(
                [
                    pa.field("data", pa.string(), metadata={b"PARQUET:field_id": b"2"}),
                ]
            )
        )
        pq.write_table(delete_table2, delete_file_path2)

        io = PyArrowFileIO()
        data_file = DataFile.from_args(
            content=DataFileContent.DATA,
            file_path=data_file_path,
            file_format=FileFormat.PARQUET,
            partition=Record(),
            record_count=4,
            file_size_in_bytes=os.path.getsize(data_file_path),
        )
        data_file.spec_id = 0

        task = FileScanTask(data_file=data_file, delete_files=set())

        eq_deletes = [
            ({1}, delete_table1),
            ({2}, delete_table2),
        ]

        batches = list(
            _task_to_record_batches(
                io=io,
                task=task,
                bound_row_filter=AlwaysTrue(),
                projected_schema=table_schema,
                table_schema=table_schema,
                projected_field_ids={1, 2},
                positional_deletes=None,
                equality_deletes=eq_deletes,
                case_sensitive=True,
            )
        )

        # 5. Verify results
        result_table = pa.Table.from_batches(batches)
        assert result_table.to_pydict() == {"id": [1, 4], "data": ["a", "d"]}


def test_arrow_scan_with_equality_deletes(table_schema: Schema) -> None:
    with tempfile.TemporaryDirectory() as temp_dir:
        # 1. Create data file
        data_file_path = os.path.join(temp_dir, "data.parquet")
        data_table = pa.Table.from_arrays([pa.array([1, 2, 3], type=pa.int32()), pa.array(["a", "b", "c"])], names=["id", "data"])
        data_table = data_table.cast(
            pa.schema(
                [
                    pa.field("id", pa.int32(), metadata={b"PARQUET:field_id": b"1"}),
                    pa.field("data", pa.string(), metadata={b"PARQUET:field_id": b"2"}),
                ]
            )
        )
        pq.write_table(data_table, data_file_path)

        # 2. Create equality delete file (deleting id=2)
        delete_file_path = os.path.join(temp_dir, "delete.parquet")
        delete_table = pa.Table.from_arrays([pa.array([2], type=pa.int32())], names=["id"])
        delete_table = delete_table.cast(
            pa.schema(
                [
                    pa.field("id", pa.int32(), metadata={b"PARQUET:field_id": b"1"}),
                ]
            )
        )
        pq.write_table(delete_table, delete_file_path)

        # 3. Set up Iceberg metadata
        io = PyArrowFileIO()
        data_file = DataFile.from_args(
            content=DataFileContent.DATA,
            file_path=data_file_path,
            file_format=FileFormat.PARQUET,
            partition=Record(),
            record_count=3,
            file_size_in_bytes=os.path.getsize(data_file_path),
        )
        data_file.spec_id = 0
        delete_file = DataFile.from_args(
            content=DataFileContent.EQUALITY_DELETES,
            file_path=delete_file_path,
            file_format=FileFormat.PARQUET,
            partition=Record(),
            record_count=1,
            file_size_in_bytes=os.path.getsize(delete_file_path),
            equality_ids=[1],
        )
        delete_file.spec_id = 0

        task = FileScanTask(
            data_file=data_file,
            delete_files={delete_file},
        )

        metadata = TableMetadataV2(
            location=temp_dir,
            table_uuid="fb072c92-a02b-11e9-ae9c-1bb7bc9eca94",
            last_column_id=2,
            format_version=2,
            current_schema_id=table_schema.schema_id,
            schemas=[table_schema],
            partition_specs=[PartitionSpec(spec_id=0)],
            default_spec_id=0,
            last_partition_id=1000,
            properties={},
            snapshots=[],
        )

        scan = ArrowScan(
            table_metadata=metadata,
            io=io,
            projected_schema=table_schema,
            row_filter=AlwaysTrue(),
            case_sensitive=True,
        )

        # 4. Run to_table
        result_table = scan.to_table(tasks=[task])

        # 5. Verify results
        assert result_table.to_pydict() == {"id": [1, 3], "data": ["a", "c"]}


def test_block_writing_equality_deletes(table_schema: Schema, catalog: Catalog) -> None:
    identifier = "default.test_block_writing_equality_deletes"

    # Create table with MoR delete mode
    catalog.create_table(
        identifier,
        schema=table_schema,
        properties={TableProperties.DELETE_MODE: TableProperties.DELETE_MODE_MERGE_ON_READ, "format-version": "2"},
    )

    tbl = catalog.load_table(identifier)

    df = pa.Table.from_pydict({"id": [1], "data": ["a"]})
    tbl.append(df)

    # Create a Equality Delete file to force writing it.
    delete_file = DataFile.from_args(
        content=DataFileContent.EQUALITY_DELETES,
        file_path="/tmp/delete.parquet",
        file_format=FileFormat.PARQUET,
        partition=Record(),
        record_count=1,
        file_size_in_bytes=100,
        equality_ids=[1],
    )
    delete_file.spec_id = 0

    with pytest.raises(NotImplementedError) as exc:
        with tbl.transaction() as tx:
            with tx.update_snapshot().fast_append() as append:
                append.append_data_file(delete_file)

    assert "PyIceberg does not support writing EQUALITY_DELETES" in str(exc.value)
