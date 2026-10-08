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

from datetime import datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from pyiceberg.io.pyarrow import PyArrowFileIO, _dataframe_to_data_files, _determine_partitions
from pyiceberg.partitioning import PartitionField, PartitionFieldValue, PartitionKey, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table.metadata import TableMetadataV1
from pyiceberg.transforms import BucketTransform, IdentityTransform, MonthTransform, VoidTransform, YearTransform
from pyiceberg.typedef import Record
from pyiceberg.types import LongType, NestedField, StringType, TimestampType


@pytest.mark.parametrize("epoch", ["epoch1", None])
def test_partition_key_with_retired_identity_fields(epoch: str | None) -> None:
    schema = Schema(
        NestedField(1, "sampling_strategy", StringType()),
        NestedField(2, "dt", StringType()),
        NestedField(3, "LABEL/user_id", LongType()),
        NestedField(3569, "longseq_serving_epoch_str", StringType()),
    )
    spec = PartitionSpec(
        PartitionField(1, 1000, IdentityTransform(), "sampling_strategy"),
        PartitionField(2, 1001, IdentityTransform(), "dt"),
        PartitionField(3, 1002, BucketTransform(65536), "LABEL/user_id_bucket"),
        PartitionField(3569, 1003, VoidTransform(), "longseq_serving_epoch_str_1003"),
        PartitionField(3569, 1004, VoidTransform(), "longseq_serving_epoch_str_1004"),
        PartitionField(3569, 1005, IdentityTransform(), "longseq_serving_epoch_str"),
        spec_id=5,
    )
    values = ["strategy_ctm", "2026-10-06", 42, None, None, epoch]
    key = PartitionKey(
        field_values=[PartitionFieldValue(field, value) for field, value in zip(spec.fields, values, strict=True)],
        partition_spec=spec,
        schema=schema,
    )

    assert key.partition == Record(*values)
    assert key.to_path() == (
        "sampling_strategy=strategy_ctm/dt=2026-10-06/LABEL%2Fuser_id_bucket=42/"
        "longseq_serving_epoch_str_1003=null/longseq_serving_epoch_str_1004=null/"
        f"longseq_serving_epoch_str={epoch if epoch is not None else 'null'}"
    )
    assert key.partition_spec == spec
    assert [field.field_id for field in spec.fields] == [1000, 1001, 1002, 1003, 1004, 1005]


def test_partition_key_with_multiple_active_transforms() -> None:
    schema = Schema(NestedField(1, "created_at", TimestampType()))
    spec = PartitionSpec(
        PartitionField(1, 1000, YearTransform(), "created_at_year"),
        PartitionField(1, 1001, MonthTransform(), "created_at_month"),
    )
    key = PartitionKey(
        field_values=[PartitionFieldValue(spec.fields[0], 56), PartitionFieldValue(spec.fields[1], 681)],
        partition_spec=spec,
        schema=schema,
    )

    assert key.partition == Record(56, 681)
    assert key.to_path() == "created_at_year=2026/created_at_month=2026-10"


def test_determine_partitions_retains_void_slots_and_epoch_groups() -> None:
    schema = Schema(NestedField(3569, "longseq_serving_epoch_str", StringType()))
    spec = PartitionSpec(
        PartitionField(3569, 1003, VoidTransform(), "longseq_serving_epoch_str_1003"),
        PartitionField(3569, 1004, VoidTransform(), "longseq_serving_epoch_str_1004"),
        PartitionField(3569, 1005, IdentityTransform(), "longseq_serving_epoch_str"),
        spec_id=5,
    )
    rows = pa.table({"longseq_serving_epoch_str": ["epoch1", "epoch2", "epoch1"]})

    partitions = list(_determine_partitions(spec, schema, rows))

    assert len(partitions) == 2
    assert {tuple(partition.partition_key.partition[i] for i in range(len(spec.fields))) for partition in partitions} == {
        (None, None, "epoch1"),
        (None, None, "epoch2"),
    }
    for partition in partitions:
        epoch = partition.partition_key.partition[2]
        assert set(partition.arrow_table_partition.column("longseq_serving_epoch_str").to_pylist()) == {epoch}


def test_determine_partitions_with_multiple_active_transforms() -> None:
    schema = Schema(NestedField(1, "created_at", TimestampType()))
    spec = PartitionSpec(
        PartitionField(1, 1000, YearTransform(), "created_at_year"),
        PartitionField(1, 1001, MonthTransform(), "created_at_month"),
    )
    rows = pa.table({"created_at": [datetime(2026, 10, 1), datetime(2026, 11, 1)]})

    partitions = list(_determine_partitions(spec, schema, rows))

    assert {tuple(partition.partition_key.partition[i] for i in range(len(spec.fields))) for partition in partitions} == {
        (56, 681),
        (56, 682),
    }
    assert {partition.partition_key.to_path() for partition in partitions} == {
        "created_at_year=2026/created_at_month=2026-10",
        "created_at_year=2026/created_at_month=2026-11",
    }


def test_write_parquet_with_retired_identity_fields(tmp_path: Path) -> None:
    schema = Schema(NestedField(3569, "longseq_serving_epoch_str", StringType()))
    spec = PartitionSpec(
        PartitionField(3569, 1003, VoidTransform(), "longseq_serving_epoch_str_1003"),
        PartitionField(3569, 1004, VoidTransform(), "longseq_serving_epoch_str_1004"),
        PartitionField(3569, 1005, IdentityTransform(), "longseq_serving_epoch_str"),
        spec_id=5,
    )
    metadata = TableMetadataV1.model_validate(
        {
            "location": str(tmp_path),
            "last-column-id": 3569,
            "schema": schema.model_dump(mode="json", by_alias=True),
            "schemas": [schema.model_dump(mode="json", by_alias=True)],
            "partition-specs": [spec.model_dump(mode="json", by_alias=True)],
            "default-spec-id": 5,
            "last-partition-id": 1005,
        }
    )
    rows = pa.table({"longseq_serving_epoch_str": ["epoch1", "epoch2", "epoch1"]})

    files = list(_dataframe_to_data_files(metadata, rows, PyArrowFileIO()))

    assert len(files) == 2
    assert sum(data_file.record_count for data_file in files) == 3
    assert {tuple(data_file.partition[i] for i in range(len(spec.fields))) for data_file in files} == {
        (None, None, "epoch1"),
        (None, None, "epoch2"),
    }
    for data_file in files:
        assert set(pq.ParquetFile(data_file.file_path).read().column("longseq_serving_epoch_str").to_pylist()) == {
            data_file.partition[2]
        }
    assert metadata.format_version == 1
    assert metadata.spec() == spec
