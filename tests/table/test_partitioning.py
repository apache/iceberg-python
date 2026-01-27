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
import datetime
from decimal import Decimal
from typing import Any
from uuid import UUID

import pytest

from pyiceberg.exceptions import ValidationError
from pyiceberg.partitioning import UNPARTITIONED_PARTITION_SPEC, PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import (
    BucketTransform,
    DayTransform,
    HourTransform,
    IdentityTransform,
    MonthTransform,
    TruncateTransform,
    YearTransform,
)
from pyiceberg.typedef import Record
from pyiceberg.types import (
    BinaryType,
    DateType,
    DecimalType,
    FixedType,
    IntegerType,
    LongType,
    NestedField,
    PrimitiveType,
    StringType,
    StructType,
    TimestampType,
    TimestamptzType,
    TimeType,
    UnknownType,
    UUIDType,
)


def test_partition_field_init() -> None:
    bucket_transform = BucketTransform(100)  # type: ignore
    partition_field = PartitionField(3, 1000, bucket_transform, "id")

    assert partition_field.source_id == 3
    assert partition_field.field_id == 1000
    assert partition_field.transform == bucket_transform
    assert partition_field.name == "id"
    assert partition_field == partition_field
    assert str(partition_field) == "1000: id: bucket[100](3)"
    assert (
        repr(partition_field)
        == "PartitionField(source_id=3, field_id=1000, transform=BucketTransform(num_buckets=100), name='id')"
    )


def test_unpartitioned_partition_spec_repr() -> None:
    assert repr(PartitionSpec()) == "PartitionSpec(spec_id=0)"


def test_partition_spec_init() -> None:
    bucket_transform: BucketTransform = BucketTransform(4)  # type: ignore

    id_field1 = PartitionField(3, 1001, bucket_transform, "id")
    partition_spec1 = PartitionSpec(id_field1)

    assert partition_spec1.spec_id == 0
    assert partition_spec1 == partition_spec1
    assert partition_spec1 != id_field1
    assert str(partition_spec1) == f"[\n  {str(id_field1)}\n]"
    assert not partition_spec1.is_unpartitioned()
    # only differ by PartitionField field_id
    id_field2 = PartitionField(3, 1002, bucket_transform, "id")
    partition_spec2 = PartitionSpec(id_field2)
    assert partition_spec1 != partition_spec2
    assert partition_spec1.compatible_with(partition_spec2)
    assert partition_spec1.fields_by_source_id(3) == [id_field1]
    # Does not exist
    assert partition_spec1.fields_by_source_id(1925) == []


def test_partition_compatible_with() -> None:
    bucket_transform: BucketTransform = BucketTransform(4)  # type: ignore
    field1 = PartitionField(3, 100, bucket_transform, "id")
    field2 = PartitionField(3, 102, bucket_transform, "id")
    lhs = PartitionSpec(
        field1,
    )
    rhs = PartitionSpec(field1, field2)
    assert not lhs.compatible_with(rhs)


def test_unpartitioned() -> None:
    assert len(UNPARTITIONED_PARTITION_SPEC.fields) == 0
    assert UNPARTITIONED_PARTITION_SPEC.is_unpartitioned()
    assert str(UNPARTITIONED_PARTITION_SPEC) == "[]"


def test_serialize_unpartitioned_spec() -> None:
    assert UNPARTITIONED_PARTITION_SPEC.model_dump_json() == """{"spec-id":0,"fields":[]}"""


def test_serialize_partition_spec() -> None:
    partitioned = PartitionSpec(
        PartitionField(source_id=1, field_id=1000, transform=TruncateTransform(width=19), name="str_truncate"),
        PartitionField(source_id=2, field_id=1001, transform=BucketTransform(num_buckets=25), name="int_bucket"),
        spec_id=3,
    )
    assert partitioned.model_dump_json() == (
        '{"spec-id":3,"fields":['
        '{"source-id":1,"field-id":1000,"transform":"truncate[19]","name":"str_truncate"},'
        '{"source-id":2,"field-id":1001,"transform":"bucket[25]","name":"int_bucket"}]}'
    )


def test_deserialize_unpartition_spec() -> None:
    json_partition_spec = """{"spec-id":0,"fields":[]}"""
    spec = PartitionSpec.model_validate_json(json_partition_spec)

    assert spec == PartitionSpec(spec_id=0)


def test_deserialize_partition_spec() -> None:
    json_partition_spec = (
        '{"spec-id": 3, "fields": ['
        '{"source-id": 1, "field-id": 1000, "transform": "truncate[19]", "name": "str_truncate"}, '
        '{"source-id": 2, "field-id": 1001, "transform": "bucket[25]", "name": "int_bucket"}]}'
    )

    spec = PartitionSpec.model_validate_json(json_partition_spec)

    assert spec == PartitionSpec(
        PartitionField(source_id=1, field_id=1000, transform=TruncateTransform(width=19), name="str_truncate"),
        PartitionField(source_id=2, field_id=1001, transform=BucketTransform(num_buckets=25), name="int_bucket"),
        spec_id=3,
    )


def test_partition_spec_to_path() -> None:
    schema = Schema(
        NestedField(field_id=1, name="str", field_type=StringType(), required=False),
        NestedField(field_id=2, name="other_str", field_type=StringType(), required=False),
        NestedField(field_id=3, name="int", field_type=IntegerType(), required=True),
    )

    spec = PartitionSpec(
        PartitionField(source_id=1, field_id=1000, transform=TruncateTransform(width=19), name="my#str%bucket"),
        PartitionField(source_id=2, field_id=1001, transform=IdentityTransform(), name="other str+bucket"),
        PartitionField(source_id=3, field_id=1002, transform=BucketTransform(num_buckets=25), name="my!int:bucket"),
        spec_id=3,
    )

    record = Record("my+str", "( )", 10)

    # Both partition field names and values should be URL encoded, with spaces mapping to plus signs, to match the Java
    # behaviour: https://github.com/apache/iceberg/blob/ca3db931b0f024f0412084751ac85dd4ef2da7e7/api/src/main/java/org/apache/iceberg/PartitionSpec.java#L198-L204
    assert spec.partition_to_path(record, schema) == "my%23str%25bucket=my%2Bstr/other+str%2Bbucket=%28+%29/my%21int%3Abucket=10"


def test_partition_spec_to_path_dropped_source_id() -> None:
    schema = Schema(
        NestedField(field_id=1, name="str", field_type=StringType(), required=False),
        NestedField(field_id=2, name="other_str", field_type=StringType(), required=False),
        NestedField(field_id=3, name="int", field_type=IntegerType(), required=True),
    )

    spec = PartitionSpec(
        PartitionField(source_id=1, field_id=1000, transform=TruncateTransform(width=19), name="my#str%bucket"),
        PartitionField(source_id=2, field_id=1001, transform=IdentityTransform(), name="other str+bucket"),
        # Point partition field to missing source id
        PartitionField(source_id=4, field_id=1002, transform=BucketTransform(num_buckets=25), name="my!int:bucket"),
        spec_id=3,
    )

    record = Record("my+str", "( )", 10)

    # Both partition field names and values should be URL encoded, with spaces mapping to plus signs, to match the Java
    # behaviour: https://github.com/apache/iceberg/blob/ca3db931b0f024f0412084751ac85dd4ef2da7e7/api/src/main/java/org/apache/iceberg/PartitionSpec.java#L198-L204
    assert spec.partition_to_path(record, schema) == "my%23str%25bucket=my%2Bstr/other+str%2Bbucket=%28+%29/my%21int%3Abucket=10"


def test_partition_type(table_schema_simple: Schema) -> None:
    spec = PartitionSpec(
        PartitionField(source_id=1, field_id=1000, transform=TruncateTransform(width=19), name="str_truncate"),
        PartitionField(source_id=2, field_id=1001, transform=BucketTransform(num_buckets=25), name="int_bucket"),
        spec_id=3,
    )

    assert spec.partition_type(table_schema_simple) == StructType(
        NestedField(field_id=1000, name="str_truncate", field_type=StringType(), required=False),
        NestedField(field_id=1001, name="int_bucket", field_type=IntegerType(), required=True),
    )


def test_partition_type_missing_source_field(table_schema_simple: Schema) -> None:
    spec = PartitionSpec(
        PartitionField(source_id=1, field_id=1000, transform=TruncateTransform(width=19), name="str_truncate"),
        PartitionField(source_id=10, field_id=1001, transform=BucketTransform(num_buckets=25), name="int_bucket"),
        spec_id=3,
    )

    assert spec.partition_type(table_schema_simple) == StructType(
        NestedField(field_id=1000, name="str_truncate", field_type=StringType(), required=False),
        NestedField(field_id=1001, name="int_bucket", field_type=UnknownType(), required=False),
    )


@pytest.mark.parametrize(
    "source_type, value",
    [
        (IntegerType(), 22),
        (LongType(), 22),
        (DecimalType(5, 9), Decimal(19.25)),
        (DateType(), datetime.date(1925, 5, 22)),
        (TimeType(), datetime.time(19, 25, 00)),
        (TimestampType(), datetime.datetime(2022, 5, 1, 22, 1, 1)),
        (TimestamptzType(), datetime.datetime(2022, 5, 1, 22, 1, 1, tzinfo=datetime.timezone.utc)),
        (StringType(), "abc"),
        (UUIDType(), UUID("12345678-1234-5678-1234-567812345678").bytes),
        (FixedType(5), 'b"\x8e\xd1\x87\x01"'),
        (BinaryType(), b"\x8e\xd1\x87\x01"),
    ],
)
def test_transform_consistency_with_pyarrow_transform(source_type: PrimitiveType, value: Any) -> None:
    import pyarrow as pa

    all_transforms = [  # type: ignore
        IdentityTransform(),
        BucketTransform(10),
        TruncateTransform(10),
        YearTransform(),
        MonthTransform(),
        DayTransform(),
        HourTransform(),
    ]
    for t in all_transforms:
        if t.can_transform(source_type):
            assert t.transform(source_type)(value) == t.pyarrow_transform(source_type)(pa.array([value])).to_pylist()[0]


def test_deserialize_partition_field_v2() -> None:
    json_partition_spec = """{"source-id": 1, "field-id": 1000, "transform": "truncate[19]", "name": "str_truncate"}"""

    field = PartitionField.model_validate_json(json_partition_spec)
    assert field == PartitionField(source_id=1, field_id=1000, transform=TruncateTransform(width=19), name="str_truncate")


def test_deserialize_partition_field_v3() -> None:
    json_partition_spec = """{"source-ids": [1], "field-id": 1000, "transform": "truncate[19]", "name": "str_truncate"}"""

    field = PartitionField.model_validate_json(json_partition_spec)
    assert field == PartitionField(source_id=1, field_id=1000, transform=TruncateTransform(width=19), name="str_truncate")


def test_incompatible_source_column_not_found() -> None:
    schema = Schema(NestedField(1, "foo", IntegerType()), NestedField(2, "bar", IntegerType()))

    spec = PartitionSpec(PartitionField(3, 1000, IdentityTransform(), "some_partition"))

    with pytest.raises(ValidationError) as exc:
        spec.check_compatible(schema)

    assert "Cannot find source column for partition field: 1000: some_partition: identity(3)" in str(exc.value)


def test_incompatible_non_primitive_type() -> None:
    schema = Schema(NestedField(1, "foo", StructType()), NestedField(2, "bar", IntegerType()))

    spec = PartitionSpec(PartitionField(1, 1000, IdentityTransform(), "some_partition"))

    with pytest.raises(ValidationError) as exc:
        spec.check_compatible(schema)

    assert "Cannot partition by non-primitive source field: 1: foo: optional struct<>" in str(exc.value)


def test_incompatible_transform_source_type() -> None:
    schema = Schema(NestedField(1, "foo", IntegerType()), NestedField(2, "bar", IntegerType()))

    spec = PartitionSpec(PartitionField(1, 1000, YearTransform(), "some_partition"))

    with pytest.raises(ValidationError) as exc:
        spec.check_compatible(schema)

    assert "Invalid source field foo with type int for transform: year" in str(exc.value)
