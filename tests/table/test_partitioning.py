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
from pyiceberg.partitioning import (
    UNPARTITIONED_PARTITION_SPEC,
    PartitionField,
    PartitionSpec,
    assign_fresh_partition_spec_ids_for_replace,
)
from pyiceberg.schema import Schema
from pyiceberg.transforms import (
    BucketTransform,
    DayTransform,
    HourTransform,
    IdentityTransform,
    MonthTransform,
    TruncateTransform,
    VoidTransform,
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


def test_deserialize_partition_field_empty_source_ids_rejected() -> None:
    json_partition_spec = """{"source-ids": [], "field-id": 1000, "transform": "identity", "name": "x"}"""
    with pytest.raises(Exception, match="Empty source-ids is not allowed"):
        PartitionField.model_validate_json(json_partition_spec)


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


_REPLACE_SCHEMA_FOR_PARTITION = Schema(
    NestedField(field_id=1, name="id", field_type=IntegerType(), required=False),
    NestedField(field_id=2, name="data", field_type=StringType(), required=False),
)


@pytest.mark.parametrize(
    "new_spec, existing_specs, last_partition_id, expected_field_id, expected_last_partition_id",
    [
        # Reuse-by-identity: same (source_id, IdentityTransform) already in an existing spec.
        pytest.param(
            PartitionSpec(PartitionField(source_id=1, field_id=999, transform=IdentityTransform(), name="id")),
            [PartitionSpec(PartitionField(source_id=1, field_id=1000, transform=IdentityTransform(), name="id"), spec_id=0)],
            1000,
            1000,
            1000,
            id="reuse-identity",
        ),
        # Reuse-by-(source,bucket): same source_id + same BucketTransform, even under a renamed field.
        pytest.param(
            PartitionSpec(PartitionField(source_id=1, field_id=999, transform=BucketTransform(8), name="id_bucket_renamed")),
            [
                PartitionSpec(
                    PartitionField(source_id=1, field_id=1042, transform=BucketTransform(8), name="id_bucket"), spec_id=0
                )
            ],
            1042,
            1042,
            1042,
            id="reuse-bucket-under-rename",
        ),
        # No match: fresh id above last_partition_id.
        pytest.param(
            PartitionSpec(PartitionField(source_id=1, field_id=999, transform=IdentityTransform(), name="id")),
            [PartitionSpec(spec_id=0)],
            999,
            1000,
            1000,
            id="new-field-above-last-partition-id",
        ),
    ],
)
def test_assign_fresh_partition_spec_ids_for_replace_v2(
    new_spec: PartitionSpec,
    existing_specs: list[PartitionSpec],
    last_partition_id: int,
    expected_field_id: int,
    expected_last_partition_id: int,
) -> None:
    fresh_spec, new_last_pid = assign_fresh_partition_spec_ids_for_replace(
        new_spec, _REPLACE_SCHEMA_FOR_PARTITION, _REPLACE_SCHEMA_FOR_PARTITION, existing_specs, last_partition_id
    )
    assert fresh_spec.fields[0].field_id == expected_field_id
    assert new_last_pid == expected_last_partition_id


def test_assign_fresh_partition_spec_ids_for_replace_v1_carries_forward_as_void() -> None:
    """v1 specs are append-only: a field absent from the new spec is carried forward as void."""
    current_spec = PartitionSpec(PartitionField(source_id=1, field_id=1000, transform=IdentityTransform(), name="id"), spec_id=0)
    # New spec drops "id" entirely, partitioned by "data" instead.
    new_spec = PartitionSpec(PartitionField(source_id=2, field_id=999, transform=IdentityTransform(), name="data"))
    fresh_spec, new_last_pid = assign_fresh_partition_spec_ids_for_replace(
        new_spec,
        _REPLACE_SCHEMA_FOR_PARTITION,
        _REPLACE_SCHEMA_FOR_PARTITION,
        existing_specs=[current_spec],
        last_partition_id=1000,
        format_version=1,
        current_spec=current_spec,
    )
    # Two fields: the carried-forward void at field_id=1000, and the new "data" field above it.
    fields_by_id = {f.field_id: f for f in fresh_spec.fields}
    assert isinstance(fields_by_id[1000].transform, VoidTransform)
    assert fields_by_id[1000].name == "id"
    assert fields_by_id[1001].name == "data"
    assert isinstance(fields_by_id[1001].transform, IdentityTransform)
    assert new_last_pid == 1001


def test_assign_fresh_partition_spec_ids_for_replace_v1_renames_void_on_name_collision() -> None:
    """When a void field's name collides with a new field's name, a unique suffix is added."""
    current_spec = PartitionSpec(
        PartitionField(source_id=1, field_id=1000, transform=IdentityTransform(), name="data"), spec_id=0
    )
    # New spec partitions "data" by a different transform — the OLD "data" must be voided
    # under a different name to avoid collision with the NEW "data" partition.
    new_spec = PartitionSpec(PartitionField(source_id=2, field_id=999, transform=IdentityTransform(), name="data"))
    fresh_spec, _ = assign_fresh_partition_spec_ids_for_replace(
        new_spec,
        _REPLACE_SCHEMA_FOR_PARTITION,
        _REPLACE_SCHEMA_FOR_PARTITION,
        existing_specs=[current_spec],
        last_partition_id=1000,
        format_version=1,
        current_spec=current_spec,
    )
    void_field = next(f for f in fresh_spec.fields if isinstance(f.transform, VoidTransform))
    assert void_field.name != "data", "void name must not collide with active partition name"
    assert void_field.name == "data_1000"


def test_assign_fresh_partition_spec_ids_for_replace_v1_keeps_field_preserves_id() -> None:
    """v1 carry-forward: when a current-spec field is also in the new spec, its field_id is preserved."""
    schema = Schema(
        NestedField(field_id=1, name="id", field_type=IntegerType(), required=False),
        NestedField(field_id=2, name="data", field_type=StringType(), required=False),
    )
    current_spec = PartitionSpec(PartitionField(source_id=1, field_id=1000, transform=IdentityTransform(), name="id"), spec_id=0)
    # New spec keeps the same (source, transform) on "id" — should reuse field_id=1000, no void emitted.
    new_spec = PartitionSpec(PartitionField(source_id=1, field_id=999, transform=IdentityTransform(), name="id"))
    fresh_spec, new_last_pid = assign_fresh_partition_spec_ids_for_replace(
        new_spec,
        schema,
        schema,
        existing_specs=[current_spec],
        last_partition_id=1000,
        format_version=1,
        current_spec=current_spec,
    )
    assert [f.field_id for f in fresh_spec.fields] == [1000]
    assert fresh_spec.fields[0].name == "id"
    assert isinstance(fresh_spec.fields[0].transform, IdentityTransform)
    assert not any(isinstance(f.transform, VoidTransform) for f in fresh_spec.fields)
    assert new_last_pid == 1000


def test_assign_fresh_partition_spec_ids_for_replace_v1_void_name_uses_multi_suffix_loop() -> None:
    """When `name` and `name_<field_id>` are both already used, append `_2`, `_3`, ... until unique."""
    # Three columns, one role each: source for the current (about-to-be-voided) partition,
    # source for the new partition that collides on the void's preferred name, and source for
    # the new partition that collides on the void's first fallback name.
    schema = Schema(
        NestedField(field_id=1, name="current_source", field_type=IntegerType(), required=False),
        NestedField(field_id=2, name="collide_on_name", field_type=IntegerType(), required=False),
        NestedField(field_id=3, name="collide_on_fallback", field_type=IntegerType(), required=False),
    )
    # Current v1 spec partitions source=1 by bucket(4) at field_id=1000, named "p" — for
    # non-identity transforms the partition NAME doesn't have to match the source column name.
    current_spec = PartitionSpec(PartitionField(source_id=1, field_id=1000, transform=BucketTransform(4), name="p"), spec_id=0)
    # New spec has two partition fields named "p" and "p_1000" — colliding with both the
    # void's preferred name and its first fallback. Both are on different sources, so they
    # do not match the current (source=1, bucket[4]) key and the current field becomes void.
    new_spec = PartitionSpec(
        PartitionField(source_id=2, field_id=997, transform=BucketTransform(4), name="p"),
        PartitionField(source_id=3, field_id=998, transform=BucketTransform(4), name="p_1000"),
    )
    fresh_spec, _ = assign_fresh_partition_spec_ids_for_replace(
        new_spec,
        schema,
        schema,
        existing_specs=[current_spec],
        last_partition_id=1000,
        format_version=1,
        current_spec=current_spec,
    )
    void_field = next(f for f in fresh_spec.fields if isinstance(f.transform, VoidTransform))
    assert void_field.name == "p_1000_2"


def test_assign_fresh_partition_spec_ids_for_replace_v2_prefers_highest_field_id_for_repeated_key() -> None:
    """v2: when the same (source_id, transform) appears across multiple specs, the highest field_id wins."""
    # Two historical specs both partition by (source_id=1, IdentityTransform), with different field_ids.
    existing_specs = [
        PartitionSpec(PartitionField(source_id=1, field_id=1000, transform=IdentityTransform(), name="id"), spec_id=0),
        PartitionSpec(PartitionField(source_id=1, field_id=1002, transform=IdentityTransform(), name="id_v2"), spec_id=1),
    ]
    # New spec uses the same (source, transform) — should reuse the highest historical field_id (1002).
    new_spec = PartitionSpec(PartitionField(source_id=1, field_id=999, transform=IdentityTransform(), name="id"))
    fresh_spec, new_last_pid = assign_fresh_partition_spec_ids_for_replace(
        new_spec, _REPLACE_SCHEMA_FOR_PARTITION, _REPLACE_SCHEMA_FOR_PARTITION, existing_specs, last_partition_id=1002
    )
    assert fresh_spec.fields[0].field_id == 1002
    assert new_last_pid == 1002
