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
# pylint: disable=protected-access,unused-argument,redefined-outer-name
import re
from typing import Any

import pyarrow as pa
import pytest

from pyiceberg.expressions import (
    And,
    BoundEqualTo,
    BoundGreaterThan,
    BoundIsNaN,
    BoundIsNull,
    BoundReference,
    Not,
    Or,
)
from pyiceberg.expressions.literals import literal
from pyiceberg.io.pyarrow import (
    _ConvertToArrowSchema,
    _ConvertToIceberg,
    _ConvertToIcebergWithoutIDs,
    _expression_to_complementary_pyarrow,
    _HasIds,
    _NullNaNUnmentionedTermsCollector,
    _pyarrow_schema_ensure_large_types,
    pyarrow_to_schema,
    schema_to_pyarrow,
    visit_pyarrow,
)
from pyiceberg.schema import Accessor, Schema, visit
from pyiceberg.table.name_mapping import MappedField, NameMapping
from pyiceberg.types import (
    BinaryType,
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    FixedType,
    FloatType,
    IcebergType,
    IntegerType,
    ListType,
    LongType,
    MapType,
    NestedField,
    StringType,
    StructType,
    TimestampType,
    TimestamptzType,
    TimeType,
)


def test_pyarrow_binary_to_iceberg() -> None:
    length = 23
    pyarrow_type = pa.binary(length)
    converted_iceberg_type = visit_pyarrow(pyarrow_type, _ConvertToIceberg())
    assert converted_iceberg_type == FixedType(length)
    assert visit(converted_iceberg_type, _ConvertToArrowSchema()) == pyarrow_type


def test_pyarrow_decimal128_to_iceberg() -> None:
    precision = 26
    scale = 20
    pyarrow_type = pa.decimal128(precision, scale)
    converted_iceberg_type = visit_pyarrow(pyarrow_type, _ConvertToIceberg())
    assert converted_iceberg_type == DecimalType(precision, scale)
    assert visit(converted_iceberg_type, _ConvertToArrowSchema()) == pyarrow_type


def test_pyarrow_decimal256_to_iceberg() -> None:
    precision = 26
    scale = 20
    pyarrow_type = pa.decimal256(precision, scale)
    with pytest.raises(TypeError, match=re.escape("Unsupported type: decimal256(26, 20)")):
        visit_pyarrow(pyarrow_type, _ConvertToIceberg())


def test_pyarrow_boolean_to_iceberg() -> None:
    pyarrow_type = pa.bool_()
    converted_iceberg_type = visit_pyarrow(pyarrow_type, _ConvertToIceberg())
    assert converted_iceberg_type == BooleanType()
    assert visit(converted_iceberg_type, _ConvertToArrowSchema()) == pyarrow_type


def test_pyarrow_int8_to_iceberg() -> None:
    pyarrow_type = pa.int8()
    converted_iceberg_type = visit_pyarrow(pyarrow_type, _ConvertToIceberg())
    assert converted_iceberg_type == IntegerType()


def test_pyarrow_int16_to_iceberg() -> None:
    pyarrow_type = pa.int16()
    converted_iceberg_type = visit_pyarrow(pyarrow_type, _ConvertToIceberg())
    assert converted_iceberg_type == IntegerType()


def test_pyarrow_int32_to_iceberg() -> None:
    pyarrow_type = pa.int32()
    converted_iceberg_type = visit_pyarrow(pyarrow_type, _ConvertToIceberg())
    assert converted_iceberg_type == IntegerType()
    assert visit(converted_iceberg_type, _ConvertToArrowSchema()) == pyarrow_type


def test_pyarrow_int64_to_iceberg() -> None:
    pyarrow_type = pa.int64()
    converted_iceberg_type = visit_pyarrow(pyarrow_type, _ConvertToIceberg())
    assert converted_iceberg_type == LongType()
    assert visit(converted_iceberg_type, _ConvertToArrowSchema()) == pyarrow_type


def test_pyarrow_float32_to_iceberg() -> None:
    pyarrow_type = pa.float32()
    converted_iceberg_type = visit_pyarrow(pyarrow_type, _ConvertToIceberg())
    assert converted_iceberg_type == FloatType()
    assert visit(converted_iceberg_type, _ConvertToArrowSchema()) == pyarrow_type


def test_pyarrow_float64_to_iceberg() -> None:
    pyarrow_type = pa.float64()
    converted_iceberg_type = visit_pyarrow(pyarrow_type, _ConvertToIceberg())
    assert converted_iceberg_type == DoubleType()
    assert visit(converted_iceberg_type, _ConvertToArrowSchema()) == pyarrow_type


def test_pyarrow_date32_to_iceberg() -> None:
    pyarrow_type = pa.date32()
    converted_iceberg_type = visit_pyarrow(pyarrow_type, _ConvertToIceberg())
    assert converted_iceberg_type == DateType()
    assert visit(converted_iceberg_type, _ConvertToArrowSchema()) == pyarrow_type


def test_pyarrow_date64_to_iceberg() -> None:
    pyarrow_type = pa.date64()
    with pytest.raises(TypeError, match=re.escape("Unsupported type: date64")):
        visit_pyarrow(pyarrow_type, _ConvertToIceberg())


def test_pyarrow_time32_to_iceberg() -> None:
    pyarrow_type = pa.time32("ms")
    with pytest.raises(TypeError, match=re.escape("Unsupported type: time32[ms]")):
        visit_pyarrow(pyarrow_type, _ConvertToIceberg())
    pyarrow_type = pa.time32("s")
    with pytest.raises(TypeError, match=re.escape("Unsupported type: time32[s]")):
        visit_pyarrow(pyarrow_type, _ConvertToIceberg())


def test_pyarrow_time64_us_to_iceberg() -> None:
    pyarrow_type = pa.time64("us")
    converted_iceberg_type = visit_pyarrow(pyarrow_type, _ConvertToIceberg())
    assert converted_iceberg_type == TimeType()
    assert visit(converted_iceberg_type, _ConvertToArrowSchema()) == pyarrow_type


def test_pyarrow_time64_ns_to_iceberg() -> None:
    pyarrow_type = pa.time64("ns")
    with pytest.raises(TypeError, match=re.escape("Unsupported type: time64[ns]")):
        visit_pyarrow(pyarrow_type, _ConvertToIceberg())


@pytest.mark.parametrize("precision", ["s", "ms", "us", "ns"])
def test_pyarrow_timestamp_to_iceberg(precision: str) -> None:
    pyarrow_type = pa.timestamp(unit=precision)
    converted_iceberg_type = visit_pyarrow(pyarrow_type, _ConvertToIceberg(downcast_ns_timestamp_to_us=True))
    assert converted_iceberg_type == TimestampType()
    # all timestamp types are converted to 'us' precision
    assert visit(converted_iceberg_type, _ConvertToArrowSchema()) == pa.timestamp(unit="us")


def test_pyarrow_timestamp_invalid_units() -> None:
    pyarrow_type = pa.timestamp(unit="ns")
    with pytest.raises(
        TypeError,
        match=re.escape(
            "Iceberg does not yet support 'ns' timestamp precision. Use 'downcast-ns-timestamp-to-us-on-write' configuration property to automatically downcast 'ns' to 'us' on write."
        ),
    ):
        visit_pyarrow(pyarrow_type, _ConvertToIceberg())


def test_pyarrow_timestamp_tz_to_iceberg() -> None:
    pyarrow_type = pa.timestamp(unit="us", tz="UTC")
    pyarrow_type_zero_offset = pa.timestamp(unit="us", tz="+00:00")
    converted_iceberg_type = visit_pyarrow(pyarrow_type, _ConvertToIceberg())
    converted_iceberg_type_zero_offset = visit_pyarrow(pyarrow_type_zero_offset, _ConvertToIceberg())
    assert converted_iceberg_type == TimestamptzType()
    assert converted_iceberg_type_zero_offset == TimestamptzType()
    assert visit(converted_iceberg_type, _ConvertToArrowSchema()) == pyarrow_type
    assert visit(converted_iceberg_type_zero_offset, _ConvertToArrowSchema()) == pyarrow_type


def test_pyarrow_timestamp_tz_invalid_units() -> None:
    pyarrow_type = pa.timestamp(unit="ns", tz="UTC")
    with pytest.raises(
        TypeError,
        match=re.escape(
            "Iceberg does not yet support 'ns' timestamp precision. Use 'downcast-ns-timestamp-to-us-on-write' configuration property to automatically downcast 'ns' to 'us' on write."
        ),
    ):
        visit_pyarrow(pyarrow_type, _ConvertToIceberg())


def test_pyarrow_timestamp_tz_invalid_tz() -> None:
    pyarrow_type = pa.timestamp(unit="us", tz="US/Pacific")
    with pytest.raises(TypeError, match=re.escape("Unsupported type: timestamp[us, tz=US/Pacific]")):
        visit_pyarrow(pyarrow_type, _ConvertToIceberg())


def test_pyarrow_string_to_iceberg() -> None:
    pyarrow_type = pa.large_string()
    converted_iceberg_type = visit_pyarrow(pyarrow_type, _ConvertToIceberg())
    assert converted_iceberg_type == StringType()
    assert visit(converted_iceberg_type, _ConvertToArrowSchema()) == pyarrow_type


def test_pyarrow_variable_binary_to_iceberg() -> None:
    pyarrow_type = pa.large_binary()
    converted_iceberg_type = visit_pyarrow(pyarrow_type, _ConvertToIceberg())
    assert converted_iceberg_type == BinaryType()
    assert visit(converted_iceberg_type, _ConvertToArrowSchema()) == pyarrow_type


def test_pyarrow_struct_to_iceberg() -> None:
    pyarrow_struct = pa.struct([
        pa.field("foo", pa.string(), nullable=True, metadata={"PARQUET:field_id": "1", "doc": "foo doc"}),
        pa.field("bar", pa.int32(), nullable=False, metadata={"PARQUET:field_id": "2"}),
        pa.field("baz", pa.bool_(), nullable=True, metadata={"PARQUET:field_id": "3"}),
    ])
    expected = StructType(
        NestedField(field_id=1, name="foo", field_type=StringType(), required=False, doc="foo doc"),
        NestedField(field_id=2, name="bar", field_type=IntegerType(), required=True),
        NestedField(field_id=3, name="baz", field_type=BooleanType(), required=False),
    )
    assert visit_pyarrow(pyarrow_struct, _ConvertToIceberg()) == expected


def test_pyarrow_list_to_iceberg() -> None:
    pyarrow_list = pa.list_(pa.field("element", pa.int32(), nullable=False, metadata={"PARQUET:field_id": "1"}))
    expected = ListType(
        element_id=1,
        element_type=IntegerType(),
        element_required=True,
    )
    assert visit_pyarrow(pyarrow_list, _ConvertToIceberg()) == expected


def test_pyarrow_large_list_to_iceberg() -> None:
    pyarrow_list = pa.large_list(pa.field("element", pa.int32(), nullable=False, metadata={"PARQUET:field_id": "1"}))
    expected = ListType(
        element_id=1,
        element_type=IntegerType(),
        element_required=True,
    )
    assert visit_pyarrow(pyarrow_list, _ConvertToIceberg()) == expected


def test_pyarrow_fixed_size_list_to_iceberg() -> None:
    pyarrow_list = pa.list_(pa.field("element", pa.int32(), nullable=False, metadata={"PARQUET:field_id": "1"}), 1)
    expected = ListType(
        element_id=1,
        element_type=IntegerType(),
        element_required=True,
    )
    assert visit_pyarrow(pyarrow_list, _ConvertToIceberg()) == expected


def test_pyarrow_map_to_iceberg() -> None:
    pyarrow_map = pa.map_(
        pa.field("key", pa.int32(), nullable=False, metadata={"PARQUET:field_id": "1"}),
        pa.field("value", pa.string(), nullable=False, metadata={"PARQUET:field_id": "2"}),
    )
    expected = MapType(
        key_id=1,
        key_type=IntegerType(),
        value_id=2,
        value_type=StringType(),
        value_required=True,
    )
    assert visit_pyarrow(pyarrow_map, _ConvertToIceberg()) == expected


@pytest.mark.parametrize(
    "value_type, expected_result",
    [
        (pa.string(), StringType()),
        (pa.int32(), IntegerType()),
        (pa.float64(), DoubleType()),
    ],
)
def test_pyarrow_dictionary_encoded_type_to_iceberg(value_type: pa.DataType, expected_result: IcebergType) -> None:
    pyarrow_dict = pa.dictionary(pa.int32(), value_type)
    assert visit_pyarrow(pyarrow_dict, _ConvertToIceberg()) == expected_result


def test_round_schema_conversion_simple(table_schema_simple: Schema) -> None:
    actual = str(pyarrow_to_schema(schema_to_pyarrow(table_schema_simple)))
    expected = """table {
  1: foo: optional string
  2: bar: required int
  3: baz: optional boolean
}"""
    assert actual == expected


def test_round_schema_conversion_nested(table_schema_nested: Schema) -> None:
    actual = str(pyarrow_to_schema(schema_to_pyarrow(table_schema_nested)))
    expected = """table {
  1: foo: optional string
  2: bar: required int
  3: baz: optional boolean
  4: qux: required list<string>
  6: quux: required map<string, map<string, int>>
  11: location: required list<struct<13: latitude: optional float, 14: longitude: optional float>>
  15: person: optional struct<16: name: optional string, 17: age: required int>
}"""
    assert actual == expected


def test_round_schema_large_string() -> None:
    schema = pa.schema([pa.field("animals", pa.large_string())])
    actual = str(pyarrow_to_schema(schema, name_mapping=NameMapping([MappedField(field_id=1, names=["animals"])])))
    expected = """table {
  1: animals: optional string
}"""
    assert actual == expected


def test_simple_schema_has_missing_ids() -> None:
    schema = pa.schema([
        pa.field("foo", pa.string(), nullable=False),
    ])
    visitor = _HasIds()
    has_ids = visit_pyarrow(schema, visitor)
    assert not has_ids


def test_simple_schema_has_missing_ids_partial() -> None:
    schema = pa.schema([
        pa.field("foo", pa.string(), nullable=False, metadata={"PARQUET:field_id": "1", "doc": "foo doc"}),
        pa.field("bar", pa.int32(), nullable=False),
    ])
    visitor = _HasIds()
    has_ids = visit_pyarrow(schema, visitor)
    assert not has_ids


def test_nested_schema_has_missing_ids() -> None:
    schema = pa.schema([
        pa.field("foo", pa.string(), nullable=False),
        pa.field(
            "quux",
            pa.map_(
                pa.string(),
                pa.map_(pa.string(), pa.int32()),
            ),
            nullable=False,
        ),
    ])
    visitor = _HasIds()
    has_ids = visit_pyarrow(schema, visitor)
    assert not has_ids


def test_nested_schema_has_ids() -> None:
    schema = pa.schema([
        pa.field("foo", pa.string(), nullable=False, metadata={"PARQUET:field_id": "1", "doc": "foo doc"}),
        pa.field(
            "quux",
            pa.map_(
                pa.field("key", pa.string(), nullable=False, metadata={"PARQUET:field_id": "7"}),
                pa.field(
                    "value",
                    pa.map_(
                        pa.field("key", pa.string(), nullable=False, metadata={"PARQUET:field_id": "9"}),
                        pa.field("value", pa.int32(), metadata={"PARQUET:field_id": "10"}),
                    ),
                    nullable=False,
                    metadata={"PARQUET:field_id": "8"},
                ),
            ),
            nullable=False,
            metadata={"PARQUET:field_id": "6", "doc": "quux doc"},
        ),
    ])
    visitor = _HasIds()
    has_ids = visit_pyarrow(schema, visitor)
    assert has_ids


def test_nested_schema_has_partial_missing_ids() -> None:
    schema = pa.schema([
        pa.field("foo", pa.string(), nullable=False, metadata={"PARQUET:field_id": "1", "doc": "foo doc"}),
        pa.field(
            "quux",
            pa.map_(
                pa.field("key", pa.string(), nullable=False, metadata={"PARQUET:field_id": "7"}),
                pa.field(
                    "value",
                    pa.map_(pa.field("key", pa.string(), nullable=False), pa.field("value", pa.int32())),
                    nullable=False,
                ),
            ),
            nullable=False,
            metadata={"PARQUET:field_id": "6", "doc": "quux doc"},
        ),
    ])
    visitor = _HasIds()
    has_ids = visit_pyarrow(schema, visitor)
    assert not has_ids


def test_pyarrow_schema_to_schema_missing_ids_and_name_mapping(pyarrow_schema_simple_without_ids: pa.Schema) -> None:
    schema = pyarrow_schema_simple_without_ids
    with pytest.raises(ValueError) as exc_info:
        _ = pyarrow_to_schema(schema)
    assert (
        "Parquet file does not have field-ids and the Iceberg table does not have 'schema.name-mapping.default' defined"
        in str(exc_info.value)
    )


def test_simple_pyarrow_schema_to_schema_missing_ids_using_name_mapping(
    pyarrow_schema_simple_without_ids: pa.Schema, iceberg_schema_simple: Schema
) -> None:
    schema = pyarrow_schema_simple_without_ids
    name_mapping = NameMapping([
        MappedField(field_id=1, names=["foo"]),
        MappedField(field_id=2, names=["bar"]),
        MappedField(field_id=3, names=["baz"]),
    ])

    assert pyarrow_to_schema(schema, name_mapping) == iceberg_schema_simple


def test_simple_pyarrow_schema_to_schema_missing_ids_using_name_mapping_partial_exception(
    pyarrow_schema_simple_without_ids: pa.Schema,
) -> None:
    schema = pyarrow_schema_simple_without_ids
    name_mapping = NameMapping([
        MappedField(field_id=1, names=["foo"]),
    ])
    with pytest.raises(ValueError) as exc_info:
        _ = pyarrow_to_schema(schema, name_mapping)
    assert "Could not find field with name: bar" in str(exc_info.value)


def test_nested_pyarrow_schema_to_schema_missing_ids_using_name_mapping(
    pyarrow_schema_nested_without_ids: pa.Schema, iceberg_schema_nested: Schema
) -> None:
    schema = pyarrow_schema_nested_without_ids

    name_mapping = NameMapping([
        MappedField(field_id=1, names=["foo"]),
        MappedField(field_id=2, names=["bar"]),
        MappedField(field_id=3, names=["baz"]),
        MappedField(field_id=4, names=["qux"], fields=[MappedField(field_id=5, names=["element"])]),
        MappedField(
            field_id=6,
            names=["quux"],
            fields=[
                MappedField(field_id=7, names=["key"]),
                MappedField(
                    field_id=8,
                    names=["value"],
                    fields=[
                        MappedField(field_id=9, names=["key"]),
                        MappedField(field_id=10, names=["value"]),
                    ],
                ),
            ],
        ),
        MappedField(
            field_id=11,
            names=["location"],
            fields=[
                MappedField(
                    field_id=12,
                    names=["element"],
                    fields=[
                        MappedField(field_id=13, names=["latitude"]),
                        MappedField(field_id=14, names=["longitude"]),
                    ],
                )
            ],
        ),
        MappedField(
            field_id=15,
            names=["person"],
            fields=[
                MappedField(field_id=16, names=["name"]),
                MappedField(field_id=17, names=["age"]),
            ],
        ),
    ])

    assert pyarrow_to_schema(schema, name_mapping) == iceberg_schema_nested


def test_pyarrow_schema_to_schema_missing_ids_using_name_mapping_nested_missing_id() -> None:
    schema = pa.schema([
        pa.field("foo", pa.string(), nullable=False),
        pa.field(
            "quux",
            pa.map_(
                pa.string(),
                pa.map_(pa.string(), pa.int32()),
            ),
            nullable=False,
        ),
    ])

    name_mapping = NameMapping([
        MappedField(field_id=1, names=["foo"]),
        MappedField(
            field_id=6,
            names=["quux"],
            fields=[
                MappedField(field_id=7, names=["key"]),
                MappedField(
                    field_id=8,
                    names=["value"],
                    fields=[
                        MappedField(field_id=10, names=["value"]),
                    ],
                ),
            ],
        ),
    ])
    with pytest.raises(ValueError) as exc_info:
        _ = pyarrow_to_schema(schema, name_mapping)
    assert "Could not find field with name: quux.value.key" in str(exc_info.value)


def test_pyarrow_schema_to_schema_fresh_ids_simple_schema(
    pyarrow_schema_simple_without_ids: pa.Schema, iceberg_schema_simple_no_ids: Schema
) -> None:
    assert visit_pyarrow(pyarrow_schema_simple_without_ids, _ConvertToIcebergWithoutIDs()) == iceberg_schema_simple_no_ids


def test_pyarrow_schema_to_schema_fresh_ids_nested_schema(
    pyarrow_schema_nested_without_ids: pa.Schema, iceberg_schema_nested_no_ids: Schema
) -> None:
    assert visit_pyarrow(pyarrow_schema_nested_without_ids, _ConvertToIcebergWithoutIDs()) == iceberg_schema_nested_no_ids


def test_pyarrow_schema_ensure_large_types(pyarrow_schema_nested_without_ids: pa.Schema) -> None:
    expected_schema = pa.schema([
        pa.field("foo", pa.large_string(), nullable=False),
        pa.field("bar", pa.int32(), nullable=False),
        pa.field("baz", pa.bool_(), nullable=True),
        pa.field("qux", pa.large_list(pa.large_string()), nullable=False),
        pa.field(
            "quux",
            pa.map_(
                pa.large_string(),
                pa.map_(pa.large_string(), pa.int32()),
            ),
            nullable=False,
        ),
        pa.field(
            "location",
            pa.large_list(
                pa.struct([
                    pa.field("latitude", pa.float32(), nullable=False),
                    pa.field("longitude", pa.float32(), nullable=False),
                ]),
            ),
            nullable=False,
        ),
        pa.field(
            "person",
            pa.struct([
                pa.field("name", pa.large_string(), nullable=True),
                pa.field("age", pa.int32(), nullable=False),
            ]),
            nullable=True,
        ),
    ])
    assert _pyarrow_schema_ensure_large_types(pyarrow_schema_nested_without_ids) == expected_schema


@pytest.fixture
def bound_reference_str() -> BoundReference[Any]:
    return BoundReference(
        field=NestedField(1, "string_field", StringType(), required=False), accessor=Accessor(position=0, inner=None)
    )


@pytest.fixture
def bound_reference_float() -> BoundReference[Any]:
    return BoundReference(
        field=NestedField(2, "float_field", FloatType(), required=False), accessor=Accessor(position=1, inner=None)
    )


@pytest.fixture
def bound_reference_double() -> BoundReference[Any]:
    return BoundReference(
        field=NestedField(3, "double_field", DoubleType(), required=False),
        accessor=Accessor(position=2, inner=None),
    )


@pytest.fixture
def bound_eq_str_field(bound_reference_str: BoundReference[Any]) -> BoundEqualTo[Any]:
    return BoundEqualTo(term=bound_reference_str, literal=literal("hello"))


@pytest.fixture
def bound_greater_than_float_field(bound_reference_float: BoundReference[Any]) -> BoundGreaterThan[Any]:
    return BoundGreaterThan(term=bound_reference_float, literal=literal(100))


@pytest.fixture
def bound_is_nan_float_field(bound_reference_float: BoundReference[Any]) -> BoundIsNaN[Any]:
    return BoundIsNaN(bound_reference_float)


@pytest.fixture
def bound_eq_double_field(bound_reference_double: BoundReference[Any]) -> BoundEqualTo[Any]:
    return BoundEqualTo(term=bound_reference_double, literal=literal(False))


@pytest.fixture
def bound_is_null_double_field(bound_reference_double: BoundReference[Any]) -> BoundIsNull[Any]:
    return BoundIsNull(bound_reference_double)


def test_collect_null_nan_unmentioned_terms(
    bound_eq_str_field: BoundEqualTo[Any], bound_is_nan_float_field: BoundIsNaN[Any], bound_is_null_double_field: BoundIsNull[Any]
) -> None:
    bound_expr = And(
        Or(And(bound_eq_str_field, bound_is_nan_float_field), bound_is_null_double_field), Not(bound_is_nan_float_field)
    )
    collector = _NullNaNUnmentionedTermsCollector()
    collector.collect(bound_expr)
    assert {t.ref().field.name for t in collector.null_unmentioned_bound_terms} == {
        "float_field",
        "string_field",
    }
    assert {t.ref().field.name for t in collector.nan_unmentioned_bound_terms} == {
        "string_field",
        "double_field",
    }
    assert {t.ref().field.name for t in collector.is_null_or_not_bound_terms} == {
        "double_field",
    }
    assert {t.ref().field.name for t in collector.is_nan_or_not_bound_terms} == {"float_field"}


def test_collect_null_nan_unmentioned_terms_with_multiple_predicates_on_the_same_term(
    bound_eq_str_field: BoundEqualTo[Any],
    bound_greater_than_float_field: BoundGreaterThan[Any],
    bound_is_nan_float_field: BoundIsNaN[Any],
    bound_eq_double_field: BoundEqualTo[Any],
    bound_is_null_double_field: BoundIsNull[Any],
) -> None:
    """Test a single term appears multiple places in the expression tree"""
    bound_expr = And(
        Or(
            And(bound_eq_str_field, bound_greater_than_float_field),
            And(bound_is_nan_float_field, bound_eq_double_field),
            bound_greater_than_float_field,
        ),
        Not(bound_is_null_double_field),
    )
    collector = _NullNaNUnmentionedTermsCollector()
    collector.collect(bound_expr)
    assert {t.ref().field.name for t in collector.null_unmentioned_bound_terms} == {
        "float_field",
        "string_field",
    }
    assert {t.ref().field.name for t in collector.nan_unmentioned_bound_terms} == {
        "string_field",
        "double_field",
    }
    assert {t.ref().field.name for t in collector.is_null_or_not_bound_terms} == {
        "double_field",
    }
    assert {t.ref().field.name for t in collector.is_nan_or_not_bound_terms} == {"float_field"}


def test_expression_to_complementary_pyarrow(
    bound_eq_str_field: BoundEqualTo[Any],
    bound_greater_than_float_field: BoundGreaterThan[Any],
    bound_is_nan_float_field: BoundIsNaN[Any],
    bound_eq_double_field: BoundEqualTo[Any],
    bound_is_null_double_field: BoundIsNull[Any],
) -> None:
    bound_expr = And(
        Or(
            And(bound_eq_str_field, bound_greater_than_float_field),
            And(bound_is_nan_float_field, bound_eq_double_field),
            bound_greater_than_float_field,
        ),
        Not(bound_is_null_double_field),
    )
    result = _expression_to_complementary_pyarrow(bound_expr)
    # Notice an isNan predicate on a str column is automatically converted to always false and removed from Or and thus will not appear in the pc.expr.
    assert (
        repr(result)
        == """<pyarrow.compute.Expression (((invert((((((string_field == "hello") and (float_field > 100)) or (is_nan(float_field) and (double_field == 0))) or (float_field > 100)) and invert(is_null(double_field, {nan_is_null=false})))) or is_null(float_field, {nan_is_null=false})) or is_null(string_field, {nan_is_null=false})) or is_nan(double_field))>"""
    )
