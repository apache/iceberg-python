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
# pylint:disable=redefined-outer-name,eval-used

import pickle
import uuid
from decimal import Decimal

import pytest
from typing_extensions import assert_type

from pyiceberg.expressions import (
    AlwaysFalse,
    AlwaysTrue,
    And,
    BooleanExpression,
    BoundEqualTo,
    BoundGreaterThan,
    BoundGreaterThanOrEqual,
    BoundIn,
    BoundIsNaN,
    BoundIsNull,
    BoundLessThan,
    BoundLessThanOrEqual,
    BoundNotEqualTo,
    BoundNotIn,
    BoundNotNaN,
    BoundNotNull,
    BoundReference,
    EqualTo,
    GreaterThan,
    GreaterThanOrEqual,
    In,
    IsNaN,
    IsNull,
    LessThan,
    LessThanOrEqual,
    LiteralPredicate,
    Not,
    NotEqualTo,
    NotIn,
    NotNaN,
    NotNull,
    NotStartsWith,
    Or,
    Reference,
    StartsWith,
    UnboundPredicate,
)
from pyiceberg.expressions.literals import Literal, literal
from pyiceberg.expressions.visitors import _from_byte_buffer
from pyiceberg.schema import Accessor, Schema
from pyiceberg.typedef import Record
from pyiceberg.types import (
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    ListType,
    LongType,
    NestedField,
    StringType,
    StructType,
)


def test_isnull_inverse() -> None:
    assert ~IsNull(Reference("a")) == NotNull(Reference("a"))


def test_isnull_bind() -> None:
    schema = Schema(NestedField(2, "a", IntegerType()), schema_id=1)
    bound = BoundIsNull(BoundReference(schema.find_field(2), schema.accessor_for_field(2)))
    assert IsNull(Reference("a")).bind(schema) == bound


def test_invert_is_null_bind() -> None:
    schema = Schema(NestedField(2, "a", IntegerType(), required=False), schema_id=1)
    assert ~IsNull(Reference("a")).bind(schema) == NotNull(Reference("a")).bind(schema)


def test_invert_not_null_bind() -> None:
    schema = Schema(NestedField(2, "a", IntegerType(), required=False), schema_id=1)
    assert ~NotNull(Reference("a")).bind(schema) == IsNull(Reference("a")).bind(schema)


def test_invert_is_nan_bind() -> None:
    schema = Schema(NestedField(2, "a", DoubleType(), required=False), schema_id=1)
    assert ~IsNaN(Reference("a")).bind(schema) == NotNaN(Reference("a")).bind(schema)


def test_invert_not_nan_bind() -> None:
    schema = Schema(NestedField(2, "a", DoubleType(), required=False), schema_id=1)
    assert ~NotNaN(Reference("a")).bind(schema) == IsNaN(Reference("a")).bind(schema)


def test_bind_expr_does_not_exists() -> None:
    schema = Schema(NestedField(2, "a", IntegerType()), schema_id=1)
    with pytest.raises(ValueError) as exc_info:
        IsNull(Reference("b")).bind(schema)

    assert str(exc_info.value) == "Could not find field with name b, case_sensitive=True"


def test_bind_does_not_exists() -> None:
    schema = Schema(NestedField(2, "a", IntegerType()), schema_id=1)
    with pytest.raises(ValueError) as exc_info:
        Reference("b").bind(schema)

    assert str(exc_info.value) == "Could not find field with name b, case_sensitive=True"


def test_isnull_bind_required() -> None:
    schema = Schema(NestedField(2, "a", IntegerType(), required=True), schema_id=1)
    assert IsNull(Reference("a")).bind(schema) == AlwaysFalse()


def test_notnull_inverse() -> None:
    assert ~NotNull(Reference("a")) == IsNull(Reference("a"))


def test_notnull_bind() -> None:
    schema = Schema(NestedField(2, "a", IntegerType()), schema_id=1)
    bound = BoundNotNull(BoundReference(schema.find_field(2), schema.accessor_for_field(2)))
    assert NotNull(Reference("a")).bind(schema) == bound


def test_notnull_bind_required() -> None:
    schema = Schema(NestedField(2, "a", IntegerType(), required=True), schema_id=1)
    assert NotNull(Reference("a")).bind(schema) == AlwaysTrue()


def test_notnull_bind_top_struct() -> None:
    schema = Schema(
        NestedField(
            3,
            "struct_col",
            required=False,
            field_type=StructType(
                NestedField(1, "id", IntegerType(), required=True),
                NestedField(2, "cost", DecimalType(38, 18), required=False),
            ),
        ),
        schema_id=1,
    )
    bound = BoundNotNull(BoundReference(schema.find_field(3), schema.accessor_for_field(3)))
    assert NotNull(Reference("struct_col")).bind(schema) == bound


def test_isnan_inverse() -> None:
    assert ~IsNaN(Reference("f")) == NotNaN(Reference("f"))


def test_isnan_bind_float() -> None:
    schema = Schema(NestedField(2, "f", FloatType()), schema_id=1)
    bound = BoundIsNaN(BoundReference(schema.find_field(2), schema.accessor_for_field(2)))
    assert IsNaN(Reference("f")).bind(schema) == bound


def test_isnan_bind_double() -> None:
    schema = Schema(NestedField(2, "d", DoubleType()), schema_id=1)
    bound = BoundIsNaN(BoundReference(schema.find_field(2), schema.accessor_for_field(2)))
    assert IsNaN(Reference("d")).bind(schema) == bound


def test_isnan_bind_nonfloat() -> None:
    schema = Schema(NestedField(2, "i", IntegerType()), schema_id=1)
    assert IsNaN(Reference("i")).bind(schema) == AlwaysFalse()


def test_notnan_inverse() -> None:
    assert ~NotNaN(Reference("f")) == IsNaN(Reference("f"))


def test_notnan_bind_float() -> None:
    schema = Schema(NestedField(2, "f", FloatType()), schema_id=1)
    bound = BoundNotNaN(BoundReference(schema.find_field(2), schema.accessor_for_field(2)))
    assert NotNaN(Reference("f")).bind(schema) == bound


def test_notnan_bind_double() -> None:
    schema = Schema(NestedField(2, "d", DoubleType()), schema_id=1)
    bound = BoundNotNaN(BoundReference(schema.find_field(2), schema.accessor_for_field(2)))
    assert NotNaN(Reference("d")).bind(schema) == bound


def test_notnan_bind_nonfloat() -> None:
    schema = Schema(NestedField(2, "i", IntegerType()), schema_id=1)
    assert NotNaN(Reference("i")).bind(schema) == AlwaysTrue()


def test_ref_binding_case_sensitive(table_schema_simple: Schema) -> None:
    ref = Reference("foo")
    bound = BoundReference(table_schema_simple.find_field(1), table_schema_simple.accessor_for_field(1))
    assert ref.bind(table_schema_simple, case_sensitive=True) == bound


def test_ref_binding_case_sensitive_failure(table_schema_simple: Schema) -> None:
    ref = Reference("Foo")
    with pytest.raises(ValueError):
        ref.bind(table_schema_simple, case_sensitive=True)


def test_ref_binding_case_insensitive(table_schema_simple: Schema) -> None:
    ref = Reference("Foo")
    bound = BoundReference(table_schema_simple.find_field(1), table_schema_simple.accessor_for_field(1))
    assert ref.bind(table_schema_simple, case_sensitive=False) == bound


def test_ref_binding_case_insensitive_failure(table_schema_simple: Schema) -> None:
    ref = Reference("Foot")
    with pytest.raises(ValueError):
        ref.bind(table_schema_simple, case_sensitive=False)


def test_ref_binding_nested_struct_field() -> None:
    """Test binding references to nested struct fields (issue #953)."""
    schema = Schema(
        NestedField(field_id=1, name="age", field_type=IntegerType(), required=True),
        NestedField(
            field_id=2,
            name="employment",
            field_type=StructType(
                NestedField(field_id=3, name="status", field_type=StringType(), required=False),
                NestedField(field_id=4, name="company", field_type=StringType(), required=False),
            ),
            required=False,
        ),
        NestedField(
            field_id=5,
            name="contact",
            field_type=StructType(
                NestedField(field_id=6, name="email", field_type=StringType(), required=False),
            ),
            required=False,
        ),
        schema_id=1,
    )

    # Test that nested field names are in the index
    assert "employment.status" in schema._name_to_id
    assert "employment.company" in schema._name_to_id
    assert "contact.email" in schema._name_to_id

    # Test binding a reference to nested fields
    ref = Reference("employment.status")
    bound = ref.bind(schema, case_sensitive=True)
    assert bound.field.field_id == 3
    assert bound.field.name == "status"

    # Test with different nested field
    ref2 = Reference("contact.email")
    bound2 = ref2.bind(schema, case_sensitive=True)
    assert bound2.field.field_id == 6
    assert bound2.field.name == "email"

    # Test case-insensitive binding
    ref3 = Reference("EMPLOYMENT.STATUS")
    bound3 = ref3.bind(schema, case_sensitive=False)
    assert bound3.field.field_id == 3

    # Test that binding fails for non-existent nested field
    ref4 = Reference("employment.department")
    with pytest.raises(ValueError):
        ref4.bind(schema, case_sensitive=True)


def test_in_to_eq() -> None:
    assert In("x", (34.56,)) == EqualTo("x", 34.56)


def test_empty_bind_in(table_schema_simple: Schema) -> None:
    bound = BoundIn(BoundReference(table_schema_simple.find_field(1), table_schema_simple.accessor_for_field(1)), set())
    assert bound == AlwaysFalse()


def test_empty_bind_not_in(table_schema_simple: Schema) -> None:
    bound = BoundNotIn(BoundReference(table_schema_simple.find_field(1), table_schema_simple.accessor_for_field(1)), set())
    assert bound == AlwaysTrue()


def test_bind_not_in_equal_term(table_schema_simple: Schema) -> None:
    bound = BoundNotIn(
        BoundReference(table_schema_simple.find_field(1), table_schema_simple.accessor_for_field(1)), {literal("hello")}
    )
    assert (
        BoundNotEqualTo(
            term=BoundReference(
                field=NestedField(field_id=1, name="foo", field_type=StringType(), required=False),
                accessor=Accessor(position=0, inner=None),
            ),
            literal=literal("hello"),
        )
        == bound
    )


def test_in_empty() -> None:
    assert In(Reference("foo"), ()) == AlwaysFalse()


def test_in_set() -> None:
    assert In(Reference("foo"), {"a", "bc", "def"}).literals == {literal("a"), literal("bc"), literal("def")}


def test_in_tuple() -> None:
    assert In(Reference("foo"), ("a", "bc", "def")).literals == {literal("a"), literal("bc"), literal("def")}


def test_in_list() -> None:
    assert In(Reference("foo"), ["a", "bc", "def"]).literals == {literal("a"), literal("bc"), literal("def")}


def test_not_in_empty() -> None:
    assert NotIn(Reference("foo"), ()) == AlwaysTrue()


def test_not_in_equal() -> None:
    assert NotIn(Reference("foo"), ("hello",)) == NotEqualTo(term=Reference(name="foo"), literal="hello")


def test_bind_in(table_schema_simple: Schema) -> None:
    bound = BoundIn(
        BoundReference(table_schema_simple.find_field(1), table_schema_simple.accessor_for_field(1)),
        {literal("hello"), literal("world")},
    )
    assert In(Reference("foo"), ("hello", "world")).bind(table_schema_simple) == bound


def test_bind_in_invert(table_schema_simple: Schema) -> None:
    bound = BoundIn(
        BoundReference(table_schema_simple.find_field(1), table_schema_simple.accessor_for_field(1)),
        {literal("hello"), literal("world")},
    )
    assert ~bound == BoundNotIn(
        BoundReference(table_schema_simple.find_field(1), table_schema_simple.accessor_for_field(1)),
        {literal("hello"), literal("world")},
    )


def test_bind_not_in_invert(table_schema_simple: Schema) -> None:
    bound = BoundNotIn(
        BoundReference(table_schema_simple.find_field(1), table_schema_simple.accessor_for_field(1)),
        {literal("hello"), literal("world")},
    )
    assert ~bound == BoundIn(
        BoundReference(table_schema_simple.find_field(1), table_schema_simple.accessor_for_field(1)),
        {literal("hello"), literal("world")},
    )


def test_bind_dedup(table_schema_simple: Schema) -> None:
    bound = BoundIn(
        BoundReference(table_schema_simple.find_field(1), table_schema_simple.accessor_for_field(1)),
        {literal("hello"), literal("world")},
    )
    assert In(Reference("foo"), ("hello", "world", "world")).bind(table_schema_simple) == bound


def test_bind_dedup_to_eq(table_schema_simple: Schema) -> None:
    bound = BoundEqualTo(
        BoundReference(table_schema_simple.find_field(1), table_schema_simple.accessor_for_field(1)), literal("hello")
    )
    assert In(Reference("foo"), ("hello", "hello")).bind(table_schema_simple) == bound


def test_bound_equal_to_invert(table_schema_simple: Schema) -> None:
    bound = BoundEqualTo(
        BoundReference(table_schema_simple.find_field(1), table_schema_simple.accessor_for_field(1)), literal("hello")
    )
    assert ~bound == BoundNotEqualTo(
        term=BoundReference(
            field=NestedField(field_id=1, name="foo", field_type=StringType(), required=False),
            accessor=Accessor(position=0, inner=None),
        ),
        literal=literal("hello"),
    )


def test_bound_not_equal_to_invert(table_schema_simple: Schema) -> None:
    bound = BoundNotEqualTo(
        BoundReference(table_schema_simple.find_field(1), table_schema_simple.accessor_for_field(1)), literal("hello")
    )
    assert ~bound == BoundEqualTo(
        term=BoundReference(
            field=NestedField(field_id=1, name="foo", field_type=StringType(), required=False),
            accessor=Accessor(position=0, inner=None),
        ),
        literal=literal("hello"),
    )


def test_bound_greater_than_or_equal_invert(table_schema_simple: Schema) -> None:
    bound = BoundGreaterThanOrEqual(
        BoundReference(table_schema_simple.find_field(1), table_schema_simple.accessor_for_field(1)), literal("hello")
    )
    assert ~bound == BoundLessThan(
        term=BoundReference(
            field=NestedField(field_id=1, name="foo", field_type=StringType(), required=False),
            accessor=Accessor(position=0, inner=None),
        ),
        literal=literal("hello"),
    )


def test_bound_greater_than_invert(table_schema_simple: Schema) -> None:
    bound = BoundGreaterThan(
        BoundReference(table_schema_simple.find_field(1), table_schema_simple.accessor_for_field(1)), literal("hello")
    )
    assert ~bound == BoundLessThanOrEqual(
        term=BoundReference(
            field=NestedField(field_id=1, name="foo", field_type=StringType(), required=False),
            accessor=Accessor(position=0, inner=None),
        ),
        literal=literal("hello"),
    )


def test_bound_less_than_invert(table_schema_simple: Schema) -> None:
    bound = BoundLessThan(
        BoundReference(table_schema_simple.find_field(1), table_schema_simple.accessor_for_field(1)), literal("hello")
    )
    assert ~bound == BoundGreaterThanOrEqual(
        term=BoundReference(
            field=NestedField(field_id=1, name="foo", field_type=StringType(), required=False),
            accessor=Accessor(position=0, inner=None),
        ),
        literal=literal("hello"),
    )


def test_bound_less_than_or_equal_invert(table_schema_simple: Schema) -> None:
    bound = BoundLessThanOrEqual(
        BoundReference(table_schema_simple.find_field(1), table_schema_simple.accessor_for_field(1)), literal("hello")
    )
    assert ~bound == BoundGreaterThan(
        term=BoundReference(
            field=NestedField(field_id=1, name="foo", field_type=StringType(), required=False),
            accessor=Accessor(position=0, inner=None),
        ),
        literal=literal("hello"),
    )


def test_not_equal_to_invert() -> None:
    bound = NotEqualTo(
        term=Reference("foo"),
        literal="hello",
    )
    assert ~bound == EqualTo(
        term=Reference("foo"),
        literal="hello",
    )


def test_greater_than_or_equal_invert() -> None:
    bound = GreaterThanOrEqual(
        term=Reference("foo"),
        literal="hello",
    )
    assert ~bound == LessThan(
        term=Reference("foo"),
        literal="hello",
    )


def test_less_than_or_equal_invert() -> None:
    bound = LessThanOrEqual(
        term=Reference("foo"),
        literal="hello",
    )
    assert ~bound == GreaterThan(
        term=Reference("foo"),
        literal="hello",
    )


@pytest.mark.parametrize(
    "pred",
    [
        NotIn(Reference("foo"), ("hello", "world")),
        NotEqualTo(Reference("foo"), "hello"),
        EqualTo(Reference("foo"), "hello"),
        GreaterThan(Reference("foo"), "hello"),
        LessThan(Reference("foo"), "hello"),
        GreaterThanOrEqual(Reference("foo"), "hello"),
        LessThanOrEqual(Reference("foo"), "hello"),
    ],
)
def test_bind(pred: UnboundPredicate, table_schema_simple: Schema) -> None:
    assert pred.bind(table_schema_simple, case_sensitive=True).term.field == table_schema_simple.find_field(
        pred.term.name,  # type: ignore
        case_sensitive=True,
    )


@pytest.mark.parametrize(
    "pred",
    [
        In(Reference("Bar"), (5, 2)),
        NotIn(Reference("Bar"), (5, 2)),
        NotEqualTo(Reference("Bar"), 5),
        EqualTo(Reference("Bar"), 5),
        GreaterThan(Reference("Bar"), 5),
        LessThan(Reference("Bar"), 5),
        GreaterThanOrEqual(Reference("Bar"), 5),
        LessThanOrEqual(Reference("Bar"), 5),
    ],
)
def test_bind_case_insensitive(pred: UnboundPredicate, table_schema_simple: Schema) -> None:
    assert pred.bind(table_schema_simple, case_sensitive=False).term.field == table_schema_simple.find_field(
        pred.term.name,  # type: ignore
        case_sensitive=False,
    )


@pytest.mark.parametrize(
    "exp, testexpra, testexprb",
    [
        (
            And(IsNull("a"), IsNull("b")),
            And(IsNull("a"), IsNull("b")),
            Or(IsNull("a"), IsNull("b")),
        ),
        (
            Or(IsNull("a"), IsNull("b")),
            Or(IsNull("a"), IsNull("b")),
            And(IsNull("a"), IsNull("b")),
        ),
        (Not(IsNull("a")), Not(IsNull("a")), IsNull("b")),
        (IsNull("a"), IsNull("a"), IsNull("b")),
        (IsNull("b"), IsNull("b"), IsNull("a")),
        (
            In(Reference("foo"), ("hello", "world")),
            In(Reference("foo"), ("hello", "world")),
            In(Reference("not_foo"), ("hello", "world")),
        ),
        (
            In(Reference("foo"), ("hello", "world")),
            In(Reference("foo"), ("hello", "world")),
            In(Reference("foo"), ("goodbye", "world")),
        ),
    ],
)
def test_eq(exp: BooleanExpression, testexpra: BooleanExpression, testexprb: BooleanExpression) -> None:
    assert exp == testexpra and exp != testexprb


@pytest.mark.parametrize(
    "lhs, rhs",
    [
        (
            And(IsNull("a"), IsNull("b")),
            Or(NotNull("a"), NotNull("b")),
        ),
        (
            Or(IsNull("a"), IsNull("b")),
            And(NotNull("a"), NotNull("b")),
        ),
        (
            Not(IsNull("a")),
            IsNull("a"),
        ),
        (
            In(Reference("foo"), ("hello", "world")),
            NotIn(Reference("foo"), ("hello", "world")),
        ),
        (
            NotIn(Reference("foo"), ("hello", "world")),
            In(Reference("foo"), ("hello", "world")),
        ),
        (GreaterThan(Reference("foo"), 5), LessThanOrEqual(Reference("foo"), 5)),
        (LessThan(Reference("foo"), 5), GreaterThanOrEqual(Reference("foo"), 5)),
        (EqualTo(Reference("foo"), 5), NotEqualTo(Reference("foo"), 5)),
        (
            IsNull("a"),
            NotNull("a"),
        ),
    ],
)
def test_negate(lhs: BooleanExpression, rhs: BooleanExpression) -> None:
    assert ~lhs == rhs


@pytest.mark.parametrize(
    "lhs, rhs",
    [
        (
            And(IsNull("a"), IsNull("b"), IsNull("a")),
            And(IsNull("a"), And(IsNull("b"), IsNull("a"))),
        ),
        (
            Or(IsNull("a"), IsNull("b"), IsNull("a")),
            Or(IsNull("a"), Or(IsNull("b"), IsNull("a"))),
        ),
        (Not(Not(IsNull("a"))), IsNull("a")),
    ],
)
def test_reduce(lhs: BooleanExpression, rhs: BooleanExpression) -> None:
    assert lhs == rhs


@pytest.mark.parametrize(
    "lhs, rhs",
    [
        (And(AlwaysTrue(), IsNull("b")), IsNull("b")),
        (And(AlwaysFalse(), IsNull("b")), AlwaysFalse()),
        (And(IsNull("b"), AlwaysTrue()), IsNull("b")),
        (Or(AlwaysTrue(), IsNull("b")), AlwaysTrue()),
        (Or(AlwaysFalse(), IsNull("b")), IsNull("b")),
        (Or(IsNull("a"), AlwaysFalse()), IsNull("a")),
        (Not(Not(IsNull("a"))), IsNull("a")),
        (Not(AlwaysTrue()), AlwaysFalse()),
        (Not(AlwaysFalse()), AlwaysTrue()),
    ],
)
def test_base_AlwaysTrue_base_AlwaysFalse(lhs: BooleanExpression, rhs: BooleanExpression) -> None:
    assert lhs == rhs


def test_invert_always() -> None:
    assert ~AlwaysFalse() == AlwaysTrue()
    assert ~AlwaysTrue() == AlwaysFalse()


def test_accessor_base_class() -> None:
    """Test retrieving a value at a position of a container using an accessor"""

    struct = Record(*[None] * 12)

    uuid_value = uuid.uuid4()

    struct[0] = "foo"
    struct[1] = "bar"
    struct[2] = "baz"
    struct[3] = 1
    struct[4] = 2
    struct[5] = 3
    struct[6] = 1.234
    struct[7] = Decimal("1.234")
    struct[8] = uuid_value
    struct[9] = True
    struct[10] = False
    struct[11] = b"\x19\x04\x9e?"

    assert Accessor(position=0).get(struct) == "foo"
    assert Accessor(position=1).get(struct) == "bar"
    assert Accessor(position=2).get(struct) == "baz"
    assert Accessor(position=3).get(struct) == 1
    assert Accessor(position=4).get(struct) == 2
    assert Accessor(position=5).get(struct) == 3
    assert Accessor(position=6).get(struct) == 1.234
    assert Accessor(position=7).get(struct) == Decimal("1.234")
    assert Accessor(position=8).get(struct) == uuid_value
    assert Accessor(position=9).get(struct) is True
    assert Accessor(position=10).get(struct) is False
    assert Accessor(position=11).get(struct) == b"\x19\x04\x9e?"


@pytest.fixture
def field() -> NestedField:
    return NestedField(field_id=1, name="foo", field_type=StringType(), required=False)


@pytest.fixture
def accessor() -> Accessor:
    return Accessor(position=1)


@pytest.fixture
def term(field: NestedField, accessor: Accessor) -> BoundReference:
    return BoundReference(
        field=field,
        accessor=accessor,
    )


def test_bound_reference(field: NestedField, accessor: Accessor) -> None:
    bound_ref = BoundReference(field=field, accessor=accessor)
    assert str(bound_ref) == f"BoundReference(field={repr(field)}, accessor={repr(accessor)})"
    assert repr(bound_ref) == f"BoundReference(field={repr(field)}, accessor={repr(accessor)})"
    assert bound_ref == eval(repr(bound_ref))
    assert bound_ref == pickle.loads(pickle.dumps(bound_ref))


def test_reference() -> None:
    abc = "abc"
    ref = Reference(abc)
    assert str(ref) == "Reference(name='abc')"
    assert repr(ref) == "Reference(name='abc')"
    assert ref == eval(repr(ref))
    assert ref == pickle.loads(pickle.dumps(ref))
    assert ref.model_dump_json() == '"abc"'
    assert Reference.model_validate_json('"abc"') == ref


def test_and() -> None:
    null = IsNull(Reference("a"))
    nan = IsNaN(Reference("b"))
    and_ = And(null, nan)

    # Some syntactic sugar
    assert and_ == null & nan

    assert str(and_) == f"And(left={str(null)}, right={str(nan)})"
    assert repr(and_) == f"And(left={repr(null)}, right={repr(nan)})"
    assert and_ == eval(repr(and_))
    assert and_ == pickle.loads(pickle.dumps(and_))

    with pytest.raises(ValueError, match="Expected BooleanExpression, got: abc"):
        null & "abc"


def test_and_serialization() -> None:
    expr = And(EqualTo("x", 1), GreaterThan("y", 2))
    json_repr = '{"type":"and","left":{"term":"x","type":"eq","value":1},"right":{"term":"y","type":"gt","value":2}}'

    assert expr.model_dump_json() == json_repr
    assert BooleanExpression.model_validate_json(json_repr) == expr


def test_or() -> None:
    null = IsNull(Reference("a"))
    nan = IsNaN(Reference("b"))
    or_ = Or(null, nan)

    # Some syntactic sugar
    assert or_ == null | nan

    assert str(or_) == f"Or(left={str(null)}, right={str(nan)})"
    assert repr(or_) == f"Or(left={repr(null)}, right={repr(nan)})"
    assert or_ == eval(repr(or_))
    assert or_ == pickle.loads(pickle.dumps(or_))

    with pytest.raises(ValueError, match="Expected BooleanExpression, got: abc"):
        null | "abc"


def test_or_serialization() -> None:
    left = EqualTo("a", 10)
    right = EqualTo("b", 20)
    or_ = Or(left, right)
    json_repr = '{"type":"or","left":{"term":"a","type":"eq","value":10},"right":{"term":"b","type":"eq","value":20}}'

    assert or_.model_dump_json() == json_repr
    assert BooleanExpression.model_validate_json(json_repr) == or_


def test_not() -> None:
    null = IsNull(Reference("a"))
    not_ = Not(null)
    assert str(not_) == f"Not(child={str(null)})"
    assert repr(not_) == f"Not(child={repr(null)})"
    assert not_ == eval(repr(not_))
    assert not_ == pickle.loads(pickle.dumps(not_))


def test_not_json_serialization_and_deserialization() -> None:
    not_expr = Not(GreaterThan("a", 22))
    json_str = not_expr.model_dump_json()
    assert json_str == """{"type":"not","child":{"term":"a","type":"gt","value":22}}"""


def test_always_true() -> None:
    always_true = AlwaysTrue()
    assert always_true.model_dump_json() == "true"
    assert BooleanExpression.model_validate_json("true") == always_true
    assert str(always_true) == "AlwaysTrue()"
    assert repr(always_true) == "AlwaysTrue()"
    assert always_true == eval(repr(always_true))
    assert always_true == pickle.loads(pickle.dumps(always_true))


def test_always_false() -> None:
    always_false = AlwaysFalse()
    assert always_false.model_dump_json() == "false"
    assert BooleanExpression.model_validate_json("false") == always_false
    assert str(always_false) == "AlwaysFalse()"
    assert repr(always_false) == "AlwaysFalse()"
    assert always_false == eval(repr(always_false))
    assert always_false == pickle.loads(pickle.dumps(always_false))


def test_bound_reference_field_property() -> None:
    field = NestedField(field_id=1, name="foo", field_type=StringType(), required=False)
    position1_accessor = Accessor(position=1)
    bound_ref = BoundReference(field=field, accessor=position1_accessor)
    assert bound_ref.field == NestedField(field_id=1, name="foo", field_type=StringType(), required=False)


def test_bound_is_null(term: BoundReference) -> None:
    bound_is_null = BoundIsNull(term)
    assert str(bound_is_null) == f"BoundIsNull(term={str(term)})"
    assert repr(bound_is_null) == f"BoundIsNull(term={repr(term)})"
    assert bound_is_null == eval(repr(bound_is_null))


def test_bound_is_not_null(term: BoundReference) -> None:
    bound_not_null = BoundNotNull(term)
    assert str(bound_not_null) == f"BoundNotNull(term={str(term)})"
    assert repr(bound_not_null) == f"BoundNotNull(term={repr(term)})"
    assert bound_not_null == eval(repr(bound_not_null))


def test_is_null() -> None:
    ref = Reference("a")
    is_null = IsNull(ref)
    assert str(is_null) == f"IsNull(term={str(ref)})"
    assert repr(is_null) == f"IsNull(term={repr(ref)})"
    assert is_null == eval(repr(is_null))
    assert is_null == pickle.loads(pickle.dumps(is_null))
    pred = IsNull(term="foo")
    json_repr = '{"term":"foo","type":"is-null"}'
    assert pred.model_dump_json() == json_repr
    assert BooleanExpression.model_validate_json(json_repr) == pred


def test_not_null() -> None:
    ref = Reference("a")
    non_null = NotNull(ref)
    assert str(non_null) == f"NotNull(term={str(ref)})"
    assert repr(non_null) == f"NotNull(term={repr(ref)})"
    assert non_null == eval(repr(non_null))
    assert non_null == pickle.loads(pickle.dumps(non_null))
    pred = NotNull(term="foo")
    json_repr = '{"term":"foo","type":"not-null"}'
    assert pred.model_dump_json() == json_repr
    assert BooleanExpression.model_validate_json(json_repr) == pred


def test_bound_is_nan(accessor: Accessor) -> None:
    # We need a FloatType here
    term = BoundReference(
        field=NestedField(field_id=1, name="foo", field_type=FloatType(), required=False),
        accessor=accessor,
    )
    bound_is_nan = BoundIsNaN(term)
    assert str(bound_is_nan) == f"BoundIsNaN(term={str(term)})"
    assert repr(bound_is_nan) == f"BoundIsNaN(term={repr(term)})"
    assert bound_is_nan == eval(repr(bound_is_nan))
    assert bound_is_nan == pickle.loads(pickle.dumps(bound_is_nan))


def test_bound_is_not_nan(accessor: Accessor) -> None:
    # We need a FloatType here
    term = BoundReference(
        field=NestedField(field_id=1, name="foo", field_type=FloatType(), required=False),
        accessor=accessor,
    )
    bound_not_nan = BoundNotNaN(term)
    assert str(bound_not_nan) == f"BoundNotNaN(term={str(term)})"
    assert repr(bound_not_nan) == f"BoundNotNaN(term={repr(term)})"
    assert bound_not_nan == eval(repr(bound_not_nan))
    assert bound_not_nan == pickle.loads(pickle.dumps(bound_not_nan))


def test_is_nan() -> None:
    ref = Reference("a")
    is_nan = IsNaN(ref)
    assert str(is_nan) == f"IsNaN(term={str(ref)})"
    assert repr(is_nan) == f"IsNaN(term={repr(ref)})"
    assert is_nan == eval(repr(is_nan))
    assert is_nan == pickle.loads(pickle.dumps(is_nan))
    json_repr = '{"term":"a","type":"is-nan"}'
    assert is_nan.model_dump_json() == json_repr
    assert BooleanExpression.model_validate_json(json_repr) == is_nan


def test_not_nan() -> None:
    ref = Reference("a")
    not_nan = NotNaN(ref)
    assert str(not_nan) == f"NotNaN(term={str(ref)})"
    assert repr(not_nan) == f"NotNaN(term={repr(ref)})"
    assert not_nan == eval(repr(not_nan))
    assert not_nan == pickle.loads(pickle.dumps(not_nan))
    json_repr = '{"term":"a","type":"not-nan"}'
    assert not_nan.model_dump_json() == json_repr
    assert BooleanExpression.model_validate_json(json_repr) == not_nan


def test_bound_in(term: BoundReference) -> None:
    bound_in = BoundIn(term, {literal("a"), literal("b"), literal("c")})
    assert str(bound_in) == f"BoundIn({str(term)}, {{a, b, c}})"
    assert repr(bound_in) == f"BoundIn({repr(term)}, {{literal('a'), literal('b'), literal('c')}})"
    assert bound_in == eval(repr(bound_in))
    assert bound_in == pickle.loads(pickle.dumps(bound_in))


def test_bound_not_in(term: BoundReference) -> None:
    bound_not_in = BoundNotIn(term, {literal("a"), literal("b"), literal("c")})
    assert str(bound_not_in) == f"BoundNotIn({str(term)}, {{a, b, c}})"
    assert repr(bound_not_in) == f"BoundNotIn({repr(term)}, {{literal('a'), literal('b'), literal('c')}})"
    assert bound_not_in == eval(repr(bound_not_in))
    assert bound_not_in == pickle.loads(pickle.dumps(bound_not_in))


def test_in() -> None:
    ref = Reference("a")
    unbound_in = In(ref, ["a", "b", "c"])
    json_repr = unbound_in.model_dump_json()
    assert json_repr.startswith('{"term":"a","type":"in","values":[')
    assert BooleanExpression.model_validate_json(json_repr) == unbound_in
    assert str(unbound_in) == f"In({str(ref)}, {{a, b, c}})"
    assert repr(unbound_in) == f"In({repr(ref)}, {{literal('a'), literal('b'), literal('c')}})"
    assert unbound_in == eval(repr(unbound_in))
    assert unbound_in == pickle.loads(pickle.dumps(unbound_in))


def test_not_in() -> None:
    ref = Reference("a")
    not_in = NotIn(ref, ["a", "b", "c"])
    json_repr = not_in.model_dump_json()
    assert not_in.model_dump_json().startswith('{"term":"a","type":"not-in","values":')
    assert BooleanExpression.model_validate_json(json_repr) == not_in
    assert str(not_in) == f"NotIn({str(ref)}, {{a, b, c}})"
    assert repr(not_in) == f"NotIn({repr(ref)}, {{literal('a'), literal('b'), literal('c')}})"
    assert not_in == eval(repr(not_in))
    assert not_in == pickle.loads(pickle.dumps(not_in))


def test_bound_equal_to(term: BoundReference) -> None:
    bound_equal_to = BoundEqualTo(term, literal("a"))
    assert str(bound_equal_to) == f"BoundEqualTo(term={str(term)}, literal=literal('a'))"
    assert repr(bound_equal_to) == f"BoundEqualTo(term={repr(term)}, literal=literal('a'))"
    assert bound_equal_to == eval(repr(bound_equal_to))
    assert bound_equal_to == pickle.loads(pickle.dumps(bound_equal_to))


def test_bound_not_equal_to(term: BoundReference) -> None:
    bound_not_equal_to = BoundNotEqualTo(term, literal("a"))
    assert str(bound_not_equal_to) == f"BoundNotEqualTo(term={str(term)}, literal=literal('a'))"
    assert repr(bound_not_equal_to) == f"BoundNotEqualTo(term={repr(term)}, literal=literal('a'))"
    assert bound_not_equal_to == eval(repr(bound_not_equal_to))
    assert bound_not_equal_to == pickle.loads(pickle.dumps(bound_not_equal_to))


def test_bound_greater_than_or_equal_to(term: BoundReference) -> None:
    bound_greater_than_or_equal_to = BoundGreaterThanOrEqual(term, literal("a"))
    assert str(bound_greater_than_or_equal_to) == f"BoundGreaterThanOrEqual(term={str(term)}, literal=literal('a'))"
    assert repr(bound_greater_than_or_equal_to) == f"BoundGreaterThanOrEqual(term={repr(term)}, literal=literal('a'))"
    assert bound_greater_than_or_equal_to == eval(repr(bound_greater_than_or_equal_to))
    assert bound_greater_than_or_equal_to == pickle.loads(pickle.dumps(bound_greater_than_or_equal_to))


def test_bound_greater_than(term: BoundReference) -> None:
    bound_greater_than = BoundGreaterThan(term, literal("a"))
    assert str(bound_greater_than) == f"BoundGreaterThan(term={str(term)}, literal=literal('a'))"
    assert repr(bound_greater_than) == f"BoundGreaterThan(term={repr(term)}, literal=literal('a'))"
    assert bound_greater_than == eval(repr(bound_greater_than))
    assert bound_greater_than == pickle.loads(pickle.dumps(bound_greater_than))


def test_bound_less_than(term: BoundReference) -> None:
    bound_less_than = BoundLessThan(term, literal("a"))
    assert str(bound_less_than) == f"BoundLessThan(term={str(term)}, literal=literal('a'))"
    assert repr(bound_less_than) == f"BoundLessThan(term={repr(term)}, literal=literal('a'))"
    assert bound_less_than == eval(repr(bound_less_than))
    assert bound_less_than == pickle.loads(pickle.dumps(bound_less_than))


def test_bound_less_than_or_equal(term: BoundReference) -> None:
    bound_less_than_or_equal = BoundLessThanOrEqual(term, literal("a"))
    assert str(bound_less_than_or_equal) == f"BoundLessThanOrEqual(term={str(term)}, literal=literal('a'))"
    assert repr(bound_less_than_or_equal) == f"BoundLessThanOrEqual(term={repr(term)}, literal=literal('a'))"
    assert bound_less_than_or_equal == eval(repr(bound_less_than_or_equal))
    assert bound_less_than_or_equal == pickle.loads(pickle.dumps(bound_less_than_or_equal))


def test_equal_to() -> None:
    equal_to = EqualTo(Reference("a"), literal("a"))
    json_repr = '{"term":"a","type":"eq","value":"a"}'
    assert equal_to.model_dump_json() == json_repr
    assert BooleanExpression.model_validate_json(json_repr) == equal_to
    assert str(equal_to) == "EqualTo(term=Reference(name='a'), literal=literal('a'))"
    assert repr(equal_to) == "EqualTo(term=Reference(name='a'), literal=literal('a'))"
    assert equal_to == eval(repr(equal_to))
    assert equal_to == pickle.loads(pickle.dumps(equal_to))


def test_not_equal_to() -> None:
    not_equal_to = NotEqualTo(Reference("a"), literal("a"))
    json_repr = '{"term":"a","type":"not-eq","value":"a"}'
    assert not_equal_to.model_dump_json() == json_repr
    assert BooleanExpression.model_validate_json(json_repr) == not_equal_to
    assert str(not_equal_to) == "NotEqualTo(term=Reference(name='a'), literal=literal('a'))"
    assert repr(not_equal_to) == "NotEqualTo(term=Reference(name='a'), literal=literal('a'))"
    assert not_equal_to == eval(repr(not_equal_to))
    assert not_equal_to == pickle.loads(pickle.dumps(not_equal_to))


def test_greater_than_or_equal_to() -> None:
    greater_than_or_equal_to = GreaterThanOrEqual(Reference("a"), literal("a"))
    json_repr = '{"term":"a","type":"gt-eq","value":"a"}'
    assert greater_than_or_equal_to.model_dump_json() == json_repr
    assert BooleanExpression.model_validate_json(json_repr) == greater_than_or_equal_to
    assert str(greater_than_or_equal_to) == "GreaterThanOrEqual(term=Reference(name='a'), literal=literal('a'))"
    assert repr(greater_than_or_equal_to) == "GreaterThanOrEqual(term=Reference(name='a'), literal=literal('a'))"
    assert greater_than_or_equal_to == eval(repr(greater_than_or_equal_to))
    assert greater_than_or_equal_to == pickle.loads(pickle.dumps(greater_than_or_equal_to))


def test_greater_than() -> None:
    greater_than = GreaterThan(Reference("a"), literal("a"))
    json_repr = '{"term":"a","type":"gt","value":"a"}'
    assert greater_than.model_dump_json() == json_repr
    assert BooleanExpression.model_validate_json(json_repr) == greater_than
    assert str(greater_than) == "GreaterThan(term=Reference(name='a'), literal=literal('a'))"
    assert repr(greater_than) == "GreaterThan(term=Reference(name='a'), literal=literal('a'))"
    assert greater_than == eval(repr(greater_than))
    assert greater_than == pickle.loads(pickle.dumps(greater_than))


def test_less_than() -> None:
    less_than = LessThan(Reference("a"), literal("a"))
    json_repr = '{"term":"a","type":"lt","value":"a"}'
    assert less_than.model_dump_json() == json_repr
    assert BooleanExpression.model_validate_json(json_repr) == less_than
    assert str(less_than) == "LessThan(term=Reference(name='a'), literal=literal('a'))"
    assert repr(less_than) == "LessThan(term=Reference(name='a'), literal=literal('a'))"
    assert less_than == eval(repr(less_than))
    assert less_than == pickle.loads(pickle.dumps(less_than))


def test_less_than_or_equal() -> None:
    less_than_or_equal = LessThanOrEqual(Reference("a"), literal("a"))
    json_repr = '{"term":"a","type":"lt-eq","value":"a"}'
    assert less_than_or_equal.model_dump_json() == json_repr
    assert BooleanExpression.model_validate_json(json_repr) == less_than_or_equal
    assert str(less_than_or_equal) == "LessThanOrEqual(term=Reference(name='a'), literal=literal('a'))"
    assert repr(less_than_or_equal) == "LessThanOrEqual(term=Reference(name='a'), literal=literal('a'))"
    assert less_than_or_equal == eval(repr(less_than_or_equal))
    assert less_than_or_equal == pickle.loads(pickle.dumps(less_than_or_equal))


def test_starts_with() -> None:
    starts_with = StartsWith(Reference("a"), literal("a"))
    json_repr = '{"term":"a","type":"starts-with","value":"a"}'
    assert starts_with.model_dump_json() == json_repr
    assert BooleanExpression.model_validate_json(json_repr) == starts_with


def test_not_starts_with() -> None:
    not_starts_with = NotStartsWith(Reference("a"), literal("a"))
    json_repr = '{"term":"a","type":"not-starts-with","value":"a"}'
    assert not_starts_with.model_dump_json() == json_repr
    assert BooleanExpression.model_validate_json(json_repr) == not_starts_with


def test_bound_reference_eval(table_schema_simple: Schema) -> None:
    """Test creating a BoundReference and evaluating it on a StructProtocol"""
    struct = Record("foovalue", 123, True)

    position1_accessor = Accessor(position=0)
    position2_accessor = Accessor(position=1)
    position3_accessor = Accessor(position=2)

    field1 = table_schema_simple.find_field(1)
    field2 = table_schema_simple.find_field(2)
    field3 = table_schema_simple.find_field(3)

    bound_ref1 = BoundReference(field=field1, accessor=position1_accessor)
    bound_ref2 = BoundReference(field=field2, accessor=position2_accessor)
    bound_ref3 = BoundReference(field=field3, accessor=position3_accessor)

    assert bound_ref1.eval(struct) == "foovalue"
    assert bound_ref2.eval(struct) == 123
    assert bound_ref3.eval(struct) is True


def test_non_primitive_from_byte_buffer() -> None:
    with pytest.raises(ValueError) as exc_info:
        _ = _from_byte_buffer(ListType(element_id=1, element_type=StringType()), b"\0x00")

    assert str(exc_info.value) == "Expected a PrimitiveType, got: <class 'pyiceberg.types.ListType'>"


def test_string_argument_unbound_unary() -> None:
    assert IsNull("a") == IsNull(Reference("a"))


def test_string_argument_unbound_literal() -> None:
    assert EqualTo("a", "b") == EqualTo(Reference("a"), "b")


def test_string_argument_unbound_set() -> None:
    assert In("a", {"b", "c"}) == In(Reference("a"), {"b", "c"})


@pytest.fixture
def int_schema() -> Schema:
    return Schema(NestedField(field_id=1, name="a", field_type=IntegerType(), required=False))


@pytest.fixture
def above_int_max() -> Literal[int]:
    return literal(IntegerType.max + 1)


@pytest.fixture
def below_int_min() -> Literal[int]:
    return literal(IntegerType.min - 1)


def test_above_int_bounds_equal_to(int_schema: Schema, above_int_max: Literal[int], below_int_min: Literal[int]) -> None:
    assert EqualTo("a", above_int_max).bind(int_schema) is AlwaysFalse()
    assert EqualTo("a", below_int_min).bind(int_schema) is AlwaysFalse()


def test_above_int_bounds_not_equal_to(int_schema: Schema, above_int_max: Literal[int], below_int_min: Literal[int]) -> None:
    assert NotEqualTo("a", above_int_max).bind(int_schema) is AlwaysTrue()
    assert NotEqualTo("a", below_int_min).bind(int_schema) is AlwaysTrue()


def test_above_int_bounds_less_than(int_schema: Schema, above_int_max: Literal[int], below_int_min: Literal[int]) -> None:
    assert LessThan("a", above_int_max).bind(int_schema) is AlwaysTrue()
    assert LessThan("a", below_int_min).bind(int_schema) is AlwaysFalse()


def test_above_int_bounds_less_than_or_equal(
    int_schema: Schema, above_int_max: Literal[int], below_int_min: Literal[int]
) -> None:
    assert LessThanOrEqual("a", above_int_max).bind(int_schema) is AlwaysTrue()
    assert LessThanOrEqual("a", below_int_min).bind(int_schema) is AlwaysFalse()


def test_above_int_bounds_greater_than(int_schema: Schema, above_int_max: Literal[int], below_int_min: Literal[int]) -> None:
    assert GreaterThan("a", above_int_max).bind(int_schema) is AlwaysFalse()
    assert GreaterThan("a", below_int_min).bind(int_schema) is AlwaysTrue()


def test_above_int_bounds_greater_than_or_equal(
    int_schema: Schema, above_int_max: Literal[int], below_int_min: Literal[int]
) -> None:
    assert GreaterThanOrEqual("a", above_int_max).bind(int_schema) is AlwaysFalse()
    assert GreaterThanOrEqual("a", below_int_min).bind(int_schema) is AlwaysTrue()


@pytest.fixture
def float_schema() -> Schema:
    return Schema(NestedField(field_id=1, name="a", field_type=FloatType(), required=False))


@pytest.fixture
def above_float_max() -> Literal[float]:
    return literal(FloatType.max * 2)


@pytest.fixture
def below_float_min() -> Literal[float]:
    return literal(FloatType.min * 2)


def test_above_float_bounds_equal_to(
    float_schema: Schema, above_float_max: Literal[float], below_float_min: Literal[float]
) -> None:
    assert EqualTo("a", above_float_max).bind(float_schema) is AlwaysFalse()
    assert EqualTo("a", below_float_min).bind(float_schema) is AlwaysFalse()


def test_above_float_bounds_not_equal_to(
    float_schema: Schema, above_float_max: Literal[float], below_float_min: Literal[float]
) -> None:
    assert NotEqualTo("a", above_float_max).bind(float_schema) is AlwaysTrue()
    assert NotEqualTo("a", below_float_min).bind(float_schema) is AlwaysTrue()


def test_above_float_bounds_less_than(
    float_schema: Schema, above_float_max: Literal[float], below_float_min: Literal[float]
) -> None:
    assert LessThan("a", above_float_max).bind(float_schema) is AlwaysTrue()
    assert LessThan("a", below_float_min).bind(float_schema) is AlwaysFalse()


def test_above_float_bounds_less_than_or_equal(
    float_schema: Schema, above_float_max: Literal[float], below_float_min: Literal[float]
) -> None:
    assert LessThanOrEqual("a", above_float_max).bind(float_schema) is AlwaysTrue()
    assert LessThanOrEqual("a", below_float_min).bind(float_schema) is AlwaysFalse()


def test_above_float_bounds_greater_than(
    float_schema: Schema, above_float_max: Literal[float], below_float_min: Literal[float]
) -> None:
    assert GreaterThan("a", above_float_max).bind(float_schema) is AlwaysFalse()
    assert GreaterThan("a", below_float_min).bind(float_schema) is AlwaysTrue()


def test_above_float_bounds_greater_than_or_equal(
    float_schema: Schema, above_float_max: Literal[float], below_float_min: Literal[float]
) -> None:
    assert GreaterThanOrEqual("a", above_float_max).bind(float_schema) is AlwaysFalse()
    assert GreaterThanOrEqual("a", below_float_min).bind(float_schema) is AlwaysTrue()


@pytest.fixture
def long_schema() -> Schema:
    return Schema(NestedField(field_id=1, name="a", field_type=LongType(), required=False))


@pytest.fixture
def above_long_max() -> Literal[float]:
    return literal(LongType.max + 1)


@pytest.fixture
def below_long_min() -> Literal[float]:
    return literal(LongType.min - 1)


def test_above_long_bounds_equal_to(long_schema: Schema, above_long_max: Literal[int], below_long_min: Literal[int]) -> None:
    assert EqualTo("a", above_long_max).bind(long_schema) is AlwaysFalse()
    assert EqualTo("a", below_long_min).bind(long_schema) is AlwaysFalse()


def test_above_long_bounds_not_equal_to(long_schema: Schema, above_long_max: Literal[int], below_long_min: Literal[int]) -> None:
    assert NotEqualTo("a", above_long_max).bind(long_schema) is AlwaysTrue()
    assert NotEqualTo("a", below_long_min).bind(long_schema) is AlwaysTrue()


def test_above_long_bounds_less_than(long_schema: Schema, above_long_max: Literal[int], below_long_min: Literal[int]) -> None:
    assert LessThan("a", above_long_max).bind(long_schema) is AlwaysTrue()
    assert LessThan("a", below_long_min).bind(long_schema) is AlwaysFalse()


def test_above_long_bounds_less_than_or_equal(
    long_schema: Schema, above_long_max: Literal[int], below_long_min: Literal[int]
) -> None:
    assert LessThanOrEqual("a", above_long_max).bind(long_schema) is AlwaysTrue()
    assert LessThanOrEqual("a", below_long_min).bind(long_schema) is AlwaysFalse()


def test_above_long_bounds_greater_than(long_schema: Schema, above_long_max: Literal[int], below_long_min: Literal[int]) -> None:
    assert GreaterThan("a", above_long_max).bind(long_schema) is AlwaysFalse()
    assert GreaterThan("a", below_long_min).bind(long_schema) is AlwaysTrue()


def test_above_long_bounds_greater_than_or_equal(
    long_schema: Schema, above_long_max: Literal[int], below_long_min: Literal[int]
) -> None:
    assert GreaterThanOrEqual("a", above_long_max).bind(long_schema) is AlwaysFalse()
    assert GreaterThanOrEqual("a", below_long_min).bind(long_schema) is AlwaysTrue()


def test_eq_bound_expression(bound_reference_str: BoundReference) -> None:
    assert BoundEqualTo(term=bound_reference_str, literal=literal("a")) != BoundGreaterThanOrEqual(
        term=bound_reference_str, literal=literal("a")
    )
    assert BoundEqualTo(term=bound_reference_str, literal=literal("a")) == BoundEqualTo(
        term=bound_reference_str, literal=literal("a")
    )


def test_nested_bind() -> None:
    schema = Schema(NestedField(1, "foo", StructType(NestedField(2, "bar", StringType()))), schema_id=1)
    bound = BoundIsNull(BoundReference(schema.find_field(2), schema.accessor_for_field(2)))
    assert IsNull(Reference("foo.bar")).bind(schema) == bound


def test_bind_dot_name() -> None:
    schema = Schema(NestedField(1, "foo.bar", StringType()), schema_id=1)
    bound = BoundIsNull(BoundReference(schema.find_field(1), schema.accessor_for_field(1)))
    assert IsNull(Reference("foo.bar")).bind(schema) == bound


def test_nested_bind_with_dot_name() -> None:
    schema = Schema(NestedField(1, "foo.bar", StructType(NestedField(2, "baz", StringType()))), schema_id=1)
    bound = BoundIsNull(BoundReference(schema.find_field(2), schema.accessor_for_field(2)))
    assert IsNull(Reference("foo.bar.baz")).bind(schema) == bound


def test_bind_ambiguous_name() -> None:
    with pytest.raises(ValueError) as exc_info:
        Schema(
            NestedField(1, "foo", StructType(NestedField(2, "bar", StringType()))),
            NestedField(3, "foo.bar", StringType()),
            schema_id=1,
        )
    assert "Invalid schema, multiple fields for name foo.bar: 2 and 3" in str(exc_info)


#   __  __      ___
#  |  \/  |_  _| _ \_  _
#  | |\/| | || |  _/ || |
#  |_|  |_|\_, |_|  \_, |
#          |__/     |__/


def _assert_literal_predicate_type(expr: LiteralPredicate) -> None:
    assert_type(expr, LiteralPredicate)


_assert_literal_predicate_type(EqualTo("a", "b"))
_assert_literal_predicate_type(In("a", ("a", "b", "c")))
_assert_literal_predicate_type(In("a", (1, 2, 3)))
_assert_literal_predicate_type(NotIn("a", ("a", "b", "c")))
assert_type(In("a", ("a", "b", "c")), In)
assert_type(In("a", (1, 2, 3)), In)
assert_type(NotIn("a", ("a", "b", "c")), NotIn)
