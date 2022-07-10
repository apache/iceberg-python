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

import uuid
from decimal import Decimal
from typing import List

import pytest

from pyiceberg.expressions import base
from pyiceberg.expressions.literals import literal
from pyiceberg.types import NestedField, StringType
from pyiceberg.utils.singleton import Singleton


class ExpressionA(base.BooleanExpression, Singleton):
    def __invert__(self):
        return ExpressionB()

    def __repr__(self):
        return "ExpressionA()"

    def __str__(self):
        return "testexpra"


class ExpressionB(base.BooleanExpression, Singleton):
    def __invert__(self):
        return ExpressionA()

    def __repr__(self):
        return "ExpressionB()"

    def __str__(self):
        return "testexprb"


class BooleanExpressionVisitor(base.BooleanExpressionVisitor[List]):
    """A test implementation of a BooleanExpressionVisit

    As this visitor visits each node, it appends an element to a `visit_histor` list. This enables testing that a given expression is
    visited in an expected order by the `visit` method.
    """

    def __init__(self):
        self.visit_history: List = []

    def visit_true(self) -> List:
        self.visit_history.append("TRUE")
        return self.visit_history

    def visit_false(self) -> List:
        self.visit_history.append("FALSE")
        return self.visit_history

    def visit_not(self, child_result: List) -> List:
        self.visit_history.append("NOT")
        return self.visit_history

    def visit_and(self, left_result: List, right_result: List) -> List:
        self.visit_history.append("AND")
        return self.visit_history

    def visit_or(self, left_result: List, right_result: List) -> List:
        self.visit_history.append("OR")
        return self.visit_history

    def visit_unbound_predicate(self, predicate) -> List:
        self.visit_history.append("UNBOUND PREDICATE")
        return self.visit_history

    def visit_bound_predicate(self, predicate) -> List:
        self.visit_history.append("BOUND PREDICATE")
        return self.visit_history

    def visit_test_expression_a(self) -> List:
        self.visit_history.append("ExpressionA")
        return self.visit_history

    def visit_test_expression_b(self) -> List:
        self.visit_history.append("ExpressionB")
        return self.visit_history


@base.visit.register(ExpressionA)
def _(obj: ExpressionA, visitor: BooleanExpressionVisitor) -> List:
    """Visit a ExpressionA with a BooleanExpressionVisitor"""
    return visitor.visit_test_expression_a()


@base.visit.register(ExpressionB)
def _(obj: ExpressionB, visitor: BooleanExpressionVisitor) -> List:
    """Visit a ExpressionB with a BooleanExpressionVisitor"""
    return visitor.visit_test_expression_b()


@pytest.mark.parametrize(
    "op, rep",
    [
        (
            base.And(ExpressionA(), ExpressionB()),
            "And(ExpressionA(), ExpressionB())",
        ),
        (
            base.Or(ExpressionA(), ExpressionB()),
            "Or(ExpressionA(), ExpressionB())",
        ),
        (base.Not(ExpressionA()), "Not(ExpressionA())"),
    ],
)
def test_reprs(op, rep):
    assert repr(op) == rep


@pytest.mark.parametrize(
    "op, string",
    [
        (base.And(ExpressionA(), ExpressionB()), "(testexpra and testexprb)"),
        (base.Or(ExpressionA(), ExpressionB()), "(testexpra or testexprb)"),
        (base.Not(ExpressionA()), "(not testexpra)"),
    ],
)
def test_strs(op, string):
    assert str(op) == string


@pytest.mark.parametrize(
    "a,  schema, case_sensitive, success",
    [
        (
            base.In(base.Reference("foo"), (literal("hello"), literal("world"))),
            "table_schema_simple",
            True,
            True,
        ),
        (
            base.In(base.Reference("not_foo"), (literal("hello"), literal("world"))),
            "table_schema_simple",
            False,
            False,
        ),
        (
            base.In(base.Reference("Bar"), (literal("hello"), literal("world"))),
            "table_schema_simple",
            False,
            True,
        ),
        (
            base.In(base.Reference("Bar"), (literal("hello"), literal("world"))),
            "table_schema_simple",
            True,
            False,
        ),
    ],
)
def test_bind(a, schema, case_sensitive, success, request):
    schema = request.getfixturevalue(schema)
    if success:
        assert a.bind(schema, case_sensitive).term.field == schema.find_field(a.term.name, case_sensitive)
    else:
        with pytest.raises(ValueError):
            a.bind(schema, case_sensitive)


@pytest.mark.parametrize(
    "exp, testexpra, testexprb",
    [
        (
            base.And(ExpressionA(), ExpressionB()),
            base.And(ExpressionA(), ExpressionB()),
            base.Or(ExpressionA(), ExpressionB()),
        ),
        (
            base.Or(ExpressionA(), ExpressionB()),
            base.Or(ExpressionA(), ExpressionB()),
            base.And(ExpressionA(), ExpressionB()),
        ),
        (base.Not(ExpressionA()), base.Not(ExpressionA()), ExpressionB()),
        (ExpressionA(), ExpressionA(), ExpressionB()),
        (ExpressionB(), ExpressionB(), ExpressionA()),
        (
            base.In(base.Reference("foo"), (literal("hello"), literal("world"))),
            base.In(base.Reference("foo"), (literal("hello"), literal("world"))),
            base.In(base.Reference("not_foo"), (literal("hello"), literal("world"))),
        ),
        (
            base.In(base.Reference("foo"), (literal("hello"), literal("world"))),
            base.In(base.Reference("foo"), (literal("hello"), literal("world"))),
            base.In(base.Reference("foo"), (literal("goodbye"), literal("world"))),
        ),
    ],
)
def test_eq(exp, testexpra, testexprb):
    assert exp == testexpra and exp != testexprb


@pytest.mark.parametrize(
    "lhs, rhs, raises",
    [
        (base.And(ExpressionA(), ExpressionB()), base.Or(ExpressionB(), ExpressionA()), False),
        (base.Or(ExpressionA(), ExpressionB()), base.And(ExpressionB(), ExpressionA()), False),
        (base.Not(ExpressionA()), ExpressionA(), False),
        (base.In(base.Reference("foo"), (literal("hello"), literal("world"))), None, True),
        (ExpressionA(), ExpressionB(), False),
    ],
)
def test_negate(lhs, rhs, raises):
    if not raises:
        assert ~lhs == rhs
    else:
        with pytest.raises(TypeError):
            ~lhs  # pylint: disable=W0104


@pytest.mark.parametrize(
    "lhs, rhs",
    [
        (
            base.And(ExpressionA(), ExpressionB(), ExpressionA()),
            base.And(base.And(ExpressionA(), ExpressionB()), ExpressionA()),
        ),
        (
            base.Or(ExpressionA(), ExpressionB(), ExpressionA()),
            base.Or(base.Or(ExpressionA(), ExpressionB()), ExpressionA()),
        ),
        (base.Not(base.Not(ExpressionA())), ExpressionA()),
    ],
)
def test_reduce(lhs, rhs):
    assert lhs == rhs


@pytest.mark.parametrize(
    "lhs, rhs",
    [
        (base.And(base.AlwaysTrue(), ExpressionB()), ExpressionB()),
        (base.And(base.AlwaysFalse(), ExpressionB()), base.AlwaysFalse()),
        (base.Or(base.AlwaysTrue(), ExpressionB()), base.AlwaysTrue()),
        (base.Or(base.AlwaysFalse(), ExpressionB()), ExpressionB()),
        (base.Not(base.Not(ExpressionA())), ExpressionA()),
    ],
)
def test_base_AlwaysTrue_base_AlwaysFalse(lhs, rhs):
    assert lhs == rhs


def test_accessor_base_class(foo_struct):
    """Test retrieving a value at a position of a container using an accessor"""

    uuid_value = uuid.uuid4()

    foo_struct.set(0, "foo")
    foo_struct.set(1, "bar")
    foo_struct.set(2, "baz")
    foo_struct.set(3, 1)
    foo_struct.set(4, 2)
    foo_struct.set(5, 3)
    foo_struct.set(6, 1.234)
    foo_struct.set(7, Decimal("1.234"))
    foo_struct.set(8, uuid_value)
    foo_struct.set(9, True)
    foo_struct.set(10, False)
    foo_struct.set(11, b"\x19\x04\x9e?")

    assert base.Accessor(position=0).get(foo_struct) == "foo"
    assert base.Accessor(position=1).get(foo_struct) == "bar"
    assert base.Accessor(position=2).get(foo_struct) == "baz"
    assert base.Accessor(position=3).get(foo_struct) == 1
    assert base.Accessor(position=4).get(foo_struct) == 2
    assert base.Accessor(position=5).get(foo_struct) == 3
    assert base.Accessor(position=6).get(foo_struct) == 1.234
    assert base.Accessor(position=7).get(foo_struct) == Decimal("1.234")
    assert base.Accessor(position=8).get(foo_struct) == uuid_value
    assert base.Accessor(position=9).get(foo_struct) is True
    assert base.Accessor(position=10).get(foo_struct) is False
    assert base.Accessor(position=11).get(foo_struct) == b"\x19\x04\x9e?"


def test_bound_reference_str_and_repr():
    """Test str and repr of BoundReference"""
    field = NestedField(field_id=1, name="foo", field_type=StringType(), required=False)
    position1_accessor = base.Accessor(position=1)
    bound_ref = base.BoundReference(field=field, accessor=position1_accessor)
    assert str(bound_ref) == f"BoundReference(field={repr(field)}, accessor={repr(position1_accessor)})"
    assert repr(bound_ref) == f"BoundReference(field={repr(field)}, accessor={repr(position1_accessor)})"


def test_bound_reference_field_property():
    """Test str and repr of BoundReference"""
    field = NestedField(field_id=1, name="foo", field_type=StringType(), required=False)
    position1_accessor = base.Accessor(position=1)
    bound_ref = base.BoundReference(field=field, accessor=position1_accessor)
    assert bound_ref.field == NestedField(field_id=1, name="foo", field_type=StringType(), required=False)


def test_bound_reference(table_schema_simple, foo_struct):
    """Test creating a BoundReference and evaluating it on a StructProtocol"""
    foo_struct.set(pos=1, value="foovalue")
    foo_struct.set(pos=2, value=123)
    foo_struct.set(pos=3, value=True)

    position1_accessor = base.Accessor(position=1)
    position2_accessor = base.Accessor(position=2)
    position3_accessor = base.Accessor(position=3)

    field1 = table_schema_simple.find_field(1)
    field2 = table_schema_simple.find_field(2)
    field3 = table_schema_simple.find_field(3)

    bound_ref1 = base.BoundReference(field=field1, accessor=position1_accessor)
    bound_ref2 = base.BoundReference(field=field2, accessor=position2_accessor)
    bound_ref3 = base.BoundReference(field=field3, accessor=position3_accessor)

    assert bound_ref1.eval(foo_struct) == "foovalue"
    assert bound_ref2.eval(foo_struct) == 123
    assert bound_ref3.eval(foo_struct) is True


def test_boolean_expression_visitor():
    """Test post-order traversal of boolean expression visit method"""
    expr = base.And(
        base.Or(base.Not(ExpressionA()), base.Not(ExpressionB()), ExpressionA(), ExpressionB()),
        base.Not(ExpressionA()),
        ExpressionB(),
    )
    visitor = BooleanExpressionVisitor()
    result = base.visit(expr, visitor=visitor)
    assert result == [
        "ExpressionA",
        "NOT",
        "ExpressionB",
        "NOT",
        "OR",
        "ExpressionA",
        "OR",
        "ExpressionB",
        "OR",
        "ExpressionA",
        "NOT",
        "AND",
        "ExpressionB",
        "AND",
    ]


def test_boolean_expression_visit_raise_not_implemented_error():
    """Test raise NotImplementedError when visiting an unsupported object type"""
    visitor = BooleanExpressionVisitor()
    with pytest.raises(NotImplementedError) as exc_info:
        base.visit("foo", visitor=visitor)

    assert str(exc_info.value) == "Cannot visit unsupported expression: foo"
