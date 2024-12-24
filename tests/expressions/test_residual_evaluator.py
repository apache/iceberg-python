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
# pylint:disable=redefined-outer-name
import pytest
from pyiceberg.expressions import (
    AlwaysTrue,
    EqualTo,
    LessThan,
    AlwaysFalse,
    And,
    Or,
    GreaterThan,
    GreaterThanOrEqual,
    UnboundPredicate,
    BoundPredicate,
    BoundReference,
    BooleanExpression,
    BoundLessThan,
    BoundGreaterThan,
    NotNull,
    IsNull,
    In,
    NotIn,
    NotNaN,
    IsNaN,
    StartsWith,
    NotStartsWith
)
from pyiceberg.expressions.residual_evaluator import residual_evaluator_of
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import IdentityTransform, DayTransform
from pyiceberg.typedef import Record
from pyiceberg.types import (
    IntegerType,
    DoubleType,
    FloatType,
    NestedField,
    StringType,
    TimestampType
)
from pyiceberg.utils.datetime import timestamp_to_micros
from pyiceberg.expressions.literals import literal


def test_identity_transform_residual():

    schema = Schema(
        NestedField(50, "dateint", IntegerType()),
        NestedField(51, "hour", IntegerType())
    )

    spec = PartitionSpec(
        PartitionField(50, 1050, IdentityTransform(), "dateint_part")
    )

    predicate = Or(
        Or(
            And(EqualTo("dateint", 20170815), LessThan("hour", 12)),
            And(LessThan("dateint", 20170815), GreaterThan("dateint", 20170801))
        ),
        And(EqualTo("dateint", 20170801), GreaterThan("hour", 11))
    )
    res_eval = residual_evaluator_of(spec=spec,expr=predicate, case_sensitive=True, schema=schema)

    residual = res_eval.residual_for(Record(dateint=20170815))

    # assert residual == True
    assert isinstance(residual, UnboundPredicate)
    assert residual.term.name == 'hour'
    # assert residual.term.field.name == 'hour'
    assert residual.literal.value == 12
    assert type(residual) == LessThan

    residual = res_eval.residual_for(Record(dateint=20170801))

    assert isinstance(residual, UnboundPredicate)
    assert residual.term.name == 'hour'
    assert residual.literal.value == 11
    assert type(residual) == GreaterThan

    residual = res_eval.residual_for(Record(dateint=20170812))

    assert residual == AlwaysTrue()

    residual = res_eval.residual_for(Record(dateint=20170817))

    assert residual == AlwaysFalse()


def test_case_insensitive_identity_transform_residuals():

    schema = Schema(
        NestedField(50, "dateint", IntegerType()),
        NestedField(51, "hour", IntegerType())
    )

    spec = PartitionSpec(
        PartitionField(50, 1050, IdentityTransform(), "dateint_part")
    )

    predicate = Or(
        Or(
            And(EqualTo("DATEINT", 20170815), LessThan("HOUR", 12)),
            And(LessThan("dateint", 20170815), GreaterThan("dateint", 20170801))
        ),
        And(EqualTo("Dateint", 20170801), GreaterThan("hOUr", 11))
    )
    res_eval = residual_evaluator_of(spec=spec,expr=predicate, case_sensitive=True, schema=schema)


    with pytest.raises(ValueError) as e:
        residual = res_eval.residual_for(Record(dateint=20170815))
    assert "Could not find field with name DATEINT, case_sensitive=True" in str(e.value)


def test_unpartitioned_residuals():


    expressions = [
        AlwaysTrue(),
        AlwaysFalse(),
        LessThan("a", 5),
        GreaterThanOrEqual("b", 16),
        NotNull("c"),
        IsNull("d"),
        In("e",[1, 2, 3]),
        NotIn("f", [1, 2, 3]),
        NotNaN("g"),
        IsNaN("h"),
        StartsWith("data", "abcd"),
        NotStartsWith("data", "abcd")
    ]

    schema = Schema(
        NestedField(50, "dateint", IntegerType()),
        NestedField(51, "hour", IntegerType()),
        NestedField(52, "a", IntegerType())
    )
    for expr in expressions:
        from pyiceberg.partitioning import UNPARTITIONED_PARTITION_SPEC
        residual_evaluator = residual_evaluator_of(
            UNPARTITIONED_PARTITION_SPEC, expr, True, schema=schema
        )
        assert residual_evaluator.residual_for(Record()) == expr


def test_in():

    schema = Schema(
        NestedField(50, "dateint", IntegerType()),
        NestedField(51, "hour", IntegerType())
    )

    spec = PartitionSpec(
        PartitionField(50, 1050, IdentityTransform(), "dateint_part")
    )

    predicate = In("dateint", [20170815, 20170816, 20170817])

    res_eval = residual_evaluator_of(spec=spec,expr=predicate, case_sensitive=True, schema=schema)

    residual = res_eval.residual_for(Record(dateint=20170815))

    assert residual == AlwaysTrue()


def test_in_timestamp():

    schema = Schema(
        NestedField(50, "ts", TimestampType()),
        NestedField(51, "hour", IntegerType())
    )


    spec = PartitionSpec(
        PartitionField(50, 1000, DayTransform(), "ts_part")
    )

    date_20191201 = literal("2019-12-01T00:00:00").to(TimestampType()).value
    date_20191202 = literal("2019-12-02T00:00:00").to(TimestampType()).value

    day = DayTransform().transform(TimestampType())
    # assert date_20191201 == True
    ts_day = day(date_20191201)

    # assert ts_day == True

    pred  = In("ts", [ date_20191202, date_20191201])

    res_eval = residual_evaluator_of(spec=spec, expr=pred, case_sensitive=True, schema=schema)

    residual = res_eval.residual_for(Record(ts_day))
    assert residual == pred

    residual = res_eval.residual_for(Record(ts_day+3))
    assert residual == AlwaysFalse()


def test_not_in():

    schema = Schema(
        NestedField(50, "dateint", IntegerType()),
        NestedField(51, "hour", IntegerType())
    )

    spec = PartitionSpec(
        PartitionField(50, 1050, IdentityTransform(), "dateint_part")
    )

    predicate = NotIn("dateint", [20170815, 20170816, 20170817])

    res_eval = residual_evaluator_of(spec=spec,expr=predicate, case_sensitive=True, schema=schema)

    residual = res_eval.residual_for(Record(dateint=20180815))
    assert residual == AlwaysTrue()

    residual = res_eval.residual_for(Record(dateint=20170815))
    assert residual == AlwaysFalse()


def test_is_nan():
    schema = Schema(
        NestedField(50, "double", DoubleType()),
        NestedField(51, "hour", IntegerType())
    )

    spec = PartitionSpec(
        PartitionField(50, 1050, IdentityTransform(), "double_part")
    )

    predicate = IsNaN("double")

    res_eval = residual_evaluator_of(spec=spec,expr=predicate, case_sensitive=True, schema=schema)

    residual = res_eval.residual_for(Record(double=None))
    assert residual == AlwaysTrue()

    residual = res_eval.residual_for(Record(double=2))
    assert residual == AlwaysFalse()


def test_is_not_nan():
    schema = Schema(
        NestedField(50, "double", DoubleType()),
        NestedField(51, "float", FloatType())
    )

    spec = PartitionSpec(
        PartitionField(50, 1050, IdentityTransform(), "double_part")
    )

    predicate = NotNaN("double")

    res_eval = residual_evaluator_of(spec=spec,expr=predicate, case_sensitive=True, schema=schema)

    residual = res_eval.residual_for(Record(double=None))
    assert residual == AlwaysFalse()


    residual = res_eval.residual_for(Record(double=2))
    assert residual == AlwaysTrue()


    spec = PartitionSpec(
        PartitionField(51, 1051, IdentityTransform(), "float_part")
    )

    predicate = NotNaN("float")

    res_eval = residual_evaluator_of(spec=spec,expr=predicate, case_sensitive=True, schema=schema)

    residual = res_eval.residual_for(Record(double=None))
    assert residual == AlwaysFalse()

    residual = res_eval.residual_for(Record(double=2))
    assert residual == AlwaysTrue()


def test_not_in_timestamp():

    schema = Schema(
        NestedField(50, "ts", TimestampType()),
        NestedField(51, "dateint", IntegerType())
    )


    spec = PartitionSpec(
        PartitionField(50, 1000, DayTransform(), "ts_part")
    )

    date_20191201 = literal("2019-12-01T00:00:00").to(TimestampType()).value
    date_20191202 = literal("2019-12-02T00:00:00").to(TimestampType()).value

    day = DayTransform().transform(TimestampType())
    # assert date_20191201 == True
    ts_day = day(date_20191201)

    # assert ts_day == True

    pred  = NotIn("ts", [ date_20191202, date_20191201])

    res_eval = residual_evaluator_of(spec=spec, expr=pred, case_sensitive=True, schema=schema)

    residual = res_eval.residual_for(Record(ts_day))
    assert residual == pred

    residual = res_eval.residual_for(Record(ts_day+3))
    assert residual == AlwaysTrue()