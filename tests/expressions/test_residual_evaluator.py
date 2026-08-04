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
from concurrent.futures import ThreadPoolExecutor
from threading import Event
from typing import Any

import pytest

from pyiceberg.expressions import (
    AlwaysFalse,
    AlwaysTrue,
    And,
    EqualTo,
    GreaterThan,
    GreaterThanOrEqual,
    In,
    IsNaN,
    IsNull,
    LessThan,
    NotIn,
    NotNaN,
    NotNull,
    NotStartsWith,
    Or,
    StartsWith,
)
from pyiceberg.expressions.literals import literal
from pyiceberg.expressions.visitors import ResidualVisitor, residual_evaluator_of
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import DayTransform, IdentityTransform
from pyiceberg.typedef import Record
from pyiceberg.types import DoubleType, FloatType, IntegerType, NestedField, StringType, StructType, TimestampType


def test_identity_transform_residual() -> None:
    schema = Schema(NestedField(50, "dateint", IntegerType()), NestedField(51, "hour", IntegerType()))

    spec = PartitionSpec(PartitionField(50, 1050, IdentityTransform(), "dateint_part"))

    predicate = Or(
        Or(
            And(EqualTo("dateint", 20170815), LessThan("hour", 12)),
            And(LessThan("dateint", 20170815), GreaterThan("dateint", 20170801)),
        ),
        And(EqualTo("dateint", 20170801), GreaterThan("hour", 11)),
    )
    res_eval = residual_evaluator_of(spec=spec, expr=predicate, case_sensitive=True, schema=schema)

    residual = res_eval.residual_for(Record(20170815))

    # assert residual == True
    assert isinstance(residual, LessThan)
    assert residual.term.name == "hour"  # type: ignore
    # assert residual.term.field.name == 'hour'
    assert residual.literal.value == 12
    assert type(residual) is LessThan

    residual = res_eval.residual_for(Record(20170801))

    # assert isinstance(residual, UnboundPredicate)
    from pyiceberg.expressions import LiteralPredicate

    assert isinstance(residual, LiteralPredicate)
    # assert isinstance(residual, GreaterThan)
    assert residual.term.name == "hour"  # type: ignore
    # assert residual.term.
    assert residual.literal.value == 11  # type :ignore
    # assert type(residual) == BoundGreaterThan

    residual = res_eval.residual_for(Record(20170812))

    assert residual == AlwaysTrue()

    residual = res_eval.residual_for(Record(20170817))

    assert residual == AlwaysFalse()


def test_residual_evaluator_does_not_mutate_prepared_state() -> None:
    schema = Schema(NestedField(1, "a", IntegerType()), NestedField(2, "b", IntegerType()))
    spec = PartitionSpec(
        PartitionField(1, 1001, IdentityTransform(), "a_part"),
        PartitionField(2, 1002, IdentityTransform(), "b_part"),
    )
    evaluator = residual_evaluator_of(
        spec=spec,
        expr=And(EqualTo("a", 1), EqualTo("b", 1)),
        case_sensitive=True,
        schema=schema,
    )
    initial_state = vars(evaluator).copy()

    assert evaluator.residual_for(Record(1, 1)) == AlwaysTrue()
    assert evaluator.residual_for(Record(0, 0)) == AlwaysFalse()
    assert evaluator.residual_for(Record(1, 1)) == AlwaysTrue()

    assert isinstance(evaluator, ResidualVisitor)
    assert vars(evaluator) == initial_state


def test_residual_visitor_preserves_public_eval_api() -> None:
    schema = Schema(NestedField(1, "a", IntegerType()))
    spec = PartitionSpec(PartitionField(1, 1001, IdentityTransform(), "a_part"))
    visitor = ResidualVisitor(schema=schema, spec=spec, case_sensitive=True, expr=EqualTo("a", 1))
    initial_state = vars(visitor).copy()

    assert visitor.eval(Record(1)) == AlwaysTrue()
    assert visitor.eval(Record(0)) == AlwaysFalse()
    assert vars(visitor) == initial_state


def test_residual_evaluator_concurrent_calls_do_not_share_partitions() -> None:
    class BlockingRecord(Record):
        def __init__(self, first_read: Event, release_first_read: Event, *values: Any) -> None:
            super().__init__(*values)
            self.first_read = first_read
            self.release_first_read = release_first_read

        def __getitem__(self, pos: int) -> Any:
            value = super().__getitem__(pos)
            if pos == 0:
                self.first_read.set()
                if not self.release_first_read.wait(timeout=5):
                    raise TimeoutError("Timed out waiting to interleave residual evaluations")
            return value

    schema = Schema(NestedField(1, "a", IntegerType()), NestedField(2, "b", IntegerType()))
    spec = PartitionSpec(
        PartitionField(1, 1001, IdentityTransform(), "a_part"),
        PartitionField(2, 1002, IdentityTransform(), "b_part"),
    )
    evaluator = residual_evaluator_of(
        spec=spec,
        expr=And(EqualTo("a", 1), EqualTo("b", 1)),
        case_sensitive=True,
        schema=schema,
    )
    first_read = Event()
    release_first_read = Event()

    with ThreadPoolExecutor(max_workers=2) as executor:
        matching_result = executor.submit(
            evaluator.residual_for,
            BlockingRecord(first_read, release_first_read, 1, 1),
        )
        assert first_read.wait(timeout=5)

        try:
            non_matching_result = executor.submit(evaluator.residual_for, Record(0, 0)).result(timeout=5)
        finally:
            release_first_read.set()

        assert matching_result.result(timeout=5) == AlwaysTrue()
        assert non_matching_result == AlwaysFalse()


def test_partition_schema_reused_across_residuals(monkeypatch: pytest.MonkeyPatch) -> None:
    schema = Schema(NestedField(50, "dateint", IntegerType()))
    spec = PartitionSpec(PartitionField(50, 1050, IdentityTransform(), "dateint_part"))
    partition_type_calls = 0
    original_partition_type = PartitionSpec.partition_type

    def counting_partition_type(self: PartitionSpec, schema: Schema) -> StructType:
        nonlocal partition_type_calls
        partition_type_calls += 1
        return original_partition_type(self, schema)

    monkeypatch.setattr(PartitionSpec, "partition_type", counting_partition_type)

    evaluator = residual_evaluator_of(spec=spec, expr=EqualTo("dateint", 20170815), case_sensitive=True, schema=schema)

    assert evaluator.residual_for(Record(20170815)) == AlwaysTrue()
    assert evaluator.residual_for(Record(20170816)) == AlwaysFalse()
    assert partition_type_calls == 1


def test_case_insensitive_identity_transform_residuals() -> None:
    schema = Schema(NestedField(50, "dateint", IntegerType()), NestedField(51, "hour", IntegerType()))

    spec = PartitionSpec(PartitionField(50, 1050, IdentityTransform(), "dateint_part"))

    predicate = Or(
        Or(
            And(EqualTo("DATEINT", 20170815), LessThan("HOUR", 12)),
            And(LessThan("dateint", 20170815), GreaterThan("dateint", 20170801)),
        ),
        And(EqualTo("Dateint", 20170801), GreaterThan("hOUr", 11)),
    )
    res_eval = residual_evaluator_of(spec=spec, expr=predicate, case_sensitive=True, schema=schema)

    with pytest.raises(ValueError) as e:
        res_eval.residual_for(Record(20170815))
    assert "Could not find field with name DATEINT, case_sensitive=True" in str(e.value)


def test_unpartitioned_residuals() -> None:
    expressions = [
        AlwaysTrue(),
        AlwaysFalse(),
        LessThan("a", 5),
        GreaterThanOrEqual("b", 16),
        NotNull("c"),
        IsNull("d"),
        In("e", [1, 2, 3]),
        NotIn("f", [1, 2, 3]),
        NotNaN("g"),
        IsNaN("h"),
        StartsWith("data", "abcd"),
        NotStartsWith("data", "abcd"),
    ]

    schema = Schema(
        NestedField(50, "dateint", IntegerType()), NestedField(51, "hour", IntegerType()), NestedField(52, "a", IntegerType())
    )
    for expr in expressions:
        from pyiceberg.partitioning import UNPARTITIONED_PARTITION_SPEC

        residual_evaluator = residual_evaluator_of(UNPARTITIONED_PARTITION_SPEC, expr, True, schema=schema)
        assert residual_evaluator.residual_for(Record()) == expr


def test_in() -> None:
    schema = Schema(NestedField(50, "dateint", IntegerType()), NestedField(51, "hour", IntegerType()))

    spec = PartitionSpec(PartitionField(50, 1050, IdentityTransform(), "dateint_part"))

    predicate = In("dateint", [20170815, 20170816, 20170817])

    res_eval = residual_evaluator_of(spec=spec, expr=predicate, case_sensitive=True, schema=schema)

    residual = res_eval.residual_for(Record(20170815))

    assert residual == AlwaysTrue()


def test_in_timestamp() -> None:
    schema = Schema(NestedField(50, "ts", TimestampType()), NestedField(51, "hour", IntegerType()))

    spec = PartitionSpec(PartitionField(50, 1000, DayTransform(), "ts_part"))

    date_20191201 = literal("2019-12-01T00:00:00").to(TimestampType()).value
    date_20191202 = literal("2019-12-02T00:00:00").to(TimestampType()).value

    day = DayTransform().transform(TimestampType())
    ts_day = day(date_20191201)  # type: ignore

    pred = In("ts", [date_20191202, date_20191201])

    res_eval = residual_evaluator_of(spec=spec, expr=pred, case_sensitive=True, schema=schema)

    residual = res_eval.residual_for(Record(ts_day))
    assert residual == pred

    residual = res_eval.residual_for(Record(ts_day + 3))  # type: ignore
    assert residual == AlwaysFalse()


def test_not_in() -> None:
    schema = Schema(NestedField(50, "dateint", IntegerType()), NestedField(51, "hour", IntegerType()))

    spec = PartitionSpec(PartitionField(50, 1050, IdentityTransform(), "dateint_part"))

    predicate = NotIn("dateint", [20170815, 20170816, 20170817])

    res_eval = residual_evaluator_of(spec=spec, expr=predicate, case_sensitive=True, schema=schema)

    residual = res_eval.residual_for(Record(20180815))
    assert residual == AlwaysTrue()

    residual = res_eval.residual_for(Record(20170815))
    assert residual == AlwaysFalse()


def test_is_nan() -> None:
    schema = Schema(NestedField(50, "double", DoubleType()), NestedField(51, "hour", IntegerType()))

    spec = PartitionSpec(PartitionField(50, 1050, IdentityTransform(), "double_part"))

    predicate = IsNaN("double")

    res_eval = residual_evaluator_of(spec=spec, expr=predicate, case_sensitive=True, schema=schema)

    residual = res_eval.residual_for(Record(float("nan")))
    assert residual == AlwaysTrue()

    residual = res_eval.residual_for(Record(2))
    assert residual == AlwaysFalse()


def test_is_not_nan() -> None:
    schema = Schema(NestedField(50, "double", DoubleType()), NestedField(51, "float", FloatType()))

    spec = PartitionSpec(PartitionField(50, 1050, IdentityTransform(), "double_part"))

    predicate = NotNaN("double")

    res_eval = residual_evaluator_of(spec=spec, expr=predicate, case_sensitive=True, schema=schema)

    residual = res_eval.residual_for(Record(None))
    assert residual == AlwaysFalse()

    residual = res_eval.residual_for(Record(2))
    assert residual == AlwaysTrue()

    spec = PartitionSpec(PartitionField(51, 1051, IdentityTransform(), "float_part"))

    predicate = NotNaN("float")

    res_eval = residual_evaluator_of(spec=spec, expr=predicate, case_sensitive=True, schema=schema)

    residual = res_eval.residual_for(Record(None))
    assert residual == AlwaysFalse()

    residual = res_eval.residual_for(Record(2))
    assert residual == AlwaysTrue()


def test_not_in_timestamp() -> None:
    schema = Schema(NestedField(50, "ts", TimestampType()), NestedField(51, "dateint", IntegerType()))

    spec = PartitionSpec(PartitionField(50, 1000, DayTransform(), "ts_part"))

    date_20191201 = literal("2019-12-01T00:00:00").to(TimestampType()).value
    date_20191202 = literal("2019-12-02T00:00:00").to(TimestampType()).value

    day = DayTransform().transform(TimestampType())
    ts_day = day(date_20191201)  # type: ignore

    pred = NotIn("ts", [date_20191202, date_20191201])

    res_eval = residual_evaluator_of(spec=spec, expr=pred, case_sensitive=True, schema=schema)

    residual = res_eval.residual_for(Record(ts_day))
    assert residual == pred
    ts_day += 3  # type: ignore
    residual = res_eval.residual_for(Record(ts_day))
    assert residual == AlwaysTrue()


def test_starts_with() -> None:
    schema = Schema(NestedField(1, "x", StringType()))
    spec = PartitionSpec(PartitionField(1, 1001, IdentityTransform(), "x_part"))

    predicate = StartsWith("x", "a")
    res_eval = residual_evaluator_of(spec=spec, expr=predicate, case_sensitive=True, schema=schema)

    assert res_eval.residual_for(Record("bb")) == AlwaysFalse()
    assert res_eval.residual_for(Record("abc")) == AlwaysTrue()
    assert res_eval.residual_for(Record("a")) == AlwaysTrue()
    assert res_eval.residual_for(Record("zoo")) == AlwaysFalse()


def test_not_starts_with() -> None:
    schema = Schema(NestedField(1, "x", StringType()))
    spec = PartitionSpec(PartitionField(1, 1001, IdentityTransform(), "x_part"))

    predicate = NotStartsWith("x", "a")
    res_eval = residual_evaluator_of(spec=spec, expr=predicate, case_sensitive=True, schema=schema)

    assert res_eval.residual_for(Record("bb")) == AlwaysTrue()
    assert res_eval.residual_for(Record("abc")) == AlwaysFalse()
    assert res_eval.residual_for(Record("a")) == AlwaysFalse()
    assert res_eval.residual_for(Record("zoo")) == AlwaysTrue()
