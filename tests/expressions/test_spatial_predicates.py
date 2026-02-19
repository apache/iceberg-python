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
import struct

import pytest

from pyiceberg.expressions import BooleanExpression, BoundSTContains, Not, Reference, STContains, STIntersects
from pyiceberg.expressions.visitors import bind, expression_evaluator, translate_column_names
from pyiceberg.schema import Schema
from pyiceberg.typedef import Record
from pyiceberg.types import GeometryType, IntegerType, NestedField


def _point_wkb(x: float, y: float) -> bytes:
    return struct.pack("<BIdd", 1, 1, x, y)


def test_st_contains_bind() -> None:
    schema = Schema(NestedField(1, "geom", GeometryType(), required=False), schema_id=1)
    expr = STContains("geom", _point_wkb(1.0, 2.0))
    bound = bind(schema, expr, case_sensitive=True)
    assert isinstance(bound, BoundSTContains)
    assert bound.geometry == _point_wkb(1.0, 2.0)


def test_st_contains_bind_fails_for_non_geospatial_field() -> None:
    schema = Schema(NestedField(1, "id", IntegerType(), required=False), schema_id=1)
    with pytest.raises(TypeError) as exc_info:
        bind(schema, STContains("id", _point_wkb(1.0, 2.0)), case_sensitive=True)
    assert "geometry/geography" in str(exc_info.value)


def test_spatial_predicate_json_parsing() -> None:
    expr = BooleanExpression.model_validate({"type": "st-intersects", "term": "geom", "value": _point_wkb(1.0, 2.0)})
    assert isinstance(expr, STIntersects)
    assert expr.geometry == _point_wkb(1.0, 2.0)


def test_spatial_predicate_invert_returns_not() -> None:
    expr = STContains("geom", _point_wkb(1.0, 2.0))
    assert isinstance(~expr, Not)


def test_spatial_expression_evaluator_not_implemented() -> None:
    schema = Schema(NestedField(1, "geom", GeometryType(), required=False), schema_id=1)
    evaluator = expression_evaluator(schema, STContains("geom", _point_wkb(1.0, 2.0)), case_sensitive=True)
    with pytest.raises(NotImplementedError) as exc_info:
        evaluator(Record(_point_wkb(1.0, 2.0)))
    assert "st-contains row-level evaluation is not implemented" in str(exc_info.value)


def test_translate_column_names_for_spatial_predicate() -> None:
    original_schema = Schema(NestedField(1, "geom_original", GeometryType(), required=False), schema_id=1)
    file_schema = Schema(NestedField(1, "geom_file", GeometryType(), required=False), schema_id=1)

    bound_expr = bind(original_schema, STContains("geom_original", _point_wkb(1.0, 2.0)), case_sensitive=True)
    translated_expr = translate_column_names(bound_expr, file_schema, case_sensitive=True)

    assert isinstance(translated_expr, STContains)
    assert translated_expr.term == Reference("geom_file")
    assert translated_expr.geometry == _point_wkb(1.0, 2.0)


def test_translate_column_names_for_spatial_predicate_missing_column_raises() -> None:
    original_schema = Schema(NestedField(1, "geom", GeometryType(), required=False), schema_id=1)
    file_schema = Schema(NestedField(2, "other_col", IntegerType(), required=False), schema_id=1)
    bound_expr = bind(original_schema, STContains("geom", _point_wkb(1.0, 2.0)), case_sensitive=True)

    with pytest.raises(NotImplementedError) as exc_info:
        translate_column_names(bound_expr, file_schema, case_sensitive=True)
    assert "Spatial predicate translation is not supported when source columns are missing" in str(exc_info.value)
