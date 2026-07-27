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


"""Tests for expression_to_sql: IN with NULL, deep nesting, and literal-to-SQL for all types."""

from __future__ import annotations

import sys
from unittest.mock import MagicMock

from pyiceberg.expressions import EqualTo
from pyiceberg.schema import Schema
from pyiceberg.types import IntegerType, NestedField


def _try_import_datafusion() -> bool:
    """Check if datafusion is importable (for skipif decorators)."""
    try:
        import datafusion  # noqa: F401

        return True
    except ImportError:
        return False


# =============================================================================
# Schema type promotion (string → large_string)
# =============================================================================


class TestExpressionToSqlInWithNull:
    """T3: Verify expression_to_sql handles IN predicates with NULL values.

    Per Iceberg spec, equality deletes use IS NOT DISTINCT FROM semantics:
    NULL in the delete set matches NULL in the data. The SQL generation for
    IN predicates must produce: (col IN (non_null_vals) OR col IS NULL)
    when the literal set contains NULL.

    NOTE: The Iceberg expression API (In("col", {None})) does not allow None in
    literal sets at construction time. NULL in BoundIn.literals arises only via
    internal paths (e.g., during expression transformation/rewriting). These tests
    exercise the SQL visitor directly to validate the NULL-handling branches.
    """

    def test_visit_in_with_null_produces_or_is_null(self) -> None:
        """visit_in with {1, 2, None} produces: ("id" IN (1, 2) OR "id" IS NULL)."""
        from pyiceberg.execution.expression_to_sql import _ConvertToSqlExpression

        visitor = _ConvertToSqlExpression()

        # Create a mock BoundTerm that returns field name "id"
        mock_term = MagicMock()
        mock_field = MagicMock()
        mock_field.name = "id"
        mock_term.ref.return_value = MagicMock(field=mock_field)

        # Call visit_in directly with a set containing None
        sql = visitor.visit_in(mock_term, {1, 2, None})

        # Must contain IS NULL (for the NULL in the set)
        assert "IS NULL" in sql, f"IN with NULL should produce 'OR col IS NULL' clause, got: {sql}"
        # Must contain IN (...) for the non-NULL values
        assert "IN" in sql, f"Expected IN clause, got: {sql}"
        # Must be an OR combination
        assert "OR" in sql, f"Expected OR for NULL handling, got: {sql}"

    def test_visit_in_without_null_does_not_produce_is_null(self) -> None:
        """visit_in with {1, 2, 3} (no NULL) produces plain IN without IS NULL."""
        from pyiceberg.execution.expression_to_sql import expression_to_sql
        from pyiceberg.expressions import In
        from pyiceberg.expressions.visitors import bind

        schema = Schema(
            NestedField(field_id=1, name="id", field_type=IntegerType(), required=False),
        )

        in_expr = In("id", {1, 2, 3})
        bound_expr = bind(schema, in_expr, case_sensitive=True)

        sql = expression_to_sql(bound_expr)

        # Should NOT contain IS NULL
        assert "IS NULL" not in sql, f"IN without NULL should not produce IS NULL clause, got: {sql}"
        assert "IN" in sql

    def test_visit_in_with_only_null_produces_is_null(self) -> None:
        """visit_in with {None} produces just: "id" IS NULL."""
        from pyiceberg.execution.expression_to_sql import _ConvertToSqlExpression

        visitor = _ConvertToSqlExpression()

        mock_term = MagicMock()
        mock_field = MagicMock()
        mock_field.name = "id"
        mock_term.ref.return_value = MagicMock(field=mock_field)

        # Call visit_in directly with only None
        sql = visitor.visit_in(mock_term, {None})

        # Should produce IS NULL without an IN clause
        assert "IS NULL" in sql, f"IN with only NULL should produce IS NULL, got: {sql}"
        # Should NOT have "IN (" since there are no non-null values
        assert "IN (" not in sql, f"IN with only NULL should not have IN clause, got: {sql}"

    def test_visit_not_in_with_null_produces_is_not_null(self) -> None:
        """visit_not_in with {2, None} produces: ("id" NOT IN (2) AND "id" IS NOT NULL)."""
        from pyiceberg.execution.expression_to_sql import _ConvertToSqlExpression

        visitor = _ConvertToSqlExpression()

        mock_term = MagicMock()
        mock_field = MagicMock()
        mock_field.name = "id"
        mock_term.ref.return_value = MagicMock(field=mock_field)

        sql = visitor.visit_not_in(mock_term, {2, None})

        # Must contain IS NOT NULL (for the NULL in the exclusion set)
        assert "IS NOT NULL" in sql, f"NOT IN with NULL should produce 'AND col IS NOT NULL' clause, got: {sql}"
        assert "NOT IN" in sql, f"Expected NOT IN clause, got: {sql}"
        # Must be an AND combination
        assert "AND" in sql, f"Expected AND for NULL handling, got: {sql}"


# =============================================================================
# orchestrate_scan with Empty Task Iterator
# =============================================================================


class TestExpressionToSqlDeepNesting:
    """T3: Verify expression_to_sql handles deeply nested expression trees.

    The BoundBooleanExpressionVisitor uses recursion. Deeply nested expressions
    (e.g., 100+ levels of nested AND from programmatic filter construction) could
    theoretically hit Python's recursion limit (default 1000). This tests that
    realistic nesting depths succeed, and documents the practical limit.
    """

    def test_deeply_nested_and_100_levels(self):
        """100-level nested AND tree produces valid SQL without stack overflow."""
        from pyiceberg.execution.expression_to_sql import expression_to_sql
        from pyiceberg.expressions import And
        from pyiceberg.expressions.visitors import bind

        schema = Schema(
            NestedField(field_id=1, name="id", field_type=IntegerType(), required=False),
        )

        # Build a 100-level nested AND tree:
        # (id = 1) AND ((id = 2) AND ((id = 3) AND ... ))
        expr = EqualTo("id", 1)
        for i in range(2, 101):
            expr = And(expr, EqualTo("id", i))

        bound = bind(schema, expr, case_sensitive=True)
        sql = expression_to_sql(bound)

        # Verify structure: should contain 99 AND keywords
        assert sql.count("AND") == 99, f"Expected 99 ANDs in deeply nested expression, got {sql.count('AND')}"
        # Verify all values are present
        for i in range(1, 101):
            assert str(i) in sql, f"Value {i} missing from SQL output"

    def test_deeply_nested_or_100_levels(self):
        """100-level nested OR tree produces valid SQL without stack overflow."""
        from pyiceberg.execution.expression_to_sql import expression_to_sql
        from pyiceberg.expressions import Or
        from pyiceberg.expressions.visitors import bind

        schema = Schema(
            NestedField(field_id=1, name="id", field_type=IntegerType(), required=False),
        )

        expr = EqualTo("id", 1)
        for i in range(2, 101):
            expr = Or(expr, EqualTo("id", i))

        bound = bind(schema, expr, case_sensitive=True)
        sql = expression_to_sql(bound)

        assert sql.count("OR") == 99, f"Expected 99 ORs in deeply nested expression, got {sql.count('OR')}"

    def test_mixed_and_or_50_levels(self):
        """Mixed AND/OR nesting: 50 levels alternating AND and OR."""
        from pyiceberg.execution.expression_to_sql import expression_to_sql
        from pyiceberg.expressions import And, Or
        from pyiceberg.expressions.visitors import bind

        schema = Schema(
            NestedField(field_id=1, name="id", field_type=IntegerType(), required=False),
        )

        expr = EqualTo("id", 1)
        for i in range(2, 51):
            if i % 2 == 0:
                expr = And(expr, EqualTo("id", i))
            else:
                expr = Or(expr, EqualTo("id", i))

        bound = bind(schema, expr, case_sensitive=True)
        sql = expression_to_sql(bound)

        # Should produce valid SQL with both AND and OR
        assert "AND" in sql
        assert "OR" in sql

    def test_nesting_at_recursion_boundary_500_levels(self):
        """500-level nesting tests approaching Python's default recursion limit.

        Python's default recursion limit is 1000. Each visitor level adds ~3
        stack frames (visit_and/or → left → right). At 500 expression nodes,
        we're at ~1500 frames -- this exceeds the default limit.

        Both bind() and expression_to_sql() use recursive visitors, so the limit
        applies to the full pipeline. This test documents the practical boundary
        by temporarily increasing the recursion limit, proving the logic is
        correct when the limit allows it. In production, expression trees with
        >200 levels are pathological -- normal filter pushdown never produces them.
        """
        from pyiceberg.execution.expression_to_sql import expression_to_sql
        from pyiceberg.expressions import And
        from pyiceberg.expressions.visitors import bind

        schema = Schema(
            NestedField(field_id=1, name="id", field_type=IntegerType(), required=False),
        )

        expr = EqualTo("id", 1)
        for i in range(2, 501):
            expr = And(expr, EqualTo("id", i))

        # Both bind() and expression_to_sql() are recursive visitors.
        # We must increase the recursion limit for the full pipeline to succeed.
        old_limit = sys.getrecursionlimit()
        sys.setrecursionlimit(5000)
        try:
            bound = bind(schema, expr, case_sensitive=True)
            sql = expression_to_sql(bound)
            assert sql.count("AND") == 499
        finally:
            sys.setrecursionlimit(old_limit)


class TestLiteralToSqlAllTypes:
    """Verify _literal_to_sql handles all Iceberg-supported literal types.

    Iceberg supports: bool, int, float, str, bytes, UUID, Decimal, date,
    datetime, time, and None. Each must produce valid DataFusion SQL.
    """

    def test_bool_true(self) -> None:
        from pyiceberg.execution.expression_to_sql import _literal_to_sql

        assert _literal_to_sql(True) == "TRUE"

    def test_bool_false(self) -> None:
        from pyiceberg.execution.expression_to_sql import _literal_to_sql

        assert _literal_to_sql(False) == "FALSE"

    def test_int(self) -> None:
        from pyiceberg.execution.expression_to_sql import _literal_to_sql

        assert _literal_to_sql(42) == "42"

    def test_negative_int(self) -> None:
        from pyiceberg.execution.expression_to_sql import _literal_to_sql

        assert _literal_to_sql(-7) == "-7"

    def test_float(self) -> None:
        from pyiceberg.execution.expression_to_sql import _literal_to_sql

        assert _literal_to_sql(3.14) == "3.14"

    def test_string(self) -> None:
        from pyiceberg.execution.expression_to_sql import _literal_to_sql

        assert _literal_to_sql("hello") == "'hello'"

    def test_string_with_quote(self) -> None:
        from pyiceberg.execution.expression_to_sql import _literal_to_sql

        assert _literal_to_sql("it's") == "'it''s'"

    def test_bytes(self) -> None:
        from pyiceberg.execution.expression_to_sql import _literal_to_sql

        assert _literal_to_sql(b"\x01\x02\x03") == "X'010203'"

    def test_uuid(self) -> None:
        from uuid import UUID

        from pyiceberg.execution.expression_to_sql import _literal_to_sql

        result = _literal_to_sql(UUID("12345678-1234-5678-1234-567812345678"))
        assert result == "'12345678-1234-5678-1234-567812345678'"

    def test_decimal(self) -> None:
        from decimal import Decimal

        from pyiceberg.execution.expression_to_sql import _literal_to_sql

        assert _literal_to_sql(Decimal("123.456")) == "123.456"

    def test_date(self) -> None:
        import datetime

        from pyiceberg.execution.expression_to_sql import _literal_to_sql

        assert _literal_to_sql(datetime.date(2024, 6, 15)) == "DATE '2024-06-15'"

    def test_datetime(self) -> None:
        import datetime

        from pyiceberg.execution.expression_to_sql import _literal_to_sql

        result = _literal_to_sql(datetime.datetime(2024, 6, 15, 10, 30, 0))
        assert result == "TIMESTAMP '2024-06-15T10:30:00'"

    def test_time(self) -> None:
        import datetime

        from pyiceberg.execution.expression_to_sql import _literal_to_sql

        assert _literal_to_sql(datetime.time(14, 30, 0)) == "TIME '14:30:00'"

    def test_none(self) -> None:
        from pyiceberg.execution.expression_to_sql import _literal_to_sql

        assert _literal_to_sql(None) == "NULL"


# =============================================================================
# Test Gap 1: Schema evolution during scan (projected schema has new columns)
# =============================================================================
