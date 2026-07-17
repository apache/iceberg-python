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

"""Convert Iceberg expressions and sort directions to SQL strings.

Provides two utilities for the DataFusion backend:

1. expression_to_sql: Converts bound Iceberg BooleanExpressions to SQL WHERE
   clauses using the BoundBooleanExpressionVisitor pattern (same infrastructure
   as expression_to_pyarrow in pyiceberg/io/pyarrow.py). All 17 Iceberg predicate
   types are handled via abstract method enforcement.

2. sort_direction_to_sql: Converts Iceberg sort direction strings to SQL keywords
   (ASC/DESC). Used by DataFusion's ORDER BY clause generation.

The generated SQL is standard SQL compatible with DataFusion.
Literal values are properly escaped to prevent SQL injection.
"""

from __future__ import annotations

import datetime
from decimal import Decimal
from typing import Any
from uuid import UUID

from pyiceberg.expressions import BooleanExpression, BoundTerm
from pyiceberg.expressions.visitors import BoundBooleanExpressionVisitor, visit
from pyiceberg.typedef import LiteralValue

__all__ = ["expression_to_sql", "sort_direction_to_sql"]


def expression_to_sql(expr: BooleanExpression) -> str:
    """Convert an Iceberg BooleanExpression to a SQL WHERE clause.

    Example:
        >>> from pyiceberg.expressions import AlwaysTrue
        >>> expression_to_sql(AlwaysTrue())
        '1=1'
    """
    return visit(expr, _ConvertToSqlExpression())


def sort_direction_to_sql(direction: str) -> str:
    """Convert a sort direction string ("ascending"/"descending") to SQL ASC/DESC.

    Args:
        direction: One of "ascending" or "descending".

    Returns:
        SQL sort keyword: "ASC" or "DESC".

    Raises:
        ValueError: If direction is not one of the valid values.

    Examples:
        >>> from pyiceberg.execution.expression_to_sql import sort_direction_to_sql
        >>> sort_direction_to_sql("ascending")
        'ASC'
        >>> sort_direction_to_sql("descending")
        'DESC'
    """
    if direction == "ascending":
        return "ASC"
    elif direction == "descending":
        return "DESC"
    else:
        raise ValueError(f"Invalid sort direction: '{direction}'. Must be 'ascending' or 'descending'.")


def _escape_sql_string(value: str) -> str:
    """Escape a string literal for SQL by doubling single quotes."""
    return value.replace("'", "''")


def _escape_sql_like(value: str) -> str:
    """Escape LIKE pattern metacharacters and quotes for safe SQL embedding."""
    value = value.replace("\\", "\\\\")  # escape the escape char first
    value = value.replace("%", "\\%")
    value = value.replace("_", "\\_")
    return _escape_sql_string(value)


def _literal_to_sql(value: Any) -> str:
    """Convert a Python literal value to its SQL representation."""
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    elif isinstance(value, str):
        return f"'{_escape_sql_string(value)}'"
    elif isinstance(value, (int, float, Decimal)):
        return str(value)
    elif isinstance(value, bytes):
        return f"X'{value.hex()}'"
    elif isinstance(value, UUID):
        return f"'{value}'"
    elif isinstance(value, datetime.datetime):
        return f"TIMESTAMP '{value.isoformat()}'"
    elif isinstance(value, datetime.date):
        return f"DATE '{value.isoformat()}'"
    elif isinstance(value, datetime.time):
        return f"TIME '{value.isoformat()}'"
    elif value is None:
        return "NULL"
    else:
        return str(value)


def _quote_identifier(name: str) -> str:
    """Quote a SQL identifier with double-quotes per SQL standard."""
    escaped = name.replace('"', '""')
    return f'"{escaped}"'


def _deterministic_sort_key(value: Any) -> tuple[str, Any]:
    """Produce a deterministic sort key for reproducible IN clause ordering.

    Groups by type name first (so ints sort together, strings sort together),
    then by natural ordering within each type. Ensures SQL output is identical
    across runs for testing and cache stability.
    """
    try:
        return (type(value).__name__, value)
    except TypeError:
        return (type(value).__name__, _literal_to_sql(value))


def _unwrap_literal(literal: Any) -> Any:
    """Extract the raw Python value from an Iceberg Literal object."""
    if literal is None:
        return None
    if hasattr(literal, "value"):
        return literal.value
    return literal


class _ConvertToSqlExpression(BoundBooleanExpressionVisitor[str]):
    """Convert bound Iceberg expressions to SQL strings."""

    def _col(self, term: BoundTerm) -> str:
        """Extract and quote the column name from a bound term."""
        return _quote_identifier(term.ref().field.name)

    def visit_in(self, term: BoundTerm, literals: set[LiteralValue]) -> str:
        # NULL in SQL IN never matches; emit OR col IS NULL for IS NOT DISTINCT FROM semantics.
        unwrapped = {_unwrap_literal(lit) for lit in literals}
        non_null = {val for val in unwrapped if val is not None}
        has_null = None in unwrapped

        if non_null:
            values = ", ".join(_literal_to_sql(lit) for lit in sorted(non_null, key=_deterministic_sort_key))
            in_clause = f"{self._col(term)} IN ({values})"
            if has_null:
                return f"({in_clause} OR {self._col(term)} IS NULL)"
            return in_clause
        elif has_null:
            return f"{self._col(term)} IS NULL"
        else:
            return "1=0"  # empty set -- no matches

    def visit_not_in(self, term: BoundTerm, literals: set[LiteralValue]) -> str:
        # NOT IN with NULL returns UNKNOWN for every row; handle with IS NOT NULL.
        unwrapped = {_unwrap_literal(lit) for lit in literals}
        non_null = {val for val in unwrapped if val is not None}
        has_null = None in unwrapped

        if non_null:
            values = ", ".join(_literal_to_sql(lit) for lit in sorted(non_null, key=_deterministic_sort_key))
            not_in_clause = f"{self._col(term)} NOT IN ({values})"
            if has_null:
                return f"({not_in_clause} AND {self._col(term)} IS NOT NULL)"
            return not_in_clause
        elif has_null:
            return f"{self._col(term)} IS NOT NULL"
        else:
            return "1=1"  # empty exclusion set -- all rows match

    def visit_is_nan(self, term: BoundTerm) -> str:
        return f"isnan({self._col(term)})"

    def visit_not_nan(self, term: BoundTerm) -> str:
        return f"NOT isnan({self._col(term)})"

    def visit_is_null(self, term: BoundTerm) -> str:
        return f"{self._col(term)} IS NULL"

    def visit_not_null(self, term: BoundTerm) -> str:
        return f"{self._col(term)} IS NOT NULL"

    def visit_equal(self, term: BoundTerm, literal: LiteralValue) -> str:
        val = _unwrap_literal(literal)
        if val is None:
            return f"{self._col(term)} IS NULL"
        return f"{self._col(term)} = {_literal_to_sql(val)}"

    def visit_not_equal(self, term: BoundTerm, literal: LiteralValue) -> str:
        val = _unwrap_literal(literal)
        if val is None:
            return f"{self._col(term)} IS NOT NULL"
        return f"{self._col(term)} != {_literal_to_sql(val)}"

    def visit_greater_than_or_equal(self, term: BoundTerm, literal: LiteralValue) -> str:
        val = _unwrap_literal(literal)
        if val is None:
            return "1=0"
        return f"{self._col(term)} >= {_literal_to_sql(val)}"

    def visit_greater_than(self, term: BoundTerm, literal: LiteralValue) -> str:
        val = _unwrap_literal(literal)
        if val is None:
            return "1=0"
        return f"{self._col(term)} > {_literal_to_sql(val)}"

    def visit_less_than(self, term: BoundTerm, literal: LiteralValue) -> str:
        val = _unwrap_literal(literal)
        if val is None:
            return "1=0"
        return f"{self._col(term)} < {_literal_to_sql(val)}"

    def visit_less_than_or_equal(self, term: BoundTerm, literal: LiteralValue) -> str:
        val = _unwrap_literal(literal)
        if val is None:
            return "1=0"
        return f"{self._col(term)} <= {_literal_to_sql(val)}"

    def visit_starts_with(self, term: BoundTerm, literal: LiteralValue) -> str:
        val = _unwrap_literal(literal)
        if val is None:
            return "1=0"
        escaped = _escape_sql_like(str(val))
        return f"{self._col(term)} LIKE '{escaped}%' ESCAPE '\\'"

    def visit_not_starts_with(self, term: BoundTerm, literal: LiteralValue) -> str:
        val = _unwrap_literal(literal)
        if val is None:
            return "1=1"
        escaped = _escape_sql_like(str(val))
        return f"{self._col(term)} NOT LIKE '{escaped}%' ESCAPE '\\'"

    def visit_true(self) -> str:
        return "1=1"

    def visit_false(self) -> str:
        return "1=0"

    def visit_not(self, child_result: str) -> str:
        return f"NOT ({child_result})"

    def visit_and(self, left_result: str, right_result: str) -> str:
        return f"({left_result} AND {right_result})"

    def visit_or(self, left_result: str, right_result: str) -> str:
        return f"({left_result} OR {right_result})"
