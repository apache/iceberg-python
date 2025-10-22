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
from __future__ import annotations

from typing import Any

from pyiceberg.expressions import (
    BoundEqualTo,
    BoundGreaterThan,
    BoundGreaterThanOrEqual,
    BoundIn,
    BoundIsNaN,
    BoundIsNull,
    BoundLessThan,
    BoundLessThanOrEqual,
    BoundLiteralPredicate,
    BoundNotEqualTo,
    BoundNotIn,
    BoundNotNaN,
    BoundNotNull,
    BoundNotStartsWith,
    BoundPredicate,
    BoundSetPredicate,
    BoundStartsWith,
    BoundUnaryPredicate,
)
from pyiceberg.expressions.visitors import BooleanExpressionVisitor
from pyiceberg.manifest import DataFile
from pyiceberg.schema import Schema
from pyiceberg.table.bloom_filter import BloomFilter


class BloomFilterEvaluator(BooleanExpressionVisitor[bool]):
    """Evaluator that uses bloom filters to check if a file might contain matching rows.

    This evaluator helps prune data files that definitely cannot contain rows matching
    a query predicate by using bloom filters for column values.
    """

    def __init__(self, data_file: DataFile, schema: Schema):
        """Initialize the bloom filter evaluator.

        Args:
            data_file: The data file to evaluate bloom filters for.
            schema: The table schema for column resolution.
        """
        self.data_file = data_file
        self.schema = schema

    def visit_true(self) -> bool:
        """Visit AlwaysTrue - file might contain matching rows."""
        return True

    def visit_false(self) -> bool:
        """Visit AlwaysFalse - file definitely contains no matching rows."""
        return False

    def visit_not(self, child_result: bool) -> bool:
        """Visit Not - invert the child result."""
        return not child_result

    def visit_and(self, left_result: bool, right_result: bool) -> bool:
        """Visit And - both conditions must allow the file."""
        return left_result and right_result

    def visit_or(self, left_result: bool, right_result: bool) -> bool:
        """Visit Or - at least one condition must allow the file."""
        return left_result or right_result

    def visit_unbound_predicate(self, predicate: object) -> bool:
        """Visit an unbound predicate - conservatively allow the file."""
        # Unbound predicates haven't been bound to a schema, so we can't evaluate them
        return True

    def visit_bound_predicate(self, predicate: BoundPredicate[Any]) -> bool:
        """Visit a bound predicate and evaluate using bloom filter if available."""
        if isinstance(predicate, BoundUnaryPredicate):
            # Unary predicates (IsNull, IsNaN, etc.)
            return self._visit_unary_predicate(predicate)
        elif isinstance(predicate, BoundLiteralPredicate):
            # Literal predicates with a single value (EqualTo, NotEqualTo, etc.)
            return self._visit_literal_predicate(predicate)
        elif isinstance(predicate, BoundSetPredicate):
            # Set predicates (In, NotIn)
            return self._visit_set_predicate(predicate)
        else:
            # Unknown predicate type - be conservative and allow the file
            return True

    def visit_predicate(self, predicate: BoundPredicate[Any]) -> bool:
        """Visit a bound predicate and evaluate using bloom filter if available."""
        if isinstance(predicate, BoundUnaryPredicate):
            # Unary predicates (IsNull, IsNaN, etc.)
            return self._visit_unary_predicate(predicate)
        elif isinstance(predicate, BoundLiteralPredicate):
            # Literal predicates with a single value (EqualTo, NotEqualTo, etc.)
            return self._visit_literal_predicate(predicate)
        elif isinstance(predicate, BoundSetPredicate):
            # Set predicates (In, NotIn)
            return self._visit_set_predicate(predicate)
        else:
            # Unknown predicate type - be conservative and allow the file
            return True

    def _visit_unary_predicate(self, predicate: BoundUnaryPredicate[Any]) -> bool:
        """Evaluate unary predicates using bloom filter."""
        if isinstance(predicate, BoundIsNull):
            # IsNull cannot use bloom filter (nulls not in BF)
            return True
        elif isinstance(predicate, BoundIsNaN):
            # IsNaN cannot use bloom filter (NaN not in BF)
            return True
        elif isinstance(predicate, BoundNotNull):
            # NotNull cannot use bloom filter effectively
            return True
        elif isinstance(predicate, BoundNotNaN):
            # NotNaN cannot use bloom filter effectively
            return True
        else:
            # Unknown unary predicate
            return True

    def _visit_literal_predicate(self, predicate: BoundLiteralPredicate[Any]) -> bool:
        """Evaluate literal predicates using bloom filter."""
        term = predicate.term
        literal = predicate.literal
        column_id = term.ref().field.field_id

        # Get the bloom filter for this column
        bloom_filter_bytes = self.data_file.get_bloom_filter(column_id)
        if bloom_filter_bytes is None:
            # No bloom filter for this column - can't prune
            return True

        # Deserialize the bloom filter
        try:
            bloom_filter = BloomFilter.from_bytes(bloom_filter_bytes)
        except Exception:
            # Error deserializing - be conservative
            return True

        if isinstance(predicate, BoundEqualTo):
            # For EqualTo, check if value might be in the filter
            return bloom_filter.might_contain(literal.value)
        elif isinstance(predicate, BoundNotEqualTo):
            # For NotEqualTo, we can't prune based on bloom filter
            # (we need to be in the filter to exclude based on NOT)
            return True
        elif isinstance(predicate, BoundLessThan):
            # For LessThan, we can't use bloom filter effectively
            return True
        elif isinstance(predicate, BoundLessThanOrEqual):
            # For LessThanOrEqual, we can't use bloom filter effectively
            return True
        elif isinstance(predicate, BoundGreaterThan):
            # For GreaterThan, we can't use bloom filter effectively
            return True
        elif isinstance(predicate, BoundGreaterThanOrEqual):
            # For GreaterThanOrEqual, we can't use bloom filter effectively
            return True
        elif isinstance(predicate, BoundStartsWith):
            # For StartsWith, we can't use exact bloom filter matching
            return True
        elif isinstance(predicate, BoundNotStartsWith):
            # For NotStartsWith, we can't prune based on bloom filter
            return True
        else:
            # Unknown literal predicate
            return True

    def _visit_set_predicate(self, predicate: BoundSetPredicate[Any]) -> bool:
        """Evaluate set predicates using bloom filter."""
        term = predicate.term
        column_id = term.ref().field.field_id

        # Get the bloom filter for this column
        bloom_filter_bytes = self.data_file.get_bloom_filter(column_id)
        if bloom_filter_bytes is None:
            # No bloom filter for this column - can't prune
            return True

        # Deserialize the bloom filter
        try:
            bloom_filter = BloomFilter.from_bytes(bloom_filter_bytes)
        except Exception:
            # Error deserializing - be conservative
            return True

        if isinstance(predicate, BoundIn):
            # For IN predicate, check if any value might be in the filter
            # If at least one value might be in the filter, we can't prune the file
            for value in predicate.literals:
                if bloom_filter.might_contain(value.value):
                    return True
            # None of the values are in the filter - can prune the file
            return False
        elif isinstance(predicate, BoundNotIn):
            # For NOT IN predicate, we can't prune based on bloom filter
            return True
        else:
            # Unknown set predicate
            return True
