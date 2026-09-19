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

"""Deprecated: upsert helpers have moved to pyiceberg.io.pyarrow.

All functions in this module are re-exported from ``pyiceberg.io.pyarrow``
and will emit a ``DeprecationWarning`` when called. Import directly from
``pyiceberg.io.pyarrow`` instead.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from pyiceberg.expressions import BooleanExpression
from pyiceberg.io.pyarrow import (
    upsert_create_match_filter,
    upsert_get_rows_to_update,
    upsert_has_duplicate_rows,
)
from pyiceberg.utils.deprecated import deprecated

if TYPE_CHECKING:
    import pyarrow as pa

_DEPRECATION_IN = "0.13.0"
_REMOVAL_IN = "0.14.0"
_HELP = "Use the equivalent function from pyiceberg.io.pyarrow instead"


@deprecated(deprecated_in=_DEPRECATION_IN, removed_in=_REMOVAL_IN, help_message=_HELP)
def create_match_filter(df: pa.Table, join_cols: list[str]) -> BooleanExpression:
    """Build an Iceberg filter expression matching the unique keys in df."""
    return upsert_create_match_filter(df, join_cols)


@deprecated(deprecated_in=_DEPRECATION_IN, removed_in=_REMOVAL_IN, help_message=_HELP)
def has_duplicate_rows(df: pa.Table, join_cols: list[str]) -> bool:
    """Check for duplicate rows in a table based on the join columns."""
    return upsert_has_duplicate_rows(df, join_cols)


@deprecated(deprecated_in=_DEPRECATION_IN, removed_in=_REMOVAL_IN, help_message=_HELP)
def get_rows_to_update(source_table: pa.Table, target_table: pa.Table, join_cols: list[str]) -> pa.Table:
    """Return rows from source that need to be updated in the target table."""
    return upsert_get_rows_to_update(source_table, target_table, join_cols)
