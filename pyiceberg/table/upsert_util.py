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
import functools
import operator
from typing import TYPE_CHECKING

from pyiceberg.expressions import (
    AlwaysFalse,
    BooleanExpression,
    EqualTo,
    In,
    Or,
)
from pyiceberg.io.pyarrow import (
    upsert_get_rows_to_update,
    upsert_has_duplicate_rows,
    upsert_unique_keys,
)

if TYPE_CHECKING:
    import pyarrow as pa


def create_match_filter(df: "pa.Table", join_cols: list[str]) -> BooleanExpression:
    """Build an Iceberg filter expression matching the unique keys in df."""
    unique_keys = upsert_unique_keys(df, join_cols)

    if len(join_cols) == 1:
        return In(join_cols[0], unique_keys[0].to_pylist())
    else:
        filters = [
            functools.reduce(operator.and_, [EqualTo(col, row[col]) for col in join_cols]) for row in unique_keys.to_pylist()
        ]

        if len(filters) == 0:
            return AlwaysFalse()
        elif len(filters) == 1:
            return filters[0]
        else:
            return Or(*filters)


def has_duplicate_rows(df: "pa.Table", join_cols: list[str]) -> bool:
    """Check for duplicate rows in a table based on the join columns."""
    return upsert_has_duplicate_rows(df, join_cols)


def get_rows_to_update(source_table: "pa.Table", target_table: "pa.Table", join_cols: list[str]) -> "pa.Table":
    """Return rows from source that need to be updated in the target table based on the join columns.

    The table is joined on the identifier columns, and then checked if there are any updated rows.
    Those are selected and everything is renamed correctly.
    """
    return upsert_get_rows_to_update(source_table, target_table, join_cols)
