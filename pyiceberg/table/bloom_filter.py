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
"""Bloom filter support for reading from Parquet files."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import pyarrow.parquet as pq


def get_parquet_bloom_filter_for_column(parquet_file: pq.ParquetFile, column_name: str, row_group_index: int) -> Any | None:
    """Extract bloom filter for a specific column from a Parquet row group.

    Args:
        parquet_file: PyArrow ParquetFile object.
        column_name: Name of the column to get bloom filter for.
        row_group_index: Index of the row group.

    Returns:
        Bloom filter object if available, None otherwise.
    """
    try:
        # PyArrow provides access to bloom filters through the row group metadata
        row_group = parquet_file.metadata.row_group(row_group_index)

        # Find the column by name
        for i in range(row_group.num_columns):
            column = row_group.column(i)
            if column.path_in_schema == column_name:
                # Check if bloom filter is available
                if hasattr(column, "bloom_filter"):
                    return column.bloom_filter
                break

        return None
    except Exception:
        # If bloom filter reading fails, return None
        return None


def bloom_filter_might_contain(bloom_filter: Any, value: Any) -> bool:
    """Check if a Parquet bloom filter might contain a value.

    Args:
        bloom_filter: PyArrow bloom filter object.
        value: Value to check.

    Returns:
        True if value might be in the filter, False if definitely not.
    """
    if bloom_filter is None or value is None:
        return True  # Conservative: assume it might contain

    try:
        # PyArrow bloom filters have a check method
        if hasattr(bloom_filter, "check"):
            return bloom_filter.check(value)
        elif hasattr(bloom_filter, "__contains__"):
            return value in bloom_filter
        else:
            return True  # Conservative: assume it might contain
    except Exception:
        return True  # On error, be conservative
