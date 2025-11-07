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
"""Tests for bloom filter utility functions."""

from unittest.mock import MagicMock

from pyiceberg.table.bloom_filter import bloom_filter_might_contain, get_parquet_bloom_filter_for_column


class TestBloomFilterUtilities:
    """Test cases for Parquet bloom filter reading utilities."""

    def test_get_parquet_bloom_filter_returns_none_when_not_available(self) -> None:
        """Test that getting a bloom filter returns None when not available."""
        # Mock a ParquetFile without bloom filters
        mock_parquet_file = MagicMock()
        mock_row_group = MagicMock()
        mock_column = MagicMock()
        mock_column.path_in_schema = "test_column"
        del mock_column.bloom_filter  # Ensure bloom_filter attribute doesn't exist

        mock_row_group.num_columns = 1
        mock_row_group.column.return_value = mock_column
        mock_parquet_file.metadata.row_group.return_value = mock_row_group

        result = get_parquet_bloom_filter_for_column(mock_parquet_file, "test_column", 0)
        assert result is None

    def test_bloom_filter_might_contain_returns_true_when_filter_is_none(self) -> None:
        """Test that might_contain returns True conservatively when filter is None."""
        result = bloom_filter_might_contain(None, "test_value")
        assert result is True

    def test_bloom_filter_might_contain_returns_true_when_value_is_none(self) -> None:
        """Test that might_contain returns True conservatively when value is None."""
        mock_filter = MagicMock()
        result = bloom_filter_might_contain(mock_filter, None)
        assert result is True

    def test_bloom_filter_might_contain_uses_check_method(self) -> None:
        """Test that might_contain uses the check method if available."""
        mock_filter = MagicMock()
        mock_filter.check.return_value = True

        result = bloom_filter_might_contain(mock_filter, "test_value")
        assert result is True
        mock_filter.check.assert_called_once_with("test_value")

    def test_bloom_filter_might_contain_uses_contains_method(self) -> None:
        """Test that might_contain uses __contains__ if check is not available."""
        mock_filter = MagicMock()
        del mock_filter.check  # Remove check method
        mock_filter.__contains__.return_value = True

        result = bloom_filter_might_contain(mock_filter, "test_value")
        assert result is True

    def test_bloom_filter_might_contain_returns_true_on_exception(self) -> None:
        """Test that might_contain returns True conservatively on exception."""
        mock_filter = MagicMock()
        mock_filter.check.side_effect = Exception("Test error")

        result = bloom_filter_might_contain(mock_filter, "test_value")
        assert result is True
