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
"""Tests for bloom filter functionality."""

import pytest

from pyiceberg.table.bloom_filter import BloomFilter, BloomFilterBuilder


class TestBloomFilter:
    """Test cases for BloomFilter class."""

    def test_bloom_filter_creation(self):
        """Test creating a bloom filter."""
        bf = BloomFilter(100, 3)
        assert bf.num_bytes == 100
        assert bf.num_hash_functions == 3
        assert len(bf.bit_array) == 100

    def test_add_and_might_contain_string(self):
        """Test adding and checking string values."""
        bf = BloomFilter(256, 3)

        # Add some strings
        bf.add("hello")
        bf.add("world")

        # Check that added values might be in the filter
        assert bf.might_contain("hello")
        assert bf.might_contain("world")

    def test_add_and_might_contain_integer(self):
        """Test adding and checking integer values."""
        bf = BloomFilter(256, 3)

        # Add some integers
        bf.add(42)
        bf.add(100)
        bf.add(9999)

        # Check that added values might be in the filter
        assert bf.might_contain(42)
        assert bf.might_contain(100)
        assert bf.might_contain(9999)

    def test_add_and_might_contain_float(self):
        """Test adding and checking float values."""
        bf = BloomFilter(256, 3)

        # Add some floats
        bf.add(3.14)
        bf.add(2.71)

        # Check that added values might be in the filter
        assert bf.might_contain(3.14)
        assert bf.might_contain(2.71)

    def test_add_and_might_contain_bytes(self):
        """Test adding and checking bytes values."""
        bf = BloomFilter(256, 3)

        # Add some bytes
        bf.add(b"hello")
        bf.add(b"world")

        # Check that added values might be in the filter
        assert bf.might_contain(b"hello")
        assert bf.might_contain(b"world")

    def test_add_and_might_contain_bool(self):
        """Test adding and checking boolean values."""
        bf = BloomFilter(256, 3)

        # Add booleans
        bf.add(True)
        bf.add(False)

        # Check that added values might be in the filter
        assert bf.might_contain(True)
        assert bf.might_contain(False)

    def test_add_null_value(self):
        """Test that null values are not added to the filter."""
        bf = BloomFilter(256, 3)

        # Add null
        bf.add(None)

        # Null should not be in the filter
        assert not bf.might_contain(None)

    def test_serialization_and_deserialization(self):
        """Test serializing and deserializing a bloom filter."""
        bf1 = BloomFilter(256, 3)
        bf1.add("hello")
        bf1.add("world")
        bf1.add(42)

        # Serialize
        serialized = bf1.to_bytes()
        assert isinstance(serialized, bytes)
        assert len(serialized) == 256

        # Deserialize
        bf2 = BloomFilter.from_bytes(serialized, 3)
        assert bf2.num_bytes == 256
        assert bf2.num_hash_functions == 3

        # Check that values are still found
        assert bf2.might_contain("hello")
        assert bf2.might_contain("world")
        assert bf2.might_contain(42)

    def test_false_positives_with_small_filter(self):
        """Test that false positives are possible with a small filter."""
        # Small filter to increase false positive rate
        bf = BloomFilter(16, 1)

        # Add one value
        bf.add("value1")

        # The added value should be found
        assert bf.might_contain("value1")

        # Some other values might also be found (false positives possible)
        # This is expected behavior for bloom filters


class TestBloomFilterBuilder:
    """Test cases for BloomFilterBuilder class."""

    def test_optimal_num_bytes_calculation(self):
        """Test optimal number of bytes calculation."""
        # Test with reasonable values
        num_bytes_1 = BloomFilterBuilder.optimal_num_bytes(1000, 0.05)
        assert num_bytes_1 > 0

        num_bytes_2 = BloomFilterBuilder.optimal_num_bytes(10000, 0.01)
        assert num_bytes_2 > num_bytes_1  # More elements or lower FPP should need more space

    def test_optimal_num_hash_functions_calculation(self):
        """Test optimal number of hash functions calculation."""
        num_hash = BloomFilterBuilder.optimal_num_hash_functions(256, 1000)
        assert num_hash >= 1

        # More bytes relative to elements should result in fewer hash functions
        num_hash_2 = BloomFilterBuilder.optimal_num_hash_functions(512, 1000)
        assert num_hash_2 >= num_hash

    def test_create_optimally_configured_filter(self):
        """Test creating an optimally configured bloom filter."""
        bf = BloomFilterBuilder.create(1000, 0.05)

        assert bf.num_bytes > 0
        assert bf.num_hash_functions >= 1

        # Add some values and verify they can be found
        for i in range(100):
            bf.add(f"value_{i}")

        for i in range(100):
            assert bf.might_contain(f"value_{i}")

    def test_create_with_different_false_positive_rates(self):
        """Test creating filters with different false positive rates."""
        num_elements = 1000

        bf_fpp_5 = BloomFilterBuilder.create(num_elements, 0.05)
        bf_fpp_1 = BloomFilterBuilder.create(num_elements, 0.01)

        # Lower FPP should use more space
        assert bf_fpp_1.num_bytes >= bf_fpp_5.num_bytes

    def test_invalid_false_positive_rate(self):
        """Test that invalid false positive rates raise an error."""
        with pytest.raises(ValueError):
            BloomFilterBuilder.optimal_num_bytes(1000, 0.0)

        with pytest.raises(ValueError):
            BloomFilterBuilder.optimal_num_bytes(1000, 1.0)

        with pytest.raises(ValueError):
            BloomFilterBuilder.optimal_num_bytes(1000, -0.1)


class TestBloomFilterEdgeCases:
    """Test edge cases for bloom filter."""

    def test_empty_filter(self):
        """Test querying an empty bloom filter."""
        bf = BloomFilter(10, 1)

        # Nothing has been added, so most queries should return False
        # (though there could be false positives from the empty state)
        assert not bf.might_contain("anything")

    def test_large_filter(self):
        """Test working with a large bloom filter."""
        bf = BloomFilter(10000, 5)

        # Add many values
        for i in range(5000):
            bf.add(f"value_{i}")

        # All added values should be found
        for i in range(5000):
            assert bf.might_contain(f"value_{i}")

    def test_multiple_hash_functions(self):
        """Test that multiple hash functions work correctly."""
        bf1 = BloomFilter(1000, 1)
        bf2 = BloomFilter(1000, 5)

        # Add same values to both
        for i in range(100):
            bf1.add(f"value_{i}")
            bf2.add(f"value_{i}")

        # Both should find the added values
        for i in range(100):
            assert bf1.might_contain(f"value_{i}")
            assert bf2.might_contain(f"value_{i}")

    def test_negative_and_large_integers(self):
        """Test handling of negative and large integers."""
        bf = BloomFilter(256, 3)

        # Test negative integer
        bf.add(-42)
        assert bf.might_contain(-42)

        # Test large integer (64-bit)
        large_int = 9223372036854775807  # Max 64-bit int
        bf.add(large_int)
        assert bf.might_contain(large_int)

        # Test very large integer (beyond 64-bit)
        very_large_int = 92233720368547758070
        bf.add(very_large_int)
        assert bf.might_contain(very_large_int)

    def test_mixed_types(self):
        """Test adding and finding mixed types."""
        bf = BloomFilter(512, 3)

        # Add different types
        values = [
            "string",
            42,
            3.14,
            b"bytes",
            True,
            False,
        ]

        for val in values:
            bf.add(val)

        # All should be found
        for val in values:
            assert bf.might_contain(val)

