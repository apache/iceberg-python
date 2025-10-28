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
"""Bloom filter implementation for Iceberg row-level filtering."""

from __future__ import annotations

import struct

import mmh3


class BloomFilter:
    """Bloom filter implementation for Iceberg column value filtering.
    
    A bloom filter is a space-efficient probabilistic data structure that can determine
    whether an element may be in a set or is definitely not in the set. In Iceberg,
    bloom filters are used at the file level to help prune data files that cannot
    contain rows matching a query predicate.
    """

    MAGIC = b"ORC"
    SERIAL_NUMBER = 4

    def __init__(self, num_bytes: int, num_hash_functions: int = 1):
        """Initialize a bloom filter.

        Args:
            num_bytes: Size of the bloom filter in bytes.
            num_hash_functions: Number of hash functions to use (default: 1).
        """
        self.num_bytes = num_bytes
        self.num_hash_functions = num_hash_functions
        # Initialize bit array as bytearray
        self.bit_array = bytearray(num_bytes)

    def add(self, value: str | bytes | int | float | bool | None) -> None:
        """Add a value to the bloom filter.

        Args:
            value: The value to add to the filter.
        """
        if value is None:
            # Null values are typically not added to bloom filters in Iceberg
            return

        # Convert value to bytes
        value_bytes = self._to_bytes(value)

        # Hash the value with multiple hash functions
        for i in range(self.num_hash_functions):
            # Create a unique hash input for each hash function
            hash_input = value_bytes + struct.pack(">I", i)
            # Use MurmurHash3 (128-bit)
            hash_value = mmh3.hash128(hash_input, signed=False)
            # Calculate bit positions
            bit_pos = hash_value % (self.num_bytes * 8)
            # Set the bit
            byte_index = bit_pos // 8
            bit_index = bit_pos % 8
            self.bit_array[byte_index] |= 1 << bit_index

    def might_contain(self, value: str | bytes | int | float | bool | None) -> bool:
        """Check if a value might be in the bloom filter.

        Returns True if the value might be in the set, False if it definitely is not.

        Args:
            value: The value to check.

        Returns:
            True if the value might be in the set, False if definitely not.
        """
        if value is None:
            # Null values are not in bloom filters
            return False

        # Convert value to bytes
        value_bytes = self._to_bytes(value)

        # Check all hash positions
        for i in range(self.num_hash_functions):
            # Create a unique hash input for each hash function
            hash_input = value_bytes + struct.pack(">I", i)
            # Use MurmurHash3 (128-bit)
            hash_value = mmh3.hash128(hash_input, signed=False)
            # Calculate bit position
            bit_pos = hash_value % (self.num_bytes * 8)
            # Check if the bit is set
            byte_index = bit_pos // 8
            bit_index = bit_pos % 8
            if not (self.bit_array[byte_index] & (1 << bit_index)):
                # If any hash position is not set, value is definitely not in the set
                return False

        # All hash positions are set, value might be in the set
        return True

    def to_bytes(self) -> bytes:
        """Serialize the bloom filter to bytes.

        Returns:
            Serialized bloom filter as bytes.
        """
        return bytes(self.bit_array)

    @classmethod
    def from_bytes(cls, data: bytes, num_hash_functions: int = 1) -> BloomFilter:
        """Deserialize a bloom filter from bytes.

        Args:
            data: Serialized bloom filter bytes.
            num_hash_functions: Number of hash functions used in the filter.

        Returns:
            BloomFilter instance.
        """
        bf = cls(len(data), num_hash_functions)
        bf.bit_array = bytearray(data)
        return bf

    @staticmethod
    def _to_bytes(value: str | bytes | int | float | bool) -> bytes:
        """Convert a value to bytes for hashing.

        Args:
            value: Value to convert.

        Returns:
            Bytes representation of the value.
        """
        if isinstance(value, bytes):
            return value
        elif isinstance(value, str):
            return value.encode("utf-8")
        elif isinstance(value, bool):
            # bool before int because bool is subclass of int
            return struct.pack(">B", 1 if value else 0)
        elif isinstance(value, int):
            # Handle both 32-bit and 64-bit integers
            if -2147483648 <= value <= 2147483647:
                return struct.pack(">i", value)
            else:
                return struct.pack(">q", value)
        elif isinstance(value, float):
            return struct.pack(">d", value)
        else:
            raise TypeError(f"Unsupported type for bloom filter: {type(value)}")

    def __repr__(self) -> str:
        """Return string representation of the bloom filter."""
        return f"BloomFilter(num_bytes={self.num_bytes}, num_hash_functions={self.num_hash_functions})"


class BloomFilterBuilder:
    """Builder for creating bloom filters with specific false positive rates."""

    @staticmethod
    def optimal_num_bytes(num_elements: int, false_positive_rate: float = 0.05) -> int:
        """Calculate optimal bloom filter size in bytes given element count and FPP.

        Uses the formula: m = -(n * ln(p)) / (ln(2)^2)
        where n is number of elements and p is false positive rate.

        Args:
            num_elements: Expected number of elements to add.
            false_positive_rate: Desired false positive rate (default: 0.05).

        Returns:
            Optimal number of bytes for the bloom filter.
        """
        import math

        if num_elements <= 0:
            return 1
        if false_positive_rate <= 0 or false_positive_rate >= 1:
            raise ValueError("False positive rate must be between 0 and 1")

        # Calculate optimal number of bits
        num_bits = -(num_elements * math.log(false_positive_rate)) / (math.log(2) ** 2)
        # Convert to bytes (round up)
        num_bytes = int(math.ceil(num_bits / 8))
        return max(1, num_bytes)

    @staticmethod
    def optimal_num_hash_functions(num_bytes: int, num_elements: int) -> int:
        """Calculate optimal number of hash functions.

        Uses the formula: k = (m / n) * ln(2)
        where m is number of bytes and n is number of elements.

        Args:
            num_bytes: Size of bloom filter in bytes.
            num_elements: Expected number of elements to add.

        Returns:
            Optimal number of hash functions.
        """
        import math

        if num_elements <= 0 or num_bytes <= 0:
            return 1
        k = (num_bytes / num_elements) * math.log(2)
        return max(1, int(round(k)))

    @staticmethod
    def create(num_elements: int, false_positive_rate: float = 0.05) -> BloomFilter:
        """Create an optimally configured bloom filter.

        Args:
            num_elements: Expected number of elements to add.
            false_positive_rate: Desired false positive rate (default: 0.05).

        Returns:
            Optimally configured BloomFilter instance.
        """
        num_bytes = BloomFilterBuilder.optimal_num_bytes(num_elements, false_positive_rate)
        num_hash_functions = BloomFilterBuilder.optimal_num_hash_functions(num_bytes, num_elements)
        return BloomFilter(num_bytes, num_hash_functions)
