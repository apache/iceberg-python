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
from collections.abc import Callable, Iterator
from typing import Any, Generic, TypeVar

from pyiceberg.partitioning import PartitionSpec
from pyiceberg.typedef import Record

T = TypeVar("T")


class PartitionMap(Generic[T]):
    """A map-like structure that organizes values by partition spec ID and partition values.

    Attributes:
        _specs_by_id: Dictionary mapping spec IDs to PartitionSpec objects
        _map: Internal dictionary storing values by composite keys

    """

    def __init__(self, specs_by_id: dict[int, PartitionSpec] | None = None) -> None:
        """Initialize a new PartitionMap."""
        self._specs_by_id = specs_by_id or {}
        self._map: dict[tuple[int, tuple[Any, ...]], T] = {}

    def get(self, spec_id: int, partition: Record | None) -> T | None:
        """Get a value by spec ID and partition."""
        key = self._make_key(spec_id, partition)
        return self._map.get(key)

    def put(self, spec_id: int, partition: Record | None, value: T) -> None:
        """Put a value by spec ID and partition."""
        if spec_id not in self._specs_by_id:
            raise ValueError(f"Cannot find spec with ID {spec_id}: {self._specs_by_id}")
        key = self._make_key(spec_id, partition)
        self._map[key] = value

    def compute_if_absent(self, spec_id: int, partition: Record | None, factory: Callable[[], T]) -> T:
        """Get a value by spec ID and partition, creating it if it doesn't exist."""
        if spec_id not in self._specs_by_id:
            raise ValueError(f"Cannot find spec with ID {spec_id}: {self._specs_by_id}")

        key = self._make_key(spec_id, partition)
        if key not in self._map:
            self._map[key] = factory()
        return self._map[key]

    def _make_key(self, spec_id: int, partition: Record | None) -> tuple[int, tuple[Any, ...]]:
        """Create a composite key from spec ID and partition."""
        if partition is None:
            partition_values = ()
        else:
            partition_values = tuple(partition._data)
        return spec_id, partition_values

    def values(self) -> Iterator[T]:
        """Get all values in the map."""
        return iter(self._map.values())

    def is_empty(self) -> bool:
        """Check if the map is empty."""
        return len(self._map) == 0
