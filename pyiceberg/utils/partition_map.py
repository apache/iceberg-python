from typing import Any, Dict, Generic, Iterator, Optional, Tuple, TypeVar, Callable

from pyiceberg.partitioning import PartitionSpec

T = TypeVar("T")


class PartitionMap(Generic[T]):

    def __init__(self, specs_by_id: Optional[Dict[int, PartitionSpec]] = None):
        self._specs_by_id = specs_by_id or {}
        self._map: Dict[Tuple[int, Tuple[Any, ...]], T] = {}
    
    def get(self, spec_id: int, partition: Any) -> Optional[T]:
        """Get a value by spec ID and partition."""
        key = self._make_key(spec_id, partition)
        return self._map.get(key)
    
    def put(self, spec_id: int, partition: Any, value: T) -> None:
        """Put a value by spec ID and partition."""
        key = self._make_key(spec_id, partition)
        self._map[key] = value
    
    def  compute_if_absent(self, spec_id: int, partition: Any, factory: Callable[[], T]) -> T:
        """Get a value by spec ID and partition, creating it if it doesn't exist."""
        key = self._make_key(spec_id, partition)
        if key not in self._map:
            self._map[key] = factory()
        return self._map[key]
    
    def _make_key(self, spec_id: int, partition: Any) -> Tuple[int, Tuple[Any, ...]]:
        """Create a composite key from spec ID and partition."""
        if hasattr(partition, "_data"):
            partition_values = tuple(partition._data)
        elif isinstance(partition, dict):
            partition_values = tuple(partition.values())
        elif isinstance(partition, (list, tuple)):
            partition_values = tuple(partition)
        else:
            partition_values = (partition,) if partition is not None else ()
        
        return (spec_id, partition_values)
    
    def values(self) -> Iterator[T]:
        """Get all values in the map."""
        return iter(self._map.values())
    
    def is_empty(self) -> bool:
        """Check if the map is empty."""
        return len(self._map) == 0
