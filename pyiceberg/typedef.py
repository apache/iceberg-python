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

import sys
from abc import abstractmethod
from collections.abc import Callable, Iterator
from datetime import date, datetime, time
from decimal import Decimal
from typing import (
    TYPE_CHECKING,
    Any,
    Generic,
    Literal,
    Protocol,
    SupportsIndex,
    TypeAlias,
    TypeVar,
    Union,
    overload,
    runtime_checkable,
)
from uuid import UUID

from pydantic import BaseModel, ConfigDict, RootModel
from typing_extensions import Self

if TYPE_CHECKING:
    from pyiceberg.expressions.literals import Literal as IcebergLiteral
    from pyiceberg.types import StructType

    LiteralValue = IcebergLiteral[Any]
else:
    # Use Any for runtime to avoid circular import - type checkers will use TYPE_CHECKING version
    LiteralValue = Any  # type: ignore[assignment,misc]


class FrozenDict(dict[Any, Any]):
    def __setitem__(self, instance: Any, value: Any) -> None:
        """Assign a value to a FrozenDict."""
        raise AttributeError("FrozenDict does not support assignment")

    def update(self, *args: Any, **kwargs: Any) -> None:
        raise AttributeError("FrozenDict does not support .update()")


UTF8 = "utf-8"

EMPTY_DICT = FrozenDict()

K = TypeVar("K")
V = TypeVar("V")


# from https://stackoverflow.com/questions/2912231/is-there-a-clever-way-to-pass-the-key-to-defaultdicts-default-factory
class KeyDefaultDict(dict[K, V]):
    def __init__(self, default_factory: Callable[[K], V]):
        super().__init__()
        self.default_factory = default_factory

    def __missing__(self, key: K) -> V:
        """Define behavior if you access a non-existent key in a KeyDefaultDict."""
        val = self.default_factory(key)
        self[key] = val
        return val


Identifier = tuple[str, ...]
"""A tuple of strings representing a table identifier.

Each string in the tuple represents a part of the table's unique path. For example,
a table in a namespace might be identified as:

    ("namespace", "table_name")

Examples:
    >>> identifier: Identifier = ("namespace", "table_name")
"""

Properties = dict[str, Any]
"""A dictionary type for properties in PyIceberg."""


RecursiveDict = dict[str, Union[str, "RecursiveDict"]]
"""A recursive dictionary type for nested structures in PyIceberg."""

# Represents the literal value
L = TypeVar("L", str, bool, int, float, bytes, UUID, Decimal, datetime, date, time, covariant=True)


@runtime_checkable
class StructProtocol(Protocol):  # pragma: no cover
    """A generic protocol used by accessors to get and set at positions of an object."""

    @abstractmethod
    def __getitem__(self, pos: int) -> Any:
        """Fetch a value from a StructProtocol."""

    @abstractmethod
    def __setitem__(self, pos: int, value: Any) -> None:
        """Assign a value to a StructProtocol."""


class IcebergBaseModel(BaseModel):
    """
    This class extends the Pydantic BaseModel to set default values by overriding them.

    This is because we always want to set by_alias to True. In Python, the dash can't
    be used in variable names, and this is used throughout the Iceberg spec.

    The same goes for exclude_none, if a field is None we want to omit it from
    serialization, for example, the doc attribute on the NestedField object.
    Default non-null values will be serialized.

    This is recommended by Pydantic:
    https://pydantic-docs.helpmanual.io/usage/model_config/#change-behaviour-globally
    """

    model_config = ConfigDict(populate_by_name=True, frozen=True)

    def _exclude_private_properties(self, exclude: set[str] | None = None) -> set[str]:
        # A small trick to exclude private properties. Properties are serialized by pydantic,
        # regardless if they start with an underscore.
        # This will look at the dict, and find the fields and exclude them
        return set.union(
            {field for field in self.__dict__ if field.startswith("_") and not field == "__root__"}, exclude or set()
        )

    def model_dump(
        self, exclude_none: bool = True, exclude: set[str] | None = None, by_alias: bool = True, **kwargs: Any
    ) -> dict[str, Any]:
        return super().model_dump(
            exclude_none=exclude_none, exclude=self._exclude_private_properties(exclude), by_alias=by_alias, **kwargs
        )

    def model_dump_json(
        self, exclude_none: bool = True, exclude: set[str] | None = None, by_alias: bool = True, **kwargs: Any
    ) -> str:
        return super().model_dump_json(
            exclude_none=exclude_none, exclude=self._exclude_private_properties(exclude), by_alias=by_alias, **kwargs
        )


T = TypeVar("T")
S = TypeVar("S")


class IcebergRootModel(RootModel[T], Generic[T]):
    """
    This class extends the Pydantic BaseModel to set default values by overriding them.

    This is because we always want to set by_alias to True. In Python, the dash can't
    be used in variable names, and this is used throughout the Iceberg spec.

    The same goes for exclude_none, if a field is None we want to omit it from
    serialization, for example, the doc attribute on the NestedField object.
    Default non-null values will be serialized.

    This is recommended by Pydantic:
    https://pydantic-docs.helpmanual.io/usage/model_config/#change-behaviour-globally
    """

    model_config = ConfigDict(frozen=True)


class Record(StructProtocol):
    __slots__ = ("_data",)
    _data: list[Any]

    @classmethod
    def _bind(cls, struct: StructType, **arguments: Any) -> Self:
        return cls(*[arguments[field.name] if field.name in arguments else field.initial_default for field in struct.fields])

    def __init__(self, *data: Any) -> None:
        self._data = list(data)

    def __setitem__(self, pos: int, value: Any) -> None:
        """Assign a value to a Record."""
        self._data[pos] = value

    def __getitem__(self, pos: int) -> Any:
        """Fetch a value from a Record."""
        return self._data[pos]

    def __eq__(self, other: Any) -> bool:
        """Return the equality of two instances of the Record class."""
        return self._data == other._data if isinstance(other, Record) else False

    def __repr__(self) -> str:
        """Return the string representation of the Record class."""
        return f"{self.__class__.__name__}[{', '.join(str(v) for v in self._data)}]"

    def __len__(self) -> int:
        """Return the number of fields in the Record class."""
        return len(self._data)

    def __hash__(self) -> int:
        """Return hash value of the Record class."""
        return hash(str(self))


TableVersion: TypeAlias = Literal[1, 2, 3]
ViewVersion: TypeAlias = Literal[1]

FetchNextPage: TypeAlias = Callable[[str], tuple[list[T], str | None]]


class PaginationList(list[T]):
    """A list that lazily fetches subsequent pages from a paginated API.

    The first page is pre-loaded on construction.  Subsequent pages are only
    fetched when the caller iterates past items already in memory.  Operations
    that require the complete result set — ``len()``, ``in``, slicing,
    ``repr()`` — trigger a full fetch of all remaining pages.

    Args:
        first_page: Items from the first API response.
        next_page_token: Pagination token returned with the first response,
            or ``None`` if no further pages exist.
        fetch_next_page: Callable matching ``FetchNextPage[T]`` — accepts a
            page token and returns ``(items, next_page_token_or_None)``.
    """

    def __init__(
        self,
        first_page: list[T],
        next_page_token: str | None,
        fetch_next_page: FetchNextPage[T],
    ) -> None:
        super().__init__(first_page)
        self._next_page_token = next_page_token
        self._fetch_next_page = fetch_next_page

    def _fetch_all(self) -> None:
        while self._next_page_token:
            items, self._next_page_token = self._fetch_next_page(self._next_page_token)
            list.extend(self, items)

    def _fetch_through_index(self, idx: int) -> None:
        while list.__len__(self) <= idx and self._next_page_token:
            items, self._next_page_token = self._fetch_next_page(self._next_page_token)
            list.extend(self, items)

    def __iter__(self) -> Iterator[T]:
        """Iterate lazily, fetching pages only as the caller advances."""
        idx = 0
        while True:
            if idx < list.__len__(self):
                yield list.__getitem__(self, idx)
                idx += 1
            elif self._next_page_token:
                items, self._next_page_token = self._fetch_next_page(self._next_page_token)
                list.extend(self, items)
            else:
                return

    def __len__(self) -> int:
        """Return the total number of items, fetching all pages first."""
        self._fetch_all()
        return list.__len__(self)

    def __contains__(self, item: object) -> bool:
        """Return True if item is present, fetching all pages first."""
        self._fetch_all()
        return list.__contains__(self, item)

    def __repr__(self) -> str:
        """Return string representation after fetching all pages."""
        self._fetch_all()
        return f"PaginationList({list.__repr__(self)})"

    def __eq__(self, other: object) -> bool:
        """Compare equality after fetching all pages."""
        self._fetch_all()
        return list.__eq__(self, other)

    def __ne__(self, other: object) -> bool:
        """Compare inequality after fetching all pages."""
        return not self.__eq__(other)

    @overload
    def __getitem__(self, idx: SupportsIndex) -> T: ...

    @overload
    def __getitem__(self, idx: slice) -> list[T]: ...

    def __getitem__(self, idx: SupportsIndex | slice) -> T | list[T]:
        """Fetch pages as needed before returning the requested item(s)."""
        if isinstance(idx, slice):
            self._fetch_all()
        else:
            i = idx.__index__()
            if i < 0:
                self._fetch_all()
            else:
                self._fetch_through_index(i)
        return list.__getitem__(self, idx)

    def count(self, value: T) -> int:
        """Return the number of occurrences of value, fetching all pages first."""
        self._fetch_all()
        return list.count(self, value)

    def index(self, value: T, start: SupportsIndex = 0, stop: SupportsIndex = sys.maxsize, /) -> int:
        """Return the index of the first occurrence of value, fetching all pages first."""
        self._fetch_all()
        return list.index(self, value, start, stop)

    def __reversed__(self) -> Iterator[T]:
        """Return an iterator over the items in reverse order, fetching all pages first."""
        self._fetch_all()
        return list.__reversed__(self)

    def copy(self) -> list[T]:
        """Return a plain list with all items, fetching all pages first."""
        self._fetch_all()
        return list.copy(self)

    @overload
    def __add__(self, other: list[T]) -> list[T]: ...

    @overload
    def __add__(self, other: list[S]) -> list[S | T]: ...

    def __add__(self, other: list[Any]) -> list[Any]:
        """Return self + other as a plain list, fetching all pages first."""
        self._fetch_all()
        return list.__add__(self, other)

    def __radd__(self, other: list[T]) -> list[T]:
        """Return other + self as a plain list, fetching all pages first."""
        self._fetch_all()
        return list.__add__(other, self)

    def __mul__(self, n: SupportsIndex) -> list[T]:
        """Return self * n as a plain list, fetching all pages first."""
        self._fetch_all()
        return list.__mul__(self, n)

    def __rmul__(self, n: SupportsIndex) -> list[T]:
        """Return n * self as a plain list, fetching all pages first."""
        self._fetch_all()
        return list.__mul__(self, n)
