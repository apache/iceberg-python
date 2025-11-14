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
from abc import ABCMeta
from typing import TYPE_CHECKING, Any, Callable, TypeVar

if TYPE_CHECKING:
    from pyiceberg.catalog import Catalog

_T = TypeVar("_T", bound="Catalog")


class NamespaceMeta(type):
    """Metaclass for managing namespace support configuration in catalog implementations.

    This metaclass automatically handles the inheritance and initialization of namespace-related
    attributes for catalog classes. It ensures that namespace support configuration is properly
    propagated through class inheritance hierarchies.

    Attributes:
    _support_namespaces (bool): Indicates whether the catalog supports nested namespaces.
                Defaults to False. When True, the catalog can handle hierarchical namespace
                structures beyond simple flat namespaces.
    _max_namespace_depth (int): Maximum depth allowed for nested namespaces.
        Defaults to -1 (unlimited depth). When set to a positive integer,
        restricts the nesting level of namespaces that can be created.
    """

    _support_namespaces: bool = False
    _max_namespace_depth: int = -1

    def __new__(mcls, name: str, bases: tuple[type, ...], atrrs: dict[str, Any], /, **kwargs: Any) -> type:
        cls = super().__new__(mcls, name, bases, atrrs, **kwargs)
        if "_support_namespaces" in atrrs:
            pass  # Already defined in the class
        elif hasattr(bases[0], "_support_namespaces"):
            cls._support_namespaces = bases[0]._support_namespaces
        else:
            cls._support_namespaces = NamespaceMeta._support_namespaces
        return cls


class CatalogABCMeta(NamespaceMeta, ABCMeta):
    """Metaclass for catalog implementations that combines namespace and abstract base class functionality.

    This metaclass inherits from both NamespaceMeta and ABCMeta.
    """


def multiple_namespaces(
    _cls: type[_T] | None = None, /, disabled: bool = False, max_depth: int = -1
) -> type[_T] | Callable[[type[_T]], type[_T]]:
    def wrapper(cls: type[_T]) -> type[_T]:
        if not hasattr(cls, "_support_namespaces"):
            raise ValueError(f"{cls.__name__} must inherit Catalog with CatalogABCMeta and NamespaceMeta to use this decorator")
        if max_depth >= 0 and max_depth <= 1 or disabled:
            cls._support_namespaces = False
        else:
            cls._support_namespaces = True
            cls._max_namespace_depth = max_depth
        return cls

    return wrapper if _cls is None else wrapper(_cls)
