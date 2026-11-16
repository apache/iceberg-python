#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.
"""Support for pickling objects that hold process-local state such as locks and caches."""

from typing import Any, ClassVar


class _TransientStateMixin:
    """Excludes process-local attributes from pickling and rebuilds them on unpickle.

    Subclasses list those attributes in `_transient_attrs`, create them in `_init_transient`,
    and call `_init_transient` from `__init__`. Place the mixin before other bases so its
    `__getstate__` and `__setstate__` take precedence.
    """

    _transient_attrs: ClassVar[tuple[str, ...]] = ()

    def _init_transient(self) -> None:
        """Create the attributes listed in `_transient_attrs`."""

    def __getstate__(self) -> dict[str, Any]:
        """Return the instance state without the transient attributes."""
        state = self.__dict__.copy()
        for attr in self._transient_attrs:
            state.pop(attr, None)
        return state

    def __setstate__(self, state: dict[str, Any]) -> None:
        """Restore the instance state and rebuild the transient attributes."""
        self.__dict__.update(state)
        self._init_transient()
