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

from typing import (
    Any,
)

from pydantic import Field

from pyiceberg.typedef import Identifier
from pyiceberg.view.metadata import ViewMetadata


class View:
    """An Iceberg view."""

    _identifier: Identifier = Field()
    metadata: ViewMetadata

    def __init__(
        self,
        identifier: Identifier,
        metadata: ViewMetadata,
    ) -> None:
        self._identifier = identifier
        self.metadata = metadata

    def name(self) -> Identifier:
        """Return the identifier of this view."""
        return self._identifier

    def __eq__(self, other: Any) -> bool:
        """Return the equality of two instances of the View class."""
        return self.name() == other.name() and self.metadata == other.metadata if isinstance(other, View) else False
