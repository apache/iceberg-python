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
"""Key management client interface for table encryption."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field

from pyiceberg.typedef import EMPTY_DICT, Properties


@dataclass(frozen=True)
class GeneratedKey:
    """A newly generated key, both in the clear and wrapped by the key management service."""

    key: bytes = field(repr=False)
    wrapped_key: bytes


class KeyManagementClient(ABC):
    """A base class for key management service implementations.

    Wraps and unwraps table encryption keys using master keys that the service holds.

    Implementations are loaded by name from the catalog properties, so a subclass must keep
    this constructor signature, as `FileIO` does.
    """

    properties: Properties

    def __init__(self, properties: Properties = EMPTY_DICT) -> None:
        self.properties = properties

    @abstractmethod
    def wrap_key(self, key: bytes, wrapping_key_id: str) -> bytes:
        """Wrap a key using the master key identified by `wrapping_key_id`.

        Args:
            key (bytes): The key to wrap.
            wrapping_key_id (str): Identifies the master key held by the service.
        """

    @abstractmethod
    def unwrap_key(self, wrapped_key: bytes, wrapping_key_id: str) -> bytes:
        """Unwrap a key using the master key identified by `wrapping_key_id`.

        Args:
            wrapped_key (bytes): The wrapped key, as returned by `wrap_key`.
            wrapping_key_id (str): Identifies the master key held by the service.
        """

    def supports_key_generation(self) -> bool:
        """Whether the service generates keys itself, rather than only wrapping them."""
        return False

    def generate_key(self, wrapping_key_id: str) -> GeneratedKey:
        """Generate a new key, wrapped by the master key identified by `wrapping_key_id`.

        Args:
            wrapping_key_id (str): Identifies the master key held by the service.
        """
        raise NotImplementedError(f"{type(self).__name__} does not support key generation")
