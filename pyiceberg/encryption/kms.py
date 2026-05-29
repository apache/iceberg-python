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

import importlib
import logging
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

from pyiceberg.encryption.ciphers import aes_gcm_decrypt, aes_gcm_encrypt

if TYPE_CHECKING:
    from pyiceberg.typedef import Properties

logger = logging.getLogger(__name__)

PY_KMS_IMPL = "py-kms-impl"


class KeyManagementClient(ABC):
    @abstractmethod
    def wrap_key(self, key: bytes, wrapping_key_id: str) -> bytes: ...

    @abstractmethod
    def unwrap_key(self, wrapped_key: bytes, wrapping_key_id: str) -> bytes: ...

    def initialize(self, properties: dict[str, str]) -> None:  # noqa: B027
        """Initialize the KMS client from catalog/table properties."""


class InMemoryKms(KeyManagementClient):
    """In-memory KMS for testing. NOT for production use."""

    def __init__(self, master_keys: dict[str, bytes] | None = None) -> None:
        self._master_keys: dict[str, bytes] = dict(master_keys) if master_keys else {}

    def initialize(self, properties: dict[str, str]) -> None:
        prefix = "encryption.kms.key."
        for key, value in properties.items():
            if key.startswith(prefix):
                self._master_keys[key[len(prefix) :]] = bytes.fromhex(value)

    def wrap_key(self, key: bytes, wrapping_key_id: str) -> bytes:
        return aes_gcm_encrypt(self._master(wrapping_key_id), key, aad=None)

    def unwrap_key(self, wrapped_key: bytes, wrapping_key_id: str) -> bytes:
        return aes_gcm_decrypt(self._master(wrapping_key_id), wrapped_key, aad=None)

    def _master(self, wrapping_key_id: str) -> bytes:
        master_key = self._master_keys.get(wrapping_key_id)
        if master_key is None:
            raise ValueError(f"Wrapping key not found: {wrapping_key_id}")
        return master_key


def load_kms_client(properties: Properties) -> KeyManagementClient | None:
    """Instantiate a KeyManagementClient from a fully-qualified `py-kms-impl` (or return None)."""
    kms_impl = properties.get(PY_KMS_IMPL)
    if kms_impl is None:
        return None

    path_parts = kms_impl.split(".")
    if len(path_parts) < 2:
        raise ValueError(f"py-kms-impl should be a full path (module.ClassName), got: {kms_impl}")

    module_name, class_name = ".".join(path_parts[:-1]), path_parts[-1]
    try:
        module = importlib.import_module(module_name)
    except ModuleNotFoundError as e:
        raise ValueError(f"Could not import KMS module: {module_name}") from e

    kms_class = getattr(module, class_name, None)
    if kms_class is None:
        raise ValueError(f"KMS class {class_name} not found in module {module_name}")

    if not (isinstance(kms_class, type) and issubclass(kms_class, KeyManagementClient)):
        raise ValueError(f"{kms_impl} is not a subclass of KeyManagementClient")

    client = kms_class()
    client.initialize(dict(properties))
    return client
