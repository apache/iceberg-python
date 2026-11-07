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
"""An in-memory key management client for tests, mirroring Java's `MemoryMockKMS`."""

from __future__ import annotations

from pyiceberg.encryption.ciphers import AesGcmCipher, AesKeySize, SecureKey
from pyiceberg.encryption.kms import KeyManagementClient
from pyiceberg.typedef import EMPTY_DICT, Properties


class MemoryKeyManagementClient(KeyManagementClient):
    """A key management service that holds its master keys in memory.

    Master keys live only in this process, with no durability or access control, so this is
    for tests only.
    """

    def __init__(self, properties: Properties = EMPTY_DICT, *, master_key_size: AesKeySize = AesKeySize.BITS_128) -> None:
        super().__init__(properties)
        self._master_key_size = master_key_size
        self._master_keys: dict[str, SecureKey] = {}

    def __repr__(self) -> str:
        """Return a representation that counts the master keys without exposing them."""
        return f"MemoryKeyManagementClient(master_key_size={self._master_key_size!r}, key_count={len(self._master_keys)})"

    def add_master_key(self, wrapping_key_id: str, key: SecureKey | None = None) -> SecureKey:
        """Register a master key under `wrapping_key_id`, generating one when `key` is omitted.

        Args:
            wrapping_key_id (str): The id to register the master key under.
            key (SecureKey | None): Known key material, for tests that share it with another client.
        """
        if wrapping_key_id in self._master_keys:
            raise ValueError(f"Master key already exists: {wrapping_key_id}")

        master_key = SecureKey.generate(self._master_key_size) if key is None else key
        self._master_keys[wrapping_key_id] = master_key
        return master_key

    def _cipher(self, wrapping_key_id: str) -> AesGcmCipher:
        if (master_key := self._master_keys.get(wrapping_key_id)) is None:
            raise ValueError(f"Master key not found: {wrapping_key_id}")

        return AesGcmCipher(master_key)

    def wrap_key(self, key: bytes, wrapping_key_id: str) -> bytes:
        """Wrap a key with the registered master key, without AAD, as Java and iceberg-rust do."""
        return self._cipher(wrapping_key_id).encrypt(key)

    def unwrap_key(self, wrapped_key: bytes, wrapping_key_id: str) -> bytes:
        """Unwrap a key wrapped by `wrap_key`."""
        return self._cipher(wrapping_key_id).decrypt(wrapped_key)
