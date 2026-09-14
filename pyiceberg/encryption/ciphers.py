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
"""AES-GCM primitives for table encryption."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from enum import IntEnum
from typing import TYPE_CHECKING

from pyiceberg.utils.lazy_import import not_installed

if TYPE_CHECKING:
    from cryptography.exceptions import InvalidTag
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM


class AesKeySize(IntEnum):
    """The supported AES key sizes, in bits."""

    BITS_128 = 128
    BITS_192 = 192
    BITS_256 = 256

    @property
    def key_length(self) -> int:
        """Return the key length in bytes."""
        return self // 8

    @classmethod
    def from_key_length(cls, key_length: int) -> AesKeySize:
        """Return the key size for a key of `key_length` bytes."""
        try:
            return cls(key_length * 8)
        except ValueError as e:
            raise ValueError(f"Unsupported key length: {key_length} (must be 16, 24 or 32)") from e


@dataclass(frozen=True)
class SecureKey:
    """An AES key of a length the spec allows, kept out of reprs and tracebacks."""

    key: bytes = field(repr=False)

    def __post_init__(self) -> None:
        """Reject keys that are not a supported AES key length."""
        AesKeySize.from_key_length(len(self.key))

    @property
    def key_size(self) -> AesKeySize:
        """Return the size of this key."""
        return AesKeySize.from_key_length(len(self.key))

    @classmethod
    def generate(cls, key_size: AesKeySize = AesKeySize.BITS_128) -> SecureKey:
        """Generate a new key of `key_size`."""
        return cls(os.urandom(key_size.key_length))


class AesGcmCipher:
    """Encrypts and decrypts using AES-GCM.

    Ciphertext is laid out as `nonce || ciphertext || tag`, matching Java and iceberg-rust.
    """

    NONCE_LENGTH = 12
    TAG_LENGTH = 16

    def __init__(self, key: SecureKey) -> None:
        try:
            from cryptography.exceptions import InvalidTag
            from cryptography.hazmat.primitives.ciphers.aead import AESGCM
        except ImportError:
            raise not_installed("cryptography", extras_name="encryption") from None

        self._aes_gcm: AESGCM = AESGCM(key.key)
        self._invalid_tag: type[InvalidTag] = InvalidTag

    def encrypt(self, plaintext: bytes, aad: bytes | None = None) -> bytes:
        """Encrypt `plaintext`, authenticating `aad` alongside it.

        Args:
            plaintext (bytes): The data to encrypt.
            aad (bytes | None): Additional data to authenticate but not encrypt.
        """
        nonce = os.urandom(self.NONCE_LENGTH)
        return nonce + self._aes_gcm.encrypt(nonce, plaintext, aad)

    def decrypt(self, ciphertext: bytes, aad: bytes | None = None) -> bytes:
        """Decrypt `ciphertext`, verifying `aad` alongside it.

        Args:
            ciphertext (bytes): The data to decrypt, as returned by `encrypt`.
            aad (bytes | None): The additional data that was authenticated on encryption.
        """
        if len(ciphertext) < self.NONCE_LENGTH + self.TAG_LENGTH:
            raise ValueError(
                f"Ciphertext too short: expected at least {self.NONCE_LENGTH + self.TAG_LENGTH} bytes, got {len(ciphertext)}"
            )

        nonce, encrypted = ciphertext[: self.NONCE_LENGTH], ciphertext[self.NONCE_LENGTH :]
        try:
            return self._aes_gcm.decrypt(nonce, encrypted, aad)
        except self._invalid_tag as e:
            raise ValueError("AES-GCM decryption failed") from e
