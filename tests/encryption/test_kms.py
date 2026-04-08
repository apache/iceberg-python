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
import os

import pytest

from pyiceberg.encryption.kms import InMemoryKms, KeyManagementClient, load_kms_client


class TestInMemoryKms:
    def test_wrap_unwrap_roundtrip(self) -> None:
        master_key = os.urandom(16)
        kms = InMemoryKms(master_keys={"keyA": master_key})
        plaintext_key = os.urandom(16)
        wrapped = kms.wrap_key(plaintext_key, "keyA")
        unwrapped = kms.unwrap_key(wrapped, "keyA")
        assert unwrapped == plaintext_key

    def test_unknown_wrapping_key(self) -> None:
        kms = InMemoryKms()
        with pytest.raises(ValueError, match="Wrapping key not found"):
            kms.wrap_key(b"key", "nonexistent")

    def test_unknown_unwrapping_key(self) -> None:
        kms = InMemoryKms()
        with pytest.raises(ValueError, match="Wrapping key not found"):
            kms.unwrap_key(b"wrapped", "nonexistent")

    def test_initialize_from_properties(self) -> None:
        kms = InMemoryKms()
        key_hex = os.urandom(16).hex()
        kms.initialize({"encryption.kms.key.testKey": key_hex})
        # Should be able to wrap/unwrap with the initialized key
        wrapped = kms.wrap_key(b"secret", "testKey")
        assert kms.unwrap_key(wrapped, "testKey") == b"secret"

    def test_initialize_ignores_unrelated_properties(self) -> None:
        kms = InMemoryKms()
        kms.initialize({"some.other.prop": "value", "encryption.kms.key.k1": os.urandom(16).hex()})
        with pytest.raises(ValueError, match="Wrapping key not found"):
            kms.wrap_key(b"key", "nonexistent")

    def test_wrap_unwrap_with_standard_test_keys(self) -> None:
        """Wrap/unwrap with the standard Iceberg test master keys."""
        kms = InMemoryKms(
            master_keys={
                "keyA": b"0123456789012345",
                "keyB": b"1123456789012345",
            }
        )
        plaintext = os.urandom(16)
        wrapped_a = kms.wrap_key(plaintext, "keyA")
        assert kms.unwrap_key(wrapped_a, "keyA") == plaintext
        wrapped_b = kms.wrap_key(plaintext, "keyB")
        assert kms.unwrap_key(wrapped_b, "keyB") == plaintext

    def test_wrong_master_key_fails_unwrap(self) -> None:
        from cryptography.exceptions import InvalidTag

        kms = InMemoryKms(
            master_keys={
                "keyA": os.urandom(16),
                "keyB": os.urandom(16),
            }
        )
        wrapped = kms.wrap_key(b"secret", "keyA")
        with pytest.raises(InvalidTag):
            kms.unwrap_key(wrapped, "keyB")


class TestLoadKmsClient:
    def test_returns_none_when_not_configured(self) -> None:
        assert load_kms_client({}) is None

    def test_loads_in_memory_kms(self) -> None:
        client = load_kms_client(
            {
                "py-kms-impl": "pyiceberg.encryption.kms.InMemoryKms",
                "encryption.kms.key.myKey": os.urandom(16).hex(),
            }
        )
        assert client is not None
        assert isinstance(client, InMemoryKms)
        # Should be initialized — the key should be usable
        wrapped = client.wrap_key(b"data", "myKey")
        assert client.unwrap_key(wrapped, "myKey") == b"data"

    def test_invalid_short_path(self) -> None:
        with pytest.raises(ValueError, match="full path"):
            load_kms_client({"py-kms-impl": "InMemoryKms"})

    def test_nonexistent_module(self) -> None:
        with pytest.raises(ValueError, match="Could not import"):
            load_kms_client({"py-kms-impl": "nonexistent.module.Kms"})

    def test_nonexistent_class(self) -> None:
        with pytest.raises(ValueError, match="not found in module"):
            load_kms_client({"py-kms-impl": "pyiceberg.encryption.kms.NonexistentClass"})

    def test_not_a_subclass(self) -> None:
        with pytest.raises(ValueError, match="not a subclass"):
            # AESGCM is a real class but not a KeyManagementClient
            load_kms_client({"py-kms-impl": "cryptography.hazmat.primitives.ciphers.aead.AESGCM"})

    def test_custom_kms_impl(self) -> None:
        """Verify that a custom KMS implementation can be loaded by module path."""

        class _TestKms(KeyManagementClient):
            initialized_with: dict[str, str] = {}

            def wrap_key(self, key: bytes, wrapping_key_id: str) -> bytes:
                return key

            def unwrap_key(self, wrapped_key: bytes, wrapping_key_id: str) -> bytes:
                return wrapped_key

            def initialize(self, properties: dict[str, str]) -> None:
                _TestKms.initialized_with = properties

        # Register in the module namespace so importlib can find it
        import pyiceberg.encryption.kms as kms_module

        kms_module._TestKms = _TestKms  # type: ignore[attr-defined]
        try:
            client = load_kms_client({"py-kms-impl": "pyiceberg.encryption.kms._TestKms", "foo": "bar"})
            assert client is not None
            assert isinstance(client, _TestKms)
            assert _TestKms.initialized_with.get("foo") == "bar"
        finally:
            delattr(kms_module, "_TestKms")
