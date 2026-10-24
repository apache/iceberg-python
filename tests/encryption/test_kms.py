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

import pytest
from memory_kms import MemoryKeyManagementClient

from pyiceberg.encryption.ciphers import AesGcmCipher, AesKeySize, SecureKey
from pyiceberg.encryption.kms import GeneratedKey, KeyManagementClient

MASTER_KEY_ID = "master-key"
MASTER_KEY = SecureKey(b"0123456789012345")
DEK = b"6543210987654321"


@pytest.fixture
def kms() -> MemoryKeyManagementClient:
    client = MemoryKeyManagementClient()
    client.add_master_key(MASTER_KEY_ID)
    return client


def test_key_management_client_cannot_be_instantiated() -> None:
    with pytest.raises(TypeError, match="abstract"):
        KeyManagementClient()  # type: ignore[abstract]


def test_client_keeps_its_properties() -> None:
    assert MemoryKeyManagementClient().properties == {}
    assert MemoryKeyManagementClient({"kms.key": "value"}).properties == {"kms.key": "value"}


def test_generated_key_repr_redacts_key() -> None:
    generated = GeneratedKey(key=DEK, wrapped_key=b"wrapped")

    assert repr(generated) == "GeneratedKey(wrapped_key=b'wrapped')"
    assert repr(DEK) not in repr(generated)


def test_key_generation_is_unsupported_by_default(kms: MemoryKeyManagementClient) -> None:
    assert kms.supports_key_generation() is False

    with pytest.raises(NotImplementedError, match="MemoryKeyManagementClient does not support key generation"):
        kms.generate_key(MASTER_KEY_ID)


def test_wrap_unwrap_round_trip(kms: MemoryKeyManagementClient) -> None:
    wrapped = kms.wrap_key(DEK, MASTER_KEY_ID)

    assert wrapped != DEK
    assert kms.unwrap_key(wrapped, MASTER_KEY_ID) == DEK


@pytest.mark.parametrize("key_size", list(AesKeySize))
def test_wrap_unwrap_round_trip_for_each_master_key_size(key_size: AesKeySize) -> None:
    kms = MemoryKeyManagementClient(master_key_size=key_size)
    master_key = kms.add_master_key(MASTER_KEY_ID)

    assert master_key.key_size == key_size
    assert kms.unwrap_key(kms.wrap_key(DEK, MASTER_KEY_ID), MASTER_KEY_ID) == DEK


def test_wrap_key_does_not_reuse_nonce(kms: MemoryKeyManagementClient) -> None:
    first, second = kms.wrap_key(DEK, MASTER_KEY_ID), kms.wrap_key(DEK, MASTER_KEY_ID)

    assert first != second
    assert kms.unwrap_key(first, MASTER_KEY_ID) == kms.unwrap_key(second, MASTER_KEY_ID) == DEK


def test_wrap_key_is_not_bound_to_the_wrapping_key_id(kms: MemoryKeyManagementClient) -> None:
    """No AAD is used when wrapping, matching Java's `MemoryMockKMS` and iceberg-rust."""
    kms.add_master_key("other-key", MASTER_KEY)
    kms.add_master_key("same-key-different-id", MASTER_KEY)

    wrapped = kms.wrap_key(DEK, "other-key")

    assert kms.unwrap_key(wrapped, "same-key-different-id") == DEK


def test_generated_master_keys_are_unique() -> None:
    kms = MemoryKeyManagementClient()

    assert kms.add_master_key("first") != kms.add_master_key("second")


def test_add_master_key_with_known_key_material() -> None:
    kms = MemoryKeyManagementClient()

    assert kms.add_master_key(MASTER_KEY_ID, MASTER_KEY) == MASTER_KEY
    assert AesGcmCipher(MASTER_KEY).decrypt(kms.wrap_key(DEK, MASTER_KEY_ID)) == DEK


def test_add_master_key_rejects_a_duplicate_id(kms: MemoryKeyManagementClient) -> None:
    with pytest.raises(ValueError, match=f"Master key already exists: {MASTER_KEY_ID}"):
        kms.add_master_key(MASTER_KEY_ID)


@pytest.mark.parametrize("key_length", [0, 15, 33])
def test_add_master_key_rejects_an_invalid_key_length(key_length: int) -> None:
    with pytest.raises(ValueError, match="Unsupported key length"):
        MemoryKeyManagementClient().add_master_key(MASTER_KEY_ID, SecureKey(bytes(key_length)))


def test_wrap_key_with_an_unknown_master_key_id(kms: MemoryKeyManagementClient) -> None:
    with pytest.raises(ValueError, match="Master key not found: missing-key"):
        kms.wrap_key(DEK, "missing-key")


def test_unwrap_key_with_an_unknown_master_key_id(kms: MemoryKeyManagementClient) -> None:
    with pytest.raises(ValueError, match="Master key not found: missing-key"):
        kms.unwrap_key(kms.wrap_key(DEK, MASTER_KEY_ID), "missing-key")


def test_unwrap_key_with_the_wrong_master_key(kms: MemoryKeyManagementClient) -> None:
    wrapped = kms.wrap_key(DEK, MASTER_KEY_ID)
    kms.add_master_key("other-key")

    with pytest.raises(ValueError, match="wrong decryption key; or corrupt/tampered data"):
        kms.unwrap_key(wrapped, "other-key")


def test_unwrap_tampered_key(kms: MemoryKeyManagementClient) -> None:
    wrapped = bytearray(kms.wrap_key(DEK, MASTER_KEY_ID))
    wrapped[-1] ^= 0xFF

    with pytest.raises(ValueError, match="wrong decryption key; or corrupt/tampered data"):
        kms.unwrap_key(bytes(wrapped), MASTER_KEY_ID)


def test_repr_redacts_master_keys(kms: MemoryKeyManagementClient) -> None:
    kms.add_master_key("other-key", MASTER_KEY)

    assert repr(kms) == "MemoryKeyManagementClient(master_key_size=<AesKeySize.BITS_128: 128>, key_count=2)"
    assert repr(MASTER_KEY.key) not in repr(kms)
