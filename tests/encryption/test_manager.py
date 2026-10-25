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
import time
from typing import Any

import pytest
from memory_kms import MemoryKeyManagementClient

from pyiceberg.encryption.ciphers import AesGcmCipher, AesKeySize, SecureKey
from pyiceberg.encryption.key_metadata import StandardKeyMetadata
from pyiceberg.encryption.kms import GeneratedKey
from pyiceberg.encryption.manager import KEK_CREATED_AT_PROPERTY, EncryptionManager
from pyiceberg.table.metadata import EncryptedKey, TableMetadataUtil

MASTER_KEY_ID = "master-1"
KEY_METADATA = StandardKeyMetadata(encryption_key=b"0123456789abcdef", aad_prefix=b"test-aad-prefix!", file_length=1024)
THREE_YEARS_MS = 3 * 365 * 24 * 60 * 60 * 1000


class CountingKeyManagementClient(MemoryKeyManagementClient):
    unwrap_calls = 0

    def unwrap_key(self, wrapped_key: bytes, wrapping_key_id: str) -> bytes:
        self.unwrap_calls += 1
        return super().unwrap_key(wrapped_key, wrapping_key_id)


class KeyGeneratingKeyManagementClient(MemoryKeyManagementClient):
    def supports_key_generation(self) -> bool:
        return True

    def generate_key(self, wrapping_key_id: str) -> GeneratedKey:
        key = SecureKey.generate().key
        return GeneratedKey(key=key, wrapped_key=self.wrap_key(key, wrapping_key_id))


@pytest.fixture
def kms() -> CountingKeyManagementClient:
    client = CountingKeyManagementClient()
    client.add_master_key(MASTER_KEY_ID)
    return client


@pytest.fixture
def manager(kms: CountingKeyManagementClient) -> EncryptionManager:
    return EncryptionManager(kms, MASTER_KEY_ID)


def kek_created_at(kms: MemoryKeyManagementClient, key_id: str, created_at_ms: int) -> EncryptedKey:
    return EncryptedKey(
        key_id=key_id,
        encrypted_key_metadata=kms.wrap_key(SecureKey.generate().key, MASTER_KEY_ID),
        encrypted_by_id=MASTER_KEY_ID,
        properties={KEK_CREATED_AT_PROPERTY: str(created_at_ms)},
    )


def now_ms() -> int:
    return time.time_ns() // 1_000_000


def test_first_wrap_creates_a_kek(manager: EncryptionManager) -> None:
    key_id = manager.encrypt_manifest_list_key_metadata(KEY_METADATA)

    kek, manifest_list_key = manager.encryption_keys
    assert kek.encrypted_by_id == MASTER_KEY_ID
    assert kek.encrypted_key_metadata
    assert abs(int(kek.properties[KEK_CREATED_AT_PROPERTY]) - now_ms()) < 60_000
    assert manifest_list_key.key_id == key_id
    assert manifest_list_key.encrypted_by_id == kek.key_id
    assert manifest_list_key.properties == {}


def test_wrap_unwrap_round_trip(manager: EncryptionManager) -> None:
    key_id = manager.encrypt_manifest_list_key_metadata(KEY_METADATA)

    assert manager.decrypt_manifest_list_key_metadata(key_id) == KEY_METADATA


def test_unwrap_with_keys_from_another_manager(kms: CountingKeyManagementClient, manager: EncryptionManager) -> None:
    key_id = manager.encrypt_manifest_list_key_metadata(KEY_METADATA)

    reader = EncryptionManager(kms, MASTER_KEY_ID, manager.encryption_keys)

    assert reader.decrypt_manifest_list_key_metadata(key_id) == KEY_METADATA


def test_unwrap_key_metadata_wrapped_as_java_does(kms: CountingKeyManagementClient) -> None:
    """Java's `EncryptionUtil.encryptManifestListKeyMetadata` wraps with AES-GCM, using the KEK timestamp as AAD."""
    kek_key = SecureKey.generate()
    kek = EncryptedKey(
        key_id="kek",
        encrypted_key_metadata=kms.wrap_key(kek_key.key, MASTER_KEY_ID),
        encrypted_by_id=MASTER_KEY_ID,
        properties={KEK_CREATED_AT_PROPERTY: "1700000000000"},
    )
    manifest_list_key = EncryptedKey(
        key_id="manifest-list-key",
        encrypted_key_metadata=AesGcmCipher(kek_key).encrypt(KEY_METADATA.to_bytes(), b"1700000000000"),
        encrypted_by_id="kek",
    )

    manager = EncryptionManager(kms, MASTER_KEY_ID, [kek, manifest_list_key])

    assert manager.decrypt_manifest_list_key_metadata("manifest-list-key") == KEY_METADATA


def test_kek_is_reused_while_active(manager: EncryptionManager) -> None:
    first = manager.encrypt_manifest_list_key_metadata(KEY_METADATA)
    second = manager.encrypt_manifest_list_key_metadata(KEY_METADATA)

    kek, first_key, second_key = manager.encryption_keys
    assert (first_key.key_id, second_key.key_id) == (first, second)
    assert first_key.encrypted_by_id == second_key.encrypted_by_id == kek.key_id


def test_kek_is_rotated_once_expired(kms: CountingKeyManagementClient) -> None:
    expired = kek_created_at(kms, "expired-kek", now_ms() - THREE_YEARS_MS)
    manager = EncryptionManager(kms, MASTER_KEY_ID, [expired])

    key_id = manager.encrypt_manifest_list_key_metadata(KEY_METADATA)

    old_kek, new_kek, manifest_list_key = manager.encryption_keys
    assert old_kek == expired
    assert new_kek.key_id != expired.key_id
    assert manifest_list_key.encrypted_by_id == new_kek.key_id
    assert manager.decrypt_manifest_list_key_metadata(key_id) == KEY_METADATA


@pytest.mark.parametrize("properties", [{}, {KEK_CREATED_AT_PROPERTY: "not-a-timestamp"}])
def test_kek_without_a_valid_timestamp_is_not_reused(kms: CountingKeyManagementClient, properties: dict[str, str]) -> None:
    kek = EncryptedKey(
        key_id="kek",
        encrypted_key_metadata=kms.wrap_key(SecureKey.generate().key, MASTER_KEY_ID),
        encrypted_by_id=MASTER_KEY_ID,
        properties=properties,
    )
    manager = EncryptionManager(kms, MASTER_KEY_ID, [kek])

    manager.encrypt_manifest_list_key_metadata(KEY_METADATA)

    _, new_kek, manifest_list_key = manager.encryption_keys
    assert new_kek.encrypted_by_id == MASTER_KEY_ID
    assert manifest_list_key.encrypted_by_id == new_kek.key_id


def test_newest_active_kek_is_used(kms: CountingKeyManagementClient) -> None:
    newer = kek_created_at(kms, "newer", now_ms() - 1_000)
    older = kek_created_at(kms, "older", now_ms() - 2_000)
    manager = EncryptionManager(kms, MASTER_KEY_ID, [newer, older])

    key_id = manager.encrypt_manifest_list_key_metadata(KEY_METADATA)

    assert len(manager.encryption_keys) == 3
    assert manager.encryption_keys[-1].key_id == key_id
    assert manager.encryption_keys[-1].encrypted_by_id == "newer"


def test_kek_wrapped_by_another_master_key_is_not_used(kms: CountingKeyManagementClient) -> None:
    kms.add_master_key("master-2")
    other = EncryptedKey(
        key_id="other",
        encrypted_key_metadata=kms.wrap_key(SecureKey.generate().key, "master-2"),
        encrypted_by_id="master-2",
        properties={KEK_CREATED_AT_PROPERTY: str(now_ms())},
    )
    manager = EncryptionManager(kms, MASTER_KEY_ID, [other])

    manager.encrypt_manifest_list_key_metadata(KEY_METADATA)

    _, new_kek, manifest_list_key = manager.encryption_keys
    assert new_kek.encrypted_by_id == MASTER_KEY_ID
    assert manifest_list_key.encrypted_by_id == new_kek.key_id


def test_kek_is_generated_by_the_kms_when_supported() -> None:
    kms = KeyGeneratingKeyManagementClient()
    kms.add_master_key(MASTER_KEY_ID)
    manager = EncryptionManager(kms, MASTER_KEY_ID)

    key_id = manager.encrypt_manifest_list_key_metadata(KEY_METADATA)

    assert (
        EncryptionManager(kms, MASTER_KEY_ID, manager.encryption_keys).decrypt_manifest_list_key_metadata(key_id) == KEY_METADATA
    )


def test_unwrapped_kek_is_cached(kms: CountingKeyManagementClient, manager: EncryptionManager) -> None:
    key_id = manager.encrypt_manifest_list_key_metadata(KEY_METADATA)
    manager.encrypt_manifest_list_key_metadata(KEY_METADATA)
    manager.decrypt_manifest_list_key_metadata(key_id)
    assert kms.unwrap_calls == 0

    reader = EncryptionManager(kms, MASTER_KEY_ID, manager.encryption_keys)
    reader.decrypt_manifest_list_key_metadata(key_id)
    reader.decrypt_manifest_list_key_metadata(key_id)
    assert kms.unwrap_calls == 1


def test_unwrap_unknown_key_id(manager: EncryptionManager) -> None:
    with pytest.raises(ValueError, match="Encryption key not found: missing"):
        manager.decrypt_manifest_list_key_metadata("missing")


def test_unwrap_key_without_encrypted_by_id(kms: CountingKeyManagementClient) -> None:
    manager = EncryptionManager(kms, MASTER_KEY_ID, [EncryptedKey(key_id="key", encrypted_key_metadata=b"wrapped")])

    with pytest.raises(ValueError, match="Encryption key key has no encrypted-by-id"):
        manager.decrypt_manifest_list_key_metadata("key")


def test_unwrap_key_with_missing_kek(kms: CountingKeyManagementClient) -> None:
    key = EncryptedKey(key_id="key", encrypted_key_metadata=b"wrapped", encrypted_by_id="missing-kek")
    manager = EncryptionManager(kms, MASTER_KEY_ID, [key])

    with pytest.raises(ValueError, match="KEK not found in encryption keys: missing-kek"):
        manager.decrypt_manifest_list_key_metadata("key")


def replace_kek_properties(manager: EncryptionManager, key_id: str, properties: dict[str, str]) -> list[EncryptedKey]:
    kek_id = next(key.encrypted_by_id for key in manager.encryption_keys if key.key_id == key_id)
    return [key.model_copy(update={"properties": properties}) if key.key_id == kek_id else key for key in manager.encryption_keys]


def test_unwrap_fails_when_kek_timestamp_is_missing(kms: CountingKeyManagementClient, manager: EncryptionManager) -> None:
    key_id = manager.encrypt_manifest_list_key_metadata(KEY_METADATA)
    reader = EncryptionManager(kms, MASTER_KEY_ID, replace_kek_properties(manager, key_id, {}))

    with pytest.raises(ValueError, match=f"is missing required {KEK_CREATED_AT_PROPERTY} property"):
        reader.decrypt_manifest_list_key_metadata(key_id)


def test_unwrap_fails_when_kek_timestamp_is_tampered(kms: CountingKeyManagementClient, manager: EncryptionManager) -> None:
    key_id = manager.encrypt_manifest_list_key_metadata(KEY_METADATA)
    reader = EncryptionManager(kms, MASTER_KEY_ID, replace_kek_properties(manager, key_id, {KEK_CREATED_AT_PROPERTY: "9999999"}))

    with pytest.raises(ValueError, match="Failed to unwrap key metadata with KEK"):
        reader.decrypt_manifest_list_key_metadata(key_id)


@pytest.mark.parametrize("key_size", list(AesKeySize))
def test_generate_key_metadata(kms: CountingKeyManagementClient, key_size: AesKeySize) -> None:
    key_metadata = EncryptionManager(kms, MASTER_KEY_ID, key_size=key_size).generate_key_metadata()

    assert len(key_metadata.encryption_key) == key_size.key_length


def test_from_table_metadata(kms: CountingKeyManagementClient, example_table_metadata_v3: dict[str, Any]) -> None:
    kek = kek_created_at(kms, "kek", now_ms())
    metadata = TableMetadataUtil.parse_obj(
        {
            **example_table_metadata_v3,
            "properties": {"encryption.key-id": MASTER_KEY_ID, "encryption.data-key-length": "32"},
            "encryption-keys": [kek.model_dump(by_alias=True, mode="json")],
        }
    )

    manager = EncryptionManager.from_table_metadata(kms, metadata)

    assert manager is not None
    assert manager.encryption_keys == [kek]
    assert len(manager.generate_key_metadata().encryption_key) == 32
    manager.encrypt_manifest_list_key_metadata(KEY_METADATA)
    assert manager.encryption_keys[-1].encrypted_by_id == "kek"


def test_from_table_metadata_defaults_to_128_bit_keys(
    kms: CountingKeyManagementClient, example_table_metadata_v3: dict[str, Any]
) -> None:
    metadata = TableMetadataUtil.parse_obj({**example_table_metadata_v3, "properties": {"encryption.key-id": MASTER_KEY_ID}})

    manager = EncryptionManager.from_table_metadata(kms, metadata)

    assert manager is not None
    assert len(manager.generate_key_metadata().encryption_key) == 16


def test_from_table_metadata_without_key_id(kms: CountingKeyManagementClient, example_table_metadata_v3: dict[str, Any]) -> None:
    metadata = TableMetadataUtil.parse_obj({**example_table_metadata_v3, "properties": {}})

    assert EncryptionManager.from_table_metadata(kms, metadata) is None
    assert EncryptionManager.from_table_metadata(None, metadata) is None


def test_from_table_metadata_before_v3(kms: CountingKeyManagementClient, example_table_metadata_v2: dict[str, Any]) -> None:
    metadata = TableMetadataUtil.parse_obj({**example_table_metadata_v2, "properties": {"encryption.key-id": MASTER_KEY_ID}})

    with pytest.raises(ValueError, match="Invalid properties for v2: encryption.key-id"):
        EncryptionManager.from_table_metadata(kms, metadata)


def test_from_table_metadata_before_v3_without_key_id(
    kms: CountingKeyManagementClient, example_table_metadata_v2: dict[str, Any]
) -> None:
    metadata = TableMetadataUtil.parse_obj({**example_table_metadata_v2, "properties": {}})

    assert EncryptionManager.from_table_metadata(kms, metadata) is None


def test_from_table_metadata_without_kms_client(example_table_metadata_v3: dict[str, Any]) -> None:
    metadata = TableMetadataUtil.parse_obj({**example_table_metadata_v3, "properties": {"encryption.key-id": MASTER_KEY_ID}})

    with pytest.raises(ValueError, match="Table has encryption.key-id set but no key management client was provided"):
        EncryptionManager.from_table_metadata(None, metadata)


def test_from_table_metadata_with_invalid_data_key_length(
    kms: CountingKeyManagementClient, example_table_metadata_v3: dict[str, Any]
) -> None:
    metadata = TableMetadataUtil.parse_obj(
        {**example_table_metadata_v3, "properties": {"encryption.key-id": MASTER_KEY_ID, "encryption.data-key-length": "20"}}
    )

    with pytest.raises(ValueError, match="Unsupported key length: 20"):
        EncryptionManager.from_table_metadata(kms, metadata)
