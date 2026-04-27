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

from pyiceberg.encryption.key_metadata import StandardKeyMetadata


class TestStandardKeyMetadata:
    def test_roundtrip_key_only(self) -> None:
        key = os.urandom(16)
        original = StandardKeyMetadata(encryption_key=key)
        serialized = original.serialize()
        restored = StandardKeyMetadata.deserialize(serialized)
        assert restored.encryption_key == key
        assert restored.aad_prefix == b""
        assert restored.file_length is None

    def test_roundtrip_with_aad_prefix(self) -> None:
        key = os.urandom(16)
        aad = os.urandom(8)
        original = StandardKeyMetadata(encryption_key=key, aad_prefix=aad)
        serialized = original.serialize()
        restored = StandardKeyMetadata.deserialize(serialized)
        assert restored.encryption_key == key
        assert restored.aad_prefix == aad
        assert restored.file_length is None

    def test_roundtrip_all_fields(self) -> None:
        key = os.urandom(32)
        aad = os.urandom(16)
        original = StandardKeyMetadata(encryption_key=key, aad_prefix=aad, file_length=12345)
        serialized = original.serialize()
        restored = StandardKeyMetadata.deserialize(serialized)
        assert restored.encryption_key == key
        assert restored.aad_prefix == aad
        assert restored.file_length == 12345

    def test_version_byte(self) -> None:
        """First byte should always be 0x01."""
        key = os.urandom(16)
        serialized = StandardKeyMetadata(encryption_key=key).serialize()
        assert serialized[0] == 0x01

    def test_deserialize_empty(self) -> None:
        with pytest.raises(ValueError, match="Empty key metadata"):
            StandardKeyMetadata.deserialize(b"")

    def test_deserialize_wrong_version(self) -> None:
        with pytest.raises(ValueError, match="Unsupported key metadata version"):
            StandardKeyMetadata.deserialize(b"\x02\x00")

    def test_frozen(self) -> None:
        """StandardKeyMetadata is a frozen dataclass."""
        skm = StandardKeyMetadata(encryption_key=b"key")
        with pytest.raises(AttributeError):
            skm.encryption_key = b"other"  # type: ignore[misc]

    def test_roundtrip_large_file_length(self) -> None:
        """Zigzag encoding should handle large values correctly."""
        key = os.urandom(16)
        original = StandardKeyMetadata(encryption_key=key, file_length=2**40)
        serialized = original.serialize()
        restored = StandardKeyMetadata.deserialize(serialized)
        assert restored.file_length == 2**40
