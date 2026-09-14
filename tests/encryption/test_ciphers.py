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
from pytest_mock import MockFixture

from pyiceberg.encryption.ciphers import AesGcmCipher, AesKeySize, SecureKey
from pyiceberg.exceptions import NotInstalledError

AES128_KEY = b"0123456789012345"
PLAINTEXT = b"the quick brown fox"

# Known-answer vectors from McGrew & Viega, "The Galois/Counter Mode of Operation
# (GCM)", shared with the NIST GCM validation suite and the Java and iceberg-rust
# test suites. They pin the `nonce || ciphertext || tag` layout against changes
# that stay self-consistent on round trip but break cross-client interoperability.
GCM_TEST_VECTORS = [
    pytest.param(
        "feffe9928665731c6d6a8f9467308308",
        "cafebabefacedbaddecaf888",
        "d9313225f88406e5a55909c5aff5269a86a7a9531534f7da2e4c303d8a318a721c3c0c95956809532fcf0e2449a6b525b16aedf5aa0de657ba637b391aafd255",
        "",
        "42831ec2217774244b7221b784d0d49ce3aa212f2c02a4e035c17e2329aca12e21d514b25466931c7d8f6a5aac84aa051ba30b396a0aac973d58e091473f5985",
        "4d5c2af327cd64a62cf35abd2ba6fab4",
        id="aes128-no-aad",
    ),
    pytest.param(
        "feffe9928665731c6d6a8f9467308308",
        "cafebabefacedbaddecaf888",
        "d9313225f88406e5a55909c5aff5269a86a7a9531534f7da2e4c303d8a318a721c3c0c95956809532fcf0e2449a6b525b16aedf5aa0de657ba637b39",
        "feedfacedeadbeeffeedfacedeadbeefabaddad2",
        "42831ec2217774244b7221b784d0d49ce3aa212f2c02a4e035c17e2329aca12e21d514b25466931c7d8f6a5aac84aa051ba30b396a0aac973d58e091",
        "5bc94fbc3221a5db94fae95ae7121a47",
        id="aes128-with-aad",
    ),
    pytest.param(
        "feffe9928665731c6d6a8f9467308308feffe9928665731c6d6a8f9467308308",
        "cafebabefacedbaddecaf888",
        "d9313225f88406e5a55909c5aff5269a86a7a9531534f7da2e4c303d8a318a721c3c0c95956809532fcf0e2449a6b525b16aedf5aa0de657ba637b39",
        "feedfacedeadbeeffeedfacedeadbeefabaddad2",
        "522dc1f099567d07f47f37a32a84427d643a8cdcbfe5c0c97598a2bd2555d1aa8cb08e48590dbb3da7b08b1056828838c5f61e6393ba7a0abcc9f662",
        "76fc6ece0f4e1768cddf8853bb2d551b",
        id="aes256-with-aad",
    ),
]


@pytest.mark.parametrize(
    "key_length, key_size",
    [(16, AesKeySize.BITS_128), (24, AesKeySize.BITS_192), (32, AesKeySize.BITS_256)],
)
def test_key_size_from_key_length(key_length: int, key_size: AesKeySize) -> None:
    assert AesKeySize.from_key_length(key_length) == key_size
    assert key_size.key_length == key_length


@pytest.mark.parametrize("key_length", [0, 4, 15, 20, 33])
def test_key_size_rejects_invalid_key_length(key_length: int) -> None:
    with pytest.raises(ValueError, match=f"Unsupported key length: {key_length}"):
        AesKeySize.from_key_length(key_length)


@pytest.mark.parametrize("key_length", [0, 4, 15, 20, 33])
def test_secure_key_rejects_invalid_key_length(key_length: int) -> None:
    with pytest.raises(ValueError, match="Unsupported key length"):
        SecureKey(bytes(key_length))


@pytest.mark.parametrize("key_size", list(AesKeySize))
def test_secure_key_generate(key_size: AesKeySize) -> None:
    key = SecureKey.generate(key_size)

    assert len(key.key) == key_size.key_length
    assert key.key_size == key_size
    assert SecureKey.generate(key_size) != key


def test_secure_key_repr_redacts_key() -> None:
    key = SecureKey(AES128_KEY)

    assert repr(key) == "SecureKey()"
    assert repr(AES128_KEY) not in repr(key)


@pytest.mark.parametrize("key_size", list(AesKeySize))
@pytest.mark.parametrize("aad", [None, b"", b"aad"])
def test_encrypt_decrypt_round_trip(key_size: AesKeySize, aad: bytes | None) -> None:
    cipher = AesGcmCipher(SecureKey.generate(key_size))

    ciphertext = cipher.encrypt(PLAINTEXT, aad)

    assert ciphertext != PLAINTEXT
    assert cipher.decrypt(ciphertext, aad) == PLAINTEXT


@pytest.mark.parametrize("key, nonce, plaintext, aad, ciphertext, tag", GCM_TEST_VECTORS)
def test_decrypt_known_answer(key: str, nonce: str, plaintext: str, aad: str, ciphertext: str, tag: str) -> None:
    cipher = AesGcmCipher(SecureKey(bytes.fromhex(key)))
    stored = bytes.fromhex(nonce + ciphertext + tag)

    assert cipher.decrypt(stored, bytes.fromhex(aad) or None) == bytes.fromhex(plaintext)


@pytest.mark.parametrize("key, nonce, plaintext, aad, ciphertext, tag", GCM_TEST_VECTORS)
def test_encrypt_known_answer(
    monkeypatch: pytest.MonkeyPatch, key: str, nonce: str, plaintext: str, aad: str, ciphertext: str, tag: str
) -> None:
    monkeypatch.setattr("pyiceberg.encryption.ciphers.os.urandom", lambda _: bytes.fromhex(nonce))
    cipher = AesGcmCipher(SecureKey(bytes.fromhex(key)))

    assert cipher.encrypt(bytes.fromhex(plaintext), bytes.fromhex(aad) or None) == bytes.fromhex(nonce + ciphertext + tag)


def test_encrypt_empty_plaintext() -> None:
    cipher = AesGcmCipher(SecureKey(AES128_KEY))

    assert cipher.decrypt(cipher.encrypt(b"")) == b""


def test_ciphertext_layout() -> None:
    cipher = AesGcmCipher(SecureKey(AES128_KEY))

    ciphertext = cipher.encrypt(PLAINTEXT)

    assert len(ciphertext) == AesGcmCipher.NONCE_LENGTH + len(PLAINTEXT) + AesGcmCipher.TAG_LENGTH


def test_nonce_is_not_reused() -> None:
    cipher = AesGcmCipher(SecureKey(AES128_KEY))

    first, second = cipher.encrypt(PLAINTEXT), cipher.encrypt(PLAINTEXT)

    assert first[: AesGcmCipher.NONCE_LENGTH] != second[: AesGcmCipher.NONCE_LENGTH]
    assert first != second


def test_decrypt_with_wrong_key() -> None:
    ciphertext = AesGcmCipher(SecureKey(AES128_KEY)).encrypt(PLAINTEXT)

    with pytest.raises(ValueError, match="AES-GCM decryption failed"):
        AesGcmCipher(SecureKey(b"5432109876543210")).decrypt(ciphertext)


def test_decrypt_with_mismatched_aad() -> None:
    cipher = AesGcmCipher(SecureKey(AES128_KEY))

    ciphertext = cipher.encrypt(PLAINTEXT, b"aad")

    with pytest.raises(ValueError, match="AES-GCM decryption failed"):
        cipher.decrypt(ciphertext, b"other aad")


def test_decrypt_tampered_ciphertext() -> None:
    cipher = AesGcmCipher(SecureKey(AES128_KEY))

    ciphertext = bytearray(cipher.encrypt(PLAINTEXT))
    ciphertext[-1] ^= 0xFF

    with pytest.raises(ValueError, match="AES-GCM decryption failed"):
        cipher.decrypt(bytes(ciphertext))


@pytest.mark.parametrize("length", [0, 1, 27])
def test_decrypt_ciphertext_too_short(length: int) -> None:
    cipher = AesGcmCipher(SecureKey(AES128_KEY))

    with pytest.raises(ValueError, match=f"Ciphertext too short: expected at least 28 bytes, got {length}"):
        cipher.decrypt(bytes(length))


def test_cipher_without_cryptography_installed_raises_not_installed_error(mocker: MockFixture) -> None:
    mocker.patch.dict("sys.modules", {"cryptography.hazmat.primitives.ciphers.aead": None})

    with pytest.raises(NotInstalledError, match=r"pyiceberg\[encryption\]"):
        AesGcmCipher(SecureKey(AES128_KEY))
