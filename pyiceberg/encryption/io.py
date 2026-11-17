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
"""Input files for metadata encrypted with AGS1 streams."""

from __future__ import annotations

import io

from pyiceberg.encryption.key_metadata import StandardKeyMetadata
from pyiceberg.encryption.stream import _decrypt_ags1
from pyiceberg.io import InputFile, InputStream


class EncryptedInputFile(InputFile):
    """Decrypts an AGS1 encrypted file, such as a manifest list, when it is opened."""

    def __init__(self, input_file: InputFile, key_metadata: StandardKeyMetadata) -> None:
        super().__init__(input_file.location)
        self._input_file = input_file
        self._key_metadata = key_metadata

    def __len__(self) -> int:
        """Return the length of the decrypted file, in bytes."""
        return len(self._decrypt())

    def exists(self) -> bool:
        """Check whether the encrypted file exists."""
        return self._input_file.exists()

    def open(self, seekable: bool = True) -> InputStream:
        """Read and decrypt the whole file, authenticating every block before returning any of it."""
        return io.BytesIO(self._decrypt())

    def _decrypt(self) -> bytes:
        with self._input_file.open(seekable=False) as stream:
            return _decrypt_ags1(stream.read(), self._key_metadata)
