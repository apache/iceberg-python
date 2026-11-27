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
"""Input and output files for metadata encrypted with AGS1 streams."""

from __future__ import annotations

import io
from collections.abc import Callable
from dataclasses import replace

from typing_extensions import override

from pyiceberg.encryption.key_metadata import StandardKeyMetadata
from pyiceberg.encryption.stream import _decrypt_ags1, _encrypt_ags1
from pyiceberg.io import InputFile, InputStream, OutputFile, OutputStream


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


class EncryptedOutputFile(OutputFile):
    """Encrypts a file, such as a manifest, into an AGS1 stream when its output stream is closed."""

    def __init__(self, output_file: OutputFile, key_metadata: StandardKeyMetadata) -> None:
        super().__init__(output_file.location)
        self._output_file = output_file
        self._key_metadata = key_metadata
        self._file_length: int | None = None

    @property
    def key_metadata(self) -> StandardKeyMetadata:
        """The key metadata to record for the file, including the length of the written file."""
        if self._file_length is None:
            raise ValueError(f"Cannot get key metadata before the file is written: {self.location}")
        return replace(self._key_metadata, file_length=self._file_length)

    @override
    def __len__(self) -> int:
        """Return the length of the encrypted file, in bytes."""
        return self._file_length if self._file_length is not None else len(self._output_file)

    @override
    def exists(self) -> bool:
        """Check whether the file exists."""
        return self._output_file.exists()

    @override
    def to_input_file(self) -> InputFile:
        """Return an input file that decrypts the written file."""
        return EncryptedInputFile(self._output_file.to_input_file(), self.key_metadata)

    @override
    def create(self, overwrite: bool = False) -> OutputStream:
        """Return a stream that buffers the file, then encrypts and writes it when closed."""
        return _EncryptingOutputStream(self._output_file.create(overwrite=overwrite), self._key_metadata, self._set_file_length)

    def _set_file_length(self, file_length: int) -> None:
        self._file_length = file_length


class _EncryptingOutputStream(io.BytesIO):
    def __init__(self, output_stream: OutputStream, key_metadata: StandardKeyMetadata, on_close: Callable[[int], None]) -> None:
        super().__init__()
        self._output_stream = output_stream
        self._key_metadata = key_metadata
        self._on_close = on_close

    def close(self) -> None:
        if self.closed:
            return
        try:
            encrypted = _encrypt_ags1(self.getvalue(), self._key_metadata)
            with self._output_stream:
                self._output_stream.write(encrypted)
            self._on_close(len(encrypted))
        finally:
            # Close even if the write fails, so garbage collection does not retry it.
            super().close()
