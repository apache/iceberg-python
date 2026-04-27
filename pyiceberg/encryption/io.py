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
"""InputFile implementation backed by in-memory bytes."""

from __future__ import annotations

import io
from types import TracebackType

from pyiceberg.io import InputFile, InputStream


class BytesInputStream(InputStream):
    """InputStream implementation backed by a bytes buffer."""

    def __init__(self, data: bytes) -> None:
        self._buffer = io.BytesIO(data)

    def read(self, size: int = 0) -> bytes:
        if size <= 0:
            return self._buffer.read()
        return self._buffer.read(size)

    def seek(self, offset: int, whence: int = 0) -> int:
        return self._buffer.seek(offset, whence)

    def tell(self) -> int:
        return self._buffer.tell()

    def close(self) -> None:
        self._buffer.close()

    def __enter__(self) -> BytesInputStream:
        """Enter the context manager."""
        return self

    def __exit__(
        self,
        exctype: type[BaseException] | None,
        excinst: BaseException | None,
        exctb: TracebackType | None,
    ) -> None:
        """Exit the context manager and close the stream."""
        self.close()


class BytesInputFile(InputFile):
    """InputFile implementation backed by in-memory bytes.

    Used to wrap decrypted data so that it can be read by
    AvroFile and other readers that expect an InputFile.
    """

    def __init__(self, location: str, data: bytes) -> None:
        super().__init__(location)
        self._data = data

    def __len__(self) -> int:
        """Return the length of the underlying data."""
        return len(self._data)

    def exists(self) -> bool:
        return True

    def open(self, seekable: bool = True) -> InputStream:
        return BytesInputStream(self._data)
