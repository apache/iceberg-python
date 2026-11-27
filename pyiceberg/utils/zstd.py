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
"""Zstandard compression backed by the stdlib ``compression.zstd`` or the ``zstandard`` package.

The stdlib module is optional (PEP 784): it is only built when libzstd is
available, so fall back to the ``zstandard`` package when it is missing, and
only raise when neither is available.
"""

from __future__ import annotations

import sys
from collections.abc import Callable

_NOT_INSTALLED = "Zstandard support not installed, please install using `pip install zstandard`"

_compress: Callable[[bytes], bytes] | None = None
_decompress: Callable[[bytes], bytes] | None = None

if sys.version_info >= (3, 14):
    try:
        from compression import zstd

        _compress = zstd.compress
        _decompress = zstd.decompress
    except ImportError:
        pass

if _compress is None or _decompress is None:
    try:
        from zstandard import ZstdCompressor, ZstdDecompressor, ZstdError

        def _compress_with_zstandard(data: bytes) -> bytes:
            return ZstdCompressor().compress(data)

        def _decompress_with_zstandard(data: bytes) -> bytes:
            uncompressed = bytearray()
            remaining = data
            while remaining:
                decompressor = ZstdDecompressor().decompressobj()
                uncompressed.extend(decompressor.decompress(remaining))
                if not decompressor.eof:
                    raise ZstdError("Zstandard frame is incomplete or truncated")
                remaining = decompressor.unused_data
            return bytes(uncompressed)

        _compress = _compress_with_zstandard
        _decompress = _decompress_with_zstandard
    except ImportError:
        pass


def compress(data: bytes) -> bytes:
    if _compress is None:
        raise ImportError(_NOT_INSTALLED)
    return _compress(data)


def decompress(data: bytes) -> bytes:
    if _decompress is None:
        raise ImportError(_NOT_INSTALLED)
    return _decompress(data)
