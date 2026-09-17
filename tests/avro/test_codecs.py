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
from types import ModuleType

import pytest

from pyiceberg.avro.codecs import bzip2, deflate, zstandard_codec
from pyiceberg.avro.codecs.codec import Codec

CODEC_MODULES = [
    (bzip2, bzip2.BZip2Codec),
    (deflate, deflate.DeflateCodec),
    (zstandard_codec, zstandard_codec.ZStandardCodec),
]


@pytest.mark.parametrize("module, codec", CODEC_MODULES)
def test_roundtrip(module: ModuleType, codec: type[Codec]) -> None:
    data = b"aaaaaaaaaa" * 1000

    compressed, _ = codec.compress(data)

    assert codec.decompress(compressed) == data


@pytest.mark.parametrize("module, codec", CODEC_MODULES)
def test_decompress_stops_at_the_limit(module: ModuleType, codec: type[Codec], monkeypatch: pytest.MonkeyPatch) -> None:
    # A highly compressible block expands far beyond its compressed size, so the
    # decoder must refuse it rather than let the block decide how much it allocates.
    compressed, compressed_size = codec.compress(b"\x00" * 1_000_000)
    monkeypatch.setattr(module, "MAX_DECOMPRESSED_BLOCK_SIZE", 1024)

    assert compressed_size < 1024

    with pytest.raises(ValueError, match="Decompressed block exceeds the maximum of 1024 bytes"):
        codec.decompress(compressed)
