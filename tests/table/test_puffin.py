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
import subprocess
import sys
from os import path

import pytest

from pyiceberg.avro.codecs.zstandard_codec import ZStandardCodec
from pyiceberg.table.puffin import PuffinFile
from pyiceberg.utils import zstd


def _open_file(file: str) -> bytes:
    cur_dir = path.dirname(path.realpath(__file__))
    with open(f"{cur_dir}/puffin/{file}", "rb") as f:
        return f.read()


def test_read_empty_uncompressed() -> None:
    puffin_bytes = _open_file("v1/empty-puffin-uncompressed.bin")
    pf = PuffinFile(puffin_bytes)

    assert pf.footer.blobs == []
    assert pf.footer.properties == {}


def test_read_compressed_zstd() -> None:
    puffin_bytes = _open_file("v1/sample-metric-data-compressed-zstd.bin")
    pf = PuffinFile(puffin_bytes)

    assert pf.footer.properties == {"created-by": "Test 1234"}
    assert len(pf.footer.blobs) == 2

    blob1 = pf.footer.blobs[0]
    assert blob1.type == "some-blob"
    assert blob1.fields == [1]
    assert blob1.snapshot_id == 2
    assert blob1.sequence_number == 1
    assert blob1.compression_codec == "zstd"
    assert pf.get_blob_payload(blob1) == b"abcdefghi"

    blob2 = pf.footer.blobs[1]
    assert blob2.type == "some-other-blob"
    assert blob2.fields == [2]
    assert blob2.compression_codec == "zstd"
    assert pf.get_blob_payload(blob2) == (
        b"some blob \x00 binary data \xf0\x9f\xa4\xaf that is not very very very very very very long, is it?"
    )


def test_read_two_blobs_uncompressed() -> None:
    puffin_bytes = _open_file("v1/sample-metric-data-uncompressed.bin")
    pf = PuffinFile(puffin_bytes)

    assert pf.footer.properties == {"created-by": "Test 1234"}
    assert len(pf.footer.blobs) == 2

    blob1 = pf.footer.blobs[0]
    assert blob1.type == "some-blob"
    assert blob1.fields == [1]
    assert blob1.snapshot_id == 2
    assert blob1.sequence_number == 1
    assert blob1.compression_codec is None
    assert pf.get_blob_payload(blob1) == b"abcdefghi"

    blob2 = pf.footer.blobs[1]
    assert blob2.type == "some-other-blob"
    assert blob2.fields == [2]
    assert blob2.compression_codec is None
    assert pf.get_blob_payload(blob2) == (
        b"some blob \x00 binary data \xf0\x9f\xa4\xaf that is not very very very very very very long, is it?"
    )


def test_zstd_roundtrip() -> None:
    data = b"hello iceberg" * 100
    compressed, size = ZStandardCodec.compress(data)

    assert size == len(compressed)
    assert ZStandardCodec.decompress(compressed) == data
    assert zstd.decompress(zstd.compress(data)) == data


def test_zstd_without_backend_raises_on_use(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(zstd, "_compress", None)
    monkeypatch.setattr(zstd, "_decompress", None)

    with pytest.raises(ImportError, match="Zstandard support not installed"):
        ZStandardCodec.compress(b"hello iceberg")

    puffin_bytes = _open_file("v1/sample-metric-data-compressed-zstd.bin")
    pf = PuffinFile(puffin_bytes)
    with pytest.raises(ImportError, match="Zstandard support not installed"):
        pf.get_blob_payload(pf.footer.blobs[0])


def test_zstd_decompress_truncated_frame() -> None:
    compressed, _ = ZStandardCodec.compress(b"hello iceberg" * 100)

    with pytest.raises(Exception, match="incomplete|truncated|ZstdError|error|end-of-stream"):
        ZStandardCodec.decompress(compressed[:-1])


def test_imports_without_zstd_backend() -> None:
    script = (
        "import sys\n"
        "from importlib.abc import MetaPathFinder\n"
        "class Blocker(MetaPathFinder):\n"
        "    def find_spec(self, name, path=None, target=None):\n"
        "        if name == 'zstandard' or name.startswith('zstandard.') or name in ('compression', 'compression.zstd'):\n"
        "            raise ImportError(f'No module named {name}')\n"
        "        return None\n"
        "sys.meta_path.insert(0, Blocker())\n"
        "import pyiceberg.manifest\n"
        "from pyiceberg.avro.codecs import ZStandardCodec\n"
        "from pyiceberg.table.puffin import PuffinFile\n"
        "try:\n"
        "    ZStandardCodec.compress(b'hi')\n"
        "    raise SystemExit('expected ImportError')\n"
        "except ImportError:\n"
        "    print('import-ok-raise-on-use')\n"
    )
    result = subprocess.run([sys.executable, "-c", script], capture_output=True, text=True, check=False)

    assert result.returncode == 0, result.stderr
    assert "import-ok-raise-on-use" in result.stdout


def test_zstd_decompress_concatenated_frames() -> None:
    first, _ = ZStandardCodec.compress(b"first")
    second, _ = ZStandardCodec.compress(b"second")

    assert ZStandardCodec.decompress(first + second) == b"firstsecond"
    with pytest.raises(Exception, match="incomplete|truncated|ZstdError|error|end-of-stream"):
        ZStandardCodec.decompress(first + second[:-1])
