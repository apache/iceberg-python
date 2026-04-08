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
from pyiceberg.encryption.io import BytesInputFile, BytesInputStream
from pyiceberg.io import InputStream


class TestBytesInputStream:
    def test_read_all(self) -> None:
        stream = BytesInputStream(b"hello")
        assert stream.read() == b"hello"

    def test_read_partial(self) -> None:
        stream = BytesInputStream(b"hello world")
        assert stream.read(5) == b"hello"
        assert stream.read(6) == b" world"

    def test_seek_and_tell(self) -> None:
        stream = BytesInputStream(b"abcdef")
        assert stream.tell() == 0
        stream.seek(3)
        assert stream.tell() == 3
        assert stream.read(2) == b"de"

    def test_context_manager(self) -> None:
        with BytesInputStream(b"data") as stream:
            assert stream.read() == b"data"

    def test_implements_input_stream_protocol(self) -> None:
        stream = BytesInputStream(b"test")
        assert isinstance(stream, InputStream)


class TestBytesInputFile:
    def test_len(self) -> None:
        f = BytesInputFile("file://test", b"hello")
        assert len(f) == 5

    def test_exists(self) -> None:
        f = BytesInputFile("file://test", b"")
        assert f.exists() is True

    def test_open_and_read(self) -> None:
        f = BytesInputFile("file://test", b"content")
        with f.open() as stream:
            assert stream.read() == b"content"

    def test_location(self) -> None:
        f = BytesInputFile("s3://bucket/path", b"data")
        assert f.location == "s3://bucket/path"
