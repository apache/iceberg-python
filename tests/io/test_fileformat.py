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

from typing import Any

import pytest

from pyiceberg.io.fileformat import DataFileStatistics, FileFormatFactory, FileFormatModel, FileFormatWriter
from pyiceberg.manifest import FileFormat


def test_get_unregistered_format_raises() -> None:
    """Getting an unregistered format should raise ValueError."""
    with pytest.raises(ValueError, match="No writer registered for"):
        FileFormatFactory.get(FileFormat.AVRO)


def test_backward_compat_import() -> None:
    """DataFileStatistics can still be imported from pyiceberg.io.pyarrow."""
    from pyiceberg.io.fileformat import DataFileStatistics as dFS  # noqa: F401
    from pyiceberg.io.pyarrow import DataFileStatistics  # noqa: F401

    assert DataFileStatistics is dFS


def test_duplicate_registration_raises() -> None:
    """Registering the same format twice should raise ValueError."""

    class _DummyModel(FileFormatModel):
        @property
        def format(self) -> FileFormat:
            return FileFormat.ORC

        def file_extension(self) -> str:
            return "orc"

        def create_writer(self, output_file: Any, file_schema: Any, properties: Any) -> Any:
            raise NotImplementedError

    original = dict(FileFormatFactory._registry)
    try:
        model = _DummyModel()
        FileFormatFactory.register(model)
        with pytest.raises(ValueError, match="already registered"):
            FileFormatFactory.register(model)
    finally:
        FileFormatFactory._registry = original


def test_result_before_close_raises() -> None:
    """Calling result before close should raise an error."""

    class _DummyWriter(FileFormatWriter):
        def write(self, table: Any) -> None:
            pass

        def close(self) -> DataFileStatistics:
            raise NotImplementedError

    writer = _DummyWriter()
    with pytest.raises(RuntimeError, match="Writer has not been closed yet"):
        writer.result()
