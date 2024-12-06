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

from pyiceberg.manifest import FileFormat


@pytest.mark.parametrize(
    "raw_file_format,expected_file_format",
    [
        ("avro", FileFormat("AVRO")),
        ("AVRO", FileFormat("AVRO")),
        ("parquet", FileFormat("PARQUET")),
        ("PARQUET", FileFormat("PARQUET")),
        ("orc", FileFormat("ORC")),
        ("ORC", FileFormat("ORC")),
        ("NOT_EXISTS", None),
    ],
)
def test_file_format_case_insensitive(raw_file_format: str, expected_file_format: FileFormat) -> None:
    if expected_file_format:
        parsed_file_format = FileFormat(raw_file_format)
        assert parsed_file_format == expected_file_format, (
            f"File format {raw_file_format}: {parsed_file_format} != {expected_file_format}"
        )
    else:
        with pytest.raises(ValueError):
            _ = FileFormat(raw_file_format)
