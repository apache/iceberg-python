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

import json
import os
import uuid
from typing import Any

import pytest
from pytest_mock import MockFixture

from pyiceberg.serializers import ToOutputFile
from pyiceberg.table import StaticTable
from pyiceberg.table.metadata import TableMetadataV1
from pyiceberg.table.update import AssertRefSnapshotId, TableRequirement
from pyiceberg.typedef import IcebergBaseModel


def test_legacy_current_snapshot_id(
    mocker: MockFixture, tmp_path_factory: pytest.TempPathFactory, example_table_metadata_no_snapshot_v1: dict[str, Any]
) -> None:
    from pyiceberg.io.pyarrow import PyArrowFileIO

    metadata_location = str(tmp_path_factory.mktemp("metadata") / f"{uuid.uuid4()}.metadata.json")
    metadata = TableMetadataV1(**example_table_metadata_no_snapshot_v1)
    ToOutputFile.table_metadata(metadata, PyArrowFileIO().new_output(location=metadata_location), overwrite=True)
    static_table = StaticTable.from_metadata(metadata_location)
    assert static_table.metadata.current_snapshot_id is None

    mocker.patch.dict(os.environ, values={"PYICEBERG_LEGACY_CURRENT_SNAPSHOT_ID": "True"})

    ToOutputFile.table_metadata(metadata, PyArrowFileIO().new_output(location=metadata_location), overwrite=True)
    with PyArrowFileIO().new_input(location=metadata_location).open() as input_stream:
        metadata_json_bytes = input_stream.read()
    assert json.loads(metadata_json_bytes)["current-snapshot-id"] == -1
    backwards_compatible_static_table = StaticTable.from_metadata(metadata_location)
    assert backwards_compatible_static_table.metadata.current_snapshot_id is None
    assert backwards_compatible_static_table.metadata == static_table.metadata


def test_null_serializer_field() -> None:
    class ExampleRequest(IcebergBaseModel):
        requirements: tuple[TableRequirement, ...]

    request = ExampleRequest(requirements=(AssertRefSnapshotId(ref="main", snapshot_id=None),))
    dumped_json = request.model_dump_json()
    expected_json = """{"type":"assert-ref-snapshot-id","ref":"main","snapshot-id":null}"""
    assert expected_json in dumped_json
