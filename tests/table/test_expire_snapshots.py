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
from unittest.mock import MagicMock
from uuid import uuid4

from pyiceberg.table import CommitTableResponse, Table


def test_expire_snapshot(table_v2: Table) -> None:
    EXPIRE_SNAPSHOT = 3051729675574597004
    KEEP_SNAPSHOT = 3055729675574597004
    # Mock the catalog's commit_table method
    mock_response = CommitTableResponse(
        # Use the table's current metadata but keep only the snapshot not to be expired
        metadata=table_v2.metadata.model_copy(update={"snapshots": [KEEP_SNAPSHOT]}),
        metadata_location="mock://metadata/location",
        uuid=uuid4(),
    )

    # Mock the catalog object and its commit_table method to return the mock response
    table_v2.catalog = MagicMock()
    table_v2.catalog.commit_table.return_value = mock_response

    # Remove any refs that protect the snapshot to be expired
    table_v2.metadata.refs = {
        k: v for k, v in table_v2.metadata.refs.items() if getattr(v, "snapshot_id", None) != EXPIRE_SNAPSHOT
    }

    # Assert fixture data to validate test assumptions
    assert len(table_v2.metadata.snapshots) == 2
    assert len(table_v2.metadata.snapshot_log) == 2
    assert len(table_v2.metadata.refs) == 2

    # Expire the snapshot directly without using a transaction
    try:
        table_v2.expire_snapshots().expire_snapshot_by_id(EXPIRE_SNAPSHOT).commit()
    except Exception as e:
        raise AssertionError(f"Commit failed with error: {e}") from e

    # Assert that commit_table was called once
    table_v2.catalog.commit_table.assert_called_once()

    # Assert the expired snapshot ID is no longer present
    remaining_snapshots = table_v2.metadata.snapshots
    assert EXPIRE_SNAPSHOT not in remaining_snapshots

    # Assert the length of snapshots after expiration
    assert len(table_v2.metadata.snapshots) == 1
