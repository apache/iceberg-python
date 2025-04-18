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
# pylint:disable=redefined-outer-name,eval-used
from typing import cast

from pyiceberg.manifest import ManifestContent
from pyiceberg.table import Table
from pyiceberg.table.snapshots import Operation, Snapshot
from pyiceberg.table.update.validate import validation_history


def test_validation_history(table_v2_with_extensive_snapshots: Table) -> None:
    """Test the validation history function."""
    oldest_snapshot = table_v2_with_extensive_snapshots.snapshots()[0]
    newest_snapshot = cast(Snapshot, table_v2_with_extensive_snapshots.current_snapshot())
    manifests, snapshots = validation_history(
        table_v2_with_extensive_snapshots, newest_snapshot, {Operation.APPEND}, ManifestContent.DATA, oldest_snapshot
    )
    assert len(snapshots) == 2
