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
# pylint:disable=eval-used
import pytest
from pydantic import ValidationError

from pyiceberg import exceptions
from pyiceberg.table.refs import SnapshotRef, SnapshotRefType


def test_snapshot_with_properties_repr() -> None:
    snapshot_ref = SnapshotRef(
        snapshot_id=3051729675574597004,
        snapshot_ref_type=SnapshotRefType.TAG,
        min_snapshots_to_keep=None,
        max_snapshot_age_ms=None,
        max_ref_age_ms=10000000,
    )

    assert (
        repr(snapshot_ref)
        == """SnapshotRef(snapshot_id=3051729675574597004, snapshot_ref_type=SnapshotRefType.TAG, min_snapshots_to_keep=None, max_snapshot_age_ms=None, max_ref_age_ms=10000000)"""
    )
    assert snapshot_ref == eval(repr(snapshot_ref))


def test_snapshot_with_invalid_field() -> None:
    # min_snapshots_to_keep, if present, must be greater than 0
    with pytest.raises(ValidationError):
        SnapshotRef(
            snapshot_id=3051729675574597004,
            snapshot_ref_type=SnapshotRefType.TAG,
            min_snapshots_to_keep=-1,
            max_snapshot_age_ms=None,
            max_ref_age_ms=10000000,
        )

    # max_snapshot_age_ms, if present, must be greater than 0
    with pytest.raises(ValidationError):
        SnapshotRef(
            snapshot_id=3051729675574597004,
            snapshot_ref_type=SnapshotRefType.TAG,
            min_snapshots_to_keep=1,
            max_snapshot_age_ms=-1,
            max_ref_age_ms=10000000,
        )

    # max_ref_age_ms, if present, must be greater than 0
    with pytest.raises(ValidationError):
        SnapshotRef(
            snapshot_id=3051729675574597004,
            snapshot_ref_type=SnapshotRefType.TAG,
            min_snapshots_to_keep=None,
            max_snapshot_age_ms=None,
            max_ref_age_ms=-1,
        )

    with pytest.raises(exceptions.ValidationError, match="Tags do not support setting minSnapshotsToKeep"):
        SnapshotRef(
            snapshot_id=3051729675574597004,
            snapshot_ref_type=SnapshotRefType.TAG,
            min_snapshots_to_keep=1,
            max_snapshot_age_ms=None,
            max_ref_age_ms=10000000,
        )

    with pytest.raises(exceptions.ValidationError, match="Tags do not support setting maxSnapshotAgeMs"):
        SnapshotRef(
            snapshot_id=3051729675574597004,
            snapshot_ref_type=SnapshotRefType.TAG,
            min_snapshots_to_keep=None,
            max_snapshot_age_ms=1,
            max_ref_age_ms=100000,
        )
