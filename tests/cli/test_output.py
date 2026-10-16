#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.
import json

import pytest

from pyiceberg.cli.output import JsonOutput
from pyiceberg.table.refs import SnapshotRefType


def test_json_describe_refs_single_ref(capsys: pytest.CaptureFixture) -> None:
    JsonOutput().describe_refs(
        [
            (
                "main",
                SnapshotRefType.BRANCH,
                {
                    "min_snapshots_to_keep": "1",
                    "max_snapshot_age_ms": "432000000",
                    "max_ref_age_ms": "forever",
                },
            )
        ]
    )

    assert json.loads(capsys.readouterr().out) == [
        {
            "name": "main",
            "type": "branch",
            "min_snapshots_to_keep": "1",
            "max_snapshot_age_ms": "432000000",
            "max_ref_age_ms": "forever",
        }
    ]


def test_json_describe_refs_multiple_refs(capsys: pytest.CaptureFixture) -> None:
    JsonOutput().describe_refs(
        [
            (
                "main",
                SnapshotRefType.BRANCH,
                {
                    "min_snapshots_to_keep": "1",
                    "max_snapshot_age_ms": "432000000",
                    "max_ref_age_ms": "forever",
                },
            ),
            ("v1.0", SnapshotRefType.TAG, {"max_ref_age_ms": "forever"}),
            ("no-retention", SnapshotRefType.BRANCH, {}),
        ]
    )

    assert json.loads(capsys.readouterr().out) == [
        {
            "name": "main",
            "type": "branch",
            "min_snapshots_to_keep": "1",
            "max_snapshot_age_ms": "432000000",
            "max_ref_age_ms": "forever",
        },
        {"name": "v1.0", "type": "tag", "max_ref_age_ms": "forever"},
        {"name": "no-retention", "type": "branch"},
    ]
