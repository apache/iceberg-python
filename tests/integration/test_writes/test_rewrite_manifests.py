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
# pylint:disable=redefined-outer-name
from typing import List

import pyarrow as pa
import pytest

from pyiceberg.catalog import Catalog
from pyiceberg.manifest import ManifestFile
from pyiceberg.table import TableProperties
from utils import _create_table


@pytest.fixture(scope="session", autouse=True)
def table_v1_with_null(session_catalog: Catalog, arrow_table_with_null: pa.Table) -> None:
    identifier = "default.arrow_table_v1_with_null"
    tbl = _create_table(session_catalog, identifier, {"format-version": "1"}, [arrow_table_with_null])
    assert tbl.format_version == 1, f"Expected v1, got: v{tbl.format_version}"


@pytest.fixture(scope="session", autouse=True)
def table_v1_without_data(session_catalog: Catalog, arrow_table_without_data: pa.Table) -> None:
    identifier = "default.arrow_table_v1_without_data"
    tbl = _create_table(session_catalog, identifier, {"format-version": "1"}, [arrow_table_without_data])
    assert tbl.format_version == 1, f"Expected v1, got: v{tbl.format_version}"


@pytest.fixture(scope="session", autouse=True)
def table_v1_with_only_nulls(session_catalog: Catalog, arrow_table_with_only_nulls: pa.Table) -> None:
    identifier = "default.arrow_table_v1_with_only_nulls"
    tbl = _create_table(session_catalog, identifier, {"format-version": "1"}, [arrow_table_with_only_nulls])
    assert tbl.format_version == 1, f"Expected v1, got: v{tbl.format_version}"


@pytest.fixture(scope="session", autouse=True)
def table_v1_appended_with_null(session_catalog: Catalog, arrow_table_with_null: pa.Table) -> None:
    identifier = "default.arrow_table_v1_appended_with_null"
    tbl = _create_table(session_catalog, identifier, {"format-version": "1"}, 2 * [arrow_table_with_null])
    assert tbl.format_version == 1, f"Expected v1, got: v{tbl.format_version}"


@pytest.fixture(scope="session", autouse=True)
def table_v2_with_null(session_catalog: Catalog, arrow_table_with_null: pa.Table) -> None:
    identifier = "default.arrow_table_v2_with_null"
    tbl = _create_table(session_catalog, identifier, {"format-version": "2"}, [arrow_table_with_null])
    assert tbl.format_version == 2, f"Expected v2, got: v{tbl.format_version}"


@pytest.fixture(scope="session", autouse=True)
def table_v2_without_data(session_catalog: Catalog, arrow_table_without_data: pa.Table) -> None:
    identifier = "default.arrow_table_v2_without_data"
    tbl = _create_table(session_catalog, identifier, {"format-version": "2"}, [arrow_table_without_data])
    assert tbl.format_version == 2, f"Expected v2, got: v{tbl.format_version}"


@pytest.fixture(scope="session", autouse=True)
def table_v2_with_only_nulls(session_catalog: Catalog, arrow_table_with_only_nulls: pa.Table) -> None:
    identifier = "default.arrow_table_v2_with_only_nulls"
    tbl = _create_table(session_catalog, identifier, {"format-version": "2"}, [arrow_table_with_only_nulls])
    assert tbl.format_version == 2, f"Expected v2, got: v{tbl.format_version}"


@pytest.fixture(scope="session", autouse=True)
def table_v2_appended_with_null(session_catalog: Catalog, arrow_table_with_null: pa.Table) -> None:
    identifier = "default.arrow_table_v2_appended_with_null"
    tbl = _create_table(session_catalog, identifier, {"format-version": "2"}, 2 * [arrow_table_with_null])
    assert tbl.format_version == 2, f"Expected v2, got: v{tbl.format_version}"



@pytest.mark.integration
def test_rewrite_v1_v2_manifests(session_catalog: Catalog, arrow_table_with_null: pa.Table) -> None:
    identifier = "default.test_rewrite_v1_v2_manifests"
    # Create a v1 table and append data
    tbl = _create_table(
        session_catalog,
        identifier,
        {"format-version": "1"},
        [arrow_table_with_null],
    )
    assert tbl.format_version == 1, f"Expected v1, got: v{tbl.format_version}"

    # Upgrade to v2 and append more data
    with tbl.transaction() as tx:
        tx.upgrade_table_version(format_version=2)

    tbl.append(arrow_table_with_null)
    assert tbl.format_version == 2, f"Expected v2, got: v{tbl.format_version}"

    with tbl.transaction() as tx:  # type: ignore[unreachable]
        tx.set_properties({TableProperties.MANIFEST_MERGE_ENABLED: "true", TableProperties.MANIFEST_MIN_MERGE_COUNT: "2"})

    # Get initial manifest state
    manifests = tbl.inspect.manifests()
    assert len(manifests) == 2, "Should have 2 manifests before rewrite"

    # Execute rewrite manifests
    result = tbl.rewrite_manifests()

    assert len(result.rewritten_manifests) == 2, "Action should rewrite 2 manifests"
    assert len(result.added_manifests) == 1, "Action should add 1 manifest"

    tbl.refresh()

    # Verify final state
    current_snapshot = tbl.current_snapshot()
    if not current_snapshot:
        raise AssertionError("Expected a current snapshot")

    new_manifests = current_snapshot.manifests(tbl.io)
    assert len(new_manifests) == 1, "Should have 1 manifest after rewrite"
    assert new_manifests[0].existing_files_count == 2, "Should have 2 existing files in the new manifest"
    assert new_manifests[0].added_files_count == 0, "Should have no added files in the new manifest"
    assert new_manifests[0].deleted_files_count == 0, "Should have no deleted files in the new manifest"
    assert new_manifests[0].sequence_number is not None, "Should have a sequence number in the new manifest"

    # Validate the data is intact
    expected_records_count = arrow_table_with_null.shape[0] * 2
    result_df = tbl.scan().to_pandas()
    actual_records_count = result_df.shape[0]
    assert expected_records_count == actual_records_count, "Record count must match"


@pytest.mark.integration
def test_rewrite_manifests_empty_table(session_catalog: Catalog) -> None:
    # Create an unpartitioned table
    identifier = "default.test_rewrite_manifests_empty_table"
    tbl = _create_table(session_catalog, identifier, {"format-version": "2"})

    assert tbl.current_snapshot() is None, "Table must be empty"

    # Execute rewrite manifests action
    tbl.rewrite_manifests()

    tbl.refresh()
    assert tbl.current_snapshot() is None, "Table must stay empty"


@pytest.mark.integration
def test_rewrite_small_manifests_non_partitioned_table(session_catalog: Catalog, arrow_table_with_null: pa.Table) -> None:
    identifier = "default.test_rewrite_small_manifests_non_partitioned_table"
    tbl = _create_table(session_catalog, identifier, {"format-version": "2"})
    tbl.append(arrow_table_with_null)
    tbl.append(arrow_table_with_null)

    tbl.transaction().set_properties(
        {TableProperties.MANIFEST_MERGE_ENABLED: "true", TableProperties.MANIFEST_MIN_MERGE_COUNT: "2"}
    ).commit_transaction()
    tbl = tbl.refresh()
    manifests = tbl.inspect.manifests()
    assert len(manifests) == 2, "Should have 2 manifests before rewrite"

    result = tbl.rewrite_manifests()

    assert len(result.rewritten_manifests) == 2, "Action should rewrite 2 manifests"
    assert len(result.added_manifests) == 1, "Action should add 1 manifest"

    tbl.refresh()

    current_snapshot = tbl.current_snapshot()
    if not current_snapshot:
        raise AssertionError
    new_manifests = current_snapshot.manifests(tbl.io)
    assert len(new_manifests) == 1, "Should have 1 manifest after rewrite"
    assert new_manifests[0].existing_files_count == 2, "Should have 4 files in the new manifest"
    assert new_manifests[0].added_files_count == 0, "Should have no added files in the new manifest"
    assert new_manifests[0].deleted_files_count == 0, "Should have no deleted files in the new manifest"

    # Validate the records
    expected_records_count = arrow_table_with_null.shape[0] * 2
    result_df = tbl.scan().to_pandas()
    actual_records_count = result_df.shape[0]
    assert expected_records_count == actual_records_count, "Rows must match"


def compute_manifest_entry_size_bytes(manifests: List[ManifestFile]) -> float:
    total_size = 0
    num_entries = 0

    for manifest in manifests:
        total_size += manifest.manifest_length
        num_entries += (
            (manifest.added_files_count or 0) + (manifest.existing_files_count or 0) + (manifest.deleted_files_count or 0)
        )
    return total_size / num_entries if num_entries > 0 else 0


def test_rewrite_small_manifests_partitioned_table(session_catalog: Catalog) -> None:
    records1 = pa.Table.from_pydict({"c1": [1, 1], "c2": [None, "BBBBBBBBBB"], "c3": ["AAAA", "BBBB"]})

    records2 = records2 = pa.Table.from_pydict({"c1": [2, 2], "c2": ["CCCCCCCCCC", "DDDDDDDDDD"], "c3": ["CCCC", "DDDD"]})

    records3 = records3 = pa.Table.from_pydict({"c1": [3, 3], "c2": ["EEEEEEEEEE", "FFFFFFFFFF"], "c3": ["EEEE", "FFFF"]})

    records4 = records4 = pa.Table.from_pydict({"c1": [4, 4], "c2": ["GGGGGGGGGG", "HHHHHHHHHG"], "c3": ["GGGG", "HHHH"]})

    schema = pa.schema(
        [
            ("c1", pa.int64()),
            ("c2", pa.string()),
            ("c3", pa.string()),
        ]
    )

    identifier = "default.test_rewrite_small_manifests_non_partitioned_table"
    tbl = _create_table(session_catalog, identifier, {"format-version": "2"}, schema=schema)

    tbl.append(records1)
    tbl.append(records2)
    tbl.append(records3)
    tbl.append(records4)
    tbl.refresh()

    tbl = tbl.refresh()
    manifests = tbl.current_snapshot().manifests(tbl.io)  # type: ignore
    assert len(manifests) == 4, "Should have 4 manifests before rewrite"

    # manifest_entry_size_bytes = compute_manifest_entry_size_bytes(manifests)
    target_manifest_size_bytes = 5200 * 2 + 100

    tbl = (
        tbl.transaction()
        .set_properties(
            {
                TableProperties.MANIFEST_TARGET_SIZE_BYTES: str(target_manifest_size_bytes),
                TableProperties.MANIFEST_MERGE_ENABLED: "true",
                TableProperties.MANIFEST_MIN_MERGE_COUNT: "2",
            }
        )
        .commit_transaction()
    )

    result = tbl.rewrite_manifests()

    tbl.refresh()
    assert len(result.rewritten_manifests) == 4, "Action should rewrite 4 manifests"
    assert len(result.added_manifests) == 2, "Action should add 2 manifests"

    new_manifests = tbl.current_snapshot().manifests(tbl.io)  # type: ignore
    assert len(new_manifests) == 2, "Should have 2 manifests after rewrite"

    assert new_manifests[0].existing_files_count == 2
    assert new_manifests[0].added_files_count == 0
    assert new_manifests[0].deleted_files_count == 0

    assert new_manifests[1].existing_files_count == 2
    assert new_manifests[1].added_files_count == 0
    assert new_manifests[1].deleted_files_count == 0

    sorted_df = tbl.scan().to_pandas().sort_values(["c1", "c2"], ascending=[False, False])
    expectedRecords = (
        pa.concat_tables([records1, records2, records3, records4]).to_pandas().sort_values(["c1", "c2"], ascending=[False, False])
    )
    from pandas.testing import assert_frame_equal

    assert_frame_equal(sorted_df.reset_index(drop=True), expectedRecords.reset_index(drop=True))
