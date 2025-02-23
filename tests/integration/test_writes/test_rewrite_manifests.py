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

import pyarrow as pa
import pytest

from pyiceberg.catalog import Catalog
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


@pytest.fixture(scope="session", autouse=True)
def table_v1_v2_appended_with_null(session_catalog: Catalog, arrow_table_with_null: pa.Table) -> None:
    identifier = "default.arrow_table_v1_v2_appended_with_null"
    tbl = _create_table(session_catalog, identifier, {"format-version": "1"}, [arrow_table_with_null])
    assert tbl.format_version == 1, f"Expected v1, got: v{tbl.format_version}"

    with tbl.transaction() as tx:
        tx.upgrade_table_version(format_version=2)

    tbl.append(arrow_table_with_null)

    assert tbl.format_version == 2, f"Expected v2, got: v{tbl.format_version}"


# @pytest.mark.integration
# def test_summaries(spark: SparkSession, session_catalog: Catalog, arrow_table_with_null: pa.Table) -> None:
#     identifier = "default.arrow_table_summaries"
#     tbl = _create_table(session_catalog, identifier, {"format-version": "2"})
#     tbl.append(arrow_table_with_null)
#     tbl.append(arrow_table_with_null)
#
#     # tbl.rewrite_manifests()
#
#     # records1 = [ThreeColumnRecord(1, None, "AAAA")]
#     # write_records(spark, table_location, records1)
#     before_pandas = tbl.scan().to_pandas()
#     before_count = before_pandas.shape[0]
#     tbl.refresh()
#     manifests = tbl.inspect.manifests().to_pylist()
#     assert len(manifests) == 2, "Should have 2 manifests before rewrite"
#
#     tbl.rewrite_manifests()
#     tbl.refresh()
#
#     after_pandas = tbl.scan().to_pandas()
#     after_count = before_pandas.shape[0]
#     manifests = tbl.inspect.manifests().to_pylist()
#     assert len(manifests) == 1, "Should have 1 manifests before rewrite"
#
#     snaps = tbl.inspect.snapshots().to_pandas()
#     print(snaps)


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
    tbl.refresh()

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


def test_rewrite_small_manifests_partitioned_table(session_catalog: Catalog, arrow_table_with_null: pa.Table) -> None:
    # Create and append data files
    # records1 = [ThreeColumnRecord(1, None, "AAAA"), ThreeColumnRecord(1, "BBBBBBBBBB", "BBBB")]
    # records2 = [ThreeColumnRecord(2, "CCCCCCCCCC", "CCCC"), ThreeColumnRecord(2, "DDDDDDDDDD", "DDDD")]
    # records3 = [ThreeColumnRecord(3, "EEEEEEEEEE", "EEEE"), ThreeColumnRecord(3, "FFFFFFFFFF", "FFFF")]
    # records4 = [ThreeColumnRecord(4, "GGGGGGGGGG", "GGGG"), ThreeColumnRecord(4, "HHHHHHHHHH", "HHHH")]
    # self.table.newFastAppend().appendFile(DataFile.from_records(records1)).commit()
    # self.table.newFastAppend().appendFile(DataFile.from_records(records2)).commit()
    # self.table.newFastAppend().appendFile(DataFile.from_records(records3)).commit()
    # self.table.newFastAppend().appendFile(DataFile.from_records(records4)).commit()

    identifier = "default.test_rewrite_small_manifests_non_partitioned_table"
    tbl = _create_table(session_catalog, identifier, {"format-version": "2"})
    tbl.append(arrow_table_with_null)
    tbl.append(arrow_table_with_null)
    tbl.append(arrow_table_with_null)
    tbl.append(arrow_table_with_null)

    tbl.refresh()
    manifests = tbl.current_snapshot().manifests(tbl.io)
    assert len(manifests) == 4, "Should have 4 manifests before rewrite"

    # Perform the rewrite manifests action
    # actions = SparkActions.get()
    result = tbl.rewrite_manifests()

    assert len(result.rewritten_manifests) == 4, "Action should rewrite 4 manifests"
    assert len(result.added_manifests) == 2, "Action should add 2 manifests"

    tbl.refresh()
    new_manifests = tbl.current_snapshot().manifests(tbl.io)
    assert len(new_manifests) == 2, "Should have 2 manifests after rewrite"

    assert new_manifests[0].existing_files_count == 4
    assert new_manifests[0].added_files_count == 0
    assert new_manifests[0].deleted_files_count == 0
    #
    # assertnew_manifests[1].existingFilesCount(), 4)
    # self.assertFalse(new_manifests[1].hasAddedFiles())
    # self.assertFalse(new_manifests[1].hasDeletedFiles())
    #
    # # Validate the records
    # expected_records = records1 + records2 + records3 + records4
    # result_df = tbl.read()
    # actual_records = result_df.collect()
    # self.assertEqual(actual_records, expected_records, "Rows must match")
