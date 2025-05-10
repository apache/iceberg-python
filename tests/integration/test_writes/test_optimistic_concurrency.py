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

import pyarrow as pa
import pytest
from pyspark.sql import SparkSession

from pyiceberg.catalog import Catalog
from pyiceberg.exceptions import CommitFailedException
from utils import _create_table


@pytest.mark.integration
@pytest.mark.parametrize("format_version", [1, 2])
def test_conflict_delete_delete(
    spark: SparkSession, session_catalog: Catalog, arrow_table_with_null: pa.Table, format_version: int
) -> None:
    """This test should start passing once optimistic concurrency control has been implemented."""
    identifier = "default.test_conflict"
    tbl1 = _create_table(session_catalog, identifier, {"format-version": format_version}, [arrow_table_with_null])
    tbl2 = session_catalog.load_table(identifier)

    tbl1.delete("string == 'z'")

    with pytest.raises(CommitFailedException, match="(branch main has changed: expected id ).*"):
        # tbl2 isn't aware of the commit by tbl1
        tbl2.delete("string == 'z'")


@pytest.mark.integration
@pytest.mark.parametrize("format_version", [1, 2])
def test_conflict_delete_append(
    spark: SparkSession, session_catalog: Catalog, arrow_table_with_null: pa.Table, format_version: int
) -> None:
    """This test should start passing once optimistic concurrency control has been implemented."""
    identifier = "default.test_conflict"
    tbl1 = _create_table(session_catalog, identifier, {"format-version": format_version}, [arrow_table_with_null])
    tbl2 = session_catalog.load_table(identifier)

    # This is allowed
    tbl1.delete("string == 'z'")

    with pytest.raises(CommitFailedException, match="(branch main has changed: expected id ).*"):
        # tbl2 isn't aware of the commit by tbl1
        tbl2.append(arrow_table_with_null)


@pytest.mark.integration
@pytest.mark.parametrize("format_version", [1, 2])
def test_conflict_append_delete(
    spark: SparkSession, session_catalog: Catalog, arrow_table_with_null: pa.Table, format_version: int
) -> None:
    """This test should start passing once optimistic concurrency control has been implemented."""
    identifier = "default.test_conflict"
    tbl1 = _create_table(session_catalog, identifier, {"format-version": format_version}, [arrow_table_with_null])
    tbl2 = session_catalog.load_table(identifier)

    tbl1.append(arrow_table_with_null)

    with pytest.raises(CommitFailedException, match="(branch main has changed: expected id ).*"):
        # tbl2 isn't aware of the commit by tbl1
        tbl2.delete("string == 'z'")


@pytest.mark.integration
@pytest.mark.parametrize("format_version", [1, 2])
def test_conflict_append_append(
    spark: SparkSession, session_catalog: Catalog, arrow_table_with_null: pa.Table, format_version: int
) -> None:
    """This test should start passing once optimistic concurrency control has been implemented."""
    identifier = "default.test_conflict"
    tbl1 = _create_table(session_catalog, identifier, {"format-version": format_version}, [arrow_table_with_null])
    tbl2 = session_catalog.load_table(identifier)

    tbl1.append(arrow_table_with_null)

    with pytest.raises(CommitFailedException, match="(branch main has changed: expected id ).*"):
        # tbl2 isn't aware of the commit by tbl1
        tbl2.append(arrow_table_with_null)
