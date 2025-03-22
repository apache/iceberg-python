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
from typing import TYPE_CHECKING

import pytest

from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.table.statistics import BlobMetadata, StatisticsFile

if TYPE_CHECKING:
    import pyarrow as pa

    from pyiceberg.catalog import Catalog
    from pyiceberg.schema import Schema
    from pyiceberg.table import Table


def _create_table_with_schema(catalog: "Catalog", schema: "Schema") -> "Table":
    tbl_name = "default.test_table_statistics_operations"

    try:
        catalog.drop_table(tbl_name)
    except NoSuchTableError:
        pass
    return catalog.create_table(identifier=tbl_name, schema=schema)


@pytest.mark.integration
@pytest.mark.parametrize("catalog", [pytest.lazy_fixture("session_catalog_hive"), pytest.lazy_fixture("session_catalog")])
def test_manage_statistics(catalog: "Catalog", arrow_table_with_null: "pa.Table") -> None:
    tbl = _create_table_with_schema(catalog, arrow_table_with_null.schema)

    tbl.append(arrow_table_with_null)
    tbl.append(arrow_table_with_null)

    add_snapshot_id_1 = tbl.history()[0].snapshot_id
    add_snapshot_id_2 = tbl.history()[1].snapshot_id

    def create_statistics_file(snapshot_id: int, type_name: str) -> StatisticsFile:
        blob_metadata = BlobMetadata(
            type=type_name,
            snapshot_id=snapshot_id,
            sequence_number=2,
            fields=[1],
            properties={"prop-key": "prop-value"},
        )

        statistics_file = StatisticsFile(
            snapshot_id=snapshot_id,
            statistics_path="s3://bucket/warehouse/stats.puffin",
            file_size_in_bytes=124,
            file_footer_size_in_bytes=27,
            blob_metadata=[blob_metadata],
        )

        return statistics_file

    statistics_file_snap_1 = create_statistics_file(add_snapshot_id_1, "apache-datasketches-theta-v1")
    statistics_file_snap_2 = create_statistics_file(add_snapshot_id_2, "deletion-vector-v1")

    with tbl.update_statistics() as update:
        update.set_statistics(statistics_file_snap_1)
        update.set_statistics(statistics_file_snap_2)

    assert len(tbl.metadata.statistics) == 2

    with tbl.update_statistics() as update:
        update.remove_statistics(add_snapshot_id_1)

    assert len(tbl.metadata.statistics) == 1

    with tbl.transaction() as txn:
        with txn.update_statistics() as update:
            update.set_statistics(statistics_file_snap_1)
            update.set_statistics(statistics_file_snap_2)

    assert len(tbl.metadata.statistics) == 2
