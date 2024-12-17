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

from pyiceberg.catalog import Catalog
from pyiceberg.table.statistics import BlobMetadata, StatisticsFile


@pytest.mark.integration
@pytest.mark.parametrize("catalog", [pytest.lazy_fixture("session_catalog_hive"), pytest.lazy_fixture("session_catalog")])
def test_manage_statistics(catalog: Catalog) -> None:
    identifier = "default.test_table_statistics_operations"
    tbl = catalog.load_table(identifier)

    add_snapshot_id_1 = tbl.history()[0].snapshot_id
    add_snapshot_id_2 = tbl.history()[1].snapshot_id

    def create_statistics_file(snapshot_id: int) -> StatisticsFile:
        blob_metadata = BlobMetadata(
            type="boring-type",
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

    statistics_file_snap_1 = create_statistics_file(add_snapshot_id_1)
    statistics_file_snap_2 = create_statistics_file(add_snapshot_id_2)

    with tbl.update_statistics() as update:
        update.set_statistics(add_snapshot_id_1, statistics_file_snap_1)
        update.set_statistics(add_snapshot_id_2, statistics_file_snap_2)

    assert len(tbl.metadata.statistics) == 2

    with tbl.update_statistics() as update:
        update.remove_statistics(add_snapshot_id_1)

    assert len(tbl.metadata.statistics) == 1
