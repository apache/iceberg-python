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

from pyiceberg.catalog import Catalog
from pyiceberg.table import Table
from pyiceberg.table.statistics import BlobMetadata, StatisticsFile


def _create_table_with_two_snapshots(catalog: Catalog) -> tuple[Table, int, int]:
    arrow_table = pa.table({"id": [1, 2, 3]})
    catalog.create_namespace("default")
    table = catalog.create_table("default.test_update_statistics", schema=arrow_table.schema)
    table.append(arrow_table)
    table.append(arrow_table)
    snapshot_id_1, snapshot_id_2 = [snapshot.snapshot_id for snapshot in table.snapshots()]
    return table, snapshot_id_1, snapshot_id_2


def _statistics_file(snapshot_id: int, statistics_path: str) -> StatisticsFile:
    return StatisticsFile(
        snapshot_id=snapshot_id,
        statistics_path=statistics_path,
        file_size_in_bytes=124,
        file_footer_size_in_bytes=27,
        blob_metadata=[
            BlobMetadata(
                type="apache-datasketches-theta-v1",
                snapshot_id=snapshot_id,
                sequence_number=1,
                fields=[1],
                properties={},
            )
        ],
    )


def test_update_statistics_set_and_remove_in_one_update(catalog: Catalog) -> None:
    table, snapshot_id_1, snapshot_id_2 = _create_table_with_two_snapshots(catalog)
    stats_1 = _statistics_file(snapshot_id_1, "s3://bucket/stats-1.puffin")
    stats_2 = _statistics_file(snapshot_id_2, "s3://bucket/stats-2.puffin")

    table.update_statistics().set_statistics(stats_1).commit()
    assert table.metadata.statistics == [stats_1]

    # Chaining set_statistics followed by remove_statistics must preserve both updates
    table.update_statistics().set_statistics(stats_2).remove_statistics(snapshot_id_1).commit()
    assert table.metadata.statistics == [stats_2]
    assert catalog.load_table(table.name()).metadata.statistics == [stats_2]

    # Context manager with set_statistics followed by remove_statistics (docstring pattern)
    with table.update_statistics() as update:
        update.set_statistics(stats_1)
        update.remove_statistics(snapshot_id_2)
    assert table.metadata.statistics == [stats_1]
    assert catalog.load_table(table.name()).metadata.statistics == [stats_1]

    # Reverse order: remove_statistics followed by set_statistics
    table.update_statistics().remove_statistics(snapshot_id_1).set_statistics(stats_2).commit()
    assert table.metadata.statistics == [stats_2]
    assert catalog.load_table(table.name()).metadata.statistics == [stats_2]


def test_update_statistics_multiple_removals_in_one_update(catalog: Catalog) -> None:
    table, snapshot_id_1, snapshot_id_2 = _create_table_with_two_snapshots(catalog)
    stats_1 = _statistics_file(snapshot_id_1, "s3://bucket/stats-1.puffin")
    stats_2 = _statistics_file(snapshot_id_2, "s3://bucket/stats-2.puffin")

    with table.update_statistics() as update:
        update.set_statistics(stats_1)
        update.set_statistics(stats_2)
    assert table.metadata.statistics == [stats_1, stats_2]

    with table.update_statistics() as update:
        update.remove_statistics(snapshot_id_1)
        update.remove_statistics(snapshot_id_2)
    assert table.metadata.statistics == []
    assert catalog.load_table(table.name()).metadata.statistics == []


def test_update_statistics_set_and_remove_in_transaction(catalog: Catalog) -> None:
    table, snapshot_id_1, snapshot_id_2 = _create_table_with_two_snapshots(catalog)
    stats_1 = _statistics_file(snapshot_id_1, "s3://bucket/stats-1.puffin")
    stats_2 = _statistics_file(snapshot_id_2, "s3://bucket/stats-2.puffin")

    table.update_statistics().set_statistics(stats_1).commit()

    with table.transaction() as transaction:
        with transaction.update_statistics() as update:
            update.set_statistics(stats_2)
            update.remove_statistics(snapshot_id_1)
        assert transaction.table_metadata.statistics == [stats_2]
        assert table.metadata.statistics == [stats_1]

    assert table.metadata.statistics == [stats_2]
    assert catalog.load_table(table.name()).metadata.statistics == [stats_2]
