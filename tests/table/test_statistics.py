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

from pyiceberg.catalog import Catalog
from pyiceberg.table import Table
from pyiceberg.table.statistics import BlobMetadata, PartitionStatisticsFile, StatisticsFile


def test_partition_statistics_file() -> None:
    partition_statistics_file_json = (
        """{"snapshot-id":123,"statistics-path":"s3://bucket/statistics.parquet","file-size-in-bytes":345}"""
    )
    partition_statistics_file = PartitionStatisticsFile.model_validate_json(partition_statistics_file_json)

    assert partition_statistics_file == PartitionStatisticsFile(
        snapshot_id=123, statistics_path="s3://bucket/statistics.parquet", file_size_in_bytes=345
    )

    assert partition_statistics_file.model_dump_json() == partition_statistics_file_json


def test_statistics_file() -> None:
    statistics_file_json = """{"snapshot-id":123,"statistics-path":"s3://bucket/statistics.parquet","file-size-in-bytes":345,"file-footer-size-in-bytes":456,"blob-metadata":[{"type":"apache-datasketches-theta-v1","snapshot-id":567,"sequence-number":22,"fields":[1,2,3],"properties":{"foo":"bar"}}]}"""
    statistics_file = StatisticsFile.model_validate_json(statistics_file_json)

    assert statistics_file == StatisticsFile(
        snapshot_id=123,
        statistics_path="s3://bucket/statistics.parquet",
        file_size_in_bytes=345,
        file_footer_size_in_bytes=456,
        key_metadata=None,
        blob_metadata=[
            BlobMetadata(
                type="apache-datasketches-theta-v1",
                snapshot_id=567,
                sequence_number=22,
                fields=[1, 2, 3],
                properties={"foo": "bar"},
            )
        ],
    )

    assert statistics_file.model_dump_json() == statistics_file_json


def _create_table_with_two_snapshots(catalog: Catalog) -> tuple[Table, int, int]:
    arrow_table = pa.table({"id": [1, 2, 3]})
    catalog.create_namespace("default")
    table = catalog.create_table("default.test_partition_statistics", schema=arrow_table.schema)
    table.append(arrow_table)
    table.append(arrow_table)
    snapshot_id_1, snapshot_id_2 = [snapshot.snapshot_id for snapshot in table.snapshots()]
    return table, snapshot_id_1, snapshot_id_2


def _partition_statistics_file(snapshot_id: int, statistics_path: str) -> PartitionStatisticsFile:
    return PartitionStatisticsFile(snapshot_id=snapshot_id, statistics_path=statistics_path, file_size_in_bytes=124)


def test_update_partition_statistics(catalog: Catalog) -> None:
    table, snapshot_id_1, snapshot_id_2 = _create_table_with_two_snapshots(catalog)
    stats_1 = _partition_statistics_file(snapshot_id_1, "s3://bucket/partition-stats-1.parquet")
    stats_1_replaced = _partition_statistics_file(snapshot_id_1, "s3://bucket/partition-stats-1-v2.parquet")
    stats_2 = _partition_statistics_file(snapshot_id_2, "s3://bucket/partition-stats-2.parquet")

    table.update_partition_statistics().set_partition_statistics(**stats_1.model_dump(by_alias=False)).commit()
    assert table.metadata.partition_statistics == [stats_1]

    # A file set for a snapshot that already has one replaces it
    table.update_partition_statistics().set_partition_statistics(**stats_1_replaced.model_dump(by_alias=False)).commit()
    assert table.metadata.partition_statistics == [stats_1_replaced]

    with table.update_partition_statistics() as update:
        update.set_partition_statistics(**stats_2.model_dump(by_alias=False))
    assert table.metadata.partition_statistics == [stats_1_replaced, stats_2]

    table.update_partition_statistics().remove_partition_statistics(snapshot_id_1).commit()
    assert table.metadata.partition_statistics == [stats_2]
    assert catalog.load_table(table.name()).metadata.partition_statistics == [stats_2]


def test_update_partition_statistics_set_and_remove_in_one_update(catalog: Catalog) -> None:
    table, snapshot_id_1, snapshot_id_2 = _create_table_with_two_snapshots(catalog)
    stats_1 = _partition_statistics_file(snapshot_id_1, "s3://bucket/partition-stats-1.parquet")
    stats_1_replaced = _partition_statistics_file(snapshot_id_1, "s3://bucket/partition-stats-1-v2.parquet")
    stats_2 = _partition_statistics_file(snapshot_id_2, "s3://bucket/partition-stats-2.parquet")

    # Operations on different snapshots are both applied, in either call order
    table.update_partition_statistics().set_partition_statistics(**stats_1.model_dump(by_alias=False)).commit()
    table.update_partition_statistics().set_partition_statistics(
        **stats_2.model_dump(by_alias=False)
    ).remove_partition_statistics(snapshot_id_1).commit()
    assert table.metadata.partition_statistics == [stats_2]

    with table.update_partition_statistics() as update:
        update.set_partition_statistics(**stats_1.model_dump(by_alias=False))
        update.remove_partition_statistics(snapshot_id_2)
    assert table.metadata.partition_statistics == [stats_1]

    # On the same snapshot, the removal wins in either call order, with or without an existing file
    for has_existing_file in (False, True):
        for remove_first in (False, True):
            if has_existing_file:
                table.update_partition_statistics().set_partition_statistics(**stats_1.model_dump(by_alias=False)).commit()
            else:
                table.update_partition_statistics().remove_partition_statistics(snapshot_id_1).commit()
            with table.update_partition_statistics() as update:
                if remove_first:
                    update.remove_partition_statistics(snapshot_id_1).set_partition_statistics(
                        **stats_1_replaced.model_dump(by_alias=False)
                    )
                else:
                    update.set_partition_statistics(**stats_1_replaced.model_dump(by_alias=False)).remove_partition_statistics(
                        snapshot_id_1
                    )
            assert table.metadata.partition_statistics == []

    # Repeated operations: the last file set wins, and repeated removals are no-ops
    with table.update_partition_statistics() as update:
        update.set_partition_statistics(**stats_1.model_dump(by_alias=False)).set_partition_statistics(
            **stats_1_replaced.model_dump(by_alias=False)
        )
    assert table.metadata.partition_statistics == [stats_1_replaced]

    with table.update_partition_statistics() as update:
        update.remove_partition_statistics(snapshot_id_1)
        update.remove_partition_statistics(snapshot_id_1)
    assert table.metadata.partition_statistics == []


def test_update_partition_statistics_in_transaction(catalog: Catalog) -> None:
    table, snapshot_id_1, snapshot_id_2 = _create_table_with_two_snapshots(catalog)
    stats_1 = _partition_statistics_file(snapshot_id_1, "s3://bucket/partition-stats-1.parquet")
    stats_2 = _partition_statistics_file(snapshot_id_2, "s3://bucket/partition-stats-2.parquet")

    with table.transaction() as transaction:
        transaction.update_partition_statistics().set_partition_statistics(**stats_1.model_dump(by_alias=False)).commit()
        with transaction.update_partition_statistics() as update:
            update.set_partition_statistics(**stats_2.model_dump(by_alias=False))

        # The pending changes are visible in the transaction only, until it commits
        assert transaction.table_metadata.partition_statistics == [stats_1, stats_2]
        assert table.metadata.partition_statistics == []
        assert catalog.load_table(table.name()).metadata.partition_statistics == []

    assert table.metadata.partition_statistics == [stats_1, stats_2]
    assert catalog.load_table(table.name()).metadata.partition_statistics == [stats_1, stats_2]

    # An exception raised inside the transaction rolls back its pending changes
    def abort_transaction() -> None:
        with table.transaction() as transaction:
            transaction.update_partition_statistics().remove_partition_statistics(snapshot_id_1).commit()
            raise RuntimeError("abort")

    with pytest.raises(RuntimeError, match="abort"):
        abort_transaction()

    assert table.metadata.partition_statistics == [stats_1, stats_2]
    assert catalog.load_table(table.name()).metadata.partition_statistics == [stats_1, stats_2]


def test_remove_partition_statistics_without_file_is_noop(catalog: Catalog) -> None:
    table, snapshot_id_1, snapshot_id_2 = _create_table_with_two_snapshots(catalog)
    stats_2 = _partition_statistics_file(snapshot_id_2, "s3://bucket/partition-stats-2.parquet")
    table.update_partition_statistics().set_partition_statistics(**stats_2.model_dump(by_alias=False)).commit()
    metadata_before = table.metadata

    # Removing a snapshot without a file commits without error, once or repeated, and leaves the metadata unchanged
    table.update_partition_statistics().remove_partition_statistics(snapshot_id_1).commit()
    with table.update_partition_statistics() as update:
        update.remove_partition_statistics(snapshot_id_1)
        update.remove_partition_statistics(snapshot_id_1)

    assert table.metadata == metadata_before
    assert catalog.load_table(table.name()).metadata.partition_statistics == [stats_2]
