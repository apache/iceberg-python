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

import pytest
from pyspark.sql import SparkSession

from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.expressions import EqualTo
from pyiceberg.table.snapshots import Operation, Summary


def run_spark_commands(spark: SparkSession, sqls: List[str]) -> None:
    for sql in sqls:
        spark.sql(sql)


@pytest.mark.integration
@pytest.mark.parametrize("format_version", [1, 2])
def test_partitioned_table_delete_full_file(spark: SparkSession, session_catalog: RestCatalog, format_version: int) -> None:
    identifier = 'default.table_partitioned_delete'

    run_spark_commands(
        spark,
        [
            f"DROP TABLE IF EXISTS {identifier}",
            f"""
            CREATE TABLE {identifier} (
                number_partitioned  int,
                number              int
            )
            USING iceberg
            PARTITIONED BY (number_partitioned)
            TBLPROPERTIES('format-version' = {format_version})
        """,
            f"""
            INSERT INTO {identifier} VALUES (10, 20), (10, 30)
        """,
            f"""
            INSERT INTO {identifier} VALUES (11, 20), (11, 30)
        """,
        ],
    )

    tbl = session_catalog.load_table(identifier)
    tbl.delete(EqualTo("number_partitioned", 10))

    # No overwrite operation
    assert [snapshot.summary.operation.value for snapshot in tbl.snapshots()] == ['append', 'append', 'delete']
    assert tbl.scan().to_arrow().to_pydict() == {'number_partitioned': [11, 11], 'number': [20, 30]}


@pytest.mark.integration
@pytest.mark.parametrize("format_version", [1, 2])
def test_partitioned_table_rewrite(spark: SparkSession, session_catalog: RestCatalog, format_version: int) -> None:
    identifier = 'default.table_partitioned_delete'

    run_spark_commands(
        spark,
        [
            f"DROP TABLE IF EXISTS {identifier}",
            f"""
            CREATE TABLE {identifier} (
                number_partitioned  int,
                number              int
            )
            USING iceberg
            PARTITIONED BY (number_partitioned)
            TBLPROPERTIES('format-version' = {format_version})
        """,
            f"""
            INSERT INTO {identifier} VALUES (10, 20), (10, 30)
        """,
            f"""
            INSERT INTO {identifier} VALUES (11, 20), (11, 30)
        """,
        ],
    )

    tbl = session_catalog.load_table(identifier)
    tbl.delete(EqualTo("number", 20))

    # We don't delete a whole partition, so there is only a overwrite
    assert [snapshot.summary.operation.value for snapshot in tbl.snapshots()] == ['append', 'append', 'overwrite']
    assert tbl.scan().to_arrow().to_pydict() == {'number_partitioned': [11, 10], 'number': [30, 30]}


@pytest.mark.integration
@pytest.mark.parametrize("format_version", [1, 2])
def test_partitioned_table_no_match(spark: SparkSession, session_catalog: RestCatalog, format_version: int) -> None:
    identifier = 'default.table_partitioned_delete'

    run_spark_commands(
        spark,
        [
            f"DROP TABLE IF EXISTS {identifier}",
            f"""
            CREATE TABLE {identifier} (
                number_partitioned  int,
                number              int
            )
            USING iceberg
            PARTITIONED BY (number_partitioned)
            TBLPROPERTIES('format-version' = {format_version})
        """,
            f"""
            INSERT INTO {identifier} VALUES (10, 20), (10, 30)
        """,
        ],
    )

    tbl = session_catalog.load_table(identifier)
    tbl.delete(EqualTo("number_partitioned", 22))  # Does not affect any data

    # Open for discussion, do we want to create a new snapshot?
    assert [snapshot.summary.operation.value for snapshot in tbl.snapshots()] == ['append']
    assert tbl.scan().to_arrow().to_pydict() == {'number_partitioned': [10, 10], 'number': [20, 30]}


@pytest.mark.integration
def test_partitioned_table_positional_deletes(spark: SparkSession, session_catalog: RestCatalog) -> None:
    identifier = 'default.table_partitioned_delete'

    run_spark_commands(
        spark,
        [
            f"DROP TABLE IF EXISTS {identifier}",
            f"""
            CREATE TABLE {identifier} (
                number_partitioned  int,
                number              int
            )
            USING iceberg
            PARTITIONED BY (number_partitioned)
            TBLPROPERTIES(
                'format-version' = 2,
                'write.delete.mode'='merge-on-read',
                'write.update.mode'='merge-on-read',
                'write.merge.mode'='merge-on-read'
            )
        """,
            f"""
            INSERT INTO {identifier} VALUES (10, 20), (10, 30), (10, 40)
        """,
            # Generate a positional delete
            f"""
            DELETE FROM {identifier} WHERE number = 30
        """,
        ],
    )

    tbl = session_catalog.load_table(identifier)

    # Assert that there is just a single Parquet file, that has one merge on read file
    files = list(tbl.scan().plan_files())
    assert len(files) == 1
    assert len(files[0].delete_files) == 1

    # Will rewrite a data file with a positional delete
    tbl.delete(EqualTo("number", 40))

    # One positional delete has been added, but an OVERWRITE status is set
    # https://github.com/apache/iceberg/issues/10122
    assert [snapshot.summary.operation.value for snapshot in tbl.snapshots()] == ['append', 'overwrite', 'overwrite']
    assert tbl.scan().to_arrow().to_pydict() == {'number_partitioned': [10], 'number': [20]}


@pytest.mark.integration
def test_partitioned_table_positional_deletes_sequence_number(spark: SparkSession, session_catalog: RestCatalog) -> None:
    identifier = 'default.table_partitioned_delete_sequence_number'

    # This test case is a bit more complex. Here we run a MoR delete on a file, we make sure that
    # the manifest gets rewritten (but not the data file with a MoR), and check if the delete is still there
    # to assure that the sequence numbers are maintained

    run_spark_commands(
        spark,
        [
            f"DROP TABLE IF EXISTS {identifier}",
            f"""
            CREATE TABLE {identifier} (
                number_partitioned  int,
                number              int
            )
            USING iceberg
            PARTITIONED BY (number_partitioned)
            TBLPROPERTIES(
                'format-version' = 2,
                'write.delete.mode'='merge-on-read',
                'write.update.mode'='merge-on-read',
                'write.merge.mode'='merge-on-read'
            )
        """,
            f"""
            INSERT INTO {identifier} VALUES (10, 100), (10, 101), (20, 200), (20, 201), (20, 202)
        """,
            # Generate a positional delete
            f"""
            DELETE FROM {identifier} WHERE number = 101
        """,
        ],
    )

    tbl = session_catalog.load_table(identifier)

    files = list(tbl.scan().plan_files())
    assert len(files) == 2

    # Will rewrite a data file with a positional delete
    tbl.delete(EqualTo("number", 201))

    # One positional delete has been added, but an OVERWRITE status is set
    # https://github.com/apache/iceberg/issues/10122
    snapshots = tbl.snapshots()
    assert len(snapshots) == 3

    # Snapshots produced by Spark
    assert [snapshot.summary.operation.value for snapshot in tbl.snapshots()[0:2]] == ['append', 'overwrite']

    # Will rewrite one parquet file
    assert snapshots[2].summary == Summary(
        Operation.OVERWRITE,
        **{
            'added-files-size': '1145',
            'added-data-files': '1',
            'added-records': '2',
            'changed-partition-count': '1',
            'total-files-size': snapshots[2].summary['total-files-size'],
            'total-delete-files': '0',
            'total-data-files': '1',
            'total-position-deletes': '0',
            'total-records': '2',
            'total-equality-deletes': '0',
            'deleted-data-files': '2',
            'removed-delete-files': '1',
            'deleted-records': '5',
            'removed-files-size': snapshots[2].summary['removed-files-size'],
            'removed-position-deletes': '1',
        },
    )

    assert tbl.scan().to_arrow().to_pydict() == {'number_partitioned': [20, 20, 10], 'number': [200, 202, 100]}
