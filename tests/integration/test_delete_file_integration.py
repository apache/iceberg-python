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
import pandas as pd
import pytest
from pyspark.sql import SparkSession

from pyiceberg.catalog import Catalog


@pytest.mark.integration
def test_basic_positional_deletes(spark: SparkSession, session_catalog: Catalog) -> None:
    identifier = "default.test_basic_positional_deletes"

    spark.sql(f"DROP TABLE IF EXISTS {identifier}")
    spark.sql(f"""
        CREATE TABLE {identifier} (id INT, data STRING)
        USING iceberg
        TBLPROPERTIES(
            'format-version' = '2',
            'write.delete.mode' = 'merge-on-read',
            'write.update.mode' = 'merge-on-read',
            'write.merge.mode' = 'merge-on-read'
        )
    """)

    spark.sql(f"INSERT INTO {identifier} VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e')")
    spark.sql(f"DELETE FROM {identifier} WHERE id IN (2, 4)")

    # Expected output
    # {
    #     "id": [1, 3, 5],
    #     "data": ["a", "c", "e"]
    # }

    spark_df = spark.sql(f"SELECT * FROM {identifier} ORDER BY id").toPandas()

    table = session_catalog.load_table(identifier)
    pyiceberg_df = table.scan().to_pandas().sort_values("id").reset_index(drop=True)
    pd.testing.assert_frame_equal(spark_df, pyiceberg_df, check_dtype=False)


@pytest.mark.integration
def test_partitioned_deletes(spark: SparkSession, session_catalog: Catalog) -> None:
    identifier = "default.test_partitioned_deletes"

    spark.sql(f"DROP TABLE IF EXISTS {identifier}")
    spark.sql(f"""
        CREATE TABLE {identifier} (id INT, data STRING, part INT)
        USING iceberg
        PARTITIONED BY (part)
        TBLPROPERTIES(
            'format-version' = '2',
            'write.delete.mode' = 'merge-on-read',
            'write.update.mode' = 'merge-on-read',
            'write.merge.mode' = 'merge-on-read'
        )
    """)

    spark.sql(f"""
        INSERT INTO {identifier} VALUES
        (1, 'a', 1), (2, 'b', 1), (3, 'c', 1),
        (4, 'd', 2), (5, 'e', 2), (6, 'f', 2)
    """)

    spark.sql(f"DELETE FROM {identifier} WHERE part = 1 AND id = 2")

    spark.sql(f"ALTER TABLE {identifier} SET TBLPROPERTIES('format-version' = '3')")

    spark.sql(f"DELETE FROM {identifier} WHERE part = 2 AND id = 5")

    # Expected output
    # {
    #     "id": [1, 3, 4, 5, 6],
    #     "data": ["a", "c", "d", "e", "f"],
    #     "part": [1, 1, 2, 2, 2]
    # }

    spark_df = spark.sql(f"SELECT * FROM {identifier} ORDER BY id").toPandas()

    table = session_catalog.load_table(identifier)
    pyiceberg_df = table.scan().to_pandas().sort_values("id").reset_index(drop=True)

    pd.testing.assert_frame_equal(spark_df, pyiceberg_df, check_dtype=False)


@pytest.mark.integration
def test_multiple_deletes(spark: SparkSession, session_catalog: Catalog) -> None:
    identifier = "default.test_multiple_deletes"

    spark.sql(f"DROP TABLE IF EXISTS {identifier}")
    spark.sql(f"""
        CREATE TABLE {identifier} (id INT, data STRING, category STRING)
        USING iceberg
        TBLPROPERTIES(
            'format-version' = '2',
            'write.delete.mode' = 'merge-on-read',
            'write.update.mode' = 'merge-on-read',
            'write.merge.mode' = 'merge-on-read'
        )
    """)

    spark.sql(f"""
        INSERT INTO {identifier} VALUES
        (1, 'a', 'x'), (2, 'b', 'y'), (3, 'c', 'x'),
        (4, 'd', 'y'), (5, 'e', 'x'), (6, 'f', 'y')
    """)

    spark.sql(f"DELETE FROM {identifier} WHERE id = 1")
    spark.sql(f"DELETE FROM {identifier} WHERE category = 'y' AND id > 4")
    spark.sql(f"DELETE FROM {identifier} WHERE data = 'c'")

    spark_df = spark.sql(f"SELECT * FROM {identifier} ORDER BY id").toPandas()

    table = session_catalog.load_table(identifier)
    pyiceberg_df = table.scan().to_pandas().sort_values("id").reset_index(drop=True)

    pd.testing.assert_frame_equal(spark_df, pyiceberg_df, check_dtype=False)


@pytest.mark.integration
def test_sequence_number_filtering(spark: SparkSession, session_catalog: Catalog) -> None:
    identifier = "default.test_sequence_number_filtering"

    spark.sql(f"DROP TABLE IF EXISTS {identifier}")
    spark.sql(f"""
        CREATE TABLE {identifier} (id INT, data STRING)
        USING iceberg
        TBLPROPERTIES(
            'format-version' = '2',
            'write.delete.mode' = 'merge-on-read',
            'write.update.mode' = 'merge-on-read',
            'write.merge.mode' = 'merge-on-read'
        )
    """)

    spark.sql(f"INSERT INTO {identifier} VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e')")  # Seq 0
    spark.sql(f"DELETE FROM {identifier} WHERE id = 1")  # Seq 1
    spark.sql(f"INSERT INTO {identifier} VALUES (6, 'f')")  # Seq 2
    spark.sql(f"DELETE FROM {identifier} WHERE id = 3")  # Seq 3
    spark.sql(f"INSERT INTO {identifier} VALUES (7, 'g')")  # Seq 4
    spark.sql(f"DELETE FROM {identifier} WHERE id = 5")  # Seq 5

    spark_df = spark.sql(f"SELECT * FROM {identifier} ORDER BY id").toPandas()

    table = session_catalog.load_table(identifier)
    pyiceberg_df = table.scan().to_pandas().sort_values("id").reset_index(drop=True)

    pd.testing.assert_frame_equal(spark_df, pyiceberg_df, check_dtype=False)


@pytest.mark.integration
def test_unpartitioned_and_partitioned_deletes(spark: SparkSession, session_catalog: Catalog) -> None:
    identifier = "default.test_unpartitioned_and_partitioned_deletes"

    spark.sql(f"DROP TABLE IF EXISTS {identifier}")
    spark.sql(f"""
        CREATE TABLE {identifier} (id INT, data STRING, part INT)
        USING iceberg
        PARTITIONED BY (part)
        TBLPROPERTIES(
            'format-version' = '2',
            'write.delete.mode' = 'merge-on-read',
            'write.update.mode' = 'merge-on-read',
            'write.merge.mode' = 'merge-on-read'
        )
    """)

    spark.sql(f"""
        INSERT INTO {identifier} VALUES
        (1, 'a', 1), (2, 'b', 1), (3, 'c', 1),
        (4, 'd', 2), (5, 'e', 2), (6, 'f', 2),
        (7, 'g', 3), (8, 'h', 3), (9, 'i', 3)
    """)

    # Unpartitioned deletes
    spark.sql(f"DELETE FROM {identifier} WHERE data IN ('b', 'e', 'h')")

    # Partition-specific delete
    spark.sql(f"DELETE FROM {identifier} WHERE part = 1 AND id = 3")

    spark.sql(f"ALTER TABLE {identifier} SET TBLPROPERTIES('format-version' = '3')")

    spark.sql(f"DELETE FROM {identifier} WHERE part = 3 AND id = 9")

    spark_df = spark.sql(f"SELECT * FROM {identifier} ORDER BY id").toPandas()

    table = session_catalog.load_table(identifier)
    pyiceberg_df = table.scan().to_pandas().sort_values("id").reset_index(drop=True)

    pd.testing.assert_frame_equal(spark_df, pyiceberg_df, check_dtype=False)


@pytest.mark.integration
def test_multi_partition(spark: SparkSession, session_catalog: Catalog) -> None:
    identifier = "default.test_multi_partition"

    spark.sql(f"DROP TABLE IF EXISTS {identifier}")
    spark.sql(f"""
        CREATE TABLE {identifier} (id INT, data STRING, year INT, month INT)
        USING iceberg
        PARTITIONED BY (year, month)
        TBLPROPERTIES(
            'format-version' = '2',
            'write.delete.mode' = 'merge-on-read',
            'write.update.mode' = 'merge-on-read',
            'write.merge.mode' = 'merge-on-read'
        )
    """)

    spark.sql(f"""
        INSERT INTO {identifier}
        SELECT id, CONCAT('data_', id), 2023 + (id % 2), 1 + (id % 12)
        FROM range(500)
    """)

    spark.sql(f"DELETE FROM {identifier} WHERE year = 2023 AND month <= 3")
    spark.sql(f"DELETE FROM {identifier} WHERE year = 2024 AND month > 9")
    spark.sql(f"DELETE FROM {identifier} WHERE id % 10 = 0")

    spark.sql(f"ALTER TABLE {identifier} SET TBLPROPERTIES('format-version' = '3')")

    spark.sql(f"DELETE FROM {identifier} WHERE year = 2023 AND month = 6 AND id < 50")

    spark_df = spark.sql(f"SELECT * FROM {identifier} ORDER BY id").toPandas()

    table = session_catalog.load_table(identifier)
    pyiceberg_df = table.scan().to_pandas().sort_values("id").reset_index(drop=True)

    pd.testing.assert_frame_equal(spark_df, pyiceberg_df, check_dtype=False)


@pytest.mark.integration
def test_empty_results(spark: SparkSession, session_catalog: Catalog) -> None:
    identifier = "default.test_empty_results"

    spark.sql(f"DROP TABLE IF EXISTS {identifier}")
    spark.sql(f"""
        CREATE TABLE {identifier} (id INT, data STRING)
        USING iceberg
        TBLPROPERTIES(
            'format-version' = '2',
            'write.delete.mode' = 'merge-on-read',
            'write.update.mode' = 'merge-on-read',
            'write.merge.mode' = 'merge-on-read'
        )
    """)

    spark.sql(f"INSERT INTO {identifier} VALUES (1, 'a'), (2, 'b'), (3, 'c')")
    spark.sql(f"DELETE FROM {identifier} WHERE id IN (1, 2, 3)")

    spark_df = spark.sql(f"SELECT * FROM {identifier} ORDER BY id").toPandas()

    table = session_catalog.load_table(identifier)
    pyiceberg_df = table.scan().to_pandas().sort_values("id").reset_index(drop=True)

    pd.testing.assert_frame_equal(spark_df, pyiceberg_df, check_dtype=False)
