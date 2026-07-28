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
import time
from datetime import date

import pytest
from pyspark.sql import SparkSession

from pyiceberg.catalog import Catalog
from pyiceberg.catalog.hive import HiveCatalog


@pytest.mark.integration
def test_migrate_table(
    session_catalog_hive: Catalog,
    spark: SparkSession,
) -> None:
    """
    Imported tables are an edge case since the partition column is not stored
    in the Parquet files:

    test_migrate_table_hive_1754486926/dt=2022-01-01/part-00000-30a9798b-7597-4027-86d9-79d7c529bc87.c000.snappy.parquet
    {
      "type" : "record",
      "name" : "spark_schema",
      "fields" : [ {
        "name" : "number",
        "type" : "int"
      } ]
    }

    PyIceberg will project this column when the table is being read
    """
    # Create new tables to avoid complex cleanup
    src_table_identifier = f"spark_catalog.default.test_migrate_table_hive_{int(time.time())}"
    dst_table_identifier = f"default.test_migrate_table_{int(time.time())}"

    spark.sql(f"""
        CREATE TABLE {src_table_identifier} (
            number INTEGER
        )
        PARTITIONED BY (dt date)
        STORED AS parquet
    """)

    spark.sql(f"""
        INSERT OVERWRITE TABLE {src_table_identifier}
        PARTITION (dt='2022-01-01')
        VALUES (1), (2), (3)
    """)

    spark.sql(f"""
        INSERT OVERWRITE TABLE {src_table_identifier}
        PARTITION (dt='2023-01-01')
        VALUES (4), (5), (6)
    """)

    # Docs: https://iceberg.apache.org/docs/latest/hive-migration/#snapshot-hive-table-to-iceberg
    spark.sql(f"""
        CALL hive.system.snapshot('{src_table_identifier}', 'hive.{dst_table_identifier}')
    """)

    tbl = session_catalog_hive.load_table(dst_table_identifier)
    assert tbl.schema().column_names == ["number", "dt"]

    assert set(tbl.scan().to_arrow().column(1).combine_chunks().tolist()) == {date(2023, 1, 1), date(2022, 1, 1)}
    assert tbl.scan(row_filter="number > 3").to_arrow().column(0).combine_chunks().tolist() == [4, 5, 6]
    assert tbl.scan(row_filter="dt == '2023-01-01'").to_arrow().column(0).combine_chunks().tolist() == [4, 5, 6]
    assert tbl.scan(row_filter="dt == '2022-01-01'").to_arrow().column(0).combine_chunks().tolist() == [1, 2, 3]
    assert tbl.scan(row_filter="dt < '2022-02-01'").to_arrow().column(0).combine_chunks().tolist() == [1, 2, 3]


@pytest.mark.integration
def test_list_views(
    session_catalog_hive: HiveCatalog,
    spark: SparkSession,
) -> None:
    """
    Verify that a view created by Spark through the Iceberg Hive catalog
    can be discovered by PyIceberg HiveCatalog.list_views().

    The test also verifies that:
    - Iceberg tables are not returned as views.
    - Multiple views are returned.
    - Returned identifiers use the expected (namespace, view_name) format.
    """
    suffix = int(time.time())
    catalog_name = "hive"
    namespace = "default"
    table_name = f"table_{suffix}"
    first_view_name = f"first_view_{suffix}"
    second_view_name = f"second_view_{suffix}"
    table_identifier = f"{catalog_name}.{namespace}.{table_name}"
    first_view_identifier = f"{catalog_name}.{namespace}.{first_view_name}"
    second_view_identifier = f"{catalog_name}.{namespace}.{second_view_name}"

    spark.sql(f"""
        CREATE TABLE {table_identifier} (
            id INTEGER,
            name STRING,
            dt DATE
        )
        USING iceberg
    """)

    spark.sql(f"""
        CREATE VIEW {first_view_identifier} AS
        SELECT id, name
        FROM {table_identifier}
    """)

    spark.sql(f"""
        CREATE VIEW {second_view_identifier} AS
        SELECT id, name, dt
        FROM {table_identifier}
    """)

    views = set(session_catalog_hive.list_views(namespace))

    assert (namespace, first_view_name) in views
    assert (namespace, second_view_name) in views

    # A table in the same namespace must not be returned as a view.
    assert (namespace, table_name) not in views
