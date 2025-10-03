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

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date, date_add, expr

from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import FixedType, NestedField, UUIDType

# Create SparkSession against the remote Spark Connect server
spark = SparkSession.builder.remote("sc://localhost:15002").getOrCreate()

catalogs = {
    "rest": load_catalog(
        "rest",
        **{
            "type": "rest",
            "uri": "http://localhost:8181",
            "s3.endpoint": "http://localhost:9000",
            "s3.access-key-id": "admin",
            "s3.secret-access-key": "password",
        },
    ),
    "hive": load_catalog(
        "hive",
        **{
            "type": "hive",
            "uri": "thrift://localhost:9083",
            "s3.endpoint": "http://localhost:9000",
            "s3.access-key-id": "admin",
            "s3.secret-access-key": "password",
        },
    ),
}

for catalog_name, catalog in catalogs.items():
    spark.sql(
        f"""
      CREATE DATABASE IF NOT EXISTS {catalog_name}.default;
    """
    )

    schema = Schema(
        NestedField(field_id=1, name="uuid_col", field_type=UUIDType(), required=False),
        NestedField(field_id=2, name="fixed_col", field_type=FixedType(25), required=False),
    )

    catalog.create_table(identifier="default.test_uuid_and_fixed_unpartitioned", schema=schema)

    spark.sql(
        f"""
        INSERT INTO {catalog_name}.default.test_uuid_and_fixed_unpartitioned VALUES
        ('102cb62f-e6f8-4eb0-9973-d9b012ff0967', CAST('1234567890123456789012345' AS BINARY)),
        ('ec33e4b2-a834-4cc3-8c4a-a1d3bfc2f226', CAST('1231231231231231231231231' AS BINARY)),
        ('639cccce-c9d2-494a-a78c-278ab234f024', CAST('12345678901234567ass12345' AS BINARY)),
        ('c1b0d8e0-0b0e-4b1e-9b0a-0e0b0d0c0a0b', CAST('asdasasdads12312312312111' AS BINARY)),
        ('923dae77-83d6-47cd-b4b0-d383e64ee57e', CAST('qweeqwwqq1231231231231111' AS BINARY));
        """
    )

    spark.sql(
        f"""
      CREATE OR REPLACE TABLE {catalog_name}.default.test_null_nan
      USING iceberg
      AS SELECT
        1            AS idx,
        float('NaN') AS col_numeric
    UNION ALL SELECT
        2            AS idx,
        null         AS col_numeric
    UNION ALL SELECT
        3            AS idx,
        1            AS col_numeric
    """
    )

    spark.sql(
        f"""
      CREATE OR REPLACE TABLE {catalog_name}.default.test_null_nan_rewritten
      USING iceberg
      AS SELECT * FROM default.test_null_nan
    """
    )

    spark.sql(
        f"""
    CREATE OR REPLACE TABLE {catalog_name}.default.test_limit as
      SELECT * LATERAL VIEW explode(ARRAY(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)) AS idx;
    """
    )

    # Merge on read has been implemented in version ≥2:
    #   v2: Using positional deletes
    #   v3: Using deletion vectors

    for format_version in [2, 3]:
        identifier = f"{catalog_name}.default.test_positional_mor_deletes_v{format_version}"
        spark.sql(
            f"""
        CREATE OR REPLACE TABLE {identifier} (
            dt     date,
            number integer,
            letter string
        )
        USING iceberg
        TBLPROPERTIES (
            'write.delete.mode'='merge-on-read',
            'write.update.mode'='merge-on-read',
            'write.merge.mode'='merge-on-read',
            'format-version'='{format_version}'
        );
        """
        )

        spark.sql("""
        SELECT * FROM VALUES
            (CAST('2023-03-01' AS date), 1, 'a'),
            (CAST('2023-03-02' AS date), 2, 'b'),
            (CAST('2023-03-03' AS date), 3, 'c'),
            (CAST('2023-03-04' AS date), 4, 'd'),
            (CAST('2023-03-05' AS date), 5, 'e'),
            (CAST('2023-03-06' AS date), 6, 'f'),
            (CAST('2023-03-07' AS date), 7, 'g'),
            (CAST('2023-03-08' AS date), 8, 'h'),
            (CAST('2023-03-09' AS date), 9, 'i'),
            (CAST('2023-03-10' AS date), 10, 'j'),
            (CAST('2023-03-11' AS date), 11, 'k'),
            (CAST('2023-03-12' AS date), 12, 'l')
        AS t(dt, number, letter)
        """).coalesce(1).writeTo(identifier).append()

        spark.sql(f"ALTER TABLE {identifier} CREATE TAG tag_12")

        spark.sql(f"ALTER TABLE {identifier} CREATE BRANCH without_5")

        spark.sql(f"DELETE FROM {identifier}.branch_without_5 WHERE number = 5")

        spark.sql(f"DELETE FROM {identifier} WHERE number = 9")

        identifier = f"{catalog_name}.default.test_positional_mor_double_deletes_v{format_version}"

        spark.sql(
            f"""
          CREATE OR REPLACE TABLE {identifier} (
            dt     date,
            number integer,
            letter string
          )
          USING iceberg
          TBLPROPERTIES (
            'write.delete.mode'='merge-on-read',
            'write.update.mode'='merge-on-read',
            'write.merge.mode'='merge-on-read',
            'format-version'='{format_version}'
          );
        """
        )

        spark.sql("""
        SELECT * FROM VALUES
            (CAST('2023-03-01' AS date), 1, 'a'),
            (CAST('2023-03-02' AS date), 2, 'b'),
            (CAST('2023-03-03' AS date), 3, 'c'),
            (CAST('2023-03-04' AS date), 4, 'd'),
            (CAST('2023-03-05' AS date), 5, 'e'),
            (CAST('2023-03-06' AS date), 6, 'f'),
            (CAST('2023-03-07' AS date), 7, 'g'),
            (CAST('2023-03-08' AS date), 8, 'h'),
            (CAST('2023-03-09' AS date), 9, 'i'),
            (CAST('2023-03-10' AS date), 10, 'j'),
            (CAST('2023-03-11' AS date), 11, 'k'),
            (CAST('2023-03-12' AS date), 12, 'l')
        AS t(dt, number, letter)
        """).coalesce(1).writeTo(identifier).append()

        # Perform two deletes, should produce:
        #   v2: two positional delete files in v2
        #   v3: one deletion vector since they are merged
        spark.sql(f"DELETE FROM {identifier} WHERE number = 9")
        spark.sql(f"DELETE FROM {identifier} WHERE letter == 'f'")

    all_types_dataframe = (
        spark.range(0, 5, 1, 5)
        .withColumnRenamed("id", "longCol")
        .withColumn("intCol", expr("CAST(longCol AS INT)"))
        .withColumn("floatCol", expr("CAST(longCol AS FLOAT)"))
        .withColumn("doubleCol", expr("CAST(longCol AS DOUBLE)"))
        .withColumn("dateCol", date_add(current_date(), 1))
        .withColumn("timestampCol", expr("TO_TIMESTAMP(dateCol)"))
        .withColumn("stringCol", expr("CAST(dateCol AS STRING)"))
        .withColumn("booleanCol", expr("longCol > 5"))
        .withColumn("binaryCol", expr("CAST(longCol AS BINARY)"))
        .withColumn("byteCol", expr("CAST(longCol AS BYTE)"))
        .withColumn("decimalCol", expr("CAST(longCol AS DECIMAL(10, 2))"))
        .withColumn("shortCol", expr("CAST(longCol AS SHORT)"))
        .withColumn("mapCol", expr("MAP(longCol, decimalCol)"))
        .withColumn("arrayCol", expr("ARRAY(longCol)"))
        .withColumn("structCol", expr("STRUCT(mapCol, arrayCol)"))
    )

    all_types_dataframe.writeTo(f"{catalog_name}.default.test_all_types").tableProperty("format-version", "2").partitionedBy(
        "intCol"
    ).createOrReplace()

    for table_name, partition in [
        ("test_partitioned_by_identity", "ts"),
        ("test_partitioned_by_years", "years(dt)"),
        ("test_partitioned_by_months", "months(dt)"),
        ("test_partitioned_by_days", "days(ts)"),
        ("test_partitioned_by_hours", "hours(ts)"),
        ("test_partitioned_by_truncate", "truncate(1, letter)"),
        ("test_partitioned_by_bucket", "bucket(16, number)"),
    ]:
        spark.sql(
            f"""
          CREATE OR REPLACE TABLE {catalog_name}.default.{table_name} (
            dt     date,
            ts     timestamp,
            number integer,
            letter string
          )
          USING iceberg;
        """
        )

        spark.sql(f"ALTER TABLE {catalog_name}.default.{table_name} ADD PARTITION FIELD {partition}")

        spark.sql(
            f"""
        INSERT INTO {catalog_name}.default.{table_name}
        VALUES
            (CAST('2022-03-01' AS date), CAST('2022-03-01 01:22:00' AS timestamp), 1, 'a'),
            (CAST('2022-03-02' AS date), CAST('2022-03-02 02:22:00' AS timestamp), 2, 'b'),
            (CAST('2022-03-03' AS date), CAST('2022-03-03 03:22:00' AS timestamp), 3, 'c'),
            (CAST('2022-03-04' AS date), CAST('2022-03-04 04:22:00' AS timestamp), 4, 'd'),
            (CAST('2023-03-05' AS date), CAST('2023-03-05 05:22:00' AS timestamp), 5, 'e'),
            (CAST('2023-03-06' AS date), CAST('2023-03-06 06:22:00' AS timestamp), 6, 'f'),
            (CAST('2023-03-07' AS date), CAST('2023-03-07 07:22:00' AS timestamp), 7, 'g'),
            (CAST('2023-03-08' AS date), CAST('2023-03-08 08:22:00' AS timestamp), 8, 'h'),
            (CAST('2023-03-09' AS date), CAST('2023-03-09 09:22:00' AS timestamp), 9, 'i'),
            (CAST('2023-03-10' AS date), CAST('2023-03-10 10:22:00' AS timestamp), 10, 'j'),
            (CAST('2023-03-11' AS date), CAST('2023-03-11 11:22:00' AS timestamp), 11, 'k'),
            (CAST('2023-03-12' AS date), CAST('2023-03-12 12:22:00' AS timestamp), 12, 'l');
        """
        )

    spark.sql(
        f"""
    CREATE OR REPLACE TABLE {catalog_name}.default.test_table_version (
        dt     date,
        number integer,
        letter string
    )
    USING iceberg
    TBLPROPERTIES (
        'format-version'='1'
    );
    """
    )

    spark.sql(
        f"""
    CREATE TABLE {catalog_name}.default.test_table_sanitized_character (
        `letter/abc` string
    )
    USING iceberg
    TBLPROPERTIES (
        'format-version'='1'
    );
    """
    )

    spark.sql(
        f"""
    INSERT INTO {catalog_name}.default.test_table_sanitized_character
    VALUES
        ('123')
    """
    )

    spark.sql(
        f"""
    INSERT INTO {catalog_name}.default.test_table_sanitized_character
    VALUES
        ('123')
    """
    )

    spark.sql(
        f"""
    CREATE TABLE {catalog_name}.default.test_table_add_column (
        a string
    )
    USING iceberg
    """
    )

    spark.sql(f"INSERT INTO {catalog_name}.default.test_table_add_column VALUES ('1')")

    spark.sql(f"ALTER TABLE {catalog_name}.default.test_table_add_column ADD COLUMN b string")

    spark.sql(f"INSERT INTO {catalog_name}.default.test_table_add_column VALUES ('2', '2')")

    spark.sql(
        f"""
    CREATE TABLE {catalog_name}.default.test_table_empty_list_and_map (
        col_list             array<int>,
        col_map              map<int, int>,
        col_struct           struct<test:int>,
        col_list_with_struct array<struct<test:int>>
    )
    USING iceberg
    TBLPROPERTIES (
        'format-version'='1'
    );
    """
    )

    spark.sql(
        f"""
    INSERT INTO {catalog_name}.default.test_table_empty_list_and_map
    VALUES (null, null, null, null),
           (array(), map(), struct(1), array(struct(1)))
    """
    )

    spark.sql(
        f"""
        CREATE OR REPLACE TABLE {catalog_name}.default.test_table_snapshot_operations (
            number integer
        )
        USING iceberg
        TBLPROPERTIES (
            'format-version'='2'
        );
        """
    )

    spark.sql(
        f"""
        INSERT INTO {catalog_name}.default.test_table_snapshot_operations
        VALUES (1)
        """
    )

    spark.sql(
        f"""
        INSERT INTO {catalog_name}.default.test_table_snapshot_operations
        VALUES (2)
        """
    )

    spark.sql(
        f"""
        DELETE FROM {catalog_name}.default.test_table_snapshot_operations
        WHERE number = 2
        """
    )

    spark.sql(
        f"""
        INSERT INTO {catalog_name}.default.test_table_snapshot_operations
        VALUES (3)
        """
    )

    spark.sql(
        f"""
        INSERT INTO {catalog_name}.default.test_table_snapshot_operations
        VALUES (4)
        """
    )

    spark.sql(
        f"""
        CREATE OR REPLACE TABLE {catalog_name}.default.test_empty_scan_ordered_str (id string NOT NULL)
        USING iceberg
        TBLPROPERTIES ('format-version'='2')
        """
    )
    spark.sql(f"ALTER TABLE {catalog_name}.default.test_empty_scan_ordered_str WRITE ORDERED BY id")
    spark.sql(f"INSERT INTO {catalog_name}.default.test_empty_scan_ordered_str VALUES 'a', 'c'")
