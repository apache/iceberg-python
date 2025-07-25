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
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, List

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException

from pyiceberg.catalog import Catalog
from pyiceberg.partitioning import PartitionField, PartitionFieldValue, PartitionKey, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import (
    BucketTransform,
    DayTransform,
    HourTransform,
    IdentityTransform,
    MonthTransform,
    TruncateTransform,
    YearTransform,
)
from pyiceberg.typedef import Record
from pyiceberg.types import (
    BinaryType,
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    FixedType,
    FloatType,
    IntegerType,
    LongType,
    NestedField,
    StringType,
    TimestampType,
    TimestamptzType,
    UUIDType,
)

TABLE_SCHEMA = Schema(
    NestedField(field_id=1, name="boolean_field", field_type=BooleanType(), required=False),
    NestedField(field_id=2, name="string_field", field_type=StringType(), required=False),
    NestedField(field_id=3, name="string_long_field", field_type=StringType(), required=False),
    NestedField(field_id=4, name="int_field", field_type=IntegerType(), required=False),
    NestedField(field_id=5, name="long_field", field_type=LongType(), required=False),
    NestedField(field_id=6, name="float_field", field_type=FloatType(), required=False),
    NestedField(field_id=7, name="double_field", field_type=DoubleType(), required=False),
    NestedField(field_id=8, name="timestamp_field", field_type=TimestampType(), required=False),
    NestedField(field_id=9, name="timestamptz_field", field_type=TimestamptzType(), required=False),
    NestedField(field_id=10, name="date_field", field_type=DateType(), required=False),
    # NestedField(field_id=11, name="time", field_type=TimeType(), required=False),
    NestedField(field_id=11, name="binary_field", field_type=BinaryType(), required=False),
    NestedField(field_id=12, name="fixed_field", field_type=FixedType(16), required=False),
    NestedField(field_id=13, name="decimal_field", field_type=DecimalType(5, 2), required=False),
    NestedField(field_id=14, name="uuid_field", field_type=UUIDType(), required=False),
    NestedField(field_id=15, name="special#string+field", field_type=StringType(), required=False),
)


identifier = "default.test_table"


@pytest.mark.parametrize(
    "partition_fields, partition_values, expected_partition_record, expected_hive_partition_path_slice, spark_create_table_sql_for_justification, spark_data_insert_sql_for_justification",
    [
        # Identity Transform
        (
            [PartitionField(source_id=1, field_id=1001, transform=IdentityTransform(), name="boolean_field")],
            [False],
            Record(False),
            "boolean_field=false",
            f"""CREATE TABLE {identifier} (
                boolean_field boolean,
                string_field string
            )
            USING iceberg
            PARTITIONED BY (
                identity(boolean_field)  -- Partitioning by 'boolean_field'
            )
            """,
            f"""INSERT INTO {identifier}
            VALUES
            (false, 'Boolean field set to false');
            """,
        ),
        (
            [PartitionField(source_id=2, field_id=1001, transform=IdentityTransform(), name="string_field")],
            ["sample_string"],
            Record("sample_string"),
            "string_field=sample_string",
            f"""CREATE TABLE {identifier} (
                string_field string,
                another_string_field string
            )
            USING iceberg
            PARTITIONED BY (
                identity(string_field)
            )
            """,
            f"""INSERT INTO {identifier}
            VALUES
            ('sample_string', 'Another string value')
            """,
        ),
        (
            [PartitionField(source_id=4, field_id=1001, transform=IdentityTransform(), name="int_field")],
            [42],
            Record(42),
            "int_field=42",
            f"""CREATE TABLE {identifier} (
                int_field int,
                string_field string
            )
            USING iceberg
            PARTITIONED BY (
                identity(int_field)
            )
            """,
            f"""INSERT INTO {identifier}
            VALUES
            (42, 'Associated string value for int 42')
            """,
        ),
        (
            [PartitionField(source_id=5, field_id=1001, transform=IdentityTransform(), name="long_field")],
            [1234567890123456789],
            Record(1234567890123456789),
            "long_field=1234567890123456789",
            f"""CREATE TABLE {identifier} (
                long_field bigint,
                string_field string
            )
            USING iceberg
            PARTITIONED BY (
                identity(long_field)
            )
            """,
            f"""INSERT INTO {identifier}
            VALUES
            (1234567890123456789, 'Associated string value for long 1234567890123456789')
            """,
        ),
        (
            [PartitionField(source_id=6, field_id=1001, transform=IdentityTransform(), name="float_field")],
            [3.14],
            Record(3.14),
            "float_field=3.14",
            # spark writes differently as pyiceberg, Record[float_field=3.140000104904175], path:float_field=3.14 (Record has difference)
            # so justification (compare expected value with spark behavior) would fail.
            None,
            None,
            # f"""CREATE TABLE {identifier} (
            #     float_field float,
            #     string_field string
            # )
            # USING iceberg
            # PARTITIONED BY (
            #     identity(float_field)
            # )
            # """,
            # f"""INSERT INTO {identifier}
            # VALUES
            # (3.14, 'Associated string value for float 3.14')
            # """
        ),
        (
            [PartitionField(source_id=7, field_id=1001, transform=IdentityTransform(), name="double_field")],
            [6.282],
            Record(6.282),
            "double_field=6.282",
            # spark writes differently as pyiceberg, Record[double_field=6.2820000648498535] path:double_field=6.282 (Record has difference)
            # so justification (compare expected value with spark behavior) would fail.
            None,
            None,
            # f"""CREATE TABLE {identifier} (
            #     double_field double,
            #     string_field string
            # )
            # USING iceberg
            # PARTITIONED BY (
            #     identity(double_field)
            # )
            # """,
            # f"""INSERT INTO {identifier}
            # VALUES
            # (6.282, 'Associated string value for double 6.282')
            # """
        ),
        (
            [PartitionField(source_id=8, field_id=1001, transform=IdentityTransform(), name="timestamp_field")],
            [datetime(2023, 1, 1, 12, 0, 1, 999)],
            Record(1672574401000999),
            "timestamp_field=2023-01-01T12%3A00%3A01.000999",
            f"""CREATE TABLE {identifier} (
                timestamp_field timestamp_ntz,
                string_field string
            )
            USING iceberg
            PARTITIONED BY (
                identity(timestamp_field)
            )
            """,
            f"""INSERT INTO {identifier}
            VALUES
            (CAST('2023-01-01 12:00:01.000999' AS TIMESTAMP_NTZ), 'Associated string value for timestamp 2023-01-01T12:00:00')
            """,
        ),
        (
            [PartitionField(source_id=8, field_id=1001, transform=IdentityTransform(), name="timestamp_field")],
            [datetime(2023, 1, 1, 12, 0, 1)],
            Record(1672574401000000),
            "timestamp_field=2023-01-01T12%3A00%3A01",
            f"""CREATE TABLE {identifier} (
                timestamp_field timestamp_ntz,
                string_field string
            )
            USING iceberg
            PARTITIONED BY (
                identity(timestamp_field)
            )
            """,
            f"""INSERT INTO {identifier}
            VALUES
            (CAST('2023-01-01 12:00:01' AS TIMESTAMP_NTZ), 'Associated string value for timestamp 2023-01-01T12:00:00')
            """,
        ),
        (
            [PartitionField(source_id=8, field_id=1001, transform=IdentityTransform(), name="timestamp_field")],
            [datetime(2023, 1, 1, 12, 0, 0)],
            Record(1672574400000000),
            "timestamp_field=2023-01-01T12%3A00%3A00",
            # Spark writes differently as pyiceberg, so justification (compare expected value with spark behavior) would fail
            # AssertionError: assert 'timestamp_field=2023-01-01T12%3A00%3A00' in 's3://warehouse/default/test_table/data/timestamp_field=2023-01-01T12%3A00/00000-5-f9dca69a-9fb7-4830-9ef6-62d3d7afc09e-00001.parquet'
            # TLDR: CAST('2023-01-01 12:00:00' AS TIMESTAMP_NTZ) becomes 2023-01-01T12:00 in the hive partition path when spark writes it (without the seconds).
            None,
            None,
            # f"""CREATE TABLE {identifier} (
            #     timestamp_field timestamp_ntz,
            #     string_field string
            # )
            # USING iceberg
            # PARTITIONED BY (
            #     identity(timestamp_field)
            # )
            # """,
            # f"""INSERT INTO {identifier}
            # VALUES
            # (CAST('2023-01-01 12:00:00' AS TIMESTAMP_NTZ), 'Associated string value for timestamp 2023-01-01T12:00:00')
            # """
        ),
        (
            [PartitionField(source_id=9, field_id=1001, transform=IdentityTransform(), name="timestamptz_field")],
            [datetime(2023, 1, 1, 12, 0, 1, 999, tzinfo=timezone(timedelta(hours=3)))],
            Record(1672563601000999),
            "timestamptz_field=2023-01-01T09%3A00%3A01.000999%2B00%3A00",
            # Spark writes differently as pyiceberg, so justification (compare expected value with spark behavior) would fail
            # AssertionError: assert 'timestamptz_field=2023-01-01T09%3A00%3A01.000999%2B00%3A00' in 's3://warehouse/default/test_table/data/timestamptz_field=2023-01-01T09%3A00%3A01.000999Z/00000-5-b710fc4d-66b6-47f1-b8ae-6208f8aaa2d4-00001.parquet'
            # TLDR: CAST('2023-01-01 12:00:01.000999+03:00' AS TIMESTAMP) becomes 2023-01-01T09:00:01.000999Z in the hive partition path when spark writes it (while iceberg: timestamptz_field=2023-01-01T09:00:01.000999+00:00).
            None,
            None,
            # f"""CREATE TABLE {identifier} (
            #     timestamptz_field timestamp,
            #     string_field string
            # )
            # USING iceberg
            # PARTITIONED BY (
            #     identity(timestamptz_field)
            # )
            # """,
            # f"""INSERT INTO {identifier}
            # VALUES
            # (CAST('2023-01-01 12:00:01.000999+03:00' AS TIMESTAMP), 'Associated string value for timestamp 2023-01-01 12:00:01.000999+03:00')
            # """
        ),
        (
            [PartitionField(source_id=10, field_id=1001, transform=IdentityTransform(), name="date_field")],
            [date(2023, 1, 1)],
            Record(19358),
            "date_field=2023-01-01",
            f"""CREATE TABLE {identifier} (
                date_field date,
                string_field string
            )
            USING iceberg
            PARTITIONED BY (
                identity(date_field)
            )
            """,
            f"""INSERT INTO {identifier}
            VALUES
            (CAST('2023-01-01' AS DATE), 'Associated string value for date 2023-01-01')
            """,
        ),
        (
            [PartitionField(source_id=11, field_id=1001, transform=IdentityTransform(), name="binary_field")],
            [b"example"],
            Record(b"example"),
            "binary_field=ZXhhbXBsZQ%3D%3D",
            f"""CREATE TABLE {identifier} (
                binary_field binary,
                string_field string
            )
            USING iceberg
            PARTITIONED BY (
                identity(binary_field)
            )
            """,
            f"""INSERT INTO {identifier}
            VALUES
            (CAST('example' AS BINARY), 'Associated string value for binary `example`')
            """,
        ),
        (
            [PartitionField(source_id=13, field_id=1001, transform=IdentityTransform(), name="decimal_field")],
            [Decimal("123.45")],
            Record(Decimal("123.45")),
            "decimal_field=123.45",
            f"""CREATE TABLE {identifier} (
                decimal_field decimal(5,2),
                string_field string
            )
            USING iceberg
            PARTITIONED BY (
                identity(decimal_field)
            )
            """,
            f"""INSERT INTO {identifier}
            VALUES
            (123.45, 'Associated string value for decimal 123.45')
            """,
        ),
        # # Year Month Day Hour Transform
        # Month Transform
        (
            [PartitionField(source_id=8, field_id=1001, transform=MonthTransform(), name="timestamp_field_month")],
            [datetime(2023, 1, 1, 11, 55, 59, 999999)],
            Record((2023 - 1970) * 12),
            "timestamp_field_month=2023-01",
            f"""CREATE TABLE {identifier} (
                timestamp_field timestamp_ntz,
                string_field string
            )
            USING iceberg
            PARTITIONED BY (
                month(timestamp_field)  -- Partitioning by month from 'timestamp_field'
            )
            """,
            f"""INSERT INTO {identifier}
            VALUES
            (CAST('2023-01-01 11:55:59.999999' AS TIMESTAMP_NTZ), 'Event at 2023-01-01 11:55:59.999999');
            """,
        ),
        (
            [PartitionField(source_id=9, field_id=1001, transform=MonthTransform(), name="timestamptz_field_month")],
            [datetime(2023, 1, 1, 12, 0, 1, 999, tzinfo=timezone(timedelta(hours=3)))],
            Record((2023 - 1970) * 12 + 1 - 1),
            "timestamptz_field_month=2023-01",
            f"""CREATE TABLE {identifier} (
                timestamptz_field timestamp,
                string_field string
            )
            USING iceberg
            PARTITIONED BY (
                month(timestamptz_field)
            )
            """,
            f"""INSERT INTO {identifier}
            VALUES
            (CAST('2023-01-01 12:00:01.000999+03:00' AS TIMESTAMP), 'Event at 2023-01-01 12:00:01.000999+03:00');
            """,
        ),
        (
            [PartitionField(source_id=10, field_id=1001, transform=MonthTransform(), name="date_field_month")],
            [date(2023, 1, 1)],
            Record((2023 - 1970) * 12),
            "date_field_month=2023-01",
            f"""CREATE TABLE {identifier} (
                date_field date,
                string_field string
            )
            USING iceberg
            PARTITIONED BY (
                month(date_field)  -- Partitioning by month from 'date_field'
            )
            """,
            f"""INSERT INTO {identifier}
            VALUES
            (CAST('2023-01-01' AS DATE), 'Event on 2023-01-01');
            """,
        ),
        # Year Transform
        (
            [PartitionField(source_id=8, field_id=1001, transform=YearTransform(), name="timestamp_field_year")],
            [datetime(2023, 1, 1, 11, 55, 59, 999999)],
            Record(2023 - 1970),
            "timestamp_field_year=2023",
            f"""CREATE TABLE {identifier}  (
                timestamp_field timestamp,
                string_field string
            )
            USING iceberg
            PARTITIONED BY (
                year(timestamp_field)  -- Partitioning by year from 'timestamp_field'
            )
            """,
            f"""INSERT INTO {identifier}
            VALUES
            (CAST('2023-01-01 11:55:59.999999' AS TIMESTAMP), 'Event at 2023-01-01 11:55:59.999999');
            """,
        ),
        (
            [PartitionField(source_id=9, field_id=1001, transform=YearTransform(), name="timestamptz_field_year")],
            [datetime(2023, 1, 1, 12, 0, 1, 999, tzinfo=timezone(timedelta(hours=3)))],
            Record(53),
            "timestamptz_field_year=2023",
            f"""CREATE TABLE {identifier} (
                timestamptz_field timestamp,
                string_field string
            )
            USING iceberg
            PARTITIONED BY (
                year(timestamptz_field)
            )
            """,
            f"""INSERT INTO {identifier}
            VALUES
            (CAST('2023-01-01 12:00:01.000999+03:00' AS TIMESTAMP), 'Event at 2023-01-01 12:00:01.000999+03:00');
            """,
        ),
        (
            [PartitionField(source_id=10, field_id=1001, transform=YearTransform(), name="date_field_year")],
            [date(2023, 1, 1)],
            Record(2023 - 1970),
            "date_field_year=2023",
            f"""CREATE TABLE {identifier} (
                date_field date,
                string_field string
            )
            USING iceberg
            PARTITIONED BY (
                year(date_field)  -- Partitioning by year from 'date_field'
            )
            """,
            f"""INSERT INTO {identifier}
            VALUES
            (CAST('2023-01-01' AS DATE), 'Event on 2023-01-01');
            """,
        ),
        # # Day Transform
        (
            [PartitionField(source_id=8, field_id=1001, transform=DayTransform(), name="timestamp_field_day")],
            [datetime(2023, 1, 1, 11, 55, 59, 999999)],
            Record(19358),
            "timestamp_field_day=2023-01-01",
            f"""CREATE TABLE {identifier} (
                timestamp_field timestamp,
                string_field string
            )
            USING iceberg
            PARTITIONED BY (
                day(timestamp_field)  -- Partitioning by day from 'timestamp_field'
            )
            """,
            f"""INSERT INTO {identifier}
            VALUES
            (CAST('2023-01-01' AS DATE), 'Event on 2023-01-01');
            """,
        ),
        (
            [PartitionField(source_id=9, field_id=1001, transform=DayTransform(), name="timestamptz_field_day")],
            [datetime(2023, 1, 1, 12, 0, 1, 999, tzinfo=timezone(timedelta(hours=3)))],
            Record(19358),
            "timestamptz_field_day=2023-01-01",
            f"""CREATE TABLE {identifier} (
                timestamptz_field timestamp,
                string_field string
            )
            USING iceberg
            PARTITIONED BY (
                day(timestamptz_field)
            )
            """,
            f"""INSERT INTO {identifier}
            VALUES
            (CAST('2023-01-01 12:00:01.000999+03:00' AS TIMESTAMP), 'Event at 2023-01-01 12:00:01.000999+03:00');
            """,
        ),
        (
            [PartitionField(source_id=10, field_id=1001, transform=DayTransform(), name="date_field_day")],
            [date(2023, 1, 1)],
            Record(19358),
            "date_field_day=2023-01-01",
            f"""CREATE TABLE {identifier} (
                date_field date,
                string_field string
            )
            USING iceberg
            PARTITIONED BY (
                day(date_field)  -- Partitioning by day from 'date_field'
            )
            """,
            f"""INSERT INTO {identifier}
            VALUES
            (CAST('2023-01-01' AS DATE), 'Event on 2023-01-01');
            """,
        ),
        # Hour Transform
        (
            [PartitionField(source_id=8, field_id=1001, transform=HourTransform(), name="timestamp_field_hour")],
            [datetime(2023, 1, 1, 11, 55, 59, 999999)],
            Record(464603),
            "timestamp_field_hour=2023-01-01-11",
            f"""CREATE TABLE {identifier} (
                timestamp_field timestamp,
                string_field string
            )
            USING iceberg
            PARTITIONED BY (
                hour(timestamp_field)  -- Partitioning by hour from 'timestamp_field'
            )
            """,
            f"""INSERT INTO {identifier}
                VALUES
                (CAST('2023-01-01 11:55:59.999999' AS TIMESTAMP), 'Event within the 11th hour of 2023-01-01');
                """,
        ),
        (
            [PartitionField(source_id=9, field_id=1001, transform=HourTransform(), name="timestamptz_field_hour")],
            [datetime(2023, 1, 1, 12, 0, 1, 999, tzinfo=timezone(timedelta(hours=3)))],
            Record(464601),
            "timestamptz_field_hour=2023-01-01-09",
            f"""CREATE TABLE {identifier} (
                timestamptz_field timestamp,
                string_field string
            )
            USING iceberg
            PARTITIONED BY (
                hour(timestamptz_field)
            )
            """,
            f"""INSERT INTO {identifier}
            VALUES
            (CAST('2023-01-01 12:00:01.000999+03:00' AS TIMESTAMP), 'Event at 2023-01-01 12:00:01.000999+03:00');
            """,
        ),
        # Truncate Transform
        (
            [PartitionField(source_id=4, field_id=1001, transform=TruncateTransform(10), name="int_field_trunc")],
            [12345],
            Record(12340),
            "int_field_trunc=12340",
            f"""CREATE TABLE {identifier} (
                int_field int,
                string_field string
            )
            USING iceberg
            PARTITIONED BY (
                truncate(int_field, 10)  -- Truncating 'int_field' integer column to a width of 10
            )
            """,
            f"""INSERT INTO {identifier}
                VALUES
                (12345, 'Sample data for int');
            """,
        ),
        (
            [PartitionField(source_id=5, field_id=1001, transform=TruncateTransform(2), name="bigint_field_trunc")],
            [2**32 + 1],
            Record(2**32),  # 4294967296
            "bigint_field_trunc=4294967296",
            f"""CREATE TABLE {identifier} (
                bigint_field bigint,
                string_field string
            )
            USING iceberg
            PARTITIONED BY (
                truncate(bigint_field, 2)  -- Truncating 'bigint_field' long column to a width of 2
            )
            """,
            f"""INSERT INTO {identifier}
            VALUES
            (4294967297, 'Sample data for long');
            """,
        ),
        (
            [PartitionField(source_id=2, field_id=1001, transform=TruncateTransform(3), name="string_field_trunc")],
            ["abcdefg"],
            Record("abc"),
            "string_field_trunc=abc",
            f"""CREATE TABLE {identifier} (
                string_field string,
                another_string_field string
            )
            USING iceberg
            PARTITIONED BY (
                truncate(string_field, 3)  -- Truncating 'string_field' string column to a length of 3 characters
            )
            """,
            f"""INSERT INTO {identifier}
            VALUES
            ('abcdefg', 'Another sample for string');
            """,
        ),
        (
            [PartitionField(source_id=13, field_id=1001, transform=TruncateTransform(width=5), name="decimal_field_trunc")],
            [Decimal("678.93")],
            Record(Decimal("678.90")),
            "decimal_field_trunc=678.90",  # Assuming truncation width of 1 leads to truncating to 670
            f"""CREATE TABLE {identifier} (
                decimal_field decimal(5,2),
                string_field string
            )
            USING iceberg
            PARTITIONED BY (
                truncate(decimal_field, 2)
            )
            """,
            f"""INSERT INTO {identifier}
            VALUES
            (678.90, 'Associated string value for decimal 678.90')
            """,
        ),
        (
            [PartitionField(source_id=11, field_id=1001, transform=TruncateTransform(10), name="binary_field_trunc")],
            [b"HELLOICEBERG"],
            Record(b"HELLOICEBE"),
            "binary_field_trunc=SEVMTE9JQ0VCRQ%3D%3D",
            f"""CREATE TABLE {identifier} (
                binary_field binary,
                string_field string
            )
            USING iceberg
            PARTITIONED BY (
                truncate(binary_field, 10)  -- Truncating 'binary_field' binary column to a length of 10 bytes
            )
            """,
            f"""INSERT INTO {identifier}
                VALUES
                (binary('HELLOICEBERG'), 'Sample data for binary');
            """,
        ),
        # Bucket Transform
        (
            [PartitionField(source_id=4, field_id=1001, transform=BucketTransform(2), name="int_field_bucket")],
            [10],
            Record(0),
            "int_field_bucket=0",
            f"""CREATE TABLE {identifier} (
                int_field int,
                string_field string
            )
            USING iceberg
            PARTITIONED BY (
                bucket(2, int_field)  -- Distributing 'int_field' across 2 buckets
            )
            """,
            f"""INSERT INTO {identifier}
            VALUES
            (10, 'Integer with value 10');
            """,
        ),
        # Test multiple field combinations could generate the Partition record and hive partition path correctly
        (
            [
                PartitionField(source_id=8, field_id=1001, transform=YearTransform(), name="timestamp_field_year"),
                PartitionField(source_id=10, field_id=1002, transform=DayTransform(), name="date_field_day"),
            ],
            [
                datetime(2023, 1, 1, 11, 55, 59, 999999),
                date(2023, 1, 1),
            ],
            Record(53, 19358),
            "timestamp_field_year=2023/date_field_day=2023-01-01",
            f"""CREATE TABLE {identifier} (
                timestamp_field timestamp,
                date_field date,
                string_field string
            )
            USING iceberg
            PARTITIONED BY (
                year(timestamp_field),
                day(date_field)
            )
            """,
            f"""INSERT INTO {identifier}
            VALUES
            (CAST('2023-01-01 11:55:59.999999' AS TIMESTAMP), CAST('2023-01-01' AS DATE), 'some data');
            """,
        ),
        # Test that special characters are URL-encoded
        (
            [PartitionField(source_id=15, field_id=1001, transform=IdentityTransform(), name="special#string+field")],
            ["special string"],
            Record("special string"),
            "special%23string%2Bfield=special+string",
            f"""CREATE TABLE {identifier} (
                `special#string+field` string
            )
            USING iceberg
            PARTITIONED BY (
                identity(`special#string+field`)
            )
            """,
            f"""INSERT INTO {identifier}
            VALUES
            ('special string')
            """,
        ),
    ],
)
@pytest.mark.integration
def test_partition_key(
    session_catalog: Catalog,
    spark: SparkSession,
    partition_fields: List[PartitionField],
    partition_values: List[Any],
    expected_partition_record: Record,
    expected_hive_partition_path_slice: str,
    spark_create_table_sql_for_justification: str,
    spark_data_insert_sql_for_justification: str,
) -> None:
    field_values = [
        PartitionFieldValue(field, field.transform.transform(TABLE_SCHEMA.find_field(field.source_id).field_type)(value))
        for field, value in zip(partition_fields, partition_values)
    ]
    spec = PartitionSpec(*partition_fields)

    key = PartitionKey(
        field_values=field_values,
        partition_spec=spec,
        schema=TABLE_SCHEMA,
    )

    # key.partition is used to write the metadata in DataFile, ManifestFile and all above layers
    assert key.partition == expected_partition_record
    # key.to_path() generates the hive partitioning part of the to-write parquet file path
    assert key.to_path() == expected_hive_partition_path_slice

    # Justify expected values are not made up but conforming to spark behaviors
    if spark_create_table_sql_for_justification is not None and spark_data_insert_sql_for_justification is not None:
        try:
            spark.sql(f"drop table {identifier}")
        except AnalysisException:
            pass

        spark.sql(spark_create_table_sql_for_justification)
        spark.sql(spark_data_insert_sql_for_justification)

        iceberg_table = session_catalog.load_table(identifier=identifier)
        snapshot = iceberg_table.current_snapshot()
        assert snapshot
        spark_partition_for_justification = (
            snapshot.manifests(iceberg_table.io)[0].fetch_manifest_entry(iceberg_table.io)[0].data_file.partition
        )
        spark_path_for_justification = (
            snapshot.manifests(iceberg_table.io)[0].fetch_manifest_entry(iceberg_table.io)[0].data_file.file_path
        )
        # Special characters in partition value are sanitized when written to the data file's partition field
        assert spark_partition_for_justification == expected_partition_record
        assert expected_hive_partition_path_slice in spark_path_for_justification
