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

import math
from datetime import date, datetime

import pyarrow as pa
import pytest
import pytz
from pyspark.sql import SparkSession

from pyiceberg.catalog import Catalog
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.typedef import Properties
from pyiceberg.types import (
    BinaryType,
    BooleanType,
    DateType,
    DoubleType,
    FixedType,
    FloatType,
    IntegerType,
    LongType,
    NestedField,
    StringType,
    TimestampType,
    TimestamptzType,
)

TABLE_SCHEMA = Schema(
    NestedField(field_id=1, name="bool", field_type=BooleanType(), required=False),
    NestedField(field_id=2, name="string", field_type=StringType(), required=False),
    NestedField(field_id=3, name="string_long", field_type=StringType(), required=False),
    NestedField(field_id=4, name="int", field_type=IntegerType(), required=False),
    NestedField(field_id=5, name="long", field_type=LongType(), required=False),
    NestedField(field_id=6, name="float", field_type=FloatType(), required=False),
    NestedField(field_id=7, name="double", field_type=DoubleType(), required=False),
    NestedField(field_id=8, name="timestamp", field_type=TimestampType(), required=False),
    NestedField(field_id=9, name="timestamptz", field_type=TimestamptzType(), required=False),
    NestedField(field_id=10, name="date", field_type=DateType(), required=False),
    # NestedField(field_id=11, name="time", field_type=TimeType(), required=False),
    # NestedField(field_id=12, name="uuid", field_type=UuidType(), required=False),
    NestedField(field_id=12, name="binary", field_type=BinaryType(), required=False),
    NestedField(field_id=13, name="fixed", field_type=FixedType(16), required=False),
)


def _create_table(session_catalog: Catalog, identifier: str, properties: Properties) -> Table:
    try:
        session_catalog.drop_table(identifier=identifier)
    except NoSuchTableError:
        pass

    return session_catalog.create_table(identifier=identifier, schema=TABLE_SCHEMA, properties=properties)


@pytest.mark.integration
@pytest.mark.parametrize("format_version", [1, 2])
def test_inspect_snapshots(
    spark: SparkSession, session_catalog: Catalog, arrow_table_with_null: pa.Table, format_version: int
) -> None:
    identifier = "default.table_metadata_snapshots"
    tbl = _create_table(session_catalog, identifier, properties={"format-version": format_version})

    tbl.overwrite(arrow_table_with_null)
    # should produce a DELETE entry
    tbl.overwrite(arrow_table_with_null)
    # Since we don't rewrite, this should produce a new manifest with an ADDED entry
    tbl.append(arrow_table_with_null)

    df = tbl.inspect.snapshots()

    assert df.column_names == [
        'committed_at',
        'snapshot_id',
        'parent_id',
        'operation',
        'manifest_list',
        'summary',
    ]

    for committed_at in df['committed_at']:
        assert isinstance(committed_at.as_py(), datetime)

    for snapshot_id in df['snapshot_id']:
        assert isinstance(snapshot_id.as_py(), int)

    assert df['parent_id'][0].as_py() is None
    assert df['parent_id'][1:] == df['snapshot_id'][:2]

    assert [operation.as_py() for operation in df['operation']] == ['append', 'overwrite', 'append']

    for manifest_list in df['manifest_list']:
        assert manifest_list.as_py().startswith("s3://")

    assert df['summary'][0].as_py() == [
        ('added-files-size', '5459'),
        ('added-data-files', '1'),
        ('added-records', '3'),
        ('total-data-files', '1'),
        ('total-delete-files', '0'),
        ('total-records', '3'),
        ('total-files-size', '5459'),
        ('total-position-deletes', '0'),
        ('total-equality-deletes', '0'),
    ]

    lhs = spark.table(f"{identifier}.snapshots").toPandas()
    rhs = df.to_pandas()
    for column in df.column_names:
        for left, right in zip(lhs[column].to_list(), rhs[column].to_list()):
            if column == 'summary':
                # Arrow returns a list of tuples, instead of a dict
                right = dict(right)

            if isinstance(left, float) and math.isnan(left) and isinstance(right, float) and math.isnan(right):
                # NaN != NaN in Python
                continue

            assert left == right, f"Difference in column {column}: {left} != {right}"


@pytest.mark.integration
@pytest.mark.parametrize("format_version", [1, 2])
def test_inspect_entries(
    spark: SparkSession, session_catalog: Catalog, arrow_table_with_null: pa.Table, format_version: int
) -> None:
    identifier = "default.table_metadata_entries"
    tbl = _create_table(session_catalog, identifier, properties={"format-version": format_version})

    # Write some data
    tbl.append(arrow_table_with_null)

    df = tbl.inspect.entries()

    assert df.column_names == [
        'status',
        'snapshot_id',
        'sequence_number',
        'file_sequence_number',
        'data_file',
        'readable_metrics',
    ]

    # Make sure that they are filled properly
    for int_column in ['status', 'snapshot_id', 'sequence_number', 'file_sequence_number']:
        for value in df[int_column]:
            assert isinstance(value.as_py(), int)

    for snapshot_id in df['snapshot_id']:
        assert isinstance(snapshot_id.as_py(), int)

    lhs = df.to_pandas()
    rhs = spark.table(f"{identifier}.entries").toPandas()
    for column in df.column_names:
        for left, right in zip(lhs[column].to_list(), rhs[column].to_list()):
            if column == 'data_file':
                right = right.asDict(recursive=True)
                for df_column in left.keys():
                    if df_column == 'partition':
                        # Spark leaves out the partition if the table is unpartitioned
                        continue

                    df_lhs = left[df_column]
                    df_rhs = right[df_column]
                    if isinstance(df_rhs, dict):
                        # Arrow turns dicts into lists of tuple
                        df_lhs = dict(df_lhs)

                    assert df_lhs == df_rhs, f"Difference in data_file column {df_column}: {df_lhs} != {df_rhs}"
            elif column == 'readable_metrics':
                right = right.asDict(recursive=True)

                assert list(left.keys()) == [
                    'bool',
                    'string',
                    'string_long',
                    'int',
                    'long',
                    'float',
                    'double',
                    'timestamp',
                    'timestamptz',
                    'date',
                    'binary',
                    'fixed',
                ]

                assert left.keys() == right.keys()

                for rm_column in left.keys():
                    rm_lhs = left[rm_column]
                    rm_rhs = right[rm_column]

                    assert rm_lhs['column_size'] == rm_rhs['column_size']
                    assert rm_lhs['value_count'] == rm_rhs['value_count']
                    assert rm_lhs['null_value_count'] == rm_rhs['null_value_count']
                    assert rm_lhs['nan_value_count'] == rm_rhs['nan_value_count']

                    if rm_column == 'timestamptz':
                        # PySpark does not correctly set the timstamptz
                        rm_rhs['lower_bound'] = rm_rhs['lower_bound'].replace(tzinfo=pytz.utc)
                        rm_rhs['upper_bound'] = rm_rhs['upper_bound'].replace(tzinfo=pytz.utc)

                    assert rm_lhs['lower_bound'] == rm_rhs['lower_bound']
                    assert rm_lhs['upper_bound'] == rm_rhs['upper_bound']
            else:
                assert left == right, f"Difference in column {column}: {left} != {right}"


@pytest.mark.integration
@pytest.mark.parametrize("format_version", [1, 2])
def test_inspect_entries_partitioned(spark: SparkSession, session_catalog: Catalog, format_version: int) -> None:
    identifier = "default.table_metadata_entries_partitioned"
    try:
        session_catalog.drop_table(identifier=identifier)
    except NoSuchTableError:
        pass

    spark.sql(
        f"""
        CREATE TABLE {identifier} (
            dt date
        )
        PARTITIONED BY (months(dt))
    """
    )

    spark.sql(
        f"""
        INSERT INTO {identifier} VALUES (CAST('2021-01-01' AS date))
    """
    )

    spark.sql(
        f"""
        ALTER TABLE {identifier}
        REPLACE PARTITION FIELD dt_month WITH days(dt)
    """
    )

    spark.sql(
        f"""
        INSERT INTO {identifier} VALUES (CAST('2021-02-01' AS date))
    """
    )

    df = session_catalog.load_table(identifier).inspect.entries()

    assert df.to_pydict()['data_file'][0]['partition'] == {'dt_day': date(2021, 2, 1), 'dt_month': None}
    assert df.to_pydict()['data_file'][1]['partition'] == {'dt_day': None, 'dt_month': 612}
