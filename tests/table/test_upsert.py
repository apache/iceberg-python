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
import uuid
from datetime import date, datetime, time, timezone
from decimal import Decimal
from pathlib import PosixPath
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc
import pytest
from datafusion import SessionContext
from pyarrow import Table as pa_table

from pyiceberg.catalog import Catalog
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.expressions import AlwaysTrue, And, EqualTo, Reference
from pyiceberg.expressions.literals import LongLiteral
from pyiceberg.io.pyarrow import UnsupportedPyArrowTypeException, schema_to_pyarrow
from pyiceberg.schema import Schema
from pyiceberg.table import Table, UpsertResult
from pyiceberg.table.snapshots import Operation
from pyiceberg.table.upsert_util import create_match_filter
from pyiceberg.types import (
    BinaryType,
    BooleanType,
    DateType,
    DecimalType,
    FixedType,
    IntegerType,
    LongType,
    NestedField,
    PrimitiveType,
    StringType,
    StructType,
    TimestampType,
    TimestamptzType,
    TimeType,
)
from tests.catalog.test_base import InMemoryCatalog


@pytest.fixture
def catalog(tmp_path: PosixPath) -> InMemoryCatalog:
    catalog = InMemoryCatalog("test.in_memory.catalog", warehouse=tmp_path.absolute().as_posix())
    catalog.create_namespace("default")
    return catalog


def _drop_table(catalog: Catalog, identifier: str) -> None:
    try:
        catalog.drop_table(identifier)
    except NoSuchTableError:
        pass


def show_iceberg_table(table: Table, ctx: SessionContext) -> None:
    import pyarrow.dataset as ds

    table_name = "target"
    if ctx.table_exist(table_name):
        ctx.deregister_table(table_name)
    ctx.register_dataset(table_name, ds.dataset(table.scan().to_arrow()))
    ctx.sql(f"SELECT * FROM {table_name} limit 5").show()


def show_df(df: pa_table, ctx: SessionContext) -> None:
    import pyarrow.dataset as ds

    ctx.register_dataset("df", ds.dataset(df))
    ctx.sql("select * from df limit 10").show()


def gen_source_dataset(start_row: int, end_row: int, composite_key: bool, add_dup: bool, ctx: SessionContext) -> pa_table:
    additional_columns = ", t.order_id + 1000 as order_line_id" if composite_key else ""

    dup_row = (
        f"""
        UNION ALL
        (
        SELECT t.order_id {additional_columns}
            , date '2021-01-01' as order_date, 'B' as order_type
        from t
        limit 1
        )
    """
        if add_dup
        else ""
    )

    sql = f"""
        with t as (SELECT unnest(range({start_row},{end_row + 1})) as order_id)
        SELECT t.order_id {additional_columns}
            , date '2021-01-01' as order_date, 'B' as order_type
        from t
        {dup_row}
    """

    df = ctx.sql(sql).to_arrow_table()

    return df


def gen_target_iceberg_table(
    start_row: int, end_row: int, composite_key: bool, ctx: SessionContext, catalog: InMemoryCatalog, identifier: str
) -> Table:
    additional_columns = ", t.order_id + 1000 as order_line_id" if composite_key else ""

    df = ctx.sql(f"""
        with t as (SELECT unnest(range({start_row},{end_row + 1})) as order_id)
        SELECT t.order_id {additional_columns}
            , date '2021-01-01' as order_date, 'A' as order_type
        from t
    """).to_arrow_table()

    table = catalog.create_table(identifier, df.schema)

    table.append(df)

    return table


def assert_upsert_result(res: UpsertResult, expected_updated: int, expected_inserted: int) -> None:
    assert res.rows_updated == expected_updated, f"rows updated should be {expected_updated}, but got {res.rows_updated}"
    assert res.rows_inserted == expected_inserted, f"rows inserted should be {expected_inserted}, but got {res.rows_inserted}"


@pytest.mark.parametrize(
    (
        "join_cols, src_start_row, src_end_row, target_start_row, target_end_row, "
        "when_matched_update_all, when_not_matched_insert_all, expected_updated, expected_inserted"
    ),
    [
        (["order_id"], 1, 2, 2, 3, True, True, 1, 1),  # single row
        (["order_id"], 5001, 15000, 1, 10000, True, True, 5000, 5000),  # 10k rows
        (["order_id"], 501, 1500, 1, 1000, True, False, 500, 0),  # update only
        (["order_id"], 501, 1500, 1, 1000, False, True, 0, 500),  # insert only
    ],
)
def test_merge_rows(
    catalog: Catalog,
    join_cols: list[str],
    src_start_row: int,
    src_end_row: int,
    target_start_row: int,
    target_end_row: int,
    when_matched_update_all: bool,
    when_not_matched_insert_all: bool,
    expected_updated: int,
    expected_inserted: int,
) -> None:
    identifier = "default.test_merge_rows"
    _drop_table(catalog, identifier)

    ctx = SessionContext()

    source_df = gen_source_dataset(src_start_row, src_end_row, False, False, ctx)
    ice_table = gen_target_iceberg_table(target_start_row, target_end_row, False, ctx, catalog, identifier)
    res = ice_table.upsert(
        df=source_df,
        join_cols=join_cols,
        when_matched_update_all=when_matched_update_all,
        when_not_matched_insert_all=when_not_matched_insert_all,
    )

    assert_upsert_result(res, expected_updated, expected_inserted)


def test_merge_scenario_skip_upd_row(catalog: Catalog) -> None:
    """
    tests a single insert and update; skips a row that does not need to be updated
    """
    identifier = "default.test_merge_scenario_skip_upd_row"
    _drop_table(catalog, identifier)

    ctx = SessionContext()

    df = ctx.sql("""
        select 1 as order_id, date '2021-01-01' as order_date, 'A' as order_type
        union all
        select 2 as order_id, date '2021-01-01' as order_date, 'A' as order_type
    """).to_arrow_table()

    table = catalog.create_table(identifier, df.schema)

    table.append(df)

    source_df = ctx.sql("""
        select 1 as order_id, date '2021-01-01' as order_date, 'A' as order_type
        union all
        select 2 as order_id, date '2021-01-01' as order_date, 'B' as order_type
        union all
        select 3 as order_id, date '2021-01-01' as order_date, 'A' as order_type
    """).to_arrow_table()

    res = table.upsert(df=source_df, join_cols=["order_id"])

    expected_updated = 1
    expected_inserted = 1

    assert_upsert_result(res, expected_updated, expected_inserted)


def test_merge_scenario_date_as_key(catalog: Catalog) -> None:
    """
    tests a single insert and update; primary key is a date column
    """

    ctx = SessionContext()

    identifier = "default.test_merge_scenario_date_as_key"
    _drop_table(catalog, identifier)

    df = ctx.sql("""
        select date '2021-01-01' as order_date, 'A' as order_type
        union all
        select date '2021-01-02' as order_date, 'A' as order_type
    """).to_arrow_table()

    table = catalog.create_table(identifier, df.schema)

    table.append(df)

    source_df = ctx.sql("""
        select date '2021-01-01' as order_date, 'A' as order_type
        union all
        select date '2021-01-02' as order_date, 'B' as order_type
        union all
        select date '2021-01-03' as order_date, 'A' as order_type
    """).to_arrow_table()

    res = table.upsert(df=source_df, join_cols=["order_date"])

    expected_updated = 1
    expected_inserted = 1

    assert_upsert_result(res, expected_updated, expected_inserted)


def test_merge_scenario_string_as_key(catalog: Catalog) -> None:
    """
    tests a single insert and update; primary key is a string column
    """

    identifier = "default.test_merge_scenario_string_as_key"
    _drop_table(catalog, identifier)

    ctx = SessionContext()

    df = ctx.sql("""
        select 'abc' as order_id, 'A' as order_type
        union all
        select 'def' as order_id, 'A' as order_type
    """).to_arrow_table()

    table = catalog.create_table(identifier, df.schema)

    table.append(df)

    source_df = ctx.sql("""
        select 'abc' as order_id, 'A' as order_type
        union all
        select 'def' as order_id, 'B' as order_type
        union all
        select 'ghi' as order_id, 'A' as order_type
    """).to_arrow_table()

    res = table.upsert(df=source_df, join_cols=["order_id"])

    expected_updated = 1
    expected_inserted = 1

    assert_upsert_result(res, expected_updated, expected_inserted)


def test_merge_scenario_composite_key(catalog: Catalog) -> None:
    """
    tests merging 200 rows with a composite key
    """

    identifier = "default.test_merge_scenario_composite_key"
    _drop_table(catalog, identifier)

    ctx = SessionContext()

    table = gen_target_iceberg_table(1, 200, True, ctx, catalog, identifier)
    source_df = gen_source_dataset(101, 300, True, False, ctx)

    res = table.upsert(df=source_df, join_cols=["order_id", "order_line_id"])

    expected_updated = 100
    expected_inserted = 100

    assert_upsert_result(res, expected_updated, expected_inserted)


def test_merge_source_dups(catalog: Catalog) -> None:
    """
    tests duplicate rows in source
    """

    identifier = "default.test_merge_source_dups"
    _drop_table(catalog, identifier)

    ctx = SessionContext()

    table = gen_target_iceberg_table(1, 10, False, ctx, catalog, identifier)
    source_df = gen_source_dataset(5, 15, False, True, ctx)

    with pytest.raises(Exception, match="Duplicate rows found in source dataset based on the key columns. No upsert executed"):
        table.upsert(df=source_df, join_cols=["order_id"])


def test_key_cols_misaligned(catalog: Catalog) -> None:
    """
    tests join columns missing from one of the tables
    """

    identifier = "default.test_key_cols_misaligned"
    _drop_table(catalog, identifier)

    ctx = SessionContext()

    df = ctx.sql("select 1 as order_id, date '2021-01-01' as order_date, 'A' as order_type").to_arrow_table()

    table = catalog.create_table(identifier, df.schema)

    table.append(df)

    df_src = ctx.sql("select 1 as item_id, date '2021-05-01' as order_date, 'B' as order_type").to_arrow_table()

    with pytest.raises(ValueError, match="Join column 'order_id' does not exist in the source schema"):
        table.upsert(df=df_src, join_cols=["order_id"])


def test_upsert_with_identifier_fields(catalog: Catalog) -> None:
    identifier = "default.test_upsert_with_identifier_fields"
    _drop_table(catalog, identifier)

    schema = Schema(
        NestedField(1, "city", StringType(), required=True),
        NestedField(2, "population", IntegerType(), required=True),
        # Mark City as the identifier field, also known as the primary-key
        identifier_field_ids=[1],
    )

    tbl = catalog.create_table(identifier, schema=schema)

    arrow_schema = pa.schema(
        [
            pa.field("city", pa.string(), nullable=False),
            pa.field("population", pa.int32(), nullable=False),
        ]
    )

    # Write some data
    df = pa.Table.from_pylist(
        [
            {"city": "Amsterdam", "population": 921402},
            {"city": "San Francisco", "population": 808988},
            {"city": "Drachten", "population": 45019},
            {"city": "Paris", "population": 2103000},
        ],
        schema=arrow_schema,
    )
    tbl.append(df)

    df = pa.Table.from_pylist(
        [
            # Will be updated, the population has been updated
            {"city": "Drachten", "population": 45505},
            # New row, will be inserted
            {"city": "Berlin", "population": 3432000},
            # Ignored, already exists in the table
            {"city": "Paris", "population": 2103000},
        ],
        schema=arrow_schema,
    )
    upd = tbl.upsert(df)

    expected_operations = [Operation.APPEND, Operation.OVERWRITE, Operation.APPEND, Operation.APPEND]

    assert upd.rows_updated == 1
    assert upd.rows_inserted == 1

    assert [snap.summary.operation for snap in tbl.snapshots() if snap.summary is not None] == expected_operations

    # This should be a no-op
    upd = tbl.upsert(df)

    assert upd.rows_updated == 0
    assert upd.rows_inserted == 0

    assert [snap.summary.operation for snap in tbl.snapshots() if snap.summary is not None] == expected_operations


def test_upsert_into_empty_table(catalog: Catalog) -> None:
    identifier = "default.test_upsert_into_empty_table"
    _drop_table(catalog, identifier)

    schema = Schema(
        NestedField(1, "city", StringType(), required=True),
        NestedField(2, "inhabitants", IntegerType(), required=True),
        # Mark City as the identifier field, also known as the primary-key
        identifier_field_ids=[1],
    )

    tbl = catalog.create_table(identifier, schema=schema)

    arrow_schema = pa.schema(
        [
            pa.field("city", pa.string(), nullable=False),
            pa.field("inhabitants", pa.int32(), nullable=False),
        ]
    )

    # Write some data
    df = pa.Table.from_pylist(
        [
            {"city": "Amsterdam", "inhabitants": 921402},
            {"city": "San Francisco", "inhabitants": 808988},
            {"city": "Drachten", "inhabitants": 45019},
            {"city": "Paris", "inhabitants": 2103000},
        ],
        schema=arrow_schema,
    )
    upd = tbl.upsert(df)

    assert upd.rows_updated == 0
    assert upd.rows_inserted == 4


def test_create_match_filter_single_condition() -> None:
    """
    Test create_match_filter with a composite key where the source yields exactly one unique key.
    Expected: The function returns the single And condition directly.
    """

    data = [
        {"order_id": 101, "order_line_id": 1, "extra": "x"},
        {"order_id": 101, "order_line_id": 1, "extra": "x"},  # duplicate
    ]
    schema = pa.schema([pa.field("order_id", pa.int32()), pa.field("order_line_id", pa.int32()), pa.field("extra", pa.string())])
    table = pa.Table.from_pylist(data, schema=schema)
    expr = create_match_filter(table, ["order_id", "order_line_id"])
    assert expr == And(
        EqualTo(term=Reference(name="order_id"), literal=LongLiteral(101)),
        EqualTo(term=Reference(name="order_line_id"), literal=LongLiteral(1)),
    )


def test_upsert_with_duplicate_rows_in_table(catalog: Catalog) -> None:
    identifier = "default.test_upsert_with_duplicate_rows_in_table"

    _drop_table(catalog, identifier)
    schema = Schema(
        NestedField(1, "city", StringType(), required=True),
        NestedField(2, "inhabitants", IntegerType(), required=True),
        # Mark City as the identifier field, also known as the primary-key
        identifier_field_ids=[1],
    )

    tbl = catalog.create_table(identifier, schema=schema)

    arrow_schema = pa.schema(
        [
            pa.field("city", pa.string(), nullable=False),
            pa.field("inhabitants", pa.int32(), nullable=False),
        ]
    )

    # Write some data
    df = pa.Table.from_pylist(
        [
            {"city": "Drachten", "inhabitants": 45019},
            {"city": "Drachten", "inhabitants": 45019},
        ],
        schema=arrow_schema,
    )
    tbl.append(df)

    df = pa.Table.from_pylist(
        [
            # Will be updated, the inhabitants has been updated
            {"city": "Drachten", "inhabitants": 45505},
        ],
        schema=arrow_schema,
    )

    with pytest.raises(ValueError, match="Target table has duplicate rows, aborting upsert"):
        _ = tbl.upsert(df)


def test_upsert_without_identifier_fields(catalog: Catalog) -> None:
    identifier = "default.test_upsert_without_identifier_fields"
    _drop_table(catalog, identifier)

    schema = Schema(
        NestedField(1, "city", StringType(), required=True),
        NestedField(2, "population", IntegerType(), required=True),
        # No identifier field :o
        identifier_field_ids=[],
    )

    tbl = catalog.create_table(identifier, schema=schema)
    # Write some data
    df = pa.Table.from_pylist(
        [
            {"city": "Amsterdam", "population": 921402},
            {"city": "San Francisco", "population": 808988},
            {"city": "Drachten", "population": 45019},
            {"city": "Paris", "population": 2103000},
        ],
        schema=schema_to_pyarrow(schema),
    )

    with pytest.raises(
        ValueError, match="Join columns could not be found, please set identifier-field-ids or pass in explicitly."
    ):
        tbl.upsert(df)


def test_upsert_with_struct_field_as_non_join_key(catalog: Catalog) -> None:
    identifier = "default.test_upsert_struct_field_fails"
    _drop_table(catalog, identifier)

    schema = Schema(
        NestedField(1, "id", IntegerType(), required=True),
        NestedField(
            2,
            "nested_type",
            StructType(
                NestedField(3, "sub1", StringType(), required=True),
                NestedField(4, "sub2", StringType(), required=True),
            ),
            required=False,
        ),
        identifier_field_ids=[1],
    )

    tbl = catalog.create_table(identifier, schema=schema)

    arrow_schema = pa.schema(
        [
            pa.field("id", pa.int32(), nullable=False),
            pa.field(
                "nested_type",
                pa.struct(
                    [
                        pa.field("sub1", pa.large_string(), nullable=False),
                        pa.field("sub2", pa.large_string(), nullable=False),
                    ]
                ),
                nullable=True,
            ),
        ]
    )

    initial_data = pa.Table.from_pylist(
        [
            {
                "id": 1,
                "nested_type": {"sub1": "bla1", "sub2": "bla"},
            }
        ],
        schema=arrow_schema,
    )
    tbl.append(initial_data)

    update_data = pa.Table.from_pylist(
        [
            {
                "id": 2,
                "nested_type": {"sub1": "bla1", "sub2": "bla"},
            },
            {
                "id": 1,
                "nested_type": {"sub1": "bla1", "sub2": "bla2"},
            },
        ],
        schema=arrow_schema,
    )

    res = tbl.upsert(update_data, join_cols=["id"])

    expected_updated = 1
    expected_inserted = 1

    assert_upsert_result(res, expected_updated, expected_inserted)

    update_data = pa.Table.from_pylist(
        [
            {
                "id": 2,
                "nested_type": {"sub1": "bla1", "sub2": "bla"},
            },
            {
                "id": 1,
                "nested_type": {"sub1": "bla1", "sub2": "bla2"},
            },
        ],
        schema=arrow_schema,
    )

    res = tbl.upsert(update_data, join_cols=["id"])

    expected_updated = 0
    expected_inserted = 0

    assert_upsert_result(res, expected_updated, expected_inserted)


def test_upsert_with_struct_field_as_join_key(catalog: Catalog) -> None:
    identifier = "default.test_upsert_with_struct_field_as_join_key"
    _drop_table(catalog, identifier)

    schema = Schema(
        NestedField(1, "id", IntegerType(), required=True),
        NestedField(
            2,
            "nested_type",
            StructType(
                NestedField(3, "sub1", StringType(), required=True),
                NestedField(4, "sub2", StringType(), required=True),
            ),
            required=False,
        ),
        identifier_field_ids=[1],
    )

    tbl = catalog.create_table(identifier, schema=schema)

    arrow_schema = pa.schema(
        [
            pa.field("id", pa.int32(), nullable=False),
            pa.field(
                "nested_type",
                pa.struct(
                    [
                        pa.field("sub1", pa.large_string(), nullable=False),
                        pa.field("sub2", pa.large_string(), nullable=False),
                    ]
                ),
                nullable=True,
            ),
        ]
    )

    initial_data = pa.Table.from_pylist(
        [
            {
                "id": 1,
                "nested_type": {"sub1": "bla1", "sub2": "bla"},
            }
        ],
        schema=arrow_schema,
    )
    tbl.append(initial_data)

    update_data = pa.Table.from_pylist(
        [
            {
                "id": 2,
                "nested_type": {"sub1": "bla1", "sub2": "bla"},
            },
            {
                "id": 1,
                "nested_type": {"sub1": "bla1", "sub2": "bla"},
            },
        ],
        schema=arrow_schema,
    )

    with pytest.raises(
        ValueError,
        match=(
            "Nested column 'nested_type' of type 'struct<sub1: large_string not null, sub2: large_string not null>' "
            "cannot be used as a join key in upsert"
        ),
    ):
        _ = tbl.upsert(update_data, join_cols=["nested_type"])


def test_upsert_with_nulls(catalog: Catalog) -> None:
    identifier = "default.test_upsert_with_nulls"
    _drop_table(catalog, identifier)

    schema = pa.schema(
        [
            ("foo", pa.string()),
            ("bar", pa.int32()),
            ("baz", pa.bool_()),
        ]
    )

    # create table with null value
    table = catalog.create_table(identifier, schema)
    data_with_null = pa.Table.from_pylist(
        [
            {"foo": "apple", "bar": None, "baz": False},
            {"foo": "banana", "bar": None, "baz": False},
        ],
        schema=schema,
    )
    table.append(data_with_null)
    assert table.scan().to_arrow()["bar"].is_null()

    # upsert table with non-null value
    data_without_null = pa.Table.from_pylist(
        [
            {"foo": "apple", "bar": 7, "baz": False},
        ],
        schema=schema,
    )
    upd = table.upsert(data_without_null, join_cols=["foo"])
    assert upd.rows_updated == 1
    assert upd.rows_inserted == 0
    assert table.scan().to_arrow() == pa.Table.from_pylist(
        [
            {"foo": "apple", "bar": 7, "baz": False},
            {"foo": "banana", "bar": None, "baz": False},
        ],
        schema=schema,
    )


def test_transaction(catalog: Catalog) -> None:
    """Test the upsert within a Transaction. Make sure that if something fails the entire Transaction is
    rolled back."""
    identifier = "default.test_merge_source_dups"
    _drop_table(catalog, identifier)

    ctx = SessionContext()

    table = gen_target_iceberg_table(1, 10, False, ctx, catalog, identifier)
    df_before_transaction = table.scan().to_arrow()

    source_df = gen_source_dataset(5, 15, False, True, ctx)

    with pytest.raises(Exception, match="Duplicate rows found in source dataset based on the key columns. No upsert executed"):
        with table.transaction() as tx:
            tx.delete(delete_filter=AlwaysTrue())
            tx.upsert(df=source_df, join_cols=["order_id"])

    df = table.scan().to_arrow()

    assert df_before_transaction == df


def test_transaction_multiple_upserts(catalog: Catalog) -> None:
    identifier = "default.test_multi_upsert"
    _drop_table(catalog, identifier)

    schema = Schema(
        NestedField(1, "id", IntegerType(), required=True),
        NestedField(2, "name", StringType(), required=True),
        identifier_field_ids=[1],
    )

    tbl = catalog.create_table(identifier, schema=schema)

    # Define exact schema: required int32 and required string
    arrow_schema = pa.schema(
        [
            pa.field("id", pa.int32(), nullable=False),
            pa.field("name", pa.string(), nullable=False),
        ]
    )

    tbl.append(pa.Table.from_pylist([{"id": 1, "name": "Alice"}], schema=arrow_schema))

    df = pa.Table.from_pylist([{"id": 2, "name": "Bob"}, {"id": 1, "name": "Alicia"}], schema=arrow_schema)

    with tbl.transaction() as txn:
        txn.delete(delete_filter="id = 1")
        txn.append(df)

        # This should read the uncommitted changes
        txn.upsert(df, join_cols=["id"])

    result = tbl.scan().to_arrow().to_pylist()
    assert sorted(result, key=lambda x: x["id"]) == [
        {"id": 1, "name": "Alicia"},
        {"id": 2, "name": "Bob"},
    ]


def test_stage_only_upsert(catalog: Catalog) -> None:
    identifier = "default.test_stage_only_dynamic_partition_overwrite_files"
    _drop_table(catalog, identifier)

    schema = Schema(
        NestedField(1, "city", StringType(), required=True),
        NestedField(2, "inhabitants", IntegerType(), required=True),
        # Mark City as the identifier field, also known as the primary-key
        identifier_field_ids=[1],
    )

    tbl = catalog.create_table(identifier, schema=schema)

    arrow_schema = pa.schema(
        [
            pa.field("city", pa.string(), nullable=False),
            pa.field("inhabitants", pa.int32(), nullable=False),
        ]
    )

    # Write some data
    df = pa.Table.from_pylist(
        [
            {"city": "Amsterdam", "inhabitants": 921402},
            {"city": "San Francisco", "inhabitants": 808988},
            {"city": "Drachten", "inhabitants": 45019},
            {"city": "Paris", "inhabitants": 2103000},
        ],
        schema=arrow_schema,
    )

    tbl.append(df.slice(0, 1))
    current_snapshot = tbl.metadata.current_snapshot_id
    assert current_snapshot is not None

    original_count = len(tbl.scan().to_arrow())
    assert original_count == 1

    # write to staging snapshot
    upd = tbl.upsert(df, branch=None)
    assert upd.rows_updated == 0
    assert upd.rows_inserted == 3

    assert current_snapshot == tbl.metadata.current_snapshot_id
    assert len(tbl.scan().to_arrow()) == original_count
    snapshots = tbl.snapshots()
    assert len(snapshots) == 2

    # Write to main ref
    tbl.append(df.slice(1, 1))
    # Main ref has changed
    assert current_snapshot != tbl.metadata.current_snapshot_id
    assert len(tbl.scan().to_arrow()) == 2
    snapshots = tbl.snapshots()
    assert len(snapshots) == 3

    sorted_snapshots = sorted(tbl.snapshots(), key=lambda s: s.timestamp_ms)
    operations = [snapshot.summary.operation.value if snapshot.summary else None for snapshot in sorted_snapshots]
    parent_snapshot_id = [snapshot.parent_snapshot_id for snapshot in sorted_snapshots]
    assert operations == ["append", "append", "append"]
    # both subsequent parent id should be the first snapshot id
    assert parent_snapshot_id == [None, current_snapshot, current_snapshot]


def test_upsert_snapshot_properties(catalog: Catalog) -> None:
    """Test that snapshot_properties are applied to snapshots created by upsert."""
    identifier = "default.test_upsert_snapshot_properties"
    _drop_table(catalog, identifier)

    schema = Schema(
        NestedField(1, "city", StringType(), required=True),
        NestedField(2, "population", IntegerType(), required=True),
        identifier_field_ids=[1],
    )

    tbl = catalog.create_table(identifier, schema=schema)
    arrow_schema = pa.schema(
        [
            pa.field("city", pa.string(), nullable=False),
            pa.field("population", pa.int32(), nullable=False),
        ]
    )

    # Initial data
    df = pa.Table.from_pylist(
        [{"city": "Amsterdam", "population": 921402}],
        schema=arrow_schema,
    )
    tbl.append(df)
    initial_snapshot_count = len(list(tbl.snapshots()))

    # Upsert with snapshot_properties (both update and insert)
    df = pa.Table.from_pylist(
        [
            {"city": "Amsterdam", "population": 950000},  # Update
            {"city": "Berlin", "population": 3432000},  # Insert
        ],
        schema=arrow_schema,
    )
    result = tbl.upsert(df, snapshot_properties={"test_prop": "test_value"})

    assert result.rows_updated == 1
    assert result.rows_inserted == 1

    # Verify properties are on the snapshots created by upsert
    snapshots = list(tbl.snapshots())
    # Upsert should have created additional snapshots (overwrite + append)
    assert len(snapshots) > initial_snapshot_count

    # Check that all new snapshots have the snapshot_properties
    for snapshot in snapshots[initial_snapshot_count:]:
        assert snapshot.summary is not None
        assert snapshot.summary.additional_properties.get("test_prop") == "test_value"


_UUID_BYTES = uuid.uuid4().bytes
_STRUCT = pa.struct([("a", pa.int32())])
_MAP = pa.map_(pa.string(), pa.int32())


@pytest.mark.parametrize(
    "table_type, source_key, expected_error, match",
    [
        pytest.param(pa.float32(), pa.array([1.0], pa.float32()), ValueError, "Floating point column 'k'", id="float32"),
        pytest.param(pa.float64(), pa.array([1.0], pa.float64()), ValueError, "Floating point column 'k'", id="float64"),
        pytest.param(
            _STRUCT, pa.array([{"a": 1}], _STRUCT), ValueError, "Nested column 'k' of type 'struct<a: int32>'", id="struct"
        ),
        pytest.param(
            pa.list_(pa.int32()),
            pa.array([[1]], pa.list_(pa.int32())),
            ValueError,
            "Nested column 'k' of type 'large_list",
            id="list",
        ),
        pytest.param(
            _MAP, pa.array([[("a", 1)]], _MAP), ValueError, "Nested column 'k' of type 'map<large_string, int32>'", id="map"
        ),
        pytest.param(
            pa.uuid(),
            pa.array([_UUID_BYTES], pa.uuid()),
            NotImplementedError,
            "Column 'k' of type 'extension<arrow.uuid>'",
            id="uuid-table",
        ),
        pytest.param(
            pa.uuid(),
            pa.array([_UUID_BYTES], pa.binary(16)),
            NotImplementedError,
            "Column 'k' of type 'extension<arrow.uuid>'",
            id="uuid-table-fixed-source",
        ),
        pytest.param(
            pa.string(),
            pa.array([_UUID_BYTES], pa.uuid()),
            NotImplementedError,
            "Extension type 'extension<arrow.uuid>' for column 'k'",
            id="uuid-extension-source",
        ),
        pytest.param(
            pa.string(),
            pa.array(["a"]).dictionary_encode(),
            NotImplementedError,
            "Dictionary-encoded column 'k'",
            id="dictionary-string",
        ),
        pytest.param(
            pa.int64(),
            pa.array([1]).dictionary_encode(),
            NotImplementedError,
            "Dictionary-encoded column 'k'",
            id="dictionary-int",
        ),
        pytest.param(pa.int32(), pa.array([None], pa.null()), ValueError, "Null-type column 'k'", id="null-type"),
        pytest.param(
            pa.string(),
            pa.array(["a"], pa.string_view()),
            NotImplementedError,
            "View-typed column 'k' of type 'string_view'",
            id="string-view",
        ),
        pytest.param(
            pa.binary(),
            pa.array([b"a"], pa.binary_view()),
            NotImplementedError,
            "View-typed column 'k' of type 'binary_view'",
            id="binary-view",
        ),
        pytest.param(
            pa.int32(),
            pc.run_end_encode(pa.array([1], pa.int32())),
            UnsupportedPyArrowTypeException,
            "unsupported type: run_end_encoded",
            id="run-end-encoded",
        ),
        pytest.param(
            pa.int32(), pa.array([1, None], pa.int32()), ValueError, "Join column 'k' contains null values", id="null-values"
        ),
        pytest.param(
            pa.int32(), pa.array([None], pa.int32()), ValueError, "Join column 'k' contains null values", id="all-null-values"
        ),
        pytest.param(pa.int32(), pa.array(["1"]), ValueError, "Mismatch in fields", id="wrong-type-source"),
    ],
)
def test_upsert_rejects_unsupported_join_key(
    catalog: Catalog, table_type: pa.DataType, source_key: pa.Array, expected_error: type[Exception], match: str
) -> None:
    """Unreliable or unsupported join keys fail with a descriptive error before anything is written."""
    identifier = "default.test_upsert_rejects_unsupported_join_key"
    _drop_table(catalog, identifier)
    table = catalog.create_table(identifier, pa.schema([("k", table_type), ("payload", pa.string())]))
    source = pa.table({"k": source_key, "payload": ["val"] * len(source_key)})

    with pytest.raises(expected_error, match=match):
        table.upsert(source, join_cols=["k"])

    assert table.current_snapshot() is None


@pytest.mark.parametrize(
    "join_cols, identifier_field_ids, drop_source_columns, match",
    [
        pytest.param(["missing"], [], [], "Join column 'missing' does not exist in the table schema", id="not-in-table"),
        pytest.param(["K"], [], [], "Join column 'K' does not exist in the table schema", id="case-mismatch"),
        pytest.param(["k"], [], ["k"], "Join column 'k' does not exist in the source schema", id="required-not-in-source"),
        pytest.param(["opt"], [], ["opt"], "Join column 'opt' does not exist in the source schema", id="optional-not-in-source"),
        pytest.param(
            ["k", "opt"], [], ["opt"], "Join column 'opt' does not exist in the source schema", id="composite-second-missing"
        ),
        pytest.param(["k", "k"], [], [], "Duplicate join columns: k", id="duplicate-join-cols"),
        pytest.param(["s.x"], [], [], "Only top-level columns can be used as join keys", id="nested-path"),
        pytest.param(None, [4], [], "Only top-level columns can be used as join keys", id="nested-identifier-field"),
        pytest.param([], [], [], "Join columns could not be found", id="empty-join-cols"),
        pytest.param(None, [], [], "Join columns could not be found", id="no-identifier-fields"),
        pytest.param("k", [], [], "join_cols must be a list of column names, got str", id="string-not-list"),
        pytest.param({"k"}, [], [], "join_cols must be a list of column names, got set", id="set-not-list"),
    ],
)
def test_upsert_rejects_invalid_join_cols(
    catalog: Catalog, join_cols: Any, identifier_field_ids: list[int], drop_source_columns: list[str], match: str
) -> None:
    """Join column resolution fails clearly for missing, duplicate, nested, mistyped, or unresolvable columns."""
    identifier = "default.test_upsert_rejects_invalid_join_cols"
    _drop_table(catalog, identifier)
    schema = Schema(
        NestedField(1, "k", IntegerType(), required=True),
        NestedField(2, "opt", IntegerType(), required=False),
        NestedField(3, "s", StructType(NestedField(4, "x", IntegerType(), required=True)), required=True),
        NestedField(5, "payload", StringType(), required=False),
        identifier_field_ids=identifier_field_ids,
    )
    table = catalog.create_table(identifier, schema)
    source = pa.Table.from_pylist(
        [{"k": 1, "opt": 1, "s": {"x": 1}, "payload": "val"}],
        schema=pa.schema(
            [
                pa.field("k", pa.int32(), nullable=False),
                pa.field("opt", pa.int32(), nullable=True),
                pa.field("s", pa.struct([pa.field("x", pa.int32(), nullable=False)]), nullable=False),
                pa.field("payload", pa.string(), nullable=True),
            ]
        ),
    ).drop_columns(drop_source_columns)

    with pytest.raises(ValueError, match=match):
        table.upsert(source, join_cols=join_cols)

    assert table.current_snapshot() is None


_UTC = timezone.utc


@pytest.mark.parametrize(
    "iceberg_type, arrow_type, existing_key, new_key",
    [
        pytest.param(BooleanType(), pa.bool_(), False, True, id="boolean"),
        pytest.param(IntegerType(), pa.int32(), 1, 2, id="int"),
        pytest.param(LongType(), pa.int64(), 1, 2, id="long"),
        pytest.param(LongType(), pa.int32(), 1, 2, id="long-from-int32-source"),
        pytest.param(DecimalType(10, 2), pa.decimal128(10, 2), Decimal("1.50"), Decimal("2.50"), id="decimal"),
        pytest.param(DateType(), pa.date32(), date(2024, 1, 1), date(2024, 1, 2), id="date"),
        pytest.param(TimeType(), pa.time64("us"), time(1, 2, 3), time(4, 5, 6), id="time"),
        pytest.param(TimestampType(), pa.timestamp("us"), datetime(2024, 1, 1), datetime(2024, 1, 2), id="timestamp"),
        pytest.param(
            TimestamptzType(),
            pa.timestamp("us", "UTC"),
            datetime(2024, 1, 1, tzinfo=_UTC),
            datetime(2024, 1, 2, tzinfo=_UTC),
            id="timestamptz",
        ),
        pytest.param(StringType(), pa.string(), "a", "b", id="string"),
        pytest.param(StringType(), pa.large_string(), "a", "b", id="string-from-large-string-source"),
        pytest.param(BinaryType(), pa.binary(), b"a", b"b", id="binary"),
        pytest.param(BinaryType(), pa.large_binary(), b"a", b"b", id="binary-from-large-binary-source"),
        pytest.param(FixedType(2), pa.binary(2), b"aa", b"bb", id="fixed"),
    ],
)
def test_upsert_accepts_supported_join_key(
    catalog: Catalog, iceberg_type: PrimitiveType, arrow_type: pa.DataType, existing_key: Any, new_key: Any
) -> None:
    """Every supported primitive key updates matched rows and inserts new ones, including from a multi-chunk source."""
    identifier = "default.test_upsert_accepts_supported_join_key"
    _drop_table(catalog, identifier)
    schema = Schema(NestedField(1, "k", iceberg_type, required=True), NestedField(2, "payload", StringType(), required=True))
    arrow_schema = pa.schema([pa.field("k", arrow_type, nullable=False), pa.field("payload", pa.string(), nullable=False)])
    table = catalog.create_table(identifier, schema)
    table.append(pa.Table.from_pylist([{"k": existing_key, "payload": "old"}], schema=arrow_schema))

    source = pa.concat_tables(
        [
            pa.Table.from_pylist([{"k": existing_key, "payload": "updated"}], schema=arrow_schema),
            pa.Table.from_pylist([{"k": new_key, "payload": "inserted"}], schema=arrow_schema),
        ]
    )
    result = table.upsert(source, join_cols=["k"])

    assert (result.rows_updated, result.rows_inserted) == (1, 1)
    rows = table.scan().to_arrow().to_pylist()
    assert {(row["k"], row["payload"]) for row in rows} == {(existing_key, "updated"), (new_key, "inserted")}


def test_upsert_allows_null_join_key_values_in_target(catalog: Catalog) -> None:
    """Null key values are only rejected in the source; existing null-key rows in the table are left untouched."""
    identifier = "default.test_upsert_allows_null_join_key_values_in_target"
    _drop_table(catalog, identifier)
    table = catalog.create_table(identifier, pa.schema([("k", pa.int32()), ("payload", pa.string())]))
    table.append(pa.table({"k": pa.array([None, 1], pa.int32()), "payload": ["orphan", "old"]}))

    result = table.upsert(pa.table({"k": pa.array([1], pa.int32()), "payload": ["new"]}), join_cols=["k"])

    assert (result.rows_updated, result.rows_inserted) == (1, 0)
    rows = table.scan().to_arrow().to_pylist()
    assert {(row["k"], row["payload"]) for row in rows} == {(None, "orphan"), (1, "new")}
