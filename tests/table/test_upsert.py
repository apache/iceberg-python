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
from pathlib import PosixPath

import pyarrow as pa
import pytest
from datafusion import SessionContext
from pyarrow import Table as pa_table

from pyiceberg.catalog import Catalog
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.expressions import AlwaysTrue, And, EqualTo, Reference
from pyiceberg.expressions.literals import LongLiteral
from pyiceberg.io.pyarrow import schema_to_pyarrow
from pyiceberg.schema import Schema
from pyiceberg.table import UpsertResult
from pyiceberg.table.snapshots import Operation
from pyiceberg.table.upsert_util import create_match_filter
from pyiceberg.types import IntegerType, NestedField, StringType, StructType
from tests.catalog.test_base import InMemoryCatalog, Table


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
    "join_cols, src_start_row, src_end_row, target_start_row, target_end_row, when_matched_update_all, when_not_matched_insert_all, expected_updated, expected_inserted",
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

    with pytest.raises(Exception, match=r"""Field ".*" does not exist in schema"""):
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
        pa.lib.ArrowNotImplementedError, match="Keys of type struct<sub1: large_string not null, sub2: large_string not null>"
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
