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
from pyiceberg.table import Table, UpsertResult
from pyiceberg.table.snapshots import Operation
from pyiceberg.table.upsert_util import create_match_filter
from pyiceberg.types import IntegerType, NestedField, StringType, StructType
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

    df = ctx.sql(
        f"""
        with t as (SELECT unnest(range({start_row},{end_row + 1})) as order_id)
        SELECT t.order_id {additional_columns}
            , date '2021-01-01' as order_date, 'A' as order_type
        from t
    """
    ).to_arrow_table()

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

    df = ctx.sql(
        """
        select 1 as order_id, date '2021-01-01' as order_date, 'A' as order_type
        union all
        select 2 as order_id, date '2021-01-01' as order_date, 'A' as order_type
    """
    ).to_arrow_table()

    table = catalog.create_table(identifier, df.schema)

    table.append(df)

    source_df = ctx.sql(
        """
        select 1 as order_id, date '2021-01-01' as order_date, 'A' as order_type
        union all
        select 2 as order_id, date '2021-01-01' as order_date, 'B' as order_type
        union all
        select 3 as order_id, date '2021-01-01' as order_date, 'A' as order_type
    """
    ).to_arrow_table()

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

    df = ctx.sql(
        """
        select date '2021-01-01' as order_date, 'A' as order_type
        union all
        select date '2021-01-02' as order_date, 'A' as order_type
    """
    ).to_arrow_table()

    table = catalog.create_table(identifier, df.schema)

    table.append(df)

    source_df = ctx.sql(
        """
        select date '2021-01-01' as order_date, 'A' as order_type
        union all
        select date '2021-01-02' as order_date, 'B' as order_type
        union all
        select date '2021-01-03' as order_date, 'A' as order_type
    """
    ).to_arrow_table()

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

    df = ctx.sql(
        """
        select 'abc' as order_id, 'A' as order_type
        union all
        select 'def' as order_id, 'A' as order_type
    """
    ).to_arrow_table()

    table = catalog.create_table(identifier, df.schema)

    table.append(df)

    source_df = ctx.sql(
        """
        select 'abc' as order_id, 'A' as order_type
        union all
        select 'def' as order_id, 'B' as order_type
        union all
        select 'ghi' as order_id, 'A' as order_type
    """
    ).to_arrow_table()

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


def test_coarse_match_filter_composite_key() -> None:
    """
    Test that create_coarse_match_filter produces efficient In() predicates for composite keys.
    """
    from pyiceberg.table.upsert_util import create_coarse_match_filter, create_match_filter
    from pyiceberg.expressions import Or, And, In

    # Create a table with composite key that has overlapping values
    # (1, 'x'), (2, 'y'), (1, 'z') - exact filter should have 3 conditions
    # coarse filter should have In(a, [1,2]) AND In(b, ['x','y','z'])
    data = [
        {"a": 1, "b": "x", "val": 1},
        {"a": 2, "b": "y", "val": 2},
        {"a": 1, "b": "z", "val": 3},
    ]
    schema = pa.schema([pa.field("a", pa.int32()), pa.field("b", pa.string()), pa.field("val", pa.int32())])
    table = pa.Table.from_pylist(data, schema=schema)

    exact_filter = create_match_filter(table, ["a", "b"])
    coarse_filter = create_coarse_match_filter(table, ["a", "b"])

    # Exact filter is an Or of And conditions
    assert isinstance(exact_filter, Or)

    # Coarse filter is an And of In conditions
    assert isinstance(coarse_filter, And)
    assert "In" in str(coarse_filter)


def test_vectorized_comparison_primitives() -> None:
    """Test vectorized comparison with primitive types."""
    from pyiceberg.table.upsert_util import _compare_columns_vectorized

    # Test integers
    source = pa.array([1, 2, 3, 4])
    target = pa.array([1, 2, 5, 4])
    diff = _compare_columns_vectorized(source, target)
    assert diff.to_pylist() == [False, False, True, False]

    # Test strings
    source = pa.array(["a", "b", "c"])
    target = pa.array(["a", "x", "c"])
    diff = _compare_columns_vectorized(source, target)
    assert diff.to_pylist() == [False, True, False]

    # Test floats
    source = pa.array([1.0, 2.5, 3.0])
    target = pa.array([1.0, 2.5, 3.1])
    diff = _compare_columns_vectorized(source, target)
    assert diff.to_pylist() == [False, False, True]


def test_vectorized_comparison_nulls() -> None:
    """Test vectorized comparison handles nulls correctly."""
    from pyiceberg.table.upsert_util import _compare_columns_vectorized

    # null vs non-null = different
    source = pa.array([1, None, 3])
    target = pa.array([1, 2, 3])
    diff = _compare_columns_vectorized(source, target)
    assert diff.to_pylist() == [False, True, False]

    # non-null vs null = different
    source = pa.array([1, 2, 3])
    target = pa.array([1, None, 3])
    diff = _compare_columns_vectorized(source, target)
    assert diff.to_pylist() == [False, True, False]

    # null vs null = same (no update needed)
    source = pa.array([1, None, 3])
    target = pa.array([1, None, 3])
    diff = _compare_columns_vectorized(source, target)
    assert diff.to_pylist() == [False, False, False]


def test_vectorized_comparison_structs() -> None:
    """Test vectorized comparison with nested struct types."""
    from pyiceberg.table.upsert_util import _compare_columns_vectorized

    struct_type = pa.struct([("x", pa.int32()), ("y", pa.string())])

    # Same structs
    source = pa.array([{"x": 1, "y": "a"}, {"x": 2, "y": "b"}], type=struct_type)
    target = pa.array([{"x": 1, "y": "a"}, {"x": 2, "y": "b"}], type=struct_type)
    diff = _compare_columns_vectorized(source, target)
    assert diff.to_pylist() == [False, False]

    # Different struct values
    source = pa.array([{"x": 1, "y": "a"}, {"x": 2, "y": "b"}], type=struct_type)
    target = pa.array([{"x": 1, "y": "a"}, {"x": 2, "y": "c"}], type=struct_type)
    diff = _compare_columns_vectorized(source, target)
    assert diff.to_pylist() == [False, True]


def test_vectorized_comparison_nested_structs() -> None:
    """Test vectorized comparison with deeply nested struct types."""
    from pyiceberg.table.upsert_util import _compare_columns_vectorized

    inner_struct = pa.struct([("val", pa.int32())])
    outer_struct = pa.struct([("inner", inner_struct), ("name", pa.string())])

    source = pa.array(
        [{"inner": {"val": 1}, "name": "a"}, {"inner": {"val": 2}, "name": "b"}],
        type=outer_struct,
    )
    target = pa.array(
        [{"inner": {"val": 1}, "name": "a"}, {"inner": {"val": 3}, "name": "b"}],
        type=outer_struct,
    )
    diff = _compare_columns_vectorized(source, target)
    assert diff.to_pylist() == [False, True]


def test_vectorized_comparison_lists() -> None:
    """Test vectorized comparison with list types (falls back to Python comparison)."""
    from pyiceberg.table.upsert_util import _compare_columns_vectorized

    list_type = pa.list_(pa.int32())

    source = pa.array([[1, 2], [3, 4]], type=list_type)
    target = pa.array([[1, 2], [3, 5]], type=list_type)
    diff = _compare_columns_vectorized(source, target)
    assert diff.to_pylist() == [False, True]


def test_get_rows_to_update_no_non_key_cols() -> None:
    """Test get_rows_to_update when all columns are key columns."""
    from pyiceberg.table.upsert_util import get_rows_to_update

    # All columns are key columns, so no non-key columns to compare
    source = pa.Table.from_pydict({"id": [1, 2, 3]})
    target = pa.Table.from_pydict({"id": [1, 2, 3]})
    rows = get_rows_to_update(source, target, ["id"])
    assert len(rows) == 0


def test_upsert_with_list_field(catalog: Catalog) -> None:
    """Test upsert with list type as non-key column."""
    from pyiceberg.types import ListType

    identifier = "default.test_upsert_with_list_field"
    _drop_table(catalog, identifier)

    schema = Schema(
        NestedField(1, "id", IntegerType(), required=True),
        NestedField(
            2,
            "tags",
            ListType(element_id=3, element_type=StringType(), element_required=False),
            required=False,
        ),
        identifier_field_ids=[1],
    )

    tbl = catalog.create_table(identifier, schema=schema)

    arrow_schema = pa.schema(
        [
            pa.field("id", pa.int32(), nullable=False),
            pa.field("tags", pa.list_(pa.large_string()), nullable=True),
        ]
    )

    initial_data = pa.Table.from_pylist(
        [
            {"id": 1, "tags": ["a", "b"]},
            {"id": 2, "tags": ["c"]},
        ],
        schema=arrow_schema,
    )
    tbl.append(initial_data)

    # Update with changed list
    update_data = pa.Table.from_pylist(
        [
            {"id": 1, "tags": ["a", "b"]},  # Same - no update
            {"id": 2, "tags": ["c", "d"]},  # Changed - should update
            {"id": 3, "tags": ["e"]},  # New - should insert
        ],
        schema=arrow_schema,
    )

    res = tbl.upsert(update_data, join_cols=["id"])
    assert res.rows_updated == 1
    assert res.rows_inserted == 1


def test_vectorized_comparison_struct_level_nulls() -> None:
    """Test vectorized comparison handles struct-level nulls correctly (not just field-level nulls)."""
    from pyiceberg.table.upsert_util import _compare_columns_vectorized

    struct_type = pa.struct([("x", pa.int32()), ("y", pa.string())])

    # null struct vs non-null struct = different
    source = pa.array([{"x": 1, "y": "a"}, None, {"x": 3, "y": "c"}], type=struct_type)
    target = pa.array([{"x": 1, "y": "a"}, {"x": 2, "y": "b"}, {"x": 3, "y": "c"}], type=struct_type)
    diff = _compare_columns_vectorized(source, target)
    assert diff.to_pylist() == [False, True, False]

    # non-null struct vs null struct = different
    source = pa.array([{"x": 1, "y": "a"}, {"x": 2, "y": "b"}, {"x": 3, "y": "c"}], type=struct_type)
    target = pa.array([{"x": 1, "y": "a"}, None, {"x": 3, "y": "c"}], type=struct_type)
    diff = _compare_columns_vectorized(source, target)
    assert diff.to_pylist() == [False, True, False]

    # null struct vs null struct = same (no update needed)
    source = pa.array([{"x": 1, "y": "a"}, None, {"x": 3, "y": "c"}], type=struct_type)
    target = pa.array([{"x": 1, "y": "a"}, None, {"x": 3, "y": "c"}], type=struct_type)
    diff = _compare_columns_vectorized(source, target)
    assert diff.to_pylist() == [False, False, False]


def test_vectorized_comparison_empty_struct_with_nulls() -> None:
    """Test that empty structs with null values are compared correctly."""
    from pyiceberg.table.upsert_util import _compare_columns_vectorized

    # Empty struct type - edge case where only struct-level null handling matters
    empty_struct_type = pa.struct([])

    # null vs non-null empty struct = different
    source = pa.array([{}, None, {}], type=empty_struct_type)
    target = pa.array([{}, {}, {}], type=empty_struct_type)
    diff = _compare_columns_vectorized(source, target)
    assert diff.to_pylist() == [False, True, False]

    # null vs null empty struct = same
    source = pa.array([None, None], type=empty_struct_type)
    target = pa.array([None, None], type=empty_struct_type)
    diff = _compare_columns_vectorized(source, target)
    assert diff.to_pylist() == [False, False]


# ============================================================================
# Tests for create_coarse_match_filter and _is_numeric_type
# ============================================================================


@pytest.mark.parametrize(
    "dtype,expected_numeric",
    [
        (pa.int8(), True),
        (pa.int16(), True),
        (pa.int32(), True),
        (pa.int64(), True),
        (pa.uint8(), True),
        (pa.uint16(), True),
        (pa.uint32(), True),
        (pa.uint64(), True),
        (pa.float16(), True),
        (pa.float32(), True),
        (pa.float64(), True),
        (pa.string(), False),
        (pa.binary(), False),
        (pa.date32(), False),
        (pa.date64(), False),
        (pa.timestamp("us"), False),
        (pa.timestamp("ns"), False),
        (pa.decimal128(10, 2), False),
        (pa.decimal256(20, 4), False),
        (pa.bool_(), False),
        (pa.large_string(), False),
        (pa.large_binary(), False),
    ],
)
def test_is_numeric_type(dtype: pa.DataType, expected_numeric: bool) -> None:
    """Test that _is_numeric_type correctly identifies all numeric types."""
    from pyiceberg.table.upsert_util import _is_numeric_type

    assert _is_numeric_type(dtype) == expected_numeric


# ============================================================================
# Thresholding Tests (Small vs Large Datasets)
# ============================================================================


def test_coarse_match_filter_small_dataset_uses_in_filter() -> None:
    """Test that small datasets (< 10,000 unique keys) use In() filter."""
    from pyiceberg.expressions import In

    from pyiceberg.table.upsert_util import LARGE_FILTER_THRESHOLD, create_coarse_match_filter

    # Create a dataset with 100 unique keys (well below threshold)
    num_keys = 100
    data = {"id": list(range(num_keys)), "value": list(range(num_keys))}
    schema = pa.schema([pa.field("id", pa.int64()), pa.field("value", pa.int64())])
    table = pa.Table.from_pydict(data, schema=schema)

    result = create_coarse_match_filter(table, ["id"])

    assert num_keys < LARGE_FILTER_THRESHOLD
    assert isinstance(result, In)
    assert result.term.name == "id"
    assert len(result.literals) == num_keys


def test_coarse_match_filter_threshold_boundary_uses_in_filter() -> None:
    """Test that datasets at threshold - 1 (9,999 unique keys) still use In() filter."""
    from pyiceberg.expressions import In

    from pyiceberg.table.upsert_util import LARGE_FILTER_THRESHOLD, create_coarse_match_filter

    # Create a dataset with exactly threshold - 1 unique keys
    num_keys = LARGE_FILTER_THRESHOLD - 1
    data = {"id": list(range(num_keys)), "value": list(range(num_keys))}
    schema = pa.schema([pa.field("id", pa.int64()), pa.field("value", pa.int64())])
    table = pa.Table.from_pydict(data, schema=schema)

    result = create_coarse_match_filter(table, ["id"])

    assert isinstance(result, In)
    assert result.term.name == "id"
    assert len(result.literals) == num_keys


def test_coarse_match_filter_above_threshold_uses_optimized_filter() -> None:
    """Test that datasets >= 10,000 unique keys use optimized filter strategy."""
    from pyiceberg.expressions import GreaterThanOrEqual, LessThanOrEqual

    from pyiceberg.table.upsert_util import LARGE_FILTER_THRESHOLD, create_coarse_match_filter

    # Create a dense dataset (consecutive IDs) with exactly threshold unique keys
    num_keys = LARGE_FILTER_THRESHOLD
    data = {"id": list(range(num_keys)), "value": list(range(num_keys))}
    schema = pa.schema([pa.field("id", pa.int64()), pa.field("value", pa.int64())])
    table = pa.Table.from_pydict(data, schema=schema)

    result = create_coarse_match_filter(table, ["id"])

    # Dense IDs should use range filter (And of GreaterThanOrEqual and LessThanOrEqual)
    assert isinstance(result, And)
    assert isinstance(result.left, GreaterThanOrEqual)
    assert isinstance(result.right, LessThanOrEqual)
    assert result.left.literal.value == 0
    assert result.right.literal.value == num_keys - 1


def test_coarse_match_filter_large_dataset() -> None:
    """Test that large datasets (100,000 unique keys) use optimized filter."""
    from pyiceberg.expressions import GreaterThanOrEqual, LessThanOrEqual

    from pyiceberg.table.upsert_util import LARGE_FILTER_THRESHOLD, create_coarse_match_filter

    # Create a dense dataset with 100,000 unique keys
    num_keys = 100_000
    data = {"id": list(range(num_keys)), "value": list(range(num_keys))}
    schema = pa.schema([pa.field("id", pa.int64()), pa.field("value", pa.int64())])
    table = pa.Table.from_pydict(data, schema=schema)

    result = create_coarse_match_filter(table, ["id"])

    assert num_keys >= LARGE_FILTER_THRESHOLD
    # Dense IDs should use range filter
    assert isinstance(result, And)
    assert isinstance(result.left, GreaterThanOrEqual)
    assert isinstance(result.right, LessThanOrEqual)


# ============================================================================
# Density Calculation Tests
# ============================================================================


def test_coarse_match_filter_dense_ids_use_range_filter() -> None:
    """Test that dense IDs (density > 10%) use range filter."""
    from pyiceberg.expressions import GreaterThanOrEqual, LessThanOrEqual

    from pyiceberg.table.upsert_util import LARGE_FILTER_THRESHOLD, create_coarse_match_filter

    # Create dense IDs: all values from 0 to N-1 (100% density)
    num_keys = LARGE_FILTER_THRESHOLD
    data = {"id": list(range(num_keys)), "value": list(range(num_keys))}
    schema = pa.schema([pa.field("id", pa.int64()), pa.field("value", pa.int64())])
    table = pa.Table.from_pydict(data, schema=schema)

    result = create_coarse_match_filter(table, ["id"])

    # Density = 10000 / (9999 - 0 + 1) = 100%
    # Should use range filter
    assert isinstance(result, And)
    assert isinstance(result.left, GreaterThanOrEqual)
    assert isinstance(result.right, LessThanOrEqual)


def test_coarse_match_filter_moderately_dense_ids_use_range_filter() -> None:
    """Test that moderately dense IDs (50% density) use range filter."""
    from pyiceberg.expressions import GreaterThanOrEqual, LessThanOrEqual

    from pyiceberg.table.upsert_util import LARGE_FILTER_THRESHOLD, create_coarse_match_filter

    # Create IDs: 0, 2, 4, 6, ... (every other number) - 50% density
    num_keys = LARGE_FILTER_THRESHOLD
    data = {"id": list(range(0, num_keys * 2, 2)), "value": list(range(num_keys))}
    schema = pa.schema([pa.field("id", pa.int64()), pa.field("value", pa.int64())])
    table = pa.Table.from_pydict(data, schema=schema)

    result = create_coarse_match_filter(table, ["id"])

    # Density = 10000 / (19998 - 0 + 1) ~= 50%
    # Should use range filter since density > 10%
    assert isinstance(result, And)
    assert isinstance(result.left, GreaterThanOrEqual)
    assert isinstance(result.right, LessThanOrEqual)


def test_coarse_match_filter_sparse_ids_use_always_true() -> None:
    """Test that sparse IDs (density <= 10%) use AlwaysTrue (full scan)."""
    from pyiceberg.table.upsert_util import LARGE_FILTER_THRESHOLD, create_coarse_match_filter

    # Create sparse IDs: values spread across a large range
    # 10,000 values in range of ~110,000 = ~9% density
    num_keys = LARGE_FILTER_THRESHOLD
    ids = list(range(0, num_keys * 11, 11))  # 0, 11, 22, 33, ...
    data = {"id": ids, "value": list(range(num_keys))}
    schema = pa.schema([pa.field("id", pa.int64()), pa.field("value", pa.int64())])
    table = pa.Table.from_pydict(data, schema=schema)

    result = create_coarse_match_filter(table, ["id"])

    # Density ~= 10000 / ((10000-1)*11 + 1) = 9.09% < 10%
    # Should use AlwaysTrue (full scan)
    assert isinstance(result, AlwaysTrue)


def test_coarse_match_filter_density_boundary_at_10_percent() -> None:
    """Test exact 10% boundary density behavior."""
    from pyiceberg.expressions import GreaterThanOrEqual, LessThanOrEqual

    from pyiceberg.table.upsert_util import LARGE_FILTER_THRESHOLD, create_coarse_match_filter

    # Create IDs at exactly ~10% density
    # 10,000 values in range of 100,000 = exactly 10%
    num_keys = LARGE_FILTER_THRESHOLD
    # Generate 10,000 values in range [0, 99999] -> density = 10000/100000 = 10%
    # Using every 10th value: 0, 10, 20, ... 99990
    ids = list(range(0, num_keys * 10, 10))
    data = {"id": ids, "value": list(range(num_keys))}
    schema = pa.schema([pa.field("id", pa.int64()), pa.field("value", pa.int64())])
    table = pa.Table.from_pydict(data, schema=schema)

    result = create_coarse_match_filter(table, ["id"])

    # Density = 10000 / ((num_keys-1)*10 + 1) = 10000 / 99991 ~= 10.001%
    # Should use range filter since density > 10% (just barely)
    assert isinstance(result, And)
    assert isinstance(result.left, GreaterThanOrEqual)
    assert isinstance(result.right, LessThanOrEqual)


def test_coarse_match_filter_very_sparse_ids() -> None:
    """Test that very sparse IDs (e.g., 1, 1M, 2M) use AlwaysTrue."""
    from pyiceberg.table.upsert_util import LARGE_FILTER_THRESHOLD, create_coarse_match_filter

    # Create extremely sparse IDs
    num_keys = LARGE_FILTER_THRESHOLD
    # Values from 0 to (num_keys-1) * 1000, stepping by 1000
    ids = list(range(0, num_keys * 1000, 1000))
    data = {"id": ids, "value": list(range(num_keys))}
    schema = pa.schema([pa.field("id", pa.int64()), pa.field("value", pa.int64())])
    table = pa.Table.from_pydict(data, schema=schema)

    result = create_coarse_match_filter(table, ["id"])

    # Density = 10000 / ((10000-1)*1000 + 1) ~= 0.1%
    # Should use AlwaysTrue
    assert isinstance(result, AlwaysTrue)


# ============================================================================
# Edge Cases
# ============================================================================


def test_coarse_match_filter_empty_dataset_returns_always_false() -> None:
    """Test that empty dataset returns AlwaysFalse."""
    from pyiceberg.expressions import AlwaysFalse

    from pyiceberg.table.upsert_util import create_coarse_match_filter

    schema = pa.schema([pa.field("id", pa.int64()), pa.field("value", pa.int64())])
    table = pa.Table.from_pydict({"id": [], "value": []}, schema=schema)

    result = create_coarse_match_filter(table, ["id"])

    assert isinstance(result, AlwaysFalse)


def test_coarse_match_filter_single_value_dataset() -> None:
    """Test that single value dataset uses In() or EqualTo() with single value."""
    from pyiceberg.expressions import In

    from pyiceberg.table.upsert_util import create_coarse_match_filter

    schema = pa.schema([pa.field("id", pa.int64()), pa.field("value", pa.int64())])
    table = pa.Table.from_pydict({"id": [42], "value": [1]}, schema=schema)

    result = create_coarse_match_filter(table, ["id"])

    # PyIceberg may optimize In() with a single value to EqualTo()
    if isinstance(result, In):
        assert result.term.name == "id"
        assert len(result.literals) == 1
        assert result.literals[0].value == 42
    elif isinstance(result, EqualTo):
        assert result.term.name == "id"
        assert result.literal.value == 42
    else:
        pytest.fail(f"Expected In or EqualTo, got {type(result)}")


def test_coarse_match_filter_negative_numbers_range() -> None:
    """Test that negative number IDs produce correct min/max range."""
    from pyiceberg.expressions import GreaterThanOrEqual, LessThanOrEqual

    from pyiceberg.table.upsert_util import LARGE_FILTER_THRESHOLD, create_coarse_match_filter

    # Create dense negative IDs: -10000 to -1
    num_keys = LARGE_FILTER_THRESHOLD
    ids = list(range(-num_keys, 0))
    data = {"id": ids, "value": list(range(num_keys))}
    schema = pa.schema([pa.field("id", pa.int64()), pa.field("value", pa.int64())])
    table = pa.Table.from_pydict(data, schema=schema)

    result = create_coarse_match_filter(table, ["id"])

    # Should use range filter with negative values
    assert isinstance(result, And)
    assert isinstance(result.left, GreaterThanOrEqual)
    assert isinstance(result.right, LessThanOrEqual)
    assert result.left.literal.value == -num_keys  # min
    assert result.right.literal.value == -1  # max


def test_coarse_match_filter_mixed_sign_numbers_range() -> None:
    """Test that mixed sign IDs (-500 to 500) produce correct range spanning zero."""
    from pyiceberg.expressions import GreaterThanOrEqual, LessThanOrEqual

    from pyiceberg.table.upsert_util import LARGE_FILTER_THRESHOLD, create_coarse_match_filter

    # Create IDs spanning zero: -5000 to 4999
    num_keys = LARGE_FILTER_THRESHOLD
    ids = list(range(-num_keys // 2, num_keys // 2))
    data = {"id": ids, "value": list(range(num_keys))}
    schema = pa.schema([pa.field("id", pa.int64()), pa.field("value", pa.int64())])
    table = pa.Table.from_pydict(data, schema=schema)

    result = create_coarse_match_filter(table, ["id"])

    # Should use range filter spanning zero
    assert isinstance(result, And)
    assert isinstance(result.left, GreaterThanOrEqual)
    assert isinstance(result.right, LessThanOrEqual)
    assert result.left.literal.value == -num_keys // 2  # min
    assert result.right.literal.value == num_keys // 2 - 1  # max


def test_coarse_match_filter_float_range_filter() -> None:
    """Test that float IDs use range filter correctly."""
    from pyiceberg.expressions import GreaterThanOrEqual, LessThanOrEqual

    from pyiceberg.table.upsert_util import LARGE_FILTER_THRESHOLD, create_coarse_match_filter

    # Create dense float IDs
    num_keys = LARGE_FILTER_THRESHOLD
    ids = [float(i) for i in range(num_keys)]
    data = {"id": ids, "value": list(range(num_keys))}
    schema = pa.schema([pa.field("id", pa.float64()), pa.field("value", pa.int64())])
    table = pa.Table.from_pydict(data, schema=schema)

    result = create_coarse_match_filter(table, ["id"])

    # Should use range filter for float column
    assert isinstance(result, And)
    assert isinstance(result.left, GreaterThanOrEqual)
    assert isinstance(result.right, LessThanOrEqual)
    assert result.left.literal.value == 0.0
    assert result.right.literal.value == float(num_keys - 1)


def test_coarse_match_filter_non_numeric_column_skips_range_filter() -> None:
    """Test that non-numeric column with >10k values returns AlwaysTrue."""
    from pyiceberg.table.upsert_util import LARGE_FILTER_THRESHOLD, create_coarse_match_filter

    # Create string IDs (non-numeric) with many unique values
    num_keys = LARGE_FILTER_THRESHOLD
    ids = [f"id_{i:05d}" for i in range(num_keys)]
    data = {"id": ids, "value": list(range(num_keys))}
    schema = pa.schema([pa.field("id", pa.string()), pa.field("value", pa.int64())])
    table = pa.Table.from_pydict(data, schema=schema)

    result = create_coarse_match_filter(table, ["id"])

    # Non-numeric column with large dataset should use AlwaysTrue
    assert isinstance(result, AlwaysTrue)


# ============================================================================
# Composite Key Tests
# ============================================================================


def test_coarse_match_filter_composite_key_small_dataset() -> None:
    """Test that composite key with small dataset uses And(In(), In())."""
    from pyiceberg.expressions import In

    from pyiceberg.table.upsert_util import create_coarse_match_filter

    # Create a small dataset with composite key
    data = {
        "a": [1, 2, 3, 1, 2, 3],
        "b": ["x", "x", "x", "y", "y", "y"],
        "value": [1, 2, 3, 4, 5, 6],
    }
    schema = pa.schema([pa.field("a", pa.int64()), pa.field("b", pa.string()), pa.field("value", pa.int64())])
    table = pa.Table.from_pydict(data, schema=schema)

    result = create_coarse_match_filter(table, ["a", "b"])

    # Should be And(In(a), In(b))
    assert isinstance(result, And)
    # Check that both children are In() filters
    assert "In" in str(result)


def test_coarse_match_filter_composite_key_large_numeric_column() -> None:
    """Test composite key where one column has >10k unique numeric values."""
    from pyiceberg.expressions import GreaterThanOrEqual, In, LessThanOrEqual

    from pyiceberg.table.upsert_util import LARGE_FILTER_THRESHOLD, create_coarse_match_filter

    # Create dataset with one large dense numeric column and one small column
    num_keys = LARGE_FILTER_THRESHOLD
    data = {
        "a": list(range(num_keys)),  # 10k unique dense values
        "b": ["category_1"] * (num_keys // 2) + ["category_2"] * (num_keys // 2),  # 2 unique values
        "value": list(range(num_keys)),
    }
    schema = pa.schema([pa.field("a", pa.int64()), pa.field("b", pa.string()), pa.field("value", pa.int64())])
    table = pa.Table.from_pydict(data, schema=schema)

    result = create_coarse_match_filter(table, ["a", "b"])

    # Should be And of filters for both columns
    assert isinstance(result, And)
    # Column 'a' (large, dense, numeric) should use range filter
    # Column 'b' (small) should use In()
    result_str = str(result)
    assert "GreaterThanOrEqual" in result_str or "In" in result_str


def test_coarse_match_filter_composite_key_mixed_types() -> None:
    """Test composite key with mixed numeric and string columns with large dataset."""
    from pyiceberg.expressions import In

    from pyiceberg.table.upsert_util import LARGE_FILTER_THRESHOLD, create_coarse_match_filter

    # Create dataset with large sparse numeric column and large string column
    num_keys = LARGE_FILTER_THRESHOLD
    # Sparse numeric IDs
    ids = list(range(0, num_keys * 100, 100))
    # Many unique strings
    strings = [f"str_{i}" for i in range(num_keys)]
    data = {
        "numeric_id": ids,
        "string_id": strings,
        "value": list(range(num_keys)),
    }
    schema = pa.schema([pa.field("numeric_id", pa.int64()), pa.field("string_id", pa.string()), pa.field("value", pa.int64())])
    table = pa.Table.from_pydict(data, schema=schema)

    result = create_coarse_match_filter(table, ["numeric_id", "string_id"])

    # Both columns have large unique values
    # numeric_id is sparse (density < 10%), so should use In()
    # string_id is non-numeric, so should use In()
    assert isinstance(result, And)
