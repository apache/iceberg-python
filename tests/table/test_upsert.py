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
import datetime
from pathlib import PosixPath
from typing import Any

import pyarrow as pa
import pytest
from datafusion import SessionContext
from pyarrow import Table as pa_table

from pyiceberg.catalog import Catalog
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.expressions import AlwaysTrue, And, EqualTo, GreaterThanOrEqual, LessThanOrEqual, Reference
from pyiceberg.expressions.literals import LongLiteral
from pyiceberg.io.pyarrow import schema_to_pyarrow
from pyiceberg.partitioning import UNPARTITIONED_PARTITION_SPEC, PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table import Table, UpsertResult
from pyiceberg.table.snapshots import Operation
from pyiceberg.table.upsert_util import augment_filter_with_partition_ranges, create_match_filter
from pyiceberg.transforms import IdentityTransform
from pyiceberg.types import DateType, IntegerType, NestedField, StringType, StructType
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


# ---------------------------------------------------------------------------
# Partition-range augmentation for upsert row filters.
#
# ``Transaction.upsert`` builds its scan ``row_filter`` from ``join_cols``
# alone via ``create_match_filter``. When the partition spec sources from
# columns NOT in ``join_cols`` (a common pattern for append-only event logs
# partitioned by time but keyed by composite IDs), ``inclusive_projection``
# collapses the entire predicate to ``AlwaysTrue`` against the partition
# spec and ``DataScan.plan_files`` falls through to a full table scan.
#
# ``augment_filter_with_partition_ranges`` derives ``[min, max]`` predicates
# from ``df`` for every partition source column present in the frame and
# ANDs them into the row filter. Iceberg's inclusive projection then
# projects each range through the partition transform when planning the
# scan, enabling manifest- and file-level pruning.
#
# See related issues #2138, #2159, #3129.
# ---------------------------------------------------------------------------


class TestAugmentFilterWithPartitionRanges:
    """Pure-function tests for ``augment_filter_with_partition_ranges``.

    Asserts the structural shape of the augmented predicate. End-to-end
    file-pruning behaviour is exercised by the upsert integration tests
    below.
    """

    @staticmethod
    def _orders_schema() -> Schema:
        return Schema(
            NestedField(1, "order_id", IntegerType(), required=True),
            NestedField(2, "order_date", DateType(), required=True),
            NestedField(3, "order_type", StringType(), required=True),
        )

    @staticmethod
    def _orders_pa_schema() -> pa.Schema:
        return pa.schema(
            [
                pa.field("order_id", pa.int32(), nullable=False),
                pa.field("order_date", pa.date32(), nullable=False),
                pa.field("order_type", pa.string(), nullable=False),
            ]
        )

    def _df(self, rows: list[dict[str, object]]) -> pa.Table:
        return pa.Table.from_pylist(rows, schema=self._orders_pa_schema())

    def test_unpartitioned_spec_returns_input_unchanged(self) -> None:
        """Tables without a partition spec have nothing to project through.
        The augmentation must short-circuit and hand back the exact
        ``matched_predicate`` object — no allocation, no semantic change."""
        df = self._df([{"order_id": 1, "order_date": datetime.date(2026, 1, 1), "order_type": "A"}])
        matched = create_match_filter(df, ["order_id"])
        augmented = augment_filter_with_partition_ranges(
            matched_predicate=matched,
            df=df,
            schema=self._orders_schema(),
            spec=UNPARTITIONED_PARTITION_SPEC,
        )
        assert augmented is matched

    def test_partition_source_column_not_in_df_skipped(self) -> None:
        """Source frames that don't contain the partition source column
        can't contribute a bound — the augmentation has to skip rather
        than guess. Returns ``matched_predicate`` unchanged so the
        existing scan behaviour applies."""
        df_no_date = pa.Table.from_pylist(
            [{"order_id": 1, "order_type": "A"}],
            schema=pa.schema(
                [
                    pa.field("order_id", pa.int32(), nullable=False),
                    pa.field("order_type", pa.string(), nullable=False),
                ]
            ),
        )
        matched = create_match_filter(df_no_date, ["order_id"])
        spec = PartitionSpec(PartitionField(source_id=2, field_id=1000, transform=IdentityTransform(), name="order_date"))
        augmented = augment_filter_with_partition_ranges(
            matched_predicate=matched,
            df=df_no_date,
            schema=self._orders_schema(),
            spec=spec,
        )
        assert augmented == matched

    def test_partition_source_column_all_nulls_skipped(self) -> None:
        """When every value of the partition source column in ``df`` is
        null, there is no meaningful ``min`` / ``max`` to bound the
        predicate. Skip rather than emit a vacuous augmentation."""
        df = pa.Table.from_pylist(
            [{"order_id": 1, "order_date": None, "order_type": "A"}],
            schema=pa.schema(
                [
                    pa.field("order_id", pa.int32(), nullable=False),
                    pa.field("order_date", pa.date32(), nullable=True),
                    pa.field("order_type", pa.string(), nullable=False),
                ]
            ),
        )
        matched = create_match_filter(df, ["order_id"])
        spec = PartitionSpec(PartitionField(source_id=2, field_id=1000, transform=IdentityTransform(), name="order_date"))
        augmented = augment_filter_with_partition_ranges(
            matched_predicate=matched,
            df=df,
            schema=self._orders_schema(),
            spec=spec,
        )
        assert augmented == matched

    def test_partition_source_column_some_nulls_skipped(self) -> None:
        """Correctness guard: a partial-null source column cannot use a
        non-null ``GreaterThanOrEqual`` augmentation because destination
        rows whose partition value is NULL would be excluded from the
        match scan even though their ``(key)`` may collide with the
        null-partition source rows. Skip pruning over emitting an unsafe
        predicate."""
        df = pa.Table.from_pylist(
            [
                {"order_id": 1, "order_date": datetime.date(2026, 1, 1), "order_type": "A"},
                {"order_id": 2, "order_date": None, "order_type": "B"},
            ],
            schema=pa.schema(
                [
                    pa.field("order_id", pa.int32(), nullable=False),
                    pa.field("order_date", pa.date32(), nullable=True),
                    pa.field("order_type", pa.string(), nullable=False),
                ]
            ),
        )
        matched = create_match_filter(df, ["order_id"])
        spec = PartitionSpec(PartitionField(source_id=2, field_id=1000, transform=IdentityTransform(), name="order_date"))
        augmented = augment_filter_with_partition_ranges(
            matched_predicate=matched,
            df=df,
            schema=self._orders_schema(),
            spec=spec,
        )
        assert augmented == matched

    def test_single_value_partition_column_emits_equal_to(self) -> None:
        """``min == max`` collapses to a single ``EqualTo`` — tighter than
        the range pair and lets exact partition pruning fire (e.g. when
        every source row falls in the same hourly bucket)."""
        df = self._df(
            [
                {"order_id": 1, "order_date": datetime.date(2026, 1, 1), "order_type": "A"},
                {"order_id": 2, "order_date": datetime.date(2026, 1, 1), "order_type": "B"},
            ]
        )
        matched = create_match_filter(df, ["order_id"])
        spec = PartitionSpec(PartitionField(source_id=2, field_id=1000, transform=IdentityTransform(), name="order_date"))
        augmented = augment_filter_with_partition_ranges(
            matched_predicate=matched,
            df=df,
            schema=self._orders_schema(),
            spec=spec,
        )
        assert augmented == And(matched, EqualTo("order_date", datetime.date(2026, 1, 1)))

    def test_range_emits_gteq_and_lteq(self) -> None:
        """Multiple distinct values → ``GreaterThanOrEqual(min) AND
        LessThanOrEqual(max)`` pair, AND'd onto the original matched
        predicate. Inclusive_projection handles the partition-transform
        projection at scan time."""
        df = self._df(
            [
                {"order_id": 1, "order_date": datetime.date(2026, 1, 1), "order_type": "A"},
                {"order_id": 2, "order_date": datetime.date(2026, 1, 15), "order_type": "B"},
                {"order_id": 3, "order_date": datetime.date(2026, 2, 1), "order_type": "C"},
            ]
        )
        matched = create_match_filter(df, ["order_id"])
        spec = PartitionSpec(PartitionField(source_id=2, field_id=1000, transform=IdentityTransform(), name="order_date"))
        augmented = augment_filter_with_partition_ranges(
            matched_predicate=matched,
            df=df,
            schema=self._orders_schema(),
            spec=spec,
        )
        assert augmented == And(
            And(matched, GreaterThanOrEqual("order_date", datetime.date(2026, 1, 1))),
            LessThanOrEqual("order_date", datetime.date(2026, 2, 1)),
        )

    def test_multiple_partition_fields_share_source_id_emitted_once(self) -> None:
        """When two partition fields source from the same column (e.g.
        ``bucket(8, id), truncate(4, id)``), only one source-column range
        is emitted. ``inclusive_projection`` projects through each
        partition field independently at scan time, so a single source-
        range predicate suffices for both."""
        df = self._df(
            [
                {"order_id": 1, "order_date": datetime.date(2026, 1, 1), "order_type": "A"},
                {"order_id": 10, "order_date": datetime.date(2026, 1, 2), "order_type": "B"},
            ]
        )
        matched = create_match_filter(df, ["order_type"])

        from pyiceberg.transforms import BucketTransform, TruncateTransform

        spec = PartitionSpec(
            PartitionField(source_id=1, field_id=1000, transform=BucketTransform(8), name="order_id_bucket"),
            PartitionField(source_id=1, field_id=1001, transform=TruncateTransform(4), name="order_id_trunc"),
        )
        augmented = augment_filter_with_partition_ranges(
            matched_predicate=matched,
            df=df,
            schema=self._orders_schema(),
            spec=spec,
        )
        # Exactly one ``GreaterThanOrEqual`` + ``LessThanOrEqual`` pair on
        # ``order_id`` — not duplicated for each partition field.
        assert augmented == And(
            And(matched, GreaterThanOrEqual("order_id", 1)),
            LessThanOrEqual("order_id", 10),
        )


class TestUpsertPartitionPruningIntegration:
    """End-to-end upsert against partitioned tables.

    Verifies that the augmented row filter doesn't change upsert
    semantics — ``rows_updated`` / ``rows_inserted`` match the original
    behaviour — across the three structural cases:

    1. Partition source not in ``join_cols`` (the case the augmentation
       fires for; biggest perf gain).
    2. Partition source IS in ``join_cols`` (augmentation contributes
       redundantly but doesn't change correctness).
    3. Unpartitioned (augmentation is a no-op).
    """

    def test_upsert_correct_when_partition_col_not_in_join_cols(self, catalog: Catalog) -> None:
        """Source partitioned by ``order_date`` but keyed on ``order_id``.
        Augmentation fires — semantics must be identical to the
        unpartitioned baseline."""
        identifier = "default.test_upsert_partition_not_in_join_cols"
        _drop_table(catalog, identifier)

        schema = Schema(
            NestedField(1, "order_id", IntegerType(), required=True),
            NestedField(2, "order_date", DateType(), required=True),
            NestedField(3, "order_type", StringType(), required=True),
        )
        spec = PartitionSpec(PartitionField(source_id=2, field_id=1000, transform=IdentityTransform(), name="order_date"))
        table = catalog.create_table(identifier, schema=schema, partition_spec=spec)

        arrow_schema = pa.schema(
            [
                pa.field("order_id", pa.int32(), nullable=False),
                pa.field("order_date", pa.date32(), nullable=False),
                pa.field("order_type", pa.string(), nullable=False),
            ]
        )
        # Initial load: ids 1-5 across two different partitions.
        initial = pa.Table.from_pylist(
            [
                {"order_id": 1, "order_date": datetime.date(2026, 1, 1), "order_type": "A"},
                {"order_id": 2, "order_date": datetime.date(2026, 1, 1), "order_type": "A"},
                {"order_id": 3, "order_date": datetime.date(2026, 1, 2), "order_type": "A"},
                {"order_id": 4, "order_date": datetime.date(2026, 1, 2), "order_type": "A"},
                {"order_id": 5, "order_date": datetime.date(2026, 1, 2), "order_type": "A"},
            ],
            schema=arrow_schema,
        )
        table.append(initial)

        # Upsert: ids 3-7 (3 update existing, 2 are new), all in the
        # same partition the augmentation will prune to.
        upsert_df = pa.Table.from_pylist(
            [
                {"order_id": 3, "order_date": datetime.date(2026, 1, 2), "order_type": "B"},
                {"order_id": 4, "order_date": datetime.date(2026, 1, 2), "order_type": "B"},
                {"order_id": 5, "order_date": datetime.date(2026, 1, 2), "order_type": "B"},
                {"order_id": 6, "order_date": datetime.date(2026, 1, 2), "order_type": "B"},
                {"order_id": 7, "order_date": datetime.date(2026, 1, 2), "order_type": "B"},
            ],
            schema=arrow_schema,
        )

        res = table.upsert(df=upsert_df, join_cols=["order_id"])
        assert res.rows_updated == 3
        assert res.rows_inserted == 2

        # Sanity: the table now has 7 rows total (5 initial - 3 updated + 3 updated + 2 inserted == 7).
        final = table.scan().to_arrow()
        assert final.num_rows == 7
        assert set(final["order_id"].to_pylist()) == {1, 2, 3, 4, 5, 6, 7}

    def test_upsert_correct_when_partition_col_in_join_cols(self, catalog: Catalog) -> None:
        """Partition column IS one of the ``join_cols``. The augmentation
        adds a redundant ``order_date`` range to a predicate that already
        constrains ``order_date`` via ``create_match_filter`` — no
        semantic change should result."""
        identifier = "default.test_upsert_partition_in_join_cols"
        _drop_table(catalog, identifier)

        schema = Schema(
            NestedField(1, "order_id", IntegerType(), required=True),
            NestedField(2, "order_date", DateType(), required=True),
            NestedField(3, "order_type", StringType(), required=True),
        )
        spec = PartitionSpec(PartitionField(source_id=2, field_id=1000, transform=IdentityTransform(), name="order_date"))
        table = catalog.create_table(identifier, schema=schema, partition_spec=spec)

        arrow_schema = pa.schema(
            [
                pa.field("order_id", pa.int32(), nullable=False),
                pa.field("order_date", pa.date32(), nullable=False),
                pa.field("order_type", pa.string(), nullable=False),
            ]
        )
        initial = pa.Table.from_pylist(
            [
                {"order_id": 1, "order_date": datetime.date(2026, 1, 1), "order_type": "A"},
                {"order_id": 2, "order_date": datetime.date(2026, 1, 2), "order_type": "A"},
            ],
            schema=arrow_schema,
        )
        table.append(initial)

        upsert_df = pa.Table.from_pylist(
            [
                {"order_id": 1, "order_date": datetime.date(2026, 1, 1), "order_type": "B"},  # update
                {"order_id": 3, "order_date": datetime.date(2026, 1, 3), "order_type": "B"},  # insert
            ],
            schema=arrow_schema,
        )

        res = table.upsert(df=upsert_df, join_cols=["order_id", "order_date"])
        assert res.rows_updated == 1
        assert res.rows_inserted == 1

    def test_augmented_predicate_prunes_destination_files(self, catalog: Catalog) -> None:
        """Smoke test: ``DataScan.plan_files()`` returns strictly fewer
        files when the augmented predicate is used. This is the actual
        perf claim the optimization makes; the integration tests above
        verify semantics, this one verifies pruning happens.

        Realistic worst-case shape: the key column (``order_id``) has
        per-file lower/upper bounds that span the entire range of source
        keys (modelled by writing the same set of ``order_id`` values
        across every partition). This defeats ``_InclusiveMetricsEvaluator``
        which would otherwise prune via file-level parquet column stats —
        and is exactly the situation real workloads hit when keys are
        UUIDs or otherwise uniformly distributed across files
        independent of partition. With key-level metrics unable to
        prune, partition-spec projection on ``order_date`` is the only
        lever left, and the test pins the assertion that our
        augmentation activates it.
        """
        from pyiceberg.table import DataScan

        identifier = "default.test_augmented_predicate_prunes_files"
        _drop_table(catalog, identifier)

        schema = Schema(
            NestedField(1, "order_id", IntegerType(), required=True),
            NestedField(2, "order_date", DateType(), required=True),
            NestedField(3, "order_type", StringType(), required=True),
        )
        spec = PartitionSpec(PartitionField(source_id=2, field_id=1000, transform=IdentityTransform(), name="order_date"))
        table = catalog.create_table(identifier, schema=schema, partition_spec=spec)

        arrow_schema = pa.schema(
            [
                pa.field("order_id", pa.int32(), nullable=False),
                pa.field("order_date", pa.date32(), nullable=False),
                pa.field("order_type", pa.string(), nullable=False),
            ]
        )

        # Five separate appends, one per partition. Each appended file
        # contains the SAME set of ``order_id`` values {1..5} so per-file
        # parquet stats on ``order_id`` are identical across files. This
        # mirrors UUID-keyed workloads where per-file key bounds span
        # the full key space and metrics-level pruning is useless;
        # partition pruning is the only effective lever.
        partitions = [
            datetime.date(2026, 1, 1),
            datetime.date(2026, 1, 2),
            datetime.date(2026, 1, 3),
            datetime.date(2026, 1, 4),
            datetime.date(2026, 1, 5),
        ]
        order_ids_per_partition = [1, 2, 3, 4, 5]
        for d in partitions:
            table.append(
                pa.Table.from_pylist(
                    [{"order_id": oid, "order_date": d, "order_type": "A"} for oid in order_ids_per_partition],
                    schema=arrow_schema,
                )
            )

        # Setup invariant: one file per partition (5 total).
        all_files_scan = DataScan(table_metadata=table.metadata, io=table.io, row_filter=AlwaysTrue())
        total_files = len(list(all_files_scan.plan_files()))
        assert total_files == 5, f"setup invariant: expected 5 destination files, got {total_files}"

        # Source covers only 2 of the 5 partitions, both with order_id=2
        # (which appears in every destination file → file-level metrics
        # pruning on order_id can't help).
        src = pa.Table.from_pylist(
            [
                {"order_id": 2, "order_date": datetime.date(2026, 1, 2), "order_type": "B"},
                {"order_id": 99, "order_date": datetime.date(2026, 1, 3), "order_type": "B"},
            ],
            schema=arrow_schema,
        )

        # (a) Original behaviour: row_filter built from join_cols alone.
        # join_cols = ['order_id'] doesn't reference 'order_date', so
        # inclusive_projection collapses the partition projection to
        # AlwaysTrue. order_id=2 falls within [1, 5] in every file →
        # metrics evaluator can't prune either. All 5 files listed.
        original_predicate = create_match_filter(src, ["order_id"])
        original_files = len(
            list(DataScan(table_metadata=table.metadata, io=table.io, row_filter=original_predicate).plan_files())
        )

        # (b) Augmented predicate adds [min, max] on the partition source.
        # inclusive_projection projects through IdentityTransform; only
        # the 2 hourly partitions overlapping [2026-01-02, 2026-01-03]
        # are kept.
        augmented_predicate = augment_filter_with_partition_ranges(
            matched_predicate=original_predicate,
            df=src,
            schema=table.metadata.schema(),
            spec=table.metadata.spec(),
        )
        augmented_files = len(
            list(DataScan(table_metadata=table.metadata, io=table.io, row_filter=augmented_predicate).plan_files())
        )

        # The whole point of the optimization.
        assert original_files == 5, (
            f"original behaviour invariant: with per-file order_id bounds spanning the full source key set, "
            f"neither partition projection nor metrics evaluation can prune; expected 5 files, got {original_files}"
        )
        assert augmented_files == 2, (
            f"augmented predicate must prune to overlapping partitions only; "
            f"expected 2 files (2026-01-02, 2026-01-03), got {augmented_files}"
        )
        assert augmented_files < original_files

    def test_upsert_unpartitioned_unchanged(self, catalog: Catalog) -> None:
        """Sanity that the augmentation doesn't alter the unpartitioned
        path. Mirrors ``test_merge_rows`` but smaller — purely a
        regression guard against the augmentation accidentally tripping
        unpartitioned tables."""
        identifier = "default.test_upsert_unpartitioned_augmentation_noop"
        _drop_table(catalog, identifier)

        schema = Schema(
            NestedField(1, "order_id", IntegerType(), required=True),
            NestedField(2, "order_type", StringType(), required=True),
        )
        table = catalog.create_table(identifier, schema=schema)

        arrow_schema = pa.schema(
            [
                pa.field("order_id", pa.int32(), nullable=False),
                pa.field("order_type", pa.string(), nullable=False),
            ]
        )
        table.append(pa.Table.from_pylist([{"order_id": 1, "order_type": "A"}], schema=arrow_schema))

        upsert_df = pa.Table.from_pylist(
            [
                {"order_id": 1, "order_type": "B"},  # update
                {"order_id": 2, "order_type": "B"},  # insert
            ],
            schema=arrow_schema,
        )
        res = table.upsert(df=upsert_df, join_cols=["order_id"])
        assert res.rows_updated == 1
        assert res.rows_inserted == 1


class TestUpsertScanProjection:
    """``Transaction.upsert`` narrows the destination scan's
    ``selected_fields`` to ``join_cols`` when ``when_matched_update_all=False``.

    Rationale: the insert-on-no-match branch only reads ``join_cols``
    off each destination batch (to feed ``create_match_filter``); every
    other column is unused. Projection at the scan boundary lets the
    parquet reader prune wide non-key columns at the file level —
    significant for tables whose payload column (e.g. a JSON ``log``)
    dominates file bytes. ``_projected_field_ids`` auto-unions the
    row-filter's column ids back in, so the augmented ``created_at``
    range and the original join-key predicates still see the columns
    they need for filter evaluation.

    Falls back to ``("*",)`` when ``when_matched_update_all=True``
    because ``get_rows_to_update`` compares non-key columns to detect
    actual value changes.
    """

    @staticmethod
    def _build_partitioned_table(catalog: Catalog, identifier: str) -> Table:
        _drop_table(catalog, identifier)
        schema = Schema(
            NestedField(1, "order_id", IntegerType(), required=True),
            NestedField(2, "order_date", DateType(), required=True),
            NestedField(3, "order_type", StringType(), required=True),
        )
        spec = PartitionSpec(PartitionField(source_id=2, field_id=1000, transform=IdentityTransform(), name="order_date"))
        return catalog.create_table(identifier, schema=schema, partition_spec=spec)

    @staticmethod
    def _arrow_schema() -> pa.Schema:
        return pa.schema(
            [
                pa.field("order_id", pa.int32(), nullable=False),
                pa.field("order_date", pa.date32(), nullable=False),
                pa.field("order_type", pa.string(), nullable=False),
            ]
        )

    def _seed(self, table: Table) -> None:
        table.append(
            pa.Table.from_pylist(
                [
                    {"order_id": 1, "order_date": datetime.date(2026, 1, 1), "order_type": "A"},
                    {"order_id": 2, "order_date": datetime.date(2026, 1, 2), "order_type": "A"},
                ],
                schema=self._arrow_schema(),
            )
        )

    @pytest.fixture
    def captured_scans(self, monkeypatch: pytest.MonkeyPatch) -> list[dict[str, Any]]:
        """Spy on ``DataScan.__init__`` to capture every kwargs dict.

        Lets the tests pin which ``selected_fields`` the upsert path
        actually passes — assertions on the surfaced batch schema alone
        would miss the case where the underlying projection contract
        regresses but the test data happens to have only join_cols
        anyway.

        The spy preserves ``__init__``'s signature via
        :func:`functools.wraps` so ``DataScan.update()``'s reflective
        ``inspect.signature(type(self).__init__).parameters`` lookup
        (used by ``use_ref``) still resolves to the real parameter
        names, not the spy's ``**kwargs``.
        """
        import functools

        from pyiceberg.table import DataScan

        captured: list[dict[str, Any]] = []
        original_init = DataScan.__init__

        @functools.wraps(original_init)
        def _spy(self: DataScan, *args: Any, **kwargs: Any) -> None:
            captured.append(dict(kwargs))
            original_init(self, *args, **kwargs)

        monkeypatch.setattr(DataScan, "__init__", _spy)
        return captured

    def test_when_matched_false_projects_join_cols_only(self, catalog: Catalog, captured_scans: list[dict[str, Any]]) -> None:
        """The insert-on-no-match branch never reads non-key destination
        columns, so the scan must narrow the projection to ``join_cols``
        — saving the parquet reader from materialising wide payload
        columns just to be discarded."""
        table = self._build_partitioned_table(catalog, "default.test_upsert_projection_insert_only")
        self._seed(table)
        upsert_df = pa.Table.from_pylist(
            [
                {"order_id": 2, "order_date": datetime.date(2026, 1, 2), "order_type": "B"},
                {"order_id": 3, "order_date": datetime.date(2026, 1, 3), "order_type": "B"},
            ],
            schema=self._arrow_schema(),
        )

        # Snapshot only the scans constructed during the upsert (the
        # seed append above may have created its own).
        before = len(captured_scans)
        res = table.upsert(df=upsert_df, join_cols=["order_id"], when_matched_update_all=False)
        upsert_scans = captured_scans[before:]
        assert res.rows_inserted == 1
        assert res.rows_updated == 0

        # The upsert constructs one DataScan for the destination match.
        # ``use_ref`` may construct a second DataScan as an inherited
        # copy (via ``self.update``), which carries the same
        # ``selected_fields`` through. Pin both: at least one scan was
        # constructed during the upsert, and every scan that ran
        # carries the narrowed projection. ``_projected_field_ids``
        # auto-unions the row filter's column ids back in, so
        # ``order_date`` (added by the partition-range augmentation)
        # is still read for filter evaluation without us having to
        # list it explicitly.
        assert upsert_scans, "upsert path constructed no DataScan — projection contract regression"
        selected = [s.get("selected_fields") for s in upsert_scans]
        assert all(sf == ("order_id",) for sf in selected), (
            f"expected every DataScan during upsert to use selected_fields=('order_id',); got {selected}"
        )

    def test_when_matched_true_keeps_star_projection(self, catalog: Catalog, captured_scans: list[dict[str, Any]]) -> None:
        """The update branch's ``get_rows_to_update`` compares non-key
        columns to detect actual value changes — projecting only
        ``join_cols`` would feed it data with no non-key columns to
        compare and silently turn every match into a write-back. Must
        keep ``("*",)``."""
        table = self._build_partitioned_table(catalog, "default.test_upsert_projection_update_mode")
        self._seed(table)
        upsert_df = pa.Table.from_pylist(
            [
                {"order_id": 1, "order_date": datetime.date(2026, 1, 1), "order_type": "B"},
                {"order_id": 3, "order_date": datetime.date(2026, 1, 3), "order_type": "B"},
            ],
            schema=self._arrow_schema(),
        )

        before = len(captured_scans)
        res = table.upsert(df=upsert_df, join_cols=["order_id"], when_matched_update_all=True)
        upsert_scans = captured_scans[before:]
        assert res.rows_updated == 1
        assert res.rows_inserted == 1

        assert upsert_scans, "upsert path constructed no DataScan — projection contract regression"
        selected = [s.get("selected_fields") for s in upsert_scans]
        assert all(sf == ("*",) for sf in selected), (
            f"expected every DataScan during upsert to keep selected_fields=('*',) for the update branch; got {selected}"
        )

    def test_update_mode_actually_updates_non_key_columns(self, catalog: Catalog) -> None:
        """End-to-end correctness pin: with ``when_matched_update_all=True``
        the destination scan must read non-key columns so
        ``get_rows_to_update`` can detect ``order_type`` changes. A
        regression that narrows projection unconditionally would skip
        the comparison and silently miss updates whose non-key columns
        differ.
        """
        identifier = "default.test_upsert_update_mode_correctness"
        table = self._build_partitioned_table(catalog, identifier)
        self._seed(table)
        # Source has the same (order_id, order_date) as one destination
        # row but a different ``order_type``. Update path must detect
        # the non-key change and overwrite.
        upsert_df = pa.Table.from_pylist(
            [{"order_id": 2, "order_date": datetime.date(2026, 1, 2), "order_type": "CHANGED"}],
            schema=self._arrow_schema(),
        )
        res = table.upsert(df=upsert_df, join_cols=["order_id"], when_matched_update_all=True)
        assert res.rows_updated == 1
        assert res.rows_inserted == 0

        # Read back: the original 'A' must have been overwritten with 'CHANGED'.
        rows = {r["order_id"]: r for r in table.scan().to_arrow().to_pylist()}
        assert rows[2]["order_type"] == "CHANGED"
