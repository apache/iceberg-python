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
from typing import Tuple

import pyarrow as pa
import pytest
from pyspark.sql import SparkSession

from pyiceberg.catalog import Catalog
from pyiceberg.table.sorting import NullOrder, SortDirection, SortField, SortOrder
from pyiceberg.transforms import IdentityTransform
from utils import TABLE_SCHEMA, _create_table


@pytest.mark.integration
@pytest.mark.parametrize(
    "sort_col", ["int", "bool", "string", "string_long", "long", "float", "double", "date", "timestamp", "timestamptz", "binary"]
)
@pytest.mark.parametrize("sort_direction", [SortDirection.ASC, SortDirection.DESC])
@pytest.mark.parametrize("sort_null_ordering", [NullOrder.NULLS_FIRST, NullOrder.NULLS_LAST])
@pytest.mark.parametrize("format_version", [1, 2])
def test_query_null_sort(
    session_catalog: Catalog,
    spark: SparkSession,
    arrow_table_with_null: pa.Table,
    sort_col: str,
    sort_direction: SortDirection,
    sort_null_ordering: NullOrder,
    format_version: int,
) -> None:
    # Given
    unsorted_table_identifier = f"default.arrow_table_v{format_version}_with_null_unsorted_on_col_{sort_col}_in_direction_{sort_direction}_with_null_ordering_{str(sort_null_ordering).replace(' ', '_')}"
    sorted_table_identifier = f"default.arrow_table_v{format_version}_with_null_sorted_on_col_{sort_col}_in_direction_{sort_direction}_with_null_ordering_{str(sort_null_ordering).replace(' ', '_')}"
    nested_field = TABLE_SCHEMA.find_field(sort_col)

    unsorted_tbl = _create_table(
        session_catalog=session_catalog,
        identifier=unsorted_table_identifier,
        properties={"format-version": str(format_version)},
        data=[arrow_table_with_null],
    )

    # Collect the current data in a pandas dataframe.
    query_sorted_df = spark.sql(
        f"SELECT * FROM {unsorted_table_identifier} ORDER BY {sort_col} {sort_direction} {sort_null_ordering}"
    ).toPandas()

    # Update the sort order of the table and append
    sort_order = SortOrder(
        SortField(
            source_id=nested_field.field_id,
            direction=sort_direction,
            transform=IdentityTransform(),
            null_order=sort_null_ordering,
        )
    )

    sorted_tbl = _create_table(
        session_catalog=session_catalog,
        identifier=sorted_table_identifier,
        properties={"format-version": str(format_version)},
        data=[arrow_table_with_null],
        sort_order=sort_order,
    )

    sorted_df = spark.table(sorted_table_identifier).toPandas()

    assert len(unsorted_tbl.metadata.sort_orders) == 1, f"Expected no sort order for {unsorted_table_identifier}"
    assert sorted_tbl.format_version == format_version, f"Expected v{format_version}, got: v{sorted_tbl.format_version}"
    assert sorted_df.shape[0] == 3, f"Expected 3 total rows for {sorted_table_identifier}"
    assert sorted_df.equals(
        query_sorted_df
    ), f"Expected sorted dataframe for v{format_version}  on col: {sort_col} in direction {sort_direction} with null ordering as {sort_null_ordering}, got {sorted_df[sort_col]}"


@pytest.mark.integration
@pytest.mark.parametrize(
    "sort_col_tuple_3", [("int", "bool", "string"), ("long", "float", "double"), ("date", "timestamp", "timestamptz")]
)
@pytest.mark.parametrize("sort_direction_tuple_3", [(SortDirection.ASC, SortDirection.DESC, SortDirection.DESC)])
@pytest.mark.parametrize("sort_null_ordering_tuple_3", [(NullOrder.NULLS_FIRST, NullOrder.NULLS_FIRST, NullOrder.NULLS_LAST),(NullOrder.NULLS_FIRST, NullOrder.NULLS_FIRST, NullOrder.NULLS_FIRST)])
@pytest.mark.parametrize("format_version", [1, 2])
def test_query_null_multi_sort(
    session_catalog: Catalog,
    spark: SparkSession,
    arrow_table_with_null: pa.Table,
    sort_col_tuple_3: Tuple[str, str, str],
    sort_direction_tuple_3: Tuple[SortDirection, SortDirection, SortDirection],
    sort_null_ordering_tuple_3: Tuple[NullOrder, NullOrder, NullOrder],
    format_version: int,
) -> None:
    # Given
    unsorted_table_identifier = (
        f"default.arrow_table_v{format_version}_with_null_multi_unsorted_on_cols_{'_'.join(sort_col_tuple_3)}"
    )
    sorted_table_identifier = f"default.arrow_table_v{format_version}_with_null_multi_sorted_on_cols_{'_'.join(sort_col_tuple_3)}"

    unsorted_tbl = _create_table(
        session_catalog=session_catalog,
        identifier=unsorted_table_identifier,
        properties={"format-version": str(format_version)},
        data=[arrow_table_with_null],
    )

    sort_options_list = list(zip(sort_col_tuple_3, sort_direction_tuple_3, sort_null_ordering_tuple_3))

    # Collect the current data in a pandas dataframe.
    query_sorted_df = spark.sql(
        f"SELECT * FROM {unsorted_table_identifier} ORDER BY {','.join([f' {sort_col} {sort_direction} {sort_null_ordering}' for sort_col, sort_direction, sort_null_ordering in sort_options_list])}"
    ).toPandas()

    # Update the sort order of the table and append
    sort_order = SortOrder(*[
        SortField(
            source_id=TABLE_SCHEMA.find_field(sort_col).field_id,
            direction=sort_direction,
            transform=IdentityTransform(),
            null_order=sort_null_ordering,
        )
        for sort_col, sort_direction, sort_null_ordering in sort_options_list
    ])

    sorted_tbl = _create_table(
        session_catalog=session_catalog,
        identifier=sorted_table_identifier,
        properties={"format-version": str(format_version)},
        data=[arrow_table_with_null],
        sort_order=sort_order,
    )

    sorted_df = spark.table(sorted_table_identifier).toPandas()

    assert len(unsorted_tbl.metadata.sort_orders) == 1, f"Expected no sort order for {unsorted_table_identifier}"
    assert sorted_tbl.format_version == format_version, f"Expected v{format_version}, got: v{sorted_tbl.format_version}"
    assert sorted_df.shape[0] == 3, f"Expected 3 total rows for {sorted_table_identifier}"
    assert sorted_df.equals(
        query_sorted_df
    ), f"Expected sorted dataframe for v{format_version}  on col: {sort_options_list}, got {sorted_df}"
