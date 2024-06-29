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
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table.sorting import NullOrder, SortDirection, SortField, SortOrder
from pyiceberg.transforms import (
    BucketTransform,
    DayTransform,
    IdentityTransform,
    MonthTransform,
    TruncateTransform,
    YearTransform,
)
from utils import TABLE_SCHEMA, _create_table


@pytest.mark.integration
@pytest.mark.parametrize(
    "sort_col", ["int", "bool", "string", "string_long", "long", "float", "double", "date", "timestamp", "timestamptz", "binary"]
)
@pytest.mark.parametrize("sort_direction", [SortDirection.ASC, SortDirection.DESC])
@pytest.mark.parametrize("sort_null_ordering", [NullOrder.NULLS_FIRST, NullOrder.NULLS_LAST])
@pytest.mark.parametrize("format_version", [1, 2])
def test_query_null_append_sort(
    session_catalog: Catalog,
    spark: SparkSession,
    arrow_table_with_null: pa.Table,
    sort_col: str,
    sort_direction: SortDirection,
    sort_null_ordering: NullOrder,
    format_version: int,
) -> None:
    table_identifier = f"default.arrow_table_v{format_version}_with_null_sorted_on_col_{sort_col}_in_direction_{sort_direction}_with_null_ordering_{str(sort_null_ordering).replace(' ', '_')}"
    nested_field = TABLE_SCHEMA.find_field(sort_col)

    sort_order = SortOrder(
        SortField(
            source_id=nested_field.field_id,
            direction=sort_direction,
            transform=IdentityTransform(),
            null_order=sort_null_ordering,
        )
    )

    tbl = _create_table(
        session_catalog=session_catalog,
        identifier=table_identifier,
        properties={"format-version": str(format_version)},
        data=[],
        sort_order=sort_order,
    )

    tbl.append(arrow_table_with_null)

    query_sorted_df = spark.sql(
        f"SELECT * FROM {table_identifier} ORDER BY {sort_col} {sort_direction} {sort_null_ordering}"
    ).toPandas()

    append_sorted_df = spark.table(table_identifier).toPandas()

    assert len(tbl.metadata.sort_orders) == 1, f"Expected no sort order for {tbl}"
    assert tbl.format_version == format_version, f"Expected v{format_version}, got: v{tbl.format_version}"
    assert append_sorted_df.shape[0] == 3, f"Expected 3 total rows for {table_identifier}"
    assert append_sorted_df.equals(
        query_sorted_df
    ), f"Expected sorted dataframe for v{format_version}  on col: {sort_col} in direction {sort_direction} with null ordering as {sort_null_ordering}, got {append_sorted_df[sort_col]}"


@pytest.mark.integration
@pytest.mark.parametrize(
    "sort_col_tuple_3", [("int", "bool", "string"), ("long", "float", "double"), ("date", "timestamp", "timestamptz")]
)
@pytest.mark.parametrize("sort_direction_tuple_3", [(SortDirection.ASC, SortDirection.DESC, SortDirection.DESC)])
@pytest.mark.parametrize(
    "sort_null_ordering_tuple_3",
    [
        (NullOrder.NULLS_FIRST, NullOrder.NULLS_FIRST, NullOrder.NULLS_LAST),
        (NullOrder.NULLS_FIRST, NullOrder.NULLS_FIRST, NullOrder.NULLS_FIRST),
    ],
)
@pytest.mark.parametrize("format_version", [1, 2])
def test_query_null_append_multi_sort(
    session_catalog: Catalog,
    spark: SparkSession,
    arrow_table_with_null: pa.Table,
    sort_col_tuple_3: Tuple[str, str, str],
    sort_direction_tuple_3: Tuple[SortDirection, SortDirection, SortDirection],
    sort_null_ordering_tuple_3: Tuple[NullOrder, NullOrder, NullOrder],
    format_version: int,
) -> None:
    table_identifier = f"default.arrow_table_v{format_version}_with_null_multi_sorted_on_cols_{'_'.join(sort_col_tuple_3)}"

    sort_options_list = list(zip(sort_col_tuple_3, sort_direction_tuple_3, sort_null_ordering_tuple_3))

    sort_order = SortOrder(*[
        SortField(
            source_id=TABLE_SCHEMA.find_field(sort_col).field_id,
            direction=sort_direction,
            transform=IdentityTransform(),
            null_order=sort_null_ordering,
        )
        for sort_col, sort_direction, sort_null_ordering in sort_options_list
    ])

    tbl = _create_table(
        session_catalog=session_catalog,
        identifier=table_identifier,
        properties={"format-version": str(format_version)},
        data=[],
        sort_order=sort_order,
    )

    tbl.append(arrow_table_with_null)

    query_sorted_df = spark.sql(
        f"SELECT * FROM {table_identifier} ORDER BY {','.join([f' {sort_col} {sort_direction} {sort_null_ordering}' for sort_col, sort_direction, sort_null_ordering in sort_options_list])}"
    ).toPandas()

    append_sorted_df = spark.table(table_identifier).toPandas()

    assert tbl.format_version == format_version, f"Expected v{format_version}, got: v{tbl.format_version}"
    assert append_sorted_df.shape[0] == 3, f"Expected 3 total rows for {table_identifier}"
    assert append_sorted_df.equals(
        query_sorted_df
    ), f"Expected sorted dataframe for v{format_version}  on col: {sort_options_list}, got {append_sorted_df}"


@pytest.mark.integration
@pytest.mark.parametrize("part_col", ["int", "date", "string"])
@pytest.mark.parametrize(
    "sort_col_tuple_2", [("bool", "string_long"), ("long", "float"), ("double", "timestamp"), ("timestamptz", "binary")]
)
@pytest.mark.parametrize("format_version", [1, 2])
def test_query_null_append_partitioned_multi_sort(
    session_catalog: Catalog,
    spark: SparkSession,
    arrow_table_with_null: pa.Table,
    part_col: str,
    sort_col_tuple_2: Tuple[str, str],
    format_version: int,
) -> None:
    table_identifier = f"default.arrow_table_v{format_version}_with_null_partitioned_on_{part_col}_multi_sorted_on_cols_{'_'.join(sort_col_tuple_2)}"

    partition_spec = PartitionSpec(
        PartitionField(
            source_id=TABLE_SCHEMA.find_field(part_col).field_id, field_id=1001, transform=IdentityTransform(), name=part_col
        )
    )

    sort_order = SortOrder(*[
        SortField(
            source_id=TABLE_SCHEMA.find_field(sort_col).field_id,
            transform=IdentityTransform(),
        )
        for sort_col in sort_col_tuple_2
    ])

    tbl = _create_table(
        session_catalog=session_catalog,
        identifier=table_identifier,
        properties={"format-version": str(format_version)},
        data=[],
        partition_spec=partition_spec,
        sort_order=sort_order,
    )

    tbl.append(arrow_table_with_null)

    query_sorted_df = spark.sql(
        f"SELECT * FROM {table_identifier} ORDER BY {part_col} , {','.join([f' {sort_col} ' for sort_col in sort_col_tuple_2])}"
    ).toPandas()

    append_sorted_df = spark.sql(f"SELECT * FROM {table_identifier} ORDER BY {part_col}").toPandas()

    assert tbl.format_version == format_version, f"Expected v{format_version}, got: v{tbl.format_version}"
    assert append_sorted_df.shape[0] == 3, f"Expected 3 total rows for {table_identifier}"
    assert append_sorted_df.equals(
        query_sorted_df
    ), f"Expected sorted dataframe for v{format_version}  on col: {sort_col_tuple_2}, got {append_sorted_df}"


@pytest.mark.integration
@pytest.mark.parametrize(
    "sort_order",
    [
        SortOrder(*[
            SortField(source_id=1, transform=IdentityTransform()),
            SortField(source_id=4, transform=BucketTransform(2)),
        ]),
        SortOrder(SortField(source_id=5, transform=BucketTransform(2))),
        SortOrder(SortField(source_id=8, transform=BucketTransform(2))),
        SortOrder(SortField(source_id=9, transform=BucketTransform(2))),
        SortOrder(SortField(source_id=10, transform=BucketTransform(2))),
        SortOrder(SortField(source_id=4, transform=TruncateTransform(2))),
        SortOrder(SortField(source_id=5, transform=TruncateTransform(2))),
    ],
)
def test_invalid_sort_transform(
    session_catalog: Catalog, spark: SparkSession, arrow_table_with_null: pa.Table, sort_order: SortOrder
) -> None:
    table_identifier = (
        f"default.arrow_table_invalid_sort_transform_{','.join([str(field).replace('', '_') for field in sort_order.fields])}"
    )

    tbl = _create_table(
        session_catalog=session_catalog,
        identifier=table_identifier,
        properties={"format-version": "1"},
        schema=TABLE_SCHEMA,
        sort_order=sort_order,
    )

    with pytest.raises(
        ValueError,
        match="Not all sort transforms are supported for writes. Following sort orders cannot be written using pyarrow: *",
    ):
        tbl.append(arrow_table_with_null)


@pytest.mark.integration
@pytest.mark.parametrize(
    "sort_order",
    [
        SortOrder(*[
            SortField(source_id=8, transform=YearTransform()),
            SortField(source_id=4, transform=IdentityTransform()),
        ]),
        SortOrder(*[
            SortField(source_id=10, transform=YearTransform()),
            SortField(source_id=9, transform=DayTransform()),
        ]),
    ],
)
def test_valid_sort_transform(
    session_catalog: Catalog, spark: SparkSession, arrow_table_with_null: pa.Table, sort_order: SortOrder
) -> None:
    table_identifier = f"default.arrow_table_invalid_sort_transform_{'_'.join([f'__{field.transform}_{field.source_id}_{field.direction}_{str(field.null_order)}__'.replace(' ', '') for field in sort_order.fields])}"

    _ = _create_table(
        session_catalog=session_catalog,
        identifier=table_identifier,
        properties={"format-version": "1"},
        schema=TABLE_SCHEMA,
        data=[arrow_table_with_null],
        sort_order=sort_order,
    )

    def _get_sort_order_clause_spark_query(table_schema: Schema, sort_field: SortField) -> str:
        if isinstance(sort_field.transform, YearTransform):
            return f" YEAR({table_schema.find_field(sort_field.source_id).name}) {sort_field.direction} {sort_field.null_order} "
        elif isinstance(sort_field.transform, MonthTransform):
            return f" MONTH({table_schema.find_field(sort_field.source_id).name}) {sort_field.direction} {sort_field.null_order} "
        elif isinstance(sort_field.transform, DayTransform):
            return f" DAY({table_schema.find_field(sort_field.source_id).name}) {sort_field.direction} {sort_field.null_order} "
        elif isinstance(sort_field.transform, IdentityTransform):
            return f" {table_schema.find_field(sort_field.source_id).name} {sort_field.direction} {sort_field.null_order} "
        else:
            raise ValueError("Not Supported Transform for Test")

    query_sorted_df = spark.sql(
        f"SELECT * FROM {table_identifier} ORDER BY {','.join([_get_sort_order_clause_spark_query(TABLE_SCHEMA, field) for field in sort_order.fields])}"
    ).toPandas()

    append_sorted_df = spark.table(table_identifier).toPandas()

    assert append_sorted_df.shape[0] == 3, f"Expected 3 total rows for {table_identifier}"
    assert append_sorted_df.equals(
        query_sorted_df
    ), f"Expected sorted dataframe on col: {','.join([f'[{field}]' for field in sort_order.fields])}, got {append_sorted_df}"


@pytest.mark.integration
@pytest.mark.parametrize("format_version", [1, 2])
def test_manifest_for_sort_order(
    session_catalog: Catalog, spark: SparkSession, arrow_table_with_null: pa.Table, format_version: int
) -> None:
    table_identifier = f"default.arrow_table_v{format_version}_manifest_for_sort_order"

    tbl = _create_table(
        session_catalog=session_catalog,
        identifier=table_identifier,
        properties={"format-version": str(format_version)},
        schema=TABLE_SCHEMA,
        data=[arrow_table_with_null],
        sort_order=SortOrder(
            SortField(
                source_id=4,
                transform=IdentityTransform(),
            )
        ),
    )

    files_df = spark.sql(
        f"""
                SELECT *
                FROM {table_identifier}.files
            """
    )

    assert tbl.format_version == format_version, f"Expected v{format_version}, got: v{tbl.format_version}"
    assert files_df.count() == 1, f"Expected 1 file in {table_identifier}.files, got: {files_df.count()}"
    assert [row.sort_order_id for row in files_df.select("sort_order_id").collect()] == [
        1
    ], "Expected Sort Order Id to be set as 1 in the manifest file"


@pytest.mark.integration
@pytest.mark.parametrize("format_version", [1, 2])
def test_manifest_partitioned_for_sort_order(
    session_catalog: Catalog, spark: SparkSession, arrow_table_with_null: pa.Table, format_version: int
) -> None:
    table_identifier = f"default.arrow_table_v{format_version}_manifest_partitioned_for_sort_order"

    tbl = _create_table(
        session_catalog=session_catalog,
        identifier=table_identifier,
        properties={"format-version": str(format_version)},
        schema=TABLE_SCHEMA,
        data=[arrow_table_with_null],
        sort_order=SortOrder(
            SortField(
                source_id=4,
                transform=IdentityTransform(),
            )
        ),
        partition_spec=PartitionSpec(
            PartitionField(source_id=10, field_id=1001, transform=IdentityTransform(), name="identity_date")
        ),
    )

    files_df = spark.sql(
        f"""
                SELECT *
                FROM {table_identifier}.files
            """
    )

    assert tbl.format_version == format_version, f"Expected v{format_version}, got: v{tbl.format_version}"
    assert files_df.count() == 3, f"Expected 3 files in {table_identifier}.files, got: {files_df.count()}"
    assert [row.sort_order_id for row in files_df.select("sort_order_id").collect()] == [
        1,
        1,
        1,
    ], "Expected Sort Order Id to be set as 1 in the manifest file"
