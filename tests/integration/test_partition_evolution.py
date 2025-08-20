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

import pytest

from pyiceberg.catalog import Catalog
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.partitioning import UNPARTITIONED_PARTITION_SPEC, PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.transforms import (
    BucketTransform,
    DayTransform,
    HourTransform,
    IdentityTransform,
    MonthTransform,
    TruncateTransform,
    VoidTransform,
    YearTransform,
)
from pyiceberg.types import (
    LongType,
    NestedField,
    StringType,
    TimestampType,
)


def _simple_table(catalog: Catalog, table_schema_simple: Schema) -> Table:
    return _create_table_with_schema(catalog, table_schema_simple, "1")


def _table(catalog: Catalog) -> Table:
    schema_with_timestamp = Schema(
        NestedField(1, "id", LongType(), required=False),
        NestedField(2, "event_ts", TimestampType(), required=False),
        NestedField(3, "str", StringType(), required=False),
    )
    return _create_table_with_schema(catalog, schema_with_timestamp, "1")


def _table_v2(catalog: Catalog) -> Table:
    schema_with_timestamp = Schema(
        NestedField(1, "id", LongType(), required=False),
        NestedField(2, "event_ts", TimestampType(), required=False),
        NestedField(3, "str", StringType(), required=False),
    )
    return _create_table_with_schema(catalog, schema_with_timestamp, "2")


def _create_table_with_schema(
    catalog: Catalog, schema: Schema, format_version: str, partition_spec: PartitionSpec = UNPARTITIONED_PARTITION_SPEC
) -> Table:
    tbl_name = "default.test_schema_evolution"
    try:
        catalog.drop_table(tbl_name)
    except NoSuchTableError:
        pass

    return catalog.create_table(
        identifier=tbl_name, schema=schema, partition_spec=partition_spec, properties={"format-version": format_version}
    )


@pytest.mark.integration
@pytest.mark.parametrize("catalog", [pytest.lazy_fixture("session_catalog_hive"), pytest.lazy_fixture("session_catalog")])
def test_add_identity_partition(catalog: Catalog, table_schema_simple: Schema) -> None:
    simple_table = _simple_table(catalog, table_schema_simple)
    simple_table.update_spec().add_identity("foo").commit()
    specs = simple_table.specs()
    assert len(specs) == 2
    spec = simple_table.spec()
    assert spec.spec_id == 1
    assert spec.last_assigned_field_id == 1000


@pytest.mark.integration
@pytest.mark.parametrize("catalog", [pytest.lazy_fixture("session_catalog_hive"), pytest.lazy_fixture("session_catalog")])
def test_add_year(catalog: Catalog) -> None:
    table = _table(catalog)
    table.update_spec().add_field("event_ts", YearTransform(), "year_transform").commit()
    _validate_new_partition_fields(table, 1000, 1, 1000, PartitionField(2, 1000, YearTransform(), "year_transform"))


@pytest.mark.integration
@pytest.mark.parametrize("catalog", [pytest.lazy_fixture("session_catalog_hive"), pytest.lazy_fixture("session_catalog")])
def test_add_year_generates_default_name(catalog: Catalog) -> None:
    table = _table(catalog)
    table.update_spec().add_field("event_ts", YearTransform()).commit()
    _validate_new_partition_fields(table, 1000, 1, 1000, PartitionField(2, 1000, YearTransform(), "event_ts_year"))


@pytest.mark.integration
@pytest.mark.parametrize("catalog", [pytest.lazy_fixture("session_catalog_hive"), pytest.lazy_fixture("session_catalog")])
def test_add_month(catalog: Catalog) -> None:
    table = _table(catalog)
    table.update_spec().add_field("event_ts", MonthTransform(), "month_transform").commit()
    _validate_new_partition_fields(table, 1000, 1, 1000, PartitionField(2, 1000, MonthTransform(), "month_transform"))


@pytest.mark.integration
@pytest.mark.parametrize("catalog", [pytest.lazy_fixture("session_catalog_hive"), pytest.lazy_fixture("session_catalog")])
def test_add_month_generates_default_name(catalog: Catalog) -> None:
    table = _table(catalog)
    table.update_spec().add_field("event_ts", MonthTransform()).commit()
    _validate_new_partition_fields(table, 1000, 1, 1000, PartitionField(2, 1000, MonthTransform(), "event_ts_month"))


@pytest.mark.integration
@pytest.mark.parametrize("catalog", [pytest.lazy_fixture("session_catalog_hive"), pytest.lazy_fixture("session_catalog")])
def test_add_day(catalog: Catalog) -> None:
    table = _table(catalog)
    table.update_spec().add_field("event_ts", DayTransform(), "day_transform").commit()
    _validate_new_partition_fields(table, 1000, 1, 1000, PartitionField(2, 1000, DayTransform(), "day_transform"))


@pytest.mark.integration
@pytest.mark.parametrize("catalog", [pytest.lazy_fixture("session_catalog_hive"), pytest.lazy_fixture("session_catalog")])
def test_add_day_generates_default_name(catalog: Catalog) -> None:
    table = _table(catalog)
    table.update_spec().add_field("event_ts", DayTransform()).commit()
    _validate_new_partition_fields(table, 1000, 1, 1000, PartitionField(2, 1000, DayTransform(), "event_ts_day"))


@pytest.mark.integration
@pytest.mark.parametrize("catalog", [pytest.lazy_fixture("session_catalog_hive"), pytest.lazy_fixture("session_catalog")])
def test_add_hour(catalog: Catalog) -> None:
    table = _table(catalog)
    table.update_spec().add_field("event_ts", HourTransform(), "hour_transform").commit()
    _validate_new_partition_fields(table, 1000, 1, 1000, PartitionField(2, 1000, HourTransform(), "hour_transform"))


@pytest.mark.integration
@pytest.mark.parametrize("catalog", [pytest.lazy_fixture("session_catalog_hive"), pytest.lazy_fixture("session_catalog")])
def test_add_hour_string_transform(catalog: Catalog) -> None:
    table = _table(catalog)
    table.update_spec().add_field("event_ts", "hour", "str_hour_transform").commit()
    _validate_new_partition_fields(table, 1000, 1, 1000, PartitionField(2, 1000, HourTransform(), "str_hour_transform"))


@pytest.mark.integration
@pytest.mark.parametrize("catalog", [pytest.lazy_fixture("session_catalog_hive"), pytest.lazy_fixture("session_catalog")])
def test_add_hour_generates_default_name(catalog: Catalog) -> None:
    table = _table(catalog)
    table.update_spec().add_field("event_ts", HourTransform()).commit()
    _validate_new_partition_fields(table, 1000, 1, 1000, PartitionField(2, 1000, HourTransform(), "event_ts_hour"))


@pytest.mark.integration
@pytest.mark.parametrize("catalog", [pytest.lazy_fixture("session_catalog_hive"), pytest.lazy_fixture("session_catalog")])
def test_add_bucket(catalog: Catalog, table_schema_simple: Schema) -> None:
    simple_table = _create_table_with_schema(catalog, table_schema_simple, "1")
    simple_table.update_spec().add_field("foo", BucketTransform(12), "bucket_transform").commit()
    _validate_new_partition_fields(simple_table, 1000, 1, 1000, PartitionField(1, 1000, BucketTransform(12), "bucket_transform"))


@pytest.mark.integration
@pytest.mark.parametrize("catalog", [pytest.lazy_fixture("session_catalog_hive"), pytest.lazy_fixture("session_catalog")])
def test_add_bucket_generates_default_name(catalog: Catalog, table_schema_simple: Schema) -> None:
    simple_table = _create_table_with_schema(catalog, table_schema_simple, "1")
    simple_table.update_spec().add_field("foo", BucketTransform(12)).commit()
    _validate_new_partition_fields(simple_table, 1000, 1, 1000, PartitionField(1, 1000, BucketTransform(12), "foo_bucket_12"))


@pytest.mark.integration
@pytest.mark.parametrize("catalog", [pytest.lazy_fixture("session_catalog_hive"), pytest.lazy_fixture("session_catalog")])
def test_add_truncate(catalog: Catalog, table_schema_simple: Schema) -> None:
    simple_table = _create_table_with_schema(catalog, table_schema_simple, "1")
    simple_table.update_spec().add_field("foo", TruncateTransform(1), "truncate_transform").commit()
    _validate_new_partition_fields(
        simple_table, 1000, 1, 1000, PartitionField(1, 1000, TruncateTransform(1), "truncate_transform")
    )


@pytest.mark.integration
@pytest.mark.parametrize("catalog", [pytest.lazy_fixture("session_catalog_hive"), pytest.lazy_fixture("session_catalog")])
def test_add_truncate_generates_default_name(catalog: Catalog, table_schema_simple: Schema) -> None:
    simple_table = _create_table_with_schema(catalog, table_schema_simple, "1")
    simple_table.update_spec().add_field("foo", TruncateTransform(1)).commit()
    _validate_new_partition_fields(simple_table, 1000, 1, 1000, PartitionField(1, 1000, TruncateTransform(1), "foo_trunc_1"))


@pytest.mark.integration
@pytest.mark.parametrize("catalog", [pytest.lazy_fixture("session_catalog_hive"), pytest.lazy_fixture("session_catalog")])
def test_multiple_adds(catalog: Catalog) -> None:
    table = _table(catalog)
    table.update_spec().add_identity("id").add_field("event_ts", HourTransform(), "hourly_partitioned").add_field(
        "str", TruncateTransform(2), "truncate_str"
    ).commit()
    _validate_new_partition_fields(
        table,
        1002,
        1,
        1002,
        PartitionField(1, 1000, IdentityTransform(), "id"),
        PartitionField(2, 1001, HourTransform(), "hourly_partitioned"),
        PartitionField(3, 1002, TruncateTransform(2), "truncate_str"),
    )


@pytest.mark.integration
@pytest.mark.parametrize("catalog", [pytest.lazy_fixture("session_catalog_hive"), pytest.lazy_fixture("session_catalog")])
def test_add_void(catalog: Catalog, table_schema_simple: Schema) -> None:
    simple_table = _create_table_with_schema(catalog, table_schema_simple, "1")
    simple_table.update_spec().add_field("foo", VoidTransform(), "void_transform").commit()
    _validate_new_partition_fields(simple_table, 1000, 1, 1000, PartitionField(1, 1000, VoidTransform(), "void_transform"))


@pytest.mark.integration
@pytest.mark.parametrize("catalog", [pytest.lazy_fixture("session_catalog_hive"), pytest.lazy_fixture("session_catalog")])
def test_add_void_generates_default_name(catalog: Catalog, table_schema_simple: Schema) -> None:
    simple_table = _create_table_with_schema(catalog, table_schema_simple, "1")
    simple_table.update_spec().add_field("foo", VoidTransform()).commit()
    _validate_new_partition_fields(simple_table, 1000, 1, 1000, PartitionField(1, 1000, VoidTransform(), "foo_null"))


@pytest.mark.integration
@pytest.mark.parametrize("catalog", [pytest.lazy_fixture("session_catalog_hive"), pytest.lazy_fixture("session_catalog")])
def test_add_hour_to_day(catalog: Catalog) -> None:
    table = _table(catalog)
    table.update_spec().add_field("event_ts", DayTransform(), "daily_partitioned").commit()
    table.update_spec().add_field("event_ts", HourTransform(), "hourly_partitioned").commit()
    _validate_new_partition_fields(
        table,
        1001,
        2,
        1001,
        PartitionField(2, 1000, DayTransform(), "daily_partitioned"),
        PartitionField(2, 1001, HourTransform(), "hourly_partitioned"),
    )


@pytest.mark.integration
@pytest.mark.parametrize("catalog", [pytest.lazy_fixture("session_catalog_hive"), pytest.lazy_fixture("session_catalog")])
def test_add_multiple_buckets(catalog: Catalog) -> None:
    table = _table(catalog)
    table.update_spec().add_field("id", BucketTransform(16)).add_field("id", BucketTransform(4)).commit()
    _validate_new_partition_fields(
        table,
        1001,
        1,
        1001,
        PartitionField(1, 1000, BucketTransform(16), "id_bucket_16"),
        PartitionField(1, 1001, BucketTransform(4), "id_bucket_4"),
    )


@pytest.mark.integration
@pytest.mark.parametrize("catalog", [pytest.lazy_fixture("session_catalog_hive"), pytest.lazy_fixture("session_catalog")])
def test_remove_identity(catalog: Catalog) -> None:
    table = _table(catalog)
    table.update_spec().add_identity("id").commit()
    table.update_spec().remove_field("id").commit()
    assert len(table.specs()) == 3
    assert table.spec().spec_id == 2
    assert table.spec() == PartitionSpec(
        PartitionField(source_id=1, field_id=1000, transform=VoidTransform(), name="id"), spec_id=2
    )


@pytest.mark.integration
@pytest.mark.parametrize("catalog", [pytest.lazy_fixture("session_catalog_hive"), pytest.lazy_fixture("session_catalog")])
def test_remove_identity_v2(catalog: Catalog) -> None:
    table_v2 = _table_v2(catalog)
    table_v2.update_spec().add_identity("id").commit()
    table_v2.update_spec().remove_field("id").commit()
    assert len(table_v2.specs()) == 2
    assert table_v2.spec().spec_id == 0
    assert table_v2.spec() == PartitionSpec(spec_id=0)


@pytest.mark.integration
@pytest.mark.parametrize("catalog", [pytest.lazy_fixture("session_catalog_hive"), pytest.lazy_fixture("session_catalog")])
def test_remove_and_add_identity(catalog: Catalog) -> None:
    table = _table(catalog)
    table.update_spec().add_identity("id").commit()
    table.update_spec().remove_field("id").commit()
    table.update_spec().add_identity("id").commit()

    assert len(table.specs()) == 4
    assert table.spec().spec_id == 3
    assert table.spec() == PartitionSpec(
        PartitionField(source_id=1, field_id=1000, transform=VoidTransform(), name="id_1000"),
        PartitionField(source_id=1, field_id=1001, transform=IdentityTransform(), name="id"),
        spec_id=3,
    )


@pytest.mark.integration
@pytest.mark.parametrize("catalog", [pytest.lazy_fixture("session_catalog_hive"), pytest.lazy_fixture("session_catalog")])
def test_remove_and_add_identity_v2(catalog: Catalog) -> None:
    table_v2 = _table_v2(catalog)
    table_v2.update_spec().add_identity("id").commit()
    table_v2.update_spec().remove_field("id").commit()
    table_v2.update_spec().add_identity("id").commit()

    assert len(table_v2.specs()) == 2
    assert table_v2.spec().spec_id == 1
    assert table_v2.spec() == PartitionSpec(
        PartitionField(source_id=1, field_id=1000, transform=IdentityTransform(), name="id"), spec_id=1
    )


@pytest.mark.integration
@pytest.mark.parametrize("catalog", [pytest.lazy_fixture("session_catalog_hive"), pytest.lazy_fixture("session_catalog")])
def test_remove_bucket(catalog: Catalog) -> None:
    table = _table(catalog)
    with table.update_spec() as update:
        update.add_field("id", BucketTransform(16), "bucketed_id")
        update.add_field("event_ts", DayTransform(), "day_ts")
    with table.update_spec() as remove:
        remove.remove_field("bucketed_id")

    assert len(table.specs()) == 3
    _validate_new_partition_fields(
        table,
        1001,
        2,
        1001,
        PartitionField(source_id=1, field_id=1000, transform=VoidTransform(), name="bucketed_id"),
        PartitionField(source_id=2, field_id=1001, transform=DayTransform(), name="day_ts"),
    )


@pytest.mark.integration
@pytest.mark.parametrize("catalog", [pytest.lazy_fixture("session_catalog_hive"), pytest.lazy_fixture("session_catalog")])
def test_remove_bucket_v2(catalog: Catalog) -> None:
    table_v2 = _table_v2(catalog)
    with table_v2.update_spec() as update:
        update.add_field("id", BucketTransform(16), "bucketed_id")
        update.add_field("event_ts", DayTransform(), "day_ts")
    with table_v2.update_spec() as remove:
        remove.remove_field("bucketed_id")
    assert len(table_v2.specs()) == 3
    _validate_new_partition_fields(
        table_v2, 1001, 2, 1001, PartitionField(source_id=2, field_id=1001, transform=DayTransform(), name="day_ts")
    )


@pytest.mark.integration
@pytest.mark.parametrize("catalog", [pytest.lazy_fixture("session_catalog_hive"), pytest.lazy_fixture("session_catalog")])
def test_remove_day(catalog: Catalog) -> None:
    table = _table(catalog)
    with table.update_spec() as update:
        update.add_field("id", BucketTransform(16), "bucketed_id")
        update.add_field("event_ts", DayTransform(), "day_ts")
    with table.update_spec() as remove:
        remove.remove_field("day_ts")

    assert len(table.specs()) == 3
    _validate_new_partition_fields(
        table,
        1001,
        2,
        1001,
        PartitionField(source_id=1, field_id=1000, transform=BucketTransform(16), name="bucketed_id"),
        PartitionField(source_id=2, field_id=1001, transform=VoidTransform(), name="day_ts"),
    )


@pytest.mark.integration
@pytest.mark.parametrize("catalog", [pytest.lazy_fixture("session_catalog_hive"), pytest.lazy_fixture("session_catalog")])
def test_remove_day_v2(catalog: Catalog) -> None:
    table_v2 = _table_v2(catalog)
    with table_v2.update_spec() as update:
        update.add_field("id", BucketTransform(16), "bucketed_id")
        update.add_field("event_ts", DayTransform(), "day_ts")
    with table_v2.update_spec() as remove:
        remove.remove_field("day_ts")
    assert len(table_v2.specs()) == 3
    _validate_new_partition_fields(
        table_v2, 1000, 2, 1001, PartitionField(source_id=1, field_id=1000, transform=BucketTransform(16), name="bucketed_id")
    )


@pytest.mark.integration
@pytest.mark.parametrize("catalog", [pytest.lazy_fixture("session_catalog_hive"), pytest.lazy_fixture("session_catalog")])
def test_rename(catalog: Catalog) -> None:
    table = _table(catalog)
    table.update_spec().add_identity("id").commit()
    table.update_spec().rename_field("id", "sharded_id").commit()
    assert len(table.specs()) == 3
    assert table.spec().spec_id == 2
    _validate_new_partition_fields(table, 1000, 2, 1000, PartitionField(1, 1000, IdentityTransform(), "sharded_id"))


@pytest.mark.integration
@pytest.mark.parametrize("catalog", [pytest.lazy_fixture("session_catalog_hive"), pytest.lazy_fixture("session_catalog")])
def test_cannot_add_and_remove(catalog: Catalog) -> None:
    table = _table(catalog)
    with pytest.raises(ValueError) as exc_info:
        table.update_spec().add_identity("id").remove_field("id").commit()
    assert "Cannot delete newly added field id" in str(exc_info.value)


@pytest.mark.integration
@pytest.mark.parametrize("catalog", [pytest.lazy_fixture("session_catalog_hive"), pytest.lazy_fixture("session_catalog")])
def test_cannot_add_redundant_time_partition(catalog: Catalog) -> None:
    table = _table(catalog)
    with pytest.raises(ValueError) as exc_info:
        table.update_spec().add_field("event_ts", YearTransform(), "year_transform").add_field(
            "event_ts", HourTransform(), "hour_transform"
        ).commit()
    assert "Cannot add time partition field: hour_transform conflicts with year_transform" in str(exc_info.value)


@pytest.mark.integration
@pytest.mark.parametrize("catalog", [pytest.lazy_fixture("session_catalog_hive"), pytest.lazy_fixture("session_catalog")])
def test_cannot_delete_and_rename(catalog: Catalog) -> None:
    table = _table(catalog)
    with pytest.raises(ValueError) as exc_info:
        table.update_spec().add_identity("id").commit()
        table.update_spec().remove_field("id").rename_field("id", "sharded_id").commit()
    assert "Cannot delete and rename partition field id" in str(exc_info.value)


@pytest.mark.integration
@pytest.mark.parametrize("catalog", [pytest.lazy_fixture("session_catalog_hive"), pytest.lazy_fixture("session_catalog")])
def test_cannot_rename_and_delete(catalog: Catalog) -> None:
    table = _table(catalog)
    with pytest.raises(ValueError) as exc_info:
        table.update_spec().add_identity("id").commit()
        table.update_spec().rename_field("id", "sharded_id").remove_field("id").commit()
    assert "Cannot rename and delete field id" in str(exc_info.value)


@pytest.mark.integration
@pytest.mark.parametrize("catalog", [pytest.lazy_fixture("session_catalog_hive"), pytest.lazy_fixture("session_catalog")])
def test_cannot_add_same_tranform_for_same_field(catalog: Catalog) -> None:
    table = _table(catalog)
    with pytest.raises(ValueError) as exc_info:
        table.update_spec().add_field("str", TruncateTransform(4), "truncated_str").add_field(
            "str", TruncateTransform(4)
        ).commit()
    assert "Already added partition" in str(exc_info.value)


@pytest.mark.integration
@pytest.mark.parametrize("catalog", [pytest.lazy_fixture("session_catalog_hive"), pytest.lazy_fixture("session_catalog")])
def test_cannot_add_same_field_multiple_times(catalog: Catalog) -> None:
    table = _table(catalog)
    with pytest.raises(ValueError) as exc_info:
        table.update_spec().add_field("id", IdentityTransform(), "duplicate").add_field(
            "id", IdentityTransform(), "duplicate"
        ).commit()
    assert "Already added partition" in str(exc_info.value)


@pytest.mark.integration
@pytest.mark.parametrize("catalog", [pytest.lazy_fixture("session_catalog_hive"), pytest.lazy_fixture("session_catalog")])
def test_cannot_add_multiple_specs_same_name(catalog: Catalog) -> None:
    table = _table(catalog)
    with pytest.raises(ValueError) as exc_info:
        table.update_spec().add_field("id", IdentityTransform(), "duplicate").add_field(
            "event_ts", IdentityTransform(), "duplicate"
        ).commit()
    assert "Already added partition" in str(exc_info.value)


@pytest.mark.integration
@pytest.mark.parametrize("catalog", [pytest.lazy_fixture("session_catalog_hive"), pytest.lazy_fixture("session_catalog")])
def test_change_specs_and_schema_transaction(catalog: Catalog) -> None:
    table = _table(catalog)
    with table.transaction() as transaction:
        with transaction.update_spec() as update_spec:
            update_spec.add_identity("id").add_field("event_ts", HourTransform(), "hourly_partitioned").add_field(
                "str", TruncateTransform(2), "truncate_str"
            )

        with transaction.update_schema() as update_schema:
            update_schema.add_column("col_string", StringType())

    _validate_new_partition_fields(
        table,
        1002,
        1,
        1002,
        PartitionField(1, 1000, IdentityTransform(), "id"),
        PartitionField(2, 1001, HourTransform(), "hourly_partitioned"),
        PartitionField(3, 1002, TruncateTransform(2), "truncate_str"),
    )

    assert table.schema() == Schema(
        NestedField(field_id=1, name="id", field_type=LongType(), required=False),
        NestedField(field_id=2, name="event_ts", field_type=TimestampType(), required=False),
        NestedField(field_id=3, name="str", field_type=StringType(), required=False),
        NestedField(field_id=4, name="col_string", field_type=StringType(), required=False),
        identifier_field_ids=[],
    )
    assert table.schema().schema_id == 1


@pytest.mark.integration
@pytest.mark.parametrize("catalog", [pytest.lazy_fixture("session_catalog_hive"), pytest.lazy_fixture("session_catalog")])
def test_multiple_adds_and_remove_v1(catalog: Catalog) -> None:
    table = _table(catalog)
    with table.update_spec() as update:
        update.add_field("id", BucketTransform(16), "bucketed_id")
        update.add_field("event_ts", DayTransform(), "day_ts")
    with table.update_spec() as update:
        update.remove_field("day_ts").remove_field("bucketed_id")
    with table.update_spec() as update:
        update.add_field("str", TruncateTransform(2), "truncated_str")
    _validate_new_partition_fields(
        table,
        1002,
        3,
        1002,
        PartitionField(1, 1000, VoidTransform(), "bucketed_id"),
        PartitionField(2, 1001, VoidTransform(), "day_ts"),
        PartitionField(3, 1002, TruncateTransform(2), "truncated_str"),
    )


@pytest.mark.integration
@pytest.mark.parametrize("catalog", [pytest.lazy_fixture("session_catalog_hive"), pytest.lazy_fixture("session_catalog")])
def test_multiple_adds_and_remove_v2(catalog: Catalog) -> None:
    table_v2 = _table_v2(catalog)
    with table_v2.update_spec() as update:
        update.add_field("id", BucketTransform(16), "bucketed_id")
        update.add_field("event_ts", DayTransform(), "day_ts")
    with table_v2.update_spec() as update:
        update.remove_field("day_ts").remove_field("bucketed_id")
    with table_v2.update_spec() as update:
        update.add_field("str", TruncateTransform(2), "truncated_str")
    _validate_new_partition_fields(table_v2, 1002, 2, 1002, PartitionField(3, 1002, TruncateTransform(2), "truncated_str"))


@pytest.mark.integration
@pytest.mark.parametrize("catalog", [pytest.lazy_fixture("session_catalog_hive"), pytest.lazy_fixture("session_catalog")])
def test_multiple_remove_and_add_reuses_v2(catalog: Catalog) -> None:
    table_v2 = _table_v2(catalog)
    with table_v2.update_spec() as update:
        update.add_field("id", BucketTransform(16), "bucketed_id")
        update.add_field("event_ts", DayTransform(), "day_ts")
    with table_v2.update_spec() as update:
        update.remove_field("day_ts").remove_field("bucketed_id")
    with table_v2.update_spec() as update:
        update.add_field("id", BucketTransform(16), "bucketed_id")
    _validate_new_partition_fields(table_v2, 1000, 2, 1001, PartitionField(1, 1000, BucketTransform(16), "bucketed_id"))


def _validate_new_partition_fields(
    table: Table,
    expected_spec_last_assigned_field_id: int,
    expected_spec_id: int,
    expected_metadata_last_assigned_field_id: int,
    *expected_partition_fields: PartitionField,
) -> None:
    spec = table.spec()
    assert spec.spec_id == expected_spec_id
    assert spec.last_assigned_field_id == expected_spec_last_assigned_field_id
    assert table.last_partition_id() == expected_metadata_last_assigned_field_id
    assert len(spec.fields) == len(expected_partition_fields)
    for i in range(len(spec.fields)):
        assert spec.fields[i] == expected_partition_fields[i]


@pytest.mark.integration
@pytest.mark.parametrize("catalog", [pytest.lazy_fixture("session_catalog_hive"), pytest.lazy_fixture("session_catalog")])
def test_partition_schema_field_name_conflict(catalog: Catalog) -> None:
    schema = Schema(
        NestedField(1, "id", LongType(), required=False),
        NestedField(2, "event_ts", TimestampType(), required=False),
        NestedField(3, "another_ts", TimestampType(), required=False),
        NestedField(4, "str", StringType(), required=False),
    )
    table = _create_table_with_schema(catalog, schema, "2")

    with pytest.raises(ValueError, match="Cannot create partition with a name that exists in schema: another_ts"):
        table.update_spec().add_field("event_ts", YearTransform(), "another_ts").commit()
    with pytest.raises(ValueError, match="Cannot create partition with a name that exists in schema: id"):
        table.update_spec().add_field("event_ts", DayTransform(), "id").commit()

    with pytest.raises(ValueError, match="Cannot create identity partition sourced from different field in schema: another_ts"):
        table.update_spec().add_field("event_ts", IdentityTransform(), "another_ts").commit()
    with pytest.raises(ValueError, match="Cannot create identity partition sourced from different field in schema: str"):
        table.update_spec().add_field("id", IdentityTransform(), "str").commit()

    table.update_spec().add_field("id", IdentityTransform(), "id").commit()
    table.update_spec().add_field("event_ts", YearTransform(), "event_year").commit()


@pytest.mark.integration
@pytest.mark.parametrize("catalog", [pytest.lazy_fixture("session_catalog_hive"), pytest.lazy_fixture("session_catalog")])
def test_partition_validation_during_table_creation(catalog: Catalog) -> None:
    schema = Schema(
        NestedField(1, "id", LongType(), required=False),
        NestedField(2, "event_ts", TimestampType(), required=False),
        NestedField(3, "another_ts", TimestampType(), required=False),
        NestedField(4, "str", StringType(), required=False),
    )

    partition_spec = PartitionSpec(
        PartitionField(source_id=2, field_id=1000, transform=YearTransform(), name="another_ts"), spec_id=1
    )
    with pytest.raises(ValueError, match="Cannot create partition with a name that exists in schema: another_ts"):
        _create_table_with_schema(catalog, schema, "2", partition_spec)

    partition_spec = PartitionSpec(
        PartitionField(source_id=1, field_id=1000, transform=IdentityTransform(), name="id"), spec_id=1
    )
    _create_table_with_schema(catalog, schema, "2", partition_spec)


@pytest.mark.integration
@pytest.mark.parametrize("catalog", [pytest.lazy_fixture("session_catalog_hive"), pytest.lazy_fixture("session_catalog")])
def test_schema_evolution_partition_conflict(catalog: Catalog) -> None:
    schema = Schema(
        NestedField(1, "id", LongType(), required=False),
        NestedField(2, "event_ts", TimestampType(), required=False),
    )
    partition_spec = PartitionSpec(
        PartitionField(source_id=2, field_id=1000, transform=YearTransform(), name="event_year"),
        PartitionField(source_id=2, field_id=1001, transform=IdentityTransform(), name="first_name"),
        PartitionField(source_id=1, field_id=1002, transform=IdentityTransform(), name="id"),
        spec_id=1,
    )
    table = _create_table_with_schema(catalog, schema, "2", partition_spec)

    with pytest.raises(ValueError, match="Cannot create partition with a name that exists in schema: event_year"):
        table.update_schema().add_column("event_year", StringType()).commit()
    with pytest.raises(ValueError, match="Cannot create identity partition sourced from different field in schema: first_name"):
        table.update_schema().add_column("first_name", StringType()).commit()

    table.update_schema().add_column("other_field", StringType()).commit()

    with pytest.raises(ValueError, match="Cannot create partition with a name that exists in schema: event_year"):
        table.update_schema().rename_column("other_field", "event_year").commit()
    with pytest.raises(ValueError, match="Cannot create identity partition sourced from different field in schema: first_name"):
        table.update_schema().rename_column("other_field", "first_name").commit()

    table.update_schema().rename_column("other_field", "valid_name").commit()
