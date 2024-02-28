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

from pyiceberg.catalog import Catalog, load_catalog
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.partitioning import PartitionField, PartitionSpec
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


@pytest.fixture()
def catalog_rest() -> Catalog:
    return load_catalog(
        "local",
        **{
            "type": "rest",
            "uri": "http://localhost:8181",
            "s3.endpoint": "http://localhost:9000",
            "s3.access-key-id": "admin",
            "s3.secret-access-key": "password",
        },
    )


@pytest.fixture()
def catalog_hive() -> Catalog:
    return load_catalog(
        "local",
        **{
            "type": "hive",
            "uri": "http://localhost:9083",
            "s3.endpoint": "http://localhost:9000",
            "s3.access-key-id": "admin",
            "s3.secret-access-key": "password",
        },
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


def _create_table_with_schema(catalog: Catalog, schema: Schema, format_version: str) -> Table:
    tbl_name = "default.test_schema_evolution"
    try:
        catalog.drop_table(tbl_name)
    except NoSuchTableError:
        pass
    return catalog.create_table(identifier=tbl_name, schema=schema, properties={"format-version": format_version})


@pytest.mark.integration
@pytest.mark.parametrize('catalog', [pytest.lazy_fixture('catalog_hive'), pytest.lazy_fixture('catalog_rest')])
def test_add_identity_partition(catalog: Catalog, table_schema_simple: Schema) -> None:
    simple_table = _simple_table(catalog, table_schema_simple)
    simple_table.update_spec().add_identity("foo").commit()
    specs = simple_table.specs()
    assert len(specs) == 2
    spec = simple_table.spec()
    assert spec.spec_id == 1
    assert spec.last_assigned_field_id == 1000


@pytest.mark.integration
@pytest.mark.parametrize('catalog', [pytest.lazy_fixture('catalog_hive'), pytest.lazy_fixture('catalog_rest')])
def test_add_year(catalog: Catalog) -> None:
    table = _table(catalog)
    table.update_spec().add_field("event_ts", YearTransform(), "year_transform").commit()
    _validate_new_partition_fields(table, 1000, 1, 1000, PartitionField(2, 1000, YearTransform(), "year_transform"))


@pytest.mark.integration
@pytest.mark.parametrize('catalog', [pytest.lazy_fixture('catalog_hive'), pytest.lazy_fixture('catalog_rest')])
def test_add_month(catalog: Catalog) -> None:
    table = _table(catalog)
    table.update_spec().add_field("event_ts", MonthTransform(), "month_transform").commit()
    _validate_new_partition_fields(table, 1000, 1, 1000, PartitionField(2, 1000, MonthTransform(), "month_transform"))


@pytest.mark.integration
@pytest.mark.parametrize('catalog', [pytest.lazy_fixture('catalog_hive'), pytest.lazy_fixture('catalog_rest')])
def test_add_day(catalog: Catalog) -> None:
    table = _table(catalog)
    table.update_spec().add_field("event_ts", DayTransform(), "day_transform").commit()
    _validate_new_partition_fields(table, 1000, 1, 1000, PartitionField(2, 1000, DayTransform(), "day_transform"))


@pytest.mark.integration
@pytest.mark.parametrize('catalog', [pytest.lazy_fixture('catalog_hive'), pytest.lazy_fixture('catalog_rest')])
def test_add_hour(catalog: Catalog) -> None:
    table = _table(catalog)
    table.update_spec().add_field("event_ts", HourTransform(), "hour_transform").commit()
    _validate_new_partition_fields(table, 1000, 1, 1000, PartitionField(2, 1000, HourTransform(), "hour_transform"))


@pytest.mark.integration
@pytest.mark.parametrize('catalog', [pytest.lazy_fixture('catalog_hive'), pytest.lazy_fixture('catalog_rest')])
def test_add_bucket(catalog: Catalog, table_schema_simple: Schema) -> None:
    simple_table = _create_table_with_schema(catalog, table_schema_simple, "1")
    simple_table.update_spec().add_field("foo", BucketTransform(12), "bucket_transform").commit()
    _validate_new_partition_fields(simple_table, 1000, 1, 1000, PartitionField(1, 1000, BucketTransform(12), "bucket_transform"))


@pytest.mark.integration
@pytest.mark.parametrize('catalog', [pytest.lazy_fixture('catalog_hive'), pytest.lazy_fixture('catalog_rest')])
def test_add_truncate(catalog: Catalog, table_schema_simple: Schema) -> None:
    simple_table = _create_table_with_schema(catalog, table_schema_simple, "1")
    simple_table.update_spec().add_field("foo", TruncateTransform(1), "truncate_transform").commit()
    _validate_new_partition_fields(
        simple_table, 1000, 1, 1000, PartitionField(1, 1000, TruncateTransform(1), "truncate_transform")
    )


@pytest.mark.integration
@pytest.mark.parametrize('catalog', [pytest.lazy_fixture('catalog_hive'), pytest.lazy_fixture('catalog_rest')])
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
@pytest.mark.parametrize('catalog', [pytest.lazy_fixture('catalog_hive'), pytest.lazy_fixture('catalog_rest')])
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
@pytest.mark.parametrize('catalog', [pytest.lazy_fixture('catalog_hive'), pytest.lazy_fixture('catalog_rest')])
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
@pytest.mark.parametrize('catalog', [pytest.lazy_fixture('catalog_hive'), pytest.lazy_fixture('catalog_rest')])
def test_remove_identity(catalog: Catalog) -> None:
    table = _table(catalog)
    table.update_spec().add_identity("id").commit()
    table.update_spec().remove_field("id").commit()
    assert len(table.specs()) == 3
    assert table.spec().spec_id == 2
    assert table.spec() == PartitionSpec(
        PartitionField(source_id=1, field_id=1000, transform=VoidTransform(), name='id'), spec_id=2
    )


@pytest.mark.integration
@pytest.mark.parametrize('catalog', [pytest.lazy_fixture('catalog_hive'), pytest.lazy_fixture('catalog_rest')])
def test_remove_identity_v2(catalog: Catalog) -> None:
    table_v2 = _table_v2(catalog)
    table_v2.update_spec().add_identity("id").commit()
    table_v2.update_spec().remove_field("id").commit()
    assert len(table_v2.specs()) == 2
    assert table_v2.spec().spec_id == 0
    assert table_v2.spec() == PartitionSpec(spec_id=0)


@pytest.mark.integration
@pytest.mark.parametrize('catalog', [pytest.lazy_fixture('catalog_hive'), pytest.lazy_fixture('catalog_rest')])
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
        PartitionField(source_id=1, field_id=1000, transform=VoidTransform(), name='bucketed_id'),
        PartitionField(source_id=2, field_id=1001, transform=DayTransform(), name='day_ts'),
    )


@pytest.mark.integration
@pytest.mark.parametrize('catalog', [pytest.lazy_fixture('catalog_hive'), pytest.lazy_fixture('catalog_rest')])
def test_remove_bucket_v2(catalog: Catalog) -> None:
    table_v2 = _table_v2(catalog)
    with table_v2.update_spec() as update:
        update.add_field("id", BucketTransform(16), "bucketed_id")
        update.add_field("event_ts", DayTransform(), "day_ts")
    with table_v2.update_spec() as remove:
        remove.remove_field("bucketed_id")
    assert len(table_v2.specs()) == 3
    _validate_new_partition_fields(
        table_v2, 1001, 2, 1001, PartitionField(source_id=2, field_id=1001, transform=DayTransform(), name='day_ts')
    )


@pytest.mark.integration
@pytest.mark.parametrize('catalog', [pytest.lazy_fixture('catalog_hive'), pytest.lazy_fixture('catalog_rest')])
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
        PartitionField(source_id=1, field_id=1000, transform=BucketTransform(16), name='bucketed_id'),
        PartitionField(source_id=2, field_id=1001, transform=VoidTransform(), name='day_ts'),
    )


@pytest.mark.integration
@pytest.mark.parametrize('catalog', [pytest.lazy_fixture('catalog_hive'), pytest.lazy_fixture('catalog_rest')])
def test_remove_day_v2(catalog: Catalog) -> None:
    table_v2 = _table_v2(catalog)
    with table_v2.update_spec() as update:
        update.add_field("id", BucketTransform(16), "bucketed_id")
        update.add_field("event_ts", DayTransform(), "day_ts")
    with table_v2.update_spec() as remove:
        remove.remove_field("day_ts")
    assert len(table_v2.specs()) == 3
    _validate_new_partition_fields(
        table_v2, 1000, 2, 1001, PartitionField(source_id=1, field_id=1000, transform=BucketTransform(16), name='bucketed_id')
    )


@pytest.mark.integration
@pytest.mark.parametrize('catalog', [pytest.lazy_fixture('catalog_hive'), pytest.lazy_fixture('catalog_rest')])
def test_rename(catalog: Catalog) -> None:
    table = _table(catalog)
    table.update_spec().add_identity("id").commit()
    table.update_spec().rename_field("id", "sharded_id").commit()
    assert len(table.specs()) == 3
    assert table.spec().spec_id == 2
    _validate_new_partition_fields(table, 1000, 2, 1000, PartitionField(1, 1000, IdentityTransform(), "sharded_id"))


@pytest.mark.integration
@pytest.mark.parametrize('catalog', [pytest.lazy_fixture('catalog_hive'), pytest.lazy_fixture('catalog_rest')])
def test_cannot_add_and_remove(catalog: Catalog) -> None:
    table = _table(catalog)
    with pytest.raises(ValueError) as exc_info:
        table.update_spec().add_identity("id").remove_field("id").commit()
    assert "Cannot delete newly added field id" in str(exc_info.value)


@pytest.mark.integration
@pytest.mark.parametrize('catalog', [pytest.lazy_fixture('catalog_hive'), pytest.lazy_fixture('catalog_rest')])
def test_cannot_add_redundant_time_partition(catalog: Catalog) -> None:
    table = _table(catalog)
    with pytest.raises(ValueError) as exc_info:
        table.update_spec().add_field("event_ts", YearTransform(), "year_transform").add_field(
            "event_ts", HourTransform(), "hour_transform"
        ).commit()
    assert "Cannot add time partition field: hour_transform conflicts with year_transform" in str(exc_info.value)


@pytest.mark.integration
@pytest.mark.parametrize('catalog', [pytest.lazy_fixture('catalog_hive'), pytest.lazy_fixture('catalog_rest')])
def test_cannot_delete_and_rename(catalog: Catalog) -> None:
    table = _table(catalog)
    with pytest.raises(ValueError) as exc_info:
        table.update_spec().add_identity("id").commit()
        table.update_spec().remove_field("id").rename_field("id", "sharded_id").commit()
    assert "Cannot delete and rename partition field id" in str(exc_info.value)


@pytest.mark.integration
@pytest.mark.parametrize('catalog', [pytest.lazy_fixture('catalog_hive'), pytest.lazy_fixture('catalog_rest')])
def test_cannot_rename_and_delete(catalog: Catalog) -> None:
    table = _table(catalog)
    with pytest.raises(ValueError) as exc_info:
        table.update_spec().add_identity("id").commit()
        table.update_spec().rename_field("id", "sharded_id").remove_field("id").commit()
    assert "Cannot rename and delete field id" in str(exc_info.value)


@pytest.mark.integration
@pytest.mark.parametrize('catalog', [pytest.lazy_fixture('catalog_hive'), pytest.lazy_fixture('catalog_rest')])
def test_cannot_add_same_tranform_for_same_field(catalog: Catalog) -> None:
    table = _table(catalog)
    with pytest.raises(ValueError) as exc_info:
        table.update_spec().add_field("str", TruncateTransform(4), "truncated_str").add_field(
            "str", TruncateTransform(4)
        ).commit()
    assert "Already added partition" in str(exc_info.value)


@pytest.mark.integration
@pytest.mark.parametrize('catalog', [pytest.lazy_fixture('catalog_hive'), pytest.lazy_fixture('catalog_rest')])
def test_cannot_add_same_field_multiple_times(catalog: Catalog) -> None:
    table = _table(catalog)
    with pytest.raises(ValueError) as exc_info:
        table.update_spec().add_field("id", IdentityTransform(), "duplicate").add_field(
            "id", IdentityTransform(), "duplicate"
        ).commit()
    assert "Already added partition" in str(exc_info.value)


@pytest.mark.integration
@pytest.mark.parametrize('catalog', [pytest.lazy_fixture('catalog_hive'), pytest.lazy_fixture('catalog_rest')])
def test_cannot_add_multiple_specs_same_name(catalog: Catalog) -> None:
    table = _table(catalog)
    with pytest.raises(ValueError) as exc_info:
        table.update_spec().add_field("id", IdentityTransform(), "duplicate").add_field(
            "event_ts", IdentityTransform(), "duplicate"
        ).commit()
    assert "Already added partition" in str(exc_info.value)


@pytest.mark.integration
@pytest.mark.parametrize('catalog', [pytest.lazy_fixture('catalog_hive'), pytest.lazy_fixture('catalog_rest')])
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
        NestedField(field_id=1, name='id', field_type=LongType(), required=False),
        NestedField(field_id=2, name='event_ts', field_type=TimestampType(), required=False),
        NestedField(field_id=3, name='str', field_type=StringType(), required=False),
        NestedField(field_id=4, name='col_string', field_type=StringType(), required=False),
        schema_id=1,
        identifier_field_ids=[],
    )


@pytest.mark.integration
@pytest.mark.parametrize('catalog', [pytest.lazy_fixture('catalog_hive'), pytest.lazy_fixture('catalog_rest')])
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
@pytest.mark.parametrize('catalog', [pytest.lazy_fixture('catalog_hive'), pytest.lazy_fixture('catalog_rest')])
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
@pytest.mark.parametrize('catalog', [pytest.lazy_fixture('catalog_hive'), pytest.lazy_fixture('catalog_rest')])
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
