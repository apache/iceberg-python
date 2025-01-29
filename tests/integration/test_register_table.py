import pytest

from pyiceberg.catalog import Catalog
from pyiceberg.catalog.hive import (
    HiveCatalog,
)
from pyiceberg.exceptions import NoSuchTableError, TableAlreadyExistsError
from pyiceberg.partitioning import UNPARTITIONED_PARTITION_SPEC, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.types import (
    BooleanType,
    DateType,
    IntegerType,
    NestedField,
    StringType,
)

TABLE_SCHEMA = Schema(
    NestedField(field_id=1, name="foo", field_type=BooleanType(), required=False),
    NestedField(field_id=2, name="bar", field_type=StringType(), required=False),
    NestedField(field_id=4, name="baz", field_type=IntegerType(), required=False),
    NestedField(field_id=10, name="qux", field_type=DateType(), required=False),
)


def _create_table(
    session_catalog: Catalog,
    identifier: str,
    format_version: int,
    location: str,
    partition_spec: PartitionSpec = UNPARTITIONED_PARTITION_SPEC,
    schema: Schema = TABLE_SCHEMA,
) -> Table:
    try:
        session_catalog.drop_table(identifier=identifier)
    except NoSuchTableError:
        pass

    return session_catalog.create_table(
        identifier=identifier,
        schema=schema,
        location=location,
        properties={"format-version": str(format_version)},
        partition_spec=partition_spec,
    )


@pytest.mark.integration
def test_hive_register_table(
    session_catalog: HiveCatalog,
) -> None:
    identifier = "default.hive_register_table"
    location = "s3a://warehouse/default/hive_register_table"
    tbl = _create_table(session_catalog, identifier, 2, location)
    assert session_catalog.table_exists(identifier=identifier)
    session_catalog.drop_table(identifier=identifier)
    assert not session_catalog.table_exists(identifier=identifier)
    session_catalog.register_table(("default", "hive_register_table"), metadata_location=tbl.metadata_location)
    assert session_catalog.table_exists(identifier=identifier)


@pytest.mark.integration
def test_hive_register_table_existing(
    session_catalog: HiveCatalog,
) -> None:
    identifier = "default.hive_register_table_existing"
    location = "s3a://warehouse/default/hive_register_table_existing"
    tbl = _create_table(session_catalog, identifier, 2, location)
    assert session_catalog.table_exists(identifier=identifier)
    # Assert that registering the table again raises TableAlreadyExistsError
    with pytest.raises(TableAlreadyExistsError):
        session_catalog.register_table(("default", "hive_register_table_existing"), metadata_location=tbl.metadata_location)


@pytest.mark.integration
def test_rest_register_table(
    session_catalog: Catalog,
) -> None:
    identifier = "default.rest_register_table"
    location = "s3a://warehouse/default/rest_register_table"
    tbl = _create_table(session_catalog, identifier, 2, location)
    assert session_catalog.table_exists(identifier=identifier)
    session_catalog.drop_table(identifier=identifier)
    assert not session_catalog.table_exists(identifier=identifier)
    session_catalog.register_table(identifier=identifier, metadata_location=tbl.metadata_location)
    assert session_catalog.table_exists(identifier=identifier)


@pytest.mark.integration
def test_rest_register_table_existing(
    session_catalog: Catalog,
) -> None:
    identifier = "default.rest_register_table_existing"
    location = "s3a://warehouse/default/rest_register_table_existing"
    tbl = _create_table(session_catalog, identifier, 2, location)
    assert session_catalog.table_exists(identifier=identifier)
    # Assert that registering the table again raises TableAlreadyExistsError
    with pytest.raises(TableAlreadyExistsError):
        session_catalog.register_table(identifier=identifier, metadata_location=tbl.metadata_location)
