import pytest

from pyiceberg.catalog.s3tables import S3TableCatalog
from pyiceberg.exceptions import NoSuchNamespaceError, NoSuchTableError, TableBucketNotFound
from pyiceberg.schema import Schema
from pyiceberg.types import IntegerType


@pytest.fixture
def database_name(database_name: str) -> str:
    # naming rules prevent "-" in namespaces for s3 table buckets
    return database_name.replace("-", "_")


@pytest.fixture
def table_name(table_name: str) -> str:
    # naming rules prevent "-" in table namees for s3 table buckets
    return table_name.replace("-", "_")


@pytest.fixture
def table_bucket_arn() -> str:
    import os

    # since the moto library does not support s3tables as of 2024-12-14 we have to test against a real AWS endpoint
    # in one of the supported regions.

    return os.environ["ARN"]


@pytest.fixture
def catalog(table_bucket_arn: str) -> S3TableCatalog:
    properties = {"s3tables.table-bucket-arn": table_bucket_arn, "s3tables.region": "us-east-1", "s3.region": "us-east-1"}
    return S3TableCatalog(name="test_s3tables_catalog", **properties)


def test_creating_catalog_validates_s3_table_bucket_exists(table_bucket_arn: str) -> None:
    properties = {"s3tables.table-bucket-arn": f"{table_bucket_arn}-modified", "s3tables.region": "us-east-1"}
    with pytest.raises(TableBucketNotFound):
        S3TableCatalog(name="test_s3tables_catalog", **properties)


def test_create_namespace(catalog: S3TableCatalog, database_name: str) -> None:
    catalog.create_namespace(namespace=database_name)
    namespaces = catalog.list_namespaces()
    assert (database_name,) in namespaces


def test_load_namespace_properties(catalog: S3TableCatalog, database_name: str) -> None:
    catalog.create_namespace(namespace=database_name)
    assert database_name in catalog.load_namespace_properties(database_name)["namespace"]


def test_drop_namespace(catalog: S3TableCatalog, database_name: str) -> None:
    catalog.create_namespace(namespace=database_name)
    assert (database_name,) in catalog.list_namespaces()
    catalog.drop_namespace(namespace=database_name)
    assert (database_name,) not in catalog.list_namespaces()


def test_create_table(catalog: S3TableCatalog, database_name: str, table_name: str, table_schema_nested: Schema) -> None:
    identifier = (database_name, table_name)

    catalog.create_namespace(namespace=database_name)
    table = catalog.create_table(identifier=identifier, schema=table_schema_nested)

    assert table == catalog.load_table(identifier)


def test_create_table_in_invalid_namespace_raises_exception(
    catalog: S3TableCatalog, database_name: str, table_name: str, table_schema_nested: Schema
) -> None:
    identifier = (database_name, table_name)

    with pytest.raises(NoSuchNamespaceError):
        catalog.create_table(identifier=identifier, schema=table_schema_nested)


def test_table_exists(catalog: S3TableCatalog, database_name: str, table_name: str, table_schema_nested: Schema) -> None:
    identifier = (database_name, table_name)

    catalog.create_namespace(namespace=database_name)
    assert not catalog.table_exists(identifier=identifier)
    catalog.create_table(identifier=identifier, schema=table_schema_nested)
    assert catalog.table_exists(identifier=identifier)


def test_rename_table(catalog: S3TableCatalog, database_name: str, table_name: str, table_schema_nested: Schema) -> None:
    identifier = (database_name, table_name)

    catalog.create_namespace(namespace=database_name)
    catalog.create_table(identifier=identifier, schema=table_schema_nested)

    to_database_name = f"{database_name}new"
    to_table_name = f"{table_name}new"
    to_identifier = (to_database_name, to_table_name)
    catalog.create_namespace(namespace=to_database_name)
    catalog.rename_table(from_identifier=identifier, to_identifier=to_identifier)

    assert not catalog.table_exists(identifier=identifier)
    assert catalog.table_exists(identifier=to_identifier)


def test_list_tables(catalog: S3TableCatalog, database_name: str, table_name: str, table_schema_nested: Schema) -> None:
    identifier = (database_name, table_name)

    catalog.create_namespace(namespace=database_name)
    assert not catalog.list_tables(namespace=database_name)
    catalog.create_table(identifier=identifier, schema=table_schema_nested)
    assert catalog.list_tables(namespace=database_name)


def test_drop_table(catalog: S3TableCatalog, database_name: str, table_name: str, table_schema_nested: Schema) -> None:
    identifier = (database_name, table_name)

    catalog.create_namespace(namespace=database_name)
    catalog.create_table(identifier=identifier, schema=table_schema_nested)

    catalog.drop_table(identifier=identifier)

    with pytest.raises(NoSuchTableError):
        catalog.load_table(identifier=identifier)


def test_commit_new_column_to_table(
    catalog: S3TableCatalog, database_name: str, table_name: str, table_schema_nested: Schema
) -> None:
    identifier = (database_name, table_name)

    catalog.create_namespace(namespace=database_name)
    table = catalog.create_table(identifier=identifier, schema=table_schema_nested)

    last_updated_ms = table.metadata.last_updated_ms
    original_table_metadata_location = table.metadata_location
    original_table_last_updated_ms = table.metadata.last_updated_ms

    transaction = table.transaction()
    update = transaction.update_schema()
    update.add_column(path="b", field_type=IntegerType())
    update.commit()
    transaction.commit_transaction()

    updated_table_metadata = table.metadata
    assert updated_table_metadata.current_schema_id == 1
    assert len(updated_table_metadata.schemas) == 2
    assert updated_table_metadata.last_updated_ms > last_updated_ms
    assert updated_table_metadata.metadata_log[0].metadata_file == original_table_metadata_location
    assert updated_table_metadata.metadata_log[0].timestamp_ms == original_table_last_updated_ms
    assert table.schema().columns[-1].name == "b"


def test_commit_new_data_to_table(
    catalog: S3TableCatalog, database_name: str, table_name: str, table_schema_nested: Schema
) -> None:
    identifier = (database_name, table_name)

    catalog.create_namespace(namespace=database_name)
    table = catalog.create_table(identifier=identifier, schema=table_schema_nested)

    row_count = table.scan().to_arrow().num_rows
    last_updated_ms = table.metadata.last_updated_ms
    original_table_metadata_location = table.metadata_location
    original_table_last_updated_ms = table.metadata.last_updated_ms

    transaction = table.transaction()
    transaction.append(table.scan().to_arrow())
    transaction.commit_transaction()

    updated_table_metadata = table.metadata
    assert updated_table_metadata.last_updated_ms > last_updated_ms
    assert updated_table_metadata.metadata_log[0].metadata_file == original_table_metadata_location
    assert updated_table_metadata.metadata_log[0].timestamp_ms == original_table_last_updated_ms
    assert table.scan().to_arrow().num_rows == 2 * row_count
