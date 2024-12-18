import uuid

import boto3
import pytest

from pyiceberg.catalog.s3tables import S3TableCatalog
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.exceptions import TableBucketNotFound
from pyiceberg.schema import Schema
from pyiceberg.types import IntegerType


@pytest.fixture
def database_name(database_name):
    # naming rules prevent "-" in namespaces for s3 table buckets
    return database_name.replace("-", "_")


@pytest.fixture
def table_name(table_name):
    # naming rules prevent "-" in table namees for s3 table buckets
    return table_name.replace("-", "_")


@pytest.fixture
def table_bucket_arn():
    import os

    # since the moto library does not support s3tables as of 2024-12-14 we have to test against a real AWS endpoint
    # in one of the supported regions.

    return os.environ["ARN"]


def test_s3tables_api_raises_on_conflicting_version_tokens(table_bucket_arn, database_name, table_name):
    client = boto3.client("s3tables")
    client.create_namespace(tableBucketARN=table_bucket_arn, namespace=[database_name])
    response = client.create_table(
        tableBucketARN=table_bucket_arn, namespace=database_name, name=table_name, format="ICEBERG"
    )
    version_token = response["versionToken"]
    scrambled_version_token = version_token[::-1]

    warehouse_location = client.get_table(tableBucketARN=table_bucket_arn, namespace=database_name, name=table_name)[
        "warehouseLocation"
    ]
    metadata_location = f"{warehouse_location}/metadata/00001-{uuid.uuid4()}.metadata.json"

    with pytest.raises(client.exceptions.ConflictException):
        client.update_table_metadata_location(
            tableBucketARN=table_bucket_arn,
            namespace=database_name,
            name=table_name,
            versionToken=scrambled_version_token,
            metadataLocation=metadata_location,
        )


def test_creating_catalog_validates_s3_table_bucket_exists(table_bucket_arn):
    properties = {"warehouse": f"{table_bucket_arn}-modified"}
    with pytest.raises(TableBucketNotFound):
        S3TableCatalog(name="test_s3tables_catalog", **properties)


def test_create_namespace(table_bucket_arn, database_name: str):
    properties = {"warehouse": table_bucket_arn}
    catalog = S3TableCatalog(name="test_s3tables_catalog", **properties)
    catalog.create_namespace(namespace=database_name)
    namespaces = catalog.list_namespaces()
    assert (database_name,) in namespaces


def test_drop_namespace(table_bucket_arn, database_name: str):
    properties = {"warehouse": table_bucket_arn}
    catalog = S3TableCatalog(name="test_s3tables_catalog", **properties)
    catalog.create_namespace(namespace=database_name)
    assert (database_name,) in catalog.list_namespaces()
    catalog.drop_namespace(namespace=database_name)
    assert (database_name,) not in catalog.list_namespaces()


def test_create_table(table_bucket_arn, database_name: str, table_name: str, table_schema_nested: Schema):
    # setting FileIO to FsspecFileIO explicitly is required as pyarrwo does not work with S3 Table Buckets yet
    properties = {"warehouse": table_bucket_arn, "py-io-impl": "pyiceberg.io.fsspec.FsspecFileIO"}
    catalog = S3TableCatalog(name="test_s3tables_catalog", **properties)
    identifier = (database_name, table_name)

    catalog.create_namespace(namespace=database_name)
    table = catalog.create_table(identifier=identifier, schema=table_schema_nested)

    assert table == catalog.load_table(identifier)


def test_table_exists(table_bucket_arn, database_name: str, table_name: str, table_schema_nested: Schema):
    # setting FileIO to FsspecFileIO explicitly is required as pyarrwo does not work with S3 Table Buckets yet
    properties = {"warehouse": table_bucket_arn, "py-io-impl": "pyiceberg.io.fsspec.FsspecFileIO"}
    catalog = S3TableCatalog(name="test_s3tables_catalog", **properties)
    identifier = (database_name, table_name)

    catalog.create_namespace(namespace=database_name)
    assert not catalog.table_exists(identifier=identifier)
    catalog.create_table(identifier=identifier, schema=table_schema_nested)
    assert catalog.table_exists(identifier=identifier)


def test_drop_table(table_bucket_arn, database_name: str, table_name: str, table_schema_nested: Schema):
    # setting FileIO to FsspecFileIO explicitly is required as pyarrwo does not work with S3 Table Buckets yet
    properties = {"warehouse": table_bucket_arn, "py-io-impl": "pyiceberg.io.fsspec.FsspecFileIO"}
    catalog = S3TableCatalog(name="test_s3tables_catalog", **properties)
    identifier = (database_name, table_name)

    catalog.create_namespace(namespace=database_name)
    catalog.create_table(identifier=identifier, schema=table_schema_nested)

    catalog.drop_table(identifier=identifier)

    with pytest.raises(NoSuchTableError):
        catalog.load_table(identifier=identifier)


def test_commit_table(table_bucket_arn, database_name: str, table_name: str, table_schema_nested: Schema):
    # setting FileIO to FsspecFileIO explicitly is required as pyarrwo does not work with S3 Table Buckets yet
    properties = {"warehouse": table_bucket_arn, "py-io-impl": "pyiceberg.io.fsspec.FsspecFileIO"}
    catalog = S3TableCatalog(name="test_s3tables_catalog", **properties)
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
