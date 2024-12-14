import boto3
from pyiceberg.schema import Schema
import pytest

from pyiceberg.catalog.s3tables import S3TableCatalog
from pyiceberg.exceptions import TableBucketNotFound


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


def test_create_table(table_bucket_arn, database_name: str, table_name:str, table_schema_nested: Schema):
    # setting FileIO to FsspecFileIO explicitly is required as pyarrwo does not work with S3 Table Buckets yet
    properties = {"warehouse": table_bucket_arn, "py-io-impl": "pyiceberg.io.fsspec.FsspecFileIO"}
    catalog = S3TableCatalog(name="test_s3tables_catalog", **properties)
    identifier = (database_name, table_name)

    catalog.create_namespace(namespace=database_name)
    table = catalog.create_table(identifier=identifier, schema=table_schema_nested)

    assert table == catalog.load_table(identifier)
