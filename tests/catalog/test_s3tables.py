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


def test_s3tables_boto_api(table_bucket_arn):
    client = boto3.client("s3tables")
    response = client.list_namespaces(tableBucketARN=table_bucket_arn)
    print(response["namespaces"])

    response = client.get_table_bucket(tableBucketARN=table_bucket_arn + "abc")
    print(response)



def test_s3tables_namespaces_api(table_bucket_arn):
    client = boto3.client("s3tables")
    response = client.create_namespace(tableBucketARN=table_bucket_arn, namespace=["one", "two"])
    print(response)
    response = client.list_namespaces(tableBucketARN=table_bucket_arn)
    print(response)

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
    properties = {"warehouse": table_bucket_arn}
    catalog = S3TableCatalog(name="test_s3tables_catalog", **properties)
    identifier = (database_name, table_name)

    catalog.create_namespace(namespace=database_name)
    print(database_name, table_name)
    # this fails with
    # OSError: When completing multiple part upload for key 'metadata/00000-55a9c37c-b822-4a81-ac0e-1efbcd145dba.metadata.json' in bucket '14e4e036-d4ae-44f8-koana45eruw
    # Uunable to parse ExceptionName: S3TablesUnsupportedHeader Message: S3 Tables does not support the following header: x-amz-api-version value: 2006-03-01
    table = catalog.create_table(identifier=identifier, schema=table_schema_nested)
    print(table)
