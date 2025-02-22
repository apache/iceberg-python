#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.
import string
from random import choice

import boto3
import pytest

from pyiceberg.catalog import load_catalog
from pyiceberg.catalog.s3tables import S3TablesCatalog
from pyiceberg.exceptions import NoSuchNamespaceError, NoSuchTableError, TableBucketNotFound
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import IdentityTransform
from pyiceberg.types import IntegerType


@pytest.fixture
def database_name(database_name: str) -> str:
    # naming rules prevent "-" in namespaces for s3 table buckets
    return database_name.replace("-", "_")


@pytest.fixture
def table_name(table_name: str) -> str:
    # naming rules prevent "-" in table namees for s3 table buckets
    return table_name.replace("-", "_")


@pytest.fixture()
def aws_region(_aws_credentials: None) -> str:
    return "us-east-1"


@pytest.fixture
def table_bucket_arn(monkeypatch: pytest.MonkeyPatch, moto_endpoint_url: str, aws_region: str) -> str:
    monkeypatch.setenv("AWS_ENDPOINT_URL", moto_endpoint_url)

    prefix = "pyiceberg-table-bucket-"
    random_tag = "".join(choice(string.ascii_letters) for _ in range(12))
    name = (prefix + random_tag).lower()
    table_bucket_arn = boto3.client("s3tables", endpoint_url=moto_endpoint_url, region_name=aws_region).create_table_bucket(
        name=name
    )["arn"]
    return table_bucket_arn


@pytest.fixture(params=["pyiceberg.io.fsspec.FsspecFileIO", "pyiceberg.io.pyarrow.PyArrowFileIO"])
def file_io_impl(request: pytest.FixtureRequest) -> str:
    return request.param


@pytest.fixture
def catalog(table_bucket_arn: str, aws_region: str, moto_endpoint_url: str, file_io_impl: str) -> S3TablesCatalog:
    properties = {
        "s3tables.warehouse": table_bucket_arn,
        "s3tables.region": aws_region,
        "py-io-impl": file_io_impl,
        "s3tables.endpoint": moto_endpoint_url,
        "s3.endpoint": moto_endpoint_url,
    }
    return S3TablesCatalog(name="test_s3tables_catalog", **properties)


def test_load_catalog(table_bucket_arn: str, aws_region: str, moto_endpoint_url: str) -> None:
    properties = {
        "type": "s3tables",
        "s3tables.warehouse": table_bucket_arn,
        "s3tables.region": aws_region,
        "py-io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO",
        "s3tables.endpoint": moto_endpoint_url,
    }
    catalog = load_catalog(**properties)
    assert isinstance(catalog, S3TablesCatalog)


def test_creating_catalog_validates_s3_table_bucket_exists(table_bucket_arn: str, aws_region: str) -> None:
    properties = {"s3tables.warehouse": f"{table_bucket_arn}-modified", "s3tables.region": aws_region}
    with pytest.raises(TableBucketNotFound):
        S3TablesCatalog(name="test_s3tables_catalog", **properties)


def test_create_namespace(catalog: S3TablesCatalog, database_name: str) -> None:
    catalog.create_namespace(namespace=database_name)
    namespaces = catalog.list_namespaces()
    assert (database_name,) in namespaces


def test_load_namespace_properties(catalog: S3TablesCatalog, database_name: str) -> None:
    catalog.create_namespace(namespace=database_name)
    assert database_name in catalog.load_namespace_properties(database_name)["namespace"]


def test_load_namespace_properties_for_invalid_namespace_raises_exception(catalog: S3TablesCatalog, database_name: str) -> None:
    with pytest.raises(NoSuchNamespaceError):
        catalog.load_namespace_properties(database_name)


def test_drop_namespace(catalog: S3TablesCatalog, database_name: str) -> None:
    catalog.create_namespace(namespace=database_name)
    assert (database_name,) in catalog.list_namespaces()
    catalog.drop_namespace(namespace=database_name)
    assert (database_name,) not in catalog.list_namespaces()


def test_create_table(catalog: S3TablesCatalog, database_name: str, table_name: str, table_schema_nested: Schema) -> None:
    identifier = (database_name, table_name)

    catalog.create_namespace(namespace=database_name)
    table = catalog.create_table(identifier=identifier, schema=table_schema_nested)

    assert table == catalog.load_table(identifier)


def test_create_table_in_invalid_namespace_raises_exception(
    catalog: S3TablesCatalog, database_name: str, table_name: str, table_schema_nested: Schema
) -> None:
    identifier = (database_name, table_name)

    with pytest.raises(NoSuchNamespaceError):
        catalog.create_table(identifier=identifier, schema=table_schema_nested)


def test_table_exists(catalog: S3TablesCatalog, database_name: str, table_name: str, table_schema_nested: Schema) -> None:
    identifier = (database_name, table_name)

    catalog.create_namespace(namespace=database_name)
    assert not catalog.table_exists(identifier=identifier)
    catalog.create_table(identifier=identifier, schema=table_schema_nested)
    assert catalog.table_exists(identifier=identifier)


def test_rename_table(catalog: S3TablesCatalog, database_name: str, table_name: str, table_schema_nested: Schema) -> None:
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


def test_list_tables(catalog: S3TablesCatalog, database_name: str, table_name: str, table_schema_nested: Schema) -> None:
    identifier = (database_name, table_name)

    catalog.create_namespace(namespace=database_name)
    assert not catalog.list_tables(namespace=database_name)
    catalog.create_table(identifier=identifier, schema=table_schema_nested)
    assert catalog.list_tables(namespace=database_name)


def test_list_tables_for_invalid_namespace_raises_exception(catalog: S3TablesCatalog, database_name: str) -> None:
    with pytest.raises(NoSuchNamespaceError):
        catalog.list_tables(namespace=database_name)


def test_drop_table(catalog: S3TablesCatalog, database_name: str, table_name: str, table_schema_nested: Schema) -> None:
    identifier = (database_name, table_name)

    catalog.create_namespace(namespace=database_name)
    catalog.create_table(identifier=identifier, schema=table_schema_nested)

    catalog.drop_table(identifier=identifier)

    with pytest.raises(NoSuchTableError):
        catalog.load_table(identifier=identifier)


def test_commit_new_column_to_table(
    catalog: S3TablesCatalog, database_name: str, table_name: str, table_schema_nested: Schema
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


def test_write_pyarrow_table(catalog: S3TablesCatalog, database_name: str, table_name: str) -> None:
    identifier = (database_name, table_name)
    catalog.create_namespace(namespace=database_name)

    import pyarrow as pa

    pyarrow_table = pa.Table.from_arrays(
        [
            pa.array([None, "A", "B", "C"]),  # 'foo' column
            pa.array([1, 2, 3, 4]),  # 'bar' column
            pa.array([True, None, False, True]),  # 'baz' column
            pa.array([None, "A", "B", "C"]),  # 'large' column
        ],
        schema=pa.schema(
            [
                pa.field("foo", pa.large_string(), nullable=True),
                pa.field("bar", pa.int32(), nullable=False),
                pa.field("baz", pa.bool_(), nullable=True),
                pa.field("large", pa.large_string(), nullable=True),
            ]
        ),
    )
    table = catalog.create_table(identifier=identifier, schema=pyarrow_table.schema)
    table.append(pyarrow_table)

    assert table.scan().to_arrow().num_rows == pyarrow_table.num_rows


def test_commit_new_data_to_table(catalog: S3TablesCatalog, database_name: str, table_name: str) -> None:
    identifier = (database_name, table_name)
    catalog.create_namespace(namespace=database_name)

    import pyarrow as pa

    pyarrow_table = pa.Table.from_arrays(
        [
            pa.array([None, "A", "B", "C"]),  # 'foo' column
            pa.array([1, 2, 3, 4]),  # 'bar' column
            pa.array([True, None, False, True]),  # 'baz' column
            pa.array([None, "A", "B", "C"]),  # 'large' column
        ],
        schema=pa.schema(
            [
                pa.field("foo", pa.large_string(), nullable=True),
                pa.field("bar", pa.int32(), nullable=False),
                pa.field("baz", pa.bool_(), nullable=True),
                pa.field("large", pa.large_string(), nullable=True),
            ]
        ),
    )

    table = catalog.create_table(identifier=identifier, schema=pyarrow_table.schema)
    table.append(pyarrow_table)

    row_count = table.scan().to_arrow().num_rows
    assert row_count
    last_updated_ms = table.metadata.last_updated_ms
    original_table_metadata_location = table.metadata_location
    original_table_last_updated_ms = table.metadata.last_updated_ms

    transaction = table.transaction()
    transaction.append(table.scan().to_arrow())
    transaction.commit_transaction()

    updated_table_metadata = table.metadata
    assert updated_table_metadata.last_updated_ms > last_updated_ms
    assert updated_table_metadata.metadata_log[-1].metadata_file == original_table_metadata_location
    assert updated_table_metadata.metadata_log[-1].timestamp_ms == original_table_last_updated_ms
    assert table.scan().to_arrow().num_rows == 2 * row_count


@pytest.mark.xfail(raises=NotImplementedError)
def test_create_table_transaction(
    catalog: S3TablesCatalog, database_name: str, table_name: str, table_schema_nested: str
) -> None:
    identifier = (database_name, table_name)
    catalog.create_namespace(namespace=database_name)

    with catalog.create_table_transaction(
        identifier,
        table_schema_nested,
        partition_spec=PartitionSpec(PartitionField(source_id=1, field_id=1000, transform=IdentityTransform(), name="foo")),
    ) as txn:
        last_updated_metadata = txn.table_metadata.last_updated_ms
        with txn.update_schema() as update_schema:
            update_schema.add_column(path="b", field_type=IntegerType())

        with txn.update_spec() as update_spec:
            update_spec.add_identity("bar")

        txn.set_properties(test_a="test_aa", test_b="test_b", test_c="test_c")

    table = catalog.load_table(identifier)

    assert table.schema().find_field("b").field_type == IntegerType()
    assert table.properties == {"test_a": "test_aa", "test_b": "test_b", "test_c": "test_c"}
    assert table.spec().last_assigned_field_id == 1001
    assert table.spec().fields_by_source_id(1)[0].name == "foo"
    assert table.spec().fields_by_source_id(1)[0].field_id == 1000
    assert table.spec().fields_by_source_id(1)[0].transform == IdentityTransform()
    assert table.spec().fields_by_source_id(2)[0].name == "bar"
    assert table.spec().fields_by_source_id(2)[0].field_id == 1001
    assert table.spec().fields_by_source_id(2)[0].transform == IdentityTransform()
    assert table.metadata.last_updated_ms > last_updated_metadata
