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
import os
from unittest.mock import MagicMock

from google.api_core.exceptions import NotFound
from google.cloud.bigquery import Dataset, DatasetReference, Table, TableReference
from google.cloud.bigquery.external_config import ExternalCatalogDatasetOptions, ExternalCatalogTableOptions
from pytest_mock import MockFixture

from pyiceberg.catalog.bigquery_metastore import ICEBERG_TABLE_TYPE_VALUE, TABLE_TYPE_PROP, BigQueryMetastoreCatalog
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.schema import Schema


def dataset_mock() -> Dataset:
    d = Dataset(DatasetReference(dataset_id="my-dataset", project="my-project"))
    d.external_catalog_dataset_options = ExternalCatalogDatasetOptions(
        default_storage_location_uri="gs://test-bucket/iceberg-dataset"
    )
    return d


def table_mock() -> Table:
    t = Table(TableReference(dataset_ref=DatasetReference(dataset_id="my-dataset", project="my-project"), table_id="my-table"))
    t.external_catalog_table_options = ExternalCatalogTableOptions(
        parameters={
            "metadata_location": "gs://alexstephen-test-bq-bucket/my_iceberg_database_aaaaaaaaaaaaaaaaaaaa.db/my_iceberg_table-bbbbbbbbbbbbbbbbbbbb/metadata/12343-aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa.metadata",
            TABLE_TYPE_PROP: ICEBERG_TABLE_TYPE_VALUE,
        }
    )
    return t


def test_create_table_with_database_location(
    mocker: MockFixture, _bucket_initialize: None, table_schema_nested: Schema, gcp_dataset_name: str, table_name: str
) -> None:
    # Setup mocks for GCP.
    client_mock = MagicMock()
    client_mock.get_dataset.return_value = dataset_mock()
    client_mock.get_table.return_value = table_mock()

    # Setup mocks for GCS.
    file_mock = MagicMock()

    mocker.patch("pyiceberg.catalog.bigquery_metastore.Client", return_value=client_mock)
    mocker.patch("pyiceberg.catalog.bigquery_metastore.FromInputFile.table_metadata", return_value=file_mock)
    mocker.patch.dict(os.environ, values={"PYICEBERG_LEGACY_CURRENT_SNAPSHOT_ID": "True"})
    mocker.patch("pyiceberg.catalog.ToOutputFile.table_metadata", return_value=None)

    catalog_name = "test_ddb_catalog"
    identifier = (gcp_dataset_name, table_name)
    test_catalog = BigQueryMetastoreCatalog(
        catalog_name, **{"gcp.bigquery.project-id": "alexstephen-test-1", "warehouse": "gs://alexstephen-test-bq-bucket/"}
    )
    test_catalog.create_namespace(namespace=gcp_dataset_name)
    table = test_catalog.create_table(identifier, table_schema_nested)
    assert table.name() == identifier


def test_drop_table_with_database_location(
    mocker: MockFixture, _bucket_initialize: None, table_schema_nested: Schema, gcp_dataset_name: str, table_name: str
) -> None:
    # Setup mocks for GCP.
    client_mock = MagicMock()
    client_mock.get_dataset.return_value = dataset_mock()
    client_mock.get_table.return_value = table_mock()

    # Setup mocks for GCS.
    file_mock = MagicMock()

    mocker.patch("pyiceberg.catalog.bigquery_metastore.Client", return_value=client_mock)
    mocker.patch("pyiceberg.catalog.bigquery_metastore.FromInputFile.table_metadata", return_value=file_mock)
    mocker.patch.dict(os.environ, values={"PYICEBERG_LEGACY_CURRENT_SNAPSHOT_ID": "True"})
    mocker.patch("pyiceberg.catalog.ToOutputFile.table_metadata", return_value=None)

    catalog_name = "test_ddb_catalog"
    identifier = (gcp_dataset_name, table_name)
    test_catalog = BigQueryMetastoreCatalog(
        catalog_name, **{"gcp.bigquery.project-id": "alexstephen-test-1", "warehouse": "gs://alexstephen-test-bq-bucket/"}
    )
    test_catalog.create_namespace(namespace=gcp_dataset_name)
    test_catalog.create_table(identifier, table_schema_nested)
    test_catalog.drop_table(identifier)

    client_mock.get_table.side_effect = NotFound("Table Not Found")
    mocker.patch("pyiceberg.catalog.bigquery_metastore.Client", return_value=client_mock)

    # Expect that the table no longer exists.
    try:
        test_catalog.load_table(identifier)
        raise AssertionError()
    except NoSuchTableError:
        assert True


def test_drop_namespace(mocker: MockFixture, gcp_dataset_name: str) -> None:
    client_mock = MagicMock()
    mocker.patch("pyiceberg.catalog.bigquery_metastore.Client", return_value=client_mock)
    mocker.patch.dict(os.environ, values={"PYICEBERG_LEGACY_CURRENT_SNAPSHOT_ID": "True"})

    catalog_name = "test_catalog"
    test_catalog = BigQueryMetastoreCatalog(catalog_name, **{"gcp.bigquery.project-id": "alexstephen-test-1"})

    test_catalog.drop_namespace(gcp_dataset_name)
    client_mock.delete_dataset.assert_called_once()
    args, _ = client_mock.delete_dataset.call_args
    assert isinstance(args[0], Dataset)
    assert args[0].dataset_id == gcp_dataset_name


def test_list_tables(mocker: MockFixture, gcp_dataset_name: str) -> None:
    client_mock = MagicMock()

    # Mock list_tables to return an iterator of TableListItem
    table_list_item_1 = MagicMock()
    table_list_item_1.table_id = "iceberg_table_A"
    table_list_item_1.reference = TableReference(
        dataset_ref=DatasetReference(project="my-project", dataset_id=gcp_dataset_name), table_id="iceberg_table_A"
    )

    table_list_item_2 = MagicMock()
    table_list_item_2.table_id = "iceberg_table_B"
    table_list_item_2.reference = TableReference(
        dataset_ref=DatasetReference(project="my-project", dataset_id=gcp_dataset_name), table_id="iceberg_table_B"
    )

    client_mock.list_tables.return_value = iter([table_list_item_1, table_list_item_2])

    # Mock get_table to always return a table that is considered an Iceberg table.
    # The table_mock() function already creates a table with the necessary Iceberg properties.
    client_mock.get_table.return_value = table_mock()

    mocker.patch("pyiceberg.catalog.bigquery_metastore.Client", return_value=client_mock)
    mocker.patch.dict(os.environ, values={"PYICEBERG_LEGACY_CURRENT_SNAPSHOT_ID": "True"})

    catalog_name = "test_catalog"
    test_catalog = BigQueryMetastoreCatalog(catalog_name, **{"gcp.bigquery.project-id": "my-project"})

    tables = test_catalog.list_tables(gcp_dataset_name)

    # Assert that all tables returned by client.list_tables are listed
    assert len(tables) == 2
    assert (gcp_dataset_name, "iceberg_table_A") in tables
    assert (gcp_dataset_name, "iceberg_table_B") in tables

    client_mock.list_tables.assert_called_once_with(dataset=DatasetReference(project="my-project", dataset_id=gcp_dataset_name))


def test_list_namespaces(mocker: MockFixture) -> None:
    client_mock = MagicMock()
    dataset_item_1 = Dataset(DatasetReference(project="my-project", dataset_id="dataset1"))
    dataset_item_2 = Dataset(DatasetReference(project="my-project", dataset_id="dataset2"))
    client_mock.list_datasets.return_value = iter([dataset_item_1, dataset_item_2])

    mocker.patch("pyiceberg.catalog.bigquery_metastore.Client", return_value=client_mock)
    mocker.patch.dict(os.environ, values={"PYICEBERG_LEGACY_CURRENT_SNAPSHOT_ID": "True"})

    catalog_name = "test_catalog"
    test_catalog = BigQueryMetastoreCatalog(catalog_name, **{"gcp.bigquery.project-id": "my-project"})

    namespaces = test_catalog.list_namespaces()
    assert len(namespaces) == 2
    assert ("dataset1",) in namespaces
    assert ("dataset2",) in namespaces
    client_mock.list_datasets.assert_called_once()
