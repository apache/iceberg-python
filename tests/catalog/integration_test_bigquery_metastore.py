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

import pytest
from pytest_mock import MockFixture

from pyiceberg.catalog.bigquery_metastore import BigQueryMetastoreCatalog
from pyiceberg.exceptions import NoSuchNamespaceError, NoSuchTableError
from pyiceberg.io import load_file_io
from pyiceberg.partitioning import UNPARTITIONED_PARTITION_SPEC
from pyiceberg.schema import Schema
from pyiceberg.serializers import ToOutputFile
from pyiceberg.table.metadata import new_table_metadata
from pyiceberg.table.sorting import UNSORTED_SORT_ORDER
from tests.conftest import BQ_TABLE_METADATA_LOCATION_REGEX


@pytest.mark.skipif(os.environ.get("GCP_CREDENTIALS") is None)
def test_create_table_with_database_location(
    mocker: MockFixture, _bucket_initialize: None, table_schema_nested: Schema, gcp_dataset_name: str, table_name: str
) -> None:
    mocker.patch.dict(os.environ, values={"PYICEBERG_LEGACY_CURRENT_SNAPSHOT_ID": "True"})

    catalog_name = "test_ddb_catalog"
    identifier = (gcp_dataset_name, table_name)
    test_catalog = BigQueryMetastoreCatalog(
        catalog_name,
        **{
            "gcp.bigquery.project-id": "alexstephen-test-1",
            "warehouse": "gs://alexstephen-test-bq-bucket/",
            "gcp.bigquery.credentials-info": os.environ["GCP_CREDENTIALS"],
        },
    )
    test_catalog.create_namespace(namespace=gcp_dataset_name)
    table = test_catalog.create_table(identifier, table_schema_nested)
    assert table.name() == identifier
    assert BQ_TABLE_METADATA_LOCATION_REGEX.match(table.metadata_location)

    tables_in_namespace = test_catalog.list_tables(namespace=gcp_dataset_name)
    assert identifier in tables_in_namespace


@pytest.mark.skipif(os.environ.get("GCP_CREDENTIALS") is None)
def test_drop_table_with_database_location(
    mocker: MockFixture, _bucket_initialize: None, table_schema_nested: Schema, gcp_dataset_name: str, table_name: str
) -> None:
    mocker.patch.dict(os.environ, values={"PYICEBERG_LEGACY_CURRENT_SNAPSHOT_ID": "True"})

    catalog_name = "test_ddb_catalog"
    identifier = (gcp_dataset_name, table_name)
    test_catalog = BigQueryMetastoreCatalog(
        catalog_name,
        **{
            "gcp.bigquery.project-id": "alexstephen-test-1",
            "warehouse": "gs://alexstephen-test-bq-bucket/",
            "gcp.bigquery.credentials-info": os.environ["GCP_CREDENTIALS"],
        },
    )
    test_catalog.create_namespace(namespace=gcp_dataset_name)
    test_catalog.create_table(identifier, table_schema_nested)
    test_catalog.drop_table(identifier)

    tables_in_namespace_after_drop = test_catalog.list_tables(namespace=gcp_dataset_name)
    assert identifier not in tables_in_namespace_after_drop

    # Expect that the table no longer exists.
    try:
        test_catalog.load_table(identifier)
        raise AssertionError()
    except NoSuchTableError:
        assert True


@pytest.mark.skipif(os.environ.get("GCP_CREDENTIALS") is None)
def test_create_and_drop_namespace(
    mocker: MockFixture, _bucket_initialize: None, table_schema_nested: Schema, gcp_dataset_name: str, table_name: str
) -> None:
    mocker.patch.dict(os.environ, values={"PYICEBERG_LEGACY_CURRENT_SNAPSHOT_ID": "True"})

    # Create namespace.
    catalog_name = "test_ddb_catalog"
    test_catalog = BigQueryMetastoreCatalog(
        catalog_name,
        **{
            "gcp.bigquery.project-id": "alexstephen-test-1",
            "warehouse": "gs://alexstephen-test-bq-bucket/",
            "gcp.bigquery.credentials-info": os.environ["GCP_CREDENTIALS"],
        },
    )
    test_catalog.create_namespace(namespace=gcp_dataset_name)

    # Ensure that the namespace exists.
    namespaces = test_catalog.list_namespaces()
    assert (gcp_dataset_name,) in namespaces

    # Drop the namespace and ensure it does not exist.
    test_catalog.drop_namespace(namespace=gcp_dataset_name)
    namespaces_after_drop = test_catalog.list_namespaces()
    assert (gcp_dataset_name,) not in namespaces_after_drop

    # Verify with load_namespace_properties as well
    with pytest.raises(NoSuchNamespaceError):
        test_catalog.load_namespace_properties(gcp_dataset_name)


@pytest.mark.skipif(os.environ.get("GCP_CREDENTIALS") is None)
def test_register_table(
    mocker: MockFixture, _bucket_initialize: None, table_schema_nested: Schema, gcp_dataset_name: str, table_name: str
) -> None:
    mocker.patch.dict(os.environ, values={"PYICEBERG_LEGACY_CURRENT_SNAPSHOT_ID": "True"})

    catalog_name = "test_bq_register_catalog"
    identifier = (gcp_dataset_name, table_name)
    warehouse_path = "gs://alexstephen-test-bq-bucket/"  # Matches conftest BUCKET_NAME for GCS interaction

    test_catalog = BigQueryMetastoreCatalog(
        catalog_name,
        **{
            "gcp.bigquery.project-id": "alexstephen-test-1",
            "warehouse": "gs://alexstephen-test-bq-bucket/",
            "gcp.bigquery.credentials-info": os.environ["GCP_CREDENTIALS"],
        },
    )

    test_catalog.create_namespace(namespace=gcp_dataset_name)

    # Manually create a metadata file in GCS
    table_gcs_location = f"{warehouse_path.rstrip('/')}/{gcp_dataset_name}.db/{table_name}"
    # Construct a unique metadata file name similar to how pyiceberg would
    metadata_file_name = "00000-aaaaaaaa-aaaa-4aaa-aaaa-aaaaaaaaaaaa.metadata.json"
    metadata_gcs_path = f"{table_gcs_location}/metadata/{metadata_file_name}"

    metadata = new_table_metadata(
        location=table_gcs_location,
        schema=table_schema_nested,
        properties={},
        partition_spec=UNPARTITIONED_PARTITION_SPEC,
        sort_order=UNSORTED_SORT_ORDER,
    )
    io = load_file_io(properties=test_catalog.properties, location=metadata_gcs_path)
    test_catalog._write_metadata(metadata, io, metadata_gcs_path)
    ToOutputFile.table_metadata(metadata, io.new_output(metadata_gcs_path), overwrite=True)

    # Register the table
    registered_table = test_catalog.register_table(identifier, metadata_gcs_path)

    assert registered_table.name() == identifier
    assert registered_table.metadata_location == metadata_gcs_path
    assert registered_table.metadata.location == table_gcs_location
    assert BQ_TABLE_METADATA_LOCATION_REGEX.match(registered_table.metadata_location)

    # Verify table exists and is loadable
    loaded_table = test_catalog.load_table(identifier)
    assert loaded_table.name() == registered_table.name()
    assert loaded_table.metadata_location == metadata_gcs_path

    # Clean up
    test_catalog.drop_table(identifier)
    test_catalog.drop_namespace(gcp_dataset_name)
