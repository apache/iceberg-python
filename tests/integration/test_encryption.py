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
import boto3
import pyarrow.parquet as pq
import pytest
from pyspark.sql import SparkSession

from pyiceberg.catalog import Catalog
from pyiceberg.encryption.aws_kms import KMS_ENDPOINT, AwsKeyManagementClient
from pyiceberg.io import AWS_ACCESS_KEY_ID, AWS_REGION, AWS_SECRET_ACCESS_KEY
from pyiceberg.table import Table

# AES-128 and AES-256 data keys
DATA_KEY_LENGTHS = [16, 32]

ROWS = [
    {"id": 1, "category": "a", "payload": "alpha"},
    {"id": 2, "category": "b", "payload": None},
    {"id": 3, "category": "a", "payload": "gamma"},
    {"id": 4, "category": "b", "payload": "delta"},
]
EXTRA_ROWS = [
    {"id": 5, "category": "a", "payload": "epsilon"},
    {"id": 6, "category": "b", "payload": "zeta"},
]


@pytest.mark.integration
@pytest.mark.parametrize("data_key_length", DATA_KEY_LENGTHS)
def test_scan_encrypted_table(spark: SparkSession, session_catalog_hive: Catalog, data_key_length: int) -> None:
    identifier = "default.test_scan_encrypted_table"
    _create_encrypted_table(spark, identifier, data_key_length)
    spark.createDataFrame(ROWS).writeTo(f"hive.{identifier}").append()
    # Delete a row from each partition, so Spark writes a deletion vector per partition
    spark.sql(f"DELETE FROM hive.{identifier} WHERE id IN (1, 2)")
    table = _load_table(session_catalog_hive, identifier)

    expected = [{"id": 3, "category": "a", "payload": "gamma"}, {"id": 4, "category": "b", "payload": "delta"}]
    scan = table.scan()
    assert scan.to_arrow().sort_by("id").to_pylist() == expected
    assert scan.to_arrow_batch_reader().read_all().sort_by("id").to_pylist() == expected
    assert scan.count() == len(expected)

    filtered = table.scan(row_filter="category == 'a'", selected_fields=("id", "payload")).to_arrow()
    assert filtered.to_pylist() == [{"id": 3, "payload": "gamma"}]


@pytest.mark.integration
@pytest.mark.parametrize("data_key_length", DATA_KEY_LENGTHS)
def test_scan_encrypted_table_snapshots(spark: SparkSession, session_catalog_hive: Catalog, data_key_length: int) -> None:
    identifier = "default.test_scan_encrypted_table_snapshots"
    _create_encrypted_table(spark, identifier, data_key_length)
    spark.createDataFrame(ROWS).writeTo(f"hive.{identifier}").append()
    spark.createDataFrame(EXTRA_ROWS).writeTo(f"hive.{identifier}").append()
    table = _load_table(session_catalog_hive, identifier)

    first, second = table.snapshots()
    assert table.scan(snapshot_id=first.snapshot_id).to_arrow().sort_by("id").to_pylist() == ROWS
    assert table.scan(snapshot_id=second.snapshot_id).to_arrow().sort_by("id").to_pylist() == ROWS + EXTRA_ROWS


@pytest.mark.integration
@pytest.mark.parametrize("data_key_length", DATA_KEY_LENGTHS)
def test_scan_encrypted_table_requires_master_key(
    spark: SparkSession, session_catalog_hive: Catalog, data_key_length: int
) -> None:
    identifier = "default.test_scan_encrypted_table_requires_master_key"
    _create_encrypted_table(spark, identifier, data_key_length)
    spark.createDataFrame(ROWS).writeTo(f"hive.{identifier}").append()

    with pytest.raises(ValueError, match="no key management client was provided"):
        session_catalog_hive.load_table(identifier).scan().to_arrow()

    # Spark also encrypts the Parquet data files, so they can't be read without their keys
    table = _load_table(session_catalog_hive, identifier)
    data_files = [task.file.file_path for task in table.scan().plan_files()]
    assert data_files
    for data_file in data_files:
        with table.io.new_input(data_file).open() as stream:
            with pytest.raises(OSError, match="no decryption found"):
                pq.read_table(stream)


@pytest.mark.integration
@pytest.mark.parametrize("data_key_length", DATA_KEY_LENGTHS)
def test_inspect_encrypted_table(spark: SparkSession, session_catalog_hive: Catalog, data_key_length: int) -> None:
    identifier = "default.test_inspect_encrypted_table"
    _create_encrypted_table(spark, identifier, data_key_length)
    spark.createDataFrame(ROWS).writeTo(f"hive.{identifier}").append()
    # Delete a row from each partition, so Spark writes a deletion vector per partition
    spark.sql(f"DELETE FROM hive.{identifier} WHERE id IN (1, 2)")
    inspect = _load_table(session_catalog_hive, identifier).inspect

    files = inspect.files()
    spark_file_paths = [row.file_path for row in spark.table(f"hive.{identifier}.files").collect()]
    assert sorted(files["file_path"].to_pylist()) == sorted(spark_file_paths)
    assert all(key_metadata is not None for key_metadata in files["key_metadata"].to_pylist())
    assert sorted(entry["data_file"]["file_path"] for entry in inspect.entries().to_pylist()) == sorted(spark_file_paths)

    spark_manifest_paths = [row.path for row in spark.table(f"hive.{identifier}.manifests").collect()]
    assert sorted(inspect.manifests()["path"].to_pylist()) == sorted(spark_manifest_paths)

    # Each partition has two rows, and a deletion vector for one of them
    partitions = [
        (row["partition"]["category"], row["record_count"], row["position_delete_record_count"])
        for row in inspect.partitions().to_pylist()
    ]
    assert sorted(partitions) == [("a", 2, 1), ("b", 2, 1)]


def _create_encrypted_table(spark: SparkSession, identifier: str, data_key_length: int) -> None:
    # As of Iceberg Java 1.12, only the hive catalog supports table encryption.
    # Spark's hive catalog uses the mock KMS configured in dev/spark/spark-defaults.conf.
    kms = boto3.client(
        "kms",
        endpoint_url="http://localhost:5001",
        region_name="us-east-1",
        aws_access_key_id="admin",
        aws_secret_access_key="password",
    )
    master_key_id = kms.create_key()["KeyMetadata"]["KeyId"]

    spark.sql(f"DROP TABLE IF EXISTS hive.{identifier}")
    spark.sql(
        f"""
        CREATE TABLE hive.{identifier} (id bigint, category string, payload string)
        USING iceberg
        PARTITIONED BY (category)
        TBLPROPERTIES (
            'format-version'='3',
            'encryption.key-id'='{master_key_id}',
            'encryption.data-key-length'='{data_key_length}',
            'write.delete.mode'='merge-on-read'
        )
        """
    )


def _load_table(catalog: Catalog, identifier: str) -> Table:
    table = catalog.load_table(identifier)
    return Table(
        identifier=table.name(),
        metadata=table.metadata,
        metadata_location=table.metadata_location,
        io=table.io,
        catalog=catalog,
        kms_client=AwsKeyManagementClient(
            {
                KMS_ENDPOINT: "http://localhost:5001",
                AWS_REGION: "us-east-1",
                AWS_ACCESS_KEY_ID: "admin",
                AWS_SECRET_ACCESS_KEY: "password",
            }
        ),
    )
