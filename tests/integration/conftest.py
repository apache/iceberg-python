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
from pyiceberg.catalog import Catalog, load_catalog
import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def session_catalog() -> Catalog:
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


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    import importlib.metadata
    import os

    spark_version = ".".join(importlib.metadata.version("pyspark").split(".")[:2])
    scala_version = "2.12"
    iceberg_version = "1.4.3"

    os.environ["PYSPARK_SUBMIT_ARGS"] = (
        f"--packages org.apache.iceberg:iceberg-spark-runtime-{spark_version}_{scala_version}:{iceberg_version},"
        f"org.apache.iceberg:iceberg-aws-bundle:{iceberg_version} pyspark-shell"
    )
    os.environ["AWS_REGION"] = "us-east-1"
    os.environ["AWS_ACCESS_KEY_ID"] = "admin"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "password"

    spark = (
        SparkSession.builder.appName("PyIceberg integration test")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.integration", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.integration.catalog-impl", "org.apache.iceberg.rest.RESTCatalog")
        .config("spark.sql.catalog.integration.uri", "http://localhost:8181")
        .config("spark.sql.catalog.integration.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.integration.warehouse", "s3://warehouse/wh/")
        .config("spark.sql.catalog.integration.s3.endpoint", "http://localhost:9000")
        .config("spark.sql.catalog.integration.s3.path-style-access", "true")
        .config("spark.sql.defaultCatalog", "integration")
        .getOrCreate()
    )

    return spark