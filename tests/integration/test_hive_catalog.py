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
import time

import pytest
from pyspark.sql import SparkSession

from pyiceberg.catalog.hive import HiveCatalog


@pytest.mark.integration
def test_list_views(
    session_catalog_hive: HiveCatalog,
    spark: SparkSession,
) -> None:
    """
    Verify that a view created by Spark through the Iceberg Hive catalog
    can be discovered by PyIceberg HiveCatalog.list_views().

    The test also verifies that:
    - Iceberg tables are not returned as views.
    - Multiple views are returned.
    - Returned identifiers use the expected (namespace, view_name) format.
    """
    suffix = int(time.time())
    catalog_name = "hive"
    namespace = "default"
    table_name = f"table_{suffix}"
    first_view_name = f"first_view_{suffix}"
    second_view_name = f"second_view_{suffix}"
    table_identifier = f"{catalog_name}.{namespace}.{table_name}"
    first_view_identifier = f"{catalog_name}.{namespace}.{first_view_name}"
    second_view_identifier = f"{catalog_name}.{namespace}.{second_view_name}"

    spark.sql(f"""
        CREATE TABLE {table_identifier} (
            id INTEGER,
            name STRING,
            dt DATE
        )
        USING iceberg
    """)

    spark.sql(f"""
        CREATE VIEW {first_view_identifier} AS
        SELECT id, name
        FROM {table_identifier}
    """)

    spark.sql(f"""
        CREATE VIEW {second_view_identifier} AS
        SELECT id, name, dt
        FROM {table_identifier}
    """)

    views = set(session_catalog_hive.list_views(namespace))

    assert (namespace, first_view_name) in views
    assert (namespace, second_view_name) in views

    # A table in the same namespace must not be returned as a view.
    assert (namespace, table_name) not in views


@pytest.mark.integration
def test_list_views_non_existent_namespace(
    session_catalog_hive: HiveCatalog,
    spark: SparkSession,
) -> None:
    database_name = "non_existent_namespace"
    try:
        session_catalog_hive.list_views(database_name)
    except Exception as e:
        assert f"Namespace does not exist: {database_name}" in str(e)
