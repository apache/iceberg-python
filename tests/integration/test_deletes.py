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
import pytest
from pyspark.sql import DataFrame, SparkSession

from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.expressions import EqualTo


@pytest.fixture
def test_deletes_table(spark: SparkSession) -> DataFrame:
    identifier = 'default.table_partitioned_delete'

    spark.sql(f"DROP TABLE IF EXISTS {identifier}")

    spark.sql(
        f"""
        CREATE TABLE {identifier} (
            number_partitioned  int,
            number              int
        )
        USING iceberg
        PARTITIONED BY (number_partitioned)
    """
    )
    spark.sql(
        f"""
        INSERT INTO {identifier} VALUES (10, 20), (10, 30)
    """
    )
    spark.sql(
        f"""
        INSERT INTO {identifier} VALUES (11, 20), (11, 30)
    """
    )

    return spark.table(identifier)


def test_partition_deletes(test_deletes_table: DataFrame, session_catalog: RestCatalog) -> None:
    identifier = 'default.table_partitioned_delete'

    tbl = session_catalog.load_table(identifier)
    tbl.delete(EqualTo("number_partitioned", 10))

    assert tbl.scan().to_arrow().to_pydict() == {'number_partitioned': [11, 11], 'number': [20, 30]}


def test_deletes(test_deletes_table: DataFrame, session_catalog: RestCatalog) -> None:
    identifier = 'default.table_partitioned_delete'

    tbl = session_catalog.load_table(identifier)
    tbl.delete(EqualTo("number", 30))

    assert tbl.scan().to_arrow().to_pydict() == {'number_partitioned': [11, 11], 'number': [20, 20]}
