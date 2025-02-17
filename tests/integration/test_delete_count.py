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
import random
from datetime import datetime, timedelta
from typing import Generator, List

import pyarrow as pa
import pytest
from pyarrow import compute as pc
from pyspark.sql import SparkSession

from pyiceberg.catalog import Catalog
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.expressions import And, EqualTo, GreaterThanOrEqual, LessThan
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.transforms import HourTransform, IdentityTransform
from pyiceberg.types import LongType, NestedField, StringType


def run_spark_commands(spark: SparkSession, sqls: List[str]) -> None:
    for sql in sqls:
        spark.sql(sql)


@pytest.fixture()
def test_table(session_catalog: RestCatalog) -> Generator[Table, None, None]:
    identifier = "default.__test_table"
    arrow_table = pa.Table.from_arrays([pa.array([1, 2, 3, 4, 5]), pa.array(["a", "b", "c", "d", "e"])], names=["idx", "value"])
    test_table = session_catalog.create_table(
        identifier,
        schema=Schema(
            NestedField(1, "idx", LongType()),
            NestedField(2, "value", StringType()),
        ),
    )
    test_table.append(arrow_table)

    yield test_table

    session_catalog.drop_table(identifier)


@pytest.mark.integration
@pytest.mark.parametrize("format_version", [1, 2])
def test_partitioned_table_delete_full_file(spark: SparkSession, session_catalog: RestCatalog, format_version: int) -> None:
    identifier = "default.table_partitioned_delete"
    run_spark_commands(
        spark,
        [
            f"DROP TABLE IF EXISTS {identifier}",
            f"""
            CREATE TABLE {identifier} (
                number_partitioned  long,
                number              long
            )
            USING iceberg
            PARTITIONED BY (number_partitioned)
            TBLPROPERTIES('format-version' = {format_version})
        """,
            f"""
            INSERT INTO {identifier} VALUES (10, 20), (10, 30)
        """,
            f"""
            INSERT INTO {identifier} VALUES (11, 20), (11, 30)
        """,
        ],
    )

    tbl = session_catalog.load_table(identifier)
    tbl.delete(EqualTo("number_partitioned", 10))

    # No overwrite operation
    assert [snapshot.summary.operation.value for snapshot in tbl.snapshots()] == ["append", "append", "delete"]
    assert tbl.scan().to_arrow().to_pydict() == {"number_partitioned": [11, 11], "number": [20, 30]}

    assert tbl.scan().count() == len(tbl.scan().to_arrow())
    filter = And(EqualTo("number_partitioned", 11), GreaterThanOrEqual("number", 5))
    assert tbl.scan(filter).count() == len(tbl.scan(filter).to_arrow())
    N = 10
    d = {
        "number_partitioned": pa.array([i * 10 for i in range(N)]),
        "number": pa.array([random.choice([10, 20, 40]) for _ in range(N)]),
    }
    with tbl.update_spec() as update:
        update.add_field("number", transform=IdentityTransform())

    data = pa.Table.from_pydict(d)

    tbl.overwrite(df=data, overwrite_filter=filter)


@pytest.mark.integration
def test_rewrite_manifest_after_partition_evolution(session_catalog: Catalog) -> None:
    random.seed(876)
    N = 1440
    d = {
        "timestamp": pa.array([datetime(2023, 1, 1, 0, 0, 0) + timedelta(minutes=i) for i in range(N)]),
        "category": pa.array([random.choice(["A", "B", "C"]) for _ in range(N)]),
        "value": pa.array([random.gauss(0, 1) for _ in range(N)]),
    }
    data = pa.Table.from_pydict(d)

    try:
        session_catalog.drop_table(
            identifier="default.test_error_table",
        )
    except NoSuchTableError:
        pass

    table = session_catalog.create_table(
        "default.test_error_table",
        schema=data.schema,
    )
    with table.update_spec() as update:
        update.add_field("timestamp", transform=HourTransform())

    table.append(data)
    assert table.scan().count() == len(table.scan().to_arrow())

    with table.update_spec() as update:
        update.add_field("category", transform=IdentityTransform())

    data_ = data.filter(
        (pc.field("category") == "A")
        & (pc.field("timestamp") >= datetime(2023, 1, 1, 0))
        & (pc.field("timestamp") < datetime(2023, 1, 1, 1))
    )

    filter = And(
        And(
            GreaterThanOrEqual("timestamp", datetime(2023, 1, 1, 0).isoformat()),
            LessThan("timestamp", datetime(2023, 1, 1, 1).isoformat()),
        ),
        EqualTo("category", "A"),
    )
    # filter = GreaterThanOrEqual("timestamp", datetime(2023, 1, 1, 0).isoformat())
    # filter = LessThan("timestamp", datetime(2023, 1, 1, 1).isoformat())
    # filter = EqualTo("category", "A")
    # assert table.scan().plan_files()[0].file.partition == {"category": "A"}
    assert table.scan().count() == len(table.scan().to_arrow())
    assert table.scan(filter).count() == len(table.scan(filter).to_arrow())
    table.overwrite(df=data_, overwrite_filter=filter)
