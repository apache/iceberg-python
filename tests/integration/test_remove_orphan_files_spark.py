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

import uuid
from collections.abc import Iterable
from datetime import datetime, timedelta, timezone

import pyarrow as pa
import pytest
from pyspark.sql import SparkSession

from pyiceberg.catalog import Catalog
from pyiceberg.table import Table

SCHEMA = pa.schema([pa.field("id", pa.int64(), nullable=False), pa.field("data", pa.string(), nullable=True)])

# Files that no snapshot references, and hidden files that neither implementation may report.
ORPHANS = ("data/orphan.parquet", "data/extra/orphan.parquet", "metadata/orphan.metadata.json")
HIDDEN = ("data/_temporary/part.parquet", "data/.staging/part.parquet")


def _create_table(catalog: Catalog, identifier: str) -> Table:
    """Create a table with two snapshots of committed data plus the orphan and hidden files."""
    catalog.create_namespace_if_not_exists("default")
    table = catalog.create_table(identifier, schema=SCHEMA)
    table.append(pa.Table.from_pylist([{"id": 1, "data": "a"}, {"id": 2, "data": "b"}], schema=SCHEMA))
    table.append(pa.Table.from_pylist([{"id": 3, "data": "c"}], schema=SCHEMA))

    for relative in ORPHANS + HIDDEN:
        with table.io.new_output(f"{table.location()}/{relative}").create() as out:
            out.write(b"junk")

    return table


def _relative(table: Table, locations: Iterable[str]) -> set[str]:
    """Strip the table location so results from two tables of differently named files can be compared."""
    prefix = table.location().split("://")[-1]
    return {location.split("://")[-1].removeprefix(prefix).lstrip("/") for location in locations}


@pytest.mark.integration
def test_remove_orphan_files_matches_spark(session_catalog: Catalog, spark: SparkSession) -> None:
    """The same table laid out twice yields the same orphans through PyIceberg and through Spark."""
    # A cutoff in the future puts every file in scope, so only reachability decides the outcome.
    cutoff = datetime.now(tz=timezone.utc) + timedelta(days=1)
    suffix = uuid.uuid4().hex[:8]

    identifier = f"default.orphans_pyiceberg_{suffix}"
    table = _create_table(session_catalog, identifier)
    result = table.maintenance.remove_orphan_files().older_than(cutoff).delete_with(lambda _: None).execute()
    ours = _relative(table, result.orphan_file_locations)

    spark_identifier = f"default.orphans_spark_{suffix}"
    spark_table = _create_table(session_catalog, spark_identifier)
    # The procedure rejects a cutoff newer than 24 hours unless the session is marked as testing,
    # and a core Spark config can only be set once the SQL layer stops rejecting those.
    spark.conf.set("spark.sql.legacy.setCommandRejectsSparkCoreConfs", "false")
    spark.conf.set("spark.testing", "true")
    try:
        rows = spark.sql(
            f"CALL rest.system.remove_orphan_files("
            f"table => '{spark_identifier}', "
            f"older_than => TIMESTAMP '{cutoff:%Y-%m-%d %H:%M:%S}', "
            f"dry_run => true, "
            f"prefix_listing => true)"
        ).collect()
    finally:
        spark.conf.unset("spark.testing")
        spark.conf.unset("spark.sql.legacy.setCommandRejectsSparkCoreConfs")
    theirs = _relative(spark_table, [row.orphan_file_location for row in rows])

    assert ours == set(ORPHANS)
    assert ours == theirs

    session_catalog.drop_table(identifier)
    session_catalog.drop_table(spark_identifier)
