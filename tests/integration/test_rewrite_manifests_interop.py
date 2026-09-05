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
from typing import TYPE_CHECKING

import pyarrow as pa
import pytest

from pyiceberg.catalog import Catalog
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.manifest import ManifestContent

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


@pytest.mark.integration
def test_spark_reads_table_after_rewrite_manifests(session_catalog: Catalog, spark: "SparkSession") -> None:
    identifier = "default.test_rewrite_manifests_interop"
    try:
        session_catalog.drop_table(identifier)
    except NoSuchTableError:
        pass

    table = session_catalog.create_table(identifier, schema=pa.schema([pa.field("id", pa.int64())]))
    for i in range(3):
        table.append(pa.table({"id": pa.array([i * 3 + 1, i * 3 + 2, i * 3 + 3], type=pa.int64())}))

    table = session_catalog.load_table(identifier)
    snapshot = table.current_snapshot()
    assert snapshot is not None
    assert len([m for m in snapshot.manifests(table.io) if m.content == ManifestContent.DATA]) == 3

    table.maintenance.rewrite_manifests().commit()

    table = session_catalog.load_table(identifier)
    snapshot = table.current_snapshot()
    assert snapshot is not None
    assert len([m for m in snapshot.manifests(table.io) if m.content == ManifestContent.DATA]) == 1

    # Spark must read the rewritten table with the same data
    spark_rows = spark.table(f"{identifier}").collect()
    assert sorted(row.id for row in spark_rows) == list(range(1, 10))

    # Spark must see the replace snapshot and the preserved data files
    snapshots = spark.sql(f"SELECT operation FROM {identifier}.snapshots ORDER BY committed_at").collect()
    assert [row.operation for row in snapshots] == ["append", "append", "append", "replace"]

    files = spark.sql(f"SELECT file_path FROM {identifier}.files").collect()
    assert len(files) == 3

    # Spark sees the same manifest consolidation
    manifests = spark.sql(f"SELECT path FROM {identifier}.manifests").collect()
    assert len(manifests) == 1

    # time travel to the pre-rewrite snapshot still works from Spark
    previous_snapshot_id = snapshot.parent_snapshot_id
    assert previous_snapshot_id is not None
    previous_rows = spark.sql(f"SELECT id FROM {identifier} VERSION AS OF {previous_snapshot_id}").collect()
    assert sorted(row.id for row in previous_rows) == list(range(1, 10))
