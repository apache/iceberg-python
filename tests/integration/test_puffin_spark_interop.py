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
import pytest
from pyspark.sql import SparkSession

from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.manifest import ManifestContent
from pyiceberg.table.puffin import PuffinFile


def run_spark_commands(spark: SparkSession, sqls: list[str]) -> None:
    for sql in sqls:
        spark.sql(sql)


@pytest.mark.integration
def test_read_spark_written_puffin_dv(spark: SparkSession, session_catalog: RestCatalog) -> None:
    """Verify pyiceberg can read Puffin DVs written by Spark."""
    identifier = "default.spark_puffin_format_test"

    run_spark_commands(spark, [f"DROP TABLE IF EXISTS {identifier}"])
    run_spark_commands(
        spark,
        [
            f"""
            CREATE TABLE {identifier} (id BIGINT)
            USING iceberg
            TBLPROPERTIES (
                'format-version' = '3',
                'write.delete.mode' = 'merge-on-read'
            )
            """,
        ],
    )

    df = spark.range(1, 51)
    df.coalesce(1).writeTo(identifier).append()

    files_before = spark.sql(f"SELECT * FROM {identifier}.files").collect()
    assert len(files_before) == 1, f"Expected 1 file, got {len(files_before)}"

    run_spark_commands(spark, [f"DELETE FROM {identifier} WHERE id IN (10, 20, 30, 40)"])

    table = session_catalog.load_table(identifier)
    current_snapshot = table.current_snapshot()
    assert current_snapshot is not None

    manifests = current_snapshot.manifests(table.io)
    delete_manifests = [m for m in manifests if m.content == ManifestContent.DELETES]
    assert len(delete_manifests) > 0, "Expected delete manifest with DVs"

    delete_manifest = delete_manifests[0]
    entries = list(delete_manifest.fetch_manifest_entry(table.io))
    assert len(entries) > 0, "Expected at least one delete file entry"

    delete_entry = entries[0]
    puffin_path = delete_entry.data_file.file_path
    assert puffin_path.endswith(".puffin"), f"Expected Puffin file, got: {puffin_path}"

    input_file = table.io.new_input(puffin_path)
    with input_file.open() as f:
        puffin_bytes = f.read()

    puffin = PuffinFile(puffin_bytes)

    assert len(puffin.footer.blobs) == 1, "Expected exactly one blob"

    blob = puffin.footer.blobs[0]
    assert blob.type == "deletion-vector-v1"
    assert "referenced-data-file" in blob.properties
    assert blob.properties["cardinality"] == "4"

    dv_dict = puffin.to_vector()
    assert len(dv_dict) == 1, "Expected one data file's deletions"

    for data_file_path, chunked_array in dv_dict.items():
        positions = chunked_array.to_pylist()
        assert len(positions) == 4, f"Expected 4 deleted positions, got {len(positions)}"
        assert sorted(positions) == [9, 19, 29, 39], f"Unexpected positions: {positions}"
