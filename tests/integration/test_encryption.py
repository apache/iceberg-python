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
"""Read Spark-written, Parquet-encrypted Iceberg tables (Hive catalog) via PyIceberg.

`hive.default.test_encrypted` is written by Spark in `dev/provision.py` using
`encryption.kms-impl=org.apache.iceberg.encryption.UnitestKMS`. UnitestKMS uses fixed master
keys keyA/keyB which we mirror into PyIceberg's InMemoryKms below.
"""

from __future__ import annotations

import pytest

from pyiceberg.catalog import load_catalog

_KEY_A_HEX = b"0123456789012345".hex()
_KEY_B_HEX = b"1123456789012345".hex()


@pytest.fixture(scope="module")
def hive_catalog_with_kms():  # type: ignore[no-untyped-def]
    return load_catalog(
        "local",
        **{
            "type": "hive",
            "uri": "thrift://localhost:9083",
            "s3.endpoint": "http://localhost:9000",
            "s3.access-key-id": "admin",
            "s3.secret-access-key": "password",
            "py-kms-impl": "pyiceberg.encryption.kms.InMemoryKms",
            "encryption.kms.key.keyA": _KEY_A_HEX,
            "encryption.kms.key.keyB": _KEY_B_HEX,
        },
    )


@pytest.mark.integration
def test_encrypted_table_metadata(hive_catalog_with_kms) -> None:  # type: ignore[no-untyped-def]
    tbl = hive_catalog_with_kms.load_table("default.test_encrypted")

    assert tbl.metadata.format_version == 3
    assert tbl.metadata.properties.get("encryption.key-id") == "keyA"
    assert tbl.metadata.encryption_keys
    assert tbl.current_snapshot() is not None
    assert tbl.current_snapshot().key_id is not None


@pytest.mark.integration
def test_encrypted_table_to_arrow(hive_catalog_with_kms) -> None:  # type: ignore[no-untyped-def]
    tbl = hive_catalog_with_kms.load_table("default.test_encrypted")
    result = tbl.scan().to_arrow().sort_by("id")
    assert result.column("id").to_pylist() == [1, 2, 3]
    assert result.column("data").to_pylist() == ["alice", "bob", "charlie"]
    assert result.column("value").to_pylist() == [1.0, 2.0, 3.0]


@pytest.mark.integration
def test_encrypted_table_to_pandas(hive_catalog_with_kms) -> None:  # type: ignore[no-untyped-def]
    tbl = hive_catalog_with_kms.load_table("default.test_encrypted")
    df = tbl.scan().to_pandas().sort_values("id").reset_index(drop=True)
    assert list(df["id"]) == [1, 2, 3]
    assert list(df["data"]) == ["alice", "bob", "charlie"]
    assert list(df["value"]) == [1.0, 2.0, 3.0]


@pytest.mark.integration
def test_encrypted_table_to_duckdb(hive_catalog_with_kms) -> None:  # type: ignore[no-untyped-def]
    tbl = hive_catalog_with_kms.load_table("default.test_encrypted")
    con = tbl.scan().to_duckdb("encrypted")
    rows = con.execute("SELECT id, data, value FROM encrypted ORDER BY id").fetchall()
    assert rows == [(1, "alice", 1.0), (2, "bob", 2.0), (3, "charlie", 3.0)]


@pytest.mark.integration
def test_encrypted_table_to_polars(hive_catalog_with_kms) -> None:  # type: ignore[no-untyped-def]
    tbl = hive_catalog_with_kms.load_table("default.test_encrypted")
    df = tbl.scan().to_polars().sort("id")
    assert df["id"].to_list() == [1, 2, 3]
    assert df["data"].to_list() == ["alice", "bob", "charlie"]
    assert df["value"].to_list() == [1.0, 2.0, 3.0]


@pytest.mark.integration
def test_encrypted_table_direct_parquet_read_fails(hive_catalog_with_kms) -> None:  # type: ignore[no-untyped-def]
    # Mirrors iceberg-java's TestTableEncryption#testDirectDataFileRead — proves the data
    # files are really PME-encrypted (raw read without keys must fail), so the above tests
    # can't silently pass on plaintext.
    import pyarrow.parquet as pq

    tbl = hive_catalog_with_kms.load_table("default.test_encrypted")
    data_files = [task.file.file_path for task in tbl.scan().plan_files()]
    assert data_files

    for file_path in data_files:
        with pytest.raises(OSError, match="encrypted"), tbl.io.new_input(file_path).open() as fi:
            pq.read_table(fi)
