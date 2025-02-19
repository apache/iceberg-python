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
import pyarrow as pa

from pyiceberg.catalog import Catalog
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, StringType, IntegerType


def _drop_table(catalog: Catalog, identifier: str) -> None:
    try:
        catalog.drop_table(identifier)
    except NoSuchTableError:
        pass
def test_vo(session_catalog: Catalog):
    catalog = session_catalog
    identifier = "default.test_upsert_benchmark"
    _drop_table(catalog, identifier)

    schema = Schema(
        NestedField(1, "idx", IntegerType(), required=True),
        NestedField(2, "number", IntegerType(), required=True),
        # Mark City as the identifier field, also known as the primary-key
        identifier_field_ids=[1],
    )

    tbl = catalog.create_table(identifier, schema=schema)

    arrow_schema = pa.schema(
        [
            pa.field("idx", pa.int32(), nullable=False),
            pa.field("number", pa.int32(), nullable=False),
        ]
    )

    # Write some data
    df = pa.Table.from_pylist(
        [
            {"idx": idx, "number": idx}
            for idx in range(1, 100000)
        ],
        schema=arrow_schema,
    )
    tbl.append(df)

    df_upsert = pa.Table.from_pylist(
        # Overlap
        [
            {"idx": idx, "number": idx}
            for idx in range(80000, 90000)
        ]+
        # Update
        [
            {"idx": idx, "number": idx + 1}
            for idx in range(90000, 100000)
        ]
        # Insert
        + [
            {"idx": idx, "number": idx}
            for idx in range(100000, 110000)],
        schema=arrow_schema,
    )

    start = time.time()

    tbl.upsert(df_upsert)

    stop = time.time()

    print(f"Took {stop-start} seconds")