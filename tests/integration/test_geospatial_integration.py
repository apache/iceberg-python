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
import struct
from uuid import uuid4

import pyarrow as pa
import pytest
from pytest_lazy_fixtures import lf

from pyiceberg.catalog import Catalog, load_catalog
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.io.pyarrow import schema_to_pyarrow
from pyiceberg.schema import Schema
from pyiceberg.table import TableProperties
from pyiceberg.table.metadata import SUPPORTED_TABLE_FORMAT_VERSION
from pyiceberg.types import GeographyType, GeometryType, IntegerType, NestedField


@pytest.fixture()
def rest_catalog() -> Catalog:
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


def _drop_if_exists(catalog: Catalog, identifier: str) -> None:
    try:
        catalog.drop_table(identifier)
    except NoSuchTableError:
        pass


def _as_bytes(value: object) -> bytes:
    if isinstance(value, bytes):
        return value
    if isinstance(value, bytearray):
        return bytes(value)
    if isinstance(value, memoryview):
        return value.tobytes()
    if hasattr(value, "to_wkb"):
        return bytes(value.to_wkb())
    raise TypeError(f"Unsupported value type: {type(value)}")


@pytest.mark.integration
@pytest.mark.parametrize("catalog", [lf("session_catalog"), lf("rest_catalog")])
def test_write_read_roundtrip_geospatial(catalog: Catalog) -> None:
    identifier = f"default.test_geospatial_roundtrip_{uuid4().hex[:8]}"
    _drop_if_exists(catalog, identifier)

    schema = Schema(
        NestedField(1, "id", IntegerType(), required=True),
        NestedField(2, "geom", GeometryType(), required=False),
        NestedField(3, "geog", GeographyType(), required=False),
    )
    table = catalog.create_table(
        identifier=identifier,
        schema=schema,
        properties={TableProperties.FORMAT_VERSION: "3"},
    )

    geom = struct.pack("<BIIdddd", 1, 2, 2, 1.0, 2.0, 3.0, 4.0)
    geog = struct.pack("<BIIdddd", 1, 2, 2, 170.0, 10.0, -170.0, 20.0)
    data = pa.Table.from_pydict(
        {"id": [1], "geom": [geom], "geog": [geog]},
        schema=schema_to_pyarrow(schema),
    )

    if SUPPORTED_TABLE_FORMAT_VERSION < 3:
        with pytest.raises((NotImplementedError, ValueError), match=r"(V3|v3|version: 3)"):
            table.append(data)
        return

    table.append(data)

    scanned = table.scan().to_arrow()
    assert scanned["id"][0].as_py() == 1
    assert _as_bytes(scanned["geom"][0].as_py()) == geom
    assert _as_bytes(scanned["geog"][0].as_py()) == geog


@pytest.mark.integration
@pytest.mark.parametrize("catalog", [lf("session_catalog"), lf("rest_catalog")])
def test_schema_evolution_add_geometry(catalog: Catalog) -> None:
    identifier = f"default.test_geospatial_evolution_{uuid4().hex[:8]}"
    _drop_if_exists(catalog, identifier)

    schema = Schema(NestedField(1, "id", IntegerType(), required=True))
    table = catalog.create_table(
        identifier=identifier,
        schema=schema,
        properties={TableProperties.FORMAT_VERSION: "3"},
    )
    table.update_schema().add_column("geom", GeometryType()).commit()

    reloaded = catalog.load_table(identifier)
    assert isinstance(reloaded.schema().find_field("geom").field_type, GeometryType)
