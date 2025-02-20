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

from pyiceberg.catalog import Catalog
from pyiceberg.exceptions import NoSuchTableError, TableAlreadyExistsError
from pyiceberg.partitioning import UNPARTITIONED_PARTITION_SPEC, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.types import (
    BooleanType,
    DateType,
    IntegerType,
    NestedField,
    StringType,
)

TABLE_SCHEMA = Schema(
    NestedField(field_id=1, name="foo", field_type=BooleanType(), required=False),
    NestedField(field_id=2, name="bar", field_type=StringType(), required=False),
    NestedField(field_id=4, name="baz", field_type=IntegerType(), required=False),
    NestedField(field_id=10, name="qux", field_type=DateType(), required=False),
)


def _create_table(
    session_catalog: Catalog,
    identifier: str,
    format_version: int,
    location: str,
    partition_spec: PartitionSpec = UNPARTITIONED_PARTITION_SPEC,
    schema: Schema = TABLE_SCHEMA,
) -> Table:
    try:
        session_catalog.drop_table(identifier=identifier)
    except NoSuchTableError:
        pass

    return session_catalog.create_table(
        identifier=identifier,
        schema=schema,
        location=location,
        properties={"format-version": str(format_version)},
        partition_spec=partition_spec,
    )


@pytest.mark.integration
@pytest.mark.parametrize("catalog", [pytest.lazy_fixture("session_catalog_hive"), pytest.lazy_fixture("session_catalog")])
def test_register_table(
    catalog: Catalog,
) -> None:
    identifier = "default.register_table"
    location = "s3a://warehouse/default/register_table"
    tbl = _create_table(catalog, identifier, 2, location)
    assert catalog.table_exists(identifier=identifier)
    catalog.drop_table(identifier=identifier)
    assert not catalog.table_exists(identifier=identifier)
    catalog.register_table(("default", "register_table"), metadata_location=tbl.metadata_location)
    assert catalog.table_exists(identifier=identifier)


@pytest.mark.integration
@pytest.mark.parametrize("catalog", [pytest.lazy_fixture("session_catalog_hive"), pytest.lazy_fixture("session_catalog")])
def test_register_table_existing(
    catalog: Catalog,
) -> None:
    identifier = "default.register_table_existing"
    location = "s3a://warehouse/default/register_table_existing"
    tbl = _create_table(catalog, identifier, 2, location)
    assert catalog.table_exists(identifier=identifier)
    # Assert that registering the table again raises TableAlreadyExistsError
    with pytest.raises(TableAlreadyExistsError):
        catalog.register_table(("default", "register_table_existing"), metadata_location=tbl.metadata_location)
