#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.
# pylint:disable=redefined-outer-name
from typing import List, Optional, Union

import pyarrow as pa
import pytest

from pyiceberg.catalog import Catalog, load_catalog
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.partitioning import UNPARTITIONED_PARTITION_SPEC, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.typedef import EMPTY_DICT, Properties
from tests.integration.test_writes.utils import TABLE_SCHEMA


def _create_table(
    session_catalog: Catalog,
    identifier: str,
    properties: Properties = EMPTY_DICT,
    data: Optional[List[pa.Table]] = None,
    partition_spec: PartitionSpec = UNPARTITIONED_PARTITION_SPEC,
    schema: Union[Schema, "pa.Schema"] = TABLE_SCHEMA,
) -> Table:
    try:
        session_catalog.drop_table(identifier=identifier)
    except NoSuchTableError:
        pass

    tbl = session_catalog.create_table(identifier=identifier, schema=schema, properties=properties, partition_spec=partition_spec)

    if data is not None:
        for d in data:
            tbl.append(d)

    return tbl


@pytest.mark.integration
def test_rest_catalog_with_empty_name(arrow_table_with_null: pa.Table) -> None:
    identifier = "default.test_rest_append"
    test_catalog = load_catalog(
        "",  # intentionally empty
        **{
            "type": "rest",
            "uri": "http://localhost:8181",
            "s3.endpoint": "http://localhost:9000",
            "s3.access-key-id": "admin",
            "s3.secret-access-key": "password",
        },
    )
    tbl = _create_table(test_catalog, identifier, data=[])
    tbl.append(arrow_table_with_null)
