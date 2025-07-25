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


from pathlib import Path

import pyarrow as pa
import pytest
from datafusion import SessionContext

from pyiceberg.catalog import Catalog, load_catalog


@pytest.fixture(scope="session")
def warehouse(tmp_path_factory: pytest.TempPathFactory) -> Path:
    return tmp_path_factory.mktemp("warehouse")


@pytest.fixture(scope="session")
def catalog(warehouse: Path) -> Catalog:
    catalog = load_catalog(
        "default",
        uri=f"sqlite:///{warehouse}/pyiceberg_catalog.db",
        warehouse=f"file://{warehouse}",
    )
    return catalog


def test_datafusion_register_pyiceberg_table(catalog: Catalog, arrow_table_with_null: pa.Table) -> None:
    catalog.create_namespace_if_not_exists("default")
    iceberg_table = catalog.create_table_if_not_exists(
        "default.dataset",
        schema=arrow_table_with_null.schema,
    )
    iceberg_table.append(arrow_table_with_null)

    ctx = SessionContext()
    ctx.register_table_provider("test", iceberg_table)

    datafusion_table = ctx.table("test")
    assert datafusion_table is not None

    assert datafusion_table.to_arrow_table().to_pylist() == iceberg_table.scan().to_arrow().to_pylist()

    from pandas.testing import assert_frame_equal

    assert_frame_equal(
        datafusion_table.to_arrow_table().to_pandas(),
        iceberg_table.scan().to_arrow().to_pandas(),
    )
