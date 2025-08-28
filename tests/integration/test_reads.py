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

import math
import time
import uuid
from datetime import datetime, timedelta
from pathlib import PosixPath
from urllib.parse import urlparse

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from hive_metastore.ttypes import LockRequest, LockResponse, LockState, UnlockRequest
from pyarrow.fs import S3FileSystem
from pydantic_core import ValidationError
from pyspark.sql import SparkSession

from pyiceberg.catalog import Catalog
from pyiceberg.catalog.hive import HiveCatalog, _HiveClient
from pyiceberg.exceptions import CommitFailedException, NoSuchTableError
from pyiceberg.expressions import (
    And,
    EqualTo,
    GreaterThanOrEqual,
    IsNaN,
    LessThan,
    NotEqualTo,
    NotNaN,
    NotNull,
)
from pyiceberg.io import PYARROW_USE_LARGE_TYPES_ON_READ
from pyiceberg.io.pyarrow import (
    pyarrow_to_schema,
)
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.types import (
    BinaryType,
    BooleanType,
    IntegerType,
    LongType,
    NestedField,
    StringType,
    TimestampType,
)
from pyiceberg.utils.concurrent import ExecutorFactory

DEFAULT_PROPERTIES = {"write.parquet.compression-codec": "zstd"}


TABLE_NAME = ("default", "t1")


def create_table(catalog: Catalog) -> Table:
    try:
        catalog.drop_table(TABLE_NAME)
    except NoSuchTableError:
        pass  # Just to make sure that the table doesn't exist

    schema = Schema(
        NestedField(field_id=1, name="str", field_type=StringType(), required=False),
        NestedField(field_id=2, name="int", field_type=IntegerType(), required=True),
        NestedField(field_id=3, name="bool", field_type=BooleanType(), required=False),
        NestedField(field_id=4, name="datetime", field_type=TimestampType(), required=False),
    )

    return catalog.create_table(identifier=TABLE_NAME, schema=schema)


@pytest.mark.integration
@pytest.mark.parametrize("catalog", [pytest.lazy_fixture("session_catalog_hive"), pytest.lazy_fixture("session_catalog")])
def test_table_properties(catalog: Catalog) -> None:
    table = create_table(catalog)

    assert table.properties == DEFAULT_PROPERTIES

    with table.transaction() as transaction:
        transaction.set_properties(abc="🤪")
    assert table.properties == dict(abc="🤪", **DEFAULT_PROPERTIES)

    with table.transaction() as transaction:
        transaction.remove_properties("abc")
    assert table.properties == DEFAULT_PROPERTIES

    table = table.transaction().set_properties(abc="def").commit_transaction()
    assert table.properties == dict(abc="def", **DEFAULT_PROPERTIES)

    table = table.transaction().remove_properties("abc").commit_transaction()
    assert table.properties == DEFAULT_PROPERTIES

    table = table.transaction().set_properties(abc=123).commit_transaction()
    # properties are stored as strings in the iceberg spec
    assert table.properties == dict(abc="123", **DEFAULT_PROPERTIES)

    with pytest.raises(ValidationError) as exc_info:
        table.transaction().set_properties(property_name=None).commit_transaction()
    assert "None type is not a supported value in properties: property_name" in str(exc_info.value)


@pytest.mark.integration
@pytest.mark.parametrize("catalog", [pytest.lazy_fixture("session_catalog_hive")])
def test_hive_properties(catalog: Catalog) -> None:
    table = create_table(catalog)
    table.transaction().set_properties({"abc": "def", "p1": "123"}).commit_transaction()

    hive_client: _HiveClient = _HiveClient(catalog.properties["uri"])

    with hive_client as open_client:
        hive_table = open_client.get_table(*TABLE_NAME)
        assert hive_table.parameters.get("abc") == "def"
        assert hive_table.parameters.get("p1") == "123"
        assert hive_table.parameters.get("not_exist_parameter") is None

    table.transaction().remove_properties("abc").commit_transaction()

    with hive_client as open_client:
        hive_table = open_client.get_table(*TABLE_NAME)
        assert hive_table.parameters.get("abc") is None


@pytest.mark.integration
@pytest.mark.parametrize("catalog", [pytest.lazy_fixture("session_catalog_hive"), pytest.lazy_fixture("session_catalog")])
def test_table_properties_dict(catalog: Catalog) -> None:
    table = create_table(catalog)

    assert table.properties == DEFAULT_PROPERTIES

    with table.transaction() as transaction:
        transaction.set_properties({"abc": "🤪"})
    assert table.properties == dict({"abc": "🤪"}, **DEFAULT_PROPERTIES)

    with table.transaction() as transaction:
        transaction.remove_properties("abc")
    assert table.properties == DEFAULT_PROPERTIES

    table = table.transaction().set_properties({"abc": "def"}).commit_transaction()
    assert table.properties == dict({"abc": "def"}, **DEFAULT_PROPERTIES)

    table = table.transaction().remove_properties("abc").commit_transaction()
    assert table.properties == DEFAULT_PROPERTIES

    table = table.transaction().set_properties({"abc": 123}).commit_transaction()
    # properties are stored as strings in the iceberg spec
    assert table.properties == dict({"abc": "123"}, **DEFAULT_PROPERTIES)

    with pytest.raises(ValidationError) as exc_info:
        table.transaction().set_properties({"property_name": None}).commit_transaction()
    assert "None type is not a supported value in properties: property_name" in str(exc_info.value)


@pytest.mark.integration
@pytest.mark.parametrize("catalog", [pytest.lazy_fixture("session_catalog_hive"), pytest.lazy_fixture("session_catalog")])
def test_table_properties_error(catalog: Catalog) -> None:
    table = create_table(catalog)
    properties = {"abc": "def"}
    with pytest.raises(ValueError) as e:
        table.transaction().set_properties(properties, abc="def").commit_transaction()
    assert "Cannot pass both properties and kwargs" in str(e.value)


@pytest.mark.integration
@pytest.mark.parametrize("catalog", [pytest.lazy_fixture("session_catalog_hive"), pytest.lazy_fixture("session_catalog")])
def test_pyarrow_nan(catalog: Catalog) -> None:
    table_test_null_nan = catalog.load_table("default.test_null_nan")
    arrow_table = table_test_null_nan.scan(row_filter=IsNaN("col_numeric"), selected_fields=("idx", "col_numeric")).to_arrow()
    assert len(arrow_table) == 1
    assert arrow_table["idx"][0].as_py() == 1
    assert math.isnan(arrow_table["col_numeric"][0].as_py())


@pytest.mark.integration
@pytest.mark.parametrize("catalog", [pytest.lazy_fixture("session_catalog_hive"), pytest.lazy_fixture("session_catalog")])
def test_pyarrow_nan_rewritten(catalog: Catalog) -> None:
    table_test_null_nan_rewritten = catalog.load_table("default.test_null_nan_rewritten")
    arrow_table = table_test_null_nan_rewritten.scan(
        row_filter=IsNaN("col_numeric"), selected_fields=("idx", "col_numeric")
    ).to_arrow()
    assert len(arrow_table) == 1
    assert arrow_table["idx"][0].as_py() == 1
    assert math.isnan(arrow_table["col_numeric"][0].as_py())


@pytest.mark.integration
@pytest.mark.parametrize("catalog", [pytest.lazy_fixture("session_catalog_hive"), pytest.lazy_fixture("session_catalog")])
@pytest.mark.skip(reason="Fixing issues with NaN's: https://github.com/apache/arrow/issues/34162")
def test_pyarrow_not_nan_count(catalog: Catalog) -> None:
    table_test_null_nan = catalog.load_table("default.test_null_nan")
    not_nan = table_test_null_nan.scan(row_filter=NotNaN("col_numeric"), selected_fields=("idx",)).to_arrow()
    assert len(not_nan) == 2


@pytest.mark.integration
@pytest.mark.parametrize("catalog", [pytest.lazy_fixture("session_catalog_hive"), pytest.lazy_fixture("session_catalog")])
def test_pyarrow_batches_nan(catalog: Catalog) -> None:
    table_test_null_nan = catalog.load_table("default.test_null_nan")
    arrow_batch_reader = table_test_null_nan.scan(
        row_filter=IsNaN("col_numeric"), selected_fields=("idx", "col_numeric")
    ).to_arrow_batch_reader()
    assert isinstance(arrow_batch_reader, pa.RecordBatchReader)
    arrow_table = arrow_batch_reader.read_all()
    assert len(arrow_table) == 1
    assert arrow_table["idx"][0].as_py() == 1
    assert math.isnan(arrow_table["col_numeric"][0].as_py())


@pytest.mark.integration
@pytest.mark.parametrize("catalog", [pytest.lazy_fixture("session_catalog_hive"), pytest.lazy_fixture("session_catalog")])
def test_pyarrow_batches_nan_rewritten(catalog: Catalog) -> None:
    table_test_null_nan_rewritten = catalog.load_table("default.test_null_nan_rewritten")
    arrow_batch_reader = table_test_null_nan_rewritten.scan(
        row_filter=IsNaN("col_numeric"), selected_fields=("idx", "col_numeric")
    ).to_arrow_batch_reader()
    assert isinstance(arrow_batch_reader, pa.RecordBatchReader)
    arrow_table = arrow_batch_reader.read_all()
    assert len(arrow_table) == 1
    assert arrow_table["idx"][0].as_py() == 1
    assert math.isnan(arrow_table["col_numeric"][0].as_py())


@pytest.mark.integration
@pytest.mark.parametrize("catalog", [pytest.lazy_fixture("session_catalog_hive"), pytest.lazy_fixture("session_catalog")])
@pytest.mark.skip(reason="Fixing issues with NaN's: https://github.com/apache/arrow/issues/34162")
def test_pyarrow_batches_not_nan_count(catalog: Catalog) -> None:
    table_test_null_nan = catalog.load_table("default.test_null_nan")
    arrow_batch_reader = table_test_null_nan.scan(
        row_filter=NotNaN("col_numeric"), selected_fields=("idx",)
    ).to_arrow_batch_reader()
    assert isinstance(arrow_batch_reader, pa.RecordBatchReader)
    arrow_table = arrow_batch_reader.read_all()
    assert len(arrow_table) == 2


@pytest.mark.integration
@pytest.mark.parametrize("catalog", [pytest.lazy_fixture("session_catalog_hive"), pytest.lazy_fixture("session_catalog")])
def test_duckdb_nan(catalog: Catalog) -> None:
    table_test_null_nan_rewritten = catalog.load_table("default.test_null_nan_rewritten")
    con = table_test_null_nan_rewritten.scan().to_duckdb("table_test_null_nan")
    result = con.query("SELECT idx, col_numeric FROM table_test_null_nan WHERE isnan(col_numeric)").fetchone()
    assert result[0] == 1
    assert math.isnan(result[1])


@pytest.mark.integration
@pytest.mark.parametrize("catalog", [pytest.lazy_fixture("session_catalog_hive"), pytest.lazy_fixture("session_catalog")])
def test_pyarrow_limit(catalog: Catalog) -> None:
    table_test_limit = catalog.load_table("default.test_limit")
    limited_result = table_test_limit.scan(selected_fields=("idx",), limit=1).to_arrow()
    assert len(limited_result) == 1

    empty_result = table_test_limit.scan(selected_fields=("idx",), limit=0).to_arrow()
    assert len(empty_result) == 0

    full_result = table_test_limit.scan(selected_fields=("idx",), limit=999).to_arrow()
    assert len(full_result) == 10

    # test `to_arrow_batch_reader`
    limited_result = table_test_limit.scan(selected_fields=("idx",), limit=1).to_arrow_batch_reader().read_all()
    assert len(limited_result) == 1

    empty_result = table_test_limit.scan(selected_fields=("idx",), limit=0).to_arrow_batch_reader().read_all()
    assert len(empty_result) == 0

    full_result = table_test_limit.scan(selected_fields=("idx",), limit=999).to_arrow_batch_reader().read_all()
    assert len(full_result) == 10


@pytest.mark.integration
@pytest.mark.parametrize("catalog", [pytest.lazy_fixture("session_catalog_hive"), pytest.lazy_fixture("session_catalog")])
def test_pyarrow_limit_with_multiple_files(catalog: Catalog) -> None:
    table_name = "default.test_pyarrow_limit_with_multiple_files"
    try:
        catalog.drop_table(table_name)
    except NoSuchTableError:
        pass
    reference_table = catalog.load_table("default.test_limit")
    data = reference_table.scan().to_arrow()
    table_test_limit = catalog.create_table(table_name, schema=reference_table.schema())

    n_files = 2
    for _ in range(n_files):
        table_test_limit.append(data)
    assert len(table_test_limit.inspect.files()) == n_files

    # test with multiple files
    limited_result = table_test_limit.scan(selected_fields=("idx",), limit=1).to_arrow()
    assert len(limited_result) == 1

    empty_result = table_test_limit.scan(selected_fields=("idx",), limit=0).to_arrow()
    assert len(empty_result) == 0

    full_result = table_test_limit.scan(selected_fields=("idx",), limit=999).to_arrow()
    assert len(full_result) == 10 * n_files

    # test `to_arrow_batch_reader`
    limited_result = table_test_limit.scan(selected_fields=("idx",), limit=1).to_arrow_batch_reader().read_all()
    assert len(limited_result) == 1

    empty_result = table_test_limit.scan(selected_fields=("idx",), limit=0).to_arrow_batch_reader().read_all()
    assert len(empty_result) == 0

    full_result = table_test_limit.scan(selected_fields=("idx",), limit=999).to_arrow_batch_reader().read_all()
    assert len(full_result) == 10 * n_files


@pytest.mark.integration
@pytest.mark.parametrize("catalog", [pytest.lazy_fixture("session_catalog_hive"), pytest.lazy_fixture("session_catalog")])
def test_daft_nan(catalog: Catalog) -> None:
    table_test_null_nan_rewritten = catalog.load_table("default.test_null_nan_rewritten")
    df = table_test_null_nan_rewritten.to_daft()
    assert df.count_rows() == 3
    assert math.isnan(df.to_pydict()["col_numeric"][0])


@pytest.mark.integration
@pytest.mark.parametrize("catalog", [pytest.lazy_fixture("session_catalog_hive"), pytest.lazy_fixture("session_catalog")])
def test_daft_nan_rewritten(catalog: Catalog) -> None:
    table_test_null_nan_rewritten = catalog.load_table("default.test_null_nan_rewritten")
    df = table_test_null_nan_rewritten.to_daft()
    df = df.where(df["col_numeric"].float.is_nan())
    df = df.select("idx", "col_numeric")
    assert df.count_rows() == 1
    assert df.to_pydict()["idx"][0] == 1
    assert math.isnan(df.to_pydict()["col_numeric"][0])


@pytest.mark.skip(reason="Bodo should not monekeypatch PyArrowFileIO, https://github.com/apache/iceberg-python/issues/2400")
@pytest.mark.integration
@pytest.mark.filterwarnings("ignore")
@pytest.mark.parametrize("catalog", [pytest.lazy_fixture("session_catalog_hive"), pytest.lazy_fixture("session_catalog")])
def test_bodo_nan(catalog: Catalog, monkeypatch: pytest.MonkeyPatch) -> None:
    # Avoid local Mac issues (see https://github.com/apache/iceberg-python/issues/2225)
    monkeypatch.setenv("BODO_DATAFRAME_LIBRARY_RUN_PARALLEL", "0")
    monkeypatch.setenv("FI_PROVIDER", "tcp")

    table_test_null_nan_rewritten = catalog.load_table("default.test_null_nan_rewritten")
    df = table_test_null_nan_rewritten.to_bodo()
    assert len(df) == 3
    assert math.isnan(df.col_numeric.iloc[0])


@pytest.mark.integration
@pytest.mark.filterwarnings("ignore")
@pytest.mark.parametrize("catalog", [pytest.lazy_fixture("session_catalog_hive"), pytest.lazy_fixture("session_catalog")])
def test_ray_nan(catalog: Catalog) -> None:
    table_test_null_nan_rewritten = catalog.load_table("default.test_null_nan_rewritten")
    ray_dataset = table_test_null_nan_rewritten.scan().to_ray()
    assert ray_dataset.count() == 3
    assert math.isnan(ray_dataset.take()[0]["col_numeric"])


@pytest.mark.integration
@pytest.mark.parametrize("catalog", [pytest.lazy_fixture("session_catalog_hive"), pytest.lazy_fixture("session_catalog")])
def test_ray_nan_rewritten(catalog: Catalog) -> None:
    table_test_null_nan_rewritten = catalog.load_table("default.test_null_nan_rewritten")
    ray_dataset = table_test_null_nan_rewritten.scan(
        row_filter=IsNaN("col_numeric"), selected_fields=("idx", "col_numeric")
    ).to_ray()
    assert ray_dataset.count() == 1
    assert ray_dataset.take()[0]["idx"] == 1
    assert math.isnan(ray_dataset.take()[0]["col_numeric"])


@pytest.mark.integration
@pytest.mark.parametrize("catalog", [pytest.lazy_fixture("session_catalog_hive"), pytest.lazy_fixture("session_catalog")])
@pytest.mark.skip(reason="Fixing issues with NaN's: https://github.com/apache/arrow/issues/34162")
def test_ray_not_nan_count(catalog: Catalog) -> None:
    table_test_null_nan_rewritten = catalog.load_table("default.test_null_nan_rewritten")
    ray_dataset = table_test_null_nan_rewritten.scan(row_filter=NotNaN("col_numeric"), selected_fields=("idx",)).to_ray()
    assert ray_dataset.count() == 2


@pytest.mark.integration
@pytest.mark.parametrize("catalog", [pytest.lazy_fixture("session_catalog_hive"), pytest.lazy_fixture("session_catalog")])
def test_ray_all_types(catalog: Catalog) -> None:
    table_test_all_types = catalog.load_table("default.test_all_types")
    ray_dataset = table_test_all_types.scan().to_ray()
    pandas_dataframe = table_test_all_types.scan().to_pandas()
    assert ray_dataset.count() == pandas_dataframe.shape[0]
    assert pandas_dataframe.equals(ray_dataset.to_pandas())


@pytest.mark.integration
@pytest.mark.parametrize("catalog", [pytest.lazy_fixture("session_catalog_hive"), pytest.lazy_fixture("session_catalog")])
def test_pyarrow_to_iceberg_all_types(catalog: Catalog) -> None:
    table_test_all_types = catalog.load_table("default.test_all_types")
    fs = S3FileSystem(
        endpoint_override=catalog.properties["s3.endpoint"],
        access_key=catalog.properties["s3.access-key-id"],
        secret_key=catalog.properties["s3.secret-access-key"],
    )
    data_file_paths = [task.file.file_path for task in table_test_all_types.scan().plan_files()]
    for data_file_path in data_file_paths:
        uri = urlparse(data_file_path)
        with fs.open_input_file(f"{uri.netloc}{uri.path}") as fout:
            parquet_schema = pq.read_schema(fout)
            stored_iceberg_schema = Schema.model_validate_json(parquet_schema.metadata.get(b"iceberg.schema"))
            converted_iceberg_schema = pyarrow_to_schema(parquet_schema)
            assert converted_iceberg_schema == stored_iceberg_schema


@pytest.mark.integration
@pytest.mark.parametrize("catalog", [pytest.lazy_fixture("session_catalog_hive"), pytest.lazy_fixture("session_catalog")])
@pytest.mark.parametrize("format_version", [2, 3])
def test_pyarrow_deletes(catalog: Catalog, format_version: int) -> None:
    # number, letter
    #  (1, 'a'),
    #  (2, 'b'),
    #  (3, 'c'),
    #  (4, 'd'),
    #  (5, 'e'),
    #  (6, 'f'),
    #  (7, 'g'),
    #  (8, 'h'),
    #  (9, 'i'), <- deleted
    #  (10, 'j'),
    #  (11, 'k'),
    #  (12, 'l')
    test_positional_mor_deletes = catalog.load_table(f"default.test_positional_mor_deletes_v{format_version}")
    arrow_table = test_positional_mor_deletes.scan().to_arrow()
    assert arrow_table["number"].to_pylist() == [1, 2, 3, 4, 5, 6, 7, 8, 10, 11, 12]

    # Checking the filter
    arrow_table = test_positional_mor_deletes.scan(
        row_filter=And(GreaterThanOrEqual("letter", "e"), LessThan("letter", "k"))
    ).to_arrow()
    assert arrow_table["number"].to_pylist() == [5, 6, 7, 8, 10]

    # Testing the combination of a filter and a limit
    arrow_table = test_positional_mor_deletes.scan(
        row_filter=And(GreaterThanOrEqual("letter", "e"), LessThan("letter", "k")), limit=1
    ).to_arrow()
    assert arrow_table["number"].to_pylist() == [5]

    # Testing the slicing of indices
    arrow_table = test_positional_mor_deletes.scan(limit=3).to_arrow()
    assert arrow_table["number"].to_pylist() == [1, 2, 3]


@pytest.mark.integration
@pytest.mark.parametrize("catalog", [pytest.lazy_fixture("session_catalog_hive"), pytest.lazy_fixture("session_catalog")])
@pytest.mark.parametrize("format_version", [2, 3])
def test_pyarrow_deletes_double(catalog: Catalog, format_version: int) -> None:
    # number, letter
    #  (1, 'a'),
    #  (2, 'b'),
    #  (3, 'c'),
    #  (4, 'd'),
    #  (5, 'e'),
    #  (6, 'f'), <- second delete
    #  (7, 'g'),
    #  (8, 'h'),
    #  (9, 'i'), <- first delete
    #  (10, 'j'),
    #  (11, 'k'),
    #  (12, 'l')
    test_positional_mor_double_deletes = catalog.load_table(f"default.test_positional_mor_double_deletes_v{format_version}")
    arrow_table = test_positional_mor_double_deletes.scan().to_arrow()
    assert arrow_table["number"].to_pylist() == [1, 2, 3, 4, 5, 7, 8, 10, 11, 12]

    # Checking the filter
    arrow_table = test_positional_mor_double_deletes.scan(
        row_filter=And(GreaterThanOrEqual("letter", "e"), LessThan("letter", "k"))
    ).to_arrow()
    assert arrow_table["number"].to_pylist() == [5, 7, 8, 10]

    # Testing the combination of a filter and a limit
    arrow_table = test_positional_mor_double_deletes.scan(
        row_filter=And(GreaterThanOrEqual("letter", "e"), LessThan("letter", "k")), limit=1
    ).to_arrow()
    assert arrow_table["number"].to_pylist() == [5]

    # Testing the slicing of indices
    arrow_table = test_positional_mor_double_deletes.scan(limit=8).to_arrow()
    assert arrow_table["number"].to_pylist() == [1, 2, 3, 4, 5, 7, 8, 10]


@pytest.mark.integration
@pytest.mark.parametrize("catalog", [pytest.lazy_fixture("session_catalog_hive"), pytest.lazy_fixture("session_catalog")])
@pytest.mark.parametrize("format_version", [2, 3])
def test_pyarrow_batches_deletes(catalog: Catalog, format_version: int) -> None:
    # number, letter
    #  (1, 'a'),
    #  (2, 'b'),
    #  (3, 'c'),
    #  (4, 'd'),
    #  (5, 'e'),
    #  (6, 'f'),
    #  (7, 'g'),
    #  (8, 'h'),
    #  (9, 'i'), <- deleted
    #  (10, 'j'),
    #  (11, 'k'),
    #  (12, 'l')
    test_positional_mor_deletes = catalog.load_table(f"default.test_positional_mor_deletes_v{format_version}")
    arrow_table = test_positional_mor_deletes.scan().to_arrow_batch_reader().read_all()
    assert arrow_table["number"].to_pylist() == [1, 2, 3, 4, 5, 6, 7, 8, 10, 11, 12]

    # Checking the filter
    arrow_table = (
        test_positional_mor_deletes.scan(row_filter=And(GreaterThanOrEqual("letter", "e"), LessThan("letter", "k")))
        .to_arrow_batch_reader()
        .read_all()
    )
    assert arrow_table["number"].to_pylist() == [5, 6, 7, 8, 10]

    # Testing the combination of a filter and a limit
    arrow_table = (
        test_positional_mor_deletes.scan(row_filter=And(GreaterThanOrEqual("letter", "e"), LessThan("letter", "k")), limit=1)
        .to_arrow_batch_reader()
        .read_all()
    )
    assert arrow_table["number"].to_pylist() == [5]

    # Testing the slicing of indices
    arrow_table = test_positional_mor_deletes.scan(limit=3).to_arrow_batch_reader().read_all()
    assert arrow_table["number"].to_pylist() == [1, 2, 3]


@pytest.mark.integration
@pytest.mark.parametrize("catalog", [pytest.lazy_fixture("session_catalog_hive"), pytest.lazy_fixture("session_catalog")])
@pytest.mark.parametrize("format_version", [2, 3])
def test_pyarrow_batches_deletes_double(catalog: Catalog, format_version: int) -> None:
    # number, letter
    #  (1, 'a'),
    #  (2, 'b'),
    #  (3, 'c'),
    #  (4, 'd'),
    #  (5, 'e'),
    #  (6, 'f'), <- second delete
    #  (7, 'g'),
    #  (8, 'h'),
    #  (9, 'i'), <- first delete
    #  (10, 'j'),
    #  (11, 'k'),
    #  (12, 'l')
    test_positional_mor_double_deletes = catalog.load_table(f"default.test_positional_mor_double_deletes_v{format_version}")
    arrow_table = test_positional_mor_double_deletes.scan().to_arrow_batch_reader().read_all()
    assert arrow_table["number"].to_pylist() == [1, 2, 3, 4, 5, 7, 8, 10, 11, 12]

    # Checking the filter
    arrow_table = (
        test_positional_mor_double_deletes.scan(row_filter=And(GreaterThanOrEqual("letter", "e"), LessThan("letter", "k")))
        .to_arrow_batch_reader()
        .read_all()
    )
    assert arrow_table["number"].to_pylist() == [5, 7, 8, 10]

    # Testing the combination of a filter and a limit
    arrow_table = (
        test_positional_mor_double_deletes.scan(
            row_filter=And(GreaterThanOrEqual("letter", "e"), LessThan("letter", "k")), limit=1
        )
        .to_arrow_batch_reader()
        .read_all()
    )
    assert arrow_table["number"].to_pylist() == [5]

    # Testing the slicing of indices
    arrow_table = test_positional_mor_double_deletes.scan(limit=8).to_arrow_batch_reader().read_all()
    assert arrow_table["number"].to_pylist() == [1, 2, 3, 4, 5, 7, 8, 10]


@pytest.mark.integration
@pytest.mark.parametrize("catalog", [pytest.lazy_fixture("session_catalog_hive"), pytest.lazy_fixture("session_catalog")])
def test_partitioned_tables(catalog: Catalog) -> None:
    for table_name, predicate in [
        ("test_partitioned_by_identity", "ts >= '2023-03-05T00:00:00+00:00'"),
        ("test_partitioned_by_years", "dt >= '2023-03-05'"),
        ("test_partitioned_by_months", "dt >= '2023-03-05'"),
        ("test_partitioned_by_days", "ts >= '2023-03-05T00:00:00+00:00'"),
        ("test_partitioned_by_hours", "ts >= '2023-03-05T00:00:00+00:00'"),
        ("test_partitioned_by_truncate", "letter >= 'e'"),
        ("test_partitioned_by_bucket", "number >= '5'"),
    ]:
        table = catalog.load_table(f"default.{table_name}")
        arrow_table = table.scan(selected_fields=("number",), row_filter=predicate).to_arrow()
        assert set(arrow_table["number"].to_pylist()) == {5, 6, 7, 8, 9, 10, 11, 12}, f"Table {table_name}, predicate {predicate}"


@pytest.mark.integration
@pytest.mark.parametrize("catalog", [pytest.lazy_fixture("session_catalog_hive"), pytest.lazy_fixture("session_catalog")])
def test_unpartitioned_uuid_table(catalog: Catalog) -> None:
    unpartitioned_uuid = catalog.load_table("default.test_uuid_and_fixed_unpartitioned")
    arrow_table_eq = unpartitioned_uuid.scan(row_filter="uuid_col == '102cb62f-e6f8-4eb0-9973-d9b012ff0967'").to_arrow()
    assert arrow_table_eq["uuid_col"].to_pylist() == [uuid.UUID("102cb62f-e6f8-4eb0-9973-d9b012ff0967")]

    arrow_table_neq = unpartitioned_uuid.scan(
        row_filter="uuid_col != '102cb62f-e6f8-4eb0-9973-d9b012ff0967' and uuid_col != '639cccce-c9d2-494a-a78c-278ab234f024'"
    ).to_arrow()
    assert arrow_table_neq["uuid_col"].to_pylist() == [
        uuid.UUID("ec33e4b2-a834-4cc3-8c4a-a1d3bfc2f226"),
        uuid.UUID("c1b0d8e0-0b0e-4b1e-9b0a-0e0b0d0c0a0b"),
        uuid.UUID("923dae77-83d6-47cd-b4b0-d383e64ee57e"),
    ]


@pytest.mark.integration
@pytest.mark.parametrize("catalog", [pytest.lazy_fixture("session_catalog_hive"), pytest.lazy_fixture("session_catalog")])
def test_unpartitioned_fixed_table(catalog: Catalog) -> None:
    fixed_table = catalog.load_table("default.test_uuid_and_fixed_unpartitioned")
    arrow_table_eq = fixed_table.scan(row_filter=EqualTo("fixed_col", b"1234567890123456789012345")).to_arrow()
    assert arrow_table_eq["fixed_col"].to_pylist() == [b"1234567890123456789012345"]

    arrow_table_neq = fixed_table.scan(
        row_filter=And(
            NotEqualTo("fixed_col", b"1234567890123456789012345"), NotEqualTo("uuid_col", "c1b0d8e0-0b0e-4b1e-9b0a-0e0b0d0c0a0b")
        )
    ).to_arrow()
    assert arrow_table_neq["fixed_col"].to_pylist() == [
        b"1231231231231231231231231",
        b"12345678901234567ass12345",
        b"qweeqwwqq1231231231231111",
    ]


@pytest.mark.integration
@pytest.mark.parametrize("catalog", [pytest.lazy_fixture("session_catalog_hive"), pytest.lazy_fixture("session_catalog")])
@pytest.mark.parametrize("format_version", [2, 3])
def test_scan_tag(catalog: Catalog, format_version: int) -> None:
    test_positional_mor_deletes = catalog.load_table(f"default.test_positional_mor_deletes_v{format_version}")
    arrow_table = test_positional_mor_deletes.scan().use_ref("tag_12").to_arrow()
    assert arrow_table["number"].to_pylist() == [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]


@pytest.mark.integration
@pytest.mark.parametrize("catalog", [pytest.lazy_fixture("session_catalog_hive"), pytest.lazy_fixture("session_catalog")])
@pytest.mark.parametrize("format_version", [2, 3])
def test_scan_branch(catalog: Catalog, format_version: int) -> None:
    test_positional_mor_deletes = catalog.load_table(f"default.test_positional_mor_deletes_v{format_version}")
    arrow_table = test_positional_mor_deletes.scan().use_ref("without_5").to_arrow()
    assert arrow_table["number"].to_pylist() == [1, 2, 3, 4, 6, 7, 8, 9, 10, 11, 12]


@pytest.mark.integration
@pytest.mark.parametrize("catalog", [pytest.lazy_fixture("session_catalog_hive"), pytest.lazy_fixture("session_catalog")])
def test_filter_on_new_column(catalog: Catalog) -> None:
    test_table_add_column = catalog.load_table("default.test_table_add_column")
    arrow_table = test_table_add_column.scan(row_filter="b == '2'").to_arrow()
    assert arrow_table["b"].to_pylist() == ["2"]

    arrow_table = test_table_add_column.scan(row_filter="b is not null").to_arrow()
    assert arrow_table["b"].to_pylist() == ["2"]

    arrow_table = test_table_add_column.scan(row_filter="b is null").to_arrow()
    assert arrow_table["b"].to_pylist() == [None]


@pytest.mark.integration
@pytest.mark.parametrize("catalog", [pytest.lazy_fixture("session_catalog_hive"), pytest.lazy_fixture("session_catalog")])
def test_filter_case_sensitive_by_default(catalog: Catalog) -> None:
    test_table_add_column = catalog.load_table("default.test_table_add_column")
    arrow_table = test_table_add_column.scan().to_arrow()
    assert "2" in arrow_table["b"].to_pylist()

    arrow_table = test_table_add_column.scan(row_filter="b == '2'").to_arrow()
    assert arrow_table["b"].to_pylist() == ["2"]

    with pytest.raises(ValueError) as e:
        _ = test_table_add_column.scan(row_filter="B == '2'").to_arrow()
    assert "Could not find field with name B" in str(e.value)


@pytest.mark.integration
@pytest.mark.parametrize("catalog", [pytest.lazy_fixture("session_catalog_hive"), pytest.lazy_fixture("session_catalog")])
def test_filter_case_sensitive(catalog: Catalog) -> None:
    test_table_add_column = catalog.load_table("default.test_table_add_column")
    arrow_table = test_table_add_column.scan().to_arrow()
    assert "2" in arrow_table["b"].to_pylist()

    arrow_table = test_table_add_column.scan(row_filter="b == '2'", case_sensitive=True).to_arrow()
    assert arrow_table["b"].to_pylist() == ["2"]

    with pytest.raises(ValueError) as e:
        _ = test_table_add_column.scan(row_filter="B == '2'", case_sensitive=True).to_arrow()
    assert "Could not find field with name B" in str(e.value)


@pytest.mark.integration
@pytest.mark.parametrize("catalog", [pytest.lazy_fixture("session_catalog_hive"), pytest.lazy_fixture("session_catalog")])
def test_filter_case_insensitive(catalog: Catalog) -> None:
    test_table_add_column = catalog.load_table("default.test_table_add_column")
    arrow_table = test_table_add_column.scan().to_arrow()
    assert "2" in arrow_table["b"].to_pylist()

    arrow_table = test_table_add_column.scan(row_filter="b == '2'", case_sensitive=False).to_arrow()
    assert arrow_table["b"].to_pylist() == ["2"]

    arrow_table = test_table_add_column.scan(row_filter="B == '2'", case_sensitive=False).to_arrow()
    assert arrow_table["b"].to_pylist() == ["2"]


@pytest.mark.integration
@pytest.mark.parametrize("catalog", [pytest.lazy_fixture("session_catalog_hive"), pytest.lazy_fixture("session_catalog")])
def test_filters_on_top_level_struct(catalog: Catalog) -> None:
    test_empty_struct = catalog.load_table("default.test_table_empty_list_and_map")

    arrow_table = test_empty_struct.scan().to_arrow()
    assert None in arrow_table["col_struct"].to_pylist()

    arrow_table = test_empty_struct.scan(row_filter=NotNull("col_struct")).to_arrow()
    assert arrow_table["col_struct"].to_pylist() == [{"test": 1}]

    arrow_table = test_empty_struct.scan(row_filter="col_struct is not null", case_sensitive=False).to_arrow()
    assert arrow_table["col_struct"].to_pylist() == [{"test": 1}]

    arrow_table = test_empty_struct.scan(row_filter="COL_STRUCT is null", case_sensitive=False).to_arrow()
    assert arrow_table["col_struct"].to_pylist() == [None]


@pytest.mark.integration
@pytest.mark.parametrize("catalog", [pytest.lazy_fixture("session_catalog_hive"), pytest.lazy_fixture("session_catalog")])
def test_upgrade_table_version(catalog: Catalog) -> None:
    table_test_table_version = catalog.load_table("default.test_table_version")

    assert table_test_table_version.format_version == 1

    with table_test_table_version.transaction() as transaction:
        transaction.upgrade_table_version(format_version=1)

    assert table_test_table_version.format_version == 1

    with table_test_table_version.transaction() as transaction:
        transaction.upgrade_table_version(format_version=2)

    assert table_test_table_version.format_version == 2

    with pytest.raises(ValueError) as e:  # type: ignore
        with table_test_table_version.transaction() as transaction:
            transaction.upgrade_table_version(format_version=1)
    assert "Cannot downgrade v2 table to v1" in str(e.value)

    with pytest.raises(ValueError) as e:
        with table_test_table_version.transaction() as transaction:
            transaction.upgrade_table_version(format_version=3)
    assert "Unsupported table format version: 3" in str(e.value)


@pytest.mark.integration
@pytest.mark.parametrize("catalog", [pytest.lazy_fixture("session_catalog_hive"), pytest.lazy_fixture("session_catalog")])
def test_sanitize_character(catalog: Catalog) -> None:
    table_test_table_sanitized_character = catalog.load_table("default.test_table_sanitized_character")
    arrow_table = table_test_table_sanitized_character.scan().to_arrow()
    assert len(arrow_table.schema.names), 1
    assert len(table_test_table_sanitized_character.schema().fields), 1
    assert arrow_table.schema.names[0] == table_test_table_sanitized_character.schema().fields[0].name


@pytest.mark.integration
@pytest.mark.parametrize("catalog", [pytest.lazy_fixture("session_catalog_hive"), pytest.lazy_fixture("session_catalog")])
def test_null_list_and_map(catalog: Catalog) -> None:
    table_test_empty_list_and_map = catalog.load_table("default.test_table_empty_list_and_map")
    arrow_table = table_test_empty_list_and_map.scan().to_arrow()
    assert arrow_table["col_list"].to_pylist() == [None, []]
    assert arrow_table["col_map"].to_pylist() == [None, []]
    # This should be:
    # assert arrow_table["col_list_with_struct"].to_pylist() == [None, [{'test': 1}]]
    # Once https://github.com/apache/arrow/issues/38809 has been fixed
    assert arrow_table["col_list_with_struct"].to_pylist() == [[], [{"test": 1}]]


@pytest.mark.integration
def test_hive_locking(session_catalog_hive: HiveCatalog) -> None:
    table = create_table(session_catalog_hive)

    database_name: str
    table_name: str
    database_name, table_name = table.name()

    hive_client: _HiveClient = _HiveClient(session_catalog_hive.properties["uri"])
    blocking_lock_request: LockRequest = session_catalog_hive._create_lock_request(database_name, table_name)

    with hive_client as open_client:
        # Force a lock on the test table
        lock: LockResponse = open_client.lock(blocking_lock_request)
        assert lock.state == LockState.ACQUIRED
        try:
            with pytest.raises(CommitFailedException, match="(Failed to acquire lock for).*"):
                table.transaction().set_properties(lock="fail").commit_transaction()
        finally:
            open_client.unlock(UnlockRequest(lock.lockid))


@pytest.mark.integration
def test_hive_locking_with_retry(session_catalog_hive: HiveCatalog) -> None:
    table = create_table(session_catalog_hive)
    database_name: str
    table_name: str
    database_name, table_name = table.name()
    session_catalog_hive._lock_check_min_wait_time = 0.1
    session_catalog_hive._lock_check_max_wait_time = 0.5
    session_catalog_hive._lock_check_retries = 5

    hive_client: _HiveClient = _HiveClient(session_catalog_hive.properties["uri"])

    executor = ExecutorFactory.get_or_create()

    with hive_client as open_client:

        def another_task() -> None:
            lock: LockResponse = open_client.lock(session_catalog_hive._create_lock_request(database_name, table_name))
            time.sleep(1)
            open_client.unlock(UnlockRequest(lock.lockid))

        # test transaction commit with concurrent locking
        executor.submit(another_task)
        time.sleep(0.5)

        table.transaction().set_properties(lock="xxx").commit_transaction()
        assert table.properties.get("lock") == "xxx"


@pytest.mark.integration
def test_configure_row_group_batch_size(session_catalog: Catalog) -> None:
    from pyiceberg.table import TableProperties

    table_name = "default.test_small_row_groups"
    try:
        session_catalog.drop_table(table_name)
    except NoSuchTableError:
        pass  # Just to make sure that the table doesn't exist

    tbl = session_catalog.create_table(
        table_name,
        Schema(
            NestedField(1, "number", LongType()),
        ),
        properties={TableProperties.PARQUET_ROW_GROUP_LIMIT: "1"},
    )

    # Write 10 row groups, that should end up as 10 batches
    entries = 10
    tbl.append(
        pa.Table.from_pylist(
            [
                {
                    "number": number,
                }
                for number in range(entries)
            ],
        )
    )

    batches = list(tbl.scan().to_arrow_batch_reader())
    assert len(batches) == entries


@pytest.mark.integration
@pytest.mark.parametrize("catalog", [pytest.lazy_fixture("session_catalog_hive"), pytest.lazy_fixture("session_catalog")])
def test_table_scan_keep_types(catalog: Catalog) -> None:
    expected_schema = pa.schema(
        [
            pa.field("string", pa.string()),
            pa.field("string-to-binary", pa.large_binary()),
            pa.field("binary", pa.binary()),
            pa.field("list", pa.list_(pa.large_string())),
        ]
    )

    identifier = "default.test_table_scan_default_to_large_types"
    arrow_table = pa.Table.from_arrays(
        [
            pa.array(["a", "b", "c"]),
            pa.array(["a", "b", "c"]),
            pa.array([b"a", b"b", b"c"]),
            pa.array([["a", "b"], ["c", "d"], ["e", "f"]]),
        ],
        schema=expected_schema,
    )

    try:
        catalog.drop_table(identifier)
    except NoSuchTableError:
        pass

    tbl = catalog.create_table(
        identifier,
        schema=arrow_table.schema,
    )

    tbl.append(arrow_table)

    with tbl.update_schema() as update_schema:
        update_schema.update_column("string-to-binary", BinaryType())

    result_table = tbl.scan().to_arrow()
    assert result_table.schema.equals(expected_schema)


@pytest.mark.integration
@pytest.mark.parametrize("catalog", [pytest.lazy_fixture("session_catalog_hive"), pytest.lazy_fixture("session_catalog")])
def test_table_scan_override_with_small_types(catalog: Catalog) -> None:
    identifier = "default.test_table_scan_override_with_small_types"
    arrow_table = pa.Table.from_arrays(
        [
            pa.array(["a", "b", "c"]),
            pa.array(["a", "b", "c"]),
            pa.array([b"a", b"b", b"c"]),
            pa.array([["a", "b"], ["c", "d"], ["e", "f"]]),
        ],
        names=["string", "string-to-binary", "binary", "list"],
    )

    try:
        catalog.drop_table(identifier)
    except NoSuchTableError:
        pass

    tbl = catalog.create_table(
        identifier,
        schema=arrow_table.schema,
    )

    tbl.append(arrow_table)

    with tbl.update_schema() as update_schema:
        update_schema.update_column("string-to-binary", BinaryType())

    tbl.io.properties[PYARROW_USE_LARGE_TYPES_ON_READ] = "False"
    result_table = tbl.scan().to_arrow()

    expected_schema = pa.schema(
        [
            pa.field("string", pa.string()),
            pa.field("string-to-binary", pa.large_binary()),
            pa.field("binary", pa.binary()),
            pa.field("list", pa.list_(pa.string())),
        ]
    )
    assert result_table.schema.equals(expected_schema)


@pytest.mark.integration
@pytest.mark.parametrize("catalog", [pytest.lazy_fixture("session_catalog_hive"), pytest.lazy_fixture("session_catalog")])
def test_empty_scan_ordered_str(catalog: Catalog) -> None:
    table_empty_scan_ordered_str = catalog.load_table("default.test_empty_scan_ordered_str")
    arrow_table = table_empty_scan_ordered_str.scan(EqualTo("id", "b")).to_arrow()
    assert len(arrow_table) == 0


@pytest.mark.integration
@pytest.mark.parametrize("catalog", [pytest.lazy_fixture("session_catalog_hive"), pytest.lazy_fixture("session_catalog")])
def test_table_scan_empty_table(catalog: Catalog) -> None:
    identifier = "default.test_table_scan_empty_table"
    arrow_table = pa.Table.from_arrays(
        [
            pa.array([]),
        ],
        schema=pa.schema([pa.field("colA", pa.string())]),
    )

    try:
        catalog.drop_table(identifier)
    except NoSuchTableError:
        pass

    tbl = catalog.create_table(
        identifier,
        schema=arrow_table.schema,
    )

    tbl.append(arrow_table)

    result_table = tbl.scan().to_arrow()

    assert len(result_table) == 0


@pytest.mark.integration
@pytest.mark.parametrize("catalog", [pytest.lazy_fixture("session_catalog_hive"), pytest.lazy_fixture("session_catalog")])
def test_read_from_s3_and_local_fs(catalog: Catalog, tmp_path: PosixPath) -> None:
    identifier = "default.test_read_from_s3_and_local_fs"
    schema = pa.schema([pa.field("colA", pa.string())])
    arrow_table = pa.Table.from_arrays([pa.array(["one"])], schema=schema)

    tmp_dir = tmp_path / "data"
    tmp_dir.mkdir()
    local_file = tmp_dir / "local_file.parquet"

    try:
        catalog.drop_table(identifier)
    except NoSuchTableError:
        pass
    tbl = catalog.create_table(identifier, schema=schema)

    # Append table to s3 endpoint
    tbl.append(arrow_table)

    # Append a local file
    pq.write_table(arrow_table, local_file)
    tbl.add_files([str(local_file)])

    result_table = tbl.scan().to_arrow()
    assert result_table["colA"].to_pylist() == ["one", "one"]


@pytest.mark.integration
@pytest.mark.parametrize("catalog", [pytest.lazy_fixture("session_catalog_hive"), pytest.lazy_fixture("session_catalog")])
def test_scan_with_datetime(catalog: Catalog) -> None:
    table = create_table(catalog)

    yesterday = datetime.now() - timedelta(days=1)
    table.append(
        pa.Table.from_pylist(
            [
                {
                    "str": "foo",
                    "int": 1,
                    "bool": True,
                    "datetime": yesterday,
                }
            ],
            schema=table.schema().as_arrow(),
        ),
    )

    df = table.scan(row_filter=GreaterThanOrEqual("datetime", yesterday)).to_pandas()
    assert len(df) == 1

    df = table.scan(row_filter=LessThan("datetime", yesterday)).to_pandas()
    assert len(df) == 0


@pytest.mark.integration
# TODO: For Hive we require writing V3
# @pytest.mark.parametrize("catalog", [pytest.lazy_fixture("session_catalog_hive"), pytest.lazy_fixture("session_catalog")])
@pytest.mark.parametrize("catalog", [pytest.lazy_fixture("session_catalog")])
def test_initial_default(catalog: Catalog, spark: SparkSession) -> None:
    identifier = "default.test_initial_default"
    try:
        catalog.drop_table(identifier)
    except NoSuchTableError:
        pass

    one_column = pa.table([pa.nulls(10, pa.int32())], names=["some_field"])

    tbl = catalog.create_table(identifier, schema=one_column.schema, properties={"format-version": "2"})

    tbl.append(one_column)

    # Do the bump version through Spark, since PyIceberg does not support this (yet)
    spark.sql(f"ALTER TABLE {identifier} SET TBLPROPERTIES('format-version'='3')")

    with tbl.update_schema() as upd:
        upd.add_column("so_true", BooleanType(), required=False, default_value=True)

    result_table = tbl.scan().filter("so_true == True").to_arrow()

    assert len(result_table) == 10


@pytest.mark.integration
@pytest.mark.parametrize("catalog", [pytest.lazy_fixture("session_catalog_hive"), pytest.lazy_fixture("session_catalog")])
def test_filter_after_arrow_scan(catalog: Catalog) -> None:
    identifier = "test_partitioned_by_hours"
    table = catalog.load_table(f"default.{identifier}")

    scan = table.scan()
    assert len(scan.to_arrow()) > 0

    scan = scan.filter("ts >= '2023-03-05T00:00:00+00:00'")
    assert len(scan.to_arrow()) > 0
