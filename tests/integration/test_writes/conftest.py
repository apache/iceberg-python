import pyarrow as pa
import pytest

from utils import TEST_DATA_WITH_NULL


@pytest.fixture(scope="session")
def pa_schema() -> pa.Schema:
    return pa.schema([
        ("bool", pa.bool_()),
        ("string", pa.string()),
        ("string_long", pa.string()),
        ("int", pa.int32()),
        ("long", pa.int64()),
        ("float", pa.float32()),
        ("double", pa.float64()),
        ("timestamp", pa.timestamp(unit="us")),
        ("timestamptz", pa.timestamp(unit="us", tz="UTC")),
        ("date", pa.date32()),
        # Not supported by Spark
        # ("time", pa.time64("us")),
        # Not natively supported by Arrow
        # ("uuid", pa.fixed(16)),
        ("binary", pa.large_binary()),
        ("fixed", pa.binary(16)),
    ])


@pytest.fixture(scope="session")
def arrow_table_with_null(pa_schema: pa.Schema) -> pa.Table:
    """PyArrow table with all kinds of columns."""
    return pa.Table.from_pydict(TEST_DATA_WITH_NULL, schema=pa_schema)


@pytest.fixture(scope="session")
def arrow_table_without_data(pa_schema: pa.Schema) -> pa.Table:
    """PyArrow table with all kinds of columns."""
    return pa.Table.from_pylist([], schema=pa_schema)


@pytest.fixture(scope="session")
def arrow_table_with_only_nulls(pa_schema: pa.Schema) -> pa.Table:
    """PyArrow table with all kinds of columns."""
    return pa.Table.from_pylist([{}, {}], schema=pa_schema)
