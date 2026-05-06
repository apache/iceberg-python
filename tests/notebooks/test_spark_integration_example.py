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

import textwrap
from pathlib import Path

import nbformat
import papermill as pm
import pytest

pytestmark = pytest.mark.notebook

NOTEBOOK_PATH = Path(__file__).parents[2] / "notebooks" / "spark_integration_example.ipynb"

# ---------------------------------------------------------------------------
# Mock pyspark
# Replaces pyspark.sql.SparkSession with a fake one
# ---------------------------------------------------------------------------
_MOCK_PYSPARK = textwrap.dedent("""\
    import sys
    import types
    from unittest.mock import MagicMock

    def _make_fake_pyspark():
        pyspark_mod = types.ModuleType("pyspark")
        sql_mod     = types.ModuleType("pyspark.sql")
        pyspark_mod.sql = sql_mod
        sys.modules.setdefault("pyspark",     pyspark_mod)
        sys.modules.setdefault("pyspark.sql", sql_mod)
        return pyspark_mod, sql_mod

    _pyspark, _sql = _make_fake_pyspark()

    _SHOW_CATALOGS = (
        "+-------------+\\n"
        "|catalogName  |\\n"
        "+-------------+\\n"
        "|spark_catalog|\\n"
        "|local        |\\n"
        "+-------------+\\n"
    )
    _SHOW_NAMESPACES = (
        "+---------+\\n"
        "|namespace|\\n"
        "+---------+\\n"
        "|default  |\\n"
        "+---------+\\n"
    )
    _SHOW_TABLES = (
        "+---------+-----------+-----------+\\n"
        "|namespace|tableName  |isTemporary|\\n"
        "+---------+-----------+-----------+\\n"
        "|default  |test_all   |false      |\\n"
        "+---------+-----------+-----------+\\n"
    )
    _DESCRIBE_TABLE = (
        "+--------------------+---------+-------+\\n"
        "|col_name            |data_type|comment|\\n"
        "+--------------------+---------+-------+\\n"
        "|boolean_col         |boolean  |null   |\\n"
        "|integer_col         |integer  |null   |\\n"
        "+--------------------+---------+-------+\\n"
    )
    _SQL_RESPONSES = {
        "SHOW CATALOGS":                        _SHOW_CATALOGS,
        "SHOW NAMESPACES":                       _SHOW_NAMESPACES,
        "SHOW TABLES FROM default":              _SHOW_TABLES,
        "DESCRIBE TABLE default.test_all_types": _DESCRIBE_TABLE,
    }

    def _make_df(output):
        df = MagicMock()
        df.show.side_effect = lambda *a, **kw: print(output, end="")
        return df

    class _FakeBuilder:
        def remote(self, url): return self
        def getOrCreate(self): return _FakeSession()

    class _FakeSession:
        builder = _FakeBuilder()
        def sql(self, query):
            key = query.strip().rstrip(";")
            output = _SQL_RESPONSES.get(key, "+------+\\n| col  |\\n+------+\\n| val  |\\n+------+\\n")
            return _make_df(output)

    _FakeSparkSession = MagicMock(spec=object)
    _FakeSparkSession.builder = _FakeBuilder()
    _sql.SparkSession = _FakeSparkSession
""")


def get_all_stdout(nb: nbformat.NotebookNode) -> str:
    """Concatenate all stdout streams from every executed cell."""
    return "".join(
        out.get("text", "")
        for cell in nb.cells
        for out in cell.get("outputs", [])
        if out.get("output_type") == "stream" and out.get("name") == "stdout"
    )


def _inject_mock_and_execute(notebook_path: Path, output_path: Path) -> nbformat.NotebookNode:
    """
    Load the real notebook, prepend the mock-pyspark setup cell, write to a
    temporary copy and execute it with papermill.
    """
    nb = nbformat.read(str(notebook_path), as_version=4)

    mock_cell = nbformat.v4.new_code_cell(_MOCK_PYSPARK)
    mock_cell.metadata["tags"] = ["injected-mock"]
    nb.cells.insert(0, mock_cell)

    patched_path = output_path.parent / "spark_patched.ipynb"
    nbformat.write(nb, str(patched_path))

    return pm.execute_notebook(str(patched_path), str(output_path), kernel_name="python3")


@pytest.fixture(scope="session")
def spark_nb(tmp_path_factory: pytest.TempPathFactory) -> nbformat.NotebookNode:
    out = tmp_path_factory.mktemp("nb_out") / "spark_integration_example_out.ipynb"
    return _inject_mock_and_execute(NOTEBOOK_PATH, out)


class TestSmoke:
    def test_notebook_completes_without_error(self, spark_nb: nbformat.NotebookNode) -> None:
        assert spark_nb is not None

    def test_all_code_cells_executed(self, spark_nb: nbformat.NotebookNode) -> None:
        for cell in spark_nb.cells:
            if cell.cell_type == "code":
                assert cell.get("execution_count") is not None, f"Cell not executed:\n{cell.source[:80]}"


class TestCellOutputs:
    def test_show_catalogs_lists_spark_catalog_and_local(self, spark_nb: nbformat.NotebookNode) -> None:
        stdout = get_all_stdout(spark_nb)
        assert "spark_catalog" in stdout
        assert "local" in stdout

    def test_show_namespaces_contains_default(self, spark_nb: nbformat.NotebookNode) -> None:
        assert "default" in get_all_stdout(spark_nb)

    def test_show_tables_produces_tabular_output(self, spark_nb: nbformat.NotebookNode) -> None:
        assert "+---------+-----------+-----------+" in get_all_stdout(spark_nb)

    def test_describe_table_lists_column_names(self, spark_nb: nbformat.NotebookNode) -> None:
        assert "col_name" in get_all_stdout(spark_nb)

    def test_describe_table_lists_data_types(self, spark_nb: nbformat.NotebookNode) -> None:
        stdout = get_all_stdout(spark_nb)
        assert "boolean" in stdout or "integer" in stdout

    def test_show_tables_includes_test_table_row(self, spark_nb: nbformat.NotebookNode) -> None:
        assert "test_all" in get_all_stdout(spark_nb)
