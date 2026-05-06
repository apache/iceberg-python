from pathlib import Path

import nbformat
import papermill as pm
import pytest

pytestmark = pytest.mark.notebook

NOTEBOOK_PATH = Path(__file__).parents[2] / "notebooks" / "pyiceberg_example.ipynb"


def get_all_stdout(nb: nbformat.NotebookNode) -> str:
    """Concatenate all stdout streams from every executed cell."""
    return "".join(
        out.get("text", "")
        for cell in nb.cells
        for out in cell.get("outputs", [])
        if out.get("output_type") == "stream" and out.get("name") == "stdout"
    )


@pytest.fixture(scope="session")
def pyiceberg_nb(tmp_path_factory: pytest.TempPathFactory) -> nbformat.NotebookNode:
    out = tmp_path_factory.mktemp("nb_out") / "pyiceberg_example_out.ipynb"
    return pm.execute_notebook(str(NOTEBOOK_PATH), str(out), kernel_name="python3")


class TestSmoke:
    def test_notebook_completes_without_error(self, pyiceberg_nb: nbformat.NotebookNode) -> None:
        """papermill raises PapermillExecutionError if any cell fails."""
        assert pyiceberg_nb is not None

    def test_all_code_cells_executed(self, pyiceberg_nb: nbformat.NotebookNode) -> None:
        for cell in pyiceberg_nb.cells:
            if cell.cell_type == "code":
                assert cell.get("execution_count") is not None, f"Cell not executed:\n{cell.source[:80]}"


class TestCellOutputs:
    def test_pyiceberg_version_printed(self, pyiceberg_nb: nbformat.NotebookNode) -> None:
        assert "PyIceberg version:" in get_all_stdout(pyiceberg_nb)

    def test_warehouse_location_printed(self, pyiceberg_nb: nbformat.NotebookNode) -> None:
        stdout = get_all_stdout(pyiceberg_nb)
        assert "Warehouse location:" in stdout
        assert "iceberg_warehouse_" in stdout

    def test_catalog_loaded_successfully(self, pyiceberg_nb: nbformat.NotebookNode) -> None:
        assert "Catalog loaded successfully!" in get_all_stdout(pyiceberg_nb)

    def test_namespace_default_created(self, pyiceberg_nb: nbformat.NotebookNode) -> None:
        assert "default" in get_all_stdout(pyiceberg_nb)

    def test_rows_written_is_five(self, pyiceberg_nb: nbformat.NotebookNode) -> None:
        assert "Rows written: 5" in get_all_stdout(pyiceberg_nb)

    def test_schema_evolved_message(self, pyiceberg_nb: nbformat.NotebookNode) -> None:
        assert "Schema evolved!" in get_all_stdout(pyiceberg_nb)

    def test_tip_per_mile_column_present_after_evolution(self, pyiceberg_nb: nbformat.NotebookNode) -> None:
        assert "tip_per_mile" in get_all_stdout(pyiceberg_nb)

    def test_filter_result_is_positive(self, pyiceberg_nb: nbformat.NotebookNode) -> None:
        """The notebook prints 'Rows with tip_per_mile > 1.0: N' — N must be > 0."""
        stdout = get_all_stdout(pyiceberg_nb)
        assert "Rows with tip_per_mile > 1.0:" in stdout
        for line in stdout.splitlines():
            if "Rows with tip_per_mile > 1.0:" in line:
                count = int(line.split(":")[-1].strip())
                assert count > 0
                break

    def test_snapshot_id_printed(self, pyiceberg_nb: nbformat.NotebookNode) -> None:
        assert "Current snapshot ID:" in get_all_stdout(pyiceberg_nb)

    def test_table_history_has_entries(self, pyiceberg_nb: nbformat.NotebookNode) -> None:
        stdout = get_all_stdout(pyiceberg_nb)
        assert "Table history:" in stdout
        assert "Snapshot:" in stdout

    def test_warehouse_contains_parquet_and_metadata_files(self, pyiceberg_nb: nbformat.NotebookNode) -> None:
        stdout = get_all_stdout(pyiceberg_nb)
        assert ".parquet" in stdout
        assert ".metadata.json" in stdout
