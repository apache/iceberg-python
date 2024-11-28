from pyiceberg.table import Table, TableMetadata, DataScan
from pyiceberg.catalog.sql import SqlCatalog

def get_plan(table: Table) -> DataScan:
    return table.scan()


def test_iceberg_count():
    table = _create_iceberg_metadata()
    assert len(table.to_arrow()) == 2


def test_iceberg_metadata_only_count():
    table = _create_iceberg_metadata()
    assert table.count() == 2
