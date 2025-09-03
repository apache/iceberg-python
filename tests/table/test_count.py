import pytest
from unittest.mock import MagicMock, Mock, patch
from pyiceberg.table import DataScan
from pyiceberg.expressions import AlwaysTrue

class DummyFile:
    def __init__(self, record_count):
        self.record_count = record_count

class DummyTask:
    def __init__(self, record_count, residual=None, delete_files=None):
        self.file = DummyFile(record_count)
        self.residual = residual if residual is not None else AlwaysTrue()
        self.delete_files = delete_files or []

def test_count_basic():
    # Create a mock table with the necessary attributes
    table = Mock(spec=DataScan)

    # Mock the plan_files method to return our dummy task
    task = DummyTask(42, residual=AlwaysTrue(), delete_files=[])
    table.plan_files = MagicMock(return_value=[task])

    # Import and call the actual count method
    from pyiceberg.table import DataScan as ActualDataScan
    table.count = ActualDataScan.count.__get__(table, ActualDataScan)

    assert table.count() == 42

def test_count_empty():
    # Create a mock table with the necessary attributes
    table = Mock(spec=DataScan)

    # Mock the plan_files method to return no tasks
    table.plan_files = MagicMock(return_value=[])

    # Import and call the actual count method
    from pyiceberg.table import DataScan as ActualDataScan
    table.count = ActualDataScan.count.__get__(table, ActualDataScan)

    assert table.count() == 0

def test_count_large():
    # Create a mock table with the necessary attributes
    table = Mock(spec=DataScan)

    # Mock the plan_files method to return multiple tasks
    tasks = [
        DummyTask(500000, residual=AlwaysTrue(), delete_files=[]),
        DummyTask(500000, residual=AlwaysTrue(), delete_files=[]),
    ]
    table.plan_files = MagicMock(return_value=tasks)

    # Import and call the actual count method
    from pyiceberg.table import DataScan as ActualDataScan
    table.count = ActualDataScan.count.__get__(table, ActualDataScan)

    assert table.count() == 1000000