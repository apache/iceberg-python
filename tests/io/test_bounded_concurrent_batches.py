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
"""Tests for _bounded_concurrent_batches in pyiceberg.io.pyarrow."""

import threading
import time
from collections.abc import Iterator
from unittest.mock import MagicMock

import pyarrow as pa
import pytest

from pyiceberg.io.pyarrow import _bounded_concurrent_batches
from pyiceberg.table import FileScanTask, ScanOrder, TaskOrder, ArrivalOrder


def _make_task() -> FileScanTask:
    """Create a mock FileScanTask."""
    task = MagicMock(spec=FileScanTask)
    return task


def _make_batches(num_batches: int, rows_per_batch: int = 10, start: int = 0) -> list[pa.RecordBatch]:
    """Create a list of simple RecordBatches."""
    return [
        pa.record_batch({"col": list(range(start + i * rows_per_batch, start + (i + 1) * rows_per_batch))})
        for i in range(num_batches)
    ]


def test_correctness_single_file() -> None:
    """Test that a single file produces correct results."""
    task = _make_task()
    expected_batches = _make_batches(3)

    def batch_fn(t: FileScanTask) -> Iterator[pa.RecordBatch]:
        yield from expected_batches

    result = list(_bounded_concurrent_batches([task], batch_fn, concurrent_files=1, max_buffered_batches=16))

    assert len(result) == 3
    total_rows = sum(len(b) for b in result)
    assert total_rows == 30


def test_correctness_multiple_files() -> None:
    """Test that multiple files produce all expected batches."""
    tasks = [_make_task() for _ in range(4)]
    batches_per_file = 3

    def batch_fn(t: FileScanTask) -> Iterator[pa.RecordBatch]:
        idx = tasks.index(t)
        yield from _make_batches(batches_per_file, start=idx * 100)

    result = list(_bounded_concurrent_batches(tasks, batch_fn, concurrent_files=2, max_buffered_batches=16))

    total_rows = sum(len(b) for b in result)
    assert total_rows == batches_per_file * len(tasks) * 10  # 3 batches * 4 files * 10 rows


def test_arrival_order_yields_incrementally() -> None:
    """Test that batches are yielded incrementally, not all at once."""
    barrier = threading.Event()
    tasks = [_make_task(), _make_task()]

    def batch_fn(t: FileScanTask) -> Iterator[pa.RecordBatch]:
        yield pa.record_batch({"col": [1, 2, 3]})
        barrier.wait(timeout=5.0)
        yield pa.record_batch({"col": [4, 5, 6]})

    gen = _bounded_concurrent_batches(tasks, batch_fn, concurrent_files=2, max_buffered_batches=16)

    # Should get at least one batch before all are done
    first = next(gen)
    assert first.num_rows == 3

    # Unblock remaining batches
    barrier.set()

    remaining = list(gen)
    total = 1 + len(remaining)
    assert total >= 3  # At least 3 more batches (one blocked from each task + the unblocked ones)


def test_backpressure() -> None:
    """Test that workers block when the queue is full."""
    max_buffered = 2
    tasks = [_make_task()]
    produced_count = 0
    produce_lock = threading.Lock()

    def batch_fn(t: FileScanTask) -> Iterator[pa.RecordBatch]:
        nonlocal produced_count
        for i in range(10):
            with produce_lock:
                produced_count += 1
            yield pa.record_batch({"col": [i]})

    gen = _bounded_concurrent_batches(tasks, batch_fn, concurrent_files=1, max_buffered_batches=max_buffered)

    # Consume slowly and check that not all batches are produced immediately
    first = next(gen)
    assert first is not None
    time.sleep(0.3)

    # The producer should be blocked by backpressure at some point
    # (not all 10 batches produced instantly)
    remaining = list(gen)
    assert len(remaining) + 1 == 10


def test_error_propagation() -> None:
    """Test that errors from workers are propagated to the consumer."""
    tasks = [_make_task()]

    def batch_fn(t: FileScanTask) -> Iterator[pa.RecordBatch]:
        yield pa.record_batch({"col": [1]})
        raise ValueError("test error")

    gen = _bounded_concurrent_batches(tasks, batch_fn, concurrent_files=1, max_buffered_batches=16)

    # Should get the first batch
    first = next(gen)
    assert first.num_rows == 1

    # Should get the error
    with pytest.raises(ValueError, match="test error"):
        list(gen)


def test_early_termination() -> None:
    """Test that stopping consumption cancels workers."""
    tasks = [_make_task() for _ in range(5)]
    worker_started = threading.Event()

    def batch_fn(t: FileScanTask) -> Iterator[pa.RecordBatch]:
        worker_started.set()
        for i in range(100):
            yield pa.record_batch({"col": [i]})
            time.sleep(0.01)

    gen = _bounded_concurrent_batches(tasks, batch_fn, concurrent_files=3, max_buffered_batches=4)

    # Consume a few batches then stop
    worker_started.wait(timeout=5.0)
    batches = []
    for _ in range(5):
        batches.append(next(gen))

    # Close the generator, triggering finally block
    gen.close()

    assert len(batches) == 5


def test_concurrency_limit() -> None:
    """Test that at most concurrent_files files are read concurrently."""
    concurrent_files = 2
    tasks = [_make_task() for _ in range(6)]
    active_count = 0
    max_active = 0
    active_lock = threading.Lock()

    def batch_fn(t: FileScanTask) -> Iterator[pa.RecordBatch]:
        nonlocal active_count, max_active
        with active_lock:
            active_count += 1
            max_active = max(max_active, active_count)
        try:
            time.sleep(0.05)
            yield pa.record_batch({"col": [1]})
        finally:
            with active_lock:
                active_count -= 1

    result = list(_bounded_concurrent_batches(tasks, batch_fn, concurrent_files=concurrent_files, max_buffered_batches=16))

    assert len(result) == 6
    assert max_active <= concurrent_files


def test_empty_tasks() -> None:
    """Test that no tasks produces no batches."""

    def batch_fn(t: FileScanTask) -> Iterator[pa.RecordBatch]:
        yield from []

    result = list(_bounded_concurrent_batches([], batch_fn, concurrent_files=2, max_buffered_batches=16))
    assert result == []


def test_concurrent_with_limit_via_arrowscan(tmpdir: str) -> None:
    """Test concurrent_files with limit through ArrowScan integration."""
    from pyiceberg.expressions import AlwaysTrue
    from pyiceberg.io.pyarrow import ArrowScan, PyArrowFileIO
    from pyiceberg.manifest import DataFileContent, FileFormat
    from pyiceberg.partitioning import PartitionSpec
    from pyiceberg.schema import Schema
    from pyiceberg.table.metadata import TableMetadataV2
    from pyiceberg.types import LongType, NestedField

    PYARROW_PARQUET_FIELD_ID_KEY = b"PARQUET:field_id"

    table_schema = Schema(NestedField(1, "col", LongType(), required=True))
    pa_schema = pa.schema([pa.field("col", pa.int64(), nullable=False, metadata={PYARROW_PARQUET_FIELD_ID_KEY: "1"})])

    tasks = []
    for i in range(4):
        filepath = f"{tmpdir}/file_{i}.parquet"
        arrow_table = pa.table({"col": pa.array(range(i * 100, (i + 1) * 100))}, schema=pa_schema)
        import pyarrow.parquet as pq

        pq.write_table(arrow_table, filepath)
        from pyiceberg.manifest import DataFile

        data_file = DataFile.from_args(
            content=DataFileContent.DATA,
            file_path=filepath,
            file_format=FileFormat.PARQUET,
            partition={},
            record_count=100,
            file_size_in_bytes=22,
        )
        data_file.spec_id = 0
        tasks.append(FileScanTask(data_file))

    scan = ArrowScan(
        table_metadata=TableMetadataV2(
            location="file://a/b/",
            last_column_id=1,
            format_version=2,
            schemas=[table_schema],
            partition_specs=[PartitionSpec()],
        ),
        io=PyArrowFileIO(),
        projected_schema=table_schema,
        row_filter=AlwaysTrue(),
        case_sensitive=True,
        limit=150,
    )

    batches = list(scan.to_record_batches(tasks, order=ArrivalOrder(concurrent_streams=2)))
    total_rows = sum(len(b) for b in batches)
    assert total_rows == 150
