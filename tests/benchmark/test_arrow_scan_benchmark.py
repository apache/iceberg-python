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
"""Benchmarks for ArrowScan.to_record_batches characteristics.

Run with:
    PYTHONPATH=. uv run pytest tests/benchmark/test_arrow_scan_benchmark.py -m benchmark -s
"""

from __future__ import annotations

import gc
import itertools
import os
import queue
import random
import statistics
import threading
import time
from collections.abc import Iterable, Iterator, Sequence
from concurrent.futures import Executor
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from pyiceberg.expressions import AlwaysTrue
from pyiceberg.io.pyarrow import ArrowScan, PyArrowFileIO, _read_all_delete_files
from pyiceberg.manifest import DataFile, DataFileContent, FileFormat
from pyiceberg.partitioning import PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table import FileScanTask
from pyiceberg.table.metadata import TableMetadataV2
from pyiceberg.typedef import Record
from pyiceberg.types import IntegerType, NestedField, StringType
from pyiceberg.utils.concurrent import ExecutorFactory


@dataclass(frozen=True)
class ScanStrategyMetrics:
    rows: int
    batches: int
    full_scan_time_ms: float
    peak_arrow_mb: float
    peak_rss_delta_mb: float


@dataclass(frozen=True)
class BenchmarkScenario:
    max_workers: int | None


@dataclass(frozen=True)
class BenchmarkImplementation:
    name: str
    scan_class: type[ArrowScan]


@dataclass(frozen=True)
class BenchmarkSummary:
    implementation: str
    worker_setting: str
    effective_workers: int
    num_files: int
    file_mb_avg: float
    total_rows: int
    total_batches: int
    full_scan_time_ms_avg: float
    full_scan_time_ms_max: float
    arr_peak_mb_avg: float
    arr_peak_mb_max: float
    rss_peak_delta_mb_avg: float
    rss_peak_delta_mb_max: float


@dataclass(frozen=True)
class BenchmarkDataset:
    parquet_files: list[Path]
    file_sizes_mb: list[float]


class LazyArrowScan(ArrowScan):
    # https://github.com/apache/iceberg-python/pull/2676
    def to_record_batches(self, tasks: Iterable[FileScanTask]) -> Iterator[pa.RecordBatch]:
        deletes_per_file = _read_all_delete_files(self._io, tasks)

        total_row_count = 0

        limit_reached = False
        for task in tasks:
            batches = self._record_batches_from_scan_tasks_and_deletes([task], deletes_per_file)
            for batch in batches:
                current_batch_size = len(batch)
                if self._limit is not None and total_row_count + current_batch_size >= self._limit:
                    yield batch.slice(0, self._limit - total_row_count)

                    limit_reached = True
                    break
                else:
                    yield batch
                    total_row_count += current_batch_size

            if limit_reached:
                # This break will also cancel all running tasks in the executor
                break


class LazyWarmupArrowScan(ArrowScan):
    def to_record_batches(self, tasks: Iterable[FileScanTask]) -> Iterator[pa.RecordBatch]:
        deletes_per_file = _read_all_delete_files(self._io, tasks)

        total_row_count = 0
        executor = ExecutorFactory.get_or_create()

        def warmup_batches_for_task(task: FileScanTask) -> Iterator[pa.RecordBatch]:
            batches = self._record_batches_from_scan_tasks_and_deletes([task], deletes_per_file)
            first_batch = next(batches, None)
            if first_batch is None:
                return iter(())
            return itertools.chain([first_batch], batches)

        for batches in executor.map(warmup_batches_for_task, tasks):
            for batch in batches:
                current_batch_size = len(batch)
                if self._limit is not None and total_row_count + current_batch_size >= self._limit:
                    yield batch.slice(0, self._limit - total_row_count)
                    return

                yield batch
                total_row_count += current_batch_size


class BoundedQueueArrowScan(ArrowScan):
    # https://github.com/apache/iceberg-python/pull/3046
    _QUEUE_SENTINEL = object()

    def _bounded_concurrent_batches(
        self,
        tasks: Sequence[FileScanTask],
        deletes_per_file: dict[str, list[object]],
        executor: Executor,
        concurrent_streams: int,
        max_buffered_batches: int = 16,
    ) -> Iterator[pa.RecordBatch]:
        if not tasks:
            return

        batch_queue: queue.Queue[pa.RecordBatch | BaseException | object] = queue.Queue(maxsize=max_buffered_batches)
        cancel = threading.Event()
        task_iter = iter(tasks)
        active_workers = 0

        def worker(task: FileScanTask) -> None:
            try:
                batches = self._record_batches_from_scan_tasks_and_deletes([task], deletes_per_file)
                for batch in batches:
                    if cancel.is_set():
                        return

                    while not cancel.is_set():
                        try:
                            batch_queue.put(batch, timeout=0.1)
                            break
                        except queue.Full:
                            continue
            except BaseException as exc:
                if not cancel.is_set():
                    batch_queue.put(exc)
            finally:
                batch_queue.put(self._QUEUE_SENTINEL)

        def submit_next_task() -> bool:
            nonlocal active_workers

            if cancel.is_set():
                return False

            task = next(task_iter, None)
            if task is None:
                return False

            executor.submit(worker, task)
            active_workers += 1
            return True

        for _ in range(max(1, concurrent_streams)):
            if not submit_next_task():
                break

        try:
            while active_workers > 0:
                item = batch_queue.get()

                if item is self._QUEUE_SENTINEL:
                    active_workers -= 1
                    submit_next_task()
                    continue

                if isinstance(item, BaseException):
                    raise item

                if isinstance(item, pa.RecordBatch):
                    yield item
        finally:
            cancel.set()
            while active_workers > 0:
                item = batch_queue.get()
                if item is self._QUEUE_SENTINEL:
                    active_workers -= 1

    def to_record_batches(self, tasks: Iterable[FileScanTask]) -> Iterator[pa.RecordBatch]:
        tasks_list = list(tasks)
        deletes_per_file = _read_all_delete_files(self._io, tasks_list)

        total_row_count = 0
        executor = ExecutorFactory.get_or_create()
        configured_workers = os.getenv("PYICEBERG_MAX_WORKERS")
        concurrent_streams = (
            max(1, int(configured_workers))
            if configured_workers is not None
            else max(1, int(getattr(executor, "_max_workers", 1)))
        )
        max_buffered_batches = max(16, concurrent_streams)

        for batch in self._bounded_concurrent_batches(
            tasks=tasks_list,
            deletes_per_file=deletes_per_file,
            executor=executor,
            concurrent_streams=concurrent_streams,
            max_buffered_batches=max_buffered_batches,
        ):
            current_batch_size = len(batch)
            if self._limit is not None and total_row_count + current_batch_size >= self._limit:
                if self._limit - total_row_count > 0:
                    yield batch.slice(0, self._limit - total_row_count)
                return

            yield batch
            total_row_count += current_batch_size


def _format_markdown_table(headers: Sequence[str], rows: Sequence[Sequence[str]]) -> str:
    if not headers:
        raise ValueError("headers must not be empty")

    if any(len(row) != len(headers) for row in rows):
        raise ValueError("every row must have the same number of columns as headers")

    widths = [max(len(headers[idx]), *(len(row[idx]) for row in rows)) for idx in range(len(headers))]

    def _format_row(values: Sequence[str]) -> str:
        return "| " + " | ".join(value.ljust(widths[idx]) for idx, value in enumerate(values)) + " |"

    separator = "| " + " | ".join("-" * width for width in widths) + " |"
    table_rows = [_format_row(headers), separator, *(_format_row(row) for row in rows)]
    return "\n".join(table_rows)


def _print_comparison_table(summaries: Sequence[BenchmarkSummary]) -> None:
    print("\n--- ArrowScan.to_record_batches Benchmark (Comparison) ---")
    print(
        f"runs_per_shape={RUNS_PER_SHAPE}, warmup_runs_per_shape={WARMUP_RUNS_PER_SHAPE}, "
        f"sleep_between_scenarios_sec={SLEEP_BETWEEN_SCENARIOS_SEC}, "
        f"files={NUM_FILES}, target_file_size_mb={TARGET_FILE_SIZE_MB} "
        "(memory only: arr_mb, rss_delta_mb)"
    )

    headers = [
        "implementation",
        "worker_setting",
        "num_files",
        "file_size_mb_avg",
        "total_rows",
        "total_batches",
        "full_scan_time_ms_avg",
        "full_scan_time_ms_max",
        "arrow_peak_mb_avg",
        "rss_peak_delta_mb_avg",
        "arrow_peak_mb_max",
        "rss_peak_delta_mb_max",
    ]

    sorted_summaries = sorted(summaries, key=lambda summary: (summary.effective_workers, summary.implementation))

    rows = [
        [
            summary.implementation,
            summary.worker_setting,
            str(summary.num_files),
            f"{summary.file_mb_avg:.2f}",
            str(summary.total_rows),
            str(summary.total_batches),
            f"{summary.full_scan_time_ms_avg:.2f}",
            f"{summary.full_scan_time_ms_max:.2f}",
            f"{summary.arr_peak_mb_avg:.2f}",
            f"{summary.rss_peak_delta_mb_avg:.2f}",
            f"{summary.arr_peak_mb_max:.2f}",
            f"{summary.rss_peak_delta_mb_max:.2f}",
        ]
        for summary in sorted_summaries
    ]
    print(_format_markdown_table(headers, rows))


def _plot_comparison_graphs(summaries: Sequence[BenchmarkSummary]) -> None:
    try:
        import matplotlib.pyplot as plt
    except ModuleNotFoundError:
        print("matplotlib not installed; skipping benchmark graphs")
        return

    output_dir = Path("tests/benchmark/artifacts")
    output_dir.mkdir(parents=True, exist_ok=True)

    implementations = sorted({summary.implementation for summary in summaries})
    worker_ticks = sorted({summary.effective_workers for summary in summaries})

    def _display_name(implementation: str) -> str:
        return implementation.replace("_", " ").title()

    def _annotate_points(axis: Any, x_values: Sequence[int], y_values: Sequence[float], color: str) -> None:
        for x_value, y_value in zip(x_values, y_values, strict=True):
            axis.annotate(
                f"{y_value:.1f}",
                xy=(x_value, y_value),
                xytext=(0, 6),
                textcoords="offset points",
                ha="center",
                va="bottom",
                fontsize=7,
                color=color,
            )

    average_metric_specs = [
        ("full_scan_time_ms_avg", "Full Scan Time Avg", "Time (ms)"),
        ("arr_peak_mb_avg", "Arrow Peak Avg", "Memory (MB)"),
    ]

    fig, axes = plt.subplots(1, 2, figsize=(14, 5), constrained_layout=True)

    color_cycle = plt.rcParams["axes.prop_cycle"].by_key().get("color", [])
    if not color_cycle:
        color_cycle = ["C0", "C1", "C2", "C3"]

    implementation_series = {
        implementation: sorted(
            [summary for summary in summaries if summary.implementation == implementation],
            key=lambda summary: (summary.effective_workers, summary.worker_setting),
        )
        for implementation in implementations
    }

    for axis, (metric_name, metric_title, y_label) in zip(axes, average_metric_specs, strict=True):
        for idx, implementation in enumerate(implementations):
            color = color_cycle[idx % len(color_cycle)]
            implementation_rows = implementation_series[implementation]
            worker_values = [summary.effective_workers for summary in implementation_rows]
            metric_values = [float(getattr(summary, metric_name)) for summary in implementation_rows]
            axis.plot(
                worker_values,
                metric_values,
                marker="o",
                color=color,
                linestyle="-",
                label=_display_name(implementation),
            )
            _annotate_points(axis, worker_values, metric_values, color)

        axis.set_title(f"{metric_title} vs Effective Workers")
        axis.set_xlabel("Effective Workers")
        axis.set_ylabel(y_label)
        axis.set_xticks(worker_ticks)
        axis.grid(True, alpha=0.3)

    axes[0].legend(fontsize=8, title="Implementation")

    plot_path = output_dir / "arrow_scan_benchmark_relationships.png"
    fig.savefig(plot_path, dpi=160)
    plt.close(fig)
    print(f"saved graph: {plot_path}")


def _reset_executor_factory() -> None:
    ExecutorFactory._instance = None
    ExecutorFactory._instance_pid = None


def _set_max_workers(monkeypatch: pytest.MonkeyPatch, max_workers: int | None) -> None:
    if max_workers is None or max_workers <= 0:
        monkeypatch.delenv("PYICEBERG_MAX_WORKERS", raising=False)
    else:
        monkeypatch.setenv("PYICEBERG_MAX_WORKERS", str(max_workers))
    _reset_executor_factory()


def _resolve_effective_workers() -> int:
    executor = ExecutorFactory.get_or_create()
    return max(1, int(getattr(executor, "_max_workers", 1)))


def _summarize_runs(
    implementation: BenchmarkImplementation,
    scenario: BenchmarkScenario,
    effective_workers: int,
    metrics_runs: Sequence[ScanStrategyMetrics],
    file_sizes_mb: Sequence[float],
) -> BenchmarkSummary:
    if not metrics_runs:
        raise ValueError("metrics_runs must not be empty")

    arr_mb = [metrics.peak_arrow_mb for metrics in metrics_runs]
    rss_delta_mb = [metrics.peak_rss_delta_mb for metrics in metrics_runs]
    full_scan_time_ms = [metrics.full_scan_time_ms for metrics in metrics_runs]
    total_rows = metrics_runs[-1].rows
    total_batches = metrics_runs[-1].batches

    return BenchmarkSummary(
        implementation=implementation.name,
        worker_setting=f"default ({effective_workers})" if scenario.max_workers is None else str(scenario.max_workers),
        effective_workers=effective_workers,
        num_files=NUM_FILES,
        file_mb_avg=statistics.mean(file_sizes_mb),
        total_rows=total_rows,
        total_batches=total_batches,
        full_scan_time_ms_avg=statistics.mean(full_scan_time_ms),
        full_scan_time_ms_max=max(full_scan_time_ms),
        arr_peak_mb_avg=statistics.mean(arr_mb),
        arr_peak_mb_max=max(arr_mb),
        rss_peak_delta_mb_avg=statistics.mean(rss_delta_mb),
        rss_peak_delta_mb_max=max(rss_delta_mb),
    )


def _create_benchmark_parquet_files(tmp_path: Path, batches_per_file: int) -> BenchmarkDataset:
    fixed_value_size_bytes = 384
    target_file_size_bytes = TARGET_FILE_SIZE_MB * 1024 * 1024
    row_size_estimate_bytes = fixed_value_size_bytes + 4
    rows_per_file = max(1, target_file_size_bytes // row_size_estimate_bytes)
    row_group_size = max(1, rows_per_file // max(1, batches_per_file))

    parquet_files: list[Path] = []
    file_sizes_mb: list[float] = []
    for file_idx in range(NUM_FILES):
        start_id = file_idx * rows_per_file + 1
        end_id = (file_idx + 1) * rows_per_file
        payload_chars = max(fixed_value_size_bytes - 32, 0)

        values = [
            f"file_{file_idx}_row_{row_id:08d}_" + ("0123456789abcdef" * ((payload_chars + 15) // 16))[:payload_chars]
            for row_id in range(start_id, end_id + 1)
        ]

        table = pa.table(
            {
                "id": pa.array(range(start_id, end_id + 1), type=pa.int32()),
                "value": values,
            }
        )

        parquet_path = tmp_path / f"to_record_batches_benchmark_{file_idx}.parquet"
        pq.write_table(
            table,
            str(parquet_path),
            compression="NONE",
            use_dictionary=False,
            row_group_size=row_group_size,
        )
        row_group_count = pq.ParquetFile(parquet_path).metadata.num_row_groups
        if row_group_count <= 1:
            raise ValueError(f"Expected multiple row groups per task file, got {row_group_count} for {parquet_path}")

        parquet_files.append(parquet_path)
        file_size_bytes = parquet_path.stat().st_size
        file_sizes_mb.append(file_size_bytes / (1024 * 1024))

    return BenchmarkDataset(
        parquet_files=parquet_files,
        file_sizes_mb=file_sizes_mb,
    )


def _run_scenario(
    implementation: BenchmarkImplementation,
    tmp_path: Path,
    dataset: BenchmarkDataset,
) -> ScanStrategyMetrics:
    gc.collect()

    schema = Schema(
        NestedField(1, "id", IntegerType(), required=True),
        NestedField(2, "value", StringType(), required=False),
    )
    table_metadata = TableMetadataV2(
        location=f"file://{tmp_path}/benchmark_location",
        last_column_id=2,
        format_version=2,
        schemas=[schema],
        partition_specs=[PartitionSpec()],
        properties={
            "schema.name-mapping.default": '[{"field-id": 1, "names": ["id"]}, {"field-id": 2, "names": ["value"]}]',
        },
    )

    scan = implementation.scan_class(
        table_metadata=table_metadata,
        io=PyArrowFileIO(),
        projected_schema=schema,
        row_filter=AlwaysTrue(),
        case_sensitive=True,
    )

    tasks: list[FileScanTask] = []
    for parquet_path in dataset.parquet_files:
        file_size_bytes = parquet_path.stat().st_size

        data_file = DataFile.from_args(
            content=DataFileContent.DATA,
            file_path=str(parquet_path),
            file_format=FileFormat.PARQUET,
            partition=Record(),
            file_size_in_bytes=file_size_bytes,
            sort_order_id=None,
            spec_id=0,
            equality_ids=None,
            key_metadata=None,
            record_count=0,
            column_sizes={},
            value_counts={},
            null_value_counts={},
            nan_value_counts={},
            lower_bounds={},
            upper_bounds={},
            split_offsets=None,
        )
        data_file.spec_id = 0
        tasks.append(FileScanTask(data_file=data_file))

    start_time = time.perf_counter()
    iterator = scan.to_record_batches(tasks)

    psutil = pytest.importorskip("psutil")
    process = psutil.Process()

    total_rows = 0
    total_batches = 0
    peak_arrow_bytes = pa.total_allocated_bytes()
    start_rss_bytes = process.memory_info().rss
    peak_rss_bytes = start_rss_bytes

    for batch in iterator:
        total_rows += len(batch)
        total_batches += 1
        peak_arrow_bytes = max(peak_arrow_bytes, pa.total_allocated_bytes())
        peak_rss_bytes = max(peak_rss_bytes, process.memory_info().rss)
    full_scan_time_ms = (time.perf_counter() - start_time) * 1000

    return ScanStrategyMetrics(
        rows=total_rows,
        batches=total_batches,
        full_scan_time_ms=full_scan_time_ms,
        peak_arrow_mb=peak_arrow_bytes / (1024 * 1024),
        peak_rss_delta_mb=max(peak_rss_bytes - start_rss_bytes, 0) / (1024 * 1024),
    )


RUNS_PER_SHAPE = 10
WARMUP_RUNS_PER_SHAPE = 2
SLEEP_BETWEEN_SCENARIOS_SEC = 0.5
NUM_FILES = 32
BATCHES_PER_FILE = 8
TARGET_FILE_SIZE_MB = 50
BENCHMARK_SCENARIOS = [
    BenchmarkScenario(max_workers=1),
    BenchmarkScenario(max_workers=2),
    BenchmarkScenario(max_workers=4),
    BenchmarkScenario(max_workers=8),
    BenchmarkScenario(max_workers=16),
    BenchmarkScenario(max_workers=32),
    BenchmarkScenario(max_workers=None),
]
BENCHMARK_IMPLEMENTATIONS = [
    BenchmarkImplementation(name="baseline (fully materialize all tasks)", scan_class=ArrowScan),
    BenchmarkImplementation(name="lazy", scan_class=LazyArrowScan),
    BenchmarkImplementation(name="lazy_warmup", scan_class=LazyWarmupArrowScan),
    BenchmarkImplementation(name="bounded_queue", scan_class=BoundedQueueArrowScan),
]


@pytest.mark.benchmark
def test_arrow_scan_to_record_batches_characteristics(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Benchmark to_record_batches memory characteristics on parquet files."""
    summaries: list[BenchmarkSummary] = []
    dataset = _create_benchmark_parquet_files(tmp_path, batches_per_file=BATCHES_PER_FILE)

    for implementation in random.sample(BENCHMARK_IMPLEMENTATIONS, k=len(BENCHMARK_IMPLEMENTATIONS)):
        for scenario in random.sample(BENCHMARK_SCENARIOS, k=len(BENCHMARK_SCENARIOS)):
            _set_max_workers(monkeypatch, scenario.max_workers)
            effective_workers = _resolve_effective_workers()

            for _ in range(WARMUP_RUNS_PER_SHAPE):
                _run_scenario(
                    implementation=implementation,
                    tmp_path=tmp_path,
                    dataset=dataset,
                )

            metrics_runs = [
                _run_scenario(
                    implementation=implementation,
                    tmp_path=tmp_path,
                    dataset=dataset,
                )
                for _ in range(RUNS_PER_SHAPE)
            ]
            summary = _summarize_runs(
                implementation,
                scenario,
                effective_workers,
                metrics_runs,
                dataset.file_sizes_mb,
            )
            summaries.append(summary)
            _reset_executor_factory()
            if SLEEP_BETWEEN_SCENARIOS_SEC > 0:
                time.sleep(SLEEP_BETWEEN_SCENARIOS_SEC)

    _print_comparison_table(summaries)
    _plot_comparison_graphs(summaries)
