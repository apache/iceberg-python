"""
Lightweight instrumentation utilities for benchmarking.

Features:
- Timing context manager that records duration_ms per block
- Optional CPU profiling via cProfile (writes .prof files)
- Optional memory profiling via tracemalloc (captures top lines)
- JSON Lines event logging for later aggregation/analysis

Usage:
    from tests.benchmark._instrumentation import InstrumentConfig, Instrumentor

    instr = Instrumentor(InstrumentConfig(enabled=True, cpu=True, mem=True, json_events=True, out_dir="/tmp/out"))
    with instr.profile_block("vortex_write"):
        ...

    instr.event("custom_marker", {"rows": 100_000})
"""

from __future__ import annotations

import contextlib
import cProfile
import json
import os
import time
import tracemalloc
from dataclasses import dataclass
from pathlib import Path
from tracemalloc import Statistic, StatisticDiff
from typing import Any, Dict, Iterator, Optional, Sequence


@dataclass
class InstrumentConfig:
    enabled: bool = False
    cpu: bool = False
    mem: bool = False
    json_events: bool = False
    out_dir: Optional[str] = None
    run_label: Optional[str] = None


class Instrumentor:
    def __init__(self, config: InstrumentConfig) -> None:
        self.config = config
        self._json_path: Optional[str] = None
        # Determine data dir - first try environment, then workspace default
        user_data_dir = os.environ.get("BENCHMARK_DATA_DIR")
        if user_data_dir:
            self.data_dir = Path(user_data_dir)
        else:
            # Default to benchmark_data in project root
            current_file = Path(__file__)
            project_root = current_file.parents[2]  # Go up from tests/benchmark/
            self.data_dir = project_root / "benchmark_data"

        # Initialize JSON logging if enabled
        if self.config.enabled and self.config.json_events:
            self._ensure_out_dir()
            out_dir = self.config.out_dir
            if out_dir is not None:
                self._json_path = str(Path(out_dir) / "events.jsonl")

    def _ensure_out_dir(self) -> None:
        if not self.config.out_dir:
            # Default to current working dir under .bench_out/<ts>
            ts = time.strftime("%Y%m%d-%H%M%S")
            self.config.out_dir = str(Path.cwd() / ".bench_out" / ts)
        out_dir = self.config.out_dir
        if out_dir is not None:
            Path(out_dir).mkdir(parents=True, exist_ok=True)

    def _cpu_prof_path(self, block_name: str) -> Path:
        safe = block_name.replace(" ", "_").replace("/", "_")
        out_dir = self.config.out_dir
        if out_dir is None:
            raise ValueError("Output directory not configured")
        return Path(out_dir) / f"{safe}.prof"

    def _mem_txt_path(self, block_name: str) -> Path:
        safe = block_name.replace(" ", "_").replace("/", "_")
        out_dir = self.config.out_dir
        if out_dir is None:
            raise ValueError("Output directory not configured")
        return Path(out_dir) / f"{safe}.mem.txt"

    @contextlib.contextmanager
    def profile_block(self, name: str, extra: Optional[Dict[str, Any]] = None) -> Iterator[None]:
        start = time.perf_counter()
        prof: Optional[cProfile.Profile] = None
        mem_before: Optional[tracemalloc.Snapshot] = None
        if self.config.enabled and self.config.cpu:
            try:
                prof = cProfile.Profile()
                prof.enable()
            except ValueError:
                # Another profiler is active; skip CPU profiling for this block
                prof = None
        if self.config.enabled and self.config.mem and tracemalloc.is_tracing():
            mem_before = tracemalloc.take_snapshot()

        try:
            yield
        finally:
            duration_ms = (time.perf_counter() - start) * 1000.0

            cpu_profile_path: Optional[str] = None
            if prof is not None:
                prof.disable()
                cpu_path = self._cpu_prof_path(name)
                prof.dump_stats(str(cpu_path))
                cpu_profile_path = str(cpu_path)

            mem_top: Optional[str] = None
            if self.config.enabled and self.config.mem and tracemalloc.is_tracing():
                try:
                    mem_after = tracemalloc.take_snapshot()
                    if mem_before is not None:
                        stats: Sequence[Statistic | StatisticDiff] = mem_after.compare_to(mem_before, "lineno")
                    else:
                        stats = mem_after.statistics("lineno")
                    # Summarize top 5 lines - use common attributes available on both types
                    top_lines = []
                    for stat in stats[:5]:
                        if hasattr(stat, "traceback"):
                            # Both Statistic and StatisticDiff have traceback, size, count
                            top_lines.append(
                                f"{stat.traceback.format()[-1].strip()} - size={stat.size / 1024:.1f} KiB, count={stat.count}"
                            )
                    mem_top = "\n".join(top_lines)
                    # Also write to a side file for convenience
                    with open(self._mem_txt_path(name), "w", encoding="utf-8") as f:
                        f.write(mem_top or "")
                except Exception:
                    # Best-effort memory capture
                    mem_top = None

            payload: Dict[str, Any] = {
                "ts": time.time(),
                "label": self.config.run_label,
                "block": name,
                "duration_ms": round(duration_ms, 3),
                "cpu_profile": cpu_profile_path,
                "mem_top": mem_top,
            }
            if extra:
                payload.update(extra)

            if self._json_path is not None:
                with open(self._json_path, "a", encoding="utf-8") as f:
                    f.write(json.dumps(payload) + "\n")

    def event(self, name: str, data: Optional[Dict[str, Any]] = None) -> None:
        if not (self.config.enabled and self._json_path is not None):
            return
        payload: Dict[str, Any] = {
            "ts": time.time(),
            "label": self.config.run_label,
            "event": name,
        }
        if data:
            payload.update(data)
        with open(self._json_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(payload) + "\n")
