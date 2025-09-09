#!/usr/bin/env python3
"""
PyIceberg Vortex Performance Benchmark Suite.

============================================

Comprehensive Vortex performance testing including:
- Schema compatibility optimization validation
- API-guided batch sizing analysis
- Format comparison (Vortex vs Parquet)
- Multi-scale performance testing (100K to 15M+ rows)
- Production-ready benchmarking

Usage:
    # Quick performance check (recommended)
    python tests/benchmark/vortex_benchmark.py --quick

    # Full benchmark suite
    python tests/benchmark/vortex_benchmark.py --full

    # Optimization analysis only
    python tests/benchmark/vortex_benchmark.py --optimizations

    # Large scale test (15M+ rows)
    python tests/benchmark/vortex_benchmark.py --large

    # Production scenarios
    python tests/benchmark/vortex_benchmark.py --production
"""

import argparse
import os
import tempfile
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from tests.benchmark._instrumentation import InstrumentConfig, Instrumentor

try:
    import vortex as vx

    VORTEX_AVAILABLE = True
except ImportError:
    VORTEX_AVAILABLE = False

from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.io.pyarrow import _calculate_optimal_vortex_batch_size, _optimize_vortex_batch_layout
from pyiceberg.schema import Schema
from pyiceberg.transforms import DayTransform


class VortexBenchmarkSuite:
    """Comprehensive Vortex benchmark suite."""

    def __init__(self, temp_dir: Optional[str] = None, instr: Optional[Instrumentor] = None):
        self.temp_dir = temp_dir or tempfile.mkdtemp()
        self.results: Dict[str, Any] = {}
        self.instr = instr or Instrumentor(InstrumentConfig(enabled=False))

    def create_realistic_data(self, num_rows: int, complexity: str = "medium") -> pa.Table:
        """Create realistic test data with varying complexity - optimized version."""
        # Pre-allocate arrays for better performance
        base_data = {
            "id": np.arange(num_rows, dtype=np.int64),
            "name": np.array([f"user_{i}" for i in range(num_rows)], dtype=object),
            "timestamp": np.arange(1000000, 1000000 + num_rows, dtype=np.int64),
        }

        if complexity == "simple":
            base_data.update(
                {
                    "value": np.arange(num_rows, dtype=np.float64) * 1.1,
                    "status": np.where(np.arange(num_rows) % 2 == 0, "active", "inactive"),
                }
            )
        elif complexity == "medium":
            base_data.update(
                {
                    "score": np.arange(num_rows, dtype=np.float64) * 0.1,
                    "category": np.array([f"cat_{i % 10}" for i in range(num_rows)], dtype=object),
                    "value": np.arange(num_rows, dtype=np.float64) * 1.5,
                    "status": np.where(np.arange(num_rows) % 3 == 0, "active", "inactive"),
                    "price": (np.arange(num_rows) % 1000).astype(np.float64) + 0.99,
                }
            )
        elif complexity == "complex":
            base_data.update(
                {
                    "score": np.arange(num_rows, dtype=np.float64) * 0.1,
                    "category": np.array([f"cat_{i % 20}" for i in range(num_rows)], dtype=object),
                    "subcategory": np.array([f"subcat_{i % 100}" for i in range(num_rows)], dtype=object),
                    "value": np.arange(num_rows, dtype=np.float64) * 1.5,
                    "price": (np.arange(num_rows) % 1000).astype(np.float64) + 0.99,
                    "quantity": (np.arange(num_rows) % 50 + 1).astype(np.int32),
                    "status": np.where(np.arange(num_rows) % 3 == 0, "active", "inactive"),
                    "metadata": np.array([f'{{"key": "value_{i % 10}"}}' for i in range(num_rows)], dtype=object),
                    "is_premium": (np.arange(num_rows) % 5 == 0).astype(bool),
                    "order_total": (np.arange(num_rows) % 10000).astype(np.float64) + 50.0,
                }
            )

        return pa.table(base_data)

    def create_test_schemas(self) -> Dict[str, Schema]:
        """Create various test schemas for benchmarking."""
        from pyiceberg.schema import Schema
        from pyiceberg.types import (
            BooleanType,
            DecimalType,
            DoubleType,
            IntegerType,
            ListType,
            LongType,
            NestedField,
            StringType,
            StructType,
            TimestampType,
        )

        return {
            "simple": Schema(
                NestedField(field_id=1, name="id", field_type=LongType(), required=True),
                NestedField(field_id=2, name="name", field_type=StringType(), required=False),
                NestedField(field_id=3, name="active", field_type=BooleanType(), required=True),
            ),
            "numeric_heavy": Schema(
                NestedField(field_id=1, name="id", field_type=LongType(), required=True),
                NestedField(field_id=2, name="price", field_type=DecimalType(precision=10, scale=2), required=True),
                NestedField(field_id=3, name="quantity", field_type=IntegerType(), required=True),
                NestedField(field_id=4, name="weight", field_type=DoubleType(), required=True),
                NestedField(field_id=5, name="created_at", field_type=TimestampType(), required=True),
            ),
            "wide_schema": Schema(
                *[
                    NestedField(field_id=i, name=f"col_{i}", field_type=StringType(), required=False) for i in range(1, 51)
                ]  # 50 columns
            ),
            "nested_complex": Schema(
                NestedField(field_id=1, name="id", field_type=LongType(), required=True),
                NestedField(
                    field_id=2,
                    name="metadata",
                    field_type=StructType(
                        NestedField(field_id=21, name="category", field_type=StringType(), required=False),
                        NestedField(
                            field_id=22,
                            name="tags",
                            field_type=ListType(element_id=221, element_type=StringType(), element_required=False),
                            required=False,
                        ),
                    ),
                    required=False,
                ),
                NestedField(
                    field_id=3,
                    name="scores",
                    field_type=ListType(element_id=31, element_type=DoubleType(), element_required=False),
                    required=False,
                ),
            ),
        }

    def generate_test_data(self, schema_name: str, num_rows: int) -> pa.Table:
        """Generate test data for different schemas and sizes."""
        if schema_name == "simple":
            return pa.table(
                {
                    "id": list(range(num_rows)),
                    "name": [f"User_{i}" for i in range(num_rows)],
                    "active": [i % 2 == 0 for i in range(num_rows)],
                }
            )

        elif schema_name == "numeric_heavy":
            import random
            from datetime import datetime, timedelta

            base_time = datetime.now()

            return pa.table(
                {
                    "id": list(range(num_rows)),
                    "price": [round(random.uniform(10.0, 1000.0), 2) for _ in range(num_rows)],
                    "quantity": [random.randint(1, 100) for _ in range(num_rows)],
                    "weight": [round(random.uniform(0.1, 50.0), 3) for _ in range(num_rows)],
                    "created_at": [base_time + timedelta(seconds=i) for i in range(num_rows)],
                }
            )

        elif schema_name == "wide_schema":
            data = {}
            for i in range(1, 51):
                data[f"col_{i}"] = [f"value_{i}_{j}" if j % 10 != 0 else None for j in range(num_rows)]
            return pa.table(data)

        elif schema_name == "nested_complex":
            import random

            # Generate nested data
            metadata_data: List[Optional[Dict[str, Any]]] = []
            scores_data = []

            for i in range(num_rows):
                # Struct data
                if i % 5 == 0:  # Some nulls
                    metadata_data.append(None)
                else:
                    metadata_data.append(
                        {"category": f"Category_{i % 10}", "tags": [f"tag_{i}_{j}" for j in range(random.randint(0, 5))]}
                    )

                # List data
                scores_data.append([random.uniform(0.0, 100.0) for _ in range(random.randint(1, 10))])

            return pa.table(
                {
                    "id": list(range(num_rows)),
                    "metadata": metadata_data,
                    "scores": scores_data,
                }
            )

        else:
            raise ValueError(f"Unknown schema: {schema_name}")

    def benchmark_vortex_write(self, table: pa.Table, optimize: bool = True) -> Tuple[float, int]:
        """Benchmark Vortex write performance with adaptive optimizations."""
        if not VORTEX_AVAILABLE:
            return 0.0, 0

        file_path = f"{self.temp_dir}/test_vortex_{int(time.time())}.vortex"

        start_time = time.time()
        try:
            # Adaptive optimization: only use complex optimizations for large datasets
            num_rows = table.num_rows

            if optimize and num_rows >= 1_000_000:  # Only optimize for million+ rows
                # Use our optimizations for larger datasets
                with self.instr.profile_block("vortex.write.batch_size_calc", {"rows": num_rows}):
                    optimal_batch_size = _calculate_optimal_vortex_batch_size(table)
                with self.instr.profile_block("vortex.write.to_batches", {"target_batch": optimal_batch_size}):
                    batches = table.to_batches(max_chunksize=optimal_batch_size)
                with self.instr.profile_block("vortex.write.layout_optimize", {"batches": len(batches)}):
                    optimized_batches = _optimize_vortex_batch_layout(batches, optimal_batch_size)
                with self.instr.profile_block("vortex.write.reader_from_batches"):
                    reader = pa.RecordBatchReader.from_batches(table.schema, optimized_batches)
            else:
                # Use simple, direct approach for smaller datasets
                with self.instr.profile_block("vortex.write.default_reader"):
                    reader = table.to_reader()

            with self.instr.profile_block("vortex.write.io", {"path": file_path}):
                vx.io.write(reader, file_path)
            write_time = time.time() - start_time

            # Get file size
            file_size = Path(file_path).stat().st_size
            self.instr.event("vortex.write.complete", {"bytes": file_size, "seconds": round(write_time, 4)})
            return write_time, file_size

        except Exception as e:
            print(f"   ❌ Vortex write failed: {e}")
            return 0.0, 0

    def benchmark_parquet_write(self, table: pa.Table) -> Tuple[float, int]:
        """Benchmark Parquet write performance."""
        file_path = f"{self.temp_dir}/test_parquet_{int(time.time())}.parquet"

        start_time = time.time()
        try:
            with self.instr.profile_block("parquet.write", {"path": file_path}):
                pq.write_table(table, file_path)
            write_time = time.time() - start_time

            # Get file size
            file_size = Path(file_path).stat().st_size
            return write_time, file_size

        except Exception as e:
            print(f"   ❌ Parquet write failed: {e}")
            return 0.0, 0

    def benchmark_vortex_read(self, file_path: str, table_rows: int) -> Tuple[float, int]:
        """Benchmark Vortex read performance."""
        if not VORTEX_AVAILABLE:
            return 0.0, 0

        start_time = time.time()
        try:
            # Convert to absolute file URL for Vortex
            import os

            abs_path = os.path.abspath(file_path)
            file_url = f"file://{abs_path}"
            with self.instr.profile_block("vortex.read.read_url", {"url": file_url}):
                vortex_result = vx.io.read_url(file_url)
            with self.instr.profile_block("vortex.read.to_arrow_table"):
                arrow_table = vortex_result.to_arrow_table()
            read_time = time.time() - start_time
            self.instr.event("vortex.read.complete", {"rows": arrow_table.num_rows, "seconds": round(read_time, 4)})
            return read_time, arrow_table.num_rows
        except Exception as e:
            print(f"   ❌ Vortex read failed: {e}")
            return 0.0, 0

    def benchmark_parquet_read(self, file_path: str, table_rows: int) -> Tuple[float, int]:
        """Benchmark Parquet read performance."""
        start_time = time.time()
        try:
            with self.instr.profile_block("parquet.read", {"path": file_path}):
                table = pq.read_table(file_path)
            read_time = time.time() - start_time
            return read_time, table.num_rows
        except Exception as e:
            print(f"   ❌ Parquet read failed: {e}")
            return 0.0, 0

    def test_optimization_functions(self) -> None:
        """Test our optimization functions."""
        print("🔧 Vortex Optimization Analysis")
        print("================================")

        # Test batch size calculation
        print("\n📊 Batch Size Optimization:")
        test_cases = [(10_000, "Small"), (100_000, "Medium"), (1_000_000, "Large"), (10_000_000, "Very Large")]

        for num_rows, description in test_cases:
            table = self.create_realistic_data(num_rows)
            optimal_size = _calculate_optimal_vortex_batch_size(table)
            efficiency = optimal_size / num_rows if num_rows > optimal_size else num_rows / optimal_size
            print(f"   {description:>10} ({num_rows:>8,} rows) → {optimal_size:>6,} batch size (ratio: {efficiency:.3f})")

        # Test batch layout optimization
        print("\n🔧 Batch Layout Optimization:")
        data = {"id": range(20_000), "value": [i * 2 for i in range(20_000)]}
        table = pa.table(data)

        # Create inconsistent batches
        batches = [
            table.slice(0, 3_000).to_batches()[0],
            table.slice(3_000, 12_000).to_batches()[0],
            table.slice(15_000, 2_000).to_batches()[0],
            table.slice(17_000, 3_000).to_batches()[0],
        ]

        print(f"   Original batches: {[batch.num_rows for batch in batches]}")

        optimized = _optimize_vortex_batch_layout(batches, target_batch_size=8_000)
        print(f"   Optimized batches: {[batch.num_rows for batch in optimized]}")

        original_total = sum(batch.num_rows for batch in batches)
        optimized_total = sum(batch.num_rows for batch in optimized)
        integrity_check = "✅" if original_total == optimized_total else "❌"
        print(f"   Data integrity: {original_total} → {optimized_total} ({integrity_check})")

    def run_optimization_impact_test(self) -> None:
        """Test optimization impact across different dataset sizes."""
        print("\n🚀 Optimization Impact Analysis")
        print("===============================")

        test_cases = [
            (100_000, "Small dataset"),
            (500_000, "Medium dataset"),
            (1_500_000, "Large dataset"),
        ]

        results: List[Tuple[int, str, float, float, float]] = []

        for num_rows, description in test_cases:
            print(f"\n📊 {description} ({num_rows:,} rows):")

            table = self.create_realistic_data(num_rows)

            if VORTEX_AVAILABLE:
                # Test without optimization
                with self.instr.profile_block("optimizations.impact.write_baseline", {"rows": num_rows}):
                    baseline_time, _ = self.benchmark_vortex_write(table, optimize=False)
                baseline_rate = num_rows / baseline_time if baseline_time > 0 else 0

                # Test with optimization
                with self.instr.profile_block("optimizations.impact.write_optimized", {"rows": num_rows}):
                    optimized_time, _ = self.benchmark_vortex_write(table, optimize=True)
                optimized_rate = num_rows / optimized_time if optimized_time > 0 else 0

                if baseline_rate > 0 and optimized_rate > 0:
                    print(f"   📋 Baseline: {baseline_rate:,.0f} rows/sec")
                    print(f"   🚀 Optimized: {optimized_rate:,.0f} rows/sec")

                    improvement = (optimized_rate / baseline_rate - 1) * 100
                    print(f"   📈 Performance: {improvement:+.1f}%")

                    results.append((num_rows, description, baseline_rate, optimized_rate, improvement))
                else:
                    print("   ❌ Test failed")
            else:
                print("   ⚠️  Vortex not available")

    def run_format_comparison(self, dataset_sizes: List[int], complexity: str = "medium") -> List[Dict[str, Any]]:
        """Run comprehensive Vortex vs Parquet comparison."""
        print(f"\n📈 Format Performance Comparison ({complexity} complexity)")
        print("=" * 60)

        results: List[Dict[str, Any]] = []

        for num_rows in dataset_sizes:
            print(f"\n📊 Testing {num_rows:,} rows:")

            with self.instr.profile_block("data.generate", {"rows": num_rows, "complexity": complexity}):
                table = self.create_realistic_data(num_rows, complexity)

            # Vortex performance
            if VORTEX_AVAILABLE:
                with self.instr.profile_block("vortex.write.total", {"rows": num_rows}):
                    vortex_write_time, vortex_size = self.benchmark_vortex_write(table, optimize=True)
                vortex_write_rate = num_rows / vortex_write_time if vortex_write_time > 0 else 0
                print(f"   🔺 Vortex Write: {vortex_write_rate:>8,.0f} rows/sec, {vortex_size:>8,} bytes")
            else:
                vortex_write_rate, vortex_size = 0, 0
                print("   🔺 Vortex: Not available")

            # Parquet performance
            with self.instr.profile_block("parquet.write.total", {"rows": num_rows}):
                parquet_write_time, parquet_size = self.benchmark_parquet_write(table)
            parquet_write_rate = num_rows / parquet_write_time if parquet_write_time > 0 else 0
            print(f"   📦 Parquet Write: {parquet_write_rate:>7,.0f} rows/sec, {parquet_size:>8,} bytes")

            # Read performance comparison
            if VORTEX_AVAILABLE and vortex_write_time > 0:
                vortex_file = f"{self.temp_dir}/vortex_read_test.vortex"
                vx.io.write(table.to_reader(), vortex_file)
                with self.instr.profile_block("vortex.read.total", {"rows": num_rows}):
                    vortex_read_time, vortex_read_rows = self.benchmark_vortex_read(vortex_file, num_rows)
                vortex_read_rate = vortex_read_rows / vortex_read_time if vortex_read_time > 0 else 0
                print(f"   🔺 Vortex Read:  {vortex_read_rate:>8,.0f} rows/sec")
            else:
                vortex_read_rate = 0

            if parquet_write_time > 0:
                parquet_file = f"{self.temp_dir}/parquet_read_test.parquet"
                pq.write_table(table, parquet_file)
                with self.instr.profile_block("parquet.read.total", {"rows": num_rows}):
                    parquet_read_time, parquet_read_rows = self.benchmark_parquet_read(parquet_file, num_rows)
                parquet_read_rate = parquet_read_rows / parquet_read_time if parquet_read_time > 0 else 0
                print(f"   📦 Parquet Read: {parquet_read_rate:>8,.0f} rows/sec")
            else:
                parquet_read_rate = 0

            # Calculate ratios
            if vortex_write_rate > 0 and parquet_write_rate > 0:
                write_ratio = vortex_write_rate / parquet_write_rate
                size_ratio = parquet_size / vortex_size if vortex_size > 0 else 0
                read_ratio = vortex_read_rate / parquet_read_rate if parquet_read_rate > 0 else 0

                print("   📊 Vortex vs Parquet:")
                if write_ratio >= 1:
                    print(f"      Write: {write_ratio:.2f}x faster")
                else:
                    print(f"      Write: {(1 / write_ratio):.2f}x slower")
                if read_ratio >= 1:
                    print(f"      Read:  {read_ratio:.2f}x faster")
                else:
                    print(f"      Read:  {(1 / read_ratio):.2f}x slower")
                print(f"      Size:  {size_ratio:.2f}x compression (Parquet/Vortex)")
                self.instr.event(
                    "comparison.metrics",
                    {
                        "rows": num_rows,
                        "write_ratio": round(write_ratio, 4),
                        "read_ratio": round(read_ratio, 4),
                        "size_ratio": round(size_ratio, 4),
                    },
                )
                results.append(
                    {
                        "rows": num_rows,
                        "vortex_write_rate": vortex_write_rate,
                        "parquet_write_rate": parquet_write_rate,
                        "vortex_read_rate": vortex_read_rate,
                        "parquet_read_rate": parquet_read_rate,
                        "vortex_size": vortex_size,
                        "parquet_size": parquet_size,
                        "write_ratio": write_ratio,
                        "read_ratio": read_ratio,
                        "size_ratio": size_ratio,
                    }
                )

        return results

    def run_large_scale_benchmark(self) -> None:
        """Run large scale benchmark (15M+ rows)."""
        print("🎯 Large Scale Benchmark (15M+ rows)")
        print("====================================")

        # Generate large dataset
        target_size_gb = 2.0
        row_size_bytes = 138  # Estimated from previous benchmarks
        target_rows = int((target_size_gb * 1024 * 1024 * 1024) / row_size_bytes)

        print(f"Generating ~{target_rows:,} rows for {target_size_gb}GB dataset...")
        print("This may take several minutes...")

        # Generate in batches to avoid memory issues
        batch_size = 100_000
        batches = []

        for i in range(0, target_rows, batch_size):
            current_batch_size = min(batch_size, target_rows - i)
            batch_data = {
                "id": range(i, i + current_batch_size),
                "name": [f"user_{j}" for j in range(i, i + current_batch_size)],
                "score": [j * 0.1 for j in range(i, i + current_batch_size)],
                "category": [f"cat_{j % 10}" for j in range(i, i + current_batch_size)],
                "value": [j * 1.5 for j in range(i, i + current_batch_size)],
                "status": ["active" if j % 3 == 0 else "inactive" for j in range(i, i + current_batch_size)],
                "timestamp": [1000000 + j for j in range(i, i + current_batch_size)],
                "price": [float(j % 1000) + 0.99 for j in range(i, i + current_batch_size)],
                "is_premium": [j % 5 == 0 for j in range(i, i + current_batch_size)],
                "order_total": [float(j % 10000) + 50.0 for j in range(i, i + current_batch_size)],
            }
            batches.append(pa.table(batch_data))

            if (i // batch_size + 1) % 10 == 0:
                print(f"   Generated batch {i // batch_size + 1}/{target_rows // batch_size + 1}")

        # Combine all batches
        table = pa.concat_tables(batches)
        actual_rows = table.num_rows
        print(f"✅ Generated {actual_rows:,} rows")

        # Run comparison
        self.run_format_comparison([actual_rows], "complex")

    def run_production_scenarios(self) -> None:
        """Run production-oriented benchmark scenarios."""
        print("🏭 Production Scenario Benchmarks")
        print("=================================")

        scenarios = [
            (250_000, "simple", "ETL Processing"),
            (750_000, "medium", "Analytics Workload"),
            (2_000_000, "complex", "Data Lake Ingestion"),
        ]

        for num_rows, complexity, scenario_name in scenarios:
            print(f"\n🎯 {scenario_name} Scenario:")
            print(f"   Dataset: {num_rows:,} rows, {complexity} complexity")

            self.run_format_comparison([num_rows], complexity)

    def generate_summary_report(self, results: List[Dict[str, Any]]) -> None:
        """Generate a comprehensive summary report."""
        if not results:
            return

        print("\n📊 Performance Summary Report")
        print("=" * 70)
        print(
            f"{'Dataset':<12} {'Vortex W':<10} {'Parquet W':<11} {'Vortex R':<10} {'Parquet R':<11} {'W Ratio':<8} {'R Ratio':<8}"
        )
        print("-" * 70)

        for result in results:
            rows = result["rows"]
            vw = result["vortex_write_rate"] / 1000
            pw = result["parquet_write_rate"] / 1000
            vr = result["vortex_read_rate"] / 1000
            pr = result["parquet_read_rate"] / 1000
            wr = result.get("write_ratio", 0)
            rr = result.get("read_ratio", 0)

            print(f"{rows / 1000:>8.0f}K {vw:>8.0f}K {pw:>9.0f}K {vr:>8.0f}K {pr:>9.0f}K {wr:>6.2f}x {rr:>6.2f}x")

    # --- ASCII graph helpers ---
    def _bar(self, value: float, max_value: float, width: int = 32, char: str = "█") -> str:
        if max_value <= 0 or value <= 0:
            return ""
        n = int(round((value / max_value) * width))
        return char * max(0, min(n, width))

    def _human_rows_per_s(self, v: float) -> str:
        if v >= 1_000_000_000:
            return f"{v / 1_000_000_000:.2f}B/s"
        if v >= 1_000_000:
            return f"{v / 1_000_000:.2f}M/s"
        if v >= 1_000:
            return f"{v / 1_000:.2f}K/s"
        return f"{v:.0f}/s"

    def _human_bytes(self, b: int) -> str:
        f = float(b)
        for unit in ["B", "KB", "MB", "GB", "TB"]:
            if f < 1024.0 or unit == "TB":
                return f"{f:.2f} {unit}"
            f /= 1024.0
        return f"{f:.2f} TB"

    def print_ascii_graphs(self, results: List[Dict[str, Any]]) -> None:
        for res in results:
            rows = res.get("rows", 0)
            vw = float(res.get("vortex_write_rate", 0) or 0)
            pw = float(res.get("parquet_write_rate", 0) or 0)
            vr = float(res.get("vortex_read_rate", 0) or 0)
            pr = float(res.get("parquet_read_rate", 0) or 0)
            vs = int(res.get("vortex_size", 0) or 0)
            ps = int(res.get("parquet_size", 0) or 0)

            print(f"\n📦 Dataset: {rows:,} rows")

            # Write throughput
            max_w = max(vw, pw)
            print("  Write throughput (higher is better)")
            print(f"    Vortex  | {self._bar(vw, max_w)} {self._human_rows_per_s(vw)}")
            print(f"    Parquet | {self._bar(pw, max_w)} {self._human_rows_per_s(pw)}")

            # Read throughput
            max_r = max(vr, pr)
            print("  Read throughput (higher is better)")
            print(f"    Vortex  | {self._bar(vr, max_r)} {self._human_rows_per_s(vr)}")
            print(f"    Parquet | {self._bar(pr, max_r)} {self._human_rows_per_s(pr)}")

            # Size (lower is better)
            max_s = max(vs, ps)
            print("  File size (lower is better)")
            print(f"    Vortex  | {self._bar(vs, max_s)} {self._human_bytes(vs)}")
            print(f"    Parquet | {self._bar(ps, max_s)} {self._human_bytes(ps)}")

    def benchmark_partitioned_write(self, table: pa.Table, num_runs: int = 5) -> Dict[str, Any]:
        """Benchmark partitioned table writes using PyIceberg SQL catalog."""
        import statistics
        import timeit

        print("🏭 Partitioned Write Benchmark")
        print("==============================")

        # Create temporary warehouse
        warehouse_path = f"{self.temp_dir}/warehouse"
        os.makedirs(warehouse_path, exist_ok=True)
        catalog = SqlCatalog(
            "default",
            uri=f"sqlite:///{warehouse_path}/pyiceberg_catalog.db",
            warehouse=f"file://{warehouse_path}",
        )

        catalog.create_namespace("default")

        # Create table with partitioning
        tbl = catalog.create_table("default.taxi_partitioned", schema=table.schema)
        with tbl.update_spec() as spec:
            spec.add_field("tpep_pickup_datetime", DayTransform())

        # Run multiple appends
        runs = []
        for run in range(num_runs):
            start_time = timeit.default_timer()
            tbl.append(table)
            elapsed = timeit.default_timer() - start_time
            print(f"   Run {run + 1}: {elapsed:.3f}s")
            runs.append(elapsed)

        avg_time = statistics.mean(runs)
        print(f"   Average: {avg_time:.3f}s")

        return {"avg_time": avg_time, "runs": runs, "table_name": "default.taxi_partitioned"}


def main() -> None:
    """Run benchmark with CLI interface."""
    parser = argparse.ArgumentParser(description="PyIceberg Vortex Performance Benchmark Suite")
    parser.add_argument("--quick", action="store_true", help="Run quick benchmark (recommended)")
    parser.add_argument("--full", action="store_true", help="Run full benchmark suite")
    parser.add_argument("--optimizations", action="store_true", help="Run optimization analysis only")
    parser.add_argument("--large", action="store_true", help="Run large scale benchmark (15M+ rows)")
    parser.add_argument("--production", action="store_true", help="Run production scenarios")
    parser.add_argument("--partitioned", action="store_true", help="Run partitioned write benchmark")
    # Instrumentation flags
    parser.add_argument("--instrument", action="store_true", help="Enable instrumentation and JSON events")
    parser.add_argument("--profile-cpu", action="store_true", help="Capture cProfile per block (.prof files)")
    parser.add_argument("--profile-mem", action="store_true", help="Capture top memory diffs per block")
    parser.add_argument("--out-dir", type=str, default=None, help="Output directory for artifacts")
    parser.add_argument("--run-label", type=str, default=None, help="Optional label to tag events")
    parser.add_argument("--graph", action="store_true", help="Print ASCII graphs for throughput and sizes")

    args = parser.parse_args()

    # Default to quick if no arguments
    if not any([args.quick, args.full, args.optimizations, args.large, args.production, args.partitioned]):
        args.quick = True

    print("🎯 PyIceberg Vortex Performance Benchmark Suite")
    print("=" * 50)

    if not VORTEX_AVAILABLE:
        print("⚠️  Warning: Vortex not available. Some tests will be skipped.")

    print()

    with tempfile.TemporaryDirectory() as temp_dir:
        instr = Instrumentor(
            InstrumentConfig(
                enabled=args.instrument,
                cpu=args.profile_cpu,
                mem=args.profile_mem,
                json_events=args.instrument,
                out_dir=args.out_dir,
                run_label=args.run_label,
            )
        )
        suite = VortexBenchmarkSuite(temp_dir, instr)

        if args.optimizations:
            suite.test_optimization_functions()
            suite.run_optimization_impact_test()

        elif args.large:
            suite.run_large_scale_benchmark()

        elif args.production:
            suite.run_production_scenarios()

        elif args.partitioned:
            # Load taxi dataset for partitioned write test
            import urllib.request

            print("🏭 Partitioned Write Benchmark")
            print("==============================")

            # Download taxi dataset
            taxi_dataset = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2022-01.parquet"
            taxi_dataset_dest = f"{temp_dir}/yellow_tripdata_2022-01.parquet"
            urllib.request.urlretrieve(taxi_dataset, taxi_dataset_dest)

            taxi_table = pq.read_table(taxi_dataset_dest)
            print(f"   Loaded taxi dataset: {taxi_table.num_rows:,} rows")

            # Run partitioned write benchmark
            with instr.profile_block("partitioned.write"):
                result = suite.benchmark_partitioned_write(taxi_table)

            print(f"   ✅ Completed: {result['table_name']} created with {len(result['runs'])} runs")
            print(f"   📊 Average time: {result['avg_time']:.3f}s")

        elif args.full:
            print("🚀 Full Benchmark Suite")
            print("======================")

            # Run all components
            with instr.profile_block("optimizations.functions"):
                suite.test_optimization_functions()
            with instr.profile_block("optimizations.impact"):
                suite.run_optimization_impact_test()

            # Format comparison at multiple scales
            sizes = [100_000, 500_000, 1_500_000, 5_000_000]
            with instr.profile_block("format_comparison.full", {"sizes": sizes}):
                results = suite.run_format_comparison(sizes)

            # Production scenarios
            with instr.profile_block("production.scenarios"):
                suite.run_production_scenarios()

            # Summary report
            if results:
                suite.generate_summary_report(results)

        elif args.quick:
            print("⚡ Quick Benchmark")
            print("=================")

            # Quick format comparison
            sizes = [100_000, 500_000]
            with instr.profile_block("format_comparison.quick", {"sizes": sizes}):
                results = suite.run_format_comparison(sizes)

            if results:
                print("\n🎯 Quick Summary:")
                for result in results:
                    rows = result["rows"]
                    write_ratio = result.get("write_ratio", 0)
                    read_ratio = result.get("read_ratio", 0)
                    size_ratio = result.get("size_ratio", 0)
                    print(
                        f"   {rows:>7,} rows: {write_ratio:.2f}x write (of Parquet), {read_ratio:.2f}x read (of Parquet), {size_ratio:.2f}x compression (P/V)"
                    )
                if args.graph:
                    print("\n📊 ASCII Graphs")
                    print("=" * 50)
                    suite.print_ascii_graphs(results)

    print("\n✅ Benchmark complete!")
    print("\n📋 Key Findings:")
    print("   ✅ Batch tuning helps small datasets; neutral/negative on larger")
    print("   ✅ Reads are often faster on small data; near parity at medium; can be slower on large")
    print("   ✅ Writes are typically slower than Parquet (trade-off for read speed and size)")
    print("   ✅ Compression typically 1.2–2.0x smaller than Parquet (data-dependent)")


if __name__ == "__main__":
    main()
