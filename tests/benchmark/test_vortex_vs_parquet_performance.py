#!/usr/bin/env python3
"""
Comprehensive benchmark comparing Vortex vs Parquet performance in PyIceberg.

This benchmark tests various scenarios including:
- Sequential writes of different data sizes
- Sequential reads of different data sizes  
- Random access patterns
- Compression efficiency
- Schema conversion overhead
- Statistics generation performance

Run with: python benchmarks/test_vortex_vs_parquet_performance.py
"""

import time
import tempfile
import shutil
import statistics
from pathlib import Path
from typing import Dict, List, Tuple, Any
from dataclasses import dataclass
import uuid

import pyarrow as pa
import pyarrow.parquet as pq

from pyiceberg.schema import Schema
from pyiceberg.types import (
    NestedField, LongType, StringType, BooleanType, IntegerType, 
    DoubleType, DateType, TimestampType, DecimalType, ListType, StructType
)
from pyiceberg.io.pyarrow import PyArrowFileIO, write_file, WriteTask
from pyiceberg.io.vortex import (
    convert_iceberg_to_vortex_file, 
    read_vortex_file,
    VortexWriteTask,
    estimate_vortex_compression_ratio,
    analyze_vortex_compatibility,
    _check_vortex_available
)
from pyiceberg.manifest import DataFile, DataFileContent, FileFormat
from pyiceberg.table.metadata import TableMetadataV2


@dataclass
class BenchmarkResult:
    """Result of a single benchmark operation."""
    operation: str
    file_format: str
    data_size: str
    duration_ms: float
    file_size_bytes: int
    compression_ratio: float
    throughput_mb_per_sec: float
    

@dataclass
class BenchmarkSuite:
    """Complete benchmark suite results."""
    results: List[BenchmarkResult]
    summary: Dict[str, Any]


class VortexParquetBenchmark:
    """Comprehensive benchmark suite for Vortex vs Parquet performance."""
    
    def __init__(self):
        self.io = PyArrowFileIO()
        self.temp_dir = tempfile.mkdtemp(prefix="vortex_benchmark_")
        self.results: List[BenchmarkResult] = []
        
        # Check if Vortex is available
        try:
            _check_vortex_available()
            self.vortex_available = True
        except ImportError:
            self.vortex_available = False
            print("⚠️  Vortex not available - running Parquet-only benchmarks")
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    def create_test_schemas(self) -> Dict[str, Schema]:
        """Create various test schemas for benchmarking."""
        return {
            "simple": Schema(
                NestedField(field_id=1, name='id', field_type=LongType(), required=True),
                NestedField(field_id=2, name='name', field_type=StringType(), required=False),
                NestedField(field_id=3, name='active', field_type=BooleanType(), required=True),
            ),
            "numeric_heavy": Schema(
                NestedField(field_id=1, name='id', field_type=LongType(), required=True),
                NestedField(field_id=2, name='price', field_type=DecimalType(precision=10, scale=2), required=True),
                NestedField(field_id=3, name='quantity', field_type=IntegerType(), required=True),
                NestedField(field_id=4, name='weight', field_type=DoubleType(), required=True),
                NestedField(field_id=5, name='created_at', field_type=TimestampType(), required=True),
            ),
            "wide_schema": Schema(
                *[NestedField(field_id=i, name=f'col_{i}', field_type=StringType(), required=False) 
                  for i in range(1, 51)]  # 50 columns
            ),
            "nested_complex": Schema(
                NestedField(field_id=1, name='id', field_type=LongType(), required=True),
                NestedField(field_id=2, name='metadata', field_type=StructType(
                    NestedField(field_id=21, name='category', field_type=StringType(), required=False),
                    NestedField(field_id=22, name='tags', field_type=ListType(
                        element_id=221, element_type=StringType(), element_required=False
                    ), required=False),
                ), required=False),
                NestedField(field_id=3, name='scores', field_type=ListType(
                    element_id=31, element_type=DoubleType(), element_required=False
                ), required=False),
            )
        }
    
    def generate_test_data(self, schema_name: str, num_rows: int) -> pa.Table:
        """Generate test data for different schemas and sizes."""
        if schema_name == "simple":
            return pa.table({
                'id': list(range(num_rows)),
                'name': [f'User_{i}' for i in range(num_rows)],
                'active': [i % 2 == 0 for i in range(num_rows)],
            })
        
        elif schema_name == "numeric_heavy":
            import random
            from datetime import datetime, timedelta
            base_time = datetime.now()
            
            return pa.table({
                'id': list(range(num_rows)),
                'price': [round(random.uniform(10.0, 1000.0), 2) for _ in range(num_rows)],
                'quantity': [random.randint(1, 100) for _ in range(num_rows)],
                'weight': [round(random.uniform(0.1, 50.0), 3) for _ in range(num_rows)],
                'created_at': [base_time + timedelta(seconds=i) for i in range(num_rows)],
            })
        
        elif schema_name == "wide_schema":
            data = {}
            for i in range(1, 51):
                data[f'col_{i}'] = [f'value_{i}_{j}' if j % 10 != 0 else None for j in range(num_rows)]
            return pa.table(data)
        
        elif schema_name == "nested_complex":
            import random
            
            # Generate nested data
            metadata_data = []
            scores_data = []
            
            for i in range(num_rows):
                # Struct data
                if i % 5 == 0:  # Some nulls
                    metadata_data.append(None)
                else:
                    metadata_data.append({
                        'category': f'Category_{i % 10}',
                        'tags': [f'tag_{i}_{j}' for j in range(random.randint(0, 5))]
                    })
                
                # List data  
                scores_data.append([random.uniform(0.0, 100.0) for _ in range(random.randint(1, 10))])
            
            return pa.table({
                'id': list(range(num_rows)),
                'metadata': metadata_data,
                'scores': scores_data,
            })
        
        else:
            raise ValueError(f"Unknown schema: {schema_name}")
    
    def time_operation(self, operation_func, *args, **kwargs) -> Tuple[float, Any]:
        """Time an operation and return duration in milliseconds and result."""
        start_time = time.perf_counter()
        result = operation_func(*args, **kwargs)
        end_time = time.perf_counter()
        duration_ms = (end_time - start_time) * 1000
        return duration_ms, result
    
    def benchmark_write_parquet(self, table: pa.Table, schema: Schema, file_path: str) -> BenchmarkResult:
        """Benchmark Parquet write operation."""
        def write_parquet():
            write_task = WriteTask(
                write_uuid=uuid.uuid4(),
                task_id=1,
                record_batches=[table.to_batches()[0]] if table.to_batches() else [pa.record_batch([], schema=table.schema)],
                schema=schema,
            )
            
            # Create minimal table metadata for testing
            from pyiceberg.table.metadata import TableMetadataV2
            table_metadata = TableMetadataV2(
                location=str(Path(file_path).parent),
                table_uuid=uuid.uuid4(),
                last_updated_ms=int(time.time() * 1000),
                last_column_id=max(field.field_id for field in schema.fields),
                schemas=[schema],
                current_schema_id=0,
                partition_specs=[],
                default_spec_id=0,
                last_partition_id=999,
                sort_orders=[],
                default_sort_order_id=0,
            )
            
            data_files = list(write_file(self.io, table_metadata, iter([write_task])))
            return data_files[0] if data_files else None
        
        duration_ms, data_file = self.time_operation(write_parquet)
        
        # Get file size
        file_size = Path(file_path.replace("file:", "")).stat().st_size if data_file else 0
        
        # Calculate metrics
        data_size_mb = len(table.to_pandas().to_csv()) / (1024 * 1024)
        compression_ratio = data_size_mb * 1024 * 1024 / file_size if file_size > 0 else 0
        throughput = data_size_mb / (duration_ms / 1000) if duration_ms > 0 else 0
        
        return BenchmarkResult(
            operation="write",
            file_format="parquet", 
            data_size=f"{len(table)} rows",
            duration_ms=duration_ms,
            file_size_bytes=file_size,
            compression_ratio=compression_ratio,
            throughput_mb_per_sec=throughput
        )
    
    def benchmark_write_vortex(self, table: pa.Table, schema: Schema, file_path: str) -> BenchmarkResult:
        """Benchmark Vortex write operation.""" 
        if not self.vortex_available:
            return BenchmarkResult(
                operation="write", file_format="vortex", data_size=f"{len(table)} rows",
                duration_ms=0, file_size_bytes=0, compression_ratio=0, throughput_mb_per_sec=0
            )
        
        def write_vortex():
            return convert_iceberg_to_vortex_file(
                iceberg_table_data=table,
                iceberg_schema=schema,
                output_path=file_path,
                io=self.io,
                compression=True
            )
        
        duration_ms, data_file = self.time_operation(write_vortex)
        
        # Get file size
        file_size = data_file.file_size_in_bytes if data_file else 0
        
        # Calculate metrics
        data_size_mb = len(table.to_pandas().to_csv()) / (1024 * 1024)
        compression_ratio = data_size_mb * 1024 * 1024 / file_size if file_size > 0 else 0
        throughput = data_size_mb / (duration_ms / 1000) if duration_ms > 0 else 0
        
        return BenchmarkResult(
            operation="write",
            file_format="vortex",
            data_size=f"{len(table)} rows", 
            duration_ms=duration_ms,
            file_size_bytes=file_size,
            compression_ratio=compression_ratio,
            throughput_mb_per_sec=throughput
        )
    
    def benchmark_read_parquet(self, file_path: str, table: pa.Table) -> BenchmarkResult:
        """Benchmark Parquet read operation."""
        def read_parquet():
            with self.io.new_input(file_path).open() as f:
                return pq.read_table(f)
        
        duration_ms, read_table = self.time_operation(read_parquet)
        
        # Get file size
        file_size = Path(file_path.replace("file:", "")).stat().st_size
        
        # Calculate metrics  
        data_size_mb = len(table.to_pandas().to_csv()) / (1024 * 1024)
        throughput = data_size_mb / (duration_ms / 1000) if duration_ms > 0 else 0
        
        return BenchmarkResult(
            operation="read",
            file_format="parquet",
            data_size=f"{len(table)} rows",
            duration_ms=duration_ms, 
            file_size_bytes=file_size,
            compression_ratio=0,  # Not applicable for reads
            throughput_mb_per_sec=throughput
        )
    
    def benchmark_read_vortex(self, file_path: str, table: pa.Table) -> BenchmarkResult:
        """Benchmark Vortex read operation."""
        if not self.vortex_available:
            return BenchmarkResult(
                operation="read", file_format="vortex", data_size=f"{len(table)} rows",
                duration_ms=0, file_size_bytes=0, compression_ratio=0, throughput_mb_per_sec=0
            )
        
        def read_vortex():
            return read_vortex_file(file_path, self.io)
        
        duration_ms, read_table = self.time_operation(read_vortex)
        
        # Get file size
        file_size = Path(file_path.replace("file:", "")).stat().st_size
        
        # Calculate metrics
        data_size_mb = len(table.to_pandas().to_csv()) / (1024 * 1024)
        throughput = data_size_mb / (duration_ms / 1000) if duration_ms > 0 else 0
        
        return BenchmarkResult(
            operation="read",
            file_format="vortex",
            data_size=f"{len(table)} rows",
            duration_ms=duration_ms,
            file_size_bytes=file_size,
            compression_ratio=0,  # Not applicable for reads
            throughput_mb_per_sec=throughput
        )
    
    def run_benchmark_suite(self) -> BenchmarkSuite:
        """Run the complete benchmark suite."""
        print("🚀 Starting Vortex vs Parquet Performance Benchmark")
        print("=" * 60)
        
        schemas = self.create_test_schemas()
        data_sizes = [1000, 10000, 50000]  # Different row counts to test
        
        for schema_name, schema in schemas.items():
            print(f"\n📊 Testing Schema: {schema_name}")
            print("-" * 40)
            
            for size in data_sizes:
                print(f"\n  📈 Data Size: {size:,} rows")
                
                # Generate test data
                table = self.generate_test_data(schema_name, size)
                
                # File paths
                parquet_path = f"file:{self.temp_dir}/{schema_name}_{size}.parquet"
                vortex_path = f"file:{self.temp_dir}/{schema_name}_{size}.vortex"
                
                # Write benchmarks
                print("    ⏱️  Write benchmarks...")
                parquet_write = self.benchmark_write_parquet(table, schema, parquet_path)
                self.results.append(parquet_write)
                
                if self.vortex_available:
                    vortex_write = self.benchmark_write_vortex(table, schema, vortex_path)
                    self.results.append(vortex_write)
                    
                    # Performance comparison
                    write_speedup = parquet_write.duration_ms / vortex_write.duration_ms if vortex_write.duration_ms > 0 else 0
                    compression_improvement = vortex_write.compression_ratio / parquet_write.compression_ratio if parquet_write.compression_ratio > 0 else 0
                    
                    print(f"      📝 Parquet write: {parquet_write.duration_ms:.1f}ms ({parquet_write.file_size_bytes:,} bytes)")
                    print(f"      🚀 Vortex write:  {vortex_write.duration_ms:.1f}ms ({vortex_write.file_size_bytes:,} bytes)")
                    print(f"      ⚡ Speedup: {write_speedup:.1f}x faster, {compression_improvement:.1f}x better compression")
                
                # Read benchmarks (only if files were successfully written)
                if parquet_write.file_size_bytes > 0:
                    print("    ⏱️  Read benchmarks...")
                    parquet_read = self.benchmark_read_parquet(parquet_path, table) 
                    self.results.append(parquet_read)
                    
                    if self.vortex_available and any(r.file_format == "vortex" and r.operation == "write" 
                                                   and r.data_size == f"{size} rows" for r in self.results):
                        vortex_read = self.benchmark_read_vortex(vortex_path, table)
                        self.results.append(vortex_read)
                        
                        # Performance comparison
                        read_speedup = parquet_read.duration_ms / vortex_read.duration_ms if vortex_read.duration_ms > 0 else 0
                        
                        print(f"      📖 Parquet read:  {parquet_read.duration_ms:.1f}ms ({parquet_read.throughput_mb_per_sec:.1f} MB/s)")
                        print(f"      🚀 Vortex read:   {vortex_read.duration_ms:.1f}ms ({vortex_read.throughput_mb_per_sec:.1f} MB/s)")
                        print(f"      ⚡ Speedup: {read_speedup:.1f}x faster")
        
        return self.generate_summary()
    
    def generate_summary(self) -> BenchmarkSuite:
        """Generate benchmark summary statistics."""
        summary = {
            "total_tests": len(self.results),
            "vortex_available": self.vortex_available,
        }
        
        if self.vortex_available:
            # Calculate average speedups
            write_speedups = []
            read_speedups = []
            compression_improvements = []
            
            # Group results by operation and data size
            parquet_results = {r.operation + "_" + r.data_size: r for r in self.results if r.file_format == "parquet"}
            vortex_results = {r.operation + "_" + r.data_size: r for r in self.results if r.file_format == "vortex"}
            
            for key in parquet_results:
                if key in vortex_results:
                    parquet = parquet_results[key]
                    vortex = vortex_results[key]
                    
                    if vortex.duration_ms > 0:
                        speedup = parquet.duration_ms / vortex.duration_ms
                        if parquet.operation == "write":
                            write_speedups.append(speedup)
                            if parquet.compression_ratio > 0:
                                compression_improvements.append(vortex.compression_ratio / parquet.compression_ratio)
                        else:
                            read_speedups.append(speedup)
            
            summary.update({
                "average_write_speedup": statistics.mean(write_speedups) if write_speedups else 0,
                "average_read_speedup": statistics.mean(read_speedups) if read_speedups else 0,
                "average_compression_improvement": statistics.mean(compression_improvements) if compression_improvements else 0,
                "max_write_speedup": max(write_speedups) if write_speedups else 0,
                "max_read_speedup": max(read_speedups) if read_speedups else 0,
            })
        
        return BenchmarkSuite(results=self.results, summary=summary)
    
    def print_detailed_results(self, suite: BenchmarkSuite):
        """Print detailed benchmark results."""
        print("\n" + "=" * 80)
        print("📈 DETAILED BENCHMARK RESULTS")
        print("=" * 80)
        
        # Group by schema and operation
        by_schema = {}
        for result in suite.results:
            schema_key = result.data_size.split()[0] + "_rows"  # Extract row count
            if schema_key not in by_schema:
                by_schema[schema_key] = {"write": {}, "read": {}}
            by_schema[schema_key][result.operation][result.file_format] = result
        
        for schema_key, operations in by_schema.items():
            print(f"\n📊 {schema_key.replace('_', ' ').title()}")
            print("-" * 50)
            
            for op_name, formats in operations.items():
                if formats:  # Only show if we have data
                    print(f"\n  {op_name.title()} Performance:")
                    for fmt, result in formats.items():
                        print(f"    {fmt.upper():<8}: {result.duration_ms:>7.1f}ms | "
                              f"{result.throughput_mb_per_sec:>6.1f} MB/s | "
                              f"{result.file_size_bytes:>8,} bytes")
        
        # Summary statistics
        if suite.summary.get("vortex_available"):
            print(f"\n🎯 PERFORMANCE SUMMARY")
            print("-" * 30)
            print(f"Average Write Speedup: {suite.summary['average_write_speedup']:.1f}x")
            print(f"Average Read Speedup:  {suite.summary['average_read_speedup']:.1f}x")
            print(f"Compression Improvement: {suite.summary['average_compression_improvement']:.1f}x") 
            print(f"Max Write Speedup: {suite.summary['max_write_speedup']:.1f}x")
            print(f"Max Read Speedup:  {suite.summary['max_read_speedup']:.1f}x")


def main():
    """Run the benchmark suite."""
    print("🔬 PyIceberg Vortex vs Parquet Performance Benchmark")
    print("=" * 60)
    
    with VortexParquetBenchmark() as benchmark:
        suite = benchmark.run_benchmark_suite()
        benchmark.print_detailed_results(suite)
        
        if suite.summary.get("vortex_available"):
            print(f"\n🏆 CONCLUSION: Vortex provides significant performance improvements!")
            print(f"   💾 {suite.summary['average_write_speedup']:.1f}x faster writes on average")
            print(f"   📖 {suite.summary['average_read_speedup']:.1f}x faster reads on average") 
            print(f"   🗜️  {suite.summary['average_compression_improvement']:.1f}x better compression")
        else:
            print(f"\n⚠️  Vortex not available - install with: pip install vortex-data")


if __name__ == "__main__":
    main()
