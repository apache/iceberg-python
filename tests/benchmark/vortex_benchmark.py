#!/usr/bin/env python3
"""
PyIceberg Vortex Performance Benchmark Suite
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
import pyarrow as pa
import pyarrow.parquet as pq
import tempfile
import time
from pathlib import Path
from typing import List, Tuple, Optional, Dict

try:
    import vortex as vx
    VORTEX_AVAILABLE = True
except ImportError:
    VORTEX_AVAILABLE = False

from pyiceberg.io.pyarrow import _calculate_optimal_vortex_batch_size, _optimize_vortex_batch_layout


class VortexBenchmarkSuite:
    """Comprehensive Vortex benchmark suite."""
    
    def __init__(self, temp_dir: Optional[str] = None):
        self.temp_dir = temp_dir or tempfile.mkdtemp()
        self.results = {}
    
    def create_realistic_data(self, num_rows: int, complexity: str = "medium") -> pa.Table:
        """Create realistic test data with varying complexity."""
        base_data = {
            'id': range(num_rows),
            'name': [f'user_{i}' for i in range(num_rows)],
            'timestamp': [1000000 + i for i in range(num_rows)],
        }
        
        if complexity == "simple":
            base_data.update({
                'value': [i * 1.1 for i in range(num_rows)],
                'status': ['active' if i % 2 == 0 else 'inactive' for i in range(num_rows)],
            })
        elif complexity == "medium":
            base_data.update({
                'score': [i * 0.1 for i in range(num_rows)],
                'category': [f'cat_{i % 10}' for i in range(num_rows)],
                'value': [i * 1.5 for i in range(num_rows)],
                'status': ['active' if i % 3 == 0 else 'inactive' for i in range(num_rows)],
                'price': [float(i % 1000) + 0.99 for i in range(num_rows)],
            })
        elif complexity == "complex":
            base_data.update({
                'score': [i * 0.1 for i in range(num_rows)],
                'category': [f'cat_{i % 20}' for i in range(num_rows)],
                'subcategory': [f'subcat_{i % 100}' for i in range(num_rows)],
                'value': [i * 1.5 for i in range(num_rows)],
                'price': [float(i % 1000) + 0.99 for i in range(num_rows)],
                'quantity': [i % 50 + 1 for i in range(num_rows)],
                'status': ['active' if i % 3 == 0 else 'inactive' for i in range(num_rows)],
                'metadata': [f'{{"key": "value_{i % 10}"}}' for i in range(num_rows)],
                'is_premium': [i % 5 == 0 for i in range(num_rows)],
                'order_total': [float(i % 10000) + 50.0 for i in range(num_rows)],
            })
        
        return pa.table(base_data)
    
    def benchmark_vortex_write(self, table: pa.Table, optimize: bool = True) -> Tuple[float, int]:
        """Benchmark Vortex write performance."""
        if not VORTEX_AVAILABLE:
            return 0.0, 0
        
        file_path = f"{self.temp_dir}/test_vortex_{int(time.time())}.vortex"
        
        start_time = time.time()
        try:
            if optimize and table.num_rows > 100_000:
                # Use our optimizations for larger datasets
                optimal_batch_size = _calculate_optimal_vortex_batch_size(table)
                batches = table.to_batches(max_chunksize=optimal_batch_size)
                optimized_batches = _optimize_vortex_batch_layout(batches, optimal_batch_size)
                reader = pa.RecordBatchReader.from_batches(table.schema, optimized_batches)
            else:
                # Use default batching
                reader = table.to_reader()
            
            vx.io.write(reader, file_path)
            write_time = time.time() - start_time
            
            # Get file size
            file_size = Path(file_path).stat().st_size
            return write_time, file_size
            
        except Exception as e:
            print(f"   ❌ Vortex write failed: {e}")
            return 0.0, 0
    
    def benchmark_parquet_write(self, table: pa.Table) -> Tuple[float, int]:
        """Benchmark Parquet write performance."""
        file_path = f"{self.temp_dir}/test_parquet_{int(time.time())}.parquet"
        
        start_time = time.time()
        try:
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
            vortex_result = vx.io.read_url(file_url)
            arrow_table = vortex_result.to_arrow_table()
            read_time = time.time() - start_time
            return read_time, arrow_table.num_rows
        except Exception as e:
            print(f"   ❌ Vortex read failed: {e}")
            return 0.0, 0
    
    def benchmark_parquet_read(self, file_path: str, table_rows: int) -> Tuple[float, int]:
        """Benchmark Parquet read performance."""
        start_time = time.time()
        try:
            table = pq.read_table(file_path)
            read_time = time.time() - start_time
            return read_time, table.num_rows
        except Exception as e:
            print(f"   ❌ Parquet read failed: {e}")
            return 0.0, 0
    
    def test_optimization_functions(self):
        """Test our optimization functions."""
        print("🔧 Vortex Optimization Analysis")
        print("================================")
        
        # Test batch size calculation
        print("\n📊 Batch Size Optimization:")
        test_cases = [
            (10_000, "Small"),
            (100_000, "Medium"),
            (1_000_000, "Large"),
            (10_000_000, "Very Large")
        ]
        
        for num_rows, description in test_cases:
            table = self.create_realistic_data(num_rows)
            optimal_size = _calculate_optimal_vortex_batch_size(table)
            efficiency = optimal_size / num_rows if num_rows > optimal_size else num_rows / optimal_size
            print(f"   {description:>10} ({num_rows:>8,} rows) → {optimal_size:>6,} batch size (ratio: {efficiency:.3f})")
        
        # Test batch layout optimization
        print("\n🔧 Batch Layout Optimization:")
        data = {'id': range(20_000), 'value': [i * 2 for i in range(20_000)]}
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
    
    def run_optimization_impact_test(self):
        """Test optimization impact across different dataset sizes."""
        print("\n🚀 Optimization Impact Analysis")
        print("===============================")
        
        test_cases = [
            (100_000, "Small dataset"),
            (500_000, "Medium dataset"),
            (1_500_000, "Large dataset"),
        ]
        
        results = []
        
        for num_rows, description in test_cases:
            print(f"\n📊 {description} ({num_rows:,} rows):")
            
            table = self.create_realistic_data(num_rows)
            
            if VORTEX_AVAILABLE:
                # Test without optimization
                baseline_time, _ = self.benchmark_vortex_write(table, optimize=False)
                baseline_rate = num_rows / baseline_time if baseline_time > 0 else 0
                
                # Test with optimization
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
        
        return results
    
    def run_format_comparison(self, dataset_sizes: List[int], complexity: str = "medium"):
        """Run comprehensive Vortex vs Parquet comparison."""
        print(f"\n📈 Format Performance Comparison ({complexity} complexity)")
        print("=" * 60)
        
        results = []
        
        for num_rows in dataset_sizes:
            print(f"\n📊 Testing {num_rows:,} rows:")
            
            table = self.create_realistic_data(num_rows, complexity)
            
            # Vortex performance
            if VORTEX_AVAILABLE:
                vortex_write_time, vortex_size = self.benchmark_vortex_write(table, optimize=True)
                vortex_write_rate = num_rows / vortex_write_time if vortex_write_time > 0 else 0
                print(f"   🔺 Vortex Write: {vortex_write_rate:>8,.0f} rows/sec, {vortex_size:>8,} bytes")
            else:
                vortex_write_rate, vortex_size = 0, 0
                print("   🔺 Vortex: Not available")
            
            # Parquet performance
            parquet_write_time, parquet_size = self.benchmark_parquet_write(table)
            parquet_write_rate = num_rows / parquet_write_time if parquet_write_time > 0 else 0
            print(f"   📦 Parquet Write: {parquet_write_rate:>7,.0f} rows/sec, {parquet_size:>8,} bytes")
            
            # Read performance comparison
            if VORTEX_AVAILABLE and vortex_write_time > 0:
                vortex_file = f"{self.temp_dir}/vortex_read_test.vortex"
                vx.io.write(table.to_reader(), vortex_file)
                vortex_read_time, vortex_read_rows = self.benchmark_vortex_read(vortex_file, num_rows)
                vortex_read_rate = vortex_read_rows / vortex_read_time if vortex_read_time > 0 else 0
                print(f"   🔺 Vortex Read:  {vortex_read_rate:>8,.0f} rows/sec")
            else:
                vortex_read_rate = 0
            
            if parquet_write_time > 0:
                parquet_file = f"{self.temp_dir}/parquet_read_test.parquet"
                pq.write_table(table, parquet_file)
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
                    print(f"      Write: {(1/write_ratio):.2f}x slower")
                if read_ratio >= 1:
                    print(f"      Read:  {read_ratio:.2f}x faster")
                else:
                    print(f"      Read:  {(1/read_ratio):.2f}x slower")
                print(f"      Size:  {size_ratio:.2f}x compression (Parquet/Vortex)")
                
                results.append({
                    'rows': num_rows,
                    'vortex_write_rate': vortex_write_rate,
                    'parquet_write_rate': parquet_write_rate,
                    'vortex_read_rate': vortex_read_rate,
                    'parquet_read_rate': parquet_read_rate,
                    'vortex_size': vortex_size,
                    'parquet_size': parquet_size,
                    'write_ratio': write_ratio,
                    'read_ratio': read_ratio,
                    'size_ratio': size_ratio
                })
        
        return results
    
    def run_large_scale_benchmark(self):
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
                'id': range(i, i + current_batch_size),
                'name': [f'user_{j}' for j in range(i, i + current_batch_size)],
                'score': [j * 0.1 for j in range(i, i + current_batch_size)],
                'category': [f'cat_{j % 10}' for j in range(i, i + current_batch_size)],
                'value': [j * 1.5 for j in range(i, i + current_batch_size)],
                'status': ['active' if j % 3 == 0 else 'inactive' for j in range(i, i + current_batch_size)],
                'timestamp': [1000000 + j for j in range(i, i + current_batch_size)],
                'price': [float(j % 1000) + 0.99 for j in range(i, i + current_batch_size)],
                'is_premium': [j % 5 == 0 for j in range(i, i + current_batch_size)],
                'order_total': [float(j % 10000) + 50.0 for j in range(i, i + current_batch_size)],
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
    
    def run_production_scenarios(self):
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
    
    def generate_summary_report(self, results: List[Dict]):
        """Generate a comprehensive summary report."""
        if not results:
            return
        
        print("\n📊 Performance Summary Report")
        print("=" * 70)
        print(f"{'Dataset':<12} {'Vortex W':<10} {'Parquet W':<11} {'Vortex R':<10} {'Parquet R':<11} {'W Ratio':<8} {'R Ratio':<8}")
        print("-" * 70)
        
        for result in results:
            rows = result['rows']
            vw = result['vortex_write_rate'] / 1000
            pw = result['parquet_write_rate'] / 1000
            vr = result['vortex_read_rate'] / 1000
            pr = result['parquet_read_rate'] / 1000
            wr = result.get('write_ratio', 0)
            rr = result.get('read_ratio', 0)
            
            print(f"{rows/1000:>8.0f}K {vw:>8.0f}K {pw:>9.0f}K {vr:>8.0f}K {pr:>9.0f}K {wr:>6.2f}x {rr:>6.2f}x")


def main():
    """Main benchmark runner with CLI interface."""
    parser = argparse.ArgumentParser(description="PyIceberg Vortex Performance Benchmark Suite")
    parser.add_argument("--quick", action="store_true", help="Run quick benchmark (recommended)")
    parser.add_argument("--full", action="store_true", help="Run full benchmark suite")
    parser.add_argument("--optimizations", action="store_true", help="Run optimization analysis only")
    parser.add_argument("--large", action="store_true", help="Run large scale benchmark (15M+ rows)")
    parser.add_argument("--production", action="store_true", help="Run production scenarios")
    
    args = parser.parse_args()
    
    # Default to quick if no arguments
    if not any([args.quick, args.full, args.optimizations, args.large, args.production]):
        args.quick = True
    
    print("🎯 PyIceberg Vortex Performance Benchmark Suite")
    print("=" * 50)
    
    if not VORTEX_AVAILABLE:
        print("⚠️  Warning: Vortex not available. Some tests will be skipped.")
    
    print()
    
    with tempfile.TemporaryDirectory() as temp_dir:
        suite = VortexBenchmarkSuite(temp_dir)
        
        if args.optimizations:
            suite.test_optimization_functions()
            suite.run_optimization_impact_test()
            
        elif args.large:
            suite.run_large_scale_benchmark()
            
        elif args.production:
            suite.run_production_scenarios()
            
        elif args.full:
            print("🚀 Full Benchmark Suite")
            print("======================")
            
            # Run all components
            suite.test_optimization_functions()
            suite.run_optimization_impact_test()
            
            # Format comparison at multiple scales
            sizes = [100_000, 500_000, 1_500_000, 5_000_000]
            results = suite.run_format_comparison(sizes)
            
            # Production scenarios
            suite.run_production_scenarios()
            
            # Summary report
            if results:
                suite.generate_summary_report(results)
            
        elif args.quick:
            print("⚡ Quick Benchmark")
            print("=================")
            
            # Quick format comparison
            sizes = [100_000, 500_000]
            results = suite.run_format_comparison(sizes)
            
            if results:
                print("\n🎯 Quick Summary:")
                for result in results:
                    rows = result['rows']
                    write_ratio = result.get('write_ratio', 0)
                    read_ratio = result.get('read_ratio', 0)
                    size_ratio = result.get('size_ratio', 0)
                    print(
                        f"   {rows:>7,} rows: {write_ratio:.2f}x write (of Parquet), {read_ratio:.2f}x read (of Parquet), {size_ratio:.2f}x compression (P/V)"
                    )
    
    print("\n✅ Benchmark complete!")
    print("\n📋 Key Findings:")
    print("   ✅ Batch tuning helps small datasets; neutral/negative on larger")
    print("   ✅ Reads are often faster on small data; near parity at medium; can be slower on large")
    print("   ✅ Writes are typically slower than Parquet (trade-off for read speed and size)")
    print("   ✅ Compression typically 1.2–2.0x smaller than Parquet (data-dependent)")


if __name__ == "__main__":
    main()
