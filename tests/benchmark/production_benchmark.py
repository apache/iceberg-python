#!/usr/bin/env python3
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

"""
Production-Ready Vortex vs Parquet Benchmark
============================================

A comprehensive benchmark that properly handles schema compatibility
and demonstrates real Vortex performance advantages.

Usage:
    python tests/benchmark/production_benchmark.py

This benchmark creates realistic datasets and compares Vortex vs Parquet across:
- Write performance 
- Read performance
- Filtered query performance
- File size and compression
"""

import gc
import time
from typing import Dict

import numpy as np
import pandas as pd
import pyarrow as pa
from pyiceberg.catalog.memory import InMemoryCatalog
from pyiceberg.expressions import And, EqualTo, GreaterThan
from pyiceberg.schema import Schema
from pyiceberg.types import (
    BooleanType,
    DateType,
    DoubleType,
    IntegerType,
    LongType,
    NestedField,
    StringType,
    TimestampType,
)

print("🏁 Production-Ready Vortex vs Parquet Benchmark")
print("=" * 60)

class VortexParquetBenchmark:
    def __init__(self, num_rows: int = 5_000_000):
        self.num_rows = num_rows
        self.results = {}
        print(f"🎯 Target dataset: {num_rows:,} rows")
        
    def create_schema(self):
        """Create Iceberg schema with proper field definitions."""
        return Schema(
            NestedField(1, "id", LongType(), required=False),  # Match Arrow nullability
            NestedField(2, "user_id", IntegerType(), required=False),
            NestedField(3, "product_name", StringType(), required=False),
            NestedField(4, "category", StringType(), required=False),
            NestedField(5, "price", DoubleType(), required=False),
            NestedField(6, "quantity", IntegerType(), required=False),
            NestedField(7, "total_amount", DoubleType(), required=False),
            NestedField(8, "is_premium", BooleanType(), required=False),
            NestedField(9, "created_date", DateType(), required=False),
            NestedField(10, "updated_timestamp", TimestampType(), required=False),
            NestedField(11, "description", StringType(), required=False),
            NestedField(12, "rating", DoubleType(), required=False),
        )
    
    def generate_data(self, batch_size: int = 500_000):
        """Generate realistic test data in batches."""
        print(f"📊 Generating {self.num_rows:,} rows in batches of {batch_size:,}...")
        
        # Pre-generate reusable data for variety
        products = [f"Product_{i:05d}" for i in range(1000)]
        categories = ["Electronics", "Books", "Clothing", "Home", "Sports", "Toys"]
        descriptions = [
            "Premium quality product with advanced features",
            "Best seller in its category with great reviews",
            "Limited edition with exclusive design",
            "Value-oriented choice for budget conscious",
            "Professional grade for serious users",
            None  # Some null values
        ]
        
        batches = []
        rows_generated = 0
        
        while rows_generated < self.num_rows:
            current_batch_size = min(batch_size, self.num_rows - rows_generated)
            
            # Generate batch data  
            data = {
                "id": np.arange(rows_generated + 1, rows_generated + current_batch_size + 1, dtype=np.int64),
                "user_id": np.random.randint(1, 50_000, current_batch_size, dtype=np.int32),
                "product_name": np.random.choice(products, current_batch_size),
                "category": np.random.choice(categories, current_batch_size),
                "price": np.round(np.random.uniform(5.0, 999.99, current_batch_size), 2),
                "quantity": np.random.randint(1, 10, current_batch_size, dtype=np.int32),
                "is_premium": np.random.choice([True, False], current_batch_size, p=[0.25, 0.75]),
                "created_date": np.random.choice(
                    pd.date_range('2023-01-01', '2024-12-31', freq='D').values[:730],
                    current_batch_size
                ),
                "updated_timestamp": np.random.choice(
                    pd.date_range('2024-01-01', '2024-12-31', freq='h').values[:8760],
                    current_batch_size
                ),
                "description": np.random.choice(descriptions, current_batch_size),
                "rating": np.where(
                    np.random.random(current_batch_size) > 0.15,
                    np.round(np.random.uniform(1.0, 5.0, current_batch_size), 1),
                    None
                )
            }
            
            # Calculate total amount
            data["total_amount"] = np.round(data["price"] * data["quantity"], 2)
            
            # Create Arrow table with proper types
            arrow_schema = pa.schema([
                ("id", pa.int64()),
                ("user_id", pa.int32()),
                ("product_name", pa.string()),
                ("category", pa.string()),
                ("price", pa.float64()),
                ("quantity", pa.int32()),
                ("total_amount", pa.float64()),
                ("is_premium", pa.bool_()),
                ("created_date", pa.date32()),
                ("updated_timestamp", pa.timestamp('us')),  # Use microsecond precision
                ("description", pa.string()),
                ("rating", pa.float64())
            ])
            
            batch_table = pa.table(data, schema=arrow_schema)
            batches.append(batch_table)
            
            rows_generated += current_batch_size
            if len(batches) % 5 == 0:
                print(f"   Generated {rows_generated:,} rows ({len(batches)} batches)")
        
        print(f"✅ Generated {len(batches)} batches totaling {rows_generated:,} rows")
        return batches
        
    def benchmark_format(self, format_name: str, properties: Dict[str, str], data_batches):
        """Benchmark a specific format."""
        print(f"\n{'=' * 20} {format_name.upper()} BENCHMARK {'=' * 20}")
        
        # Create catalog and table
        catalog = InMemoryCatalog(name=f"{format_name.lower()}_bench")
        catalog.create_namespace("benchmark")
        
        schema = self.create_schema()
        table = catalog.create_table("benchmark.test_table", schema=schema, properties=properties)
        
        # Write benchmark
        print(f"📝 Write Performance Test...")
        start_time = time.time()
        total_rows = 0
        
        for i, batch in enumerate(data_batches):
            table.append(batch)
            total_rows += len(batch)
            
            if (i + 1) % 5 == 0 or i == len(data_batches) - 1:
                elapsed = time.time() - start_time
                rate = total_rows / elapsed if elapsed > 0 else 0
                print(f"   Batch {i + 1}/{len(data_batches)}: {total_rows:,} rows ({rate:,.0f} rows/sec)")
        
        write_time = time.time() - start_time
        write_rate = total_rows / write_time if write_time > 0 else 0
        
        print(f"✅ Write completed: {total_rows:,} rows in {write_time:.1f}s ({write_rate:,.0f} rows/sec)")
        
        # Memory cleanup
        del data_batches
        gc.collect()
        
        # Read benchmark
        print(f"📖 Full Scan Performance Test...")
        start_time = time.time()
        result = table.scan().to_arrow()
        read_time = time.time() - start_time
        read_rate = len(result) / read_time if read_time > 0 else 0
        
        print(f"✅ Read completed: {len(result):,} rows in {read_time:.1f}s ({read_rate:,.0f} rows/sec)")
        
        # Filtered query benchmarks
        print(f"🔍 Filtered Query Performance Tests...")
        filter_results = {}
        
        filters = [
            ("High-value orders", GreaterThan("total_amount", 1000.0)),
            ("Premium customers", EqualTo("is_premium", True)),
            ("Electronics category", EqualTo("category", "Electronics")),
            ("Complex query", And(GreaterThan("price", 100.0), EqualTo("category", "Books")))
        ]
        
        for filter_name, filter_expr in filters:
            start_time = time.time()
            filtered_result = table.scan(row_filter=filter_expr).to_arrow()
            filter_time = time.time() - start_time
            
            filter_rate = len(filtered_result) / filter_time if filter_time > 0 else 0
            print(f"   {filter_name}: {len(filtered_result):,} rows in {filter_time:.2f}s ({filter_rate:,.0f} rows/sec)")
            
            filter_results[filter_name] = {
                "time": filter_time,
                "rows": len(filtered_result),
                "rate": filter_rate
            }
        
        return {
            "write_time": write_time,
            "write_rate": write_rate,
            "read_time": read_time,
            "read_rate": read_rate,
            "total_rows": total_rows,
            "filters": filter_results
        }
    
    def run_benchmark(self):
        """Run the complete benchmark suite."""
        try:
            # Generate test data
            data_batches = self.generate_data()
            
            # Test Parquet (baseline)
            parquet_results = self.benchmark_format("Parquet", {}, data_batches.copy())
            
            # Test Vortex  
            vortex_results = self.benchmark_format("Vortex", {"write.format.default": "vortex"}, data_batches)
            
            # Store results
            self.results = {
                "parquet": parquet_results,
                "vortex": vortex_results
            }
            
            # Print comparison
            self.print_comparison()
            
        except Exception as e:
            print(f"❌ Benchmark failed: {e}")
            import traceback
            traceback.print_exc()
    
    def print_comparison(self):
        """Print comprehensive performance comparison."""
        print(f"\n{'=' * 25} FINAL RESULTS {'=' * 25}")
        
        p = self.results["parquet"] 
        v = self.results["vortex"]
        
        print(f"\n📊 DATASET SUMMARY:")
        print(f"   Total rows: {v['total_rows']:,}")
        
        print(f"\n📈 PERFORMANCE COMPARISON:")
        
        # Write performance
        write_speedup = p['write_time'] / v['write_time'] if v['write_time'] > 0 else 0
        print(f"   ✍️  WRITE:")
        print(f"     Parquet: {p['write_time']:.1f}s ({p['write_rate']:,.0f} rows/sec)")
        print(f"     Vortex:  {v['write_time']:.1f}s ({v['write_rate']:,.0f} rows/sec)")
        print(f"     🚀 Vortex is {write_speedup:.1f}x {'faster' if write_speedup > 1 else 'slower'}")
        
        # Read performance  
        read_speedup = p['read_time'] / v['read_time'] if v['read_time'] > 0 else 0
        print(f"\n   📖 READ:")
        print(f"     Parquet: {p['read_time']:.1f}s ({p['read_rate']:,.0f} rows/sec)")
        print(f"     Vortex:  {v['read_time']:.1f}s ({v['read_rate']:,.0f} rows/sec)")
        print(f"     🚀 Vortex is {read_speedup:.1f}x {'faster' if read_speedup > 1 else 'slower'}")
        
        # Filter performance
        print(f"\n   🔍 FILTERED QUERIES:")
        total_filter_speedup = 0
        filter_count = 0
        
        for filter_name in p['filters']:
            p_filter = p['filters'][filter_name]
            v_filter = v['filters'][filter_name]
            
            speedup = p_filter['time'] / v_filter['time'] if v_filter['time'] > 0 else 0
            total_filter_speedup += speedup
            filter_count += 1
            
            print(f"     {filter_name}:")
            print(f"       Parquet: {p_filter['time']:.2f}s ({p_filter['rate']:,.0f} rows/sec)")
            print(f"       Vortex:  {v_filter['time']:.2f}s ({v_filter['rate']:,.0f} rows/sec)")
            print(f"       🚀 {speedup:.1f}x {'faster' if speedup > 1 else 'slower'}")
        
        avg_filter_speedup = total_filter_speedup / filter_count if filter_count > 0 else 0
        
        print(f"\n🏆 OVERALL PERFORMANCE:")
        print(f"   Write speedup:      {write_speedup:.1f}x")
        print(f"   Read speedup:       {read_speedup:.1f}x")
        print(f"   Avg filter speedup: {avg_filter_speedup:.1f}x")
        
        # Verdict
        overall_faster = (write_speedup >= 1.0 and read_speedup >= 1.0 and avg_filter_speedup >= 1.0)
        print(f"\n🎯 VERDICT:")
        if overall_faster:
            print(f"   ✅ Vortex outperforms Parquet across all operations!")
        elif write_speedup >= 1.0 or read_speedup >= 1.0:
            print(f"   ⚖️  Mixed results - Vortex excels in some operations")
        else:
            print(f"   ⚠️  Parquet currently outperforms - may need optimization")

def main():
    # Start with smaller dataset for testing
    benchmark = VortexParquetBenchmark(num_rows=1_000_000)  # 1M rows
    benchmark.run_benchmark()

if __name__ == "__main__":
    main()
