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
Quick Vortex vs Parquet Performance Test
========================================

A smaller-scale benchmark to validate the implementation works before
running the full 2GB test.

Usage:
    python tests/benchmark/quick_benchmark.py

This is a fast benchmark suitable for development and CI testing.
"""

import time
import tempfile
import shutil
from pathlib import Path

import pandas as pd
import pyarrow as pa
import numpy as np
from pyiceberg.catalog.memory import InMemoryCatalog
from pyiceberg.schema import Schema
from pyiceberg.types import (
    NestedField, IntegerType, LongType, StringType, 
    DoubleType, BooleanType, TimestampType, DateType
)
from pyiceberg.expressions import GreaterThan, EqualTo

print("🧪 Quick Vortex vs Parquet Performance Test")
print("=" * 50)

def create_test_data(num_rows: int = 100_000):
    """Create a smaller test dataset."""
    print(f"📊 Creating {num_rows:,} rows of test data...")
    
    # Generate data
    data = {
        "id": np.arange(1, num_rows + 1, dtype=np.int64),
        "user_id": np.random.randint(1, 1000, num_rows, dtype=np.int32),
        "product_name": [f"Product_{i % 100:03d}" for i in range(num_rows)],
        "category": np.random.choice(["Electronics", "Books", "Clothing"], num_rows),
        "price": np.round(np.random.uniform(10.0, 500.0, num_rows), 2),
        "quantity": np.random.randint(1, 5, num_rows, dtype=np.int32),
        "is_premium": np.random.choice([True, False], num_rows, p=[0.3, 0.7]),
        "created_date": np.random.choice(
            pd.date_range('2023-01-01', '2024-12-31', freq='D').values[:365],
            num_rows
        ),
        "updated_timestamp": np.random.choice(
            pd.date_range('2024-01-01', '2024-12-31', freq='h').values[:8760],
            num_rows
        ),
        "description": np.random.choice([
            "High quality product", "Best seller", "Limited edition",
            "Premium quality", None, "Customer favorite"
        ], num_rows),
        "rating": np.where(
            np.random.random(num_rows) > 0.2,
            np.round(np.random.uniform(1.0, 5.0, num_rows), 1),
            None
        )
    }
    
    # Calculate total amount
    data["total_amount"] = np.round(data["price"] * data["quantity"], 2)
    
    # Create Arrow table with proper schema
    arrow_schema = pa.schema([
        ("id", pa.int64(), False),
        ("user_id", pa.int32(), False), 
        ("product_name", pa.string(), False),
        ("category", pa.string(), False),
        ("price", pa.float64(), False),
        ("quantity", pa.int32(), False),
        ("total_amount", pa.float64(), False),
        ("is_premium", pa.bool_(), False),
        ("created_date", pa.date32(), False),
        ("updated_timestamp", pa.timestamp('us'), False),
        ("description", pa.string(), True),
        ("rating", pa.float64(), True)
    ])
    
    return pa.table(data, schema=arrow_schema)

def create_iceberg_schema():
    """Create the Iceberg schema."""
    return Schema(
        NestedField(1, "id", LongType(), required=True),
        NestedField(2, "user_id", IntegerType(), required=True),
        NestedField(3, "product_name", StringType(), required=True),
        NestedField(4, "category", StringType(), required=True), 
        NestedField(5, "price", DoubleType(), required=True),
        NestedField(6, "quantity", IntegerType(), required=True),
        NestedField(7, "total_amount", DoubleType(), required=True),
        NestedField(8, "is_premium", BooleanType(), required=True),
        NestedField(9, "created_date", DateType(), required=True),
        NestedField(10, "updated_timestamp", TimestampType(), required=True),
        NestedField(11, "description", StringType(), required=False),
        NestedField(12, "rating", DoubleType(), required=False),
    )

def benchmark_format(format_name: str, catalog, table_name: str, test_data: pa.Table, schema: Schema):
    """Benchmark a specific format."""
    print(f"\n📝 Testing {format_name}...")
    
    # Create table with format-specific properties
    properties = {}
    if format_name == "Vortex":
        properties["write.format.default"] = "vortex"
    
    table = catalog.create_table(table_name, schema=schema, properties=properties)
    
    # Test write performance
    start_time = time.time()
    table.append(test_data)
    write_time = time.time() - start_time
    
    write_rate = len(test_data) / write_time if write_time > 0 else 0
    print(f"   Write: {write_time:.2f}s ({write_rate:,.0f} rows/sec)")
    
    # Test full scan performance  
    start_time = time.time()
    result = table.scan().to_arrow()
    read_time = time.time() - start_time
    
    read_rate = len(result) / read_time if read_time > 0 else 0
    print(f"   Read:  {read_time:.2f}s ({read_rate:,.0f} rows/sec)")
    
    # Test filtered query
    start_time = time.time()
    filtered = table.scan(row_filter=GreaterThan("price", 100.0)).to_arrow()
    filter_time = time.time() - start_time
    
    filter_rate = len(filtered) / filter_time if filter_time > 0 else 0
    print(f"   Filter: {filter_time:.2f}s ({filter_rate:,.0f} rows/sec, {len(filtered):,} results)")
    
    # Get file size
    try:
        # This is a rough estimate - would need proper file path access for exact size
        size_mb = len(test_data) * 50 / (1024 * 1024)  # Rough estimate
        print(f"   Est. size: ~{size_mb:.1f} MB")
    except:
        print(f"   Size: Unknown")
    
    return {
        "write_time": write_time,
        "read_time": read_time, 
        "filter_time": filter_time,
        "write_rate": write_rate,
        "read_rate": read_rate,
        "filter_rate": filter_rate,
        "rows": len(test_data),
        "filtered_rows": len(filtered)
    }

def main():
    # Create test data
    test_data = create_test_data(1_000_000)  # 1M rows for better comparison
    schema = create_iceberg_schema()
    
    # Setup catalogs
    vortex_catalog = InMemoryCatalog(name="vortex_test")
    vortex_catalog.create_namespace("test")
    
    parquet_catalog = InMemoryCatalog(name="parquet_test")  
    parquet_catalog.create_namespace("test")
    
    try:
        # Test Vortex
        vortex_results = benchmark_format("Vortex", vortex_catalog, "test.vortex_table", test_data, schema)
        
        # Test Parquet
        parquet_results = benchmark_format("Parquet", parquet_catalog, "test.parquet_table", test_data, schema)
        
        # Compare results
        print(f"\n🏆 PERFORMANCE COMPARISON:")
        print(f"   Write speedup:  {parquet_results['write_time'] / vortex_results['write_time']:.1f}x")
        print(f"   Read speedup:   {parquet_results['read_time'] / vortex_results['read_time']:.1f}x") 
        print(f"   Filter speedup: {parquet_results['filter_time'] / vortex_results['filter_time']:.1f}x")
        
        if (vortex_results['write_time'] < parquet_results['write_time'] and
            vortex_results['read_time'] < parquet_results['read_time']):
            print(f"✅ Vortex outperforms Parquet in both read and write!")
        else:
            print(f"⚠️  Mixed results - need to investigate further")
            
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
