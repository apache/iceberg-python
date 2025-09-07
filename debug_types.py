#!/usr/bin/env python3
"""Debug script to check PyArrow types from different formats."""

import os
import tempfile

import pyarrow as pa
import pyarrow.parquet as pq
import vortex as vx

# Create test data
test_data = pa.table({
    'id': [1, 2, 3],
    'name': ['Alice', 'Bob', 'Charlie'],
    'age': [25, 30, 35]
})

print("Original test data schema:")
print(test_data.schema)

# Write to Parquet and read back
with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as f:
    parquet_path = f.name

try:
    import pyarrow.parquet
    pyarrow.parquet.write_table(test_data, parquet_path)
    parquet_table = pyarrow.parquet.read_table(parquet_path)
    print("\nParquet schema:")
    print(parquet_table.schema)
finally:
    if os.path.exists(parquet_path):
        os.unlink(parquet_path)

# Write to Vortex and read back
with tempfile.NamedTemporaryFile(suffix='.vortex', delete=False) as f:
    vortex_path = f.name

try:
    vx.io.write(test_data, vortex_path)
    vortex_file = vx.open(vortex_path)
    vortex_reader = vortex_file.to_arrow()
    
    # Get the first batch to inspect schema
    vortex_batch = next(iter(vortex_reader))
    print("\nVortex schema:")
    print(vortex_batch.schema)
    
finally:
    if os.path.exists(vortex_path):
        os.unlink(vortex_path)
