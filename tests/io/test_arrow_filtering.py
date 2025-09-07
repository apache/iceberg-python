#!/usr/bin/env python3

"""
Quick test to verify Arrow-based filtering works correctly
"""

import tempfile
from pyiceberg.catalog.memory import InMemoryCatalog
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, IntegerType, StringType
from pyiceberg.expressions import GreaterThan
import pyarrow as pa

# Set up test
catalog = InMemoryCatalog(name="test_catalog")
ns = catalog.create_namespace("test")

# Create schema
schema = Schema(
    NestedField(1, "id", IntegerType(), required=True),
    NestedField(2, "name", StringType(), required=True), 
    NestedField(3, "value", IntegerType(), required=True),
)

# Create table with Vortex format
table = catalog.create_table(
    "test.filtering_test",
    schema=schema,
    properties={"write.format.default": "vortex"}
)

# Add test data with correct types
data = pa.Table.from_pylist([
    {"id": 1, "name": "Alice", "value": 30},
    {"id": 2, "name": "Bob", "value": 60},  
    {"id": 3, "name": "Charlie", "value": 90},
], schema=pa.schema([
    pa.field("id", pa.int32(), nullable=False),
    pa.field("name", pa.string(), nullable=False),
    pa.field("value", pa.int32(), nullable=False),
]))

table.append(data)

print("✅ Data added successfully")

# Test filtering
try:
    # This should use Arrow-based filtering now
    filtered_results = table.scan(row_filter=GreaterThan("value", 50)).to_arrow()
    print(f"✅ Filtering works: Found {len(filtered_results)} rows")
    print(f"   Filtered data: {filtered_results.to_pylist()}")
    
    expected_names = {"Bob", "Charlie"}  
    actual_names = {row["name"] for row in filtered_results.to_pylist()}
    
    if actual_names == expected_names:
        print("✅ Filter results correct!")
    else:
        print(f"❌ Filter results incorrect. Expected: {expected_names}, Got: {actual_names}")
        
except Exception as e:
    print(f"❌ Filtering failed: {e}")
    import traceback
    traceback.print_exc()

print("✅ Test completed")
