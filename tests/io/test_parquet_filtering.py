#!/usr/bin/env python3

"""
Test filtering with Parquet format to see if it's a broader PyIceberg issue
"""

from pyiceberg.catalog.memory import InMemoryCatalog
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, IntegerType, StringType
from pyiceberg.expressions import GreaterThan
import pyarrow as pa

print("🧪 Testing filtering with Parquet format...")

# Set up test
catalog = InMemoryCatalog(name="test_catalog")
ns = catalog.create_namespace("test")

# Create schema
schema = Schema(
    NestedField(1, "id", IntegerType(), required=True),
    NestedField(2, "name", StringType(), required=True), 
    NestedField(3, "value", IntegerType(), required=True),
)

# Create table with default Parquet format
table = catalog.create_table(
    "test.parquet_filtering_test",
    schema=schema,
    # Default is Parquet - no properties needed
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

print("✅ Data added to Parquet table successfully")

# Test filtering with Parquet
try:
    filtered_results = table.scan(row_filter=GreaterThan("value", 50)).to_arrow()
    print(f"✅ Parquet filtering works: Found {len(filtered_results)} rows")
    print(f"   Filtered data: {filtered_results.to_pylist()}")
    
    expected_names = {"Bob", "Charlie"}  
    actual_names = {row["name"] for row in filtered_results.to_pylist()}
    
    if actual_names == expected_names:
        print("✅ Parquet filter results correct!")
    else:
        print(f"❌ Parquet filter results incorrect. Expected: {expected_names}, Got: {actual_names}")
        
except Exception as e:
    print(f"❌ Parquet filtering failed: {e}")
    import traceback
    traceback.print_exc()

print("✅ Parquet test completed")
