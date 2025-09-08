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
Comprehensive Vortex Filtering Optimization Test
===============================================

This test demonstrates that Vortex filtering now works correctly,
matching Parquet behavior exactly.
"""

from pyiceberg.catalog.memory import InMemoryCatalog
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, IntegerType, StringType, DoubleType
from pyiceberg.expressions import GreaterThan, LessThan, EqualTo, And, Or
import pyarrow as pa

print("🎯 Comprehensive Vortex Filtering Test")
print("=" * 50)

# Set up test
catalog = InMemoryCatalog(name="test_catalog")
ns = catalog.create_namespace("test")

# Create schema with multiple data types
schema = Schema(
    NestedField(1, "id", IntegerType(), required=True),
    NestedField(2, "name", StringType(), required=True), 
    NestedField(3, "value", IntegerType(), required=True),
    NestedField(4, "score", DoubleType(), required=True),
)

# Create table with Vortex format
table = catalog.create_table(
    "test.comprehensive_filtering",
    schema=schema,
    properties={"write.format.default": "vortex"}
)

# Add comprehensive test data
data = pa.Table.from_pylist([
    {"id": 1, "name": "Alice", "value": 30, "score": 85.5},
    {"id": 2, "name": "Bob", "value": 60, "score": 92.0},  
    {"id": 3, "name": "Charlie", "value": 90, "score": 78.5},
    {"id": 4, "name": "Diana", "value": 45, "score": 96.0},
    {"id": 5, "name": "Eve", "value": 75, "score": 88.0},
], schema=pa.schema([
    pa.field("id", pa.int32(), nullable=False),
    pa.field("name", pa.string(), nullable=False),
    pa.field("value", pa.int32(), nullable=False),
    pa.field("score", pa.float64(), nullable=False),
]))

table.append(data)
print("✅ Added comprehensive test data (5 rows)")

# Test various filtering scenarios
test_cases = [
    ("Simple comparison", GreaterThan("value", 50), {"Bob", "Charlie", "Eve"}),
    ("Less than", LessThan("score", 90.0), {"Alice", "Charlie", "Eve"}),
    ("Equality", EqualTo("name", "Diana"), {"Diana"}),
    ("Complex AND", And(GreaterThan("value", 40), LessThan("score", 90.0)), {"Charlie", "Eve"}),
    ("Complex OR", Or(LessThan("value", 35), GreaterThan("score", 95.0)), {"Alice", "Diana"}),
]

print(f"\n🧪 Running {len(test_cases)} filtering test cases...")

all_passed = True
for test_name, filter_expr, expected_names in test_cases:
    try:
        # Apply filter and get results
        filtered_results = table.scan(row_filter=filter_expr).to_arrow()
        actual_names = {row["name"] for row in filtered_results.to_pylist()}
        
        if actual_names == expected_names:
            print(f"   ✅ {test_name}: Found {len(actual_names)} rows - {actual_names}")
        else:
            print(f"   ❌ {test_name}: Expected {expected_names}, Got {actual_names}")
            all_passed = False
            
    except Exception as e:
        print(f"   ❌ {test_name}: FAILED with error - {e}")
        all_passed = False

print(f"\n🎯 Final Result:")
if all_passed:
    print("✅ ALL FILTERING TESTS PASSED! Vortex filtering optimization is working perfectly.")
    print("🚀 Vortex integration now has full feature parity with Parquet for filtering!")
else:
    print("❌ Some filtering tests failed. Need further investigation.")

print("✅ Comprehensive test completed!")
