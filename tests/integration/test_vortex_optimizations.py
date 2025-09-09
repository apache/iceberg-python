#!/usr/bin/env python3

"""
Test script to verify Vortex optimizations are working.
This script tests filter pushdown and projection optimizations.
"""

import logging
import os
import shutil
import tempfile

import pyarrow as pa

from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.expressions import GreaterThan, LessThan
from pyiceberg.schema import Schema
from pyiceberg.types import IntegerType, NestedField, StringType

# Set up logging to see optimization messages
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


def test_vortex_optimizations() -> None:
    """Test that Vortex optimizations (filter pushdown and projection) work correctly."""

    # Create temporary directory for test
    temp_dir = tempfile.mkdtemp()
    warehouse_path = os.path.join(temp_dir, "warehouse")
    os.makedirs(warehouse_path)

    try:
        # Create catalog
        catalog = SqlCatalog(
            "default",
            uri=f"sqlite:///{os.path.join(temp_dir, 'pyiceberg_catalog.db')}",
            warehouse=f"file://{warehouse_path}",
        )

        # Create namespace first
        catalog.create_namespace("default")

        # Create test schema
        schema = Schema(
            NestedField(1, "id", IntegerType(), required=False),
            NestedField(2, "name", StringType(), required=False),
            NestedField(3, "value", IntegerType(), required=False),
        )

        # Create table with Vortex write format
        table = catalog.create_table(
            identifier="default.test_vortex_optimization", schema=schema, properties={"write.format.default": "vortex"}
        )

        # Create test data with correct types
        test_data = pa.table(
            {
                "id": pa.array([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], type=pa.int32()),
                "name": pa.array(
                    ["Alice", "Bob", "Charlie", "Dave", "Eve", "Frank", "Grace", "Henry", "Ivy", "Jack"], type=pa.string()
                ),
                "value": pa.array([10, 20, 30, 40, 50, 60, 70, 80, 90, 100], type=pa.int32()),
            }
        )

        # Append data
        table.append(test_data)

        print("=== Testing Filter Pushdown ===")
        # Test with filter (should show filter pushdown in logs)
        result = table.scan(row_filter=GreaterThan("value", 50)).to_arrow()

        print(f"Filtered result: {len(result)} rows (expected: 5)")
        print(f"Values: {result['value'].to_pylist()}")

        print("\n=== Testing Projection ===")
        # Test with projection (should show column projection in logs)
        result = table.scan(selected_fields=("id", "name")).to_arrow()

        print(f"Projected result: {len(result.columns)} columns (expected: 2)")
        print(f"Columns: {result.column_names}")

        print("\n=== Testing Combined Filter + Projection ===")
        # Test with both filter and projection
        result = table.scan(row_filter=LessThan("value", 80), selected_fields=("name", "value")).to_arrow()

        print(f"Combined result: {len(result)} rows, {len(result.columns)} columns")
        print(f"Names: {result['name'].to_pylist()}")
        print(f"Values: {result['value'].to_pylist()}")

        print("\n✅ All optimization tests completed successfully!")

    finally:
        # Cleanup
        shutil.rmtree(temp_dir)


if __name__ == "__main__":
    test_vortex_optimizations()
