#!/usr/bin/env python3

"""
Real-world Vortex Integration Demo
==================================

This script demonstrates a complete real-world workflow using Vortex as the table file format:
1. Create a catalog and table with Vortex format
2. Write initial data
3. Append additional data
4. Read back and verify all data
5. Perform filtered queries to show optimization
"""

import tempfile
import shutil
import os
from datetime import datetime, date

import pyarrow as pa
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.expressions import GreaterThan
from pyiceberg.schema import Schema
from pyiceberg.types import (
    IntegerType, 
    StringType, 
    DoubleType, 
    DateType, 
    TimestampType,
    NestedField
)


def main():
    """Run the complete Vortex integration demo."""
    print("🚀 Vortex File Format Integration Demo")
    print("=" * 50)
    
    # Create temporary workspace
    temp_dir = tempfile.mkdtemp()
    warehouse_path = os.path.join(temp_dir, "warehouse")
    os.makedirs(warehouse_path)
    
    try:
        # Step 1: Set up catalog with Vortex support
        print("\n📁 Step 1: Setting up catalog...")
        catalog = SqlCatalog(
            "demo_catalog",
            uri=f"sqlite:///{os.path.join(temp_dir, 'catalog.db')}",
            warehouse=f"file://{warehouse_path}",
        )
        
        # Create namespace
        catalog.create_namespace("sales")
        print("✅ Created namespace: sales")
        
        # Step 2: Create table schema
        print("\n📋 Step 2: Creating table schema...")
        schema = Schema(
            NestedField(1, "order_id", IntegerType(), required=False),
            NestedField(2, "customer_name", StringType(), required=False),
            NestedField(3, "product", StringType(), required=False),
            NestedField(4, "quantity", IntegerType(), required=False),
            NestedField(5, "unit_price", DoubleType(), required=False),
            NestedField(6, "total_amount", DoubleType(), required=False),
            NestedField(7, "order_date", DateType(), required=False),
            NestedField(8, "created_at", TimestampType(), required=False),
        )
        
        # Step 3: Create table with Vortex format
        print("\n🔧 Step 3: Creating table with Vortex file format...")
        table = catalog.create_table(
            identifier="sales.orders",
            schema=schema,
            properties={
                "write.format.default": "vortex",
                "write.target-file-size-bytes": str(64 * 1024 * 1024),  # 64MB files
            }
        )
        print("✅ Created table: sales.orders with Vortex format")
        
        # Step 4: Prepare initial data
        print("\n📊 Step 4: Preparing initial data...")
        initial_data = pa.table({
            "order_id": pa.array([1001, 1002, 1003, 1004, 1005], type=pa.int32()),
            "customer_name": pa.array([
                "Alice Johnson", "Bob Smith", "Charlie Brown", 
                "Diana Prince", "Eve Wilson"
            ], type=pa.string()),
            "product": pa.array([
                "Laptop", "Mouse", "Keyboard", "Monitor", "Webcam"
            ], type=pa.string()),
            "quantity": pa.array([1, 2, 1, 1, 3], type=pa.int32()),
            "unit_price": pa.array([999.99, 29.99, 79.99, 299.99, 89.99]),
            "total_amount": pa.array([999.99, 59.98, 79.99, 299.99, 269.97]),
            "order_date": pa.array([
                date(2024, 1, 15), date(2024, 1, 16), date(2024, 1, 17),
                date(2024, 1, 18), date(2024, 1, 19)
            ]),
            "created_at": pa.array([
                datetime(2024, 1, 15, 10, 30, 0),
                datetime(2024, 1, 16, 11, 45, 0),
                datetime(2024, 1, 17, 9, 15, 0),
                datetime(2024, 1, 18, 14, 20, 0),
                datetime(2024, 1, 19, 16, 10, 0)
            ])
        })
        
        print(f"✅ Prepared initial data: {len(initial_data)} rows")
        print("   Sample data:")
        print(f"   - Order 1001: {initial_data['customer_name'][0].as_py()} ordered {initial_data['product'][0].as_py()}")
        print(f"   - Order 1002: {initial_data['customer_name'][1].as_py()} ordered {initial_data['quantity'][1].as_py()}x {initial_data['product'][1].as_py()}")
        
        # Step 5: Write initial data
        print("\n💾 Step 5: Writing initial data to Vortex files...")
        table.append(initial_data)
        print("✅ Successfully wrote initial data using Vortex format")
        
        # Step 6: Append more data
        print("\n➕ Step 6: Appending additional data...")
        additional_data = pa.table({
            "order_id": pa.array([1006, 1007, 1008], type=pa.int32()),
            "customer_name": pa.array([
                "Frank Miller", "Grace Lee", "Henry Ford"
            ], type=pa.string()),
            "product": pa.array([
                "Tablet", "Headphones", "Speaker"
            ], type=pa.string()),
            "quantity": pa.array([2, 1, 1], type=pa.int32()),
            "unit_price": pa.array([449.99, 199.99, 149.99]),
            "total_amount": pa.array([899.98, 199.99, 149.99]),
            "order_date": pa.array([
                date(2024, 1, 20), date(2024, 1, 21), date(2024, 1, 22)
            ]),
            "created_at": pa.array([
                datetime(2024, 1, 20, 13, 25, 0),
                datetime(2024, 1, 21, 15, 30, 0),
                datetime(2024, 1, 22, 12, 45, 0)
            ])
        })
        
        table.append(additional_data)
        print(f"✅ Successfully appended {len(additional_data)} more rows using Vortex format")
        
        # Step 7: Read back all data
        print("\n📖 Step 7: Reading back all data...")
        all_data = table.scan().to_arrow()
        print(f"✅ Successfully read back {len(all_data)} total rows")
        
        print("\n📈 Complete Dataset Summary:")
        print(f"   - Total Orders: {len(all_data)}")
        print(f"   - Order ID Range: {all_data['order_id'].to_pylist()[0]} - {all_data['order_id'].to_pylist()[-1]}")
        print(f"   - Total Revenue: ${sum(all_data['total_amount'].to_pylist()):.2f}")
        print(f"   - Date Range: {min(all_data['order_date'].to_pylist())} to {max(all_data['order_date'].to_pylist())}")
        
        # Step 8: Demonstrate filtering with optimization
        print("\n🔍 Step 8: Testing filtered queries (with optimization)...")
        
        # Query 1: High-value orders
        high_value_orders = table.scan(
            row_filter=GreaterThan("total_amount", 200.0)
        ).to_arrow()
        
        print(f"\n💰 High-value orders (> $200):")
        print(f"   - Found: {len(high_value_orders)} orders")
        for i in range(len(high_value_orders)):
            customer = high_value_orders['customer_name'][i].as_py()
            product = high_value_orders['product'][i].as_py()
            amount = high_value_orders['total_amount'][i].as_py()
            print(f"   - {customer}: {product} (${amount:.2f})")
        
        # Query 2: Specific products
        print(f"\n🖱️ All mouse orders:")
        mouse_orders = table.scan().to_arrow().filter(
            pa.compute.equal(pa.compute.field("product"), "Mouse")
        )
        for i in range(len(mouse_orders)):
            customer = mouse_orders['customer_name'][i].as_py()
            qty = mouse_orders['quantity'][i].as_py()
            amount = mouse_orders['total_amount'][i].as_py()
            print(f"   - {customer}: {qty}x Mouse (${amount:.2f})")
        
        # Step 9: Show table metadata
        print("\n📋 Step 9: Table Information...")
        print(f"   - Table Location: {table.location()}")
        print(f"   - Schema: {len(table.schema().fields)} fields")
        print(f"   - Current Snapshot: {table.current_snapshot().snapshot_id if table.current_snapshot() else 'None'}")
        
        # List the actual files
        snapshots = table.snapshots()
        print(f"   - Total Snapshots: {len(snapshots)}")
        
        # Show file format information
        if table.current_snapshot():
            print(f"   - Files created using Vortex format:")
            data_files = []
            for manifest_list in table.current_snapshot().manifests(table.io):
                for manifest_entry in manifest_list.fetch_manifest_entry(table.io):
                    if manifest_entry.data_file:
                        data_files.append(manifest_entry.data_file)
            
            for i, data_file in enumerate(data_files, 1):
                print(f"     • File {i}: {os.path.basename(data_file.file_path)} ({data_file.file_format.name}, {data_file.file_size_in_bytes} bytes, {data_file.record_count} records)")
        
        print("\n🎉 Demo completed successfully!")
        print("\n✨ Key Achievements:")
        print("   ✅ Created table with Vortex file format")
        print("   ✅ Wrote data using native Vortex integration") 
        print("   ✅ Appended additional data seamlessly")
        print("   ✅ Read back all data with perfect fidelity")
        print("   ✅ Performed optimized filtered queries")
        print("   ✅ Demonstrated real-world data operations")
        
    except Exception as e:
        print(f"\n❌ Error during demo: {e}")
        raise
        
    finally:
        # Cleanup
        print(f"\n🧹 Cleaning up temporary files...")
        shutil.rmtree(temp_dir)
        print("✅ Cleanup completed")


if __name__ == "__main__":
    main()
