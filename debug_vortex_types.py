#!/usr/bin/env python3

"""
Test Vortex Data Type Compatibility
===================================

Let's see what data types are actually being used when we read/write Vortex files
and how they interact with filter expressions.
"""

import tempfile
import os
import pyarrow as pa

try:
    import vortex as vx
    import vortex.expr as ve
    
    # Create some test data with int32 (like the failing test)
    test_data = pa.table({
        "id": pa.array([1, 2, 3, 4, 5], type=pa.int32()),
        "value": pa.array([10, 20, 30, 40, 50], type=pa.int32())
    })
    
    print(f"🔍 Original PyArrow table:")
    print(f"   Schema: {test_data.schema}")
    print(f"   Data types: {[field.type for field in test_data.schema]}")
    
    # Write to Vortex and read back
    with tempfile.NamedTemporaryFile(suffix='.vortex', delete=False) as tmp_file:
        temp_path = tmp_file.name
    
    try:
        # Write using Vortex
        print(f"\n📝 Writing to Vortex file: {temp_path}")
        vortex_array = vx.array(test_data)
        vx.io.write(temp_path, vortex_array)
        print("✅ Write successful")
        
        # Read back using Vortex
        print(f"\n📖 Reading from Vortex file...")
        vortex_file = vx.open(temp_path)
        read_back = vortex_file.to_arrow()
        
        print(f"   Read back schema: {read_back.schema}")
        print(f"   Read back types: {[field.type for field in read_back.schema]}")
        
        # Test creating filter expressions with different literal types
        print(f"\n🧪 Testing filter expressions...")
        
        # Test with int32 literal
        try:
            lit_int32 = ve.literal(vx.int_(), 30)  # This creates i64
            print(f"   vx.int_() literal: {lit_int32} -> i64")
        except Exception as e:
            print(f"   ❌ vx.int_() failed: {e}")
        
        # Test with scalar inference  
        try:
            scalar_30 = vx.scalar(30)
            print(f"   vx.scalar(30): {scalar_30} (dtype: {scalar_30.dtype})")
            lit_scalar = ve.literal(scalar_30.dtype, 30)
            print(f"   Scalar literal: {lit_scalar}")
        except Exception as e:
            print(f"   ❌ scalar approach failed: {e}")
            
        # Try creating a filter expression
        try:
            col_expr = ve.column("value")
            print(f"   Column expr: {col_expr}")
            
            # Create literal that might match the column type
            lit_expr = ve.literal(vx.scalar(30).dtype, 30)
            filter_expr = col_expr > lit_expr
            print(f"   Filter expr: {filter_expr}")
            
            # Try using the filter
            filtered = vortex_file.scan(expr=filter_expr)
            filtered_arrow = filtered.to_arrow()
            print(f"   ✅ Filter worked! Got {len(filtered_arrow)} rows")
            
        except Exception as e:
            print(f"   ❌ Filter failed: {e}")
            
            # Try a different approach - maybe the column data changed type
            print(f"\n🔬 Investigating column types in Vortex file...")
            try:
                # Get just the first column to check its type
                value_col = read_back.column("value")
                print(f"   Value column type: {value_col.type}")
                print(f"   Value column data: {value_col.to_pylist()}")
                
                # Maybe we need to match the exact type from the file
                if str(value_col.type) == 'int32':
                    print("   Trying int32-specific literal creation...")
                    # Create a literal that exactly matches
                    import numpy as np
                    val_np = np.int32(30)
                    scalar_int32 = vx.scalar(val_np)
                    print(f"   np.int32 scalar: {scalar_int32} (dtype: {scalar_int32.dtype})")
                    
                    lit_int32 = ve.literal(scalar_int32.dtype, val_np)
                    filter_int32 = col_expr > lit_int32
                    print(f"   int32 filter: {filter_int32}")
                    
                    # Test this filter
                    filtered_int32 = vortex_file.scan(expr=filter_int32)
                    result_int32 = filtered_int32.to_arrow()
                    print(f"   ✅ int32 filter worked! Got {len(result_int32)} rows")
                    
            except Exception as e2:
                print(f"   ❌ Column type investigation failed: {e2}")
                
    finally:
        # Cleanup
        if os.path.exists(temp_path):
            os.unlink(temp_path)
            
except ImportError as e:
    print(f"❌ Could not import vortex: {e}")
except Exception as e:
    print(f"❌ Unexpected error: {e}")
