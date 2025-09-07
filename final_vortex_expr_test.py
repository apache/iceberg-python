#!/usr/bin/env python3

"""
Final Vortex Expression API Investigation
=========================================

Now that I understand the literal(dtype, value) signature,
let's explore the correct way to create expressions.
"""

try:
    import vortex.expr as ve
    import vortex as vx  # Maybe we need vortex for data types
    
    print("🔍 Exploring Vortex expression API...")
    
    # Check what's in vortex module
    print(f"\nVortex module attributes: {[attr for attr in dir(vx) if not attr.startswith('_')]}")
    
    # Try creating column expression
    col_expr = ve.column("quantity")
    print(f"✅ Column expression: {col_expr}")
    
    # Test different dtype options for literals
    dtypes_to_try = [
        "int64", "i64", "int", "integer", 
        "f64", "float64", "float", "double",
        "bool", "boolean", "str", "string"
    ]
    
    print(f"\n🧪 Testing literal creation with different dtypes:")
    successful_literal = None
    for dtype in dtypes_to_try:
        try:
            lit = ve.literal(dtype, 100)
            print(f"   ✅ {dtype}: {lit}")
            if successful_literal is None:
                successful_literal = lit
            break
        except Exception as e:
            print(f"   ❌ {dtype}: {e}")
    
    # If we found a working literal, test operators
    if successful_literal:
        print(f"\n🧪 Testing operators with successful literal:")
        try:
            # Test various Python operators
            test_ops = [
                ("==", lambda c, l: c == l),
                ("!=", lambda c, l: c != l),
                (">", lambda c, l: c > l),
                (">=", lambda c, l: c >= l),
                ("<", lambda c, l: c < l),
                ("<=", lambda c, l: c <= l),
                ("&", lambda c, l: c & l),
                ("|", lambda c, l: c | l),
            ]
            
            for op_name, op_func in test_ops:
                try:
                    result = op_func(col_expr, successful_literal)
                    print(f"   ✅ {op_name}: {result} (type: {type(result)})")
                except Exception as e:
                    print(f"   ❌ {op_name}: {e}")
                    
        except Exception as e:
            print(f"❌ Operator testing failed: {e}")
    
    # Check if there are any other vortex modules for types
    try:
        import vortex.dtype as vdt
        print(f"\n📦 Found vortex.dtype: {[attr for attr in dir(vdt) if not attr.startswith('_')]}")
    except ImportError:
        print("\n❌ No vortex.dtype module")
    
    # Try some common arrow data types (since Vortex works with Arrow)
    try:
        import pyarrow as pa
        print(f"\n🏹 Testing with PyArrow dtypes:")
        arrow_dtypes = [pa.int64(), pa.float64(), pa.string(), pa.bool_()]
        
        for dtype in arrow_dtypes:
            try:
                lit = ve.literal(dtype, 100)
                print(f"   ✅ {dtype}: {lit}")
                successful_literal = lit
                break
            except Exception as e:
                print(f"   ❌ {dtype}: {e}")
                
    except ImportError:
        print("❌ PyArrow not available")

except ImportError as e:
    print(f"❌ Import error: {e}")
except Exception as e:
    print(f"❌ Unexpected error: {e}")
