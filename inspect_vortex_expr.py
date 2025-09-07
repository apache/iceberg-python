#!/usr/bin/env python3

"""
Inspect Vortex Expression API
============================

This script investigates the actual vortex.expr API to understand
what functions are available for filter expressions.
"""

try:
    import vortex as vx
    import vortex.expr as ve
    
    print("✅ Vortex modules imported successfully")
    print("\n🔍 Inspecting vortex.expr module...")
    
    # Get all attributes from vortex.expr
    expr_attributes = [attr for attr in dir(ve) if not attr.startswith('_')]
    
    print(f"📋 Available vortex.expr attributes ({len(expr_attributes)}):")
    for attr in sorted(expr_attributes):
        try:
            obj = getattr(ve, attr)
            obj_type = type(obj).__name__
            print(f"   • {attr}: {obj_type}")
        except Exception as e:
            print(f"   • {attr}: Error - {e}")
    
    print("\n🔧 Testing basic expression creation...")
    
    # Test creating basic expressions
    test_cases = [
        ("col", "ve.col('test_col')"),
        ("eq", "ve.eq if hasattr(ve, 'eq') else None"),
        ("equal", "ve.equal if hasattr(ve, 'equal') else None"),
        ("gt", "ve.gt if hasattr(ve, 'gt') else None"),
        ("greater", "ve.greater if hasattr(ve, 'greater') else None"),
        ("greater_than", "ve.greater_than if hasattr(ve, 'greater_than') else None"),
        ("lt", "ve.lt if hasattr(ve, 'lt') else None"),
        ("less", "ve.less if hasattr(ve, 'less') else None"),
        ("less_than", "ve.less_than if hasattr(ve, 'less_than') else None"),
    ]
    
    print("\n🧪 Testing expression functions:")
    for name, test_code in test_cases:
        try:
            result = eval(test_code)
            if result is not None:
                print(f"   ✅ {name}: {result}")
            else:
                print(f"   ❌ {name}: Not available")
        except Exception as e:
            print(f"   ❌ {name}: Error - {e}")
    
    # Try to create a simple column reference
    print("\n🏗️ Testing column creation:")
    try:
        test_col = ve.col('test_column')
        print(f"   ✅ ve.col('test_column'): {test_col} (type: {type(test_col)})")
    except Exception as e:
        print(f"   ❌ ve.col() failed: {e}")
    
    # Try some common expression patterns
    print("\n⚡ Testing common expression patterns:")
    patterns_to_try = [
        "ve.Column",
        "ve.column", 
        "ve.field",
        "ve.Expr",
        "ve.Expression",
        "ve.BinaryExpr",
        "ve.ComparisonExpr",
    ]
    
    for pattern in patterns_to_try:
        try:
            if hasattr(ve, pattern.split('.')[-1]):
                obj = getattr(ve, pattern.split('.')[-1])
                print(f"   ✅ {pattern}: {obj}")
            else:
                print(f"   ❌ {pattern}: Not available")
        except Exception as e:
            print(f"   ❌ {pattern}: Error - {e}")

except ImportError as e:
    print(f"❌ Could not import vortex modules: {e}")
except Exception as e:
    print(f"❌ Unexpected error: {e}")
