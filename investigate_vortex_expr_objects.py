#!/usr/bin/env python3

"""
Investigate Vortex Expr object methods
=====================================

Since we found ve.column() and ve.Expr class, let's see what methods
are available on Expr objects for comparisons.
"""

try:
    import vortex.expr as ve
    
    print("🔍 Testing Expr object creation and methods...")
    
    # Create a column expression
    try:
        col_expr = ve.column("test_column")
        print(f"✅ Created column: {col_expr} (type: {type(col_expr)})")
        
        # Inspect methods on the Expr object
        expr_methods = [attr for attr in dir(col_expr) if not attr.startswith('_')]
        print(f"\n📋 Available methods on Expr ({len(expr_methods)}):")
        for method in sorted(expr_methods):
            try:
                obj = getattr(col_expr, method)
                obj_type = type(obj).__name__
                print(f"   • {method}: {obj_type}")
            except Exception as e:
                print(f"   • {method}: Error - {e}")
    
    except Exception as e:
        print(f"❌ Failed to create column: {e}")
    
    # Test literal creation
    try:
        # Try different ways to create a literal
        print("\n🧪 Testing literal creation...")
        print(f"   ve.literal signature: {ve.literal}")
        
        # Try with a value
        lit_expr = ve.literal(42)
        print(f"✅ Created literal: {lit_expr} (type: {type(lit_expr)})")
        
        # Check literal methods
        lit_methods = [attr for attr in dir(lit_expr) if not attr.startswith('_')]
        print(f"   Literal methods: {lit_methods}")
        
    except Exception as e:
        print(f"❌ Failed to create literal: {e}")
    
    # Try Python operator overloading
    print("\n🧪 Testing Python operator overloading on Expr...")
    try:
        col_expr = ve.column("value")
        
        # Try to understand literal better
        import inspect
        print(f"   literal signature: {inspect.signature(ve.literal)}")
        
        lit_expr = ve.literal(50)
        
        # Test various operators
        operators_to_test = [
            ("==", "col_expr == lit_expr"),
            ("!=", "col_expr != lit_expr"), 
            (">", "col_expr > lit_expr"),
            (">=", "col_expr >= lit_expr"),
            ("<", "col_expr < lit_expr"),
            ("<=", "col_expr <= lit_expr"),
            ("&", "col_expr & lit_expr"),
            ("|", "col_expr | lit_expr"),
        ]
        
        for op_name, op_code in operators_to_test:
            try:
                result = eval(op_code)
                print(f"   ✅ {op_name}: {result} (type: {type(result)})")
            except Exception as e:
                print(f"   ❌ {op_name}: {e}")
                
    except Exception as e:
        print(f"❌ Operator testing failed: {e}")
    
    # Try to understand the Expr class better
    print("\n🔬 Investigating Expr class...")
    try:
        print(f"   Expr class: {ve.Expr}")
        print(f"   Expr.__doc__: {ve.Expr.__doc__}")
        
        # Check if Expr has class methods
        expr_class_methods = [attr for attr in dir(ve.Expr) if not attr.startswith('_')]
        print(f"   Expr class methods ({len(expr_class_methods)}): {expr_class_methods}")
        
    except Exception as e:
        print(f"❌ Expr class investigation failed: {e}")

except ImportError as e:
    print(f"❌ Could not import vortex.expr: {e}")
except Exception as e:
    print(f"❌ Unexpected error: {e}")
