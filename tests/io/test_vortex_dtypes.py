#!/usr/bin/env python3

"""
Working with Vortex DTypes
==========================

Now I can see Vortex has its own data type system with:
BinaryDType, BoolDType, PrimitiveDType, etc.
"""

try:
    import vortex as vx
    import vortex.expr as ve

    print("🔍 Testing Vortex native DTypes...")

    # Test creating literals with Vortex native dtypes
    dtypes_to_test = [
        ("PrimitiveDType", vx.PrimitiveDType),
        ("BoolDType", vx.BoolDType),
        ("BinaryDType", vx.BinaryDType),
        ("Utf8DType", vx.Utf8DType),
    ]

    col_expr = ve.column("quantity")
    print(f"✅ Column: {col_expr}")

    successful_literal = None

    for dtype_name, dtype_cls in dtypes_to_test:
        try:
            # Try to create a dtype instance first
            if dtype_name == "PrimitiveDType":
                # Try different primitive types
                for ptype in ["i64", "f64", "u64"]:
                    try:
                        # Maybe PrimitiveDType needs a primitive type parameter
                        dtype_instance = dtype_cls(ptype)
                        lit = ve.literal(dtype_instance, 100)
                        print(f"   ✅ {dtype_name}({ptype}): {lit}")
                        successful_literal = lit
                        break
                    except Exception as e:
                        print(f"   ❌ {dtype_name}({ptype}): {e}")
            else:
                try:
                    # Try instantiating the dtype
                    dtype_instance = dtype_cls()
                    lit = ve.literal(dtype_instance, 100)
                    print(f"   ✅ {dtype_name}: {lit}")
                    successful_literal = lit
                    break
                except Exception as e:
                    print(f"   ❌ {dtype_name}: {e}")

        except Exception as e:
            print(f"   ❌ {dtype_name} (outer): {e}")

    # Try using vortex helper functions
    try:
        print("\n🧪 Testing vortex helper functions...")

        # Try vortex.int_, float_, bool_, etc
        helpers_to_test = [
            ("int_", vx.int_, 100),
            ("float_", vx.float_, 100.5),
            ("bool_", vx.bool_, True),
        ]

        for helper_name, helper_func, test_value in helpers_to_test:
            try:
                # These might return dtype objects we can use
                dtype_result = helper_func()
                print(f"   {helper_name}(): {dtype_result} (type: {type(dtype_result)})")

                # Try using this as dtype for literal
                lit = ve.literal(dtype_result, test_value)
                print(f"   ✅ literal with {helper_name}: {lit}")
                successful_literal = lit
                break

            except Exception as e:
                print(f"   ❌ {helper_name}: {e}")

    except Exception as e:
        print(f"❌ Helper function testing failed: {e}")

    # If we have a successful literal, test operators
    if successful_literal:
        print("\n🧪 Testing operators:")
        operators = ["==", "!=", ">", "<", ">=", "<="]
        for op in operators:
            try:
                if op == "==":
                    result = col_expr == successful_literal
                elif op == "!=":
                    result = col_expr != successful_literal
                elif op == ">":
                    result = col_expr > successful_literal
                elif op == "<":
                    result = col_expr < successful_literal
                elif op == ">=":
                    result = col_expr >= successful_literal
                elif op == "<=":
                    result = col_expr <= successful_literal
                print(f"   ✅ {op}: {result} (type: {type(result)})")
            except Exception as e:
                print(f"   ❌ {op}: {e}")

    # Try the simplest possible approach - maybe we don't need complex dtypes
    print("\n🧪 Testing minimal literal creation...")
    simple_values = [100, 100.5, True, "test"]
    for val in simple_values:
        try:
            # Maybe there's a simpler literal function or the dtype can be inferred
            print(f"   Trying value {val} (type: {type(val).__name__})")

            # Check if vortex has a scalar function that we can use for dtype
            if hasattr(vx, "scalar"):
                try:
                    scalar_obj = vx.scalar(val)
                    print(f"     vx.scalar({val}): {scalar_obj} (type: {type(scalar_obj)})")
                    if hasattr(scalar_obj, "dtype"):
                        lit = ve.literal(scalar_obj.dtype, val)
                        print(f"     ✅ Using scalar dtype: {lit}")
                        successful_literal = lit
                        break
                except Exception as e:
                    print(f"     ❌ scalar approach: {e}")

        except Exception as e:
            print(f"   ❌ Simple value {val}: {e}")

except ImportError as e:
    print(f"❌ Import error: {e}")
except Exception as e:
    print(f"❌ Unexpected error: {e}")
