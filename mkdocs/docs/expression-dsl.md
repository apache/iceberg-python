<!--
 - Licensed to the Apache Software Foundation (ASF) under one or more
 - contributor license agreements.  See the NOTICE file distributed with
 - this work for additional information regarding copyright ownership.
 - The ASF licenses this file to You under the Apache License, Version 2.0
 - (the "License"); you may not use this file except in compliance with
 - the License.  You may obtain a copy of the License at
 -
 -   http://www.apache.org/licenses/LICENSE-2.0
 -
 - Unless required by applicable law or agreed to in writing, software
 - distributed under the License is distributed on an "AS IS" BASIS,
 - WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 - See the License for the specific language governing permissions and
 - limitations under the License.
 -->

# Expression DSL

The PyIceberg library provides a powerful expression DSL (Domain Specific Language) for building complex row filter expressions. This guide will help you understand how to use the expression DSL effectively. This DSL allows you to build type-safe expressions for use in the `row_filter` scan argument.

They are composed of terms, predicates, and logical operators.

## Basic Concepts

### Terms

Terms are the basic building blocks of expressions. They represent references to fields in your data:

```python
from pyiceberg.expressions import Reference

# Create a reference to a field named "age"
age_field = Reference("age")
```

### Predicates

Predicates are expressions that evaluate to a boolean value. They can be combined using logical operators.

#### Literal Predicates

```python
from pyiceberg.expressions import EqualTo, NotEqualTo, LessThan, LessThanOrEqual, GreaterThan, GreaterThanOrEqual

# age equals 18
age_equals_18 = EqualTo("age", 18)

# age is not equal to 18
age_not_equals_18 = NotEqualTo("age", 18)

# age is less than 18
age_less_than_18 = LessThan("age", 18)

# Less than or equal to
age_less_than_or_equal_18 = LessThanOrEqual("age", 18)

# Greater than
age_greater_than_18 = GreaterThan("age", 18)

# Greater than or equal to
age_greater_than_or_equal_18 = GreaterThanOrEqual("age", 18)
```

#### Set Predicates

```python
from pyiceberg.expressions import In, NotIn

# age is one of 18, 19, 20
age_in_set = In("age", [18, 19, 20])

# age is not 18, 19, oer 20
age_not_in_set = NotIn("age", [18, 19, 20])
```

#### Unary Predicates

```python
from pyiceberg.expressions import IsNull, NotNull

# Is null
name_is_null = IsNull("name")

# Is not null
name_is_not_null = NotNull("name")
```

#### String Predicates

```python
from pyiceberg.expressions import StartsWith, NotStartsWith

# TRUE for 'Johnathan', FALSE for 'Johan'
name_starts_with = StartsWith("name", "John")

# FALSE for 'Johnathan', TRUE for 'Johan'
name_not_starts_with = NotStartsWith("name", "John")
```

### Logical Operators

You can combine predicates using logical operators:

```python
from pyiceberg.expressions import And, Or, Not

# TRUE for 25, FALSE for 67 and 15
age_between = And(
    GreaterThanOrEqual("age", 18),
    LessThanOrEqual("age", 65)
)

# FALSE for 25, TRUE for 67 and 15
age_outside = Or(
    LessThan("age", 18),
    GreaterThan("age", 65)
)

# NOT operator
not_adult = Not(GreaterThanOrEqual("age", 18))
```

## Advanced Usage

### Complex Expressions

You can build complex expressions by combining multiple predicates and operators:

```python
from pyiceberg.expressions import And, Or, Not, EqualTo, GreaterThan, LessThan, In

# (age >= 18 AND age <= 65) AND (status = 'active' OR status = 'pending')
complex_filter = And(
    And(
        GreaterThanOrEqual("age", 18),
        LessThanOrEqual("age", 65)
    ),
    Or(
        EqualTo("status", "active"),
        EqualTo("status", "pending")
    )
)

# NOT (age < 18 OR age > 65)
age_in_range = Not(
    Or(
        LessThan("age", 18),
        GreaterThan("age", 65)
    )
)
```

### Type Safety

The expression DSL provides type safety through Python's type system. When you create expressions, the types are checked at runtime:

```python
from pyiceberg.expressions import EqualTo

# This will work
age_equals_18 = EqualTo("age", 18)

# This will raise a TypeError if the field type doesn't match
age_equals_18 = EqualTo("age", "18")  # Will fail if age is an integer field
```

## Best Practices

1. **Use Type Hints**: Always use type hints when working with expressions to catch type-related errors early.

2. **Break Down Complex Expressions**: For complex expressions, break them down into smaller, more manageable parts:

```python
# Instead of this:
complex_filter = And(
    And(
        GreaterThanOrEqual("age", 18),
        LessThanOrEqual("age", 65)
    ),
    Or(
        EqualTo("status", "active"),
        EqualTo("status", "pending")
    )
)

# Do this:
age_range = And(
    GreaterThanOrEqual("age", 18),
    LessThanOrEqual("age", 65)
)

status_filter = Or(
    EqualTo("status", "active"),
    EqualTo("status", "pending")
)

complex_filter = And(age_range, status_filter)
```

## Common Pitfalls

1. **Null Handling**: Be careful when using `IsNull` and `NotNull` predicates with required fields. The expression DSL will automatically optimize these cases:
   - `IsNull` (and `IsNaN` for doubles/floats) on a required field will always return `False`
   - `NotNull` (and `NotNaN` for doubles/floats) on a required field will always return `True`

2. **String Comparisons**: When using string predicates like `StartsWith`, ensure that the field type is actually a string type.

## Examples

Here are some practical examples of using the expression DSL:

### Basic Filtering

```python

from datetime import datetime
from pyiceberg.expressions import (
    And,
    EqualTo,
    GreaterThanOrEqual,
    LessThanOrEqual,
    GreaterThan,
    In
)

active_adult_users_filter = And(
    EqualTo("status", "active"),
    GreaterThanOrEqual("age", 18)
)


high_value_customers = And(
    GreaterThan("total_spent", 1000),
    In("membership_level", ["gold", "platinum"])
)

date_range_filter = And(
    GreaterThanOrEqual("created_at", datetime(2024, 1, 1)),
    LessThanOrEqual("created_at", datetime(2024, 12, 31))
)
```

### Multi-Condition Filter

```python
from pyiceberg.expressions import And, Or, Not, EqualTo, GreaterThan

complex_filter = And(
    Not(EqualTo("status", "deleted")),
    Or(
        And(
            EqualTo("type", "premium"),
            GreaterThan("subscription_months", 12)
        ),
        EqualTo("type", "enterprise")
    )
)
```