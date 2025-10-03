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

# Row Filter Syntax

In addition to the primary [Expression DSL](expression-dsl.md), PyIceberg provides a string-based statement interface for filtering rows in Iceberg tables. This guide explains the syntax and provides examples for supported operations.

The row filter syntax is designed to be similar to SQL WHERE clauses. Here are the basic components:

## Column References

Columns can be referenced using either unquoted or quoted identifiers:

```sql
column_name
"column.name"
```

## Literals

The following literal types are supported:

- Strings: `'hello world'`
- Numbers: `42`, `-42`, `3.14`
- Booleans: `true`, `false` (case insensitive)

## Comparison Operations

### Basic Comparisons

```sql
column = 42
column != 42
column > 42
column >= 42
column < 42
column <= 42
```

!!! note
    The `==` operator is an alias for `=` and `<>` is an alias for `!=`

### String Comparisons

```sql
column = 'hello'
column != 'world'
```

## NULL Checks

Check for NULL values using the `IS NULL` and `IS NOT NULL` operators:

```sql
column IS NULL
column IS NOT NULL
```

## NaN Checks

For floating-point columns, you can check for NaN values:

```sql
column IS NAN
column IS NOT NAN
```

## IN and NOT IN

Check if a value is in a set of values:

```sql
column IN ('a', 'b', 'c')
column NOT IN (1, 2, 3)
```

## LIKE Operations

The LIKE operator supports pattern matching with a wildcard `%` at the end of the string:

```sql
column LIKE 'prefix%'
column NOT LIKE 'prefix%'
```

!!! important
    The `%` wildcard is only supported at the end of the pattern. Using it in the middle or beginning of the pattern will raise an error.

## BETWEEN

The BETWEEN operator filters a column against an inclusive range of two comparable literals, e.g. `a between 1 and 2` is equivalent to `a >= 1 and a <= 2`.

```sql
column BETWEEN 1 AND 2
column BETWEEN 1.0 AND 2.0
column BETWEEN '2025-01-01' AND '2025-01-02'
column BETWEEN '2025-01-01T00:00:00.000000' AND '2025-01-01T00:00:00.000000'
```

## Logical Operations

Combine multiple conditions using logical operators:

```sql
column1 = 42 AND column2 = 'hello'
column1 > 0 OR column2 IS NULL
NOT (column1 = 42)
```

!!! tip
    Parentheses can be used to group logical operations for clarity:
    ```sql
    (column1 = 42 AND column2 = 'hello') OR column3 IS NULL
    ```

## Complete Examples

Here are some complete examples showing how to combine different operations:

```sql
-- Complex filter with multiple conditions
status = 'active' AND age > 18 AND NOT (country IN ('US', 'CA'))

-- Filter with string pattern matching
name LIKE 'John%' AND age >= 21

-- Filter with NULL checks and numeric comparisons
price IS NOT NULL AND price > 100 AND quantity > 0

-- Filter with multiple logical operations
(status = 'pending' OR status = 'processing') AND NOT (priority = 'low')
```

## Common Pitfalls

1. **String Quoting**: Always use single quotes for string literals. Double quotes are reserved for column identifiers.

   ```sql
   -- Correct
   name = 'John'

   -- Incorrect
   name = "John"
   ```

2. **Wildcard Usage**: The `%` wildcard in LIKE patterns can only appear at the end.

   ```sql
   -- Correct
   name LIKE 'John%'

   -- Incorrect (will raise an error)
   name LIKE '%John%'
   ```

3. **Case Sensitivity**: Boolean literals (`true`/`false`) are case insensitive, but string comparisons are case sensitive.

   ```sql
   -- All valid
   is_active = true
   is_active = TRUE
   is_active = True

   -- Case sensitive
   status = 'Active'  -- Will not match 'active'
   ```

## Best Practices

1. For complex use cases, use the primary [Expression DSL](expression-dsl.md)
2. When using multiple conditions, consider the order of operations (NOT > AND > OR)
3. For string comparisons, be consistent with case usage
