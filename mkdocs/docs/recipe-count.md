---
title: Count Recipe
---

# Counting Rows in an Iceberg Table

This recipe demonstrates how to use the `count()` function to efficiently count rows in an Iceberg table using PyIceberg.

## Basic Usage

To count all rows in a table:

```python
from pyiceberg.catalog import load_catalog

catalog = load_catalog("default")
table = catalog.load_table("default.cities")

row_count = table.count()
print(f"Total rows in table: {row_count}")
```

## Count with a Filter

To count only rows matching a filter:

```python
from pyiceberg.expressions import EqualTo

count = table.scan(row_filter=EqualTo("city", "Amsterdam")).count()
print(f"Rows with city == 'Amsterdam': {count}")
```

## Notes
- The `count()` method works for both catalog and static tables.
- Filters can be applied using the `scan` API for more granular counts.
- Deleted records are excluded from the count.

For more details, see the [API documentation](api.md).
