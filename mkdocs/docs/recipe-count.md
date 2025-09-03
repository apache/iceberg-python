---
title: Count Recipe - Efficiently Count Rows in Iceberg Tables
---

# Counting Rows in an Iceberg Table

This recipe demonstrates how to use the `count()` function to efficiently count rows in an Iceberg table using PyIceberg. The count operation is optimized for performance by reading file metadata rather than scanning actual data.

## How Count Works

The `count()` method leverages Iceberg's metadata architecture to provide fast row counts by:

1. **Reading file manifests**: Examines metadata about data files without loading the actual data
2. **Aggregating record counts**: Sums up record counts stored in Parquet file footers
3. **Applying filters at metadata level**: Pushes down predicates to skip irrelevant files
4. **Handling deletes**: Automatically accounts for delete files and tombstones

## Basic Usage

Count all rows in a table:

```python
from pyiceberg.catalog import load_catalog

catalog = load_catalog("default")
table = catalog.load_table("default.cities")

# Get total row count
row_count = table.scan().count()
print(f"Total rows in table: {row_count}")
```

## Count with Filters

Count rows matching specific conditions:

```python
from pyiceberg.expressions import GreaterThan, EqualTo, And

# Count rows with population > 1,000,000
large_cities = table.scan().filter(GreaterThan("population", 1000000)).count()
print(f"Large cities: {large_cities}")

# Count rows with specific country and population criteria
filtered_count = table.scan().filter(
    And(EqualTo("country", "Netherlands"), GreaterThan("population", 100000))
).count()
print(f"Dutch cities with population > 100k: {filtered_count}")
```

## Performance Characteristics

The count operation is highly efficient because:

- **No data scanning**: Only reads metadata from file headers
- **Parallel processing**: Can process multiple files concurrently
- **Filter pushdown**: Eliminates files that don't match criteria
- **Cached statistics**: Utilizes pre-computed record counts

## Test Scenarios

Our test suite validates count behavior across different scenarios:

### Basic Counting (test_count_basic)
```python
# Simulates a table with a single file containing 42 records
assert table.scan().count() == 42
```

### Empty Tables (test_count_empty)
```python
# Handles tables with no data files
assert empty_table.scan().count() == 0
```

### Large Datasets (test_count_large)
```python
# Aggregates counts across multiple files (2 files × 500,000 records each)
assert large_table.scan().count() == 1000000
```

## Best Practices

1. **Use count() for data validation**: Verify expected row counts after ETL operations
2. **Combine with filters**: Get targeted counts without full table scans
3. **Monitor table growth**: Track record counts over time for capacity planning
4. **Validate partitions**: Count rows per partition to ensure balanced distribution

## Common Use Cases

- **Data quality checks**: Verify ETL job outputs
- **Partition analysis**: Compare record counts across partitions
- **Performance monitoring**: Track table growth and query patterns
- **Cost estimation**: Understand data volume before expensive operations

For more details and complete API documentation, see the [API documentation](api.md#count-rows-in-a-table).
