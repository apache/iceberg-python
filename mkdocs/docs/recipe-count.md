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
# Count rows with population > 1,000,000
large_cities = table.scan().filter("population > 1000000").count()
print(f"Large cities: {large_cities}")

# Count rows with specific country and population criteria
filtered_count = table.scan().filter("country = 'Netherlands' AND population > 100000").count()
print(f"Dutch cities with population > 100k: {filtered_count}")
```

## Count with Limit

The `count()` method supports a `limit` parameter for efficient counting when you only need to know if a table has at least N rows, or when working with very large datasets:

```python
# Check if table has at least 1000 rows (stops counting after reaching 1000)
has_enough_rows = table.scan().count(limit=1000) >= 1000
print(f"Table has at least 1000 rows: {has_enough_rows}")

# Get count up to a maximum of 10,000 rows
limited_count = table.scan().count(limit=10000)
print(f"Row count (max 10k): {limited_count}")

# Combine limit with filters for efficient targeted counting
recent_orders_sample = table.scan().filter("order_date > '2023-01-01'").count(limit=5000)
print(f"Recent orders (up to 5000): {recent_orders_sample}")
```

### Performance Benefits of Limit

Using the `limit` parameter provides significant performance improvements:

- **Early termination**: Stops processing files once the limit is reached
- **Reduced I/O**: Avoids reading metadata from unnecessary files
- **Memory efficiency**: Processes only the minimum required data
- **Faster response**: Ideal for existence checks and sampling operations

!!! tip "When to Use Limit"

    **Use `limit` when:**
    - Checking if a table has "enough" data (existence checks)
    - Sampling row counts from very large tables
    - Building dashboards that show approximate counts
    - Validating data ingestion without full table scans

    **Example use cases:**
    - Data quality gates: "Does this partition have at least 1000 rows?"
    - Monitoring alerts: "Are there more than 100 error records today?"
    - Approximate statistics: "Show roughly how many records per hour"

## Performance Characteristics

The count operation is highly efficient because:

- **No data scanning**: Only reads metadata from file headers
- **Parallel processing**: Can process multiple files concurrently
- **Filter pushdown**: Eliminates files that don't match criteria
- **Cached statistics**: Utilizes pre-computed record counts

!!! tip "Even Faster: Use Snapshot Properties"

    For the fastest possible total row count (without filters), you can access the cached count directly from snapshot properties, avoiding any table scanning:

    ```python
    # Get total records from snapshot metadata (fastest method)
    total_records = table.current_snapshot().summary.additional_properties["total-records"]
    print(f"Total rows from snapshot: {total_records}")
    ```

    **When to use this approach:**
    - When you need the total table row count without any filters
    - For dashboard queries that need instant response times
    - When working with very large tables where even metadata scanning takes time
    - For monitoring and alerting systems that check table sizes frequently

    **Note:** This method only works for total counts. For filtered counts, use `table.scan().filter(...).count()`.

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

### Limit Functionality (test_count_with_limit_mock)
```python
# Tests that limit parameter is respected and provides early termination
limited_count = table.scan().count(limit=50)
assert limited_count == 50  # Stops at limit even if more rows exist

# Test with limit larger than available data
all_rows = small_table.scan().count(limit=1000)
assert all_rows == 42  # Returns actual count when limit > total rows
```

### Integration Testing (test_datascan_count_respects_limit)
```python
# Full end-to-end validation with real table operations
# Creates table, adds data, verifies limit behavior in realistic scenarios
assert table.scan().count(limit=1) == 1
assert table.scan().count() > 1  # Unlimited count returns more
```

## Best Practices

1. **Use count() for data validation**: Verify expected row counts after ETL operations
2. **Combine with filters**: Get targeted counts without full table scans
3. **Leverage limit for existence checks**: Use `count(limit=N)` when you only need to know if a table has at least N rows
4. **Monitor table growth**: Track record counts over time for capacity planning
5. **Validate partitions**: Count rows per partition to ensure balanced distribution
6. **Use appropriate limits**: Set sensible limits for dashboard queries and monitoring to improve response times

!!! warning "Limit Considerations"

    When using `limit`, remember that:
    - The count may be less than the actual total if limit is reached
    - Results are deterministic but depend on file processing order
    - Use unlimited count when you need exact totals
    - Combine with filters for more targeted limited counting

## Common Use Cases

- **Data quality checks**: Verify ETL job outputs
- **Partition analysis**: Compare record counts across partitions
- **Performance monitoring**: Track table growth and query patterns
- **Cost estimation**: Understand data volume before expensive operations

For more details and complete API documentation, see the [API documentation](api.md#count-rows-in-a-table).
