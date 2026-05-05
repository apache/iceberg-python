---
hide:
  - navigation
---

<!--
  - Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use it except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Practical Examples

This guide provides practical guidance for common PyIceberg use cases and implementation patterns.

## Common Use Cases

### CSV Migration

Migrating CSV files to Iceberg tables involves reading CSV data, converting it to Iceberg's schema, and writing it to Iceberg tables. This is one of the most common migration scenarios.

**Key Steps**:

1. Read CSV files using PyArrow
2. Convert data types appropriately
3. Create Iceberg table with proper schema
4. Write data to Iceberg table
5. Validate migration success

**Best Practices**:

- Use PyArrow for efficient CSV reading
- Handle missing values explicitly
- Validate data ranges and types
- Consider partitioning for large datasets

### Time Travel Queries

Iceberg's time travel feature allows you to query historical data and manage table versions through snapshots.

**Key Concepts**:

- **Snapshots**: Each commit creates a snapshot with unique ID and timestamp
- **Historical Queries**: Query data as it existed at specific times
- **Rollback**: Revert tables to previous states when needed
- **Audit Trail**: Complete history of all table changes

**Common Patterns**:

- Query data as of a specific snapshot ID
- Query data as of a specific timestamp
- List table history to track changes
- Rollback to known good states

### Data Quality Management

Implementing data quality checks during and after migration ensures data integrity.

**Validation Steps**:

- Row count validation
- Data sampling and comparison
- Query validation with representative tests
- Performance comparison

**Common Issues**:

- Schema mismatches between source and target
- Missing or null values
- Duplicate records
- Data type conversion errors

## Implementation Patterns

### Data Migration Pattern

```python
import pyarrow.csv as csv_pa
from pyiceberg.catalog import load_catalog

# Read CSV
csv_data = csv_pa.read_csv('data.csv')

# Create Iceberg table
catalog = load_catalog("my_catalog")
table = catalog.create_table("my_table", schema=csv_data.schema)

# Migrate data
table.append(csv_data)
```

### Time Travel Pattern

```python
# Query historical data
historical_data = table.scan(snapshot_id=old_snapshot_id).to_arrow()

# View table history
for snapshot in table.history():
    print(f"Snapshot: {snapshot.snapshot_id}, Time: {snapshot.timestamp_ms}")
```

### Schema Evolution Pattern

```python
# Add new column to existing table
with table.update_schema() as update_schema:
    update_schema.add_column(
        field_id=1000,
        name="new_column",
        field_type="string",
        required=False
    )
```

## Running Examples

### Prerequisites

Install PyIceberg with required dependencies:

```bash
pip install pyiceberg[pyarrow]
```

### Using Make Commands

PyIceberg provides convenient Make commands:

```bash
# Basic PyIceberg examples (no external infrastructure)
make notebook

# Spark integration examples (requires Docker infrastructure)
make notebook-infra
```

### Manual Setup

```bash
# Install Jupyter
pip install jupyter

# Start Jupyter Lab
jupyter lab notebooks/
```

## Best Practices

### Performance

- **Use appropriate file sizes**: Target 128MB-1GB for Iceberg data files
- **Leverage partitioning**: Design partition strategies based on query patterns
- **Use column pruning**: Only select needed columns
- **Filter early**: Apply filters as early as possible in your queries

### Data Quality

- **Validate schemas**: Ensure data types match expectations
- **Handle nulls**: Decide on null handling strategies
- **Test migrations**: Validate data integrity after migration
- **Monitor quality**: Set up data quality checks

### Production

- **Error handling**: Implement comprehensive error handling
- **Logging**: Use appropriate logging levels for troubleshooting
- **Testing**: Test examples in non-production environments first
- **Documentation**: Document your customizations and patterns

## Common Issues

### Import Errors

```bash
# Ensure all dependencies are installed
pip install pyiceberg[pyarrow,s3fs]
```

### Permission Errors

```bash
# Check catalog credentials in .pyiceberg.yaml
# Verify file system permissions for warehouse location
```

### Memory Issues

```bash
# Process data in batches for large files
# Use DuckDB for out-of-core processing
```

## Getting Help

- **Documentation**: Check the [API documentation](api.md)
- **Community**: Join the [Apache Iceberg community](https://iceberg.apache.org/community/)
- **Issues**: Report bugs on [GitHub Issues](https://github.com/apache/iceberg-python/issues)

## Contributing Examples

We welcome contributions of additional practical examples! When contributing:

1. **Follow the pattern**: Use existing code examples as templates
2. **Include error handling**: Add appropriate error handling
3. **Add documentation**: Explain the use case and when to use it
4. **Test thoroughly**: Ensure examples work correctly
5. **Document dependencies**: List all required packages

See the [contributing guide](contributing.md) for more details.
