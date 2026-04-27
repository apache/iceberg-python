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

This guide provides practical, real-world examples for common PyIceberg use cases. Each example is available as a Jupyter notebook that you can run and modify for your specific needs.

## Available Examples

### 1. CSV to Iceberg Migration
**Notebook**: `csv_migration_example.ipynb`

Migrate CSV data to Iceberg with various strategies:

- **Simple Migration**: Direct CSV to Iceberg conversion
- **Schema Enhancement**: Add computed columns during migration
- **Partitioned Migration**: Organize data for better performance
- **Data Quality**: Validate and clean data during migration
- **Best Practices**: Production migration considerations

**When to use**: Transitioning from CSV to modern table formats, data lakehouse migration

**Run the example**:
```bash
make notebook
# Open csv_migration_example.ipynb in Jupyter
```

### 2. Time Travel Queries
**Notebook**: `time_travel_example.ipynb`

Explore Iceberg's time travel capabilities:

- **Snapshots**: Understand Iceberg's snapshot mechanism
- **Historical Queries**: Query data as it existed at specific times
- **Rollback**: Revert to previous table states
- **Audit Trail**: Track complete history of table changes
- **Real-world Use Cases**: Debugging, compliance, ML, data recovery

**When to use**: Data debugging, compliance requirements, analytics, disaster recovery

**Run the example**:
```bash
make notebook
# Open time_travel_example.ipynb in Jupyter
```

## Running the Examples

### Prerequisites

Install PyIceberg with required dependencies:

```bash
pip install pyiceberg[pyarrow]
```

### Using Make Commands

PyIceberg provides convenient Make commands for running notebooks:

```bash
# Basic PyIceberg examples (no external infrastructure)
make notebook

# Spark integration examples (requires Docker infrastructure)
make notebook-infra
```

### Manual Setup

If you prefer manual setup:

```bash
# Install Jupyter
pip install jupyter

# Start Jupyter Lab
jupyter lab notebooks/
```

## Example Patterns

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

## Troubleshooting

### Common Issues

**Import Errors**:
```bash
# Ensure all dependencies are installed
pip install pyiceberg[pyarrow,s3fs]
```

**Permission Errors**:
```bash
# Check catalog credentials in .pyiceberg.yaml
# Verify file system permissions for warehouse location
```

**Memory Issues**:
```bash
# Process data in batches for large files
```

### Getting Help

- **Documentation**: Check the [main API documentation](api.md)
- **Community**: Join the [Apache Iceberg community](https://iceberg.apache.org/community/)
- **Issues**: Report bugs on [GitHub Issues](https://github.com/apache/iceberg-python/issues)

## Contributing Examples

We welcome contributions of additional practical examples! When contributing:

1. **Follow the pattern**: Use the existing notebook structure
2. **Include cleanup**: Clean up temporary resources
3. **Add documentation**: Explain the use case and when to use it
4. **Test thoroughly**: Ensure examples run successfully
5. **Document dependencies**: List all required packages

See the [contributing guide](contributing.md) for more details.

## Additional Resources

- **API Documentation**: Comprehensive API reference
- **Configuration Guide**: Catalog and table configuration options
- **Expression DSL**: Query and filter expressions
- **Community**: Connect with other users and contributors