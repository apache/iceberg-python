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

# Troubleshooting Guide

This guide helps you diagnose and resolve common issues when working with PyIceberg.

## Installation Issues

### Import Errors

**Problem**: `ModuleNotFoundError: No module named 'pyiceberg'`

**Solution**:
```bash
# Install PyIceberg
pip install pyiceberg

# Install with optional dependencies
pip install pyiceberg[pyarrow,s3fs,adlfs]
```

**Problem**: `ImportError: cannot import name 'X' from 'pyiceberg'`

**Solution**:
```bash
# Ensure you have the latest version
pip install --upgrade pyiceberg

# Check your installed version
python -c "import pyiceberg; print(pyiceberg.__version__)"
```

### Dependency Conflicts

**Problem**: Version conflicts with other packages

**Solution**:
```bash
# Use a virtual environment
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install with specific versions
pip install pyiceberg==0.6.0 pyarrow==14.0.0
```

## Catalog Connection Issues

### REST Catalog Connection

**Problem**: `Connection refused` or `Timeout` when connecting to REST catalog

**Solution**:
```yaml
# Check .pyiceberg.yaml configuration
catalog:
  my_catalog:
    uri: http://rest-catalog:8181/  # Verify URL and port
    credential: user:password  # Check credentials
```

```python
# Test connection with timeout
from pyiceberg.catalog import load_catalog
try:
    catalog = load_catalog("my_catalog")
    print("Connection successful")
except Exception as e:
    print(f"Connection failed: {e}")
```

### Hive Metastore Connection

**Problem**: `ThriftError` or connection issues with Hive Metastore

**Solution**:
```yaml
# Check Hive configuration
catalog:
  hive:
    uri: thrift://hive-metastore:9083  # Verify host and port
```

```bash
# Test Hive Metastore connectivity
telnet hive-metastore 9083
# or
nc -zv hive-metastore 9083
```

### AWS S3 Configuration

**Problem**: `Permission denied` or S3 authentication errors

**Solution**:
```yaml
# Check S3 configuration
catalog:
  my_catalog:
    uri: http://rest-catalog:8181/
    warehouse: s3://my-bucket/warehouse
    s3.endpoint: https://s3.amazonaws.com
    s3.access-key-id: YOUR_ACCESS_KEY
    s3.secret-access-key: YOUR_SECRET_KEY
```

```python
# Test S3 connectivity
import boto3
s3 = boto3.client('s3')
try:
    s3.list_buckets()
    print("S3 connection successful")
except Exception as e:
    print(f"S3 connection failed: {e}")
```

## Table Operations Issues

### Table Creation Errors

**Problem**: `TableAlreadyExistsError` when creating a table

**Solution**:
```python
# Check if table exists first
from pyiceberg.exceptions import TableAlreadyExistsError

try:
    table = catalog.create_table("my_table", schema=schema)
except TableAlreadyExistsError:
    # Load existing table instead
    table = catalog.load_table("my_table")
```

**Problem**: `NoSuchNamespaceError` when creating a table

**Solution**:
```python
# Create namespace first
catalog.create_namespace("my_namespace")

# Then create table
table = catalog.create_table("my_namespace.my_table", schema=schema)
```

### Schema Evolution Errors

**Problem**: `Schema evolution failed` when modifying schema

**Solution**:
```python
# Use proper schema evolution API
with table.update_schema() as update_schema:
    # Add new column with proper field_id
    update_schema.add_column(
        field_id=1000,
        name="new_column",
        field_type="string",
        required=False
    )
```

### Data Write Errors

**Problem**: `TypeError` when writing data with incompatible schema

**Solution**:
```python
# Ensure schema compatibility
from pyiceberg.schema import Schema

# Check table schema
table_schema = table.schema()
print(f"Table schema: {table_schema}")

# Ensure data schema matches
if data_schema != table_schema:
    # Convert data schema to match table schema
    converted_data = data.cast(table_schema)
    table.append(converted_data)
```

## Performance Issues

### Slow Query Performance

**Problem**: Queries are slower than expected

**Solution**:
```python
# Enable debug logging to identify bottlenecks
import logging
logging.basicConfig(level=logging.DEBUG)

# Check table statistics
print(f"Table statistics: {table.inspect().statistics}")

# Consider partitioning
from pyiceberg.partitioning import PartitionSpec, PartitionField
partition_spec = PartitionSpec(
    PartitionField(source_id=1, field_id=1000, transform="day", name="date_day")
)
```

### High Memory Usage

**Problem**: Out of memory errors when processing large datasets

**Solution**:
```python
# Process data in batches
batch_size = 10000
for i in range(0, len(data), batch_size):
    batch = data.slice(i, batch_size)
    table.append(batch)

# Use DuckDB for out-of-core processing
import duckdb
con = duckdb.connect()
result = con.execute("SELECT * FROM table").fetchdf()
```

### Slow File I/O

**Problem**: Slow read/write operations

**Solution**:
```python
# Use appropriate file I/O implementation
from pyiceberg.io import PyArrowFileIO

# Configure for better performance
catalog = load_catalog(
    "my_catalog",
    **{"py-io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO"}
)
```

## Data Quality Issues

### Missing or Null Values

**Problem**: Unexpected null values in data

**Solution**:
```python
# Check for null values before writing
import pyarrow.compute as pc

null_counts = {}
for field_name in data.schema.names:
    null_mask = pc.is_null(data[field_name])
    null_count = pc.sum(null_mask).as_py()
    null_counts[field_name] = null_count

print(f"Null counts: {null_counts}")

# Handle nulls explicitly
data = data.fillna({"column_name": "default_value"})
```

### Data Type Mismatches

**Problem**: Data type conversion errors

**Solution**:
```python
# Explicit type conversion
converted_data = data.cast(pa.schema([
    pa.field("id", pa.int64()),
    pa.field("value", pa.float64()),
    pa.field("name", pa.string())
]))
```

### Duplicate Data

**Problem**: Duplicate rows in table

**Solution**:
```python
# Remove duplicates using DuckDB
import duckdb
con = duckdb.connect()

deduped = con.execute("""
    SELECT DISTINCT * FROM table
""").fetchdf()

# Write deduplicated data back
table.append(pa.Table.from_pandas(deduped))
```

## Time Travel Issues

### Snapshot Not Found

**Problem**: `NoSuchSnapshotError` when querying historical data

**Solution**:
```python
# List available snapshots
for snapshot in table.history():
    print(f"Snapshot ID: {snapshot.snapshot_id}")
    print(f"Timestamp: {snapshot.timestamp_ms}")

# Use valid snapshot ID
historical_data = table.scan(snapshot_id=valid_snapshot_id).to_arrow()
```

### Rollback Failures

**Problem**: Unable to rollback to previous snapshot

**Solution**:
```python
# Check if snapshot exists
snapshot_ids = [s.snapshot_id for s in table.history()]
if target_snapshot_id in snapshot_ids:
    # Rollback using table operations
    # Note: Actual rollback implementation depends on your use case
    print("Snapshot exists, rollback possible")
else:
    print("Snapshot not found")
```

## Integration Issues

### DuckDB Integration

**Problem**: DuckDB cannot read Iceberg files

**Solution**:
```python
# Ensure DuckDB can access the data files
import duckdb
con = duckdb.connect()

# Test file access
test_query = """
    SELECT * FROM read_parquet('path/to/iceberg/data/**/*.parquet')
    LIMIT 10
"""
result = con.execute(test_query).fetchdf()
print(result)
```

### Spark Integration

**Problem**: Spark cannot read Iceberg tables

**Solution**:
```scala
// Configure Spark for Iceberg
spark.conf.set("spark.sql.catalog.my_catalog", "org.apache.iceberg.spark.SparkCatalog")
spark.conf.set("spark.sql.catalog.my_catalog.type", "rest")
spark.conf.set("spark.sql.catalog.my_catalog.uri", "http://rest-catalog:8181")

// Test table access
spark.table("my_catalog.database.table").show()
```

### Pandas Integration

**Problem**: Conversion between Iceberg and pandas fails

**Solution**:
```python
# Convert Iceberg data to pandas
import pandas as pd

# Get data as PyArrow
arrow_data = table.scan().to_arrow()

# Convert to pandas
pandas_df = arrow_data.to_pandas()

# Handle potential conversion issues
pandas_df = pandas_df.fillna(0)  # Handle nulls
pandas_df['date_column'] = pd.to_datetime(pandas_df['date_column'])  # Convert dates
```

## Logging and Debugging

### Enable Debug Logging

**Problem**: Need more information to diagnose issues

**Solution**:
```python
# Enable debug logging
import logging
logging.basicConfig(level=logging.DEBUG)

# Or use environment variable
import os
os.environ['PYICEBERG_LOG_LEVEL'] = 'DEBUG'

# Or use CLI option
# pyiceberg --log-level DEBUG describe my_table
```

### Check Configuration

**Problem**: Unsure about current configuration

**Solution**:
```python
# Check catalog configuration
from pyiceberg.catalog import load_catalog

catalog = load_catalog("my_catalog")
print(f"Catalog properties: {catalog.properties}")

# Check table configuration
table = catalog.load_table("my_table")
print(f"Table properties: {table.properties}")
print(f"Table location: {table.location()}")
```

### Validate Metadata

**Problem**: Suspect metadata corruption

**Solution**:
```python
# Validate table metadata
table = catalog.load_table("my_table")

# Check current snapshot
current_snapshot = table.current_snapshot()
print(f"Current snapshot: {current_snapshot}")

# Check schema
print(f"Schema: {table.schema()}")

# Check partition spec
print(f"Partition spec: {table.spec()}")
```

## Common Error Messages

### `NoSuchTableError`

**Cause**: Table does not exist in the catalog

**Solution**:
```python
# List available tables
tables = catalog.list_tables("namespace")
print(f"Available tables: {tables}")

# Create table if it doesn't exist
if "my_table" not in tables:
    table = catalog.create_table("my_table", schema=schema)
```

### `NoSuchNamespaceError`

**Cause**: Namespace does not exist

**Solution**:
```python
# List available namespaces
namespaces = catalog.list_namespaces()
print(f"Available namespaces: {namespaces}")

# Create namespace if it doesn't exist
if "my_namespace" not in [ns[0] for ns in namespaces]:
    catalog.create_namespace("my_namespace")
```

### `CommitFailedException`

**Cause**: Concurrent modification conflict

**Solution**:
```python
# Implement retry logic
from pyiceberg.exceptions import CommitFailedException
import time

max_retries = 3
for attempt in range(max_retries):
    try:
        table.overwrite(data)
        break
    except CommitFailedException:
        if attempt < max_retries - 1:
            time.sleep(1)  # Wait before retry
        else:
            raise
```

## Getting Additional Help

### Check Documentation

- [API Documentation](api.md) - Comprehensive API reference
- [Configuration Guide](configuration.md) - Configuration options
- [Practical Examples](practical-examples.md) - Real-world examples

### Community Resources

- [Apache Iceberg Community](https://iceberg.apache.org/community/) - Mailing lists and Slack
- [GitHub Issues](https://github.com/apache/iceberg-python/issues) - Report bugs
- [Stack Overflow](https://stackoverflow.com/questions/tagged/apache-iceberg) - Q&A

### Debug Checklist

Before seeking help, check:

- [ ] PyIceberg version and dependencies
- [ ] Catalog configuration in `.pyiceberg.yaml`
- [ ] Network connectivity to catalog and storage
- [ ] File system permissions
- [ ] Available disk space
- [ ] Memory usage
- [ ] Error messages and stack traces
- [ ] Minimal reproducible example

## Prevention and Best Practices

### Regular Maintenance

```python
# Expire old snapshots
from pyiceberg.table import ExpireSnapshots

expire_snapshots = ExpireSnapshots(table)
expire_snapshots.expire older_than_ms=timestamp_ms
expire_snapshots.commit()
```

### Monitoring

```python
# Monitor table statistics
def monitor_table(table):
    snapshot = table.current_snapshot()
    print(f"Snapshot ID: {snapshot.snapshot_id}")
    print(f"Summary: {snapshot.summary}")
    print(f"Added files: {len(snapshot.added_files())}")
    print(f"Deleted files: {len(snapshot.deleted_files())}")
```

### Backup and Recovery

```python
# Backup table metadata
metadata_location = table.metadata_location
# Store this location for recovery

# Recover from metadata backup
catalog.register_table(
    identifier="recovered_table",
    metadata_location=metadata_location
)
```

This troubleshooting guide covers the most common issues. For specific problems not covered here, please refer to the community resources or file an issue on GitHub.