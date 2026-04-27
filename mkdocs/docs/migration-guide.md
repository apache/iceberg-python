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

# Migration Guide

This guide helps you migrate data from various formats and systems to Apache Iceberg using PyIceberg.

## Overview

Migrating to Iceberg provides numerous benefits:
- **Performance**: Columnar Parquet format with predicate pushdown
- **Reliability**: ACID transactions with snapshot isolation
- **Flexibility**: Schema evolution without breaking queries
- **Time Travel**: Query historical data at any point in time
- **Compatibility**: Works with multiple compute engines

## Migration Strategies

### 1. CSV Migration

CSV is one of the most common formats to migrate from. See the [CSV Migration Example](../notebooks/csv_migration_example.ipynb) for a detailed walkthrough.

#### Basic CSV Migration

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

#### Advanced CSV Migration

- **Schema Enhancement**: Add computed columns during migration
- **Type Conversion**: Ensure proper data types
- **Partitioning**: Organize data by partition keys
- **Data Validation**: Clean and validate data

**Best Practices**:
- Use PyArrow for efficient CSV reading
- Handle missing values explicitly
- Validate data ranges and types
- Consider partitioning for large datasets

### 2. Parquet Migration

Parquet to Iceberg migration is straightforward since Iceberg uses Parquet as its default file format.

#### Basic Parquet Migration

```python
import pyarrow.parquet as pq
from pyiceberg.catalog import load_catalog

# Read Parquet
parquet_data = pq.read_table('data.parquet')

# Create Iceberg table
catalog = load_catalog("my_catalog")
table = catalog.create_table("my_table", schema=parquet_data.schema)

# Migrate data
table.append(parquet_data)
```

#### Advantages of Parquet Migration

- **No conversion needed**: Iceberg uses Parquet natively
- **Schema preservation**: Maintains existing schema
- **Performance**: Leverages existing columnar format
- **Metadata**: Preserves existing metadata

### 3. JSON Migration

JSON data requires schema inference and conversion to Iceberg's schema.

#### Basic JSON Migration

```python
import pyarrow.json as pj
from pyiceberg.catalog import load_catalog

# Read JSON
json_data = pj.read_json('data.json')

# Create Iceberg table
catalog = load_catalog("my_catalog")
table = catalog.create_table("my_table", schema=json_data.schema)

# Migrate data
table.append(json_data)
```

#### Considerations for JSON Migration

- **Schema inference**: JSON may have inconsistent schemas
- **Nested structures**: Handle nested JSON objects
- **Data types**: Ensure proper type conversion
- **Performance**: JSON is slower than Parquet

### 4. Hive Table Migration

Migrate existing Hive tables to Iceberg while maintaining compatibility.

#### Hive to Iceberg Migration

```python
from pyiceberg.catalog import load_catalog

# Load Hive catalog
catalog = load_catalog("hive", uri="thrift://hive-metastore:9083")

# Register existing Hive table as Iceberg table
catalog.register_table(
    identifier="database.table_name",
    metadata_location="s3://warehouse/path/to/metadata.json"
)
```

#### Hive Migration Considerations

- **Schema compatibility**: Ensure Hive schema maps to Iceberg types
- **Partitioning**: Preserve or optimize partition strategy
- **Data location**: Keep data in existing location or migrate
- **Query compatibility**: Test existing queries against Iceberg table

### 5. Delta Lake Migration

Migrate Delta Lake tables to Iceberg for multi-engine compatibility.

#### Delta Lake to Iceberg Migration

```python
import delta.pandas as delta_pd
import pyarrow as pa
from pyiceberg.catalog import load_catalog

# Read Delta Lake table
delta_data = delta_pd.read_delta('delta_table_path').to_arrow()

# Create Iceberg table
catalog = load_catalog("my_catalog")
table = catalog.create_table("my_table", schema=delta_data.schema)

# Migrate data
table.append(delta_data)
```

#### Delta Lake Migration Considerations

- **Schema evolution**: Handle Delta Lake schema changes
- **Time travel**: Preserve Delta Lake time travel capabilities
- **Performance**: Compare performance after migration
- **ACID properties**: Both systems support ACID, but implementation differs

### 6. Database Migration

Migrate data from traditional databases to Iceberg.

#### Database to Iceberg Migration

```python
import pyarrow as pa
from pyiceberg.catalog import load_catalog
import some_database_connector

# Connect to database
conn = some_database_connector.connect('database_url')

# Read data
cursor = conn.cursor()
cursor.execute("SELECT * FROM table_name")
data = cursor.fetchall()
columns = [desc[0] for desc in cursor.description]

# Convert to PyArrow
arrow_data = pa.array(data)
schema = pa.schema([(col, pa.string()) for col in columns])  # Adjust types as needed
table_data = pa.Table.from_arrays(arrow_data, schema=schema)

# Create Iceberg table
catalog = load_catalog("my_catalog")
table = catalog.create_table("my_table", schema=table_data.schema)

# Migrate data
table.append(table_data)
```

#### Database Migration Considerations

- **Data types**: Map database types to Iceberg types
- **Primary keys**: Handle primary key constraints
- **Foreign keys**: Iceberg doesn't enforce foreign keys
- **Indexes**: Plan for query performance without traditional indexes

## Migration Best Practices

### Planning

1. **Assess current data**: Understand data volume, structure, and access patterns
2. **Define migration strategy**: Choose appropriate migration approach
3. **Plan downtime**: Schedule migration during low-usage periods
4. **Set up monitoring**: Monitor migration progress and data quality

### Data Quality

1. **Validate schemas**: Ensure data types map correctly
2. **Handle nulls**: Decide on null handling strategy
3. **Check constraints**: Validate data constraints after migration
4. **Test queries**: Verify query results match expectations

### Performance

1. **Batch size**: Process data in appropriate batch sizes
2. **Parallel processing**: Use parallel processing for large datasets
3. **File size optimization**: Target appropriate Iceberg file sizes
4. **Partitioning**: Design partition strategy based on query patterns

### Validation

1. **Row count validation**: Ensure all rows migrated
2. **Data sampling**: Compare sample data before and after
3. **Query validation**: Test representative queries
4. **Performance validation**: Compare query performance

## Common Migration Challenges

### Schema Mismatches

**Problem**: Source schema doesn't match Iceberg type system

**Solution**:
```python
# Explicit type conversion
converted_schema = pa.schema([
    pa.field("id", pa.int64()),  # Convert to int64
    pa.field("name", pa.string()),
    pa.field("value", pa.float64())  # Convert to float64
])
converted_data = original_data.cast(converted_schema)
```

### Large Dataset Migration

**Problem**: Dataset too large for memory

**Solution**:
```python
# Process in batches
batch_size = 100000
for i in range(0, len(data), batch_size):
    batch = data.slice(i, batch_size)
    table.append(batch)
```

### Data Type Conversion

**Problem**: Incompatible data types between systems

**Solution**:
```python
# Custom type conversion
def convert_type(value):
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            return float(value)
    return value
```

### Partitioning Strategy

**Problem**: Optimal partitioning unclear

**Solution**:
- Analyze query patterns
- Choose high-cardinality columns for partitioning
- Consider date/time-based partitioning for time-series data
- Test different partitioning strategies

## Post-Migration Steps

### Validation

1. **Data integrity**: Verify data accuracy
2. **Query testing**: Test all critical queries
3. **Performance testing**: Compare query performance
4. **User acceptance**: Get user sign-off

### Optimization

1. **File compaction**: Optimize file sizes
2. **Statistics**: Update table statistics
3. **Z-ordering**: Implement Z-ordering if beneficial
4. **Partitioning**: Refine partitioning based on usage

### Documentation

1. **Update documentation**: Document new table locations
2. **Update queries**: Modify queries to use Iceberg tables
3. **Train users**: Train users on Iceberg-specific features
4. **Monitor performance**: Set up ongoing performance monitoring

### Cleanup

1. **Archive old data**: Archive or remove source data
2. **Update permissions**: Update access permissions
3. **Clean up resources**: Remove temporary files and resources
4. **Update monitoring**: Update monitoring and alerting

## Tools and Resources

### PyIceberg Features

- **Schema evolution**: Modify schemas without breaking queries
- **Partitioning**: Flexible partitioning strategies
- **Time travel**: Query historical data
- **ACID transactions**: Reliable data operations

### External Tools

- **Spark**: Distributed processing with Iceberg
- **Trino**: SQL query engine with Iceberg support
- **Pandas**: Data analysis with Iceberg integration

### Example Notebooks

Example notebooks are available in the `notebooks/` directory of the repository:
- `csv_migration_example.ipynb` - CSV to Iceberg migration
- `time_travel_example.ipynb` - Time travel queries and snapshot management

## Getting Help

- **Documentation**: Check the [API documentation](api.md)
- **Community**: Join the [Apache Iceberg community](https://iceberg.apache.org/community/)
- **Issues**: Report bugs on [GitHub Issues](https://github.com/apache/iceberg-python/issues)
- **Examples**: Review the [practical examples](practical-examples.md)

## Conclusion

Migrating to Iceberg provides significant benefits for data management and analytics. By following this guide and leveraging PyIceberg's capabilities, you can successfully migrate your data while minimizing disruption and maximizing the benefits of Iceberg's advanced features.