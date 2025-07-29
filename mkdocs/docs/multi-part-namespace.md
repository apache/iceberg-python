# Multi-Part Namespace Support

Some catalog implementations support multi-part namespaces, which allows for hierarchical organization of tables. The following table summarizes the support for multi-part namespaces across different catalog implementations in Iceberg Python.

| Catalog Implementation | Multi-Part Namespace Support | Notes |
|------------------------|------------------------------|-------|
| REST Catalog | ✅ Yes | Fully supports multi-part namespace as defined by the REST catalog specification. |
| Hive Catalog | ❌ No | Spark does not support multi-part namespace. |
| DynamoDB Catalog | ✅ Yes | Namespace is represented as a composite key in DynamoDB. |
| Glue Catalog | ❌ No | Uses AWS Glue databases which don't support multi-part namespace. |
| File Catalog | ✅ Yes | Namespace parts are represented as directory hierarchies in the file system. |
| In-Memory Catalog | ✅ Yes | Supports multi-part namespace for testing purposes. |

## Usage Example

```python
from pyiceberg.catalog import load_catalog

# Using a catalog with multi-part namespace support
catalog = load_catalog("my_catalog")

# Creating a table with a multi-part namespace
catalog.create_table("default.multi.table_name", schema, spec)

# Listing tables in a multi-part namespace
tables = catalog.list_tables("default.multi")
```

## Configuration

When using catalogs that support multi-part namespaces, make sure to use the appropriate delimiter (typically `.`) when referencing namespaces in your code.