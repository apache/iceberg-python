# DynamoDB Catalog - LocalStack Integration Guide

## Overview

The DynamoDB catalog now supports connecting to LocalStack for local development and testing. This allows you to test DynamoDB operations without connecting to AWS.

## Prerequisites

1. **Docker** installed on your system
2. **LocalStack** container running

## Starting LocalStack

```bash
# Start LocalStack with DynamoDB and S3 services
docker run -d \
  --name localstack \
  -p 4566:4566 \
  -e SERVICES=dynamodb,s3 \
  localstack/localstack:latest

# Verify LocalStack is running
curl http://localhost:4566/_localstack/health
```

## Using DynamoDB Catalog with LocalStack

### Basic Configuration

```python
from pyiceberg.catalog.dynamodb import DynamoDbCatalog

catalog = DynamoDbCatalog(
    "my_catalog",
    **{
        "warehouse": "s3://my-bucket",
        "dynamodb.endpoint": "http://localhost:4566",  # LocalStack endpoint
        "s3.endpoint": "http://localhost:4566",        # For S3 operations
        "dynamodb.region": "us-east-1",
        "dynamodb.access-key-id": "test",              # Any value works with LocalStack
        "dynamodb.secret-access-key": "test",          # Any value works with LocalStack
    }
)
```

### Configuration Properties

| Property | Description | Example |
|----------|-------------|---------|
| `dynamodb.endpoint` | DynamoDB endpoint URL | `http://localhost:4566` |
| `s3.endpoint` | S3 endpoint URL | `http://localhost:4566` |
| `dynamodb.region` | AWS region | `us-east-1` |
| `dynamodb.access-key-id` | AWS access key (any value for LocalStack) | `test` |
| `dynamodb.secret-access-key` | AWS secret key (any value for LocalStack) | `test` |
| `warehouse` | S3 warehouse location | `s3://my-bucket` |
| `table-name` | DynamoDB table name (optional) | `my_iceberg_catalog` |

### Enhancement Features with LocalStack

All three enhancement features work seamlessly with LocalStack:

#### 1. Callback Hooks

```python
def audit_callback(ctx):
    print(f"Event: {ctx.event.value}, Table: {ctx.identifier}")

catalog.register_hook(CatalogEvent.PRE_CREATE_TABLE, audit_callback)
catalog.register_hook(CatalogEvent.POST_CREATE_TABLE, audit_callback)
```

#### 2. Metadata Caching

```python
catalog = DynamoDbCatalog(
    "my_catalog",
    **{
        "dynamodb.endpoint": "http://localhost:4566",
        "dynamodb.cache.enabled": "true",           # Enable cache (default: true)
        "dynamodb.cache.ttl-seconds": "600",        # Cache TTL (default: 300)
        # ... other properties
    }
)
```

#### 3. Retry Strategy

```python
catalog = DynamoDbCatalog(
    "my_catalog",
    **{
        "dynamodb.endpoint": "http://localhost:4566",
        "dynamodb.max-retries": "5",                # Max retry attempts (default: 5)
        "dynamodb.retry-multiplier": "1.5",         # Backoff multiplier (default: 1.5)
        "dynamodb.retry-min-wait-ms": "100",        # Min wait time (default: 100)
        "dynamodb.retry-max-wait-ms": "10000",      # Max wait time (default: 10000)
        # ... other properties
    }
)
```

## Running Tests

### Unit Tests (with moto)

```bash
# Run all DynamoDB tests with moto mocking
AWS_CONFIG_FILE=/dev/null pytest tests/catalog/test_dynamodb.py -v
```

**Note**: Set `AWS_CONFIG_FILE=/dev/null` to prevent boto3 from using your AWS config file that might have LocalStack endpoint configured.

### Integration Tests (with LocalStack)

```bash
# Run LocalStack integration tests
pytest tests/catalog/test_dynamodb_localstack.py -v
```

The integration tests will automatically skip if LocalStack is not running.

## Example Operations

### Create Namespace and Table

```python
from pyiceberg.catalog.dynamodb import DynamoDbCatalog
from pyiceberg.schema import Schema
from pyiceberg.types import IntegerType, StringType, NestedField

# Create catalog
catalog = DynamoDbCatalog(
    "my_catalog",
    **{
        "warehouse": "s3://test-bucket",
        "dynamodb.endpoint": "http://localhost:4566",
        "s3.endpoint": "http://localhost:4566",
        "dynamodb.region": "us-east-1",
        "dynamodb.access-key-id": "test",
        "dynamodb.secret-access-key": "test",
    }
)

# Create namespace
catalog.create_namespace("my_database", properties={"owner": "data-team"})

# Define schema
schema = Schema(
    NestedField(field_id=1, name="id", field_type=IntegerType(), required=True),
    NestedField(field_id=2, name="name", field_type=StringType(), required=False),
)

# Create table
table = catalog.create_table(
    identifier=("my_database", "my_table"),
    schema=schema
)

print(f"Table created: {table.name()}")
```

### List Tables and Namespaces

```python
# List all namespaces
namespaces = catalog.list_namespaces()
print(f"Namespaces: {namespaces}")

# List tables in a namespace
tables = catalog.list_tables("my_database")
print(f"Tables: {tables}")
```

### Load and Drop Table

```python
# Load table
table = catalog.load_table(("my_database", "my_table"))
print(f"Loaded table: {table.name()}")

# Drop table
catalog.drop_table(("my_database", "my_table"))
print("Table dropped")
```

## Troubleshooting

### LocalStack Not Running

**Error**: `ConnectionRefusedError: [Errno 61] Connection refused`

**Solution**: Start LocalStack:
```bash
docker run -d -p 4566:4566 -e SERVICES=dynamodb,s3 localstack/localstack:latest
```

### Port Already in Use

**Error**: `docker: Error response from daemon: driver failed programming external connectivity`

**Solution**: Stop existing LocalStack container:
```bash
docker stop localstack
docker rm localstack
# Then restart
docker run -d -p 4566:4566 -e SERVICES=dynamodb,s3 localstack/localstack:latest
```

### AWS Config Conflict

**Error**: Tests connect to LocalStack instead of moto mocks

**Solution**: Disable AWS config when running moto tests:
```bash
AWS_CONFIG_FILE=/dev/null pytest tests/catalog/test_dynamodb.py -v
```

## Cleanup

```bash
# Stop and remove LocalStack container
docker stop localstack
docker rm localstack
```

## Performance Notes

- LocalStack operations are slower than moto mocks (~2-3x)
- Moto tests: ~3 minutes for 55 tests
- LocalStack tests: ~1.5 minutes for 13 tests
- Use moto for fast unit testing
- Use LocalStack for integration testing with real AWS SDK behavior

## Differences from AWS DynamoDB

LocalStack provides a good approximation of AWS DynamoDB, but:
- Some advanced features may not be fully supported
- Performance characteristics differ from production
- Error handling might not match AWS exactly
- For production validation, test against real AWS DynamoDB in a dev account

## Resources

- [LocalStack Documentation](https://docs.localstack.cloud/)
- [DynamoDB Catalog Implementation](pyiceberg/catalog/dynamodb.py)
- [LocalStack Tests](tests/catalog/test_dynamodb_localstack.py)
