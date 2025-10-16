"""Universal Iceberg REST Catalog Server.

A catalog-agnostic REST API server that exposes any PyIceberg catalog
(DynamoDB, Glue, Hive, SQL, etc.) via the Iceberg REST Catalog specification.

This enables tools like Snowflake, Spark, and Trino to access any catalog backend
through a standard REST interface.
"""

import logging
import os
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, HTTPException, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
import uvicorn

from pyiceberg.catalog import Catalog, load_catalog
from pyiceberg.exceptions import (
    NamespaceAlreadyExistsError,
    NamespaceNotEmptyError,
    NoSuchNamespaceError,
    NoSuchTableError,
    TableAlreadyExistsError,
)
from pyiceberg.partitioning import PartitionSpec, UNPARTITIONED_PARTITION_SPEC
from pyiceberg.schema import Schema
from pyiceberg.table.sorting import SortOrder, UNSORTED_SORT_ORDER
from pyiceberg.typedef import EMPTY_DICT, Identifier, Properties

# ============================================================================
# Configuration
# ============================================================================

CATALOG_NAME = os.getenv("ICEBERG_CATALOG_NAME", "production")
SERVER_HOST = os.getenv("SERVER_HOST", "0.0.0.0")
SERVER_PORT = int(os.getenv("SERVER_PORT", "8000"))
LOG_LEVEL = os.getenv("LOG_LEVEL", "info").upper()

# Configure logging
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# ============================================================================
# Initialize Catalog (Catalog-Agnostic!)
# ============================================================================


def get_catalog() -> Catalog:
    """
    Load the catalog based on configuration.

    This is the KEY ABSTRACTION - it works with ANY catalog type:
    - DynamoDB
    - Glue
    - Hive
    - SQL
    - REST (can even proxy another REST catalog! A classic Russian nesting doll situation :)
    - Custom implementations

    Returns:
        Catalog: The configured catalog instance.
    """
    try:
        # Special handling for DynamoDB catalog with LocalStack
        # This works around credential issues by pre-creating the boto3 client
        import yaml
        import os
        
        # Load the PyIceberg configuration
        config_path = os.path.expanduser("~/.pyiceberg.yaml")
        if os.path.exists(config_path):
            with open(config_path, 'r') as f:
                config = yaml.safe_load(f)
                catalog_config = config.get('catalog', {}).get(CATALOG_NAME, {})
                
                # Check if this is a DynamoDB catalog with endpoint (LocalStack)
                if catalog_config.get('type') == 'dynamodb' and catalog_config.get('dynamodb.endpoint'):
                    import boto3
                    from pyiceberg.catalog.dynamodb import DynamoDbCatalog
                    
                    logger.info(f"Creating DynamoDB catalog with pre-configured boto3 client for LocalStack")
                    
                    # Create boto3 client with explicit credentials
                    session = boto3.Session(
                        region_name=catalog_config.get('dynamodb.region', 'us-east-1'),
                        aws_access_key_id=catalog_config.get('dynamodb.access-key-id', 'test'),
                        aws_secret_access_key=catalog_config.get('dynamodb.secret-access-key', 'test'),
                    )
                    dynamodb_client = session.client(
                        'dynamodb',
                        endpoint_url=catalog_config.get('dynamodb.endpoint')
                    )
                    
                    # Create catalog with pre-configured client
                    catalog = DynamoDbCatalog(CATALOG_NAME, client=dynamodb_client, **catalog_config)
                    logger.info(f"Loaded DynamoDB catalog: {CATALOG_NAME}")
                    return catalog
        
        # Default: use standard load_catalog for all other catalog types
        catalog = load_catalog(CATALOG_NAME)
        logger.info(f"Loaded catalog: {CATALOG_NAME} (type: {catalog.properties.get('type', 'unknown')})")
        return catalog
    except Exception as e:
        logger.error(f"Failed to load catalog '{CATALOG_NAME}': {e}")
        raise


# Global catalog instance
catalog: Catalog = get_catalog()

# ============================================================================
# FastAPI Application
# ============================================================================

app = FastAPI(
    title="Universal Iceberg REST Catalog",
    description="Catalog-agnostic REST API server for Apache Iceberg",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
)


# ============================================================================
# REST API Models (Iceberg REST Specification)
# ============================================================================


class NamespaceIdent(BaseModel):
    """Namespace identifier."""

    namespace: List[str]


class TableIdent(BaseModel):
    """Table identifier."""

    namespace: List[str]
    name: str


class CreateNamespaceRequest(BaseModel):
    """Request to create a namespace."""

    namespace: List[str]
    properties: Optional[Dict[str, str]] = Field(default_factory=dict)


class CreateNamespaceResponse(BaseModel):
    """Response from creating a namespace."""

    namespace: List[str]
    properties: Optional[Dict[str, str]] = Field(default_factory=dict)


class ListNamespacesResponse(BaseModel):
    """Response from listing namespaces."""

    namespaces: List[List[str]]


class LoadNamespaceResponse(BaseModel):
    """Response from loading a namespace."""

    namespace: List[str]
    properties: Dict[str, str]


class UpdateNamespacePropertiesRequest(BaseModel):
    """Request to update namespace properties."""

    removals: Optional[List[str]] = Field(default_factory=list)
    updates: Optional[Dict[str, str]] = Field(default_factory=dict)


class UpdateNamespacePropertiesResponse(BaseModel):
    """Response from updating namespace properties."""

    removed: List[str]
    updated: List[str]
    missing: List[str]


class ListTablesResponse(BaseModel):
    """Response from listing tables."""

    identifiers: List[TableIdent]


class ConfigResponse(BaseModel):
    """Catalog configuration response."""

    defaults: Dict[str, str] = Field(default_factory=dict)
    overrides: Dict[str, str] = Field(default_factory=dict)


# ============================================================================
# Helper Functions
# ============================================================================


def namespace_to_tuple(namespace: List[str]) -> tuple:
    """Convert namespace list to tuple for catalog API."""
    return tuple(namespace)


def tuple_to_namespace(namespace_tuple: tuple) -> List[str]:
    """Convert namespace tuple to list for REST API."""
    return list(namespace_tuple)


def identifier_to_tuple(namespace: List[str], table_name: str) -> tuple:
    """Convert namespace and table name to identifier tuple."""
    return tuple(namespace + [table_name])


# ============================================================================
# REST API Endpoints - Configuration
# ============================================================================


@app.get("/v1/config", response_model=ConfigResponse)
async def get_config():
    """
    Get catalog configuration.

    Returns default and override properties for clients.
    """
    return ConfigResponse(defaults=catalog.properties, overrides={})


# ============================================================================
# REST API Endpoints - Namespaces
# ============================================================================


@app.get("/v1/namespaces", response_model=ListNamespacesResponse)
async def list_namespaces(parent: Optional[str] = None):
    """
    List all top-level namespaces.

    Works with ANY catalog backend!
    """
    try:
        namespace_list = catalog.list_namespaces()
        return ListNamespacesResponse(namespaces=[tuple_to_namespace(ns) for ns in namespace_list])
    except Exception as e:
        logger.error(f"Failed to list namespaces: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))


@app.post("/v1/namespaces", response_model=CreateNamespaceResponse, status_code=status.HTTP_201_CREATED)
async def create_namespace(request: CreateNamespaceRequest):
    """
    Create a namespace.

    Translates REST request to catalog.create_namespace()
    """
    try:
        namespace_tuple = namespace_to_tuple(request.namespace)
        catalog.create_namespace(namespace=namespace_tuple, properties=request.properties or {})
        logger.info(f"Created namespace: {request.namespace}")
        return CreateNamespaceResponse(namespace=request.namespace, properties=request.properties or {})
    except NamespaceAlreadyExistsError as e:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(e))
    except Exception as e:
        logger.error(f"Failed to create namespace {request.namespace}: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))


@app.get("/v1/namespaces/{namespace}", response_model=LoadNamespaceResponse)
async def load_namespace(namespace: str):
    """Load namespace metadata and properties."""
    try:
        namespace_parts = namespace.split(".")
        namespace_tuple = tuple(namespace_parts)
        properties = catalog.load_namespace_properties(namespace_tuple)
        return LoadNamespaceResponse(namespace=namespace_parts, properties=properties)
    except NoSuchNamespaceError as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))
    except Exception as e:
        logger.error(f"Failed to load namespace {namespace}: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))


@app.delete("/v1/namespaces/{namespace}", status_code=status.HTTP_204_NO_CONTENT)
async def drop_namespace(namespace: str):
    """Drop a namespace. Must be empty."""
    try:
        namespace_parts = namespace.split(".")
        namespace_tuple = tuple(namespace_parts)
        catalog.drop_namespace(namespace_tuple)
        logger.info(f"Dropped namespace: {namespace}")
    except NoSuchNamespaceError as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))
    except NamespaceNotEmptyError as e:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(e))
    except Exception as e:
        logger.error(f"Failed to drop namespace {namespace}: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))


@app.post("/v1/namespaces/{namespace}/properties", response_model=UpdateNamespacePropertiesResponse)
async def update_namespace_properties(namespace: str, request: UpdateNamespacePropertiesRequest):
    """Update namespace properties."""
    try:
        namespace_parts = namespace.split(".")
        namespace_tuple = tuple(namespace_parts)

        summary = catalog.update_namespace_properties(
            namespace=namespace_tuple, removals=set(request.removals) if request.removals else None, updates=request.updates or {}
        )

        logger.info(f"Updated namespace properties for {namespace}")
        return UpdateNamespacePropertiesResponse(removed=summary.removed, updated=summary.updated, missing=summary.missing)
    except NoSuchNamespaceError as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))
    except Exception as e:
        logger.error(f"Failed to update namespace properties for {namespace}: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))


# ============================================================================
# REST API Endpoints - Tables
# ============================================================================


@app.get("/v1/namespaces/{namespace}/tables", response_model=ListTablesResponse)
async def list_tables(namespace: str):
    """
    List all tables in a namespace.

    Works with ANY catalog backend!
    """
    try:
        namespace_parts = namespace.split(".")
        namespace_tuple = tuple(namespace_parts)

        table_identifiers = catalog.list_tables(namespace_tuple)

        # Convert tuples to TableIdent objects
        identifiers = []
        for table_tuple in table_identifiers:
            # table_tuple is typically (namespace, table_name) or could be nested
            if isinstance(table_tuple, tuple):
                *ns_parts, table_name = table_tuple
                identifiers.append(TableIdent(namespace=ns_parts if ns_parts else namespace_parts, name=table_name))

        return ListTablesResponse(identifiers=identifiers)
    except NoSuchNamespaceError as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))
    except Exception as e:
        logger.error(f"Failed to list tables in namespace {namespace}: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))


@app.get("/v1/namespaces/{namespace}/tables/{table}")
async def load_table(namespace: str, table: str):
    """
    Load a table's metadata.

    Works with ANY catalog backend!
    """
    try:
        namespace_parts = namespace.split(".")
        identifier = identifier_to_tuple(namespace_parts, table)

        # Load table via catalog abstraction
        loaded_table = catalog.load_table(identifier)

        # Return table metadata in REST format
        return JSONResponse(
            content={
                "metadata-location": loaded_table.metadata_location,
                "metadata": loaded_table.metadata.model_dump(),
                "config": {},
            }
        )
    except NoSuchTableError as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))
    except Exception as e:
        logger.error(f"Failed to load table {namespace}.{table}: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))


@app.head("/v1/namespaces/{namespace}/tables/{table}")
async def table_exists(namespace: str, table: str):
    """Check if a table exists."""
    namespace_parts = namespace.split(".")
    identifier = identifier_to_tuple(namespace_parts, table)

    if catalog.table_exists(identifier):
        return JSONResponse(content={"exists": True}, status_code=status.HTTP_200_OK)
    else:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Table not found")


@app.delete("/v1/namespaces/{namespace}/tables/{table}", status_code=status.HTTP_204_NO_CONTENT)
async def drop_table(namespace: str, table: str, purge: bool = False):
    """Drop a table."""
    try:
        namespace_parts = namespace.split(".")
        identifier = identifier_to_tuple(namespace_parts, table)

        if purge:
            catalog.purge_table(identifier)
            logger.info(f"Purged table: {namespace}.{table}")
        else:
            catalog.drop_table(identifier)
            logger.info(f"Dropped table: {namespace}.{table}")
    except NoSuchTableError as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))
    except Exception as e:
        logger.error(f"Failed to drop table {namespace}.{table}: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))


# ============================================================================
# Health & Monitoring
# ============================================================================


@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {"status": "healthy", "catalog": CATALOG_NAME, "catalog_type": catalog.properties.get("type", "unknown")}


@app.get("/metrics")
async def metrics():
    """Metrics endpoint for monitoring."""
    return {"catalog_name": CATALOG_NAME, "catalog_type": catalog.properties.get("type", "unknown"), "version": "1.0.0"}


# ============================================================================
# Main Entry Point
# ============================================================================

if __name__ == "__main__":
    print("=" * 70)
    print("Universal Iceberg REST Catalog Server")
    print("=" * 70)
    print(f"Catalog: {CATALOG_NAME}")
    print(f"Type: {catalog.properties.get('type', 'unknown')}")
    print(f"Listening on {SERVER_HOST}:{SERVER_PORT}")
    print("=" * 70)
    print(f"API Documentation: http://{SERVER_HOST}:{SERVER_PORT}/docs")
    print("=" * 70)

    uvicorn.run(app, host=SERVER_HOST, port=SERVER_PORT, log_level=LOG_LEVEL.lower())
