# Geospatial Types

PyIceberg supports Iceberg v3 geospatial primitive types: `geometry` and `geography`.

## Overview

Iceberg v3 introduces native support for spatial data types:

- **`geometry(C)`**: Represents geometric shapes in a coordinate reference system (CRS)
- **`geography(C, A)`**: Represents geographic shapes with CRS and calculation algorithm

Both types store values as WKB (Well-Known Binary) bytes.

## Requirements

- Iceberg format version 3 or higher
- Optional: `geoarrow-pyarrow` for GeoArrow extension type metadata and interoperability. Without it, geometry and geography are written as binary in Parquet while the Iceberg schema still preserves the spatial type. Install with `pip install pyiceberg[geoarrow]`.

## Usage

### Declaring Columns

```python
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, GeometryType, GeographyType

# Schema with geometry and geography columns
schema = Schema(
    NestedField(1, "id", IntegerType(), required=True),
    NestedField(2, "location", GeometryType(), required=True),
    NestedField(3, "boundary", GeographyType(), required=False),
)
```

### Type Parameters

#### GeometryType

```python
# Default CRS (OGC:CRS84)
GeometryType()

# Custom CRS
GeometryType("EPSG:4326")
```

#### GeographyType

```python
# Default CRS (OGC:CRS84) and algorithm (spherical)
GeographyType()

# Custom CRS
GeographyType("EPSG:4326")

# Custom CRS and algorithm
GeographyType("EPSG:4326", "planar")
```

### String Type Syntax

Types can also be specified as strings in schema definitions:

```python
# Using string type names
NestedField(1, "point", "geometry", required=True)
NestedField(2, "region", "geography", required=True)

# With parameters
NestedField(3, "location", "geometry('EPSG:4326')", required=True)
NestedField(4, "boundary", "geography('EPSG:4326', 'planar')", required=True)
```

## Data Representation

Values are represented as WKB (Well-Known Binary) bytes at runtime:

```python
# Example: Point(0, 0) in WKB format
point_wkb = bytes.fromhex("0101000000000000000000000000000000000000")
```

## Current Limitations

1. **WKB/WKT Conversion**: Converting between WKB bytes and WKT strings requires external libraries (like Shapely). PyIceberg does not include this conversion to avoid heavy dependencies.

2. **Spatial Predicates Execution**: Spatial predicate APIs (`st-contains`, `st-intersects`, `st-within`, `st-overlaps`) are available in expression trees and binding. Row-level execution and metrics/pushdown evaluation are not implemented yet.

3. **Without geoarrow-pyarrow**: When the `geoarrow-pyarrow` package is not installed, geometry and geography columns are stored as binary without GeoArrow extension type metadata. The Iceberg schema preserves type information, but other tools reading the Parquet files directly may not recognize them as spatial types. Install with `pip install pyiceberg[geoarrow]` for full GeoArrow support.

4. **GeoArrow planar ambiguity**: In GeoArrow metadata, `geometry` and `geography(..., 'planar')` can be encoded identically (no explicit edge metadata). PyIceberg resolves this ambiguity at the Arrow/Parquet schema-compatibility boundary by treating them as compatible when CRS matches, while keeping core schema compatibility strict elsewhere.

## Format Version

Geometry and geography types require Iceberg format version 3:

```python
from pyiceberg.table import TableProperties

# Creating a v3 table
table = catalog.create_table(
    identifier="db.spatial_table",
    schema=schema,
    properties={
        TableProperties.FORMAT_VERSION: "3"
    }
)
```

Attempting to use these types with format version 1 or 2 will raise a validation error.
