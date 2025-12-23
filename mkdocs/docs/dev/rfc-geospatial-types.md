# RFC: Iceberg v3 Geospatial Primitive Types

## Motivation

Apache Iceberg v3 introduces native geospatial types (`geometry` and `geography`) to support spatial data workloads. These types enable:

1. **Interoperability**: Consistent spatial data representation across Iceberg implementations
2. **Query optimization**: Future support for spatial predicate pushdown
3. **Standards compliance**: Alignment with OGC and ISO spatial data standards

This RFC describes the design and implementation of these types in PyIceberg.

## Scope

**In scope:**
- `geometry(C)` and `geography(C, A)` primitive type definitions
- Type parsing and serialization (round-trip support)
- Avro mapping (WKB bytes)
- PyArrow/Parquet conversion (with version-aware fallback)
- Format version enforcement (v3 required)

**Out of scope (future work):**
- Spatial predicate pushdown (e.g., ST_Contains, ST_Intersects)
- WKB/WKT conversion (requires external dependencies)
- Geometry/geography bounds metrics
- Spatial indexing

## Non-Goals

- Adding heavy dependencies like Shapely, GEOS, or GeoPandas
- Implementing spatial operations or computations
- Supporting format versions < 3

## Design

### Type Parameters

**GeometryType:**
- `crs` (string): Coordinate Reference System, defaults to `"OGC:CRS84"`

**GeographyType:**
- `crs` (string): Coordinate Reference System, defaults to `"OGC:CRS84"`
- `algorithm` (string): Geographic algorithm, defaults to `"spherical"`

### Type String Format

```python
# Default parameters
"geometry"
"geography"

# With custom CRS
"geometry('EPSG:4326')"
"geography('EPSG:4326')"

# With custom CRS and algorithm
"geography('EPSG:4326', 'planar')"
```

### Runtime Representation

Values are stored as WKB (Well-Known Binary) bytes at runtime. This matches the Avro and Parquet physical representation per the Iceberg spec.

### JSON Single-Value Serialization

Per the Iceberg spec, geometry/geography values should be serialized as WKT (Well-Known Text) strings in JSON. However, since we represent values as WKB bytes at runtime, conversion between WKB and WKT would require external dependencies.

**Current behavior:** `NotImplementedError` is raised for JSON serialization/deserialization until a conversion strategy is established.

### Avro Mapping

Both geometry and geography types map to Avro `bytes` type, consistent with `BinaryType` handling.

### PyArrow/Parquet Mapping

**With geoarrow-pyarrow installed:**
- Geometry types convert to GeoArrow WKB extension type with CRS metadata
- Geography types convert to GeoArrow WKB extension type with CRS and edge type metadata
- Uses `geoarrow.pyarrow.wkb().with_crs()` and `.with_edge_type()` for full GeoArrow compatibility

**Without geoarrow-pyarrow:**
- Geometry and geography types fall back to `pa.large_binary()`
- This provides WKB storage without GEO logical type metadata

## Compatibility

### Format Version

Geometry and geography types require Iceberg format version 3. Attempting to use them with format version 1 or 2 will raise a validation error via `Schema.check_format_version_compatibility()`.

### geoarrow-pyarrow

- **Optional dependency**: Install with `pip install pyiceberg[geoarrow]`
- **Without geoarrow**: Geometry/geography stored as binary columns (WKB)
- **With geoarrow**: Full GeoArrow extension type support with CRS/edge metadata

### Breaking Changes

None. These are new types that do not affect existing functionality.

## Dependency/Versioning

**Required:**
- PyIceberg core (no new dependencies)

**Optional for full functionality:**
- PyArrow 21.0.0+ for native Parquet GEO logical types

## Testing Strategy

1. **Unit tests** (`test_types.py`):
   - Type creation with default/custom parameters
   - `__str__` and `__repr__` methods
   - JSON serialization/deserialization round-trip
   - Equality, hashing, and pickling
   - `minimum_format_version()` enforcement

2. **Integration tests** (future):
   - End-to-end table creation with geometry/geography columns
   - Parquet file round-trip with PyArrow

## Known Limitations

1. **No WKB/WKT conversion**: JSON single-value serialization raises `NotImplementedError`
2. **No bounds metrics**: Cannot extract bounds from WKB without parsing
3. **No spatial predicates**: Query optimization for spatial filters not yet implemented
4. **PyArrow < 21.0.0**: Falls back to binary type without GEO metadata
5. **Reverse conversion from Parquet**: Binary columns cannot be distinguished from geometry/geography without Iceberg schema metadata

## File Locations

| Component | File |
|-----------|------|
| Type definitions | `pyiceberg/types.py` |
| Conversions | `pyiceberg/conversions.py` |
| Schema visitors | `pyiceberg/schema.py` |
| Avro conversion | `pyiceberg/utils/schema_conversion.py` |
| PyArrow conversion | `pyiceberg/io/pyarrow.py` |
| Unit tests | `tests/test_types.py` |

## References

- [Iceberg v3 Type Specification](https://iceberg.apache.org/spec/#schemas-and-data-types)
- [Arrow GEO Proposal](https://arrow.apache.org/docs/format/GeoArrow.html)
- [Arrow PR #45459](https://github.com/apache/arrow/pull/45459)
