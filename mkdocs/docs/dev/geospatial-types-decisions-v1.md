# Geospatial Types Implementation Decisions

This document records design decisions made during the implementation of Iceberg v3 geospatial primitive types (`geometry` and `geography`) in PyIceberg.

## Decision 1: Type Parameters Storage

**Decision**: Store CRS (coordinate reference system) and algorithm as string fields in the type classes, with defaults matching the Iceberg specification.

**Alternatives Considered**:

1. Store as tuple in `root` field (like DecimalType does with precision/scale)
2. Store as separate class attributes with ClassVar defaults

**Rationale**: Using explicit fields with defaults allows for proper serialization/deserialization with Pydantic while maintaining singleton behavior for default-configured types. The spec defines defaults as CRS=`"OGC:CRS84"` and algorithm=`"spherical"` for geography.

**Spec Citations**:

- "Primitive Types" section: `geometry(C)` and `geography(C, A)` type definitions
- Default CRS is `"OGC:CRS84"`, default algorithm is `"spherical"`

**Reviewer Concerns Anticipated**: Singleton pattern may not work correctly with parameterized types - addressed by inheriting from Singleton and implementing `__getnewargs__` properly.

---

## Decision 2: Format Version Enforcement

**Decision**: Enforce `minimum_format_version() = 3` for both GeometryType and GeographyType.

**Alternatives Considered**:

1. Allow in format version 2 with a warning
2. Allow without restriction

**Rationale**: Geospatial types are defined as Iceberg v3 features. Allowing them in earlier versions would break spec compliance and interoperability with other Iceberg implementations.

**Spec Citations**: These types are defined in the v3 specification.

**Reviewer Concerns Anticipated**: Users on v2 tables cannot use geospatial types - this is expected behavior per spec.

---

## Decision 3: WKB/WKT Boundary

**Decision**:

- Data files use WKB (Well-Known Binary) - stored as `bytes` at runtime
- JSON single-value serialization uses WKT (Well-Known Text) strings
- Currently, we do NOT implement WKB<->WKT conversion (requires external dependencies like Shapely/GEOS)

**Alternatives Considered**:

1. Add optional Shapely dependency for conversion
2. Implement basic WKB<->WKT conversion ourselves
3. Raise explicit errors when conversion is needed

**Rationale**: Adding heavy dependencies (Shapely/GEOS) contradicts the design constraint. Implementing our own converter would be complex and error-prone. We choose to:

- Support writing geometry/geography as WKB bytes (Avro/Parquet)
- Raise `NotImplementedError` for JSON single-value serialization until a strategy for WKT conversion is established

**Spec Citations**:

- "Avro" section: geometry/geography are bytes in WKB format
- "JSON Single-Value Serialization" section: geometry/geography as WKT strings

**Reviewer Concerns Anticipated**: Limited functionality initially - JSON value serialization will raise errors until WKB<->WKT conversion is implemented (likely via optional dependency in future).

---

## Decision 4: Avro Mapping

**Decision**: Map geometry and geography to Avro `"bytes"` type, consistent with BinaryType handling.

**Alternatives Considered**:

1. Use fixed-size bytes (not appropriate - geometries are variable size)
2. Use logical type annotation (Avro doesn't have standard geo logical types)

**Rationale**: Per spec, geometry/geography values are stored as WKB bytes. The simplest and most compatible approach is to use Avro's bytes type.

**Spec Citations**: "Avro" section specifies bytes representation.

**Reviewer Concerns Anticipated**: None - this follows the established pattern for binary data.

---

## Decision 5: PyArrow/Parquet Logical Types

**Decision**:

- When `geoarrow-pyarrow` is available, use GeoArrow WKB extension types with CRS/edge metadata
- Without `geoarrow-pyarrow`, fall back to binary columns
- Keep this optional to avoid forcing extra runtime dependencies

**Alternatives Considered**:

1. Require GeoArrow-related dependencies for all users (too restrictive)
2. Always use binary with metadata in Parquet (loses GEO logical type benefit)
3. Refuse to write entirely on old versions (too restrictive)

**Rationale**: Optional dependency keeps base installs lightweight while enabling richer interoperability when users opt in.

**Spec Citations**:

- "Parquet" section: physical binary with logical type GEOMETRY/GEOGRAPHY (WKB)

**Reviewer Concerns Anticipated**: Users may not realize they're losing metadata on older PyArrow - addressed with warning logging.

---

## Decision 6: Type String Format

**Decision**:

- `geometry` with default CRS serializes as `"geometry"`
- `geometry("EPSG:4326")` with non-default CRS serializes as `"geometry('EPSG:4326')"`
- `geography` with default CRS/algorithm serializes as `"geography"`
- `geography("EPSG:4326", "planar")` serializes as `"geography('EPSG:4326', 'planar')"`

**Alternatives Considered**:

1. Always include all parameters
2. Use different delimiters

**Rationale**: Matches existing patterns like `decimal(p, s)` and `fixed[n]`. Omitting defaults makes common cases cleaner.

**Spec Citations**: Type string representations in spec examples.

**Reviewer Concerns Anticipated**: Parsing complexity - addressed with regex patterns similar to DecimalType.

---

## Decision 7: No Spatial Predicate Pushdown

**Decision**: Spatial predicate APIs (`st-contains`, `st-intersects`, `st-within`, `st-overlaps`) are supported in expression modeling and binding, but row-level execution and pushdown remain unimplemented.

**Alternatives Considered**:

1. Implement basic bounding-box based pushdown
2. Full spatial predicate support

**Rationale**: This delivers stable expression interoperability first while avoiding incomplete execution semantics.

**Spec Citations**: N/A - this is a performance optimization, not a spec requirement.

**Reviewer Concerns Anticipated**: Users may expect spatial queries - documented as limitation.

---

## Decision 8: Geospatial Bounds Metrics

**Decision**: Implement geometry/geography bounds metrics by parsing WKB values and serializing Iceberg geospatial bound bytes.

**Alternatives Considered**:

1. Implement point bounds (xmin, ymin, zmin, mmin) / (xmax, ymax, zmax, mmax)
2. Return `None` for bounds

**Rationale**: Spec-compliant bounds are required for geospatial metrics interoperability and future predicate pruning. Implementation is dependency-free at runtime and handles antimeridian wrap for geography.

**Spec Citations**:

- "Data file metrics bounds" section

**Reviewer Concerns Anticipated**: WKB parsing complexity and malformed-input handling - addressed by conservative fallback (skip bounds for malformed values and log warnings).

---

## Decision 9: GeoArrow Planar Ambiguity Handling

**Decision**: At the Arrow/Parquet schema-compatibility boundary only, treat `geometry` as compatible with `geography(..., "planar")` when CRS strings match.

**Alternatives Considered**:

1. Always decode ambiguous `geoarrow.wkb` as geometry
2. Always decode ambiguous `geoarrow.wkb` as geography(planar)
3. Relax schema compatibility globally

**Rationale**: GeoArrow WKB metadata without explicit edge semantics does not distinguish `geometry` from `geography(planar)`. Boundary-only compatibility avoids false negatives during import/add-files flows while preserving strict type checks in core schema logic elsewhere.

**Spec Citations**: N/A (this is interoperability behavior for an ambiguous external encoding).

**Reviewer Concerns Anticipated**: Potentially hides geometry-vs-planar-geography mismatch at Arrow/Parquet boundary. Guardrail: only planar equivalence is allowed; spherical geography remains strict.

---

## Summary of Limitations

1. **No WKB<->WKT conversion**: JSON single-value serialization raises NotImplementedError
2. **No spatial predicate execution/pushdown**: API and binding exist, execution/pushdown are future enhancements
3. **GeoArrow metadata optional**: Without `geoarrow-pyarrow`, Parquet uses binary representation without GeoArrow extension metadata
4. **GeoArrow planar ambiguity**: Boundary compatibility treats `geometry` and `geography(planar)` as equivalent with matching CRS
