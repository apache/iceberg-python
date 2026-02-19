# PyIceberg Geospatial Features Implementation Plan (Acceptance-First)

## Summary

This plan targets the next geospatial increment after base `geometry`/`geography` type support:

1. Geospatial bounds metrics (spec-compliant bound serialization, including XY/Z/M)
2. Spatial predicate expression API + binding (`st-contains`, `st-intersects`, `st-within`, `st-overlaps`)
3. SQL + REST integration coverage for geospatial write/read and schema evolution

Constraints for upstream compatibility:

- No new runtime dependency
- Shapely is test-only and optional
- No spatial row-evaluation or pushdown in this increment

---

## Scope

### In Scope

- Add pure-Python geospatial utilities for WKB parsing and bounds serialization
- Compute geometry/geography bounds from row values during write
- Preserve antimeridian behavior for geography (`xmin > xmax` when needed)
- Add ST* predicate classes and visitor plumbing
- Add/extend unit and integration tests for the above
- Update geospatial documentation and design decision records

### Out of Scope

- WKB <-> WKT conversion in runtime conversions
- Spatial predicate row-level execution
- Spatial predicate manifest/metrics pushdown logic beyond conservative defaults

---

## Interfaces and API Updates

### Expression APIs

Add new expression types in `pyiceberg/expressions/__init__.py`:

- `STContains(term, geometry)`
- `STIntersects(term, geometry)`
- `STWithin(term, geometry)`
- `STOverlaps(term, geometry)`

with bound counterparts and dispatch support in `pyiceberg/expressions/visitors.py`.

Behavior:

- Binding validates target field is `GeometryType` or `GeographyType`
- Visitor support exists for all ST* types
- Row-level evaluation is intentionally not implemented (`NotImplementedError`)
- Manifest and metrics evaluators return conservative "might match" behavior for ST*

### Metrics Encoding

For geometry/geography bounds, use Iceberg geospatial bound serialization:

- XY -> 16 bytes (`x, y` little-endian doubles)
- XYZ -> 24 bytes (`x, y, z`)
- XYM -> 32 bytes (`x, y, NaN, m`)
- XYZM -> 32 bytes (`x, y, z, m`)

---

## Implementation Details

### 1. Geospatial Utility Module

New file: `pyiceberg/utils/geospatial.py`

Provides:

- `GeospatialBound`
- `GeometryEnvelope`
- `serialize_geospatial_bound(...)`
- `deserialize_geospatial_bound(...)`
- `extract_envelope_from_wkb(...)`
- `merge_envelopes(...)`

WKB support:

- Point / LineString / Polygon / MultiPoint / MultiLineString / MultiPolygon / GeometryCollection
- Endianness handling for nested geometries
- ISO Z/M type offsets and EWKB Z/M/SRID flags
- Empty or NaN coordinate payloads handled conservatively

### 2. PyArrow Write Metrics Integration

File: `pyiceberg/io/pyarrow.py`

Add:

- `GeospatialStatsAggregator`
- `geospatial_column_aggregates_from_arrow_table(...)`

Behavior:

- Skip Parquet binary min/max stats for geospatial fields
- Compute bounds by parsing actual WKB values from Arrow columns during write
- Merge geospatial aggregates into `DataFileStatistics` before serialization
- On malformed values, skip geospatial bounds for affected column with warning

### 3. Spatial Predicate Visitor Plumbing

Files:

- `pyiceberg/expressions/__init__.py`
- `pyiceberg/expressions/visitors.py`

Changes:

- Add ST* expression + bound expression classes
- Extend boolean expression model parsing for new `type` values
- Register bound predicate visitor dispatch for ST*
- Add conservative ST* handling in manifest/metrics evaluators

### 4. Testing

New/updated tests:

- `tests/utils/test_geospatial.py`
- `tests/io/test_pyarrow_stats.py`
- `tests/expressions/test_spatial_predicates.py`
- `tests/integration/test_geospatial_integration.py`

Coverage includes:

- Bounds serialization/deserialization (XY/XYZ/XYM/XYZM)
- WKB envelope extraction including geography antimeridian wrap
- Geospatial stats aggregation outputs bound-encoded lower/upper bounds
- ST* expression construction, binding, parsing, and unimplemented row-eval behavior
- SQL + REST integration round-trip and schema evolution

---

## Acceptance Criteria

- Geometry/geography columns emit spec-encoded bounds in data file metrics when written via PyIceberg
- Geography antimeridian examples produce wrapped x-bounds (`xmin > xmax`) where appropriate
- ST* predicates are bindable and serializable in expression trees
- Using ST* in row-evaluator path raises explicit "not implemented" errors
- Existing non-geospatial metrics behavior remains unchanged
- Integration tests pass for SQL + REST geospatial round-trip and schema evolution

---

## Risks and Mitigations

1. WKB parser complexity
   - Mitigation: strict parser with explicit unsupported-type errors and focused unit tests

2. Malformed payloads causing write failures
   - Mitigation: bounds are skipped per-column with warnings; write path continues

3. Future pushdown compatibility
   - Mitigation: conservative ST* evaluator behavior (`might match`) avoids false negatives

---

## Dependencies

- Runtime: unchanged
- Optional/test-only: Shapely can be used in tests but is not required by runtime implementation
