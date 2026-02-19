# PR Draft: Geospatial Compatibility, Metrics, and Spatial Expression Support

## Summary

This PR extends PyIceberg geospatial support in three areas:

1. Adds geospatial bounds metric computation from WKB values (geometry + geography).
2. Adds spatial predicate expression/binding support (`st-contains`, `st-intersects`, `st-within`, `st-overlaps`) with conservative evaluator behavior.
3. Improves Arrow/Parquet interoperability for GeoArrow WKB, including explicit handling of geometry vs planar-geography ambiguity at the schema-compatibility boundary.

This increment is compatibility-first and does **not** introduce new runtime dependencies.

## Why

Base `geometry`/`geography` types existed, but there were still practical gaps:

- Geospatial columns were not contributing spec-encoded bounds in data-file metrics.
- Spatial predicates were not modeled end-to-end in expression binding/visitor plumbing.
- GeoArrow metadata can be ambiguous for `geometry` vs `geography(..., "planar")`, causing false compatibility failures during import/add-files flows.

## What Changed

### 1) Geospatial bounds metrics from WKB

- Added pure-Python geospatial utilities in `pyiceberg/utils/geospatial.py`:
    - WKB envelope extraction
    - antimeridian-aware geography envelope merge
    - Iceberg geospatial bound serialization/deserialization
- Added `GeospatialStatsAggregator` and geospatial aggregate helpers in `pyiceberg/io/pyarrow.py`.
- Updated write/import paths to compute geospatial bounds from actual row values (not Parquet binary min/max stats):
    - `write_file(...)`
    - `parquet_file_to_data_file(...)`
- Prevented incorrect partition inference from geospatial envelope bounds.

### 2) Spatial predicate expression support

- Added expression types in `pyiceberg/expressions/__init__.py`:
    - `STContains`, `STIntersects`, `STWithin`, `STOverlaps`
    - bound counterparts and JSON parsing support
- Added visitor dispatch/plumbing in `pyiceberg/expressions/visitors.py`.
- Behavior intentionally conservative in this increment:
    - row-level expression evaluator raises `NotImplementedError`
    - manifest/metrics evaluators return conservative might-match defaults
    - translation paths preserve spatial predicates where possible

### 3) GeoArrow/Parquet compatibility improvements

- Added GeoArrow WKB decoding helper in `pyiceberg/io/pyarrow.py` to map extension metadata to Iceberg geospatial types.
- Added boundary-only compatibility option in `pyiceberg/schema.py`:
    - `_check_schema_compatible(..., allow_planar_geospatial_equivalence=False)`
- Enabled that option only in `_check_pyarrow_schema_compatible(...)` to allow:
    - `geometry` <-> `geography(..., "planar")` when CRS strings match
    - while still rejecting spherical geography mismatches
- Added one-time warning log when `geoarrow-pyarrow` is unavailable and code falls back to binary.

## Docs

- Updated user docs: `mkdocs/docs/geospatial.md`
- Added decisions record: `mkdocs/docs/dev/geospatial-types-decisions-v1.md`

## Test Coverage

Added/updated tests across:

- `tests/utils/test_geospatial.py`
- `tests/io/test_pyarrow_stats.py`
- `tests/io/test_pyarrow.py`
- `tests/expressions/test_spatial_predicates.py`
- `tests/integration/test_geospatial_integration.py`

Coverage includes:

- geospatial bound encoding/decoding (XY/XYZ/XYM/XYZM)
- geography antimeridian behavior
- geospatial metrics generation from write/import paths
- spatial predicate modeling/binding/translation behavior
- planar ambiguity compatibility guardrails
- warning behavior for missing `geoarrow-pyarrow`

## Compatibility and Scope Notes

- No user-facing API removals.
- New compatibility relaxation is intentionally scoped to Arrow/Parquet schema-compatibility boundary only.
- Core schema/type compatibility remains strict elsewhere.
- No spatial pushdown/row execution implementation in this PR.

## Follow-ups (Out of Scope Here)

- Spatial predicate execution semantics.
- Spatial predicate pushdown/pruning.
- Runtime WKB <-> WKT conversion strategy.
