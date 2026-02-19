# How To Review: Geospatial Compatibility, Metrics, and Expressions PR

## Goal

This PR is large because it spans expression APIs, Arrow/Parquet conversion, metrics generation, and documentation.
Recommended strategy: review in focused passes by concern, not file order.

## Recommended Review Order

1. **Core geospatial utility correctness**
   - `pyiceberg/utils/geospatial.py`
   - `tests/utils/test_geospatial.py`
   - Focus on envelope extraction, antimeridian behavior, and bound encoding formats.

1. **Metrics integration and write/import paths**
   - `pyiceberg/io/pyarrow.py`
   - `tests/io/test_pyarrow_stats.py`
   - Focus on:
     - geospatial bounds derived from row WKB values
     - skipping Parquet binary min/max for geospatial columns
     - partition inference not using geospatial envelope bounds

1. **GeoArrow compatibility and ambiguity boundary**
   - `pyiceberg/schema.py`
   - `pyiceberg/io/pyarrow.py`
   - `tests/io/test_pyarrow.py`
   - Confirm:
     - planar equivalence enabled only at Arrow/Parquet boundary
     - spherical mismatch still fails
     - fallback warning when GeoArrow dependency is absent

1. **Spatial expression surface area**
   - `pyiceberg/expressions/__init__.py`
   - `pyiceberg/expressions/visitors.py`
   - `tests/expressions/test_spatial_predicates.py`
   - Confirm:
     - bind-time type checks (geometry/geography only)
     - visitor plumbing is complete
     - conservative evaluator behavior is explicit and documented

1. **User-facing docs**
   - `mkdocs/docs/geospatial.md`
   - Check limitations and behavior notes match implementation.

## High-Risk Areas To Inspect Closely

1. **Boundary scope leakage**
   - Ensure planar-equivalence relaxation is not enabled globally.

2. **Envelope semantics**
   - Geography antimeridian cases (`xmin > xmax`) are expected and intentional.

3. **Metrics correctness**
   - Geospatial bounds are serialized envelopes, not raw value min/max.

4. **Conservative evaluator behavior**
   - Spatial predicates should not accidentally become strict in metrics/manifest evaluators.

## Quick Validation Commands

```bash
uv run --extra hive --extra bigquery python -m pytest tests/utils/test_geospatial.py -q
uv run --extra hive --extra bigquery python -m pytest tests/io/test_pyarrow_stats.py -k "geospatial or planar_geography_schema or partition_inference_skips_geospatial_bounds" -q
uv run --extra hive --extra bigquery python -m pytest tests/io/test_pyarrow.py -k "geoarrow or planar_geography_geometry_equivalence or spherical_geography_geometry_equivalence or logs_warning_once" -q
uv run --extra hive --extra bigquery python -m pytest tests/expressions/test_spatial_predicates.py tests/expressions/test_visitors.py -k "spatial or translate_column_names" -q
```

## Review Outcome Checklist

1. Geometry/geography bounds are present and correctly encoded for write/import paths.
2. `geometry` vs `geography(planar)` is only equivalent at Arrow/Parquet compatibility boundary with CRS equality.
3. `geography(spherical)` remains incompatible with `geometry`.
4. Spatial predicates are correctly modeled/bound; execution and pushdown remain intentionally unimplemented.
5. Missing GeoArrow dependency degrades gracefully with explicit warning.
6. Docs match implemented behavior and limitations.
