<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# PyIceberg — Agent Instructions

Project conventions, architecture, and coding patterns synthesized from PyIceberg developers.

## Security Model

When assessing potential vulnerabilities or calibrating automated security
findings, use [`SECURITY-THREAT-MODEL.md`](SECURITY-THREAT-MODEL.md) as the
authoritative detailed description of this repository's security boundaries,
trust assumptions, and non-boundaries.

## Architecture

PyIceberg is a pure-Python library — it has no separate engine modules. The code
lives under `pyiceberg/`, organized by concern rather than by engine:

- **`schema.py`, `types.py`, `transforms.py`, `partitioning.py`, `conversions.py`**: The table spec core — schemas, types, partition specs, and value conversions. Must stay engine- and catalog-agnostic.
- **`table/`**: Table abstraction, metadata (`metadata.py`), snapshots, refs, sort orders, and the transaction/update machinery (`table/update/`). The commit path lives here.
- **`catalog/`**: Catalog implementations — `rest/`, `hive`, `glue`, `dynamodb`, `sql`, `bigquery_metastore`, `memory`, `noop`. New catalogs subclass the base `Catalog` in `catalog/__init__.py`. Catalog-specific assumptions must not leak into `table/` or the spec core.
- **`io/`**: The `FileIO` abstraction over storage. `pyarrow.py` and `fsspec.py` are the two backends. Never hard-code a storage SDK where `FileIO` exists.
- **`expressions/`**: The expression DSL and its visitors (predicate binding, projection, evaluation).
- **`avro/`, `manifest.py`**: Manifest and Avro read/write — performance-sensitive, partly accelerated by Cython.
- **`cli/`**: The `pyiceberg` command-line interface (Click + Rich).
- **`utils/`**: Shared helpers (`deprecated.py`, `concurrent.py`, `config.py`, `bin_packing.py`, `singleton.py`, etc.). Check here before writing new utility code.

## Coding Conventions

### Style & Typing

- Formatting and linting are enforced by `ruff` via `prek` (pre-commit): line length **130**, double-quoted strings, isort with `pyiceberg`/`tests` as first-party. Run `make lint` — ruff autofixes most issues.
- Full type annotations are required (`mypy` runs in strict mode: `disallow_untyped_defs`, `no_implicit_optional`, `warn_unused_ignores`). Avoid Any types.
- Docstrings follow the project's pydocstyle config; one-line summaries on public functions. No personal pronouns in comments.
- Use domain-specific exceptions from pyiceberg/exceptions.py for Iceberg-specific error conditions. For invalid arguments/values, ValueError is acceptable. Don't raise bare Exception.
- Comments should be succinct and follow the same style of comments found in the rest of the codebase.

### Dependencies

- Large/integration libraries must be **optional extras** in `pyproject.toml`, not core `dependencies`.

## Testing

- Bias towards adding tests to existing files, rather than creating new files.
- Use existing test fixtures when possible.
- We have a strong bias towards integration testing over mocks. Mocks should be avoided whenever possible and should only be used if similar, existing tests are using mocks.

## Commands

- **Install / set up dev env:** `make install` (installs `uv`, syncs all extras, builds Cython, installs pre-commit hooks)
- **Run unit tests:** `make test`
- **Run a subset:** `make test PYTEST_ARGS="-v -k <pattern>"`
- **Integration tests (Spark/Docker):** `make test-integration` (rebuild with `make test-integration-rebuild`)
- **Cloud storage suites:** `make test-s3` / `make test-adls` / `make test-gcs`
- **Lint & format:** `make lint`
- **Docs preview / build:** `make docs-serve` / `make docs-build`
- **Clean build artifacts:** `make clean`
- **Use a specific Python:** prefix with `PYTHON=3.12`, e.g. `PYTHON=3.12 make install`

## PR & Commit Conventions

- Ensure that there are no existing PRs for this feature before beginning development.
- One concern per PR. Keep unrelated formatting/import churn out of feature PRs.
- Keep the first version of a PR minimal; defer optimizations and edge cases to follow-ups.
- Commit messages explain the *what* and *why*, not line-by-line implementation. Be as succinct as possible.
- The Apache License header is required on every new source file (enforced by `./dev/check-license`).
- Run `make lint` and `make test` before pushing; CI runs both plus the lockfile check.

## Boundaries

- **Never** add a hard (non-optional) dependency without discussion — keep heavy/integration libraries as optional extras.
- **Never** edit `uv.lock` by hand — regenerate it with `uv lock`.
- **Never** reach for a storage SDK (`boto3`, `s3fs`, `gcsfs`, `adlfs`) outside its `FileIO` backend module.
- **Never** break the public API or remove a public method without a `@deprecated` cycle.
- **Never** leak catalog- or engine-specific assumptions into the spec core (`schema.py`, `types.py`, `table/metadata.py`).
- **Never** commit secrets, credentials, or cloud tokens (integration tests can delete catalog data — never point them at production).
- **Ask first** before adding any third-party dependency or promoting internal (`_`-prefixed) APIs to public.
