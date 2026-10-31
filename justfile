# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# Every recipe delegates to noxfile.py, which holds the checks and the orchestration.
# Run `just --list` for the full set.

# Show available recipes
default:
    @just --list

# ----------------------------------------------------------------- setup

# Install uv, sync all extras and build the Cython extensions
install *ARGS: install-uv
    uv run nox -s install {{ARGS}}

# Install uv when it is missing
install-uv:
    command -v uv > /dev/null || curl -LsSf https://astral.sh/uv/install.sh | sh

# ----------------------------------------------------------------- quality

# Run every CI gate without modifying files
check:
    uv run nox -- --check

# Run every CI gate, fixing what can be fixed
fix:
    uv run nox

# Format Python and notebooks
format:
    uv run nox -s format

# Format noxfile.py directly, so it works before nox can load it
format-noxfile:
    uv run ruff format noxfile.py

# Lint Python and notebooks, fixing what can be fixed
lint:
    uv run nox -s lint

# Check whitespace, final newlines, YAML and Python syntax
hygiene:
    uv run nox -s hygiene

# Type-check with mypy
typecheck-mypy:
    uv run nox -s typecheck-mypy

# Type-check with ty (experimental)
typecheck-ty:
    uv run nox -s typecheck-ty

# Verify uv.lock is in sync with pyproject.toml
lock-check:
    uv lock --check

# Check license headers
check-license:
    ./dev/check-license

# ----------------------------------------------------------------- testing

# Run unit tests on every supported Python version, e.g. `just test -k schema`
test *ARGS:
    uv run nox -s tests -j auto -- {{ARGS}}

# Run integration tests against Docker services
test-integration:
    uv run nox -s integration

# Rebuild the integration Docker services from scratch
test-integration-rebuild:
    uv run nox -s integration-rebuild

# Run tests marked with @pytest.mark.s3
test-s3:
    uv run nox -s s3

# Run tests marked with @pytest.mark.adls
test-adls:
    uv run nox -s adls

# Run tests marked with @pytest.mark.gcs
test-gcs:
    uv run nox -s gcs

# Combine coverage data and report
coverage-report:
    uv run nox -s coverage

# Run the full suite with coverage and report
test-coverage:
    COVERAGE=1 uv run nox -s tests -j auto
    COVERAGE=1 uv run nox -s integration s3 adls gcs
    just coverage-report

# Run notebook tests via papermill
test-notebook:
    uv run nox -s test-notebook

# ----------------------------------------------------------------- docs

# Serve the docs with hot reload
docs-serve:
    uv run nox -s docs-serve

# Build the static documentation site
docs-build:
    uv run nox -s docs-build

# ----------------------------------------------------------------- experimentation

# Launch JupyterLab for experimentation
notebook:
    uv run nox -s notebook

# Launch JupyterLab with the integration infrastructure running
notebook-infra:
    uv run nox -s notebook-infra

# ----------------------------------------------------------------- maintenance

# Remove build artifacts and caches
clean:
    uv run nox -s clean
