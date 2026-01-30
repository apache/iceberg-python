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
# ========================
# Configuration Variables
# ========================

PYTHON ?=  # Override with e.g. PYTHON=3.11 to use specific Python version
PYTEST_ARGS ?= -v -x  # Override with e.g. PYTEST_ARGS="-vv --tb=short"
COVERAGE ?= 0      # Set COVERAGE=1 to enable coverage: make test COVERAGE=1
COVERAGE_FAIL_UNDER ?= 85  # Minimum coverage % to pass: make coverage-report COVERAGE_FAIL_UNDER=70
KEEP_COMPOSE ?= 0  # Set KEEP_COMPOSE=1 to keep containers after integration tests

# Set Python argument for uv commands if PYTHON is specified
ifneq ($(PYTHON),)
  PYTHON_ARG = --python $(PYTHON)
else
  PYTHON_ARG =
endif

ifeq ($(COVERAGE),1)
  TEST_RUNNER = uv run $(PYTHON_ARG) python -m coverage run --parallel-mode --source=pyiceberg -m
else
  TEST_RUNNER = uv run $(PYTHON_ARG) python -m
endif

ifeq ($(KEEP_COMPOSE),1)
  CLEANUP_COMMAND = echo "Keeping containers running for debugging (KEEP_COMPOSE=1)"
else
  CLEANUP_COMMAND = docker compose -f dev/docker-compose-integration.yml down -v --remove-orphans --timeout 0 2>/dev/null || true
endif

# ============
# Help Section
# ============

##@ General

help: ## Display this help message
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n"} /^[a-zA-Z_-]+:.*?##/ { printf "  \033[36m%-25s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

# ==================
# Installation Tasks
# ==================

##@ Setup

install-uv: ## Ensure uv is installed
	@if ! command -v uv > /dev/null 2>&1; then \
		echo "uv not found. Installing..."; \
		curl -LsSf https://astral.sh/uv/install.sh | sh; \
	else \
		echo "uv is already installed."; \
	fi

install: install-uv ## Install uv, dependencies, and pre-commit hooks
	uv sync $(PYTHON_ARG) --all-extras
	@# Reinstall pyiceberg if Cython extensions (.so) are missing after `make clean` (see #2869)
	@if ! find pyiceberg -name "*.so" 2>/dev/null | grep -q .; then \
		echo "Cython extensions not found, reinstalling pyiceberg..."; \
		uv sync $(PYTHON_ARG) --all-extras --reinstall-package pyiceberg; \
	fi
	@# Install pre-commit hooks (skipped outside git repo, e.g. release tarballs)
	@if [ -d .git ]; then \
		uv run $(PYTHON_ARG) prek install; \
	fi

# ===============
# Code Validation
# ===============

##@ Quality

check-license: ## Check license headers
	./dev/check-license

lint: ## Run code linters via prek (pre-commit hooks)
	uv run $(PYTHON_ARG) prek run -a

# ===============
# Testing Section
# ===============

##@ Testing

test: ## Run all unit tests (excluding integration)
	$(TEST_RUNNER) pytest tests/ -m "(unmarked or parametrize) and not integration" $(PYTEST_ARGS)

test-integration: test-integration-setup test-integration-exec test-integration-cleanup ## Run integration tests

test-integration-setup: install ## Start Docker services for integration tests
	docker compose -f dev/docker-compose-integration.yml kill
	docker compose -f dev/docker-compose-integration.yml rm -f
	docker compose -f dev/docker-compose-integration.yml up -d --build --wait
	uv run $(PYTHON_ARG) python dev/provision.py

test-integration-exec: ## Run integration tests (excluding provision)
	$(TEST_RUNNER) pytest tests/ -m integration $(PYTEST_ARGS)

test-integration-cleanup: ## Clean up integration test environment
	@if [ "${KEEP_COMPOSE}" != "1" ]; then \
		echo "Cleaning up Docker containers..."; \
	fi
	$(CLEANUP_COMMAND)

test-integration-rebuild: ## Rebuild integration Docker services from scratch
	docker compose -f dev/docker-compose-integration.yml kill
	docker compose -f dev/docker-compose-integration.yml rm -f
	docker compose -f dev/docker-compose-integration.yml build --no-cache

test-s3: ## Run tests marked with @pytest.mark.s3
	sh ./dev/run-minio.sh
	$(TEST_RUNNER) pytest tests/ -m s3 $(PYTEST_ARGS)

test-adls: ## Run tests marked with @pytest.mark.adls
	sh ./dev/run-azurite.sh
	$(TEST_RUNNER) pytest tests/ -m adls $(PYTEST_ARGS)

test-gcs: ## Run tests marked with @pytest.mark.gcs
	sh ./dev/run-gcs-server.sh
	$(TEST_RUNNER) pytest tests/ -m gcs $(PYTEST_ARGS)

test-coverage: COVERAGE=1
test-coverage: test test-integration test-s3 test-adls test-gcs coverage-report ## Run all tests with coverage and report

coverage-report: ## Combine and report coverage
	uv run $(PYTHON_ARG) coverage combine
	uv run $(PYTHON_ARG) coverage report -m --fail-under=$(COVERAGE_FAIL_UNDER)
	uv run $(PYTHON_ARG) coverage html
	uv run $(PYTHON_ARG) coverage xml

# ================
# Documentation
# ================

##@ Documentation

docs-install: ## Install docs dependencies (included in default groups)
	uv sync $(PYTHON_ARG) --group docs

docs-serve: ## Serve local docs preview (hot reload)
	uv run $(PYTHON_ARG) mkdocs serve -f mkdocs/mkdocs.yml --livereload

docs-build: ## Build the static documentation site
	uv run $(PYTHON_ARG) mkdocs build -f mkdocs/mkdocs.yml --strict

# ========================
# Experimentation
# ========================

##@ Experimentation

notebook-install: ## Install notebook dependencies
	uv sync $(PYTHON_ARG) --all-extras --group notebook

notebook: notebook-install ## Launch notebook for experimentation
	uv run jupyter lab --notebook-dir=notebooks

notebook-infra: notebook-install test-integration-setup ## Launch notebook with integration test infra (Spark, Iceberg Rest Catalog, object storage, etc.)
	uv run jupyter lab --notebook-dir=notebooks

# ===================
# Project Maintenance
# ===================

##@ Maintenance

clean: ## Remove build artifacts and caches
	@echo "Cleaning up Cython and Python cached files..."
	@rm -rf build dist *.egg-info .venv
	@find . -name "*.so" -exec echo Deleting {} \; -delete
	@find . -name "*.pyc" -exec echo Deleting {} \; -delete
	@find . -name "__pycache__" -exec echo Deleting {} \; -exec rm -rf {} +
	@find . -name "*.pyd" -exec echo Deleting {} \; -delete
	@find . -name "*.pyo" -exec echo Deleting {} \; -delete
	@echo "Cleaning up Jupyter notebook checkpoints..."
	@find . -name ".ipynb_checkpoints" -exec echo Deleting {} \; -exec rm -rf {} +
	@echo "Cleanup complete."

uv-lock: ## Regenerate uv.lock file from pyproject.toml
	uv lock $(PYTHON_ARG)

uv-lock-check: ## Verify uv.lock is up to date
	@command -v uv >/dev/null || \
	  (echo "uv is required. Run 'make install' or 'make install-uv' first." && exit 1)
	uv lock --check $(PYTHON_ARG)
