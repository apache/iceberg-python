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
  TEST_RUNNER = uv run coverage run --parallel-mode --source=pyiceberg -m
else
  TEST_RUNNER = uv run
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
	@if ! command -v uv &> /dev/null; then \
		echo "uv not found. Installing..."; \
		curl -LsSf https://astral.sh/uv/install.sh | sh; \
	else \
		echo "uv is already installed."; \
	fi

setup-venv: ## Create virtual environment
	@if [ ! -d .venv ]; then \
		echo "Creating virtual environment."; \
		uv venv $(PYTHON_ARG); \
	else \
		echo "Virtual environment already exists."; \
	fi

install-dependencies: setup-venv ## Install all dependencies including extras
	uv sync --all-extras

install: install-uv install-dependencies ## Install uv and dependencies

# ===============
# Code Validation
# ===============

##@ Quality

check-license: ## Check license headers
	./dev/check-license

lint: ## Run code linters via prek (pre-commit hooks)
	uv run prek run -a

# ===============
# Testing Section
# ===============

##@ Testing

test: ## Run all unit tests (excluding integration)
	$(TEST_RUNNER) pytest tests/ -m "(unmarked or parametrize) and not integration" $(PYTEST_ARGS)

test-integration: test-integration-setup test-integration-exec test-integration-cleanup ## Run integration tests

test-integration-setup: ## Start Docker services for integration tests
	docker compose -f dev/docker-compose-integration.yml kill
	docker compose -f dev/docker-compose-integration.yml rm -f
	docker compose -f dev/docker-compose-integration.yml up -d --wait
	uv run python dev/provision.py

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
	uv run coverage combine
	uv run coverage report -m --fail-under=$(COVERAGE_FAIL_UNDER)
	uv run coverage html
	uv run coverage xml

# ================
# Documentation
# ================

##@ Documentation

docs-install: ## Install docs dependencies (included in default groups)
	uv sync --group docs

docs-serve: ## Serve local docs preview (hot reload)
	uv run mkdocs serve -f mkdocs/mkdocs.yml

docs-build: ## Build the static documentation site
	uv run mkdocs build -f mkdocs/mkdocs.yml --strict

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
	@echo "Cleanup complete."
