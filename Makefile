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

PYTEST_ARGS ?= -v  # Override with e.g. PYTEST_ARGS="-vv --tb=short"
COVERAGE ?= 0      # Set COVERAGE=1 to enable coverage: make test COVERAGE=1
COVERAGE_FAIL_UNDER ?= 85  # Minimum coverage % to pass: make coverage-report COVERAGE_FAIL_UNDER=70

ifeq ($(COVERAGE),1)
  TEST_RUNNER = poetry run coverage run --parallel-mode --source=pyiceberg -m
else
  TEST_RUNNER = poetry run
endif

POETRY_VERSION = 2.1.4

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

install-poetry: ## Ensure Poetry is installed at the specified version
	@if ! command -v poetry &> /dev/null; then \
		echo "Poetry not found. Installing..."; \
		pip install --user poetry==$(POETRY_VERSION); \
	else \
		INSTALLED_VERSION=$$(pip show poetry | grep Version | awk '{print $$2}'); \
		if [ "$$INSTALLED_VERSION" != "$(POETRY_VERSION)" ]; then \
			echo "Updating Poetry to version $(POETRY_VERSION)..."; \
			pip install --user --upgrade poetry==$(POETRY_VERSION); \
		else \
			echo "Poetry version $(POETRY_VERSION) already installed."; \
		fi; \
	fi

install-dependencies: ## Install all dependencies including extras
	poetry install --all-extras

install: install-poetry install-dependencies ## Install Poetry and dependencies

# ===============
# Code Validation
# ===============

##@ Quality

check-license: ## Check license headers
	./dev/check-license

lint: ## Run code linters via pre-commit
	poetry run pre-commit run --all-files

# ===============
# Testing Section
# ===============

##@ Testing

test: ## Run all unit tests (excluding integration)
	$(TEST_RUNNER) pytest tests/ -m "(unmarked or parametrize) and not integration" $(PYTEST_ARGS)

test-integration: test-integration-setup test-integration-exec ## Run integration tests

test-integration-setup: ## Start Docker services for integration tests
	docker compose -f dev/docker-compose-integration.yml kill
	docker compose -f dev/docker-compose-integration.yml rm -f
	docker compose -f dev/docker-compose-integration.yml up -d
	sleep 10
	docker compose -f dev/docker-compose-integration.yml cp ./dev/provision.py spark-iceberg:/opt/spark/provision.py
	docker compose -f dev/docker-compose-integration.yml exec -T spark-iceberg ipython ./provision.py

test-integration-exec: ## Run integration tests (excluding provision)
	$(TEST_RUNNER) pytest tests/ -m integration $(PYTEST_ARGS)

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
	poetry run coverage combine
	poetry run coverage report -m --fail-under=$(COVERAGE_FAIL_UNDER)
	poetry run coverage html
	poetry run coverage xml

# ================
# Documentation
# ================

##@ Documentation

docs-install: ## Install docs dependencies
	poetry install --with docs

docs-serve: ## Serve local docs preview (hot reload)
	poetry run mkdocs serve -f mkdocs/mkdocs.yml

docs-build: ## Build the static documentation site
	poetry run mkdocs build -f mkdocs/mkdocs.yml --strict

# ===================
# Project Maintenance
# ===================

##@ Maintenance

clean: ## Remove build artifacts and caches
	@echo "Cleaning up Cython and Python cached files..."
	@rm -rf build dist *.egg-info
	@find . -name "*.so" -exec echo Deleting {} \; -delete
	@find . -name "*.pyc" -exec echo Deleting {} \; -delete
	@find . -name "__pycache__" -exec echo Deleting {} \; -exec rm -rf {} +
	@find . -name "*.pyd" -exec echo Deleting {} \; -delete
	@find . -name "*.pyo" -exec echo Deleting {} \; -delete
	@echo "Cleanup complete."
