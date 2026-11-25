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
.PHONY: help install install-uv check-license lint \
        docs-serve docs-build notebook notebook-infra \
        clean

.DEFAULT_GOAL := help
# ========================
# Configuration Variables
# ========================

PYTHON ?=  # Override with e.g. PYTHON=3.11 to use specific Python version

# Set Python argument for uv commands if PYTHON is specified
ifneq ($(PYTHON),)
  PYTHON_ARG = --python $(PYTHON)
else
  PYTHON_ARG =
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

# ================
# Documentation
# ================

##@ Documentation

docs-serve: ## Serve local docs preview (hot reload)
	uv run $(PYTHON_ARG) --no-default-groups --group docs mkdocs serve -f mkdocs/mkdocs.yml --livereload

docs-build: ## Build the static documentation site
	uv run $(PYTHON_ARG) --no-default-groups --group docs mkdocs build -f mkdocs/mkdocs.yml --strict

# ========================
# Experimentation
# ========================

##@ Experimentation

notebook: ## Launch notebook for experimentation
	uv run $(PYTHON_ARG) --all-extras --group notebook jupyter lab --notebook-dir=notebooks

notebook-infra: install ## Launch notebook with integration test infra (Spark, Iceberg Rest Catalog, object storage, etc.)
	docker compose -f dev/docker-compose-integration.yml kill
	docker compose -f dev/docker-compose-integration.yml rm -f
	docker compose -f dev/docker-compose-integration.yml up -d --build --wait
	uv run $(PYTHON_ARG) python dev/provision.py
	uv run $(PYTHON_ARG) --all-extras --group notebook jupyter lab --notebook-dir=notebooks

# ===================
# Project Maintenance
# ===================

##@ Maintenance

clean: ## Remove build artifacts and caches
	@echo "Cleaning up Cython and Python cached files..."
	@rm -rf build dist *.egg-info .venv .tox
	@find . -name "*.so" -exec echo Deleting {} \; -delete
	@find . -name "*.pyc" -exec echo Deleting {} \; -delete
	@find . -name "__pycache__" -exec echo Deleting {} \; -exec rm -rf {} +
	@find . -name "*.pyd" -exec echo Deleting {} \; -delete
	@find . -name "*.pyo" -exec echo Deleting {} \; -delete
	@echo "Cleaning up Jupyter notebook checkpoints..."
	@find . -name ".ipynb_checkpoints" -exec echo Deleting {} \; -exec rm -rf {} +
	@echo "Cleaning up coverage files..."
	@rm -rf .coverage .coverage.* htmlcov/ coverage.xml
	@echo "Cleanup complete."
