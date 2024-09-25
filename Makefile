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


help:  ## Display this help
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m\033[0m\n"} /^[a-zA-Z_-]+:.*?##/ { printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

install-poetry:  ## Install poetry if the user has not done that yet.
	 @if ! command -v poetry &> /dev/null; then \
         echo "Poetry could not be found. Installing..."; \
         pip install --user poetry==1.8.3; \
     else \
         echo "Poetry is already installed."; \
     fi

install-dependencies: ## Install dependencies including dev and all extras
	poetry install --all-extras

install: | install-poetry install-dependencies

check-license: ## Check license headers
	./dev/check-license

lint: ## lint
	poetry run pre-commit run --all-files

test: ## Run all unit tests, can add arguments with PYTEST_ARGS="-vv"
	poetry run pytest tests/ -m "(unmarked or parametrize) and not integration" ${PYTEST_ARGS}

test-s3: # Run tests marked with s3, can add arguments with PYTEST_ARGS="-vv"
	sh ./dev/run-minio.sh
	poetry run pytest tests/ -m s3 ${PYTEST_ARGS}

test-integration: ## Run all integration tests, can add arguments with PYTEST_ARGS="-vv"
	docker compose -f dev/docker-compose-integration.yml kill
	docker compose -f dev/docker-compose-integration.yml rm -f
	docker compose -f dev/docker-compose-integration.yml up -d
	sleep 10
	docker compose -f dev/docker-compose-integration.yml cp ./dev/provision.py spark-iceberg:/opt/spark/provision.py
	docker compose -f dev/docker-compose-integration.yml exec -T spark-iceberg ipython ./provision.py
	poetry run pytest tests/ -v -m integration ${PYTEST_ARGS}

test-integration-rebuild:
	docker compose -f dev/docker-compose-integration.yml kill
	docker compose -f dev/docker-compose-integration.yml rm -f
	docker compose -f dev/docker-compose-integration.yml build --no-cache

test-adls: ## Run tests marked with adls, can add arguments with PYTEST_ARGS="-vv"
	sh ./dev/run-azurite.sh
	poetry run pytest tests/ -m adls ${PYTEST_ARGS}

test-gcs: ## Run tests marked with gcs, can add arguments with PYTEST_ARGS="-vv"
	sh ./dev/run-gcs-server.sh
	poetry run  pytest tests/ -m gcs ${PYTEST_ARGS}

test-coverage-unit: # Run test with coverage for unit tests, can add arguments with PYTEST_ARGS="-vv"
	poetry run coverage run --source=pyiceberg/ --data-file=.coverage.unit -m pytest tests/ -v -m "(unmarked or parametrize) and not integration" ${PYTEST_ARGS}

test-coverage-integration: # Run test with coverage for integration tests, can add arguments with PYTEST_ARGS="-vv"
	docker compose -f dev/docker-compose-integration.yml kill
	docker compose -f dev/docker-compose-integration.yml rm -f
	docker compose -f dev/docker-compose-integration.yml up -d
	sh ./dev/run-azurite.sh
	sh ./dev/run-gcs-server.sh
	sleep 10
	docker compose -f dev/docker-compose-integration.yml cp ./dev/provision.py spark-iceberg:/opt/spark/provision.py
	docker compose -f dev/docker-compose-integration.yml exec -T spark-iceberg ipython ./provision.py
	poetry run coverage run --source=pyiceberg/ --data-file=.coverage.integration -m pytest tests/ -v -m integration ${PYTEST_ARGS}

test-coverage: | test-coverage-unit test-coverage-integration ## Run all tests with coverage including unit and integration tests
	poetry run coverage combine .coverage.unit .coverage.integration
	poetry run coverage report -m --fail-under=90
	poetry run coverage html
	poetry run coverage xml


clean: ## Clean up the project Python working environment
	@echo "Cleaning up Cython and Python cached files"
	@rm -rf build dist *.egg-info
	@find . -name "*.so" -exec echo Deleting {} \; -delete
	@find . -name "*.pyc" -exec echo Deleting {} \; -delete
	@find . -name "__pycache__" -exec echo Deleting {} \; -exec rm -rf {} +
	@find . -name "*.pyd" -exec echo Deleting {} \; -delete
	@find . -name "*.pyo" -exec echo Deleting {} \; -delete
	@echo "Cleanup complete"
