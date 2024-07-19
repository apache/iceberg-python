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

install-poetry:
	pip install poetry==1.8.3

install-dependencies:
	poetry install -E pyarrow -E hive -E s3fs -E glue -E adlfs -E duckdb -E ray -E sql-postgres -E gcsfs -E sql-sqlite -E daft

install: | install-poetry install-dependencies

check-license:
	./dev/check-license

lint:
	poetry run pre-commit run --all-files

test:
	poetry run pytest tests/ -m "(unmarked or parametrize) and not integration" ${PYTEST_ARGS}

test-s3:
	sh ./dev/run-minio.sh
	poetry run pytest tests/ -m s3 ${PYTEST_ARGS}

test-integration:
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

test-adlfs:
	sh ./dev/run-azurite.sh
	poetry run pytest tests/ -m adlfs ${PYTEST_ARGS}

test-gcs:
	sh ./dev/run-gcs-server.sh
	poetry run  pytest tests/ -m gcs ${PYTEST_ARGS}

test-coverage:
	docker compose -f dev/docker-compose-integration.yml kill
	docker compose -f dev/docker-compose-integration.yml rm -f
	docker compose -f dev/docker-compose-integration.yml up -d
	sh ./dev/run-azurite.sh
	sh ./dev/run-gcs-server.sh
	sleep 10
	docker compose -f dev/docker-compose-integration.yml cp ./dev/provision.py spark-iceberg:/opt/spark/provision.py
	docker compose -f dev/docker-compose-integration.yml exec -T spark-iceberg ipython ./provision.py
	poetry run coverage run --source=pyiceberg/ -m pytest tests/ ${PYTEST_ARGS}
	poetry run coverage report -m --fail-under=90
	poetry run coverage html
	poetry run coverage xml


clean:
	@echo "Cleaning up Cython and Python cached files"
	@rm -rf build dist *.egg-info
	@find . -name "*.so" -exec echo Deleting {} \; -delete
	@find . -name "*.pyc" -exec echo Deleting {} \; -delete
	@find . -name "__pycache__" -exec echo Deleting {} \; -exec rm -rf {} +
	@find . -name "*.pyd" -exec echo Deleting {} \; -delete
	@find . -name "*.pyo" -exec echo Deleting {} \; -delete
	@echo "Cleanup complete"
