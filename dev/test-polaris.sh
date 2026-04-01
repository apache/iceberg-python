#!/bin/bash
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

set -e

# Load credentials
if [ -f dev/polaris_creds.env ]; then
    # We use a subshell or export to ensure they are available
    export $(cat dev/polaris_creds.env | xargs)
else
    echo "dev/polaris_creds.env not found. Please run 'make test-polaris-setup' first."
    exit 1
fi

# Set environment variables for PyIceberg
export PYICEBERG_TEST_CATALOG="polaris"
export PYICEBERG_CATALOG__POLARIS__TYPE="rest"
export PYICEBERG_CATALOG__POLARIS__URI="http://localhost:8181/api/catalog"
export PYICEBERG_CATALOG__POLARIS__OAUTH2_SERVER_URI="http://localhost:8181/api/catalog/v1/oauth/tokens"
export PYICEBERG_CATALOG__POLARIS__CREDENTIAL="$CLIENT_ID:$CLIENT_SECRET"
export PYICEBERG_CATALOG__POLARIS__SCOPE="PRINCIPAL_ROLE:ALL"
export PYICEBERG_CATALOG__POLARIS__WAREHOUSE="polaris"
export PYICEBERG_CATALOG__POLARIS__HEADER__X_ICEBERG_ACCESS_DELEGATION="vended-credentials"
export PYICEBERG_CATALOG__POLARIS__HEADER__REALM="POLARIS"

# S3 Configuration for RustFS
export PYICEBERG_CATALOG__POLARIS__S3__ENDPOINT="http://localhost:9000"
export PYICEBERG_CATALOG__POLARIS__S3__ACCESS_KEY_ID="polaris_root"
export PYICEBERG_CATALOG__POLARIS__S3__SECRET_ACCESS_KEY="polaris_pass"
export PYICEBERG_CATALOG__POLARIS__S3__REGION="us-west-2"
export PYICEBERG_CATALOG__POLARIS__S3__PATH_STYLE_ACCESS="true"

# Default test runner if not provided
RUNNER=${TEST_RUNNER:-uv run python -m}

# Run pytest
# Skip test_update_namespace_properties: Polaris triggers a CommitConflictException when updates and removals are in the same request.
# TODO: Remove test exception after https://github.com/apache/polaris/pull/4013 is merged.
$RUNNER pytest tests/integration/test_catalog.py -k "rest_test_catalog and not test_update_namespace_properties" "$@"
