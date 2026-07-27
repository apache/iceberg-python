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

"""Object store bridge: translates PyIceberg io_properties to backend-native config.

PyIceberg stores credentials and storage configuration in io_properties (dict[str, str]).
Each compute backend has its own mechanism for configuring object store access.
This module provides safe, scoped translation without global side effects.

Property key conventions (from PyIceberg FileIO):
    s3.access-key-id       -> AWS access key
    s3.secret-access-key   -> AWS secret key
    s3.session-token       -> AWS session token (for temporary credentials)
    s3.region              -> AWS region (e.g., us-east-1)
    s3.endpoint            -> Custom S3 endpoint (for MinIO, LocalStack, etc.)
    s3.path-style-access   -> Use path-style S3 URLs (true/false)

    gcs.project-id         -> GCP project ID
    gcs.credentials-json   -> GCS service account JSON (file path or inline JSON)

    adls.account-name      -> Azure storage account
    adls.account-key       -> Azure storage key
    adls.sas-token         -> Azure SAS token
    adls.tenant-id         -> Azure AD tenant ID
    adls.client-id         -> Azure AD client ID
    adls.client-secret     -> Azure AD client secret
"""

from __future__ import annotations

import threading
from collections.abc import Generator, Mapping
from contextlib import contextmanager
from typing import Any

__all__ = [
    "datafusion_env_vars_from_properties",
]

# Serializes credential mutations in os.environ. Re-entrant (RLock).
# TODO(datafusion-python#1624): Remove once per-session object store config lands.
# Track: https://github.com/apache/datafusion-python/issues/1624
#        https://github.com/apache/datafusion-python/pull/1625
_ENV_LOCK = threading.RLock()


# Lock serializes concurrent env var mutations across threads. Required because
# DataFusion reads credentials from os.environ (no per-session API yet, #1624).
# Fast-path skips mutation when vars already match (uncontended lock).
@contextmanager
def _scoped_env_vars(env_vars: dict[str, str]) -> Generator[None, None, None]:
    """Set environment variables for the duration of a DataFusion operation, then restore."""
    import os

    if not env_vars:
        yield
        return

    with _ENV_LOCK:
        all_present = all(os.environ.get(key) == value for key, value in env_vars.items())
        if all_present:
            yield
            return

        original: dict[str, str | None] = {}
        for key, value in env_vars.items():
            original[key] = os.environ.get(key)
            os.environ[key] = value
        try:
            yield
        finally:
            for key, orig_value in original.items():
                if orig_value is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = orig_value


def datafusion_env_vars_from_properties(io_properties: Mapping[str, Any]) -> dict[str, str]:
    """Translate PyIceberg io_properties to DataFusion environment variable mappings."""
    env_vars: dict[str, str] = {}

    if "s3.access-key-id" in io_properties:
        env_vars["AWS_ACCESS_KEY_ID"] = str(io_properties["s3.access-key-id"])
    if "s3.secret-access-key" in io_properties:
        env_vars["AWS_SECRET_ACCESS_KEY"] = str(io_properties["s3.secret-access-key"])
    if "s3.session-token" in io_properties:
        env_vars["AWS_SESSION_TOKEN"] = str(io_properties["s3.session-token"])
    if "s3.region" in io_properties:
        env_vars["AWS_DEFAULT_REGION"] = str(io_properties["s3.region"])
    if "s3.endpoint" in io_properties:
        env_vars["AWS_ENDPOINT_URL"] = str(io_properties["s3.endpoint"])
    if io_properties.get("s3.path-style-access", "").lower() in ("true", "1", "yes"):
        env_vars["AWS_VIRTUAL_HOSTED_STYLE_REQUEST"] = "false"

    if "gcs.credentials-json" in io_properties:
        value = str(io_properties["gcs.credentials-json"])
        if value.lstrip().startswith("{"):
            env_vars["GOOGLE_SERVICE_ACCOUNT"] = value
        else:
            env_vars["GOOGLE_APPLICATION_CREDENTIALS"] = value

    if "adls.account-name" in io_properties:
        env_vars["AZURE_STORAGE_ACCOUNT_NAME"] = str(io_properties["adls.account-name"])
    if "adls.account-key" in io_properties:
        env_vars["AZURE_STORAGE_ACCOUNT_KEY"] = str(io_properties["adls.account-key"])
    if "adls.sas-token" in io_properties:
        env_vars["AZURE_STORAGE_SAS_TOKEN"] = str(io_properties["adls.sas-token"])
    if "adls.tenant-id" in io_properties:
        env_vars["AZURE_STORAGE_TENANT_ID"] = str(io_properties["adls.tenant-id"])
    if "adls.client-id" in io_properties:
        env_vars["AZURE_STORAGE_CLIENT_ID"] = str(io_properties["adls.client-id"])
    if "adls.client-secret" in io_properties:
        env_vars["AZURE_STORAGE_CLIENT_SECRET"] = str(io_properties["adls.client-secret"])

    return env_vars
