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

import json

import pytest
from requests import HTTPError, Session

from pyiceberg.catalog import Catalog, load_catalog

PRINCIPAL_TOKEN = "principal:root;realm:default-realm"
POLARIS_URL = "http://localhost:8181"
PRINCIPAL_NAME = "iceberg"
CATALOG_NAME = "polaris"
CATALOG_ROLE = "admin_role"
PRINCIPAL_ROLE = "admin_principal_role"


def create_principal(session: Session) -> str:
    response = session.get(url=f"{POLARIS_URL}/api/management/v1/principals/{PRINCIPAL_NAME}")
    try:
        # rotate creds
        response.raise_for_status()
        response = session.delete(
            url=f"{POLARIS_URL}/api/management/v1/principals/{PRINCIPAL_NAME}",
        )
    finally:
        # create principal
        data = {"principal": {"name": PRINCIPAL_NAME}, "credentialRotationRequired": "false"}
        response = session.post(
            url=f"{POLARIS_URL}/api/management/v1/principals",
            data=json.dumps(data),
        )
    credentials = response.json()["credentials"]

    principal_credential = f"{credentials['clientId']}:{credentials['clientSecret']}"
    return principal_credential


def create_catalog(session: Session) -> None:
    response = session.get(
        url=f"{POLARIS_URL}/api/management/v1/catalogs/{CATALOG_NAME}",
    )
    try:
        response.raise_for_status()
    except HTTPError:
        # Create Catalog
        data = {
            "catalog": {
                "name": CATALOG_NAME,
                "type": "INTERNAL",
                "readOnly": False,
                "properties": {"default-base-location": "file:///warehouse"},
                "storageConfigInfo": {"storageType": "FILE", "allowedLocations": ["file:///warehouse"]},
            }
        }
        response = session.post(
            url=f"{POLARIS_URL}/api/management/v1/catalogs",
            data=json.dumps(data),
        )
        response.raise_for_status()


def create_catalog_role(session: Session) -> None:
    try:
        response = session.get(url=f"{POLARIS_URL}/api/management/v1/catalogs/{CATALOG_NAME}/catalog-roles/{CATALOG_ROLE}")
        response.raise_for_status()
    except HTTPError:
        # Create Catalog Role
        data = {
            "catalogRole": {
                "name": CATALOG_ROLE,
            }
        }
        response = session.post(
            url=f"{POLARIS_URL}/api/management/v1/catalogs/{CATALOG_NAME}/catalog-roles",
            data=json.dumps(data),
        )
        response.raise_for_status()


def grant_catalog_privileges(session: Session) -> None:
    # Grant Catalog privileges to the catalog role
    data = {"grant": {"type": "catalog", "privilege": "CATALOG_MANAGE_CONTENT"}}
    response = session.put(
        url=f"{POLARIS_URL}/api/management/v1/catalogs/{CATALOG_NAME}/catalog-roles/{CATALOG_ROLE}/grants",
        data=json.dumps(data),
    )
    response.raise_for_status()


def create_principal_role(session: Session) -> None:
    try:
        response = session.get(
            url=f"{POLARIS_URL}/api/management/v1/principal-roles/{PRINCIPAL_ROLE}",
        )
        response.raise_for_status()
    except HTTPError:
        # Create a principal role
        data = {
            "principalRole": {
                "name": PRINCIPAL_ROLE,
            }
        }
        response = session.post(
            url=f"{POLARIS_URL}/api/management/v1/principal-roles",
            data=json.dumps(data),
        )
        response.raise_for_status()

    # Assign the catalog role to the principal role
    data = {
        "catalogRole": {
            "name": CATALOG_ROLE,
        }
    }
    response = session.put(
        url=f"{POLARIS_URL}/api/management/v1/principal-roles/{PRINCIPAL_ROLE}/catalog-roles/{CATALOG_NAME}",
        data=json.dumps(data),
    )
    response.raise_for_status()

    # Assign the principal role to the root principal
    data = {
        "principalRole": {
            "name": PRINCIPAL_ROLE,
        }
    }
    response = session.put(
        url=f"{POLARIS_URL}/api/management/v1/principals/{PRINCIPAL_NAME}/principal-roles",
        data=json.dumps(data),
    )
    response.raise_for_status()


@pytest.fixture(scope="session")
def principal_credential() -> str:
    session = Session()
    session.headers["Content-type"] = "application/json"
    session.headers["Accept"] = "application/json"
    session.headers["Authorization"] = f"Bearer {PRINCIPAL_TOKEN}"

    principal_credential = create_principal(session)
    create_catalog(session)
    create_catalog_role(session)
    grant_catalog_privileges(session)
    create_principal_role(session)
    return principal_credential


@pytest.fixture(scope="session")
def session_catalog() -> Catalog:
    return load_catalog(
        "local",
        **{
            "type": "rest",
            "credential": principal_credential,
            "uri": "http://localhost:8181/api/catalog",
            "s3.endpoint": "http://localhost:9000",
            "s3.access-key-id": "admin",
            "s3.secret-access-key": "password",
            "warehouse": "polaris",
            "scope": "PRINCIPAL_ROLE:ALL",
        },
    )
