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


import requests

POLARIS_URL = "http://localhost:8181/api/management/v1"
POLARIS_TOKEN_URL = "http://localhost:8181/api/catalog/v1/oauth/tokens"


def get_token(client_id: str, client_secret: str) -> str:
    response = requests.post(
        POLARIS_TOKEN_URL,
        data={
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
            "scope": "PRINCIPAL_ROLE:ALL",
        },
        headers={"realm": "POLARIS"},
    )
    response.raise_for_status()
    return response.json()["access_token"]


def provision() -> None:
    # Initial authentication with root credentials
    token = get_token("root", "s3cr3t")
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json", "realm": "POLARIS"}

    # 1. Create Principal
    principal_name = "pyiceberg_principal"
    principal_resp = requests.post(
        f"{POLARIS_URL}/principals",
        headers=headers,
        json={"name": principal_name, "type": "PRINCIPAL"},
    )
    if principal_resp.status_code == 409:
        principal_resp = requests.post(
            f"{POLARIS_URL}/principals/{principal_name}/rotate-credentials",
            headers=headers,
        )
    principal_resp.raise_for_status()
    principal_data = principal_resp.json()
    client_id = principal_data["credentials"]["clientId"]
    client_secret = principal_data["credentials"]["clientSecret"]

    # 2. Assign service_admin role to our principal
    requests.put(
        f"{POLARIS_URL}/principals/{principal_name}/principal-roles",
        headers=headers,
        json={"principalRole": {"name": "service_admin"}},
    ).raise_for_status()

    # 3. Create Principal Role for catalog access
    role_name = "pyiceberg_role"
    requests.post(
        f"{POLARIS_URL}/principal-roles",
        headers=headers,
        json={"principalRole": {"name": role_name}},
    )  # Ignore error if exists

    # 4. Link Principal to Principal Role
    requests.put(
        f"{POLARIS_URL}/principals/{principal_name}/principal-roles",
        headers=headers,
        json={"principalRole": {"name": role_name}},
    ).raise_for_status()

    # 5. Create Catalog
    catalog_name = "polaris"
    requests.post(
        f"{POLARIS_URL}/catalogs",
        headers=headers,
        json={
            "catalog": {
                "name": catalog_name,
                "type": "INTERNAL",
                "readOnly": False,
                "properties": {
                    "default-base-location": "s3://warehouse/polaris/",
                    "polaris.config.drop-with-purge.enabled": "true",
                },
                "storageConfigInfo": {
                    "storageType": "S3",
                    "allowedLocations": ["s3://warehouse/polaris/"],
                    "region": "us-east-1",
                    "endpoint": "http://minio:9000",
                },
            }
        },
    )  # Ignore error if exists

    # 6. Link catalog_admin role to our principal role
    requests.put(
        f"{POLARIS_URL}/principal-roles/{role_name}/catalog-roles/{catalog_name}",
        headers=headers,
        json={"catalogRole": {"name": "catalog_admin"}},
    ).raise_for_status()

    # 7. Grant explicit privileges to catalog_admin role for this catalog
    for privilege in [
        "CATALOG_MANAGE_CONTENT",
        "CATALOG_MANAGE_METADATA",
        "TABLE_CREATE",
        "TABLE_WRITE_DATA",
        "TABLE_LIST",
        "NAMESPACE_CREATE",
        "NAMESPACE_LIST",
    ]:
        requests.put(
            f"{POLARIS_URL}/catalogs/{catalog_name}/catalog-roles/catalog_admin/grants",
            headers=headers,
            json={"grant": {"type": "catalog", "privilege": privilege}},
        ).raise_for_status()

    # Print credentials for use in CI
    print(f"CLIENT_ID={client_id}")
    print(f"CLIENT_SECRET={client_secret}")


if __name__ == "__main__":
    provision()
