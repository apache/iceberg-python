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

from unittest import mock

from moto import mock_aws

from pyiceberg.catalog.glue import GlueCatalog
from pyiceberg.typedef import Properties
from tests.conftest import UNIFIED_AWS_SESSION_PROPERTIES


@mock_aws
def test_passing_client_profile_name_properties_to_glue() -> None:
    session_properties: Properties = {
        "client.profile-name": "profile_name",
        **UNIFIED_AWS_SESSION_PROPERTIES,
    }

    with mock.patch("boto3.Session") as mock_session:
        test_catalog = GlueCatalog("glue", **session_properties)

    mock_session.assert_called_with(
        aws_access_key_id="client.access-key-id",
        aws_secret_access_key="client.secret-access-key",
        aws_session_token="client.session-token",
        region_name="client.region",
        profile_name="profile_name",
        botocore_session=None,
    )
    assert test_catalog.glue is mock_session().client()


@mock_aws
def test_glue_profile_precedence() -> None:
    session_properties: Properties = {
        "glue.profile-name": "glue-profile",
        "client.profile-name": "client-profile",
        **UNIFIED_AWS_SESSION_PROPERTIES,
    }

    with mock.patch("boto3.Session") as mock_session:
        test_catalog = GlueCatalog("glue", **session_properties)

    mock_session.assert_called_with(
        aws_access_key_id="client.access-key-id",
        aws_secret_access_key="client.secret-access-key",
        aws_session_token="client.session-token",
        region_name="client.region",
        profile_name="glue-profile",
        botocore_session=None,
    )
    assert test_catalog.glue is mock_session().client()
