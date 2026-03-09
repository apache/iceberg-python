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


import uuid
from unittest import mock

from pyiceberg.io.fsspec import FsspecFileIO
from pyiceberg.typedef import Properties
from tests.conftest import UNIFIED_AWS_SESSION_PROPERTIES


def test_fsspec_s3_session_properties_with_profile() -> None:
    session_properties: Properties = {
        "s3.profile-name": "test-profile",
        "s3.endpoint": "http://localhost:9000",
        **UNIFIED_AWS_SESSION_PROPERTIES,
    }

    with mock.patch("s3fs.S3FileSystem") as mock_s3fs:
        s3_fileio = FsspecFileIO(properties=session_properties)
        filename = str(uuid.uuid4())

        s3_fileio.new_input(location=f"s3://warehouse/{filename}")

        mock_s3fs.assert_called_with(
            anon=False,
            client_kwargs={
                "endpoint_url": "http://localhost:9000",
                "aws_access_key_id": "client.access-key-id",
                "aws_secret_access_key": "client.secret-access-key",
                "region_name": "client.region",
                "aws_session_token": "client.session-token",
            },
            config_kwargs={},
            profile="test-profile",
        )


def test_fsspec_s3_session_properties_with_client_profile() -> None:
    session_properties: Properties = {
        "client.profile-name": "test-profile",
        "s3.endpoint": "http://localhost:9000",
        **UNIFIED_AWS_SESSION_PROPERTIES,
    }

    with mock.patch("s3fs.S3FileSystem") as mock_s3fs:
        s3_fileio = FsspecFileIO(properties=session_properties)
        filename = str(uuid.uuid4())

        s3_fileio.new_input(location=f"s3://warehouse/{filename}")

        mock_s3fs.assert_called_with(
            anon=False,
            client_kwargs={
                "endpoint_url": "http://localhost:9000",
                "aws_access_key_id": "client.access-key-id",
                "aws_secret_access_key": "client.secret-access-key",
                "region_name": "client.region",
                "aws_session_token": "client.session-token",
            },
            config_kwargs={},
            profile="test-profile",
        )


def test_fsspec_s3_session_properties_with_s3_and_client_profile() -> None:
    session_properties: Properties = {
        "s3.profile-name": "s3-profile",
        "client.profile-name": "client-profile",
        "s3.endpoint": "http://localhost:9000",
        **UNIFIED_AWS_SESSION_PROPERTIES,
    }

    with mock.patch("s3fs.S3FileSystem") as mock_s3fs:
        s3_fileio = FsspecFileIO(properties=session_properties)
        filename = str(uuid.uuid4())

        s3_fileio.new_input(location=f"s3://warehouse/{filename}")

        mock_s3fs.assert_called_with(
            anon=False,
            client_kwargs={
                "endpoint_url": "http://localhost:9000",
                "aws_access_key_id": "client.access-key-id",
                "aws_secret_access_key": "client.secret-access-key",
                "region_name": "client.region",
                "aws_session_token": "client.session-token",
            },
            config_kwargs={},
            profile="s3-profile",
        )
