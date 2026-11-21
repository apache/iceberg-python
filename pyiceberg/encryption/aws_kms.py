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
"""AWS KMS key management client for table encryption."""

from __future__ import annotations

import boto3

from pyiceberg.encryption.kms import KeyManagementClient
from pyiceberg.io import AWS_ACCESS_KEY_ID, AWS_PROFILE_NAME, AWS_REGION, AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN
from pyiceberg.typedef import EMPTY_DICT, Properties
from pyiceberg.utils.transient import _TransientStateMixin

KMS_ENDPOINT = "kms.endpoint"


class AwsKeyManagementClient(_TransientStateMixin, KeyManagementClient):
    """Wraps and unwraps keys with master keys held by AWS KMS."""

    _transient_attrs = ("_client",)

    def __init__(self, properties: Properties = EMPTY_DICT) -> None:
        super().__init__(properties)
        self._init_transient()

    def _init_transient(self) -> None:
        session = boto3.Session(
            profile_name=self.properties.get(AWS_PROFILE_NAME),
            region_name=self.properties.get(AWS_REGION),
            aws_access_key_id=self.properties.get(AWS_ACCESS_KEY_ID),
            aws_secret_access_key=self.properties.get(AWS_SECRET_ACCESS_KEY),
            aws_session_token=self.properties.get(AWS_SESSION_TOKEN),
        )
        self._client = session.client("kms", endpoint_url=self.properties.get(KMS_ENDPOINT))

    def wrap_key(self, key: bytes, wrapping_key_id: str) -> bytes:
        """Encrypt `key` with the KMS key `wrapping_key_id`."""
        return self._client.encrypt(KeyId=wrapping_key_id, Plaintext=key)["CiphertextBlob"]

    def unwrap_key(self, wrapped_key: bytes, wrapping_key_id: str) -> bytes:
        """Decrypt `wrapped_key` with the KMS key `wrapping_key_id`."""
        return self._client.decrypt(KeyId=wrapping_key_id, CiphertextBlob=wrapped_key)["Plaintext"]
