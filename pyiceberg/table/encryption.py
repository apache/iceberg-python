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

from typing import Optional
from pydantic import Field
from pyiceberg.typedef import IcebergBaseModel


class EncryptedKey(IcebergBaseModel):
    key_id: str = Field(alias="key-id", description="ID of the encryption key")
    encrypted_key_metadata: bytes = Field(alias="encrypted-key-metadata", description="Encrypted key and metadata, base64 encoded")
    encrypted_by_id: Optional[str] = Field(alias="encrypted-by-id", description="Optional ID of the key used to encrypt or wrap `key-metadata`", default=None)
    properties: Optional[dict[str, str]] = Field(alias="properties", description="A string to string map of additional metadata used by the table's encryption scheme", default=None)