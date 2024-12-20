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

import mmh3

from pyiceberg.partitioning import PartitionKey
from pyiceberg.table import LocationProvider, TableProperties
from pyiceberg.typedef import Properties
from pyiceberg.utils.properties import property_as_bool


class DefaultLocationProvider(LocationProvider):
    def __init__(self, table_location: str, table_properties: Properties):
        super().__init__(table_location, table_properties)

    def new_data_location(self, data_file_name: str, partition_key: Optional[PartitionKey] = None) -> str:
        prefix = f"{self.table_location}/data"
        return f"{prefix}/{partition_key.to_path()}/{data_file_name}" if partition_key else f"{prefix}/{data_file_name}"


HASH_BINARY_STRING_BITS = 20
ENTROPY_DIR_LENGTH = 4
ENTROPY_DIR_DEPTH = 3


class ObjectStoreLocationProvider(LocationProvider):
    _include_partition_paths: bool

    def __init__(self, table_location: str, table_properties: Properties):
        super().__init__(table_location, table_properties)
        self._include_partition_paths = property_as_bool(
            table_properties,
            TableProperties.WRITE_OBJECT_STORE_PARTITIONED_PATHS,
            TableProperties.WRITE_OBJECT_STORE_PARTITIONED_PATHS_DEFAULT,
        )

    def new_data_location(self, data_file_name: str, partition_key: Optional[PartitionKey] = None) -> str:
        if self._include_partition_paths and partition_key:
            return self.new_data_location(f"{partition_key.to_path()}/{data_file_name}")

        prefix = f"{self.table_location}/data"
        hashed_path = self._compute_hash(data_file_name)

        return (
            f"{prefix}/{hashed_path}/{data_file_name}"
            if self._include_partition_paths
            else f"{prefix}/{hashed_path}-{data_file_name}"
        )

    @staticmethod
    def _compute_hash(data_file_name: str) -> str:
        # Bitwise AND to combat sign-extension; bitwise OR to preserve leading zeroes that `bin` would otherwise strip.
        hash_code = mmh3.hash(data_file_name) & ((1 << HASH_BINARY_STRING_BITS) - 1) | (1 << HASH_BINARY_STRING_BITS)
        return ObjectStoreLocationProvider._dirs_from_hash(bin(hash_code)[-HASH_BINARY_STRING_BITS:])

    @staticmethod
    def _dirs_from_hash(file_hash: str) -> str:
        """Divides hash into directories for optimized orphan removal operation using ENTROPY_DIR_DEPTH and ENTROPY_DIR_LENGTH."""
        hash_with_dirs = []
        for i in range(0, ENTROPY_DIR_DEPTH * ENTROPY_DIR_LENGTH, ENTROPY_DIR_LENGTH):
            hash_with_dirs.append(file_hash[i : i + ENTROPY_DIR_LENGTH])

        if len(file_hash) > ENTROPY_DIR_DEPTH * ENTROPY_DIR_LENGTH:
            hash_with_dirs.append(file_hash[ENTROPY_DIR_DEPTH * ENTROPY_DIR_LENGTH :])

        return "/".join(hash_with_dirs)
