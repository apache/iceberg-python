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

from pyiceberg.io import LocationProvider
from pyiceberg.partitioning import PartitionKey
from pyiceberg.typedef import Properties


class DefaultLocationProvider(LocationProvider):

    def __init__(self, table_location: str, table_properties: Properties):
        super().__init__(table_location, table_properties)

    def new_data_location(self, data_file_name: str, partition_key: Optional[PartitionKey] = None) -> str:
        prefix = f"{self.table_location}/data"
        return f"{prefix}/{partition_key.to_path()}/{data_file_name}" if partition_key else f"{prefix}/{data_file_name}"


class ObjectStoreLocationProvider(LocationProvider):

    def __init__(self, table_location: str, table_properties: Properties):
        super().__init__(table_location, table_properties)

    def new_data_location(self, data_file_name: str, partition_key: Optional[PartitionKey] = None) -> str:
        prefix = f"{self.table_location}/data/MAGIC_HASH"
        return f"{prefix}/{partition_key.to_path()}/{data_file_name}" if partition_key else f"{prefix}/{data_file_name}"
