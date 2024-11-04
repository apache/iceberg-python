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
from typing import (
    Dict,
    List,
    Optional,
)

from pydantic import Field

from pyiceberg.typedef import IcebergBaseModel


class BlobMetadata(IcebergBaseModel):
    type: str
    snapshot_id: int = Field(alias="snapshot-id")
    sequence_number: int = Field(alias="sequence-number")
    fields: List[int]
    properties: Optional[Dict[str, str]] = None


class StatisticsFile(IcebergBaseModel):
    snapshot_id: int = Field(alias="snapshot-id")
    statistics_path: str = Field(alias="statistics-path")
    file_size_in_bytes: int = Field(alias="file-size-in-bytes")
    file_footer_size_in_bytes: int = Field(alias="file-footer-size-in-bytes")
    key_metadata: Optional[str] = Field(alias="key-metadata", default=None)
    blob_metadata: List[BlobMetadata] = Field(alias="blob-metadata")


def filter_statistics_by_snapshot_id(
    statistics: List[StatisticsFile],
    reject_snapshot_id: int,
) -> List[StatisticsFile]:
    return [stat for stat in statistics if stat.snapshot_id != reject_snapshot_id]
