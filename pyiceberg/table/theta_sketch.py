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
from __future__ import annotations

from typing import TYPE_CHECKING

import zstandard

from pyiceberg.table.puffin import PuffinBlobMetadata, PuffinFile

if TYPE_CHECKING:
    from datasketches import compact_theta_sketch

BLOB_TYPE_APACHE_DATASKETCHES_THETA_V1 = "apache-datasketches-theta-v1"


class ThetaSketch:
    field_id: int
    _sketch: compact_theta_sketch

    def __init__(self, field_id: int, sketch: compact_theta_sketch) -> None:
        self.field_id = field_id
        self._sketch = sketch

    def get_estimate(self) -> float:
        return self._sketch.get_estimate()

    def get_lower_bound(self, num_std_devs: int = 1) -> float:
        return self._sketch.get_lower_bound(num_std_devs)

    def get_upper_bound(self, num_std_devs: int = 1) -> float:
        return self._sketch.get_upper_bound(num_std_devs)

    def is_empty(self) -> bool:
        return self._sketch.is_empty()

    def is_estimation_mode(self) -> bool:
        return self._sketch.is_estimation_mode()

    @property
    def sketch(self) -> compact_theta_sketch:
        return self._sketch


def _theta_sketches_from_blob(blob: PuffinBlobMetadata, payload: bytes) -> list[ThetaSketch]:
    from datasketches import compact_theta_sketch

    if blob.compression_codec == "zstd":
        payload = zstandard.decompress(payload)

    sketch = compact_theta_sketch.deserialize(payload)
    return [ThetaSketch(field_id=field_id, sketch=sketch) for field_id in blob.fields]


def theta_sketches_from_puffin_file(puffin_file: PuffinFile) -> list[ThetaSketch]:
    sketches = []
    for blob in puffin_file.footer.blobs:
        if blob.type == BLOB_TYPE_APACHE_DATASKETCHES_THETA_V1:
            sketches.extend(_theta_sketches_from_blob(blob, puffin_file.get_blob_payload(blob)))
    return sketches
