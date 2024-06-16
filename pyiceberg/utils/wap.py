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

from typing import Optional

import pyiceberg.table as tbl


def get_wap_id(snapshot: tbl.Snapshot, state: str) -> Optional[str]:
    return snapshot.summary.get(state, None) if snapshot.summary else None


def validate_wap_publish(table_metadata: tbl.TableMetadata, snapshot: tbl.Snapshot) -> str:
    wap_id = get_wap_id(snapshot=snapshot, state=tbl.STAGED_WAP_ID_PROP)
    if wap_id not in [None, ""]:
        if is_wap_id_published(table_metadata, wap_id):
            raise ValueError(f"Duplicate request to cherry pick wap id that was published already: {wap_id}")
    return wap_id if wap_id else ""


def is_wap_id_published(table_metadata: tbl.TableMetadata, wap_id: str) -> bool:
    for ancestor in ancestors_of(table_metadata.current_snapshot(), table_metadata):
        if wap_id in (get_wap_id(ancestor, tbl.STAGED_WAP_ID_PROP), get_wap_id(ancestor, tbl.PUBLISHED_WAP_ID_PROP)):
            return True
    return False
