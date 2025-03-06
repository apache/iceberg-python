#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.

from typing import Callable, Iterable, Iterator, Optional
from pyiceberg.table.snapshots import Snapshot


def ancestors_of(snapshot_id: Optional[int], lookup_fn: Callable[[int], Optional[Snapshot]]) -> Iterable[Snapshot]:
    def _snapshot_iterator(snapshot: Snapshot) -> Iterator[Snapshot]:
        next_snapshot: Optional[Snapshot] = snapshot
        consumed = False

        while next_snapshot is not None:
            if not consumed:
                yield next_snapshot
                consumed = True

            parent_id = next_snapshot.parent_snapshot_id
            if parent_id is None:
                break

            next_snapshot = lookup_fn(parent_id)
            consumed = False
    
    snapshot: Optional[Snapshot] = lookup_fn(snapshot_id)
    if snapshot is not None:
        return _snapshot_iterator(snapshot)
    else:
        return iter([])

def ancestors_between(starting_snapshot_id: Optional[int], current_snapshot_id: Optional[int], lookup_fn: Callable[[int], Optional[Snapshot]]) -> Iterable[Snapshot]:
    if starting_snapshot_id == current_snapshot_id:
        return iter([])
    
    return ancestors_of(
        current_snapshot_id, 
        lambda snapshot_id: lookup_fn(snapshot_id) if snapshot_id != starting_snapshot_id else None
    )
    