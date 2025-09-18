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
from typing import TYPE_CHECKING, Tuple

from pyiceberg.table.sorting import SortOrder
from pyiceberg.table.update import (
    AddSortOrderUpdate,
    SetSortOrderUpdate,
    TableUpdate,
    UpdatesAndRequirements,
    UpdateTableMetadata,
)

if TYPE_CHECKING:
    from pyiceberg.table import Transaction


class UpdateSortOrder(UpdateTableMetadata["UpdateSortOrder"]):
    """
    Run sort order management operations using APIs.

    APIs include set_sort_order the operation.

    Use table.update_sort_order().<operation>().commit() to run a specific operation.
    Use table.update_sort_order().<operation-one>().<operation-two>().commit() to run multiple operations.

    Pending changes are applied on commit.

    We can also use context managers to make more changes. For example:

    new_sort_order = SortOrder(SortField(source_id=2, transform='identity'))

    with table.update_sort_order() as update:
        update.set_sort_order(sort_order=new_sort_order)
    """

    _updates: Tuple[TableUpdate, ...] = ()

    def __init__(self, transaction: "Transaction") -> None:
        super().__init__(transaction)

    def add_sort_order(self, sort_order: SortOrder) -> "UpdateSortOrder":
        self._updates += (AddSortOrderUpdate(sort_order=sort_order),)

        return self

    def set_sort_order(self, sort_order: SortOrder) -> "UpdateSortOrder":
        self._updates += (SetSortOrderUpdate(sort_order=sort_order),)

        return self

    def _commit(self) -> UpdatesAndRequirements:
        return self._updates, ()
