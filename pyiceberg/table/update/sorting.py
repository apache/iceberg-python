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

from typing import TYPE_CHECKING, Any, List, Tuple

from pyiceberg.table.sorting import NullOrder, SortDirection, SortField, SortOrder
from pyiceberg.table.update import (
    AddSortOrderUpdate,
    AssertDefaultSortOrderId,
    SetDefaultSortOrderUpdate,
    TableRequirement,
    TableUpdate,
    UpdatesAndRequirements,
    UpdateTableMetadata,
)
from pyiceberg.transforms import Transform

if TYPE_CHECKING:
    from pyiceberg.table import Transaction


class SortOrderBuilder:
    def __init__(self, last_sort_order_id: int, case_sensitive: bool = True) -> None:
        self._fields: List[SortField] = []
        self._case_sensitive = case_sensitive
        self._last_sort_order_id = last_sort_order_id

    def add_sort_field(
        self,
        source_id: int,
        transform: Transform[Any, Any],
        direction: SortDirection,
        null_order: NullOrder,
    ) -> SortOrderBuilder:
        """Add a sort field to the sort order list."""
        self._fields.append(
            SortField(
                source_id=source_id,
                transform=transform,
                direction=direction,
                null_order=null_order,
            )
        )
        return self

    @property
    def sort_order(self) -> SortOrder:
        """Return the sort order."""
        return SortOrder(*self._fields, order_id=self._last_sort_order_id + 1)


class ReplaceSortOrder(UpdateTableMetadata["ReplaceSortOrder"]):
    _transaction: Transaction
    _builder: SortOrderBuilder
    _last_assigned_order_id: int
    _case_sensitive: bool

    def __init__(self, transaction: Transaction, case_sensitive: bool = True) -> None:
        super().__init__(transaction)
        self._builder = SortOrderBuilder(
            case_sensitive=case_sensitive,
            last_sort_order_id=transaction.table_metadata.default_sort_order_id,
        )
        self._case_sensitive = case_sensitive

    def _column_name_to_id(self, column_name: str) -> int:
        return (
            self._transaction.table_metadata.schema()
            .find_field(
                name_or_id=column_name,
                case_sensitive=self._case_sensitive,
            )
            .field_id
        )

    def asc(self, source_column_name: str, transform: Transform[Any, Any], null_order: NullOrder) -> ReplaceSortOrder:
        self._builder.add_sort_field(
            source_id=self._column_name_to_id(source_column_name),
            transform=transform,
            direction=SortDirection.ASC,
            null_order=null_order,
        )
        return self

    def desc(self, source_column_name: str, transform: Transform[Any, Any], null_order: NullOrder) -> ReplaceSortOrder:
        self._builder.add_sort_field(
            source_id=self._column_name_to_id(source_column_name),
            transform=transform,
            direction=SortDirection.DESC,
            null_order=null_order,
        )
        return self

    def _apply(self) -> SortOrder:
        return self._builder.sort_order

    def _commit(self) -> UpdatesAndRequirements:
        """Apply the pending changes and commit."""
        new_sort_order = self._apply()
        requirements: Tuple[TableRequirement, ...] = ()
        updates: Tuple[TableUpdate, ...] = ()

        if self._transaction.table_metadata.default_sort_order_id != new_sort_order.order_id:
            updates = (AddSortOrderUpdate(sort_order=new_sort_order), SetDefaultSortOrderUpdate(sort_order_id=-1))
        else:
            updates = (SetDefaultSortOrderUpdate(sort_order_id=new_sort_order.order_id),)

        required_last_assigned_sort_order_id = self._transaction.table_metadata.default_sort_order_id
        requirements = (AssertDefaultSortOrderId(default_sort_order_id=required_last_assigned_sort_order_id),)

        return updates, requirements
