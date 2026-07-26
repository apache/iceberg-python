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

"""Sorted RecordBatchReader with lifecycle-managed temp file cleanup.

This module solves the lifecycle problem for sort-on-write:
- sort_from_files needs the temp Parquet file to exist during iteration
- The context manager (materialize_to_parquet) would delete it on exit
- _SortedRecordBatchReader enters the context manager, starts the sort,
  and wraps it in a RecordBatchReader that cleans up on exhaustion

Cleanup guarantees (multi-layer, ordered by priority):
1. Normal path: temp file deleted when iterator is fully consumed (else clause)
2. Exception path: temp file deleted via except clause
3. Abandoned reader (GC): _CleanupGuard.__del__ calls ctx_manager.__exit__()
4. Process exit: atexit handler in materialize.py cleans remaining tracked files
"""

from __future__ import annotations

from collections.abc import Callable, Iterator
from contextlib import AbstractContextManager
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import pyarrow as pa


class _SortedRecordBatchReader:
    """Factory for creating a streaming RecordBatchReader over sorted output."""

    @staticmethod
    def create(
        materialize_fn: Callable[[], AbstractContextManager[str]],
        sort_fn: Callable[[str], Iterator[pa.RecordBatch]],
        schema: pa.Schema,
    ) -> pa.RecordBatchReader:
        """Create a streaming RecordBatchReader for sorted output.

        The materialize_fn context manager owns the temp file lifetime;
        cleanup runs when iteration completes or an exception occurs.
        """
        import pyarrow as pa

        ctx_manager = materialize_fn()
        tmp_path = ctx_manager.__enter__()

        guard = _CleanupGuard(ctx_manager)

        def _sorted_batches_with_cleanup() -> Iterator:
            try:
                for batch in sort_fn(tmp_path):
                    if batch.schema != schema:
                        batch = batch.cast(schema)
                    yield batch
            except BaseException:
                import sys

                guard.cleanup(*sys.exc_info())
                raise
            else:
                guard.cleanup(None, None, None)

        return pa.RecordBatchReader.from_batches(schema, _sorted_batches_with_cleanup())


class _CleanupGuard:
    """Guard that ensures a context manager is exited even if the reader is abandoned."""

    __slots__ = ("_ctx_manager", "_cleaned_up", "_ref", "__weakref__")

    def __init__(self, ctx_manager: Any) -> None:
        import weakref

        self._ctx_manager = ctx_manager
        self._cleaned_up = False
        self._ref = weakref.finalize(self, _CleanupGuard._invoke_finalizer, ctx_manager)

    def cleanup(self, *exc_info: Any) -> None:
        """Release resources held by the context manager.

        Called from the generator's normal or exception path.
        """
        if not self._cleaned_up:
            self._cleaned_up = True
            self._ref.detach()
            self._ctx_manager.__exit__(*exc_info)

    @staticmethod
    def _invoke_finalizer(ctx_manager: Any) -> None:
        """Weakref finalizer callback for abandoned readers."""
        try:
            ctx_manager.__exit__(None, None, None)
        except Exception:
            pass
