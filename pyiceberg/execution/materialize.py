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

"""Materialize in-memory Arrow data to temporary Parquet files.

This module bridges in-memory Arrow data (e.g., a user-provided pa.Table for
upsert, or streamed batches from sort-on-write) to the compute backend's
file-based methods. Writing to a temp Parquet file ensures:

1. The backend controls the read lifecycle (can sort/join with bounded memory)
2. Python memory is freed after the write (GC can reclaim the user's DataFrame)
3. A single uniform code path: all data enters the backend from file paths

The temp file uses the OS temp directory (same directory DataFusion uses for
spill files). Files are cleaned up via:
1. Primary: `finally` block in the context manager (guaranteed on normal/exception exit)
2. Secondary: `atexit` handler removes any tracked temp files on interpreter shutdown
3. Tertiary: OS temp directory periodic cleanup (system-level)

Performance: Writing 100 MB of Arrow data to local SSD takes ~14ms (NVMe at 7 GB/s).
This is negligible compared to the subsequent compute operation.
"""

from __future__ import annotations

import atexit
import tempfile
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import TYPE_CHECKING

import pyarrow as pa
import pyarrow.parquet as pq

if TYPE_CHECKING:
    from collections.abc import Generator

__all__ = ["materialize_batches_to_parquet", "materialize_to_parquet"]

# Track temp files for atexit cleanup.
# Protected by _temp_files_lock for free-threaded Python (PEP 703) safety.
import threading

_active_temp_files: set[str] = set()
_temp_files_lock = threading.Lock()


def _cleanup_remaining_temp_files() -> None:
    """Atexit handler: remove any temp files not yet cleaned up."""
    try:
        with _temp_files_lock:
            paths = list(_active_temp_files)
            _active_temp_files.clear()
        for path in paths:
            try:
                Path(path).unlink(missing_ok=True)
            except OSError:
                pass
    except (AttributeError, TypeError):
        # Suppress errors during interpreter shutdown (globals may be None).
        # AttributeError: _temp_files_lock or Path may be None.
        # TypeError: Path() may fail if pathlib module is being torn down.
        pass


atexit.register(_cleanup_remaining_temp_files)


@contextmanager
def materialize_to_parquet(table: pa.Table) -> Generator[str, None, None]:
    """Write an in-memory Arrow Table to a temporary Parquet file.

    Yields:
        Path to the temporary Parquet file.

    Example:
        >>> import pyarrow as pa
        >>> from pyiceberg.execution.materialize import materialize_to_parquet
        >>> user_df = pa.table({"id": [1, 2, 3], "value": ["a", "b", "c"]})
        >>> with materialize_to_parquet(user_df) as tmp_path:
        ...     pass  # file exists here
    """
    # Use NamedTemporaryFile for cross-platform temp path, then close immediately.
    # Pattern is intentional: we need the path for pq.write_table, and manually
    # control cleanup via try/finally.
    tmp_file = tempfile.NamedTemporaryFile(suffix=".parquet", delete=False)  # noqa: SIM115
    tmp_path = tmp_file.name
    tmp_file.close()

    with _temp_files_lock:
        _active_temp_files.add(tmp_path)
    try:
        pq.write_table(table, tmp_path)
        yield tmp_path
    finally:
        with _temp_files_lock:
            _active_temp_files.discard(tmp_path)
        Path(tmp_path).unlink(missing_ok=True)


@contextmanager
def materialize_batches_to_parquet(
    batches: Iterator[pa.RecordBatch],
    schema: pa.Schema,
) -> Generator[str, None, None]:
    """Write an iterator of RecordBatches to a temporary Parquet file.

    Yields:
        Path to the temporary Parquet file.
    """
    # Use NamedTemporaryFile for cross-platform temp path, then close immediately.
    # Pattern is intentional: we need the path for ParquetWriter, and manually
    # control cleanup via try/finally.
    tmp_file = tempfile.NamedTemporaryFile(suffix=".parquet", delete=False)  # noqa: SIM115
    tmp_path = tmp_file.name
    tmp_file.close()

    with _temp_files_lock:
        _active_temp_files.add(tmp_path)
    try:
        writer = pq.ParquetWriter(tmp_path, schema=schema)
        for batch in batches:
            if batch.num_rows > 0:
                writer.write_batch(batch)
        writer.close()
        yield tmp_path
    finally:
        with _temp_files_lock:
            _active_temp_files.discard(tmp_path)
        Path(tmp_path).unlink(missing_ok=True)
