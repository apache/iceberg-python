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

"""Pluggable execution backend for PyIceberg.

Provides independently configurable read, write, and compute backends.
The architecture separates Iceberg semantics (scan planning, commits) from
data execution (read, write, sort, join, filter), allowing different engines
to handle each axis while PyIceberg retains ownership of spec logic.
"""

from __future__ import annotations

from pyiceberg.execution.engine import ExecutionEngine, build_backends, clear_config_cache, resolve_backends
from pyiceberg.execution.protocol import (
    Backends,
    ComputeBackend,
    ReadBackend,
    SortKey,
    SortKeyList,
    WriteBackend,
)

__all__ = [
    "Backends",
    "ComputeBackend",
    "ExecutionEngine",
    "ReadBackend",
    "SortKey",
    "SortKeyList",
    "WriteBackend",
    "build_backends",
    "clear_config_cache",
    "resolve_backends",
]
