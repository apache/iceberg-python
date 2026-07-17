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

"""Execution backend implementations (internal).

This package is PRIVATE -- import backends from pyiceberg.execution.protocol
(the public API) or use build_backends() from pyiceberg.execution.engine.

Direct imports from this package (e.g., from pyiceberg.execution.backends.pyarrow_backend
import PyArrowReadBackend) are for internal use and testing only. These classes may
be renamed, reorganized, or removed without notice between minor versions.

Available backends:
- pyarrow_backend: Always available (default fallback, in-memory only)
- datafusion_backend: Bounded-memory compute via spill-to-disk (optional)
"""

from __future__ import annotations

# Empty __all__ indicates this is a private package -- nothing is re-exported.
# Use pyiceberg.execution.engine.build_backends() for backend construction.
__all__: list[str] = []
