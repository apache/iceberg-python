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

"""Shared fixtures for execution backend tests."""

from __future__ import annotations

import os

import pytest


@pytest.fixture(autouse=True)
def clear_engine_detection_cache() -> None:
    """Clear the engine detection and config caches before and after each test.

    _detect_available_engines and _read_execution_section_from_file are decorated
    with @lru_cache(maxsize=1). Without clearing, tests that mock imports or write
    config files may see stale results from a previous test's cache population.
    """
    from pyiceberg.execution.engine import (
        _detect_available_engines,
        _read_execution_section_from_file,
    )

    _detect_available_engines.cache_clear()
    _read_execution_section_from_file.cache_clear()
    yield
    _detect_available_engines.cache_clear()
    _read_execution_section_from_file.cache_clear()


@pytest.fixture(autouse=True)
def isolate_from_filesystem_config(monkeypatch, tmp_path) -> None:
    """Isolate tests from user's .pyiceberg.yaml configuration.

    Without this fixture, a developer who has execution.compute-backend set in
    their .pyiceberg.yaml would see test failures because Config() reads filesystem
    state. This fixture:
    1. Removes PYICEBERG_EXECUTION__* env vars (clean env slate)
    2. Sets PYICEBERG_HOME to a fresh temp dir (no .pyiceberg.yaml to find)

    Tests that explicitly set env vars via patch.dict() will still work because
    patch.dict operates on top of this clean slate.
    """
    # Remove any PYICEBERG_EXECUTION__* env vars
    for key in list(os.environ.keys()):
        if key.startswith("PYICEBERG_EXECUTION__"):
            monkeypatch.delenv(key, raising=False)

    # Point PYICEBERG_HOME to a temp dir so Config() won't find any .pyiceberg.yaml
    monkeypatch.setenv("PYICEBERG_HOME", str(tmp_path))
    yield
