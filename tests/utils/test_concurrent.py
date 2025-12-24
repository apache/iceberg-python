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

import multiprocessing
import os
from collections.abc import Generator
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from unittest import mock

import pytest

from pyiceberg.utils.concurrent import ExecutorFactory

EMPTY_ENV: dict[str, str | None] = {}
VALID_ENV = {"PYICEBERG_MAX_WORKERS": "5"}
INVALID_ENV = {"PYICEBERG_MAX_WORKERS": "invalid"}


@pytest.fixture
def fork_process() -> Generator[None, None, None]:
    original = multiprocessing.get_start_method()
    allowed = multiprocessing.get_all_start_methods()

    assert "fork" in allowed

    multiprocessing.set_start_method("fork", force=True)

    yield

    multiprocessing.set_start_method(original, force=True)


@pytest.fixture
def spawn_process() -> Generator[None, None, None]:
    original = multiprocessing.get_start_method()
    allowed = multiprocessing.get_all_start_methods()

    assert "spawn" in allowed

    multiprocessing.set_start_method("spawn", force=True)

    yield

    multiprocessing.set_start_method(original, force=True)


def _use_executor_to_return(value: int) -> int:
    # Module level function to enabling pickling for use with ProcessPoolExecutor.
    executor = ExecutorFactory.get_or_create()
    future = executor.submit(lambda: value)
    return future.result()


def test_create_reused() -> None:
    first = ExecutorFactory.get_or_create()
    second = ExecutorFactory.get_or_create()
    assert isinstance(first, ThreadPoolExecutor)
    assert first is second


@mock.patch.dict(os.environ, EMPTY_ENV)
def test_max_workers_none() -> None:
    assert ExecutorFactory.max_workers() is None


@mock.patch.dict(os.environ, VALID_ENV)
def test_max_workers() -> None:
    assert ExecutorFactory.max_workers() == 5


@mock.patch.dict(os.environ, INVALID_ENV)
def test_max_workers_invalid() -> None:
    with pytest.raises(ValueError):
        ExecutorFactory.max_workers()


@pytest.mark.parametrize(
    "fixture_name",
    [
        pytest.param(
            "fork_process",
            marks=pytest.mark.skipif(
                "fork" not in multiprocessing.get_all_start_methods(), reason="Fork start method is not available"
            ),
        ),
        pytest.param(
            "spawn_process",
            marks=pytest.mark.skipif(
                "spawn" not in multiprocessing.get_all_start_methods(), reason="Spawn start method is not available"
            ),
        ),
    ],
)
def test_use_executor_in_different_process(fixture_name: str, request: pytest.FixtureRequest) -> None:
    # Use the fixture, which sets up fork or spawn process start method.
    request.getfixturevalue(fixture_name)

    # Use executor in main process to ensure the singleton is initialized.
    main_value = _use_executor_to_return(10)

    # Use two separate ProcessPoolExecutors to ensure different processes are used.
    with ProcessPoolExecutor() as process_executor:
        future1 = process_executor.submit(_use_executor_to_return, 20)
    with ProcessPoolExecutor() as process_executor:
        future2 = process_executor.submit(_use_executor_to_return, 30)

    assert main_value == 10
    assert future1.result() == 20
    assert future2.result() == 30
