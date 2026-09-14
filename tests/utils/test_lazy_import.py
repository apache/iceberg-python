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
import importlib

import pytest

from pyiceberg.exceptions import NotInstalledError
from pyiceberg.utils.lazy_import import not_installed, try_import


def test_try_import_returns_the_module() -> None:
    assert try_import("importlib") is importlib


def test_try_import_missing_module_with_extras() -> None:
    with pytest.raises(NotInstalledError, match=r'nonexistent needs to be installed. pip install "pyiceberg\[some-extra\]"'):
        try_import("nonexistent", extras_name="some-extra")


def test_try_import_missing_module_without_extras() -> None:
    with pytest.raises(NotInstalledError, match="nonexistent needs to be installed."):
        try_import("nonexistent")


def test_not_installed_returns_rather_than_raises() -> None:
    assert isinstance(not_installed("nonexistent", extras_name="some-extra"), NotInstalledError)
