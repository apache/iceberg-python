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
"""Helpers for importing modules that ship in an optional extra."""

from __future__ import annotations

import importlib
import types

from pyiceberg.exceptions import NotInstalledError


def try_import(module_name: str, extras_name: str | None = None) -> types.ModuleType:
    """Import `module_name`, raising `NotInstalledError` with an install hint when it is missing.

    Args:
        module_name (str): The module to import.
        extras_name (str | None): The pyiceberg extra that provides it, if any.
    """
    try:
        return importlib.import_module(module_name)
    except ImportError:
        if extras_name:
            msg = f'{module_name} needs to be installed. pip install "pyiceberg[{extras_name}]"'
        else:
            msg = f"{module_name} needs to be installed."
        raise NotInstalledError(msg) from None
