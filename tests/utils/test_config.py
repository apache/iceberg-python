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
import os
from typing import Any
from unittest import mock

import pytest
from strictyaml import as_document

from pyiceberg.typedef import UTF8, RecursiveDict
from pyiceberg.utils.config import Config, _lowercase_dictionary_keys, merge_config

EXAMPLE_ENV = {"PYICEBERG_CATALOG__PRODUCTION__URI": "https://service.io/api"}


def test_config() -> None:
    """To check if all the file lookups go well without any mocking"""
    assert Config()


@mock.patch.dict(os.environ, EXAMPLE_ENV)
def test_from_environment_variables() -> None:
    assert Config().get_catalog_config("production") == {"uri": "https://service.io/api"}


@mock.patch.dict(os.environ, EXAMPLE_ENV)
def test_from_environment_variables_uppercase() -> None:
    assert Config().get_catalog_config("PRODUCTION") == {"uri": "https://service.io/api"}


@mock.patch.dict(
    os.environ,
    {
        "PYICEBERG_CATALOG__PRODUCTION__S3__REGION": "eu-north-1",
        "PYICEBERG_CATALOG__PRODUCTION__S3__ACCESS_KEY_ID": "username",
    },
)
def test_fix_nested_objects_from_environment_variables() -> None:
    assert Config().get_catalog_config("PRODUCTION") == {
        "s3.region": "eu-north-1",
        "s3.access-key-id": "username",
    }


@mock.patch.dict(os.environ, EXAMPLE_ENV)
@mock.patch.dict(os.environ, {"PYICEBERG_CATALOG__DEVELOPMENT__URI": "https://dev.service.io/api"})
def test_list_all_known_catalogs() -> None:
    catalogs = Config().get_known_catalogs()
    assert "production" in catalogs
    assert "development" in catalogs


def test_from_configuration_files(tmp_path_factory: pytest.TempPathFactory) -> None:
    config_path = str(tmp_path_factory.mktemp("config"))
    with open(f"{config_path}/.pyiceberg.yaml", "w", encoding=UTF8) as file:
        yaml_str = as_document({"catalog": {"production": {"uri": "https://service.io/api"}}}).as_yaml()
        file.write(yaml_str)

    os.environ["PYICEBERG_HOME"] = config_path
    assert Config().get_catalog_config("production") == {"uri": "https://service.io/api"}


def test_lowercase_dictionary_keys() -> None:
    uppercase_keys = {"UPPER": {"NESTED_UPPER": {"YES"}}}
    expected = {"upper": {"nested_upper": {"YES"}}}
    assert _lowercase_dictionary_keys(uppercase_keys) == expected  # type: ignore


def test_merge_config() -> None:
    lhs: RecursiveDict = {"common_key": "abc123"}
    rhs: RecursiveDict = {"common_key": "xyz789"}
    result = merge_config(lhs, rhs)
    assert result["common_key"] == rhs["common_key"]


def test_from_configuration_files_get_typed_value(tmp_path_factory: pytest.TempPathFactory) -> None:
    config_path = str(tmp_path_factory.mktemp("config"))
    with open(f"{config_path}/.pyiceberg.yaml", "w", encoding=UTF8) as file:
        yaml_str = as_document({"max-workers": "4", "legacy-current-snapshot-id": "True"}).as_yaml()
        file.write(yaml_str)

    os.environ["PYICEBERG_HOME"] = config_path
    with pytest.raises(ValueError):
        Config().get_bool("max-workers")

    with pytest.raises(ValueError):
        Config().get_int("legacy-current-snapshot-id")

    assert Config().get_bool("legacy-current-snapshot-id")
    assert Config().get_int("max-workers") == 4


@pytest.mark.parametrize(
    "config_setup, expected_result",
    [
        # PYICEBERG_HOME takes precedence
        (
            {
                "pyiceberg_home_content": "https://service.io/pyiceberg_home",
                "home_content": "https://service.io/user-home",
                "cwd_content": "https://service.io/cwd",
            },
            "https://service.io/pyiceberg_home",
        ),
        # Home directory (~) is checked after PYICEBERG_HOME
        (
            {
                "pyiceberg_home_content": None,
                "home_content": "https://service.io/user-home",
                "cwd_content": "https://service.io/cwd",
            },
            "https://service.io/user-home",
        ),
        # Current working directory (.) is the last fallback
        (
            {
                "pyiceberg_home_content": None,
                "home_content": None,
                "cwd_content": "https://service.io/cwd",
            },
            "https://service.io/cwd",
        ),
        # No configuration files found
        (
            {
                "pyiceberg_home_content": None,
                "home_content": None,
                "cwd_content": None,
            },
            None,
        ),
    ],
)
def test_config_lookup_order(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path_factory: pytest.TempPathFactory,
    config_setup: dict[str, Any],
    expected_result: str | None,
) -> None:
    """
    Test that the configuration lookup prioritizes PYICEBERG_HOME, then home (~), then cwd.
    """

    def create_config_file(path: str, uri: str | None) -> None:
        if uri:
            config_file_path = os.path.join(path, ".pyiceberg.yaml")
            content = {"catalog": {"default": {"uri": uri}}}
            with open(config_file_path, "w", encoding="utf-8") as file:
                yaml_str = as_document(content).as_yaml()
                file.write(yaml_str)

    # Create temporary directories for PYICEBERG_HOME, home (~), and cwd
    pyiceberg_home = str(tmp_path_factory.mktemp("pyiceberg_home"))
    home_dir = str(tmp_path_factory.mktemp("home"))
    cwd_dir = str(tmp_path_factory.mktemp("cwd"))

    # Create configuration files in the respective directories
    create_config_file(pyiceberg_home, config_setup.get("pyiceberg_home_content"))
    create_config_file(home_dir, config_setup.get("home_content"))
    create_config_file(cwd_dir, config_setup.get("cwd_content"))

    # Mock environment and paths
    monkeypatch.setenv("PYICEBERG_HOME", pyiceberg_home)
    monkeypatch.setattr(os.path, "expanduser", lambda _: home_dir)
    monkeypatch.chdir(cwd_dir)

    # Perform the lookup and validate the result
    result = Config()._from_configuration_files()
    assert (
        result["catalog"]["default"]["uri"] if result else None  # type: ignore
    ) == expected_result, f"Unexpected configuration result. Expected: {expected_result}, Actual: {result}"
