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
from typing import Any, Dict, Optional
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
        # Validate lookup works with: config > home > cwd
        (
            {"config_location": "config", "config_content": {"catalog": {"default": {"uri": "https://service.io/api"}}}},
            {"catalog": {"default": {"uri": "https://service.io/api"}}},
        ),
        (
            {"config_location": "home", "config_content": {"catalog": {"default": {"uri": "https://service.io/api"}}}},
            {"catalog": {"default": {"uri": "https://service.io/api"}}},
        ),
        (
            {"config_location": "current", "config_content": {"catalog": {"default": {"uri": "https://service.io/api"}}}},
            {"catalog": {"default": {"uri": "https://service.io/api"}}},
        ),
        (
            {"config_location": "none", "config_content": None},
            None,
        ),
        # Validate lookup order: home > cwd if present in both
        (
            {
                "config_location": "both",
                "home_content": {"catalog": {"default": {"uri": "https://service.io/home"}}},
                "current_content": {"catalog": {"default": {"uri": "https://service.io/current"}}},
            },
            {"catalog": {"default": {"uri": "https://service.io/home"}}},
        ),
    ],
)
def test_from_multiple_configuration_files(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path_factory: pytest.TempPathFactory,
    config_setup: Dict[str, Any],
    expected_result: Optional[Dict[str, Any]],
) -> None:
    def create_config_files(
        paths: Dict[str, str],
        contents: Dict[str, Optional[Dict[str, Any]]],
    ) -> None:
        """Helper to create configuration files in specified paths."""
        for location, content in contents.items():
            if content:
                config_file_path = os.path.join(paths[location], ".pyiceberg.yaml")
                with open(config_file_path, "w", encoding="UTF8") as file:
                    yaml_str = as_document(content).as_yaml() if content else ""
                    file.write(yaml_str)

    paths = {
        "config": str(tmp_path_factory.mktemp("config")),
        "home": str(tmp_path_factory.mktemp("home")),
        "current": str(tmp_path_factory.mktemp("current")),
    }

    contents = {
        "config": config_setup.get("config_content") if config_setup.get("config_location") == "config" else None,
        "home": config_setup.get("home_content") if config_setup.get("config_location") in ["home", "both"] else None,
        "current": config_setup.get("current_content") if config_setup.get("config_location") in ["current", "both"] else None,
    }

    create_config_files(paths, contents)

    monkeypatch.setenv("PYICEBERG_HOME", paths["config"])
    monkeypatch.setattr(os.path, "expanduser", lambda _: paths["home"])
    if config_setup.get("config_location") in ["current", "both"]:
        monkeypatch.chdir(paths["current"])

    assert Config()._from_configuration_files() == expected_result, (
        f"Unexpected configuration result for content: {expected_result}"
    )
