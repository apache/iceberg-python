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
    "config_location, config_content, expected_result",
    [
        (
            "config",
            {"catalog": {"default": {"uri": "https://service.io/api"}}},
            {"catalog": {"default": {"uri": "https://service.io/api"}}},
        ),
        (
            "home",
            {"catalog": {"default": {"uri": "https://service.io/api"}}},
            {"catalog": {"default": {"uri": "https://service.io/api"}}},
        ),
        (
            "current",
            {"catalog": {"default": {"uri": "https://service.io/api"}}},
            {"catalog": {"default": {"uri": "https://service.io/api"}}},
        ),
        ("none", None, None),
    ],
)
def test_from_multiple_configuration_files(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path_factory: pytest.TempPathFactory,
    config_location: str,
    config_content: Optional[Dict[str, Any]],
    expected_result: Optional[Dict[str, Any]],
) -> None:
    def create_config_file(directory: str, content: Optional[Dict[str, Any]]) -> None:
        config_file_path = os.path.join(directory, ".pyiceberg.yaml")
        with open(config_file_path, "w", encoding="utf-8") as file:
            yaml_str = as_document(content).as_yaml() if content else ""
            file.write(yaml_str)

    config_path = str(tmp_path_factory.mktemp("config"))
    home_path = str(tmp_path_factory.mktemp("home"))
    current_path = str(tmp_path_factory.mktemp("current"))

    location_to_path = {
        "config": config_path,
        "home": home_path,
        "current": current_path,
    }

    if config_location in location_to_path and config_content:
        create_config_file(location_to_path[config_location], config_content)

    monkeypatch.setenv("PYICEBERG_HOME", config_path)
    monkeypatch.setattr(os.path, "expanduser", lambda _: home_path)

    if config_location == "current":
        monkeypatch.chdir(current_path)

    assert Config()._from_configuration_files() == expected_result, (
        f"Unexpected configuration result for content: {config_content}"
    )
