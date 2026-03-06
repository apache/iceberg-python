#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.
# pylint:disable=redefined-outer-name

from collections.abc import Generator
from pathlib import PosixPath

import pytest
from pytest_mock import MockFixture

from pyiceberg.catalog import Catalog, _get_env_config, load_catalog
from pyiceberg.catalog.memory import InMemoryCatalog
from pyiceberg.io import WAREHOUSE
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, StringType
from pyiceberg.utils.config import Config


@pytest.fixture
def catalog(tmp_path: PosixPath) -> Generator[Catalog, None, None]:
    catalog = InMemoryCatalog("test.in_memory.catalog", **{WAREHOUSE: tmp_path.absolute().as_posix(), "test.key": "test.value"})
    yield catalog
    catalog.close()


def test_load_catalog_in_memory() -> None:
    assert load_catalog("catalog", type="in-memory")


def test_load_catalog_impl_not_full_path() -> None:
    with pytest.raises(ValueError) as exc_info:
        load_catalog("catalog", **{"py-catalog-impl": "CustomCatalog"})

    assert "py-catalog-impl should be full path (module.CustomCatalog), got: CustomCatalog" in str(exc_info.value)


def test_load_catalog_impl_does_not_exist() -> None:
    with pytest.raises(ValueError) as exc_info:
        load_catalog("catalog", **{"py-catalog-impl": "pyiceberg.does.not.exist.Catalog"})

    assert "Could not initialize Catalog: pyiceberg.does.not.exist.Catalog" in str(exc_info.value)


def test_load_catalog_has_type_and_impl() -> None:
    with pytest.raises(ValueError) as exc_info:
        load_catalog("catalog", **{"py-catalog-impl": "pyiceberg.does.not.exist.Catalog", "type": "sql"})

    assert (
        "Must not set both catalog type and py-catalog-impl configurations, "
        "but found type sql and py-catalog-impl pyiceberg.does.not.exist.Catalog" in str(exc_info.value)
    )


def test_get_env_config_is_lazy_and_cached(mocker: MockFixture) -> None:
    original_config = _get_env_config()
    _get_env_config.cache_clear()
    config = Config({"catalog": {"test": {"type": "in-memory"}}})
    load_mock = mocker.patch("pyiceberg.catalog.Config.load", return_value=config)
    assert _get_env_config() is config
    assert _get_env_config() is config
    load_mock.assert_called_once()

    _get_env_config.cache_clear()
    mocker.patch("pyiceberg.catalog.Config.load", return_value=original_config)
    assert _get_env_config() is original_config


def test_catalog_repr(catalog: InMemoryCatalog) -> None:
    s = repr(catalog)
    assert s == "test.in_memory.catalog (<class 'pyiceberg.catalog.memory.InMemoryCatalog'>)"


class TestCatalogClose:
    """Test catalog close functionality."""

    def test_in_memory_catalog_close(self, catalog: InMemoryCatalog) -> None:
        """Test that InMemoryCatalog close method works."""
        # Should not raise any exception
        catalog.close()

    def test_in_memory_catalog_context_manager(self, catalog: InMemoryCatalog) -> None:
        """Test that InMemoryCatalog works as a context manager."""
        with InMemoryCatalog("test") as cat:
            assert cat.name == "test"
            # Create a namespace and table to test functionality
            cat.create_namespace("test_db")
            schema = Schema(NestedField(1, "name", StringType(), required=True))
            cat.create_table(("test_db", "test_table"), schema)

        # InMemoryCatalog inherits close from SqlCatalog, so engine should be disposed
        assert hasattr(cat, "engine")
