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

from pyiceberg.catalog import Catalog, _raise_if_view_exists, load_catalog
from pyiceberg.catalog.memory import InMemoryCatalog
from pyiceberg.exceptions import TableAlreadyExistsError
from pyiceberg.io import WAREHOUSE
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, StringType


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


def test_catalog_repr(catalog: InMemoryCatalog) -> None:
    s = repr(catalog)
    assert s == "test.in_memory.catalog (<class 'pyiceberg.catalog.memory.InMemoryCatalog'>)"


class _StubCatalog:
    def __init__(self, *, returns: bool | None = None, raises: type[Exception] | None = None) -> None:
        self._returns = returns
        self._raises = raises
        self.calls: list[object] = []

    def view_exists(self, identifier: object) -> bool:
        self.calls.append(identifier)
        if self._raises is not None:
            raise self._raises
        assert self._returns is not None
        return self._returns


class TestRaiseIfViewExists:
    def test_raises_when_view_exists(self) -> None:
        stub = _StubCatalog(returns=True)
        with pytest.raises(TableAlreadyExistsError, match="View with same name already exists: ns.t"):
            _raise_if_view_exists(stub, "ns.t")  # type: ignore[arg-type]
        assert stub.calls == ["ns.t"]

    def test_no_raise_when_view_absent(self) -> None:
        stub = _StubCatalog(returns=False)
        _raise_if_view_exists(stub, ("ns", "t"))  # type: ignore[arg-type]
        assert stub.calls == [("ns", "t")]

    def test_not_implemented_treated_as_no_view(self) -> None:
        stub = _StubCatalog(raises=NotImplementedError)
        _raise_if_view_exists(stub, "ns.t")  # type: ignore[arg-type]
        assert stub.calls == ["ns.t"]


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
