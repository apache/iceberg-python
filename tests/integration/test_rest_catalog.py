import pytest

from pyiceberg.catalog.rest import RestCatalog

TEST_NAMESPACE_IDENTIFIER = "TEST NS"


@pytest.mark.integration
@pytest.mark.parametrize("catalog", [pytest.lazy_fixture("session_catalog")])
def test_namespace_exists(catalog: RestCatalog) -> None:
    if not catalog.namespace_exists(TEST_NAMESPACE_IDENTIFIER):
        catalog.create_namespace(TEST_NAMESPACE_IDENTIFIER)

    assert catalog.namespace_exists(TEST_NAMESPACE_IDENTIFIER)


@pytest.mark.integration
@pytest.mark.parametrize("catalog", [pytest.lazy_fixture("session_catalog")])
def test_namespace_not_exists(catalog: RestCatalog) -> None:
    if catalog.namespace_exists(TEST_NAMESPACE_IDENTIFIER):
        catalog.drop_namespace(TEST_NAMESPACE_IDENTIFIER)

    assert not catalog.namespace_exists(TEST_NAMESPACE_IDENTIFIER)


@pytest.mark.integration
@pytest.mark.parametrize("catalog", [pytest.lazy_fixture("session_catalog")])
def test_create_namespace_if_not_exists(catalog: RestCatalog) -> None:
    if catalog.namespace_exists(TEST_NAMESPACE_IDENTIFIER):
        catalog.drop_namespace(TEST_NAMESPACE_IDENTIFIER)

    catalog.create_namespace_if_not_exists(TEST_NAMESPACE_IDENTIFIER)

    assert catalog.namespace_exists(TEST_NAMESPACE_IDENTIFIER)


@pytest.mark.integration
@pytest.mark.parametrize("catalog", [pytest.lazy_fixture("session_catalog")])
def test_create_namespace_if_already_existing(catalog: RestCatalog) -> None:
    if not catalog.namespace_exists(TEST_NAMESPACE_IDENTIFIER):
        catalog.create_namespace(TEST_NAMESPACE_IDENTIFIER)

    catalog.create_namespace_if_not_exists(TEST_NAMESPACE_IDENTIFIER)

    assert catalog.namespace_exists(TEST_NAMESPACE_IDENTIFIER)
