import uuid
from typing import (
    Dict,
    List,
    Optional,
    Set,
    Union,
)

from pyiceberg.catalog import (
    Catalog,
    Identifier,
    Properties,
    PropertiesUpdateSummary,
)
from pyiceberg.exceptions import (
    NamespaceAlreadyExistsError,
    NamespaceNotEmptyError,
    NoSuchNamespaceError,
    NoSuchTableError,
    TableAlreadyExistsError,
)
from pyiceberg.io import WAREHOUSE
from pyiceberg.partitioning import UNPARTITIONED_PARTITION_SPEC, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table import (
    CommitTableRequest,
    CommitTableResponse,
    Table,
    update_table_metadata,
)
from pyiceberg.table.metadata import new_table_metadata
from pyiceberg.table.sorting import UNSORTED_SORT_ORDER, SortOrder
from pyiceberg.typedef import EMPTY_DICT

DEFAULT_WAREHOUSE_LOCATION = "file:///tmp/warehouse"


class InMemoryCatalog(Catalog):
    """An in-memory catalog implementation."""

    __tables: Dict[Identifier, Table]
    __namespaces: Dict[Identifier, Properties]

    def __init__(self, name: str, **properties: str) -> None:
        super().__init__(name, **properties)
        self.__tables = {}
        self.__namespaces = {}
        self._warehouse_location = properties.get(WAREHOUSE, None) or DEFAULT_WAREHOUSE_LOCATION

    def create_table(
        self,
        identifier: Union[str, Identifier],
        schema: Schema,
        location: Optional[str] = None,
        partition_spec: PartitionSpec = UNPARTITIONED_PARTITION_SPEC,
        sort_order: SortOrder = UNSORTED_SORT_ORDER,
        properties: Properties = EMPTY_DICT,
        table_uuid: Optional[uuid.UUID] = None,
    ) -> Table:
        identifier = Catalog.identifier_to_tuple(identifier)
        namespace = Catalog.namespace_from(identifier)

        if identifier in self.__tables:
            raise TableAlreadyExistsError(f"Table already exists: {identifier}")
        else:
            if namespace not in self.__namespaces:
                self.__namespaces[namespace] = {}

            if not location:
                location = f'{self._warehouse_location}/{"/".join(identifier)}'

            metadata_location = f'{self._warehouse_location}/{"/".join(identifier)}/metadata/metadata.json'

            metadata = new_table_metadata(
                schema=schema,
                partition_spec=partition_spec,
                sort_order=sort_order,
                location=location,
                properties=properties,
                table_uuid=table_uuid,
            )
            table = Table(
                identifier=identifier,
                metadata=metadata,
                metadata_location=metadata_location,
                io=self._load_file_io(properties=metadata.properties, location=metadata_location),
                catalog=self,
            )
            self.__tables[identifier] = table
            return table

    def register_table(self, identifier: Union[str, Identifier], metadata_location: str) -> Table:
        raise NotImplementedError

    def _commit_table(self, table_request: CommitTableRequest) -> CommitTableResponse:
        identifier_tuple = self.identifier_to_tuple_without_catalog(
            tuple(table_request.identifier.namespace.root + [table_request.identifier.name])
        )
        current_table = self.load_table(identifier_tuple)
        base_metadata = current_table.metadata

        for requirement in table_request.requirements:
            requirement.validate(base_metadata)

        updated_metadata = update_table_metadata(base_metadata, table_request.updates)
        if updated_metadata == base_metadata:
            # no changes, do nothing
            return CommitTableResponse(metadata=base_metadata, metadata_location=current_table.metadata_location)

        # write new metadata
        new_metadata_version = self._parse_metadata_version(current_table.metadata_location) + 1
        new_metadata_location = self._get_metadata_location(current_table.metadata.location, new_metadata_version)
        self._write_metadata(updated_metadata, current_table.io, new_metadata_location)

        # update table state
        current_table.metadata = updated_metadata

        return CommitTableResponse(metadata=updated_metadata, metadata_location=new_metadata_location)

    def load_table(self, identifier: Union[str, Identifier]) -> Table:
        identifier = self.identifier_to_tuple_without_catalog(identifier)
        try:
            return self.__tables[identifier]
        except KeyError as error:
            raise NoSuchTableError(f"Table does not exist: {identifier}") from error

    def drop_table(self, identifier: Union[str, Identifier]) -> None:
        identifier = self.identifier_to_tuple_without_catalog(identifier)
        try:
            self.__tables.pop(identifier)
        except KeyError as error:
            raise NoSuchTableError(f"Table does not exist: {identifier}") from error

    def purge_table(self, identifier: Union[str, Identifier]) -> None:
        self.drop_table(identifier)

    def rename_table(self, from_identifier: Union[str, Identifier], to_identifier: Union[str, Identifier]) -> Table:
        from_identifier = self.identifier_to_tuple_without_catalog(from_identifier)
        try:
            table = self.__tables.pop(from_identifier)
        except KeyError as error:
            raise NoSuchTableError(f"Table does not exist: {from_identifier}") from error

        to_identifier = Catalog.identifier_to_tuple(to_identifier)
        to_namespace = Catalog.namespace_from(to_identifier)
        if to_namespace not in self.__namespaces:
            self.__namespaces[to_namespace] = {}

        self.__tables[to_identifier] = Table(
            identifier=to_identifier,
            metadata=table.metadata,
            metadata_location=table.metadata_location,
            io=self._load_file_io(properties=table.metadata.properties, location=table.metadata_location),
            catalog=self,
        )
        return self.__tables[to_identifier]

    def create_namespace(self, namespace: Union[str, Identifier], properties: Properties = EMPTY_DICT) -> None:
        namespace = Catalog.identifier_to_tuple(namespace)
        if namespace in self.__namespaces:
            raise NamespaceAlreadyExistsError(f"Namespace already exists: {namespace}")
        else:
            self.__namespaces[namespace] = properties if properties else {}

    def drop_namespace(self, namespace: Union[str, Identifier]) -> None:
        namespace = Catalog.identifier_to_tuple(namespace)
        if [table_identifier for table_identifier in self.__tables.keys() if namespace == table_identifier[:-1]]:
            raise NamespaceNotEmptyError(f"Namespace is not empty: {namespace}")
        try:
            self.__namespaces.pop(namespace)
        except KeyError as error:
            raise NoSuchNamespaceError(f"Namespace does not exist: {namespace}") from error

    def list_tables(self, namespace: Optional[Union[str, Identifier]] = None) -> List[Identifier]:
        if namespace:
            namespace = Catalog.identifier_to_tuple(namespace)
            list_tables = [table_identifier for table_identifier in self.__tables.keys() if namespace == table_identifier[:-1]]
        else:
            list_tables = list(self.__tables.keys())

        return list_tables

    def list_namespaces(self, namespace: Union[str, Identifier] = ()) -> List[Identifier]:
        # Hierarchical namespace is not supported. Return an empty list
        if namespace:
            return []

        return list(self.__namespaces.keys())

    def load_namespace_properties(self, namespace: Union[str, Identifier]) -> Properties:
        namespace = Catalog.identifier_to_tuple(namespace)
        try:
            return self.__namespaces[namespace]
        except KeyError as error:
            raise NoSuchNamespaceError(f"Namespace does not exist: {namespace}") from error

    def update_namespace_properties(
        self, namespace: Union[str, Identifier], removals: Optional[Set[str]] = None, updates: Properties = EMPTY_DICT
    ) -> PropertiesUpdateSummary:
        removed: Set[str] = set()
        updated: Set[str] = set()

        namespace = Catalog.identifier_to_tuple(namespace)
        if namespace in self.__namespaces:
            if removals:
                for key in removals:
                    if key in self.__namespaces[namespace]:
                        del self.__namespaces[namespace][key]
                        removed.add(key)
            if updates:
                for key, value in updates.items():
                    self.__namespaces[namespace][key] = value
                    updated.add(key)
        else:
            raise NoSuchNamespaceError(f"Namespace does not exist: {namespace}")

        expected_to_change = removed.difference(removals or set())

        return PropertiesUpdateSummary(
            removed=list(removed or []), updated=list(updates.keys() if updates else []), missing=list(expected_to_change)
        )
