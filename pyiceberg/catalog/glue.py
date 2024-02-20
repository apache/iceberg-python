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


from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    List,
    Optional,
    Set,
    Union,
    cast,
)

import boto3
from mypy_boto3_glue.client import GlueClient
from mypy_boto3_glue.type_defs import (
    ColumnTypeDef,
    DatabaseInputTypeDef,
    DatabaseTypeDef,
    StorageDescriptorTypeDef,
    TableInputTypeDef,
    TableTypeDef,
)

from pyiceberg.catalog import (
    EXTERNAL_TABLE,
    ICEBERG,
    LOCATION,
    METADATA_LOCATION,
    PREVIOUS_METADATA_LOCATION,
    TABLE_TYPE,
    Catalog,
    Identifier,
    Properties,
    PropertiesUpdateSummary,
)
from pyiceberg.exceptions import (
    CommitFailedException,
    NamespaceAlreadyExistsError,
    NamespaceNotEmptyError,
    NoSuchIcebergTableError,
    NoSuchNamespaceError,
    NoSuchPropertyException,
    NoSuchTableError,
    TableAlreadyExistsError,
)
from pyiceberg.io import load_file_io
from pyiceberg.partitioning import UNPARTITIONED_PARTITION_SPEC, PartitionSpec
from pyiceberg.schema import Schema, SchemaVisitor, visit
from pyiceberg.serializers import FromInputFile
from pyiceberg.table import CommitTableRequest, CommitTableResponse, Table, update_table_metadata
from pyiceberg.table.metadata import TableMetadata, new_table_metadata
from pyiceberg.table.sorting import UNSORTED_SORT_ORDER, SortOrder
from pyiceberg.typedef import EMPTY_DICT
from pyiceberg.types import (
    BinaryType,
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    FixedType,
    FloatType,
    IntegerType,
    ListType,
    LongType,
    MapType,
    NestedField,
    PrimitiveType,
    StringType,
    StructType,
    TimestampType,
    TimestamptzType,
    TimeType,
    UUIDType,
)

if TYPE_CHECKING:
    import pyarrow as pa

# If Glue should skip archiving an old table version when creating a new version in a commit. By
# default, Glue archives all old table versions after an UpdateTable call, but Glue has a default
# max number of archived table versions (can be increased). So for streaming use case with lots
# of commits, it is recommended to set this value to true.
GLUE_SKIP_ARCHIVE = "glue.skip-archive"
GLUE_SKIP_ARCHIVE_DEFAULT = True

ICEBERG_FIELD_ID = "iceberg.field.id"
ICEBERG_FIELD_OPTIONAL = "iceberg.field.optional"
ICEBERG_FIELD_CURRENT = "iceberg.field.current"


def _construct_parameters(
    metadata_location: str, glue_table: Optional[TableTypeDef] = None, prev_metadata_location: Optional[str] = None
) -> Properties:
    new_parameters = glue_table.get("Parameters", {}) if glue_table else {}
    new_parameters.update({TABLE_TYPE: ICEBERG.upper(), METADATA_LOCATION: metadata_location})
    if prev_metadata_location:
        new_parameters[PREVIOUS_METADATA_LOCATION] = prev_metadata_location
    return new_parameters


GLUE_PRIMITIVE_TYPES = {
    BooleanType: "boolean",
    IntegerType: "int",
    LongType: "bigint",
    FloatType: "float",
    DoubleType: "double",
    DateType: "date",
    TimeType: "string",
    StringType: "string",
    UUIDType: "string",
    TimestampType: "timestamp",
    TimestamptzType: "timestamp",
    FixedType: "binary",
    BinaryType: "binary",
}


class _IcebergSchemaToGlueType(SchemaVisitor[str]):
    def schema(self, schema: Schema, struct_result: str) -> str:
        return struct_result

    def struct(self, struct: StructType, field_results: List[str]) -> str:
        return f"struct<{','.join(field_results)}>"

    def field(self, field: NestedField, field_result: str) -> str:
        return f"{field.name}:{field_result}"

    def list(self, list_type: ListType, element_result: str) -> str:
        return f"array<{element_result}>"

    def map(self, map_type: MapType, key_result: str, value_result: str) -> str:
        return f"map<{key_result},{value_result}>"

    def primitive(self, primitive: PrimitiveType) -> str:
        if isinstance(primitive, DecimalType):
            return f"decimal({primitive.precision},{primitive.scale})"
        if (primitive_type := type(primitive)) not in GLUE_PRIMITIVE_TYPES:
            return str(primitive_type.root)
        return GLUE_PRIMITIVE_TYPES[primitive_type]


def _to_columns(metadata: TableMetadata) -> List[ColumnTypeDef]:
    results: Dict[str, ColumnTypeDef] = {}

    def _append_to_results(field: NestedField, is_current: bool) -> None:
        if field.name in results:
            return

        results[field.name] = cast(
            ColumnTypeDef,
            {
                "Name": field.name,
                "Type": visit(field.field_type, _IcebergSchemaToGlueType()),
                "Parameters": {
                    ICEBERG_FIELD_ID: str(field.field_id),
                    ICEBERG_FIELD_OPTIONAL: str(field.optional).lower(),
                    ICEBERG_FIELD_CURRENT: str(is_current).lower(),
                },
            },
        )
        if field.doc:
            results[field.name]["Comment"] = field.doc

    if current_schema := metadata.schema_by_id(metadata.current_schema_id):
        for field in current_schema.columns:
            _append_to_results(field, True)

    for schema in metadata.schemas:
        if schema.schema_id == metadata.current_schema_id:
            continue
        for field in schema.columns:
            _append_to_results(field, False)

    return list(results.values())


def _construct_table_input(
    table_name: str,
    metadata_location: str,
    properties: Properties,
    metadata: TableMetadata,
    glue_table: Optional[TableTypeDef] = None,
    prev_metadata_location: Optional[str] = None,
) -> TableInputTypeDef:
    table_input: TableInputTypeDef = {
        "Name": table_name,
        "TableType": EXTERNAL_TABLE,
        "Parameters": _construct_parameters(metadata_location, glue_table, prev_metadata_location),
        "StorageDescriptor": {
            "Columns": _to_columns(metadata),
            "Location": metadata.location,
        },
    }

    if "Description" in properties:
        table_input["Description"] = properties["Description"]

    return table_input


def _construct_rename_table_input(to_table_name: str, glue_table: TableTypeDef) -> TableInputTypeDef:
    rename_table_input: TableInputTypeDef = {"Name": to_table_name}
    # use the same Glue info to create the new table, pointing to the old metadata
    assert glue_table["TableType"]
    rename_table_input["TableType"] = glue_table["TableType"]
    if "Owner" in glue_table:
        rename_table_input["Owner"] = glue_table["Owner"]

    if "Parameters" in glue_table:
        rename_table_input["Parameters"] = glue_table["Parameters"]

    if "StorageDescriptor" in glue_table:
        # It turns out the output of StorageDescriptor is not the same as the input type
        # because the Column can have a different type, but for now it seems to work, so
        # silence the type error.
        rename_table_input["StorageDescriptor"] = cast(StorageDescriptorTypeDef, glue_table["StorageDescriptor"])

    if "Description" in glue_table:
        rename_table_input["Description"] = glue_table["Description"]

    return rename_table_input


def _construct_database_input(database_name: str, properties: Properties) -> DatabaseInputTypeDef:
    database_input: DatabaseInputTypeDef = {"Name": database_name}
    parameters = {}
    for k, v in properties.items():
        if k == "Description":
            database_input["Description"] = v
        elif k == LOCATION:
            database_input["LocationUri"] = v
        else:
            parameters[k] = v
    database_input["Parameters"] = parameters
    return database_input


class GlueCatalog(Catalog):
    def __init__(self, name: str, **properties: Any):
        super().__init__(name, **properties)

        session = boto3.Session(
            profile_name=properties.get("profile_name"),
            region_name=properties.get("region_name"),
            botocore_session=properties.get("botocore_session"),
            aws_access_key_id=properties.get("aws_access_key_id"),
            aws_secret_access_key=properties.get("aws_secret_access_key"),
            aws_session_token=properties.get("aws_session_token"),
        )
        self.glue: GlueClient = session.client("glue")

    def _convert_glue_to_iceberg(self, glue_table: TableTypeDef) -> Table:
        properties: Properties = glue_table["Parameters"]

        assert glue_table["DatabaseName"]
        assert glue_table["Parameters"]
        database_name = glue_table["DatabaseName"]
        table_name = glue_table["Name"]

        if TABLE_TYPE not in properties:
            raise NoSuchPropertyException(
                f"Property {TABLE_TYPE} missing, could not determine type: {database_name}.{table_name}"
            )
        glue_table_type = properties[TABLE_TYPE]

        if glue_table_type.lower() != ICEBERG:
            raise NoSuchIcebergTableError(
                f"Property table_type is {glue_table_type}, expected {ICEBERG}: {database_name}.{table_name}"
            )

        if METADATA_LOCATION not in properties:
            raise NoSuchPropertyException(
                f"Table property {METADATA_LOCATION} is missing, cannot find metadata for: {database_name}.{table_name}"
            )
        metadata_location = properties[METADATA_LOCATION]

        io = load_file_io(properties=self.properties, location=metadata_location)
        file = io.new_input(metadata_location)
        metadata = FromInputFile.table_metadata(file)
        return Table(
            identifier=(self.name, database_name, table_name),
            metadata=metadata,
            metadata_location=metadata_location,
            io=self._load_file_io(metadata.properties, metadata_location),
            catalog=self,
        )

    def _create_glue_table(self, database_name: str, table_name: str, table_input: TableInputTypeDef) -> None:
        try:
            self.glue.create_table(DatabaseName=database_name, TableInput=table_input)
        except self.glue.exceptions.AlreadyExistsException as e:
            raise TableAlreadyExistsError(f"Table {database_name}.{table_name} already exists") from e
        except self.glue.exceptions.EntityNotFoundException as e:
            raise NoSuchNamespaceError(f"Database {database_name} does not exist") from e

    def _update_glue_table(self, database_name: str, table_name: str, table_input: TableInputTypeDef, version_id: str) -> None:
        try:
            self.glue.update_table(
                DatabaseName=database_name,
                TableInput=table_input,
                SkipArchive=self.properties.get(GLUE_SKIP_ARCHIVE, GLUE_SKIP_ARCHIVE_DEFAULT),
                VersionId=version_id,
            )
        except self.glue.exceptions.EntityNotFoundException as e:
            raise NoSuchTableError(f"Table does not exist: {database_name}.{table_name} (Glue table version {version_id})") from e
        except self.glue.exceptions.ConcurrentModificationException as e:
            raise CommitFailedException(
                f"Cannot commit {database_name}.{table_name} because Glue detected concurrent update to table version {version_id}"
            ) from e

    def _get_glue_table(self, database_name: str, table_name: str) -> TableTypeDef:
        try:
            load_table_response = self.glue.get_table(DatabaseName=database_name, Name=table_name)
            return load_table_response["Table"]
        except self.glue.exceptions.EntityNotFoundException as e:
            raise NoSuchTableError(f"Table does not exist: {database_name}.{table_name}") from e

    def create_table(
        self,
        identifier: Union[str, Identifier],
        schema: Union[Schema, "pa.Schema"],
        location: Optional[str] = None,
        partition_spec: PartitionSpec = UNPARTITIONED_PARTITION_SPEC,
        sort_order: SortOrder = UNSORTED_SORT_ORDER,
        properties: Properties = EMPTY_DICT,
    ) -> Table:
        """
        Create an Iceberg table.

        Args:
            identifier: Table identifier.
            schema: Table's schema.
            location: Location for the table. Optional Argument.
            partition_spec: PartitionSpec for the table.
            sort_order: SortOrder for the table.
            properties: Table properties that can be a string based dictionary.

        Returns:
            Table: the created table instance.

        Raises:
            AlreadyExistsError: If a table with the name already exists.
            ValueError: If the identifier is invalid, or no path is given to store metadata.

        """
        schema: Schema = self._convert_schema_if_needed(schema)  # type: ignore

        database_name, table_name = self.identifier_to_database_and_table(identifier)

        location = self._resolve_table_location(location, database_name, table_name)
        metadata_location = self._get_metadata_location(location=location)
        metadata = new_table_metadata(
            location=location, schema=schema, partition_spec=partition_spec, sort_order=sort_order, properties=properties
        )
        io = load_file_io(properties=self.properties, location=metadata_location)
        self._write_metadata(metadata, io, metadata_location)

        table_input = _construct_table_input(table_name, metadata_location, properties, metadata)
        database_name, table_name = self.identifier_to_database_and_table(identifier)
        self._create_glue_table(database_name=database_name, table_name=table_name, table_input=table_input)

        return self.load_table(identifier=identifier)

    def register_table(self, identifier: Union[str, Identifier], metadata_location: str) -> Table:
        """Register a new table using existing metadata.

        Args:
            identifier Union[str, Identifier]: Table identifier for the table
            metadata_location str: The location to the metadata

        Returns:
            Table: The newly registered table

        Raises:
            TableAlreadyExistsError: If the table already exists
        """
        raise NotImplementedError

    def _commit_table(self, table_request: CommitTableRequest) -> CommitTableResponse:
        """Update the table.

        Args:
            table_request (CommitTableRequest): The table requests to be carried out.

        Returns:
            CommitTableResponse: The updated metadata.

        Raises:
            NoSuchTableError: If a table with the given identifier does not exist.
            CommitFailedException: If the commit failed.
        """
        identifier_tuple = self.identifier_to_tuple_without_catalog(
            tuple(table_request.identifier.namespace.root + [table_request.identifier.name])
        )
        database_name, table_name = self.identifier_to_database_and_table(identifier_tuple)

        current_glue_table = self._get_glue_table(database_name=database_name, table_name=table_name)
        glue_table_version_id = current_glue_table.get("VersionId")
        if not glue_table_version_id:
            raise CommitFailedException(f"Cannot commit {database_name}.{table_name} because Glue table version id is missing")
        current_table = self._convert_glue_to_iceberg(glue_table=current_glue_table)
        base_metadata = current_table.metadata

        # Validate the update requirements
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

        update_table_input = _construct_table_input(
            table_name=table_name,
            metadata_location=new_metadata_location,
            properties=current_table.properties,
            metadata=updated_metadata,
            glue_table=current_glue_table,
            prev_metadata_location=current_table.metadata_location,
        )

        # Pass `version_id` to implement optimistic locking: it ensures updates are rejected if concurrent
        # modifications occur. See more details at https://iceberg.apache.org/docs/latest/aws/#optimistic-locking
        self._update_glue_table(
            database_name=database_name,
            table_name=table_name,
            table_input=update_table_input,
            version_id=glue_table_version_id,
        )

        return CommitTableResponse(metadata=updated_metadata, metadata_location=new_metadata_location)

    def load_table(self, identifier: Union[str, Identifier]) -> Table:
        """Load the table's metadata and returns the table instance.

        You can also use this method to check for table existence using 'try catalog.table() except TableNotFoundError'.
        Note: This method doesn't scan data stored in the table.

        Args:
            identifier: Table identifier.

        Returns:
            Table: the table instance with its metadata.

        Raises:
            NoSuchTableError: If a table with the name does not exist, or the identifier is invalid.
        """
        identifier_tuple = self.identifier_to_tuple_without_catalog(identifier)
        database_name, table_name = self.identifier_to_database_and_table(identifier_tuple, NoSuchTableError)

        return self._convert_glue_to_iceberg(self._get_glue_table(database_name=database_name, table_name=table_name))

    def drop_table(self, identifier: Union[str, Identifier]) -> None:
        """Drop a table.

        Args:
            identifier: Table identifier.

        Raises:
            NoSuchTableError: If a table with the name does not exist, or the identifier is invalid.
        """
        identifier_tuple = self.identifier_to_tuple_without_catalog(identifier)
        database_name, table_name = self.identifier_to_database_and_table(identifier_tuple, NoSuchTableError)
        try:
            self.glue.delete_table(DatabaseName=database_name, Name=table_name)
        except self.glue.exceptions.EntityNotFoundException as e:
            raise NoSuchTableError(f"Table does not exist: {database_name}.{table_name}") from e

    def rename_table(self, from_identifier: Union[str, Identifier], to_identifier: Union[str, Identifier]) -> Table:
        """Rename a fully classified table name.

        This method can only rename Iceberg tables in AWS Glue.

        Args:
            from_identifier: Existing table identifier.
            to_identifier: New table identifier.

        Returns:
            Table: the updated table instance with its metadata.

        Raises:
            ValueError: When from table identifier is invalid.
            NoSuchTableError: When a table with the name does not exist.
            NoSuchIcebergTableError: When from table is not a valid iceberg table.
            NoSuchPropertyException: When from table miss some required properties.
            NoSuchNamespaceError: When the destination namespace doesn't exist.
        """
        from_identifier_tuple = self.identifier_to_tuple_without_catalog(from_identifier)
        from_database_name, from_table_name = self.identifier_to_database_and_table(from_identifier_tuple, NoSuchTableError)
        to_database_name, to_table_name = self.identifier_to_database_and_table(to_identifier)
        try:
            get_table_response = self.glue.get_table(DatabaseName=from_database_name, Name=from_table_name)
        except self.glue.exceptions.EntityNotFoundException as e:
            raise NoSuchTableError(f"Table does not exist: {from_database_name}.{from_table_name}") from e

        glue_table = get_table_response["Table"]

        try:
            # verify that from_identifier is a valid iceberg table
            self._convert_glue_to_iceberg(glue_table=glue_table)
        except NoSuchPropertyException as e:
            raise NoSuchPropertyException(
                f"Failed to rename table {from_database_name}.{from_table_name} since it is missing required properties"
            ) from e
        except NoSuchIcebergTableError as e:
            raise NoSuchIcebergTableError(
                f"Failed to rename table {from_database_name}.{from_table_name} since it is not a valid iceberg table"
            ) from e

        rename_table_input = _construct_rename_table_input(to_table_name=to_table_name, glue_table=glue_table)
        self._create_glue_table(database_name=to_database_name, table_name=to_table_name, table_input=rename_table_input)

        try:
            self.drop_table(from_identifier)
        except Exception as e:
            log_message = f"Failed to drop old table {from_database_name}.{from_table_name}. "

            try:
                self.drop_table(to_identifier)
                log_message += f"Rolled back table creation for {to_database_name}.{to_table_name}."
            except NoSuchTableError:
                log_message += (
                    f"Failed to roll back table creation for {to_database_name}.{to_table_name}. " f"Please clean up manually"
                )

            raise ValueError(log_message) from e

        return self.load_table(to_identifier)

    def create_namespace(self, namespace: Union[str, Identifier], properties: Properties = EMPTY_DICT) -> None:
        """Create a namespace in the catalog.

        Args:
            namespace: Namespace identifier.
            properties: A string dictionary of properties for the given namespace.

        Raises:
            ValueError: If the identifier is invalid.
            AlreadyExistsError: If a namespace with the given name already exists.
        """
        database_name = self.identifier_to_database(namespace)
        try:
            self.glue.create_database(DatabaseInput=_construct_database_input(database_name, properties))
        except self.glue.exceptions.AlreadyExistsException as e:
            raise NamespaceAlreadyExistsError(f"Database {database_name} already exists") from e

    def drop_namespace(self, namespace: Union[str, Identifier]) -> None:
        """Drop a namespace.

        A Glue namespace can only be dropped if it is empty.

        Args:
            namespace: Namespace identifier.

        Raises:
            NoSuchNamespaceError: If a namespace with the given name does not exist, or the identifier is invalid.
            NamespaceNotEmptyError: If the namespace is not empty.
        """
        database_name = self.identifier_to_database(namespace, NoSuchNamespaceError)
        try:
            table_list = self.list_tables(namespace=database_name)
        except NoSuchNamespaceError as e:
            raise NoSuchNamespaceError(f"Database does not exist: {database_name}") from e

        if len(table_list) > 0:
            raise NamespaceNotEmptyError(f"Database {database_name} is not empty")

        self.glue.delete_database(Name=database_name)

    def list_tables(self, namespace: Union[str, Identifier]) -> List[Identifier]:
        """List tables under the given namespace in the catalog (including non-Iceberg tables).

        Args:
            namespace (str | Identifier): Namespace identifier to search.

        Returns:
            List[Identifier]: list of table identifiers.

        Raises:
            NoSuchNamespaceError: If a namespace with the given name does not exist, or the identifier is invalid.
        """
        database_name = self.identifier_to_database(namespace, NoSuchNamespaceError)
        table_list: List[TableTypeDef] = []
        next_token: Optional[str] = None
        try:
            while True:
                table_list_response = (
                    self.glue.get_tables(DatabaseName=database_name)
                    if not next_token
                    else self.glue.get_tables(DatabaseName=database_name, NextToken=next_token)
                )
                table_list.extend(table_list_response["TableList"])
                next_token = table_list_response.get("NextToken")
                if not next_token:
                    break

        except self.glue.exceptions.EntityNotFoundException as e:
            raise NoSuchNamespaceError(f"Database does not exist: {database_name}") from e
        return [(database_name, table["Name"]) for table in table_list]

    def list_namespaces(self, namespace: Union[str, Identifier] = ()) -> List[Identifier]:
        """List namespaces from the given namespace. If not given, list top-level namespaces from the catalog.

        Returns:
            List[Identifier]: a List of namespace identifiers.
        """
        # Hierarchical namespace is not supported. Return an empty list
        if namespace:
            return []

        database_list: List[DatabaseTypeDef] = []
        next_token: Optional[str] = None

        while True:
            databases_response = self.glue.get_databases() if not next_token else self.glue.get_databases(NextToken=next_token)
            database_list.extend(databases_response["DatabaseList"])
            next_token = databases_response.get("NextToken")
            if not next_token:
                break

        return [self.identifier_to_tuple(database["Name"]) for database in database_list]

    def load_namespace_properties(self, namespace: Union[str, Identifier]) -> Properties:
        """Get properties for a namespace.

        Args:
            namespace: Namespace identifier.

        Returns:
            Properties: Properties for the given namespace.

        Raises:
            NoSuchNamespaceError: If a namespace with the given name does not exist, or identifier is invalid.
        """
        database_name = self.identifier_to_database(namespace, NoSuchNamespaceError)
        try:
            database_response = self.glue.get_database(Name=database_name)
        except self.glue.exceptions.EntityNotFoundException as e:
            raise NoSuchNamespaceError(f"Database does not exist: {database_name}") from e
        except self.glue.exceptions.InvalidInputException as e:
            raise NoSuchNamespaceError(f"Invalid input for namespace {database_name}") from e

        database = database_response["Database"]

        properties = dict(database.get("Parameters", {}))
        if "LocationUri" in database:
            properties["location"] = database["LocationUri"]
        if "Description" in database:
            properties["Description"] = database["Description"]

        return properties

    def update_namespace_properties(
        self, namespace: Union[str, Identifier], removals: Optional[Set[str]] = None, updates: Properties = EMPTY_DICT
    ) -> PropertiesUpdateSummary:
        """Remove provided property keys and updates properties for a namespace.

        Args:
            namespace: Namespace identifier.
            removals: Set of property keys that need to be removed. Optional Argument.
            updates: Properties to be updated for the given namespace.

        Raises:
            NoSuchNamespaceError: If a namespace with the given name does not exist， or identifier is invalid.
            ValueError: If removals and updates have overlapping keys.
        """
        current_properties = self.load_namespace_properties(namespace=namespace)
        properties_update_summary, updated_properties = self._get_updated_props_and_update_summary(
            current_properties=current_properties, removals=removals, updates=updates
        )

        database_name = self.identifier_to_database(namespace, NoSuchNamespaceError)
        self.glue.update_database(Name=database_name, DatabaseInput=_construct_database_input(database_name, updated_properties))

        return properties_update_summary
