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
import getpass
import logging
import socket
import time
from types import TracebackType
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    List,
    Optional,
    Set,
    Type,
    Union,
)
from urllib.parse import urlparse

from hive_metastore.ThriftHiveMetastore import Client
from hive_metastore.ttypes import (
    AlreadyExistsException,
    CheckLockRequest,
    FieldSchema,
    InvalidOperationException,
    LockComponent,
    LockLevel,
    LockRequest,
    LockResponse,
    LockState,
    LockType,
    MetaException,
    NoSuchObjectException,
    SerDeInfo,
    StorageDescriptor,
    UnlockRequest,
)
from hive_metastore.ttypes import Database as HiveDatabase
from hive_metastore.ttypes import Table as HiveTable
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential
from thrift.protocol import TBinaryProtocol
from thrift.transport import TSocket, TTransport

from pyiceberg.catalog import (
    EXTERNAL_TABLE,
    ICEBERG,
    LOCATION,
    METADATA_LOCATION,
    TABLE_TYPE,
    MetastoreCatalog,
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
    WaitingForLockException,
)
from pyiceberg.partitioning import UNPARTITIONED_PARTITION_SPEC, PartitionSpec
from pyiceberg.schema import Schema, SchemaVisitor, visit
from pyiceberg.serializers import FromInputFile
from pyiceberg.table import (
    CommitTableRequest,
    CommitTableResponse,
    PropertyUtil,
    StagedTable,
    Table,
    TableProperties,
)
from pyiceberg.table.sorting import UNSORTED_SORT_ORDER, SortOrder
from pyiceberg.typedef import EMPTY_DICT, Identifier, Properties
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


COMMENT = "comment"
OWNER = "owner"

# If set to true, HiveCatalog will operate in Hive2 compatibility mode
HIVE2_COMPATIBLE = "hive.hive2-compatible"
HIVE2_COMPATIBLE_DEFAULT = False

LOCK_CHECK_MIN_WAIT_TIME = "lock-check-min-wait-time"
LOCK_CHECK_MAX_WAIT_TIME = "lock-check-max-wait-time"
LOCK_CHECK_RETRIES = "lock-check-retries"
DEFAULT_LOCK_CHECK_MIN_WAIT_TIME = 0.1  # 100 milliseconds
DEFAULT_LOCK_CHECK_MAX_WAIT_TIME = 60  # 1 min
DEFAULT_LOCK_CHECK_RETRIES = 4

logger = logging.getLogger(__name__)


class _HiveClient:
    """Helper class to nicely open and close the transport."""

    _transport: TTransport
    _client: Client
    _ugi: Optional[List[str]]

    def __init__(self, uri: str, ugi: Optional[str] = None):
        url_parts = urlparse(uri)
        transport = TSocket.TSocket(url_parts.hostname, url_parts.port)
        self._transport = TTransport.TBufferedTransport(transport)
        protocol = TBinaryProtocol.TBinaryProtocol(transport)

        self._client = Client(protocol)
        self._ugi = ugi.split(':') if ugi else None

    def __enter__(self) -> Client:
        self._transport.open()
        if self._ugi:
            self._client.set_ugi(*self._ugi)
        return self._client

    def __exit__(
        self, exctype: Optional[Type[BaseException]], excinst: Optional[BaseException], exctb: Optional[TracebackType]
    ) -> None:
        self._transport.close()


def _construct_hive_storage_descriptor(
    schema: Schema, location: Optional[str], hive2_compatible: bool = False
) -> StorageDescriptor:
    ser_de_info = SerDeInfo(serializationLib="org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe")
    return StorageDescriptor(
        [
            FieldSchema(field.name, visit(field.field_type, SchemaToHiveConverter(hive2_compatible)), field.doc)
            for field in schema.fields
        ],
        location,
        "org.apache.hadoop.mapred.FileInputFormat",
        "org.apache.hadoop.mapred.FileOutputFormat",
        serdeInfo=ser_de_info,
    )


PROP_EXTERNAL = "EXTERNAL"
PROP_TABLE_TYPE = "table_type"
PROP_METADATA_LOCATION = "metadata_location"
PROP_PREVIOUS_METADATA_LOCATION = "previous_metadata_location"
DEFAULT_PROPERTIES = {TableProperties.PARQUET_COMPRESSION: TableProperties.PARQUET_COMPRESSION_DEFAULT}


def _construct_parameters(metadata_location: str, previous_metadata_location: Optional[str] = None) -> Dict[str, Any]:
    properties = {PROP_EXTERNAL: "TRUE", PROP_TABLE_TYPE: "ICEBERG", PROP_METADATA_LOCATION: metadata_location}
    if previous_metadata_location:
        properties[PROP_PREVIOUS_METADATA_LOCATION] = previous_metadata_location

    return properties


def _annotate_namespace(database: HiveDatabase, properties: Properties) -> HiveDatabase:
    params = {}
    for key, value in properties.items():
        if key == COMMENT:
            database.description = value
        elif key == LOCATION:
            database.locationUri = value
        else:
            params[key] = value
    database.parameters = params
    return database


HIVE_PRIMITIVE_TYPES = {
    BooleanType: "boolean",
    IntegerType: "int",
    LongType: "bigint",
    FloatType: "float",
    DoubleType: "double",
    DateType: "date",
    TimeType: "string",
    TimestampType: "timestamp",
    TimestamptzType: "timestamp with local time zone",
    StringType: "string",
    UUIDType: "string",
    BinaryType: "binary",
    FixedType: "binary",
}


class SchemaToHiveConverter(SchemaVisitor[str]):
    hive2_compatible: bool

    def __init__(self, hive2_compatible: bool):
        self.hive2_compatible = hive2_compatible

    def schema(self, schema: Schema, struct_result: str) -> str:
        return struct_result

    def struct(self, struct: StructType, field_results: List[str]) -> str:
        return f"struct<{','.join(field_results)}>"

    def field(self, field: NestedField, field_result: str) -> str:
        return f"{field.name}:{field_result}"

    def list(self, list_type: ListType, element_result: str) -> str:
        return f"array<{element_result}>"

    def map(self, map_type: MapType, key_result: str, value_result: str) -> str:
        # Key has to be primitive for Hive
        return f"map<{key_result},{value_result}>"

    def primitive(self, primitive: PrimitiveType) -> str:
        if isinstance(primitive, DecimalType):
            return f"decimal({primitive.precision},{primitive.scale})"
        elif self.hive2_compatible and isinstance(primitive, TimestamptzType):
            # Hive2 doesn't support timestamp with local time zone
            return "timestamp"
        else:
            return HIVE_PRIMITIVE_TYPES[type(primitive)]


class HiveCatalog(MetastoreCatalog):
    _client: _HiveClient

    def __init__(self, name: str, **properties: str):
        super().__init__(name, **properties)
        self._client = _HiveClient(properties["uri"], properties.get("ugi"))

        self._lock_check_min_wait_time = PropertyUtil.property_as_float(
            properties, LOCK_CHECK_MIN_WAIT_TIME, DEFAULT_LOCK_CHECK_MIN_WAIT_TIME
        )
        self._lock_check_max_wait_time = PropertyUtil.property_as_float(
            properties, LOCK_CHECK_MAX_WAIT_TIME, DEFAULT_LOCK_CHECK_MAX_WAIT_TIME
        )
        self._lock_check_retries = PropertyUtil.property_as_float(
            properties,
            LOCK_CHECK_RETRIES,
            DEFAULT_LOCK_CHECK_RETRIES,
        )

    def _convert_hive_into_iceberg(self, table: HiveTable) -> Table:
        properties: Dict[str, str] = table.parameters
        if TABLE_TYPE not in properties:
            raise NoSuchPropertyException(
                f"Property table_type missing, could not determine type: {table.dbName}.{table.tableName}"
            )

        table_type = properties[TABLE_TYPE]
        if table_type.lower() != ICEBERG:
            raise NoSuchIcebergTableError(
                f"Property table_type is {table_type}, expected {ICEBERG}: {table.dbName}.{table.tableName}"
            )

        if prop_metadata_location := properties.get(METADATA_LOCATION):
            metadata_location = prop_metadata_location
        else:
            raise NoSuchPropertyException(f"Table property {METADATA_LOCATION} is missing")

        io = self._load_file_io(location=metadata_location)
        file = io.new_input(metadata_location)
        metadata = FromInputFile.table_metadata(file)
        return Table(
            identifier=(self.name, table.dbName, table.tableName),
            metadata=metadata,
            metadata_location=metadata_location,
            io=self._load_file_io(metadata.properties, metadata_location),
            catalog=self,
        )

    def _convert_iceberg_into_hive(self, table: Table) -> HiveTable:
        identifier_tuple = self.identifier_to_tuple_without_catalog(table.identifier)
        database_name, table_name = self.identifier_to_database_and_table(identifier_tuple, NoSuchTableError)
        current_time_millis = int(time.time() * 1000)

        return HiveTable(
            dbName=database_name,
            tableName=table_name,
            owner=table.properties[OWNER] if table.properties and OWNER in table.properties else getpass.getuser(),
            createTime=current_time_millis // 1000,
            lastAccessTime=current_time_millis // 1000,
            sd=_construct_hive_storage_descriptor(
                table.schema(),
                table.location(),
                PropertyUtil.property_as_bool(self.properties, HIVE2_COMPATIBLE, HIVE2_COMPATIBLE_DEFAULT),
            ),
            tableType=EXTERNAL_TABLE,
            parameters=_construct_parameters(table.metadata_location),
        )

    def _create_hive_table(self, open_client: Client, hive_table: HiveTable) -> None:
        try:
            open_client.create_table(hive_table)
        except AlreadyExistsException as e:
            raise TableAlreadyExistsError(f"Table {hive_table.dbName}.{hive_table.tableName} already exists") from e

    def _get_hive_table(self, open_client: Client, database_name: str, table_name: str) -> HiveTable:
        try:
            return open_client.get_table(dbname=database_name, tbl_name=table_name)
        except NoSuchObjectException as e:
            raise NoSuchTableError(f"Table does not exists: {table_name}") from e

    def create_table(
        self,
        identifier: Union[str, Identifier],
        schema: Union[Schema, "pa.Schema"],
        location: Optional[str] = None,
        partition_spec: PartitionSpec = UNPARTITIONED_PARTITION_SPEC,
        sort_order: SortOrder = UNSORTED_SORT_ORDER,
        properties: Properties = EMPTY_DICT,
    ) -> Table:
        """Create a table.

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
            ValueError: If the identifier is invalid.
        """
        properties = {**DEFAULT_PROPERTIES, **properties}
        staged_table = self._create_staged_table(
            identifier=identifier,
            schema=schema,
            location=location,
            partition_spec=partition_spec,
            sort_order=sort_order,
            properties=properties,
        )
        database_name, table_name = self.identifier_to_database_and_table(identifier)

        self._write_metadata(staged_table.metadata, staged_table.io, staged_table.metadata_location)
        tbl = self._convert_iceberg_into_hive(staged_table)

        with self._client as open_client:
            self._create_hive_table(open_client, tbl)
            hive_table = open_client.get_table(dbname=database_name, tbl_name=table_name)

        return self._convert_hive_into_iceberg(hive_table)

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

    def _create_lock_request(self, database_name: str, table_name: str) -> LockRequest:
        lock_component: LockComponent = LockComponent(
            level=LockLevel.TABLE, type=LockType.EXCLUSIVE, dbname=database_name, tablename=table_name, isTransactional=True
        )

        lock_request: LockRequest = LockRequest(component=[lock_component], user=getpass.getuser(), hostname=socket.gethostname())

        return lock_request

    def _wait_for_lock(self, database_name: str, table_name: str, lockid: int, open_client: Client) -> LockResponse:
        @retry(
            retry=retry_if_exception_type(WaitingForLockException),
            wait=wait_exponential(multiplier=2, min=self._lock_check_min_wait_time, max=self._lock_check_max_wait_time),
            stop=stop_after_attempt(self._lock_check_retries),
            reraise=True,
        )
        def _do_wait_for_lock() -> LockResponse:
            response: LockResponse = open_client.check_lock(CheckLockRequest(lockid=lockid))
            if response.state == LockState.ACQUIRED:
                return response
            elif response.state == LockState.WAITING:
                msg = f"Wait on lock for {database_name}.{table_name}"
                logger.warning(msg)
                raise WaitingForLockException(msg)
            else:
                raise CommitFailedException(f"Failed to check lock for {database_name}.{table_name}, state: {response.state}")

        return _do_wait_for_lock()

    def _commit_table(self, table_request: CommitTableRequest) -> CommitTableResponse:
        """Update the table.

        Args:
            table_request (CommitTableRequest): The table requests to be carried out.

        Returns:
            CommitTableResponse: The updated metadata.

        Raises:
            NoSuchTableError: If a table with the given identifier does not exist.
            CommitFailedException: Requirement not met, or a conflict with a concurrent commit.
        """
        identifier_tuple = self.identifier_to_tuple_without_catalog(
            tuple(table_request.identifier.namespace.root + [table_request.identifier.name])
        )
        database_name, table_name = self.identifier_to_database_and_table(identifier_tuple, NoSuchTableError)
        # commit to hive
        # https://github.com/apache/hive/blob/master/standalone-metastore/metastore-common/src/main/thrift/hive_metastore.thrift#L1232
        with self._client as open_client:
            lock: LockResponse = open_client.lock(self._create_lock_request(database_name, table_name))

            try:
                if lock.state != LockState.ACQUIRED:
                    if lock.state == LockState.WAITING:
                        self._wait_for_lock(database_name, table_name, lock.lockid, open_client)
                    else:
                        raise CommitFailedException(f"Failed to acquire lock for {table_request.identifier}, state: {lock.state}")

                hive_table: Optional[HiveTable]
                current_table: Optional[Table]
                try:
                    hive_table = self._get_hive_table(open_client, database_name, table_name)
                    current_table = self._convert_hive_into_iceberg(hive_table)
                except NoSuchTableError:
                    hive_table = None
                    current_table = None

                updated_staged_table = self._update_and_stage_table(current_table, table_request)
                if current_table and updated_staged_table.metadata == current_table.metadata:
                    # no changes, do nothing
                    return CommitTableResponse(metadata=current_table.metadata, metadata_location=current_table.metadata_location)
                self._write_metadata(
                    metadata=updated_staged_table.metadata,
                    io=updated_staged_table.io,
                    metadata_path=updated_staged_table.metadata_location,
                )

                if hive_table and current_table:
                    # Table exists, update it.
                    hive_table.parameters = _construct_parameters(
                        metadata_location=updated_staged_table.metadata_location,
                        previous_metadata_location=current_table.metadata_location,
                    )
                    open_client.alter_table(dbname=database_name, tbl_name=table_name, new_tbl=hive_table)
                else:
                    # Table does not exist, create it.
                    hive_table = self._convert_iceberg_into_hive(
                        StagedTable(
                            identifier=(self.name, database_name, table_name),
                            metadata=updated_staged_table.metadata,
                            metadata_location=updated_staged_table.metadata_location,
                            io=updated_staged_table.io,
                            catalog=self,
                        )
                    )
                    self._create_hive_table(open_client, hive_table)
            except WaitingForLockException as e:
                raise CommitFailedException(f"Failed to acquire lock for {table_request.identifier}, state: {lock.state}") from e
            finally:
                open_client.unlock(UnlockRequest(lockid=lock.lockid))

        return CommitTableResponse(
            metadata=updated_staged_table.metadata, metadata_location=updated_staged_table.metadata_location
        )

    def load_table(self, identifier: Union[str, Identifier]) -> Table:
        """Load the table's metadata and return the table instance.

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

        with self._client as open_client:
            hive_table = self._get_hive_table(open_client, database_name, table_name)

        return self._convert_hive_into_iceberg(hive_table)

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
            with self._client as open_client:
                open_client.drop_table(dbname=database_name, name=table_name, deleteData=False)
        except NoSuchObjectException as e:
            # When the namespace doesn't exist, it throws the same error
            raise NoSuchTableError(f"Table does not exists: {table_name}") from e

    def purge_table(self, identifier: Union[str, Identifier]) -> None:
        # This requires to traverse the reachability set, and drop all the data files.
        raise NotImplementedError("Not yet implemented")

    def rename_table(self, from_identifier: Union[str, Identifier], to_identifier: Union[str, Identifier]) -> Table:
        """Rename a fully classified table name.

        Args:
            from_identifier: Existing table identifier.
            to_identifier: New table identifier.

        Returns:
            Table: the updated table instance with its metadata.

        Raises:
            ValueError: When from table identifier is invalid.
            NoSuchTableError: When a table with the name does not exist.
            NoSuchNamespaceError: When the destination namespace doesn't exist.
        """
        from_identifier_tuple = self.identifier_to_tuple_without_catalog(from_identifier)
        from_database_name, from_table_name = self.identifier_to_database_and_table(from_identifier_tuple, NoSuchTableError)
        to_database_name, to_table_name = self.identifier_to_database_and_table(to_identifier)
        try:
            with self._client as open_client:
                tbl = open_client.get_table(dbname=from_database_name, tbl_name=from_table_name)
                tbl.dbName = to_database_name
                tbl.tableName = to_table_name
                open_client.alter_table(dbname=from_database_name, tbl_name=from_table_name, new_tbl=tbl)
        except NoSuchObjectException as e:
            raise NoSuchTableError(f"Table does not exist: {from_table_name}") from e
        except InvalidOperationException as e:
            raise NoSuchNamespaceError(f"Database does not exists: {to_database_name}") from e
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
        hive_database = HiveDatabase(name=database_name, parameters=properties)

        try:
            with self._client as open_client:
                open_client.create_database(_annotate_namespace(hive_database, properties))
        except AlreadyExistsException as e:
            raise NamespaceAlreadyExistsError(f"Database {database_name} already exists") from e

    def drop_namespace(self, namespace: Union[str, Identifier]) -> None:
        """Drop a namespace.

        Args:
            namespace: Namespace identifier.

        Raises:
            NoSuchNamespaceError: If a namespace with the given name does not exist, or the identifier is invalid.
            NamespaceNotEmptyError: If the namespace is not empty.
        """
        database_name = self.identifier_to_database(namespace, NoSuchNamespaceError)
        try:
            with self._client as open_client:
                open_client.drop_database(database_name, deleteData=False, cascade=False)
        except InvalidOperationException as e:
            raise NamespaceNotEmptyError(f"Database {database_name} is not empty") from e
        except MetaException as e:
            raise NoSuchNamespaceError(f"Database does not exists: {database_name}") from e

    def list_tables(self, namespace: Union[str, Identifier]) -> List[Identifier]:
        """List tables under the given namespace in the catalog (including non-Iceberg tables).

        When the database doesn't exist, it will just return an empty list.

        Args:
            namespace: Database to list.

        Returns:
            List[Identifier]: list of table identifiers.

        Raises:
            NoSuchNamespaceError: If a namespace with the given name does not exist, or the identifier is invalid.
        """
        database_name = self.identifier_to_database(namespace, NoSuchNamespaceError)
        with self._client as open_client:
            return [(database_name, table_name) for table_name in open_client.get_all_tables(db_name=database_name)]

    def list_namespaces(self, namespace: Union[str, Identifier] = ()) -> List[Identifier]:
        """List namespaces from the given namespace. If not given, list top-level namespaces from the catalog.

        Returns:
            List[Identifier]: a List of namespace identifiers.
        """
        # Hierarchical namespace is not supported. Return an empty list
        if namespace:
            return []

        with self._client as open_client:
            return list(map(self.identifier_to_tuple, open_client.get_all_databases()))

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
            with self._client as open_client:
                database = open_client.get_database(name=database_name)
                properties = database.parameters
                properties[LOCATION] = database.locationUri
                if comment := database.description:
                    properties[COMMENT] = comment
                return properties
        except NoSuchObjectException as e:
            raise NoSuchNamespaceError(f"Database does not exists: {database_name}") from e

    def update_namespace_properties(
        self, namespace: Union[str, Identifier], removals: Optional[Set[str]] = None, updates: Properties = EMPTY_DICT
    ) -> PropertiesUpdateSummary:
        """Remove provided property keys and update properties for a namespace.

        Args:
            namespace: Namespace identifier.
            removals: Set of property keys that need to be removed. Optional Argument.
            updates: Properties to be updated for the given namespace.

        Raises:
            NoSuchNamespaceError: If a namespace with the given name does not exist
            ValueError: If removals and updates have overlapping keys.
        """
        self._check_for_overlap(updates=updates, removals=removals)
        database_name = self.identifier_to_database(namespace, NoSuchNamespaceError)
        with self._client as open_client:
            try:
                database = open_client.get_database(database_name)
                parameters = database.parameters
            except NoSuchObjectException as e:
                raise NoSuchNamespaceError(f"Database does not exists: {database_name}") from e

            removed: Set[str] = set()
            updated: Set[str] = set()

            if removals:
                for key in removals:
                    if key in parameters:
                        parameters[key] = None
                        removed.add(key)
            if updates:
                for key, value in updates.items():
                    parameters[key] = value
                    updated.add(key)

            open_client.alter_database(database_name, _annotate_namespace(database, parameters))

        expected_to_change = (removals or set()).difference(removed)

        return PropertiesUpdateSummary(removed=list(removed or []), updated=list(updated or []), missing=list(expected_to_change))
