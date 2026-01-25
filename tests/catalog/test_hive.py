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
# pylint: disable=protected-access,redefined-outer-name
import base64
import copy
import struct
import threading
import uuid
from collections.abc import Generator
from copy import deepcopy
from unittest.mock import MagicMock, call, patch

import pytest
import thrift.transport.TSocket
from hive_metastore.ttypes import (
    AlreadyExistsException,
    EnvironmentContext,
    FieldSchema,
    InvalidOperationException,
    LockResponse,
    LockState,
    MetaException,
    NoSuchObjectException,
    SerDeInfo,
    SkewedInfo,
    StorageDescriptor,
)
from hive_metastore.ttypes import Database as HiveDatabase
from hive_metastore.ttypes import Table as HiveTable

from pyiceberg.catalog import PropertiesUpdateSummary
from pyiceberg.catalog.hive import (
    DO_NOT_UPDATE_STATS,
    DO_NOT_UPDATE_STATS_DEFAULT,
    HIVE_KERBEROS_AUTH,
    HIVE_KERBEROS_SERVICE_NAME,
    LOCK_CHECK_MAX_WAIT_TIME,
    LOCK_CHECK_MIN_WAIT_TIME,
    LOCK_CHECK_RETRIES,
    HiveCatalog,
    _construct_hive_storage_descriptor,
    _HiveClient,
)
from pyiceberg.exceptions import (
    NamespaceAlreadyExistsError,
    NamespaceNotEmptyError,
    NoSuchNamespaceError,
    NoSuchTableError,
    TableAlreadyExistsError,
    WaitingForLockException,
)
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table.metadata import TableMetadataUtil, TableMetadataV1, TableMetadataV2
from pyiceberg.table.refs import SnapshotRef, SnapshotRefType
from pyiceberg.table.snapshots import (
    MetadataLogEntry,
    Operation,
    Snapshot,
    SnapshotLogEntry,
    Summary,
)
from pyiceberg.table.sorting import (
    NullOrder,
    SortDirection,
    SortField,
    SortOrder,
)
from pyiceberg.transforms import BucketTransform, IdentityTransform
from pyiceberg.typedef import UTF8
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
    StringType,
    StructType,
    TimestampType,
    TimestamptzType,
    TimeType,
    UUIDType,
)

HIVE_CATALOG_NAME = "hive"
HIVE_METASTORE_FAKE_URL = "thrift://unknown:9083"


@pytest.fixture
def hive_table(metadata_location: str) -> HiveTable:
    return HiveTable(
        tableName="new_tabl2e",
        dbName="default",
        owner="fokkodriesprong",
        createTime=1659092339,
        lastAccessTime=1659092,
        retention=0,
        sd=StorageDescriptor(
            cols=[
                FieldSchema(name="foo", type="string", comment=None),
                FieldSchema(name="bar", type="int", comment=None),
                FieldSchema(name="baz", type="boolean", comment=None),
            ],
            location="file:/tmp/new_tabl2e",
            inputFormat="org.apache.hadoop.mapred.FileInputFormat",
            outputFormat="org.apache.hadoop.mapred.FileOutputFormat",
            compressed=False,
            numBuckets=0,
            serdeInfo=SerDeInfo(
                name=None,
                serializationLib="org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
                parameters={},
                description=None,
                serializerClass=None,
                deserializerClass=None,
                serdeType=None,
            ),
            bucketCols=[],
            sortCols=[],
            parameters={},
            skewedInfo=SkewedInfo(skewedColNames=[], skewedColValues=[], skewedColValueLocationMaps={}),
            storedAsSubDirectories=False,
        ),
        partitionKeys=[],
        parameters={
            "EXTERNAL": "TRUE",
            "transient_lastDdlTime": "1659092339",
            "table_type": "ICEBERG",
            "metadata_location": metadata_location,
        },
        viewOriginalText=None,
        viewExpandedText=None,
        tableType="EXTERNAL_TABLE",
        privileges=None,
        temporary=False,
        rewriteEnabled=False,
        creationMetadata=None,
        catName="hive",
        ownerType=1,
        writeId=-1,
        isStatsCompliant=None,
        colStats=None,
        accessType=None,
        requiredReadCapabilities=None,
        requiredWriteCapabilities=None,
        id=None,
        fileMetadata=None,
        dictionary=None,
        txnId=None,
    )


@pytest.fixture(scope="session")
def hive_database(tmp_path_factory: pytest.TempPathFactory) -> HiveDatabase:
    # Pre-create the directory, this has to be done because
    # of a local FS. Not needed with an actual object store.
    database_path = tmp_path_factory.mktemp("database")
    manifest_path = database_path / "database" / "table" / "metadata"
    manifest_path.mkdir(parents=True)
    return HiveDatabase(
        name="default",
        description=None,
        locationUri=str(database_path / "database"),
        parameters={"test": "property"},
        privileges=None,
        ownerName=None,
        ownerType=1,
        catalogName="hive",
        createTime=None,
        managedLocationUri=None,
        type=None,
        connector_name=None,
        remote_dbname=None,
    )


class SaslServer(threading.Thread):
    def __init__(self, socket: thrift.transport.TSocket.TServerSocket, response: bytes) -> None:
        super().__init__()
        self.daemon = True
        self._socket = socket
        self._response = response
        self._port = None
        self._port_bound = threading.Event()
        self._clients: list[thrift.transport.TSocket.TSocket] = []  # Track accepted client connections

    def run(self) -> None:
        self._socket.listen()

        try:
            address = self._socket.handle.getsockname()
            # AF_INET addresses are 2-tuples (host, port) and AF_INET6 are
            # 4-tuples (host, port, ...), i.e. port is always at index 1.
            _host, self._port, *_ = address
        finally:
            self._port_bound.set()

        # Accept connections and respond to each connection with the same message.
        # The responsibility for closing the connection is on the client
        while True:
            try:
                client = self._socket.accept()
                if client:
                    self._clients.append(client)  # Track the client
                    client.write(self._response)
                    client.flush()
            except Exception:
                pass

    @property
    def port(self) -> int | None:
        self._port_bound.wait()
        return self._port

    def close(self) -> None:
        # Close all client connections first
        for client in self._clients:
            try:
                client.close()
            except Exception:
                pass
        self._socket.close()


@pytest.fixture(scope="session")
def kerberized_hive_metastore_fake_url() -> Generator[str, None, None]:
    server = SaslServer(
        # Port 0 means pick any available port.
        socket=thrift.transport.TSocket.TServerSocket(port=0),
        # Always return a message with status 5 (COMPLETE).
        response=struct.pack(">BI", 5, 0),
    )
    server.start()
    yield f"thrift://localhost:{server.port}"
    server.close()


def test_no_uri_supplied() -> None:
    with pytest.raises(KeyError):
        HiveCatalog("production")


def test_check_number_of_namespaces(table_schema_simple: Schema) -> None:
    catalog = HiveCatalog(HIVE_CATALOG_NAME, uri=HIVE_METASTORE_FAKE_URL)

    with pytest.raises(ValueError):
        catalog.create_table(("default", "namespace", "table"), schema=table_schema_simple)

    with pytest.raises(ValueError):
        catalog.create_table("default.namespace.table", schema=table_schema_simple)

    with pytest.raises(ValueError):
        catalog.create_table(("table",), schema=table_schema_simple)

    with pytest.raises(ValueError):
        catalog.create_table("table", schema=table_schema_simple)


@pytest.mark.parametrize("hive2_compatible", [True, False])
@patch("time.time", MagicMock(return_value=12345))
def test_create_table(
    table_schema_with_all_types: Schema, hive_database: HiveDatabase, hive_table: HiveTable, hive2_compatible: bool
) -> None:
    catalog = HiveCatalog(HIVE_CATALOG_NAME, uri=HIVE_METASTORE_FAKE_URL)
    if hive2_compatible:
        catalog = HiveCatalog(HIVE_CATALOG_NAME, uri=HIVE_METASTORE_FAKE_URL, **{"hive.hive2-compatible": "true"})

    catalog._client = MagicMock()
    catalog._client.__enter__().create_table.return_value = None
    catalog._client.__enter__().get_table.return_value = hive_table
    catalog._client.__enter__().get_database.return_value = hive_database
    catalog.create_table(("default", "table"), schema=table_schema_with_all_types, properties={"owner": "javaberg"})

    called_hive_table: HiveTable = catalog._client.__enter__().create_table.call_args[0][0]
    # This one is generated within the function itself, so we need to extract
    # it to construct the assert_called_with
    metadata_location: str = called_hive_table.parameters["metadata_location"]
    assert metadata_location.endswith(".metadata.json")
    assert "/database/table/metadata/" in metadata_location
    catalog._client.__enter__().create_table.assert_called_with(
        HiveTable(
            tableName="table",
            dbName="default",
            owner="javaberg",
            createTime=12345,
            lastAccessTime=12345,
            retention=None,
            sd=StorageDescriptor(
                cols=[
                    FieldSchema(name="boolean", type="boolean", comment=None),
                    FieldSchema(name="integer", type="int", comment=None),
                    FieldSchema(name="long", type="bigint", comment=None),
                    FieldSchema(name="float", type="float", comment=None),
                    FieldSchema(name="double", type="double", comment=None),
                    FieldSchema(name="decimal", type="decimal(32,3)", comment=None),
                    FieldSchema(name="date", type="date", comment=None),
                    FieldSchema(name="time", type="string", comment=None),
                    FieldSchema(name="timestamp", type="timestamp", comment=None),
                    FieldSchema(
                        name="timestamptz",
                        type="timestamp" if hive2_compatible else "timestamp with local time zone",
                        comment=None,
                    ),
                    FieldSchema(name="string", type="string", comment=None),
                    FieldSchema(name="uuid", type="string", comment=None),
                    FieldSchema(name="fixed", type="binary", comment=None),
                    FieldSchema(name="binary", type="binary", comment=None),
                    FieldSchema(name="list", type="array<string>", comment=None),
                    FieldSchema(name="map", type="map<string,int>", comment=None),
                    FieldSchema(name="struct", type="struct<inner_string:string,inner_int:int>", comment=None),
                ],
                location=f"{hive_database.locationUri}/table",
                inputFormat="org.apache.hadoop.mapred.FileInputFormat",
                outputFormat="org.apache.hadoop.mapred.FileOutputFormat",
                compressed=None,
                numBuckets=None,
                serdeInfo=SerDeInfo(
                    name=None,
                    serializationLib="org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
                    parameters=None,
                    description=None,
                    serializerClass=None,
                    deserializerClass=None,
                    serdeType=None,
                ),
                bucketCols=None,
                sortCols=None,
                parameters=None,
                skewedInfo=None,
                storedAsSubDirectories=None,
            ),
            partitionKeys=None,
            parameters={
                "EXTERNAL": "TRUE",
                "table_type": "ICEBERG",
                "metadata_location": metadata_location,
                "write.parquet.compression-codec": "zstd",
                "owner": "javaberg",
            },
            viewOriginalText=None,
            viewExpandedText=None,
            tableType="EXTERNAL_TABLE",
            privileges=None,
            temporary=False,
            rewriteEnabled=None,
            creationMetadata=None,
            catName=None,
            ownerType=1,
            writeId=-1,
            isStatsCompliant=None,
            colStats=None,
            accessType=None,
            requiredReadCapabilities=None,
            requiredWriteCapabilities=None,
            id=None,
            fileMetadata=None,
            dictionary=None,
            txnId=None,
        )
    )

    with open(metadata_location, encoding=UTF8) as f:
        payload = f.read()

    metadata = TableMetadataUtil.parse_raw(payload)

    assert "database/table" in metadata.location

    expected = TableMetadataV2(
        location=metadata.location,
        table_uuid=metadata.table_uuid,
        last_updated_ms=metadata.last_updated_ms,
        last_column_id=22,
        schemas=[
            Schema(
                NestedField(field_id=1, name="boolean", field_type=BooleanType(), required=True),
                NestedField(field_id=2, name="integer", field_type=IntegerType(), required=True),
                NestedField(field_id=3, name="long", field_type=LongType(), required=True),
                NestedField(field_id=4, name="float", field_type=FloatType(), required=True),
                NestedField(field_id=5, name="double", field_type=DoubleType(), required=True),
                NestedField(field_id=6, name="decimal", field_type=DecimalType(precision=32, scale=3), required=True),
                NestedField(field_id=7, name="date", field_type=DateType(), required=True),
                NestedField(field_id=8, name="time", field_type=TimeType(), required=True),
                NestedField(field_id=9, name="timestamp", field_type=TimestampType(), required=True),
                NestedField(field_id=10, name="timestamptz", field_type=TimestamptzType(), required=True),
                NestedField(field_id=11, name="string", field_type=StringType(), required=True),
                NestedField(field_id=12, name="uuid", field_type=UUIDType(), required=True),
                NestedField(field_id=13, name="fixed", field_type=FixedType(length=12), required=True),
                NestedField(field_id=14, name="binary", field_type=BinaryType(), required=True),
                NestedField(
                    field_id=15,
                    name="list",
                    field_type=ListType(type="list", element_id=18, element_type=StringType(), element_required=True),
                    required=True,
                ),
                NestedField(
                    field_id=16,
                    name="map",
                    field_type=MapType(
                        type="map", key_id=19, key_type=StringType(), value_id=20, value_type=IntegerType(), value_required=True
                    ),
                    required=True,
                ),
                NestedField(
                    field_id=17,
                    name="struct",
                    field_type=StructType(
                        NestedField(field_id=21, name="inner_string", field_type=StringType(), required=False),
                        NestedField(field_id=22, name="inner_int", field_type=IntegerType(), required=True),
                    ),
                    required=False,
                ),
                schema_id=0,
                identifier_field_ids=[2],
            )
        ],
        current_schema_id=0,
        last_partition_id=999,
        properties={"owner": "javaberg", "write.parquet.compression-codec": "zstd"},
        partition_specs=[PartitionSpec()],
        default_spec_id=0,
        current_snapshot_id=None,
        snapshots=[],
        snapshot_log=[],
        metadata_log=[],
        sort_orders=[SortOrder(order_id=0)],
        default_sort_order_id=0,
        refs={},
        format_version=2,
        last_sequence_number=0,
    )

    assert metadata.model_dump() == expected.model_dump()


@pytest.mark.parametrize("hive2_compatible", [True, False])
@patch("time.time", MagicMock(return_value=12345))
def test_create_table_with_given_location_removes_trailing_slash(
    table_schema_with_all_types: Schema, hive_database: HiveDatabase, hive_table: HiveTable, hive2_compatible: bool
) -> None:
    catalog = HiveCatalog(HIVE_CATALOG_NAME, uri=HIVE_METASTORE_FAKE_URL)
    if hive2_compatible:
        catalog = HiveCatalog(HIVE_CATALOG_NAME, uri=HIVE_METASTORE_FAKE_URL, **{"hive.hive2-compatible": "true"})

    location = f"{hive_database.locationUri}/table-given-location"

    catalog._client = MagicMock()
    catalog._client.__enter__().create_table.return_value = None
    catalog._client.__enter__().get_table.return_value = hive_table
    catalog._client.__enter__().get_database.return_value = hive_database
    catalog.create_table(
        ("default", "table"), schema=table_schema_with_all_types, properties={"owner": "javaberg"}, location=f"{location}/"
    )

    called_hive_table: HiveTable = catalog._client.__enter__().create_table.call_args[0][0]
    # This one is generated within the function itself, so we need to extract
    # it to construct the assert_called_with
    metadata_location: str = called_hive_table.parameters["metadata_location"]
    assert metadata_location.endswith(".metadata.json")
    assert "/database/table-given-location/metadata/" in metadata_location
    catalog._client.__enter__().create_table.assert_called_with(
        HiveTable(
            tableName="table",
            dbName="default",
            owner="javaberg",
            createTime=12345,
            lastAccessTime=12345,
            retention=None,
            sd=StorageDescriptor(
                cols=[
                    FieldSchema(name="boolean", type="boolean", comment=None),
                    FieldSchema(name="integer", type="int", comment=None),
                    FieldSchema(name="long", type="bigint", comment=None),
                    FieldSchema(name="float", type="float", comment=None),
                    FieldSchema(name="double", type="double", comment=None),
                    FieldSchema(name="decimal", type="decimal(32,3)", comment=None),
                    FieldSchema(name="date", type="date", comment=None),
                    FieldSchema(name="time", type="string", comment=None),
                    FieldSchema(name="timestamp", type="timestamp", comment=None),
                    FieldSchema(
                        name="timestamptz",
                        type="timestamp" if hive2_compatible else "timestamp with local time zone",
                        comment=None,
                    ),
                    FieldSchema(name="string", type="string", comment=None),
                    FieldSchema(name="uuid", type="string", comment=None),
                    FieldSchema(name="fixed", type="binary", comment=None),
                    FieldSchema(name="binary", type="binary", comment=None),
                    FieldSchema(name="list", type="array<string>", comment=None),
                    FieldSchema(name="map", type="map<string,int>", comment=None),
                    FieldSchema(name="struct", type="struct<inner_string:string,inner_int:int>", comment=None),
                ],
                location=f"{hive_database.locationUri}/table-given-location",
                inputFormat="org.apache.hadoop.mapred.FileInputFormat",
                outputFormat="org.apache.hadoop.mapred.FileOutputFormat",
                compressed=None,
                numBuckets=None,
                serdeInfo=SerDeInfo(
                    name=None,
                    serializationLib="org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
                    parameters=None,
                    description=None,
                    serializerClass=None,
                    deserializerClass=None,
                    serdeType=None,
                ),
                bucketCols=None,
                sortCols=None,
                parameters=None,
                skewedInfo=None,
                storedAsSubDirectories=None,
            ),
            partitionKeys=None,
            parameters={
                "EXTERNAL": "TRUE",
                "table_type": "ICEBERG",
                "metadata_location": metadata_location,
                "write.parquet.compression-codec": "zstd",
                "owner": "javaberg",
            },
            viewOriginalText=None,
            viewExpandedText=None,
            tableType="EXTERNAL_TABLE",
            privileges=None,
            temporary=False,
            rewriteEnabled=None,
            creationMetadata=None,
            catName=None,
            ownerType=1,
            writeId=-1,
            isStatsCompliant=None,
            colStats=None,
            accessType=None,
            requiredReadCapabilities=None,
            requiredWriteCapabilities=None,
            id=None,
            fileMetadata=None,
            dictionary=None,
            txnId=None,
        )
    )

    with open(metadata_location, encoding=UTF8) as f:
        payload = f.read()

    metadata = TableMetadataUtil.parse_raw(payload)

    assert "database/table-given-location" in metadata.location

    expected = TableMetadataV2(
        location=metadata.location,
        table_uuid=metadata.table_uuid,
        last_updated_ms=metadata.last_updated_ms,
        last_column_id=22,
        schemas=[
            Schema(
                NestedField(field_id=1, name="boolean", field_type=BooleanType(), required=True),
                NestedField(field_id=2, name="integer", field_type=IntegerType(), required=True),
                NestedField(field_id=3, name="long", field_type=LongType(), required=True),
                NestedField(field_id=4, name="float", field_type=FloatType(), required=True),
                NestedField(field_id=5, name="double", field_type=DoubleType(), required=True),
                NestedField(field_id=6, name="decimal", field_type=DecimalType(precision=32, scale=3), required=True),
                NestedField(field_id=7, name="date", field_type=DateType(), required=True),
                NestedField(field_id=8, name="time", field_type=TimeType(), required=True),
                NestedField(field_id=9, name="timestamp", field_type=TimestampType(), required=True),
                NestedField(field_id=10, name="timestamptz", field_type=TimestamptzType(), required=True),
                NestedField(field_id=11, name="string", field_type=StringType(), required=True),
                NestedField(field_id=12, name="uuid", field_type=UUIDType(), required=True),
                NestedField(field_id=13, name="fixed", field_type=FixedType(length=12), required=True),
                NestedField(field_id=14, name="binary", field_type=BinaryType(), required=True),
                NestedField(
                    field_id=15,
                    name="list",
                    field_type=ListType(type="list", element_id=18, element_type=StringType(), element_required=True),
                    required=True,
                ),
                NestedField(
                    field_id=16,
                    name="map",
                    field_type=MapType(
                        type="map", key_id=19, key_type=StringType(), value_id=20, value_type=IntegerType(), value_required=True
                    ),
                    required=True,
                ),
                NestedField(
                    field_id=17,
                    name="struct",
                    field_type=StructType(
                        NestedField(field_id=21, name="inner_string", field_type=StringType(), required=False),
                        NestedField(field_id=22, name="inner_int", field_type=IntegerType(), required=True),
                    ),
                    required=False,
                ),
                schema_id=0,
                identifier_field_ids=[2],
            )
        ],
        current_schema_id=0,
        last_partition_id=999,
        properties={"owner": "javaberg", "write.parquet.compression-codec": "zstd"},
        partition_specs=[PartitionSpec()],
        default_spec_id=0,
        current_snapshot_id=None,
        snapshots=[],
        snapshot_log=[],
        metadata_log=[],
        sort_orders=[SortOrder(order_id=0)],
        default_sort_order_id=0,
        refs={},
        format_version=2,
        last_sequence_number=0,
    )

    assert metadata.model_dump() == expected.model_dump()


@patch("time.time", MagicMock(return_value=12345))
def test_create_v1_table(table_schema_simple: Schema, hive_database: HiveDatabase, hive_table: HiveTable) -> None:
    catalog = HiveCatalog(HIVE_CATALOG_NAME, uri=HIVE_METASTORE_FAKE_URL)

    catalog._client = MagicMock()
    catalog._client.__enter__().create_table.return_value = None
    catalog._client.__enter__().get_table.return_value = hive_table
    catalog._client.__enter__().get_database.return_value = hive_database
    catalog.create_table(
        ("default", "table"), schema=table_schema_simple, properties={"owner": "javaberg", "format-version": "1"}
    )

    # Test creating V1 table
    called_v1_table: HiveTable = catalog._client.__enter__().create_table.call_args[0][0]
    metadata_location = called_v1_table.parameters["metadata_location"]
    with open(metadata_location, encoding=UTF8) as f:
        payload = f.read()

    expected_schema = Schema(
        NestedField(field_id=1, name="foo", field_type=StringType(), required=False),
        NestedField(field_id=2, name="bar", field_type=IntegerType(), required=True),
        NestedField(field_id=3, name="baz", field_type=BooleanType(), required=False),
        schema_id=0,
        identifier_field_ids=[2],
    )
    actual_v1_metadata = TableMetadataUtil.parse_raw(payload)
    expected_spec = PartitionSpec()
    expected_v1_metadata = TableMetadataV1(
        location=actual_v1_metadata.location,
        table_uuid=actual_v1_metadata.table_uuid,
        last_updated_ms=actual_v1_metadata.last_updated_ms,
        last_column_id=3,
        schema=expected_schema,
        schemas=[expected_schema],
        current_schema_id=0,
        last_partition_id=999,
        properties={"owner": "javaberg", "write.parquet.compression-codec": "zstd"},
        partition_spec=[],
        partition_specs=[expected_spec],
        default_spec_id=0,
        current_snapshot_id=None,
        snapshots=[],
        snapshot_log=[],
        metadata_log=[],
        sort_orders=[SortOrder(order_id=0)],
        default_sort_order_id=0,
        refs={},
        format_version=1,
    )

    assert actual_v1_metadata.model_dump() == expected_v1_metadata.model_dump()


def test_load_table(hive_table: HiveTable) -> None:
    catalog = HiveCatalog(HIVE_CATALOG_NAME, uri=HIVE_METASTORE_FAKE_URL)

    catalog._client = MagicMock()
    catalog._client.__enter__().get_table.return_value = hive_table
    table = catalog.load_table(("default", "new_tabl2e"))

    catalog._client.__enter__().get_table.assert_called_with(dbname="default", tbl_name="new_tabl2e")

    expected = TableMetadataV2(
        location="s3://bucket/test/location",
        table_uuid=uuid.UUID("9c12d441-03fe-4693-9a96-a0705ddf69c1"),
        last_updated_ms=1602638573590,
        last_column_id=3,
        schemas=[
            Schema(
                NestedField(field_id=1, name="x", field_type=LongType(), required=True),
                schema_id=0,
                identifier_field_ids=[],
            ),
            Schema(
                NestedField(field_id=1, name="x", field_type=LongType(), required=True),
                NestedField(field_id=2, name="y", field_type=LongType(), required=True, doc="comment"),
                NestedField(field_id=3, name="z", field_type=LongType(), required=True),
                schema_id=1,
                identifier_field_ids=[1, 2],
            ),
        ],
        current_schema_id=1,
        partition_specs=[
            PartitionSpec(PartitionField(source_id=1, field_id=1000, transform=IdentityTransform(), name="x"), spec_id=0)
        ],
        default_spec_id=0,
        last_partition_id=1000,
        properties={"read.split.target.size": "134217728"},
        current_snapshot_id=3055729675574597004,
        snapshots=[
            Snapshot(
                snapshot_id=3051729675574597004,
                parent_snapshot_id=None,
                sequence_number=0,
                timestamp_ms=1515100955770,
                manifest_list="s3://a/b/1.avro",
                summary=Summary(operation=Operation.APPEND),
                schema_id=None,
            ),
            Snapshot(
                snapshot_id=3055729675574597004,
                parent_snapshot_id=3051729675574597004,
                sequence_number=1,
                timestamp_ms=1555100955770,
                manifest_list="s3://a/b/2.avro",
                summary=Summary(operation=Operation.APPEND),
                schema_id=1,
            ),
        ],
        snapshot_log=[
            SnapshotLogEntry(snapshot_id=3051729675574597004, timestamp_ms=1515100955770),
            SnapshotLogEntry(snapshot_id=3055729675574597004, timestamp_ms=1555100955770),
        ],
        metadata_log=[MetadataLogEntry(metadata_file="s3://bucket/.../v1.json", timestamp_ms=1515100)],
        sort_orders=[
            SortOrder(
                SortField(
                    source_id=2, transform=IdentityTransform(), direction=SortDirection.ASC, null_order=NullOrder.NULLS_FIRST
                ),
                SortField(
                    source_id=3,
                    transform=BucketTransform(num_buckets=4),
                    direction=SortDirection.DESC,
                    null_order=NullOrder.NULLS_LAST,
                ),
                order_id=3,
            )
        ],
        default_sort_order_id=3,
        refs={
            "test": SnapshotRef(
                snapshot_id=3051729675574597004,
                snapshot_ref_type=SnapshotRefType.TAG,
                min_snapshots_to_keep=None,
                max_snapshot_age_ms=None,
                max_ref_age_ms=10000000,
            ),
            "main": SnapshotRef(
                snapshot_id=3055729675574597004,
                snapshot_ref_type=SnapshotRefType.BRANCH,
                min_snapshots_to_keep=None,
                max_snapshot_age_ms=None,
                max_ref_age_ms=None,
            ),
        },
        format_version=2,
        last_sequence_number=34,
    )

    assert table.name() == ("default", "new_tabl2e")
    assert expected == table.metadata


def test_load_table_from_self_identifier(hive_table: HiveTable) -> None:
    catalog = HiveCatalog(HIVE_CATALOG_NAME, uri=HIVE_METASTORE_FAKE_URL)

    catalog._client = MagicMock()
    catalog._client.__enter__().get_table.return_value = hive_table
    intermediate = catalog.load_table(("default", "new_tabl2e"))
    table = catalog.load_table(intermediate.name())

    catalog._client.__enter__().get_table.assert_called_with(dbname="default", tbl_name="new_tabl2e")

    expected = TableMetadataV2(
        location="s3://bucket/test/location",
        table_uuid=uuid.UUID("9c12d441-03fe-4693-9a96-a0705ddf69c1"),
        last_updated_ms=1602638573590,
        last_column_id=3,
        schemas=[
            Schema(
                NestedField(field_id=1, name="x", field_type=LongType(), required=True),
                schema_id=0,
                identifier_field_ids=[],
            ),
            Schema(
                NestedField(field_id=1, name="x", field_type=LongType(), required=True),
                NestedField(field_id=2, name="y", field_type=LongType(), required=True, doc="comment"),
                NestedField(field_id=3, name="z", field_type=LongType(), required=True),
                schema_id=1,
                identifier_field_ids=[1, 2],
            ),
        ],
        current_schema_id=1,
        partition_specs=[
            PartitionSpec(PartitionField(source_id=1, field_id=1000, transform=IdentityTransform(), name="x"), spec_id=0)
        ],
        default_spec_id=0,
        last_partition_id=1000,
        properties={"read.split.target.size": "134217728"},
        current_snapshot_id=3055729675574597004,
        snapshots=[
            Snapshot(
                snapshot_id=3051729675574597004,
                parent_snapshot_id=None,
                sequence_number=0,
                timestamp_ms=1515100955770,
                manifest_list="s3://a/b/1.avro",
                summary=Summary(operation=Operation.APPEND),
                schema_id=None,
            ),
            Snapshot(
                snapshot_id=3055729675574597004,
                parent_snapshot_id=3051729675574597004,
                sequence_number=1,
                timestamp_ms=1555100955770,
                manifest_list="s3://a/b/2.avro",
                summary=Summary(operation=Operation.APPEND),
                schema_id=1,
            ),
        ],
        snapshot_log=[
            SnapshotLogEntry(snapshot_id=3051729675574597004, timestamp_ms=1515100955770),
            SnapshotLogEntry(snapshot_id=3055729675574597004, timestamp_ms=1555100955770),
        ],
        metadata_log=[MetadataLogEntry(metadata_file="s3://bucket/.../v1.json", timestamp_ms=1515100)],
        sort_orders=[
            SortOrder(
                SortField(
                    source_id=2, transform=IdentityTransform(), direction=SortDirection.ASC, null_order=NullOrder.NULLS_FIRST
                ),
                SortField(
                    source_id=3,
                    transform=BucketTransform(num_buckets=4),
                    direction=SortDirection.DESC,
                    null_order=NullOrder.NULLS_LAST,
                ),
                order_id=3,
            )
        ],
        default_sort_order_id=3,
        refs={
            "test": SnapshotRef(
                snapshot_id=3051729675574597004,
                snapshot_ref_type=SnapshotRefType.TAG,
                min_snapshots_to_keep=None,
                max_snapshot_age_ms=None,
                max_ref_age_ms=10000000,
            ),
            "main": SnapshotRef(
                snapshot_id=3055729675574597004,
                snapshot_ref_type=SnapshotRefType.BRANCH,
                min_snapshots_to_keep=None,
                max_snapshot_age_ms=None,
                max_ref_age_ms=None,
            ),
        },
        format_version=2,
        last_sequence_number=34,
    )

    assert table.name() == ("default", "new_tabl2e")
    assert expected == table.metadata


def test_rename_table(hive_table: HiveTable) -> None:
    catalog = HiveCatalog(HIVE_CATALOG_NAME, uri=HIVE_METASTORE_FAKE_URL)
    catalog.table_exists = MagicMock(return_value=False)  # type: ignore[method-assign]

    renamed_table = copy.deepcopy(hive_table)
    renamed_table.dbName = "default"
    renamed_table.tableName = "new_tabl3e"

    catalog._client = MagicMock()
    catalog._client.__enter__().get_table.side_effect = [hive_table, renamed_table]
    catalog._client.__enter__().alter_table_with_environment_context.return_value = None

    from_identifier = ("default", "new_tabl2e")
    to_identifier = ("default", "new_tabl3e")
    table = catalog.rename_table(from_identifier, to_identifier)

    assert table.name() == to_identifier

    calls = [call(dbname="default", tbl_name="new_tabl2e"), call(dbname="default", tbl_name="new_tabl3e")]
    catalog._client.__enter__().get_table.assert_has_calls(calls)
    catalog._client.__enter__().alter_table_with_environment_context.assert_called_with(
        dbname="default",
        tbl_name="new_tabl2e",
        new_tbl=renamed_table,
        environment_context=EnvironmentContext(properties={DO_NOT_UPDATE_STATS: DO_NOT_UPDATE_STATS_DEFAULT}),
    )


def test_rename_table_from_self_identifier(hive_table: HiveTable) -> None:
    catalog = HiveCatalog(HIVE_CATALOG_NAME, uri=HIVE_METASTORE_FAKE_URL)
    catalog.table_exists = MagicMock(return_value=False)  # type: ignore[method-assign]

    catalog._client = MagicMock()
    catalog._client.__enter__().get_table.return_value = hive_table

    from_identifier = ("default", "new_tabl2e")
    from_table = catalog.load_table(from_identifier)
    catalog._client.__enter__().get_table.assert_called_with(dbname="default", tbl_name="new_tabl2e")

    renamed_table = copy.deepcopy(hive_table)
    renamed_table.dbName = "default"
    renamed_table.tableName = "new_tabl3e"

    catalog._client.__enter__().get_table.side_effect = [hive_table, renamed_table]
    catalog._client.__enter__().alter_table_with_environment_context.return_value = None
    to_identifier = ("default", "new_tabl3e")
    table = catalog.rename_table(from_table.name(), to_identifier)

    assert table.name() == to_identifier

    calls = [call(dbname="default", tbl_name="new_tabl2e"), call(dbname="default", tbl_name="new_tabl3e")]
    catalog._client.__enter__().get_table.assert_has_calls(calls)
    catalog._client.__enter__().alter_table_with_environment_context.assert_called_with(
        dbname="default",
        tbl_name="new_tabl2e",
        new_tbl=renamed_table,
        environment_context=EnvironmentContext(properties={DO_NOT_UPDATE_STATS: DO_NOT_UPDATE_STATS_DEFAULT}),
    )


def test_rename_table_from_does_not_exists() -> None:
    catalog = HiveCatalog(HIVE_CATALOG_NAME, uri=HIVE_METASTORE_FAKE_URL)
    catalog.table_exists = MagicMock(return_value=False)  # type: ignore[method-assign]

    catalog._client = MagicMock()
    catalog._client.__enter__().alter_table_with_environment_context.side_effect = NoSuchObjectException(
        message="hive.default.does_not_exists table not found"
    )

    with pytest.raises(NoSuchTableError) as exc_info:
        catalog.rename_table(("default", "does_not_exists"), ("default", "new_table"))

    assert "Table does not exist: does_not_exists" in str(exc_info.value)


def test_rename_table_to_namespace_does_not_exists() -> None:
    catalog = HiveCatalog(HIVE_CATALOG_NAME, uri=HIVE_METASTORE_FAKE_URL)
    catalog.table_exists = MagicMock(return_value=False)  # type: ignore[method-assign]

    catalog._client = MagicMock()
    catalog._client.__enter__().alter_table_with_environment_context.side_effect = InvalidOperationException(
        message=(
            "Unable to change partition or table. Database default does not exist "
            "Check metastore logs for detailed stack.does_not_exists"
        )
    )

    with pytest.raises(NoSuchNamespaceError) as exc_info:
        catalog.rename_table(("default", "does_exists"), ("default_does_not_exists", "new_table"))

    assert "Database does not exists: default_does_not_exists" in str(exc_info.value)


def test_rename_table_to_table_already_exists(hive_table: HiveTable) -> None:
    catalog = HiveCatalog(HIVE_CATALOG_NAME, uri=HIVE_METASTORE_FAKE_URL)
    catalog.load_table = MagicMock(return_value=hive_table)  # type: ignore[method-assign]

    with pytest.raises(TableAlreadyExistsError) as exc_info:
        catalog.rename_table(("default", "some_table"), ("default", "new_tabl2e"))

    assert "Table already exists: new_tabl2e" in str(exc_info.value)


def test_drop_database_does_not_empty() -> None:
    catalog = HiveCatalog(HIVE_CATALOG_NAME, uri=HIVE_METASTORE_FAKE_URL)

    catalog._client = MagicMock()
    catalog._client.__enter__().drop_database.side_effect = InvalidOperationException(
        message="Database not_empty is not empty. One or more tables exist."
    )

    with pytest.raises(NamespaceNotEmptyError) as exc_info:
        catalog.drop_namespace(("not_empty",))

    assert "Database not_empty is not empty" in str(exc_info.value)


def test_drop_database_does_not_exists() -> None:
    catalog = HiveCatalog(HIVE_CATALOG_NAME, uri=HIVE_METASTORE_FAKE_URL)

    catalog._client = MagicMock()
    catalog._client.__enter__().drop_database.side_effect = MetaException(message="java.lang.NullPointerException")

    with pytest.raises(NoSuchNamespaceError) as exc_info:
        catalog.drop_namespace(("does_not_exists",))

    assert "Database does not exists: does_not_exists" in str(exc_info.value)


def test_list_tables(hive_table: HiveTable) -> None:
    catalog = HiveCatalog(HIVE_CATALOG_NAME, uri=HIVE_METASTORE_FAKE_URL)

    tbl1 = deepcopy(hive_table)
    tbl1.tableName = "table1"
    tbl1.dbName = "database"
    tbl2 = deepcopy(hive_table)
    tbl2.tableName = "table2"
    tbl2.dbName = "database"
    tbl3 = deepcopy(hive_table)
    tbl3.tableName = "table3"
    tbl3.dbName = "database"
    tbl3.parameters["table_type"] = "non_iceberg"
    tbl4 = deepcopy(hive_table)
    tbl4.tableName = "table4"
    tbl4.dbName = "database"
    tbl4.parameters.pop("table_type")

    catalog._client = MagicMock()
    catalog._client.__enter__().get_all_tables.return_value = ["table1", "table2", "table3", "table4"]
    catalog._client.__enter__().get_table_objects_by_name.return_value = [tbl1, tbl2, tbl3, tbl4]

    got_tables = catalog.list_tables("database")
    assert got_tables == [("database", "table1"), ("database", "table2")]
    catalog._client.__enter__().get_all_tables.assert_called_with(db_name="database")
    catalog._client.__enter__().get_table_objects_by_name.assert_called_with(
        dbname="database", tbl_names=["table1", "table2", "table3", "table4"]
    )


def test_list_namespaces() -> None:
    catalog = HiveCatalog(HIVE_CATALOG_NAME, uri=HIVE_METASTORE_FAKE_URL)

    catalog._client = MagicMock()
    catalog._client.__enter__().get_all_databases.return_value = ["namespace1", "namespace2"]

    assert catalog.list_namespaces() == [("namespace1",), ("namespace2",)]

    catalog._client.__enter__().get_all_databases.assert_called()


def test_drop_table() -> None:
    catalog = HiveCatalog(HIVE_CATALOG_NAME, uri=HIVE_METASTORE_FAKE_URL)

    catalog._client = MagicMock()
    catalog._client.__enter__().get_all_databases.return_value = ["namespace1", "namespace2"]

    catalog.drop_table(("default", "table"))

    catalog._client.__enter__().drop_table.assert_called_with(dbname="default", name="table", deleteData=False)


def test_drop_table_from_self_identifier(hive_table: HiveTable) -> None:
    catalog = HiveCatalog(HIVE_CATALOG_NAME, uri=HIVE_METASTORE_FAKE_URL)

    catalog._client = MagicMock()
    catalog._client.__enter__().get_table.return_value = hive_table
    table = catalog.load_table(("default", "new_tabl2e"))

    catalog._client.__enter__().get_all_databases.return_value = ["namespace1", "namespace2"]
    catalog.drop_table(table.name())

    catalog._client.__enter__().drop_table.assert_called_with(dbname="default", name="new_tabl2e", deleteData=False)


def test_drop_table_does_not_exists() -> None:
    catalog = HiveCatalog(HIVE_CATALOG_NAME, uri=HIVE_METASTORE_FAKE_URL)

    catalog._client = MagicMock()
    catalog._client.__enter__().drop_table.side_effect = NoSuchObjectException(message="does_not_exists")

    with pytest.raises(NoSuchTableError) as exc_info:
        catalog.drop_table(("default", "does_not_exists"))

    assert "Table does not exists: does_not_exists" in str(exc_info.value)


def test_purge_table() -> None:
    catalog = HiveCatalog(HIVE_CATALOG_NAME, uri=HIVE_METASTORE_FAKE_URL)

    with pytest.raises(NotImplementedError):
        catalog.purge_table(("default", "does_not_exists"))


def test_create_database() -> None:
    catalog = HiveCatalog(HIVE_CATALOG_NAME, uri=HIVE_METASTORE_FAKE_URL)

    catalog._client = MagicMock()
    catalog._client.__enter__().create_database.return_value = None

    catalog.create_namespace("default", {"property": "true"})

    catalog._client.__enter__().create_database.assert_called_with(
        HiveDatabase(
            name="default",
            description=None,
            locationUri=None,
            parameters={"property": "true"},
            privileges=None,
            ownerName=None,
            ownerType=None,
            catalogName=None,
            createTime=None,
            managedLocationUri=None,
            type=None,
            connector_name=None,
            remote_dbname=None,
        )
    )


def test_create_database_already_exists() -> None:
    catalog = HiveCatalog(HIVE_CATALOG_NAME, uri=HIVE_METASTORE_FAKE_URL)

    catalog._client = MagicMock()
    catalog._client.__enter__().create_database.side_effect = AlreadyExistsException(message="Database default already exists")

    with pytest.raises(NamespaceAlreadyExistsError) as exc_info:
        catalog.create_namespace("default")

    assert "Database default already exists" in str(exc_info.value)


def test_load_namespace_properties(hive_database: HiveDatabase) -> None:
    catalog = HiveCatalog(HIVE_CATALOG_NAME, uri=HIVE_METASTORE_FAKE_URL)

    catalog._client = MagicMock()
    catalog._client.__enter__().get_database.return_value = hive_database

    assert catalog.load_namespace_properties("default2") == {"location": hive_database.locationUri, "test": "property"}

    catalog._client.__enter__().get_database.assert_called_with(name="default2")


def test_load_namespace_properties_does_not_exists() -> None:
    catalog = HiveCatalog(HIVE_CATALOG_NAME, uri=HIVE_METASTORE_FAKE_URL)

    catalog._client = MagicMock()
    catalog._client.__enter__().get_database.side_effect = NoSuchObjectException(message="does_not_exists")

    with pytest.raises(NoSuchNamespaceError) as exc_info:
        catalog.load_namespace_properties(("does_not_exists",))

    assert "Database does not exists: does_not_exists" in str(exc_info.value)


def test_update_namespace_properties(hive_database: HiveDatabase) -> None:
    catalog = HiveCatalog(HIVE_CATALOG_NAME, uri=HIVE_METASTORE_FAKE_URL)

    catalog._client = MagicMock()
    catalog._client.__enter__().get_database.return_value = hive_database
    catalog._client.__enter__().alter_database.return_value = None

    assert catalog.update_namespace_properties(
        namespace="default", removals={"test", "does_not_exists"}, updates={"label": "core"}
    ) == PropertiesUpdateSummary(removed=["test"], updated=["label"], missing=["does_not_exists"])

    catalog._client.__enter__().alter_database.assert_called_with(
        "default",
        HiveDatabase(
            name="default",
            description=None,
            locationUri=hive_database.locationUri,
            parameters={"label": "core"},
            privileges=None,
            ownerName=None,
            ownerType=1,
            catalogName="hive",
            createTime=None,
            managedLocationUri=None,
            type=None,
            connector_name=None,
            remote_dbname=None,
        ),
    )


def test_update_namespace_properties_namespace_does_not_exists() -> None:
    catalog = HiveCatalog(HIVE_CATALOG_NAME, uri=HIVE_METASTORE_FAKE_URL)

    catalog._client = MagicMock()
    catalog._client.__enter__().get_database.side_effect = NoSuchObjectException(message="does_not_exists")

    with pytest.raises(NoSuchNamespaceError) as exc_info:
        catalog.update_namespace_properties(("does_not_exists",), removals=set(), updates={})

    assert "Database does not exists: does_not_exists" in str(exc_info.value)


def test_update_namespace_properties_overlap() -> None:
    catalog = HiveCatalog(HIVE_CATALOG_NAME, uri=HIVE_METASTORE_FAKE_URL)

    with pytest.raises(ValueError) as exc_info:
        catalog.update_namespace_properties(("table",), removals=set("a"), updates={"a": "b"})

    assert "Updates and deletes have an overlap: {'a'}" in str(exc_info.value)


def test_construct_hive_storage_descriptor_simple(table_schema_simple: Schema) -> None:
    descriptor = _construct_hive_storage_descriptor(table_schema_simple, "s3://")
    assert descriptor == StorageDescriptor(
        cols=[
            FieldSchema(name="foo", type="string", comment=None),
            FieldSchema(name="bar", type="int", comment=None),
            FieldSchema(name="baz", type="boolean", comment=None),
        ],
        location="s3://",
        inputFormat="org.apache.hadoop.mapred.FileInputFormat",
        outputFormat="org.apache.hadoop.mapred.FileOutputFormat",
        compressed=None,
        numBuckets=None,
        serdeInfo=SerDeInfo(
            name=None,
            serializationLib="org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
            parameters=None,
            description=None,
            serializerClass=None,
            deserializerClass=None,
            serdeType=None,
        ),
        bucketCols=None,
        sortCols=None,
        parameters=None,
        skewedInfo=None,
        storedAsSubDirectories=None,
    )


def test_construct_hive_storage_descriptor_nested(table_schema_nested: Schema) -> None:
    descriptor = _construct_hive_storage_descriptor(table_schema_nested, "s3://")
    assert descriptor == StorageDescriptor(
        cols=[
            FieldSchema(name="foo", type="string", comment=None),
            FieldSchema(name="bar", type="int", comment=None),
            FieldSchema(name="baz", type="boolean", comment=None),
            FieldSchema(name="qux", type="array<string>", comment=None),
            FieldSchema(name="quux", type="map<string,map<string,int>>", comment=None),
            FieldSchema(name="location", type="array<struct<latitude:float,longitude:float>>", comment=None),
            FieldSchema(name="person", type="struct<name:string,age:int>", comment=None),
        ],
        location="s3://",
        inputFormat="org.apache.hadoop.mapred.FileInputFormat",
        outputFormat="org.apache.hadoop.mapred.FileOutputFormat",
        compressed=None,
        numBuckets=None,
        serdeInfo=SerDeInfo(
            name=None,
            serializationLib="org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
            parameters=None,
            description=None,
            serializerClass=None,
            deserializerClass=None,
            serdeType=None,
        ),
        bucketCols=None,
        sortCols=None,
        parameters=None,
        skewedInfo=None,
        storedAsSubDirectories=None,
    )


def test_resolve_table_location_warehouse(hive_database: HiveDatabase) -> None:
    catalog = HiveCatalog(HIVE_CATALOG_NAME, warehouse="/tmp/warehouse/", uri=HIVE_METASTORE_FAKE_URL)

    # Set this one to None, so we'll fall back to the properties
    hive_database.locationUri = None

    catalog._client = MagicMock()
    catalog._client.__enter__().get_database.return_value = hive_database

    location = catalog._resolve_table_location(None, "database", "table")
    assert location == "/tmp/warehouse/database.db/table"


def test_hive_wait_for_lock() -> None:
    lockid = 12345
    acquired = LockResponse(lockid=lockid, state=LockState.ACQUIRED)
    waiting = LockResponse(lockid=lockid, state=LockState.WAITING)
    prop = {
        "uri": HIVE_METASTORE_FAKE_URL,
        LOCK_CHECK_MIN_WAIT_TIME: 0.1,
        LOCK_CHECK_MAX_WAIT_TIME: 0.5,
        LOCK_CHECK_RETRIES: 5,
    }
    catalog = HiveCatalog(HIVE_CATALOG_NAME, **prop)  # type: ignore
    catalog._client = MagicMock()
    catalog._client.lock.return_value = LockResponse(lockid=lockid, state=LockState.WAITING)

    # lock will be acquired after 3 retries
    catalog._client.check_lock.side_effect = [waiting if i < 2 else acquired for i in range(10)]
    response: LockResponse = catalog._wait_for_lock("db", "tbl", lockid, catalog._client)
    assert response.state == LockState.ACQUIRED
    assert catalog._client.check_lock.call_count == 3

    # lock wait should exit with WaitingForLockException finally after enough retries
    catalog._client.check_lock.side_effect = [waiting for _ in range(10)]
    catalog._client.check_lock.call_count = 0
    with pytest.raises(WaitingForLockException):
        catalog._wait_for_lock("db", "tbl", lockid, catalog._client)
    assert catalog._client.check_lock.call_count == 5


def test_create_hive_client_success() -> None:
    properties = {"uri": "thrift://localhost:10000", "ugi": "user"}

    with patch("pyiceberg.catalog.hive._HiveClient", return_value=MagicMock()) as mock_hive_client:
        client = HiveCatalog._create_hive_client(properties)
        mock_hive_client.assert_called_once_with("thrift://localhost:10000", "user", False, "hive")
        assert client is not None


def test_create_hive_client_with_kerberos_success() -> None:
    properties = {
        "uri": "thrift://localhost:10000",
        "ugi": "user",
        HIVE_KERBEROS_AUTH: "true",
        HIVE_KERBEROS_SERVICE_NAME: "hiveuser",
    }
    with patch("pyiceberg.catalog.hive._HiveClient", return_value=MagicMock()) as mock_hive_client:
        client = HiveCatalog._create_hive_client(properties)
        mock_hive_client.assert_called_once_with("thrift://localhost:10000", "user", True, "hiveuser")
        assert client is not None


def test_create_hive_client_multiple_uris() -> None:
    properties = {"uri": "thrift://localhost:10000,thrift://localhost:10001", "ugi": "user"}

    with patch("pyiceberg.catalog.hive._HiveClient") as mock_hive_client:
        mock_hive_client.side_effect = [Exception("Connection failed"), MagicMock()]

        client = HiveCatalog._create_hive_client(properties)
        assert mock_hive_client.call_count == 2
        mock_hive_client.assert_has_calls(
            [call("thrift://localhost:10000", "user", False, "hive"), call("thrift://localhost:10001", "user", False, "hive")]
        )
        assert client is not None


def test_create_hive_client_failure() -> None:
    properties = {"uri": "thrift://localhost:10000,thrift://localhost:10001", "ugi": "user"}

    with patch("pyiceberg.catalog.hive._HiveClient", side_effect=Exception("Connection failed")) as mock_hive_client:
        with pytest.raises(Exception, match="Connection failed"):
            HiveCatalog._create_hive_client(properties)
        assert mock_hive_client.call_count == 2


def test_create_hive_client_with_kerberos(
    kerberized_hive_metastore_fake_url: str,
) -> None:
    properties = {
        "uri": kerberized_hive_metastore_fake_url,
        "ugi": "user",
        HIVE_KERBEROS_AUTH: "true",
    }
    client = HiveCatalog._create_hive_client(properties)
    assert client is not None


def test_create_hive_client_with_kerberos_using_context_manager(
    kerberized_hive_metastore_fake_url: str,
) -> None:
    client = _HiveClient(
        uri=kerberized_hive_metastore_fake_url,
        kerberos_auth=True,
    )
    with (
        patch(
            "puresasl.mechanisms.kerberos.authGSSClientStep",
            return_value=None,
        ),
        patch(
            "puresasl.mechanisms.kerberos.authGSSClientResponse",
            return_value=base64.b64encode(b"Some Response"),
        ),
        patch(
            "puresasl.mechanisms.GSSAPIMechanism.complete",
            return_value=True,
        ),
    ):
        with client as open_client:
            assert open_client._iprot.trans.isOpen()

        assert not open_client._iprot.trans.isOpen()
        # Use the context manager a second time to see if
        # closing and re-opening work as expected.
        with client as open_client:
            assert open_client._iprot.trans.isOpen()
