import re
from typing import TYPE_CHECKING, List, Optional, Set, Tuple, Union

import boto3
from boto3.session import UnknownServiceError

from pyiceberg.catalog import DEPRECATED_BOTOCORE_SESSION, WAREHOUSE_LOCATION, MetastoreCatalog, PropertiesUpdateSummary
from pyiceberg.exceptions import (
    CommitFailedException,
    NamespaceNotEmptyError,
    NoSuchTableError,
    TableBucketNotFound,
    S3TablesError
)
from pyiceberg.io import AWS_ACCESS_KEY_ID, AWS_REGION, AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN, PY_IO_IMPL, load_file_io
from pyiceberg.partitioning import UNPARTITIONED_PARTITION_SPEC, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.serializers import FromInputFile
from pyiceberg.table import CommitTableResponse, Table, TableRequirement
from pyiceberg.table.metadata import new_table_metadata
from pyiceberg.table.sorting import UNSORTED_SORT_ORDER, SortOrder
from pyiceberg.table.update import TableUpdate
from pyiceberg.typedef import EMPTY_DICT, Identifier, Properties
from pyiceberg.utils.properties import get_first_property_value

if TYPE_CHECKING:
    import pyarrow as pa

S3TABLES_PROFILE_NAME = "s3tables.profile-name"
S3TABLES_REGION = "s3tables.region"
S3TABLES_ACCESS_KEY_ID = "s3tables.access-key-id"
S3TABLES_SECRET_ACCESS_KEY = "s3tables.secret-access-key"
S3TABLES_SESSION_TOKEN = "s3tables.session-token"

S3TABLES_ENDPOINT = "s3tables.endpoint"

# pyarrow does not support writing to S3 Table buckets as of 2024-12-14 https://github.com/apache/iceberg-python/issues/1404#issuecomment-2543174146
S3TABLES_FILE_IO_DEFAULT = "pyiceberg.io.fsspec.FsspecFileIO"


class S3TableCatalog(MetastoreCatalog):
    def __init__(self, name: str, **properties: str):
        super().__init__(name, **properties)
        # TODO: implement a proper check for FileIO
        self.properties[PY_IO_IMPL] = S3TABLES_FILE_IO_DEFAULT

        session = boto3.Session(
            profile_name=properties.get(S3TABLES_PROFILE_NAME),
            region_name=get_first_property_value(properties, S3TABLES_REGION, AWS_REGION),
            botocore_session=properties.get(DEPRECATED_BOTOCORE_SESSION),
            aws_access_key_id=get_first_property_value(properties, S3TABLES_ACCESS_KEY_ID, AWS_ACCESS_KEY_ID),
            aws_secret_access_key=get_first_property_value(properties, S3TABLES_SECRET_ACCESS_KEY, AWS_SECRET_ACCESS_KEY),
            aws_session_token=get_first_property_value(properties, S3TABLES_SESSION_TOKEN, AWS_SESSION_TOKEN),
        )
        try:
            self.s3tables = session.client("s3tables")
        except UnknownServiceError as e:
            raise S3TablesError("'s3tables' requires boto3>=1.35.74. Current version: {boto3.__version__}.") from e

        # TODO: handle malformed properties instead of just raising a key error here
        self.table_bucket_arn = self.properties[WAREHOUSE_LOCATION]
        try:
            self.s3tables.get_table_bucket(tableBucketARN=self.table_bucket_arn)
        except self.s3tables.exceptions.NotFoundException as e:
            raise TableBucketNotFound(e) from e

    def commit_table(
        self, table: Table, requirements: Tuple[TableRequirement, ...], updates: Tuple[TableUpdate, ...]
    ) -> CommitTableResponse:
        table_identifier = table.name()
        database_name, table_name = self.identifier_to_database_and_table(table_identifier, NoSuchTableError)

        # TODO: loading table and getting versionToken should be an atomic operation, otherwise
        # the table can change between those two API calls without noticing
        # -> change this into an internal API that returns table information along with versionToken
        current_table = self.load_table(identifier=table_identifier)
        version_token = self.s3tables.get_table(tableBucketARN=self.table_bucket_arn, namespace=database_name, name=table_name)[
            "versionToken"
        ]

        updated_staged_table = self._update_and_stage_table(current_table, table_identifier, requirements, updates)
        if current_table and updated_staged_table.metadata == current_table.metadata:
            # no changes, do nothing
            return CommitTableResponse(metadata=current_table.metadata, metadata_location=current_table.metadata_location)

        self._write_metadata(
            metadata=updated_staged_table.metadata,
            io=updated_staged_table.io,
            metadata_path=updated_staged_table.metadata_location,
            overwrite=True,
        )

        # try to update metadata location which will fail if the versionToken changed meanwhile
        try:
            self.s3tables.update_table_metadata_location(
                tableBucketARN=self.table_bucket_arn,
                namespace=database_name,
                name=table_name,
                versionToken=version_token,
                metadataLocation=updated_staged_table.metadata_location,
            )
        except self.s3tables.exceptions.ConflictException as e:
            raise CommitFailedException(
                f"Cannot commit {database_name}.{table_name} because of a concurrent update to the table version {version_token}."
            ) from e
        return CommitTableResponse(
            metadata=updated_staged_table.metadata, metadata_location=updated_staged_table.metadata_location
        )

    def create_namespace(self, namespace: Union[str, Identifier], properties: Properties = ...) -> None:
        valid_namespace: str = self._validate_namespace_identifier(namespace)
        self.s3tables.create_namespace(tableBucketARN=self.table_bucket_arn, namespace=[valid_namespace])

    def _validate_namespace_identifier(self, namespace: Union[str, Identifier]) -> str:
        # for naming rules see: https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-tables-buckets-naming.html
        # TODO: extract into constant variables
        pattern = re.compile("[a-z0-9][a-z0-9_]{2,62}")
        reserved = "aws_s3_metadata"

        namespace = self.identifier_to_database(namespace)

        if not pattern.fullmatch(namespace):
            ...

        if namespace == reserved:
            ...

        return namespace

    def _validate_database_and_table_identifier(self, identifier: Union[str, Identifier]) -> Tuple[str, str]:
        # for naming rules see: https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-tables-buckets-naming.html
        # TODO: extract into constant variables
        pattern = re.compile("[a-z0-9][a-z0-9_]{2,62}")

        namespace, table_name = self.identifier_to_database_and_table(identifier)

        namespace = self._validate_namespace_identifier(namespace)

        if not pattern.fullmatch(table_name):
            # TODO: raise proper errors for invalid table_name
            ...

        return namespace, table_name

    def create_table(
        self,
        identifier: Union[str, Identifier],
        schema: Union[Schema, "pa.Schema"],
        location: Optional[str] = None,
        partition_spec: PartitionSpec = UNPARTITIONED_PARTITION_SPEC,
        sort_order: SortOrder = UNSORTED_SORT_ORDER,
        properties: Properties = EMPTY_DICT,
    ) -> Table:
        namespace, table_name = self._validate_database_and_table_identifier(identifier)

        schema: Schema = self._convert_schema_if_needed(schema)  # type: ignore

        # TODO: check whether namespace exists and if it does, whether table_name already exists
        self.s3tables.create_table(tableBucketARN=self.table_bucket_arn, namespace=namespace, name=table_name, format="ICEBERG")

        # location is given by s3 table bucket
        response = self.s3tables.get_table(tableBucketARN=self.table_bucket_arn, namespace=namespace, name=table_name)
        version_token = response["versionToken"]

        location = response["warehouseLocation"]
        metadata_location = self._get_metadata_location(location=location)
        metadata = new_table_metadata(
            location=location,
            schema=schema,
            partition_spec=partition_spec,
            sort_order=sort_order,
            properties=properties,
        )

        io = load_file_io(properties=self.properties, location=metadata_location)
        # TODO: this triggers unsupported list operation error, setting overwrite=True is a workaround for now
        # TODO: we can perform this check manually maybe?
        self._write_metadata(metadata, io, metadata_location, overwrite=True)
        # TODO: after writing need to update table metadata location
        # can this operation fail if the version token does not match?
        self.s3tables.update_table_metadata_location(
            tableBucketARN=self.table_bucket_arn,
            namespace=namespace,
            name=table_name,
            versionToken=version_token,
            metadataLocation=metadata_location,
        )

        return self.load_table(identifier=identifier)

    def drop_namespace(self, namespace: Union[str, Identifier]) -> None:
        namespace = self._validate_namespace_identifier(namespace)
        try:
            self.s3tables.delete_namespace(tableBucketARN=self.table_bucket_arn, namespace=namespace)
        except self.s3tables.exceptions.ConflictException as e:
            raise NamespaceNotEmptyError(f"Namespace {namespace} is not empty.") from e

    def drop_table(self, identifier: Union[str, Identifier]) -> None:
        namespace, table_name = self._validate_database_and_table_identifier(identifier)
        try:
            response = self.s3tables.get_table(tableBucketARN=self.table_bucket_arn, namespace=namespace, name=table_name)
        except self.s3tables.exceptions.NotFoundException as e:
            raise NoSuchTableError(f"No table with identifier {identifier} exists.") from e

        # TODO: handle conflicts due to versionToken mismatch that might occur
        self.s3tables.delete_table(
            tableBucketARN=self.table_bucket_arn,
            namespace=namespace,
            name=table_name,
            versionToken=response["versionToken"],
        )

    def list_namespaces(self, namespace: Union[str, Identifier] = ()) -> List[Identifier]:
        # TODO: s3tables only support single level namespaces
        if namespace:
            namespace = self._validate_namespace_identifier(namespace)
        paginator = self.s3tables.get_paginator("list_namespaces")

        namespaces: List[Identifier] = []
        for page in paginator.paginate(tableBucketARN=self.table_bucket_arn):
            namespaces.extend(tuple(entry["namespace"]) for entry in page["namespaces"])

        return namespaces

    def list_tables(self, namespace: Union[str, Identifier]) -> List[Identifier]:
        namespace = self._validate_namespace_identifier(namespace)
        paginator = self.s3tables.get_paginator("list_tables")
        tables: List[Identifier] = []
        for page in paginator.paginate(tableBucketARN=self.table_bucket_arn, namespace=namespace):
            tables.extend((namespace, table["name"]) for table in page["tables"])
        return tables

    def load_namespace_properties(self, namespace: Union[str, Identifier]) -> Properties:
        namespace = self._validate_namespace_identifier(namespace)
        response = self.s3tables.get_namespace(tableBucketARN=self.table_bucket_arn, namespace=namespace)
        return {
            "namespace": response["namespace"],
            "createdAt": response["createdAt"],
            "createdBy": response["createdBy"],
            "ownerAccountId": response["ownerAccountId"],
        }

    def load_table(self, identifier: Union[str, Identifier]) -> Table:
        namespace, table_name = self._validate_database_and_table_identifier(identifier)
        # TODO: raise a NoSuchTableError if it does not exist
        try:
            response = self.s3tables.get_table_metadata_location(
                tableBucketARN=self.table_bucket_arn, namespace=namespace, name=table_name
            )
        except self.s3tables.exceptions.NotFoundException as e:
            raise NoSuchTableError(f"No table with identifier {identifier} exists.") from e
        # TODO: we might need to catch if table is not initialized i.e. does not have metadata setup yet
        metadata_location = response["metadataLocation"]

        io = load_file_io(properties=self.properties, location=metadata_location)
        file = io.new_input(metadata_location)
        metadata = FromInputFile.table_metadata(file)
        return Table(
            identifier=(namespace, table_name),
            metadata=metadata,
            metadata_location=metadata_location,
            io=self._load_file_io(metadata.properties, metadata_location),
            catalog=self,
        )

    def rename_table(self, from_identifier: Union[str, Identifier], to_identifier: Union[str, Identifier]) -> Table:
        from_namespace, from_table_name = self._validate_database_and_table_identifier(from_identifier)
        to_namespace, to_table_name = self._validate_database_and_table_identifier(to_identifier)

        version_token = self.s3tables.get_table(
            tableBucketARN=self.table_bucket_arn, namespace=from_namespace, name=from_table_name
        )["versionToken"]

        self.s3tables.rename_table(
            tableBucketARN=self.table_bucket_arn,
            namespace=from_namespace,
            name=from_table_name,
            newNamespaceName=to_namespace,
            newName=to_table_name,
            versionToken=version_token,
        )

        return self.load_table(to_identifier)

    def table_exists(self, identifier: Union[str, Identifier]) -> bool:
        namespace, table_name = self._validate_database_and_table_identifier(identifier)
        try:
            self.s3tables.get_table(tableBucketARN=self.table_bucket_arn, namespace=namespace, name=table_name)
        except self.s3tables.exceptions.NotFoundException:
            return False
        return True

    def update_namespace_properties(
        self, namespace: Union[str, Identifier], removals: Optional[Set[str]] = None, updates: Properties = EMPTY_DICT
    ) -> PropertiesUpdateSummary:
        # namespace properties are read only
        raise NotImplementedError

    def purge_table(self, identifier: Union[str, Identifier]) -> None:
        # purge is not supported as s3tables doesn't support delete operations
        # table maintenance is automated
        raise NotImplementedError

    def register_table(self, identifier: Union[str, Identifier], metadata_location: str) -> Table:
        raise NotImplementedError

    def drop_view(self, identifier: Union[str, Identifier]) -> None:
        raise NotImplementedError

    def list_views(self, namespace: Union[str, Identifier]) -> List[Identifier]:
        raise NotImplementedError
