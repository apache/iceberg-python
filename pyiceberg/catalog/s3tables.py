import re
from typing import TYPE_CHECKING, List, Optional, Set, Tuple, Union

import boto3

from pyiceberg.catalog import DEPRECATED_BOTOCORE_SESSION, MetastoreCatalog, PropertiesUpdateSummary
from pyiceberg.exceptions import (
    CommitFailedException,
    InvalidNamespaceName,
    InvalidTableName,
    NamespaceNotEmptyError,
    NoSuchNamespaceError,
    NoSuchTableError,
    S3TablesError,
    TableAlreadyExistsError,
    TableBucketNotFound,
)
from pyiceberg.io import AWS_ACCESS_KEY_ID, AWS_REGION, AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN, load_file_io
from pyiceberg.partitioning import UNPARTITIONED_PARTITION_SPEC, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.serializers import FromInputFile
from pyiceberg.table import CommitTableResponse, Table
from pyiceberg.table.metadata import new_table_metadata
from pyiceberg.table.sorting import UNSORTED_SORT_ORDER, SortOrder
from pyiceberg.table.update import TableRequirement, TableUpdate
from pyiceberg.typedef import EMPTY_DICT, Identifier, Properties
from pyiceberg.utils.properties import get_first_property_value

if TYPE_CHECKING:
    import pyarrow as pa

S3TABLES_PROFILE_NAME = "s3tables.profile-name"
S3TABLES_REGION = "s3tables.region"
S3TABLES_ACCESS_KEY_ID = "s3tables.access-key-id"
S3TABLES_SECRET_ACCESS_KEY = "s3tables.secret-access-key"
S3TABLES_SESSION_TOKEN = "s3tables.session-token"

S3TABLES_TABLE_BUCKET_ARN = "s3tables.warehouse"

S3TABLES_ENDPOINT = "s3tables.endpoint"

# for naming rules see: https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-tables-buckets-naming.html
S3TABLES_VALID_NAME_REGEX = pattern = re.compile("[a-z0-9][a-z0-9_]{1,61}[a-z0-9]")
S3TABLES_RESERVED_NAMESPACE = "aws_s3_metadata"

S3TABLES_FORMAT = "ICEBERG"


class S3TablesCatalog(MetastoreCatalog):
    def __init__(self, name: str, **properties: str):
        super().__init__(name, **properties)

        self.table_bucket_arn = self.properties[S3TABLES_TABLE_BUCKET_ARN]

        session = boto3.Session(
            profile_name=properties.get(S3TABLES_PROFILE_NAME),
            region_name=get_first_property_value(properties, S3TABLES_REGION, AWS_REGION),
            botocore_session=properties.get(DEPRECATED_BOTOCORE_SESSION),
            aws_access_key_id=get_first_property_value(properties, S3TABLES_ACCESS_KEY_ID, AWS_ACCESS_KEY_ID),
            aws_secret_access_key=get_first_property_value(properties, S3TABLES_SECRET_ACCESS_KEY, AWS_SECRET_ACCESS_KEY),
            aws_session_token=get_first_property_value(properties, S3TABLES_SESSION_TOKEN, AWS_SESSION_TOKEN),
        )
        try:
            self.s3tables = session.client("s3tables", endpoint_url=properties.get(S3TABLES_ENDPOINT))
        except boto3.session.UnknownServiceError as e:
            raise S3TablesError("'s3tables' requires boto3>=1.35.74. Current version: {boto3.__version__}.") from e

        try:
            self.s3tables.get_table_bucket(tableBucketARN=self.table_bucket_arn)
        except self.s3tables.exceptions.NotFoundException as e:
            raise TableBucketNotFound(e) from e

    def commit_table(
        self, table: Table, requirements: Tuple[TableRequirement, ...], updates: Tuple[TableUpdate, ...]
    ) -> CommitTableResponse:
        table_identifier = table.name()
        database_name, table_name = self.identifier_to_database_and_table(table_identifier, NoSuchTableError)

        current_table, version_token = self._load_table_and_version(identifier=table_identifier)

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

    def create_namespace(self, namespace: Union[str, Identifier], properties: Properties = EMPTY_DICT) -> None:
        if properties:
            raise NotImplementedError("Setting namespace properties is not supported.")
        valid_namespace: str = self._validate_namespace_identifier(namespace)
        self.s3tables.create_namespace(tableBucketARN=self.table_bucket_arn, namespace=[valid_namespace])

    def _validate_namespace_identifier(self, namespace: Union[str, Identifier]) -> str:
        namespace = self.identifier_to_database(namespace)

        if not S3TABLES_VALID_NAME_REGEX.fullmatch(namespace) or namespace == S3TABLES_RESERVED_NAMESPACE:
            raise InvalidNamespaceName(
                "The specified namespace name is not valid. See https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-tables-buckets-naming.html for naming rules."
            )

        return namespace

    def _validate_database_and_table_identifier(self, identifier: Union[str, Identifier]) -> Tuple[str, str]:
        namespace, table_name = self.identifier_to_database_and_table(identifier)

        namespace = self._validate_namespace_identifier(namespace)

        if not S3TABLES_VALID_NAME_REGEX.fullmatch(table_name):
            raise InvalidTableName(
                "The specified table name is not valid. See https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-tables-buckets-naming.html for naming rules."
            )

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
        if location:
            raise NotImplementedError("S3 Tables does not support user specified table locations.")
        namespace, table_name = self._validate_database_and_table_identifier(identifier)

        schema: Schema = self._convert_schema_if_needed(schema)  # type: ignore

        # creating a new table with S3 Tables is a two step process. We first have to create an S3 Table with the
        # S3 Tables API and then write the new metadata.json to the warehouseLocation associated with the newly
        # created S3 Table.
        try:
            version_token = self.s3tables.create_table(
                tableBucketARN=self.table_bucket_arn, namespace=namespace, name=table_name, format=S3TABLES_FORMAT
            )["versionToken"]
        except self.s3tables.exceptions.NotFoundException as e:
            raise NoSuchNamespaceError(f"Cannot create {namespace}.{table_name} because no such namespace exists.") from e
        except self.s3tables.exceptions.ConflictException as e:
            raise TableAlreadyExistsError(
                f"Cannot create {namespace}.{table_name} because a table of the same name already exists in the namespace."
            ) from e

        try:
            response = self.s3tables.get_table_metadata_location(
                tableBucketARN=self.table_bucket_arn, namespace=namespace, name=table_name
            )
            warehouse_location = response["warehouseLocation"]

            metadata_location = self._get_metadata_location(location=warehouse_location)
            metadata = new_table_metadata(
                location=warehouse_location,
                schema=schema,
                partition_spec=partition_spec,
                sort_order=sort_order,
                properties=properties,
            )

            io = load_file_io(properties=self.properties, location=metadata_location)
            # this triggers unsupported list operation error as S3 Table Buckets only support a subset of the S3 Bucket API,
            # setting overwrite=True is a workaround for now since it prevents a call to list_objects
            self._write_metadata(metadata, io, metadata_location, overwrite=True)

            try:
                self.s3tables.update_table_metadata_location(
                    tableBucketARN=self.table_bucket_arn,
                    namespace=namespace,
                    name=table_name,
                    versionToken=version_token,
                    metadataLocation=metadata_location,
                )
            except self.s3tables.exceptions.ConflictException as e:
                raise CommitFailedException(
                    f"Cannot create {namespace}.{table_name} because of a concurrent update to the table version {version_token}."
                ) from e
        except:
            self.s3tables.delete_table(tableBucketARN=self.table_bucket_arn, namespace=namespace, name=table_name)
            raise

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

        version_token = response["versionToken"]
        try:
            self.s3tables.delete_table(
                tableBucketARN=self.table_bucket_arn,
                namespace=namespace,
                name=table_name,
                versionToken=version_token,
            )
        except self.s3tables.exceptions.ConflictException as e:
            raise CommitFailedException(
                f"Cannot delete {namespace}.{table_name} because of a concurrent update to the table version {version_token}."
            ) from e

    def list_namespaces(self, namespace: Union[str, Identifier] = ()) -> List[Identifier]:
        if namespace:
            # hierarchical namespaces are not supported
            return []
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
        table, _ = self._load_table_and_version(identifier)
        return table

    def _load_table_and_version(self, identifier: Union[str, Identifier]) -> Tuple[Table, str]:
        namespace, table_name = self._validate_database_and_table_identifier(identifier)

        try:
            response = self.s3tables.get_table_metadata_location(
                tableBucketARN=self.table_bucket_arn, namespace=namespace, name=table_name
            )
        except self.s3tables.exceptions.NotFoundException as e:
            raise NoSuchTableError(f"No table with identifier {identifier} exists.") from e

        metadata_location = response.get("metadataLocation")
        if not metadata_location:
            raise S3TablesError("No table metadata found.")

        version_token = response["versionToken"]

        io = load_file_io(properties=self.properties, location=metadata_location)
        file = io.new_input(metadata_location)
        metadata = FromInputFile.table_metadata(file)
        return Table(
            identifier=(namespace, table_name),
            metadata=metadata,
            metadata_location=metadata_location,
            io=self._load_file_io(metadata.properties, metadata_location),
            catalog=self,
        ), version_token

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
        raise NotImplementedError("Namespace properties are read only.")

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
