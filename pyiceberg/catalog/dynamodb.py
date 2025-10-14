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
import logging
import threading
import uuid
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from time import time
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    List,
    Optional,
    Set,
    Tuple,
    Union,
)

import boto3
from tenacity import (
    before_sleep_log,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from pyiceberg.catalog import (
    BOTOCORE_SESSION,
    ICEBERG,
    METADATA_LOCATION,
    PREVIOUS_METADATA_LOCATION,
    TABLE_TYPE,
    MetastoreCatalog,
    PropertiesUpdateSummary,
)
from pyiceberg.exceptions import (
    CommitFailedException,
    ConditionalCheckFailedException,
    GenericDynamoDbError,
    NamespaceAlreadyExistsError,
    NamespaceNotEmptyError,
    NoSuchIcebergTableError,
    NoSuchNamespaceError,
    NoSuchPropertyException,
    NoSuchTableError,
    TableAlreadyExistsError,
)
from pyiceberg.io import AWS_ACCESS_KEY_ID, AWS_REGION, AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN, load_file_io
from pyiceberg.partitioning import UNPARTITIONED_PARTITION_SPEC, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.serializers import FromInputFile
from pyiceberg.table import CommitTableResponse, Table, TableProperties
from pyiceberg.table.locations import load_location_provider
from pyiceberg.table.metadata import new_table_metadata
from pyiceberg.table.sorting import UNSORTED_SORT_ORDER, SortOrder
from pyiceberg.table.update import (
    TableRequirement,
    TableUpdate,
)
from pyiceberg.typedef import EMPTY_DICT, Identifier, Properties
from pyiceberg.utils.properties import get_first_property_value

if TYPE_CHECKING:
    import pyarrow as pa
    from mypy_boto3_dynamodb.client import DynamoDBClient


DYNAMODB_CLIENT = "dynamodb"

DYNAMODB_COL_IDENTIFIER = "identifier"
DYNAMODB_COL_NAMESPACE = "namespace"
DYNAMODB_COL_VERSION = "v"
DYNAMODB_COL_UPDATED_AT = "updated_at"
DYNAMODB_COL_CREATED_AT = "created_at"
DYNAMODB_NAMESPACE = "NAMESPACE"
DYNAMODB_NAMESPACE_GSI = "namespace-identifier"
DYNAMODB_PAY_PER_REQUEST = "PAY_PER_REQUEST"

DYNAMODB_TABLE_NAME = "table-name"
DYNAMODB_TABLE_NAME_DEFAULT = "iceberg"

PROPERTY_KEY_PREFIX = "p."

ACTIVE = "ACTIVE"
ITEM = "Item"

DYNAMODB_PROFILE_NAME = "dynamodb.profile-name"
DYNAMODB_REGION = "dynamodb.region"
DYNAMODB_ACCESS_KEY_ID = "dynamodb.access-key-id"
DYNAMODB_SECRET_ACCESS_KEY = "dynamodb.secret-access-key"
DYNAMODB_SESSION_TOKEN = "dynamodb.session-token"

# Enhancement configuration properties
DYNAMODB_CACHE_ENABLED = "dynamodb.cache.enabled"
DYNAMODB_CACHE_TTL_SECONDS = "dynamodb.cache.ttl-seconds"
DYNAMODB_MAX_RETRIES = "dynamodb.max-retries"
DYNAMODB_RETRY_MULTIPLIER = "dynamodb.retry-multiplier"
DYNAMODB_RETRY_MIN_WAIT_MS = "dynamodb.retry-min-wait-ms"
DYNAMODB_RETRY_MAX_WAIT_MS = "dynamodb.retry-max-wait-ms"

logger = logging.getLogger(__name__)


# ============================================================================
# Enhancement 1: Callback Hooks & Event System
# ============================================================================


class CatalogEvent(Enum):
    """Catalog operation events for hook callbacks."""

    PRE_CREATE_TABLE = "pre_create_table"
    POST_CREATE_TABLE = "post_create_table"
    PRE_UPDATE_TABLE = "pre_update_table"
    POST_UPDATE_TABLE = "post_update_table"
    PRE_DROP_TABLE = "pre_drop_table"
    POST_DROP_TABLE = "post_drop_table"
    PRE_COMMIT = "pre_commit"
    POST_COMMIT = "post_commit"
    PRE_REGISTER_TABLE = "pre_register_table"
    POST_REGISTER_TABLE = "post_register_table"
    ON_ERROR = "on_error"
    ON_CONCURRENT_CONFLICT = "on_concurrent_conflict"


@dataclass
class CatalogEventContext:
    """Context passed to event callbacks."""

    event: CatalogEvent
    catalog_name: str
    identifier: str | Identifier | None = None
    metadata_location: str | None = None
    error: Exception | None = None
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    extra: Dict[str, Any] = field(default_factory=dict)


# ============================================================================
# Enhancement 2: Metadata Caching Layer
# ============================================================================


class CatalogCache:
    """Thread-safe cache for catalog metadata with TTL expiration."""

    def __init__(self, ttl_seconds: int = 300) -> None:
        """
        Initialize the cache.

        Args:
            ttl_seconds: Time-to-live for cached entries in seconds.
        """
        self.ttl = timedelta(seconds=ttl_seconds)
        self._cache: Dict[str, Tuple[Any, datetime]] = {}
        self._lock = threading.Lock()

    def get(self, key: str) -> Optional[Any]:
        """
        Get a value from cache if not expired.

        Args:
            key: Cache key.

        Returns:
            Cached value if found and not expired, None otherwise.
        """
        with self._lock:
            if key in self._cache:
                value, expiry = self._cache[key]
                if datetime.now(timezone.utc) < expiry:
                    return value
                else:
                    del self._cache[key]
        return None

    def set(self, key: str, value: Any) -> None:
        """
        Store a value in cache with TTL.

        Args:
            key: Cache key.
            value: Value to cache.
        """
        with self._lock:
            self._cache[key] = (value, datetime.now(timezone.utc) + self.ttl)

    def invalidate(self, key: str) -> None:
        """
        Remove a specific key from cache.

        Args:
            key: Cache key to invalidate.
        """
        with self._lock:
            self._cache.pop(key, None)

    def clear(self) -> None:
        """Clear all cached entries."""
        with self._lock:
            self._cache.clear()

    def size(self) -> int:
        """Get the current size of the cache."""
        with self._lock:
            return len(self._cache)


# ============================================================================
# Enhancement 3: Retry Strategy with Exponential Backoff
# ============================================================================


def _get_retry_decorator(max_attempts: int, multiplier: float, min_wait: float, max_wait: float) -> Any:
    """
    Create a retry decorator with exponential backoff.

    Args:
        max_attempts: Maximum number of retry attempts.
        multiplier: Exponential backoff multiplier.
        min_wait: Minimum wait time in seconds.
        max_wait: Maximum wait time in seconds.

    Returns:
        Configured retry decorator.
    """
    return retry(
        retry=retry_if_exception_type(Exception),  # Will be filtered in the method
        stop=stop_after_attempt(max_attempts),
        wait=wait_exponential(multiplier=multiplier, min=min_wait, max=max_wait),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        reraise=True,
    )


class DynamoDbCatalog(MetastoreCatalog):
    def __init__(self, name: str, client: Optional["DynamoDBClient"] = None, **properties: str):
        """Dynamodb catalog.

        Args:
            name: Name to identify the catalog.
            client: An optional boto3 dynamodb client.
            properties: Properties for dynamodb client construction and configuration.
        """
        super().__init__(name, **properties)
        if client is not None:
            self.dynamodb = client
        else:
            session = boto3.Session(
                profile_name=properties.get(DYNAMODB_PROFILE_NAME),
                region_name=get_first_property_value(properties, DYNAMODB_REGION, AWS_REGION),
                botocore_session=properties.get(BOTOCORE_SESSION),
                aws_access_key_id=get_first_property_value(properties, DYNAMODB_ACCESS_KEY_ID, AWS_ACCESS_KEY_ID),
                aws_secret_access_key=get_first_property_value(properties, DYNAMODB_SECRET_ACCESS_KEY, AWS_SECRET_ACCESS_KEY),
                aws_session_token=get_first_property_value(properties, DYNAMODB_SESSION_TOKEN, AWS_SESSION_TOKEN),
            )
            self.dynamodb = session.client(DYNAMODB_CLIENT)

        self.dynamodb_table_name = self.properties.get(DYNAMODB_TABLE_NAME, DYNAMODB_TABLE_NAME_DEFAULT)

        # Enhancement 1: Initialize event hooks
        self._event_hooks: Dict[CatalogEvent, List[Callable[[CatalogEventContext], None]]] = defaultdict(list)

        # Enhancement 2: Initialize caching if enabled
        cache_enabled = properties.get(DYNAMODB_CACHE_ENABLED, "true").lower() == "true"
        cache_ttl = int(properties.get(DYNAMODB_CACHE_TTL_SECONDS, "300"))
        self._cache: Optional[CatalogCache] = CatalogCache(ttl_seconds=cache_ttl) if cache_enabled else None

        # Enhancement 3: Configure retry strategy
        self._max_retries = int(properties.get(DYNAMODB_MAX_RETRIES, "5"))
        self._retry_multiplier = float(properties.get(DYNAMODB_RETRY_MULTIPLIER, "1.5"))
        self._retry_min_wait = float(properties.get(DYNAMODB_RETRY_MIN_WAIT_MS, "100")) / 1000  # Convert to seconds
        self._retry_max_wait = float(properties.get(DYNAMODB_RETRY_MAX_WAIT_MS, "10000")) / 1000  # Convert to seconds

        self._ensure_catalog_table_exists_or_create()

    def _ensure_catalog_table_exists_or_create(self) -> None:
        if self._dynamodb_table_exists():
            return None

        try:
            self.dynamodb.create_table(
                TableName=self.dynamodb_table_name,
                AttributeDefinitions=CREATE_CATALOG_ATTRIBUTE_DEFINITIONS,
                KeySchema=CREATE_CATALOG_KEY_SCHEMA,
                GlobalSecondaryIndexes=CREATE_CATALOG_GLOBAL_SECONDARY_INDEXES,
                BillingMode=DYNAMODB_PAY_PER_REQUEST,
            )
        except (
            self.dynamodb.exceptions.ResourceInUseException,
            self.dynamodb.exceptions.LimitExceededException,
            self.dynamodb.exceptions.InternalServerError,
        ) as e:
            raise GenericDynamoDbError(e.message) from e

    def _dynamodb_table_exists(self) -> bool:
        try:
            response = self.dynamodb.describe_table(TableName=self.dynamodb_table_name)
        except self.dynamodb.exceptions.ResourceNotFoundException:
            return False
        except self.dynamodb.exceptions.InternalServerError as e:
            raise GenericDynamoDbError(e.message) from e

        if response["Table"]["TableStatus"] != ACTIVE:
            raise GenericDynamoDbError(f"DynamoDB table for catalog {self.dynamodb_table_name} is not {ACTIVE}")
        else:
            return True

    # ========================================================================
    # Enhancement Methods: Hooks, Caching, Retry
    # ========================================================================

    def register_hook(self, event: CatalogEvent, callback: Callable[[CatalogEventContext], None]) -> None:
        """
        Register a callback hook for a specific catalog event.

        Args:
            event: The catalog event to hook into.
            callback: Function to call when event occurs. Should accept CatalogEventContext.

        Example:
            def audit_hook(ctx: CatalogEventContext):
                logger.info(f"Event: {ctx.event}, Table: {ctx.identifier}")

            catalog.register_hook(CatalogEvent.POST_CREATE_TABLE, audit_hook)
        """
        self._event_hooks[event].append(callback)

    def _trigger_hooks(self, event: CatalogEvent, context: CatalogEventContext) -> None:
        """
        Trigger all registered hooks for an event.

        Args:
            event: The catalog event that occurred.
            context: Context information about the event.
        """
        for hook in self._event_hooks[event]:
            try:
                hook(context)
            except Exception as e:
                # Log but don't fail the operation due to hook errors
                logger.warning(f"Hook failed for {event.value}: {e}", exc_info=True)

    def _get_cache_key(self, identifier: Union[str, Identifier]) -> str:
        """Generate cache key for an identifier."""
        database_name, table_name = self.identifier_to_database_and_table(identifier)
        return f"table:{database_name}.{table_name}"

    def _invalidate_cache(self, identifier: Union[str, Identifier]) -> None:
        """Invalidate cache entry for an identifier."""
        if self._cache:
            cache_key = self._get_cache_key(identifier)
            self._cache.invalidate(cache_key)

    def _retry_dynamodb_operation(self, operation: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        """
        Execute a DynamoDB operation with retry logic.

        Args:
            operation: The operation to execute.
            *args: Positional arguments for the operation.
            **kwargs: Keyword arguments for the operation.

        Returns:
            Result of the operation.
        """
        retry_decorator = _get_retry_decorator(
            max_attempts=self._max_retries,
            multiplier=self._retry_multiplier,
            min_wait=self._retry_min_wait,
            max_wait=self._retry_max_wait,
        )

        @retry_decorator
        def _execute() -> Any:
            try:
                return operation(*args, **kwargs)
            except (
                self.dynamodb.exceptions.ProvisionedThroughputExceededException,
                self.dynamodb.exceptions.RequestLimitExceeded,
                self.dynamodb.exceptions.InternalServerError,
            ) as e:
                # Log and re-raise for retry
                logger.warning(f"DynamoDB transient error: {e}, will retry...")
                raise
            except Exception:
                # Don't retry other exceptions
                raise

        return _execute()

    # ========================================================================
    # Catalog Methods (Enhanced with Hooks, Caching, Retry)
    # ========================================================================

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
        schema: Schema = self._convert_schema_if_needed(  # type: ignore
            schema,
            int(properties.get(TableProperties.FORMAT_VERSION, TableProperties.DEFAULT_FORMAT_VERSION)),  # type: ignore
        )

        database_name, table_name = self.identifier_to_database_and_table(identifier)

        location = self._resolve_table_location(location, database_name, table_name)
        provider = load_location_provider(table_location=location, table_properties=properties)
        metadata_location = provider.new_table_metadata_file_location()

        metadata = new_table_metadata(
            location=location, schema=schema, partition_spec=partition_spec, sort_order=sort_order, properties=properties
        )
        io = load_file_io(properties=self.properties, location=metadata_location)
        self._write_metadata(metadata, io, metadata_location)

        self._ensure_namespace_exists(database_name=database_name)

        # Trigger pre-create hook
        self._trigger_hooks(
            CatalogEvent.PRE_CREATE_TABLE,
            CatalogEventContext(
                event=CatalogEvent.PRE_CREATE_TABLE,
                catalog_name=self.name,
                identifier=identifier,
                metadata_location=metadata_location,
                extra={"schema": schema, "location": location},
            ),
        )

        try:
            self._put_dynamo_item(
                item=_get_create_table_item(
                    database_name=database_name, table_name=table_name, properties=properties, metadata_location=metadata_location
                ),
                condition_expression=f"attribute_not_exists({DYNAMODB_COL_IDENTIFIER})",
            )
        except ConditionalCheckFailedException as e:
            # Trigger error hook
            self._trigger_hooks(
                CatalogEvent.ON_ERROR,
                CatalogEventContext(
                    event=CatalogEvent.ON_ERROR,
                    catalog_name=self.name,
                    identifier=identifier,
                    error=e,
                ),
            )
            raise TableAlreadyExistsError(f"Table {database_name}.{table_name} already exists") from e
        except Exception as e:
            # Trigger error hook for other exceptions
            self._trigger_hooks(
                CatalogEvent.ON_ERROR,
                CatalogEventContext(
                    event=CatalogEvent.ON_ERROR,
                    catalog_name=self.name,
                    identifier=identifier,
                    error=e,
                ),
            )
            raise

        table = self.load_table(identifier=identifier)

        # Trigger post-create hook
        self._trigger_hooks(
            CatalogEvent.POST_CREATE_TABLE,
            CatalogEventContext(
                event=CatalogEvent.POST_CREATE_TABLE,
                catalog_name=self.name,
                identifier=identifier,
                metadata_location=metadata_location,
                extra={"table": table},
            ),
        )

        return table

    def register_table(self, identifier: Union[str, Identifier], metadata_location: str) -> Table:
        """Register a new table using existing metadata.

        Args:
            identifier (Union[str, Identifier]): Table identifier for the table
            metadata_location (str): The location to the metadata

        Returns:
            Table: The newly registered table

        Raises:
            TableAlreadyExistsError: If the table already exists
        """
        database_name, table_name = self.identifier_to_database_and_table(identifier)

        # Trigger pre-register hook
        self._trigger_hooks(
            CatalogEvent.PRE_REGISTER_TABLE,
            CatalogEventContext(
                event=CatalogEvent.PRE_REGISTER_TABLE,
                catalog_name=self.name,
                identifier=identifier,
                metadata_location=metadata_location,
            ),
        )

        io = load_file_io(properties=self.properties, location=metadata_location)
        file = io.new_input(metadata_location)
        metadata = FromInputFile.table_metadata(file)

        self._ensure_namespace_exists(database_name=database_name)

        try:
            self._put_dynamo_item(
                item=_get_create_table_item(
                    database_name=database_name,
                    table_name=table_name,
                    properties=metadata.properties,
                    metadata_location=metadata_location,
                ),
                condition_expression=f"attribute_not_exists({DYNAMODB_COL_IDENTIFIER})",
            )
        except ConditionalCheckFailedException as e:
            # Trigger error hook
            self._trigger_hooks(
                CatalogEvent.ON_ERROR,
                CatalogEventContext(
                    event=CatalogEvent.ON_ERROR,
                    catalog_name=self.name,
                    identifier=identifier,
                    error=e,
                ),
            )
            raise TableAlreadyExistsError(f"Table {database_name}.{table_name} already exists") from e
        except Exception as e:
            # Trigger error hook
            self._trigger_hooks(
                CatalogEvent.ON_ERROR,
                CatalogEventContext(
                    event=CatalogEvent.ON_ERROR,
                    catalog_name=self.name,
                    identifier=identifier,
                    error=e,
                ),
            )
            raise

        table = self.load_table(identifier=identifier)

        # Trigger post-register hook
        self._trigger_hooks(
            CatalogEvent.POST_REGISTER_TABLE,
            CatalogEventContext(
                event=CatalogEvent.POST_REGISTER_TABLE,
                catalog_name=self.name,
                identifier=identifier,
                metadata_location=metadata_location,
                extra={"table": table},
            ),
        )

        return table

    def commit_table(
        self, table: Table, requirements: Tuple[TableRequirement, ...], updates: Tuple[TableUpdate, ...]
    ) -> CommitTableResponse:
        """Commit updates to a table.

        Args:
            table (Table): The table to be updated.
            requirements: (Tuple[TableRequirement, ...]): Table requirements.
            updates: (Tuple[TableUpdate, ...]): Table updates.

        Returns:
            CommitTableResponse: The updated metadata.

        Raises:
            NoSuchTableError: If a table with the given identifier does not exist.
            CommitFailedException: Requirement not met, or a conflict with a concurrent commit.
        """
        table_identifier = table.name()
        database_name, table_name = self.identifier_to_database_and_table(table_identifier, NoSuchTableError)

        # Trigger pre-commit hook
        self._trigger_hooks(
            CatalogEvent.PRE_COMMIT,
            CatalogEventContext(
                event=CatalogEvent.PRE_COMMIT,
                catalog_name=self.name,
                identifier=table_identifier,
                metadata_location=table.metadata_location,
                extra={"requirements": requirements, "updates": updates},
            ),
        )

        current_table: Optional[Table]
        current_dynamo_table_item: Optional[Dict[str, Any]]
        current_version_id: Optional[str]

        try:
            current_dynamo_table_item = self._get_iceberg_table_item(database_name=database_name, table_name=table_name)
            current_table = self._convert_dynamo_table_item_to_iceberg_table(dynamo_table_item=current_dynamo_table_item)
            # Extract the current version for optimistic locking
            current_version_id = _convert_dynamo_item_to_regular_dict(current_dynamo_table_item).get(DYNAMODB_COL_VERSION)
        except NoSuchTableError:
            current_dynamo_table_item = None
            current_table = None
            current_version_id = None

        updated_staged_table = self._update_and_stage_table(current_table, table_identifier, requirements, updates)

        if current_table and updated_staged_table.metadata == current_table.metadata:
            # No changes, do nothing
            return CommitTableResponse(metadata=current_table.metadata, metadata_location=current_table.metadata_location)

        self._write_metadata(
            metadata=updated_staged_table.metadata,
            io=updated_staged_table.io,
            metadata_path=updated_staged_table.metadata_location,
        )

        if current_table:
            # Table exists, update it with optimistic locking
            if not current_version_id:
                raise ValueError(f"Cannot commit {database_name}.{table_name} because version ID is missing from DynamoDB item")

            # Ensure we have the DynamoDB item (should always be present if current_table exists)
            if current_dynamo_table_item is None:
                raise ValueError(f"Cannot commit {database_name}.{table_name} because DynamoDB item is missing")

            # Create updated item with new version and metadata location
            updated_item = _get_update_table_item(
                current_dynamo_table_item=current_dynamo_table_item,
                metadata_location=updated_staged_table.metadata_location,
                prev_metadata_location=current_table.metadata_location,
                properties=updated_staged_table.properties,
            )

            # Use conditional expression for optimistic locking based on version
            try:
                self._put_dynamo_item(
                    item=updated_item,
                    condition_expression=f"{DYNAMODB_COL_VERSION} = :current_version",
                    expression_attribute_values={":current_version": {"S": current_version_id}},
                )
            except ConditionalCheckFailedException as e:
                # Trigger concurrent conflict hook
                self._trigger_hooks(
                    CatalogEvent.ON_CONCURRENT_CONFLICT,
                    CatalogEventContext(
                        event=CatalogEvent.ON_CONCURRENT_CONFLICT,
                        catalog_name=self.name,
                        identifier=table_identifier,
                        error=e,
                        extra={"current_version": current_version_id},
                    ),
                )
                raise CommitFailedException(
                    f"Cannot commit {database_name}.{table_name} because DynamoDB detected concurrent update (version mismatch)"
                ) from e
            except Exception as e:
                # Trigger error hook
                self._trigger_hooks(
                    CatalogEvent.ON_ERROR,
                    CatalogEventContext(
                        event=CatalogEvent.ON_ERROR,
                        catalog_name=self.name,
                        identifier=table_identifier,
                        error=e,
                    ),
                )
                raise
        else:
            # Table does not exist, create it
            create_table_item = _get_create_table_item(
                database_name=database_name,
                table_name=table_name,
                properties=updated_staged_table.properties,
                metadata_location=updated_staged_table.metadata_location,
            )
            try:
                self._put_dynamo_item(
                    item=create_table_item,
                    condition_expression=f"attribute_not_exists({DYNAMODB_COL_IDENTIFIER})",
                )
            except ConditionalCheckFailedException as e:
                # Trigger error hook
                self._trigger_hooks(
                    CatalogEvent.ON_ERROR,
                    CatalogEventContext(
                        event=CatalogEvent.ON_ERROR,
                        catalog_name=self.name,
                        identifier=table_identifier,
                        error=e,
                    ),
                )
                raise TableAlreadyExistsError(f"Table {database_name}.{table_name} already exists") from e
            except Exception as e:
                # Trigger error hook
                self._trigger_hooks(
                    CatalogEvent.ON_ERROR,
                    CatalogEventContext(
                        event=CatalogEvent.ON_ERROR,
                        catalog_name=self.name,
                        identifier=table_identifier,
                        error=e,
                    ),
                )
                raise

        # Invalidate cache after successful commit
        self._invalidate_cache(table_identifier)

        # Trigger post-commit hook
        self._trigger_hooks(
            CatalogEvent.POST_COMMIT,
            CatalogEventContext(
                event=CatalogEvent.POST_COMMIT,
                catalog_name=self.name,
                identifier=table_identifier,
                metadata_location=updated_staged_table.metadata_location,
                extra={"metadata": updated_staged_table.metadata},
            ),
        )

        return CommitTableResponse(
            metadata=updated_staged_table.metadata, metadata_location=updated_staged_table.metadata_location
        )

    def load_table(self, identifier: Union[str, Identifier]) -> Table:
        """
        Load the table's metadata and returns the table instance.

        You can also use this method to check for table existence using 'try catalog.table() except TableNotFoundError'.
        Note: This method doesn't scan data stored in the table.

        Args:
            identifier: Table identifier.

        Returns:
            Table: the table instance with its metadata.

        Raises:
            NoSuchTableError: If a table with the name does not exist, or the identifier is invalid.
        """
        # Check cache first
        if self._cache:
            cache_key = self._get_cache_key(identifier)
            cached_table = self._cache.get(cache_key)
            if cached_table:
                logger.debug(f"Cache hit for table {identifier}")
                return cached_table

        # Load from DynamoDB with retry
        database_name, table_name = self.identifier_to_database_and_table(identifier, NoSuchTableError)

        def _load_from_dynamodb() -> Table:
            dynamo_table_item = self._get_iceberg_table_item(database_name=database_name, table_name=table_name)
            return self._convert_dynamo_table_item_to_iceberg_table(dynamo_table_item=dynamo_table_item)

        table = self._retry_dynamodb_operation(_load_from_dynamodb)

        # Cache the loaded table
        if self._cache:
            cache_key = self._get_cache_key(identifier)
            self._cache.set(cache_key, table)
            logger.debug(f"Cached table {identifier}")

        return table

    def drop_table(self, identifier: Union[str, Identifier]) -> None:
        """Drop a table.

        Args:
            identifier: Table identifier.

        Raises:
            NoSuchTableError: If a table with the name does not exist, or the identifier is invalid.
        """
        database_name, table_name = self.identifier_to_database_and_table(identifier, NoSuchTableError)

        # Trigger pre-drop hook
        self._trigger_hooks(
            CatalogEvent.PRE_DROP_TABLE,
            CatalogEventContext(
                event=CatalogEvent.PRE_DROP_TABLE,
                catalog_name=self.name,
                identifier=identifier,
            ),
        )

        try:
            self._delete_dynamo_item(
                namespace=database_name,
                identifier=f"{database_name}.{table_name}",
                condition_expression=f"attribute_exists({DYNAMODB_COL_IDENTIFIER})",
            )
        except ConditionalCheckFailedException as e:
            # Trigger error hook
            self._trigger_hooks(
                CatalogEvent.ON_ERROR,
                CatalogEventContext(
                    event=CatalogEvent.ON_ERROR,
                    catalog_name=self.name,
                    identifier=identifier,
                    error=e,
                ),
            )
            raise NoSuchTableError(f"Table does not exist: {database_name}.{table_name}") from e

        # Invalidate cache
        self._invalidate_cache(identifier)

        # Trigger post-drop hook
        self._trigger_hooks(
            CatalogEvent.POST_DROP_TABLE,
            CatalogEventContext(
                event=CatalogEvent.POST_DROP_TABLE,
                catalog_name=self.name,
                identifier=identifier,
            ),
        )

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
        from_database_name, from_table_name = self.identifier_to_database_and_table(from_identifier, NoSuchTableError)
        to_database_name, to_table_name = self.identifier_to_database_and_table(to_identifier)

        from_table_item = self._get_iceberg_table_item(database_name=from_database_name, table_name=from_table_name)

        try:
            # Verify that from_identifier is a valid iceberg table
            self._convert_dynamo_table_item_to_iceberg_table(dynamo_table_item=from_table_item)
        except NoSuchPropertyException as e:
            raise NoSuchPropertyException(
                f"Failed to rename table {from_database_name}.{from_table_name} since it is missing required properties"
            ) from e
        except NoSuchIcebergTableError as e:
            raise NoSuchIcebergTableError(
                f"Failed to rename table {from_database_name}.{from_table_name} since it is not a valid iceberg table"
            ) from e

        self._ensure_namespace_exists(database_name=from_database_name)
        self._ensure_namespace_exists(database_name=to_database_name)

        try:
            self._put_dynamo_item(
                item=_get_rename_table_item(
                    from_dynamo_table_item=from_table_item, to_database_name=to_database_name, to_table_name=to_table_name
                ),
                condition_expression=f"attribute_not_exists({DYNAMODB_COL_IDENTIFIER})",
            )
        except ConditionalCheckFailedException as e:
            raise TableAlreadyExistsError(f"Table {to_database_name}.{to_table_name} already exists") from e

        try:
            self.drop_table(from_identifier)
        except (NoSuchTableError, GenericDynamoDbError) as e:
            log_message = f"Failed to drop old table {from_database_name}.{from_table_name}. "

            try:
                self.drop_table(to_identifier)
                log_message += f"Rolled back table creation for {to_database_name}.{to_table_name}."
            except (NoSuchTableError, GenericDynamoDbError):
                log_message += (
                    f"Failed to roll back table creation for {to_database_name}.{to_table_name}. Please clean up manually"
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
            self._put_dynamo_item(
                item=_get_create_database_item(database_name=database_name, properties=properties),
                condition_expression=f"attribute_not_exists({DYNAMODB_COL_NAMESPACE})",
            )
        except ConditionalCheckFailedException as e:
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
        table_identifiers = self.list_tables(namespace=database_name)

        if len(table_identifiers) > 0:
            raise NamespaceNotEmptyError(f"Database {database_name} is not empty")

        try:
            self._delete_dynamo_item(
                namespace=database_name,
                identifier=DYNAMODB_NAMESPACE,
                condition_expression=f"attribute_exists({DYNAMODB_COL_IDENTIFIER})",
            )
        except ConditionalCheckFailedException as e:
            raise NoSuchNamespaceError(f"Database does not exist: {database_name}") from e

    def list_tables(self, namespace: Union[str, Identifier]) -> List[Identifier]:
        """List Iceberg tables under the given namespace in the catalog.

        Args:
            namespace (str | Identifier): Namespace identifier to search.

        Returns:
            List[Identifier]: list of table identifiers.
        """
        database_name = self.identifier_to_database(namespace, NoSuchNamespaceError)

        paginator = self.dynamodb.get_paginator("query")

        try:
            page_iterator = paginator.paginate(
                TableName=self.dynamodb_table_name,
                IndexName=DYNAMODB_NAMESPACE_GSI,
                KeyConditionExpression=f"{DYNAMODB_COL_NAMESPACE} = :namespace ",
                ExpressionAttributeValues={
                    ":namespace": {
                        "S": database_name,
                    }
                },
            )
        except (
            self.dynamodb.exceptions.ProvisionedThroughputExceededException,
            self.dynamodb.exceptions.RequestLimitExceeded,
            self.dynamodb.exceptions.InternalServerError,
            self.dynamodb.exceptions.ResourceNotFoundException,
        ) as e:
            raise GenericDynamoDbError(e.message) from e

        table_identifiers = []
        for page in page_iterator:
            for item in page["Items"]:
                _dict = _convert_dynamo_item_to_regular_dict(item)
                identifier_col = _dict[DYNAMODB_COL_IDENTIFIER]
                if identifier_col == DYNAMODB_NAMESPACE:
                    continue

                table_identifiers.append(self.identifier_to_tuple(identifier_col))

        return table_identifiers

    def list_namespaces(self, namespace: Union[str, Identifier] = ()) -> List[Identifier]:
        """List top-level namespaces from the catalog.

        We do not support hierarchical namespace.

        Returns:
            List[Identifier]: a List of namespace identifiers.
        """
        # Hierarchical namespace is not supported. Return an empty list
        if namespace:
            return []

        paginator = self.dynamodb.get_paginator("query")

        try:
            page_iterator = paginator.paginate(
                TableName=self.dynamodb_table_name,
                ConsistentRead=True,
                KeyConditionExpression=f"{DYNAMODB_COL_IDENTIFIER} = :identifier",
                ExpressionAttributeValues={
                    ":identifier": {
                        "S": DYNAMODB_NAMESPACE,
                    }
                },
            )
        except (
            self.dynamodb.exceptions.ProvisionedThroughputExceededException,
            self.dynamodb.exceptions.RequestLimitExceeded,
            self.dynamodb.exceptions.InternalServerError,
            self.dynamodb.exceptions.ResourceNotFoundException,
        ) as e:
            raise GenericDynamoDbError(e.message) from e

        database_identifiers = []
        for page in page_iterator:
            for item in page["Items"]:
                _dict = _convert_dynamo_item_to_regular_dict(item)
                namespace_col = _dict[DYNAMODB_COL_NAMESPACE]
                database_identifiers.append(self.identifier_to_tuple(namespace_col))

        return database_identifiers

    def load_namespace_properties(self, namespace: Union[str, Identifier]) -> Properties:
        """
        Get properties for a namespace.

        Args:
            namespace: Namespace identifier.

        Returns:
            Properties: Properties for the given namespace.

        Raises:
            NoSuchNamespaceError: If a namespace with the given name does not exist, or identifier is invalid.
        """
        database_name = self.identifier_to_database(namespace, NoSuchNamespaceError)
        namespace_item = self._get_iceberg_namespace_item(database_name=database_name)
        namespace_dict = _convert_dynamo_item_to_regular_dict(namespace_item)
        return _get_namespace_properties(namespace_dict=namespace_dict)

    def update_namespace_properties(
        self, namespace: Union[str, Identifier], removals: Optional[Set[str]] = None, updates: Properties = EMPTY_DICT
    ) -> PropertiesUpdateSummary:
        """
        Remove or update provided property keys for a namespace.

        Args:
            namespace: Namespace identifier
            removals: Set of property keys that need to be removed. Optional Argument.
            updates: Properties to be updated for the given namespace.

        Raises:
            NoSuchNamespaceError: If a namespace with the given name does not exist， or identifier is invalid.
            ValueError: If removals and updates have overlapping keys.
        """
        database_name = self.identifier_to_database(namespace, NoSuchNamespaceError)
        namespace_item = self._get_iceberg_namespace_item(database_name=database_name)
        namespace_dict = _convert_dynamo_item_to_regular_dict(namespace_item)
        current_properties = _get_namespace_properties(namespace_dict=namespace_dict)

        properties_update_summary, updated_properties = self._get_updated_props_and_update_summary(
            current_properties=current_properties, removals=removals, updates=updates
        )

        try:
            self._put_dynamo_item(
                item=_get_update_database_item(
                    namespace_item=namespace_item,
                    updated_properties=updated_properties,
                ),
                condition_expression=f"attribute_exists({DYNAMODB_COL_NAMESPACE})",
            )
        except ConditionalCheckFailedException as e:
            raise NoSuchNamespaceError(f"Database {database_name} does not exist") from e

        return properties_update_summary

    def list_views(self, namespace: Union[str, Identifier]) -> List[Identifier]:
        raise NotImplementedError

    def drop_view(self, identifier: Union[str, Identifier]) -> None:
        raise NotImplementedError

    def view_exists(self, identifier: Union[str, Identifier]) -> bool:
        raise NotImplementedError

    def _get_iceberg_table_item(self, database_name: str, table_name: str) -> Dict[str, Any]:
        try:
            return self._get_dynamo_item(identifier=f"{database_name}.{table_name}", namespace=database_name)
        except ValueError as e:
            raise NoSuchTableError(f"Table does not exist: {database_name}.{table_name}") from e

    def _get_iceberg_namespace_item(self, database_name: str) -> Dict[str, Any]:
        try:
            return self._get_dynamo_item(identifier=DYNAMODB_NAMESPACE, namespace=database_name)
        except ValueError as e:
            raise NoSuchNamespaceError(f"Namespace does not exist: {database_name}") from e

    def _ensure_namespace_exists(self, database_name: str) -> Dict[str, Any]:
        return self._get_iceberg_namespace_item(database_name)

    def _get_dynamo_item(self, identifier: str, namespace: str) -> Dict[str, Any]:
        try:
            response = self.dynamodb.get_item(
                TableName=self.dynamodb_table_name,
                ConsistentRead=True,
                Key={
                    DYNAMODB_COL_IDENTIFIER: {
                        "S": identifier,
                    },
                    DYNAMODB_COL_NAMESPACE: {
                        "S": namespace,
                    },
                },
            )
            if ITEM in response:
                return response[ITEM]
            else:
                raise ValueError(f"Item not found. identifier: {identifier} - namespace: {namespace}")
        except self.dynamodb.exceptions.ResourceNotFoundException as e:
            raise ValueError(f"Item not found. identifier: {identifier} - namespace: {namespace}") from e
        except (
            self.dynamodb.exceptions.ProvisionedThroughputExceededException,
            self.dynamodb.exceptions.RequestLimitExceeded,
            self.dynamodb.exceptions.InternalServerError,
        ) as e:
            raise GenericDynamoDbError(e.message) from e

    def _put_dynamo_item(
        self, item: Dict[str, Any], condition_expression: str, expression_attribute_values: Optional[Dict[str, Any]] = None
    ) -> None:
        try:
            put_item_params = {
                "TableName": self.dynamodb_table_name,
                "Item": item,
                "ConditionExpression": condition_expression,
            }
            if expression_attribute_values:
                put_item_params["ExpressionAttributeValues"] = expression_attribute_values

            self.dynamodb.put_item(**put_item_params)
        except self.dynamodb.exceptions.ConditionalCheckFailedException as e:
            raise ConditionalCheckFailedException(f"Condition expression check failed: {condition_expression} - {item}") from e
        except (
            self.dynamodb.exceptions.ProvisionedThroughputExceededException,
            self.dynamodb.exceptions.RequestLimitExceeded,
            self.dynamodb.exceptions.InternalServerError,
            self.dynamodb.exceptions.ResourceNotFoundException,
            self.dynamodb.exceptions.ItemCollectionSizeLimitExceededException,
            self.dynamodb.exceptions.TransactionConflictException,
        ) as e:
            raise GenericDynamoDbError(e.message) from e

    def _delete_dynamo_item(self, namespace: str, identifier: str, condition_expression: str) -> None:
        try:
            self.dynamodb.delete_item(
                TableName=self.dynamodb_table_name,
                Key={
                    DYNAMODB_COL_IDENTIFIER: {
                        "S": identifier,
                    },
                    DYNAMODB_COL_NAMESPACE: {
                        "S": namespace,
                    },
                },
                ConditionExpression=condition_expression,
            )
        except self.dynamodb.exceptions.ConditionalCheckFailedException as e:
            raise ConditionalCheckFailedException(
                f"Condition expression check failed: {condition_expression} - {identifier}"
            ) from e
        except (
            self.dynamodb.exceptions.ProvisionedThroughputExceededException,
            self.dynamodb.exceptions.RequestLimitExceeded,
            self.dynamodb.exceptions.InternalServerError,
            self.dynamodb.exceptions.ResourceNotFoundException,
            self.dynamodb.exceptions.ItemCollectionSizeLimitExceededException,
            self.dynamodb.exceptions.TransactionConflictException,
        ) as e:
            raise GenericDynamoDbError(e.message) from e

    def _convert_dynamo_table_item_to_iceberg_table(self, dynamo_table_item: Dict[str, Any]) -> Table:
        table_dict = _convert_dynamo_item_to_regular_dict(dynamo_table_item)

        for prop in [_add_property_prefix(prop) for prop in (TABLE_TYPE, METADATA_LOCATION)] + [
            DYNAMODB_COL_IDENTIFIER,
            DYNAMODB_COL_NAMESPACE,
            DYNAMODB_COL_CREATED_AT,
        ]:
            if prop not in table_dict.keys():
                raise NoSuchPropertyException(f"Iceberg required property {prop} is missing: {dynamo_table_item}")

        table_type = table_dict[_add_property_prefix(TABLE_TYPE)]
        identifier = table_dict[DYNAMODB_COL_IDENTIFIER]
        metadata_location = table_dict[_add_property_prefix(METADATA_LOCATION)]
        database_name, table_name = self.identifier_to_database_and_table(identifier, NoSuchTableError)

        if table_type.lower() != ICEBERG:
            raise NoSuchIcebergTableError(
                f"Property table_type is {table_type}, expected {ICEBERG}: {database_name}.{table_name}"
            )

        io = load_file_io(properties=self.properties, location=metadata_location)
        file = io.new_input(metadata_location)
        metadata = FromInputFile.table_metadata(file)
        return Table(
            identifier=(database_name, table_name),
            metadata=metadata,
            metadata_location=metadata_location,
            io=self._load_file_io(metadata.properties, metadata_location),
            catalog=self,
        )

    def _get_default_warehouse_location(self, database_name: str, table_name: str) -> str:
        """Override the default warehouse location to follow Hive-style conventions."""
        return self._get_hive_style_warehouse_location(database_name, table_name)


def _get_create_table_item(database_name: str, table_name: str, properties: Properties, metadata_location: str) -> Dict[str, Any]:
    current_timestamp_ms = str(round(time() * 1000))
    _dict = {
        DYNAMODB_COL_IDENTIFIER: {
            "S": f"{database_name}.{table_name}",
        },
        DYNAMODB_COL_NAMESPACE: {
            "S": database_name,
        },
        DYNAMODB_COL_VERSION: {
            "S": str(uuid.uuid4()),
        },
        DYNAMODB_COL_CREATED_AT: {
            "N": current_timestamp_ms,
        },
        DYNAMODB_COL_UPDATED_AT: {
            "N": current_timestamp_ms,
        },
    }

    for key, val in properties.items():
        _dict[_add_property_prefix(key)] = {"S": val}

    _dict[_add_property_prefix(TABLE_TYPE)] = {"S": ICEBERG.upper()}
    _dict[_add_property_prefix(METADATA_LOCATION)] = {"S": metadata_location}
    _dict[_add_property_prefix(PREVIOUS_METADATA_LOCATION)] = {"S": ""}

    return _dict


def _get_rename_table_item(from_dynamo_table_item: Dict[str, Any], to_database_name: str, to_table_name: str) -> Dict[str, Any]:
    _dict = from_dynamo_table_item
    current_timestamp_ms = str(round(time() * 1000))
    _dict[DYNAMODB_COL_IDENTIFIER]["S"] = f"{to_database_name}.{to_table_name}"
    _dict[DYNAMODB_COL_NAMESPACE]["S"] = to_database_name
    _dict[DYNAMODB_COL_VERSION]["S"] = str(uuid.uuid4())
    _dict[DYNAMODB_COL_UPDATED_AT]["N"] = current_timestamp_ms
    return _dict


def _get_update_table_item(
    current_dynamo_table_item: Dict[str, Any],
    metadata_location: str,
    prev_metadata_location: str,
    properties: Properties,
) -> Dict[str, Any]:
    """Create an updated table item for DynamoDB with new metadata location and version."""
    current_timestamp_ms = str(round(time() * 1000))

    # Start with the current item
    _dict = dict(current_dynamo_table_item)

    # Update version for optimistic locking
    _dict[DYNAMODB_COL_VERSION] = {"S": str(uuid.uuid4())}
    _dict[DYNAMODB_COL_UPDATED_AT] = {"N": current_timestamp_ms}

    # Update metadata locations
    _dict[_add_property_prefix(METADATA_LOCATION)] = {"S": metadata_location}
    _dict[_add_property_prefix(PREVIOUS_METADATA_LOCATION)] = {"S": prev_metadata_location}

    # Update properties
    for key, val in properties.items():
        _dict[_add_property_prefix(key)] = {"S": val}

    return _dict


def _get_create_database_item(database_name: str, properties: Properties) -> Dict[str, Any]:
    current_timestamp_ms = str(round(time() * 1000))
    _dict = {
        DYNAMODB_COL_IDENTIFIER: {
            "S": DYNAMODB_NAMESPACE,
        },
        DYNAMODB_COL_NAMESPACE: {
            "S": database_name,
        },
        DYNAMODB_COL_VERSION: {
            "S": str(uuid.uuid4()),
        },
        DYNAMODB_COL_CREATED_AT: {
            "N": current_timestamp_ms,
        },
        DYNAMODB_COL_UPDATED_AT: {
            "N": current_timestamp_ms,
        },
    }

    for key, val in properties.items():
        _dict[_add_property_prefix(key)] = {"S": val}

    return _dict


def _get_update_database_item(namespace_item: Dict[str, Any], updated_properties: Properties) -> Dict[str, Any]:
    current_timestamp_ms = str(round(time() * 1000))

    _dict = {
        DYNAMODB_COL_IDENTIFIER: namespace_item[DYNAMODB_COL_IDENTIFIER],
        DYNAMODB_COL_NAMESPACE: namespace_item[DYNAMODB_COL_NAMESPACE],
        DYNAMODB_COL_VERSION: {
            "S": str(uuid.uuid4()),
        },
        DYNAMODB_COL_CREATED_AT: namespace_item[DYNAMODB_COL_CREATED_AT],
        DYNAMODB_COL_UPDATED_AT: {
            "N": current_timestamp_ms,
        },
    }

    for key, val in updated_properties.items():
        _dict[_add_property_prefix(key)] = {"S": val}

    return _dict


CREATE_CATALOG_ATTRIBUTE_DEFINITIONS = [
    {
        "AttributeName": DYNAMODB_COL_IDENTIFIER,
        "AttributeType": "S",
    },
    {
        "AttributeName": DYNAMODB_COL_NAMESPACE,
        "AttributeType": "S",
    },
]

CREATE_CATALOG_KEY_SCHEMA = [
    {
        "AttributeName": DYNAMODB_COL_IDENTIFIER,
        "KeyType": "HASH",
    },
    {
        "AttributeName": DYNAMODB_COL_NAMESPACE,
        "KeyType": "RANGE",
    },
]


CREATE_CATALOG_GLOBAL_SECONDARY_INDEXES = [
    {
        "IndexName": DYNAMODB_NAMESPACE_GSI,
        "KeySchema": [
            {
                "AttributeName": DYNAMODB_COL_NAMESPACE,
                "KeyType": "HASH",
            },
            {
                "AttributeName": DYNAMODB_COL_IDENTIFIER,
                "KeyType": "RANGE",
            },
        ],
        "Projection": {
            "ProjectionType": "KEYS_ONLY",
        },
    }
]


def _get_namespace_properties(namespace_dict: Dict[str, str]) -> Properties:
    return {_remove_property_prefix(key): val for key, val in namespace_dict.items() if key.startswith(PROPERTY_KEY_PREFIX)}


def _convert_dynamo_item_to_regular_dict(dynamo_json: Dict[str, Any]) -> Dict[str, str]:
    """Convert a dynamo json to a regular json.

    Example of a dynamo json:
    {
        "AlbumTitle": {
            "S": "Songs About Life",
        },
        "Artist": {
            "S": "Acme Band",
        },
        "SongTitle": {
            "S": "Happy Day",
        }
    }

    Converted to regular json:
    {
        "AlbumTitle": "Songs About Life",
        "Artist": "Acme Band",
        "SongTitle": "Happy Day"
    }

    Only "S" and "N" data types are supported since those are the only ones that Iceberg is utilizing.
    """
    regular_json = {}
    for column_name, val_dict in dynamo_json.items():
        keys = list(val_dict.keys())

        if len(keys) != 1:
            raise ValueError(f"Expecting only 1 key: {keys}")

        data_type = keys[0]
        if data_type not in ("S", "N"):
            raise ValueError("Only S and N data types are supported.")

        values = list(val_dict.values())
        if len(values) != 1:
            raise ValueError(f"Expecting only 1 value: {values}")

        column_value = values[0]
        regular_json[column_name] = column_value

    return regular_json


def _add_property_prefix(prop: str) -> str:
    return PROPERTY_KEY_PREFIX + prop


def _remove_property_prefix(prop: str) -> str:
    return prop.lstrip(PROPERTY_KEY_PREFIX)
