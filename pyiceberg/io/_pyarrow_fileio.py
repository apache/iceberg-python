# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""PyArrow-based FileIO implementation.

This module contains the FileIO implementation that relies on PyArrow's filesystem
interface. It supports S3, GCS, Azure, HDFS, OSS, and local filesystems.
"""

from __future__ import annotations

import importlib
import logging
import os
import warnings
from collections.abc import Callable
from copy import copy
from functools import lru_cache
from typing import Any
from urllib.parse import urlparse

import pyarrow
import pyarrow.fs
from pyarrow._s3fs import S3RetryStrategy
from pyarrow.fs import (
    FileInfo,
    FileSystem,
    FileType,
)
from typing_extensions import override

from pyiceberg.io import (
    ADLS_ACCOUNT_KEY,
    ADLS_ACCOUNT_NAME,
    ADLS_BLOB_STORAGE_AUTHORITY,
    ADLS_BLOB_STORAGE_SCHEME,
    ADLS_CLIENT_ID,
    ADLS_CLIENT_SECRET,
    ADLS_DFS_STORAGE_AUTHORITY,
    ADLS_DFS_STORAGE_SCHEME,
    ADLS_SAS_TOKEN,
    ADLS_TENANT_ID,
    AWS_ACCESS_KEY_ID,
    AWS_REGION,
    AWS_ROLE_ARN,
    AWS_ROLE_SESSION_NAME,
    AWS_SECRET_ACCESS_KEY,
    AWS_SESSION_TOKEN,
    GCS_DEFAULT_LOCATION,
    GCS_SERVICE_HOST,
    GCS_TOKEN,
    GCS_TOKEN_EXPIRES_AT_MS,
    HDFS_HOST,
    HDFS_KERB_TICKET,
    HDFS_PORT,
    HDFS_USER,
    S3_ACCESS_KEY_ID,
    S3_ANONYMOUS,
    S3_CONNECT_TIMEOUT,
    S3_ENDPOINT,
    S3_FORCE_VIRTUAL_ADDRESSING,
    S3_PROXY_URI,
    S3_REGION,
    S3_REQUEST_TIMEOUT,
    S3_RESOLVE_REGION,
    S3_RETRY_STRATEGY_IMPL,
    S3_ROLE_ARN,
    S3_ROLE_SESSION_NAME,
    S3_SECRET_ACCESS_KEY,
    S3_SESSION_TOKEN,
    FileIO,
    InputFile,
    InputStream,
    OutputFile,
    OutputStream,
)
from pyiceberg.typedef import EMPTY_DICT, Properties
from pyiceberg.types import strtobool
from pyiceberg.utils.datetime import millis_to_datetime
from pyiceberg.utils.properties import get_first_property_value, property_as_bool

logger = logging.getLogger(__name__)

ONE_MEGABYTE = 1024 * 1024
BUFFER_SIZE = "buffer-size"


@lru_cache
def _cached_resolve_s3_region(bucket: str) -> str | None:
    from pyarrow.fs import resolve_s3_region

    try:
        return resolve_s3_region(bucket=bucket)
    except (OSError, TypeError):
        logger.warning(f"Unable to resolve region for bucket {bucket}")
        return None


def _import_retry_strategy(impl: str) -> S3RetryStrategy | None:
    try:
        path_parts = impl.split(".")
        if len(path_parts) < 2:
            raise ValueError(f"retry-strategy-impl should be full path (module.CustomS3RetryStrategy), got: {impl}")
        module_name, class_name = ".".join(path_parts[:-1]), path_parts[-1]
        module = importlib.import_module(module_name)
        class_ = getattr(module, class_name)
        return class_()
    except (ModuleNotFoundError, AttributeError):
        warnings.warn(f"Could not initialize S3 retry strategy: {impl}", stacklevel=2)
        return None


class PyArrowLocalFileSystem(pyarrow.fs.LocalFileSystem):
    """Local filesystem that creates parent directories before writing."""

    def open_output_stream(self, path: str, *args: Any, **kwargs: Any) -> pyarrow.NativeFile:
        # In LocalFileSystem, parent directories must be first created before opening an output stream
        self.create_dir(os.path.dirname(path), recursive=True)
        return super().open_output_stream(path, *args, **kwargs)


class PyArrowFile(InputFile, OutputFile):
    """A combined InputFile and OutputFile implementation using pyarrow filesystem.

    This class generates pyarrow.lib.NativeFile instances.

    Args:
        location (str): A URI or a path to a local file.

    Attributes:
        location(str): The URI or path to a local file for a PyArrowFile instance.

    Examples:
        >>> from pyiceberg.io.pyarrow import PyArrowFile
        >>> # input_file = PyArrowFile("s3://foo/bar.txt")
        >>> # Read the contents of the PyArrowFile instance
        >>> # Make sure that you have permissions to read/write
        >>> # file_content = input_file.open().read()

        >>> # output_file = PyArrowFile("s3://baz/qux.txt")
        >>> # Write bytes to a file
        >>> # Make sure that you have permissions to read/write
        >>> # output_file.create().write(b'foobytes')
    """

    _filesystem: FileSystem
    _path: str
    _buffer_size: int

    def __init__(self, location: str, path: str, fs: FileSystem, buffer_size: int = ONE_MEGABYTE):
        self._filesystem = fs
        self._path = path
        self._buffer_size = buffer_size
        super().__init__(location=location)

    def _file_info(self) -> FileInfo:
        """Retrieve a pyarrow.fs.FileInfo object for the location.

        Raises:
            PermissionError: If the file at self.location cannot be accessed due to a permission error such as
                an AWS error code 15.
        """
        try:
            file_info = self._filesystem.get_file_info(self._path)
        except OSError as e:
            if e.errno == 13 or "AWS Error [code 15]" in str(e):
                raise PermissionError(f"Cannot get file info, access denied: {self.location}") from e
            raise  # pragma: no cover - If some other kind of OSError, raise the raw error

        if file_info.type == FileType.NotFound:
            raise FileNotFoundError(f"Cannot get file info, file not found: {self.location}")
        return file_info

    @override
    def __len__(self) -> int:
        """Return the total length of the file, in bytes."""
        file_info = self._file_info()
        return file_info.size

    @override
    def exists(self) -> bool:
        """Check whether the location exists."""
        try:
            self._file_info()  # raises FileNotFoundError if it does not exist
            return True
        except FileNotFoundError:
            return False

    @override
    def open(self, seekable: bool = True) -> InputStream:
        """Open the location using a PyArrow FileSystem inferred from the location.

        Args:
            seekable: If the stream should support seek, or if it is consumed sequential.

        Returns:
            pyarrow.lib.NativeFile: A NativeFile instance for the file located at `self.location`.

        Raises:
            FileNotFoundError: If the file at self.location does not exist.
            PermissionError: If the file at self.location cannot be accessed due to a permission error such as
                an AWS error code 15.
        """
        try:
            if seekable:
                input_file = self._filesystem.open_input_file(self._path)
            else:
                input_file = self._filesystem.open_input_stream(self._path, buffer_size=self._buffer_size)
        except (FileNotFoundError, PermissionError):
            raise
        except OSError as e:
            if e.errno == 2 or "Path does not exist" in str(e):
                raise FileNotFoundError(f"Cannot open file, does not exist: {self.location}") from e
            elif e.errno == 13 or "AWS Error [code 15]" in str(e):
                raise PermissionError(f"Cannot open file, access denied: {self.location}") from e
            raise  # pragma: no cover - If some other kind of OSError, raise the raw error
        return input_file

    @override
    def create(self, overwrite: bool = False) -> OutputStream:
        """Create a writable pyarrow.lib.NativeFile for this PyArrowFile's location.

        Args:
            overwrite (bool): Whether to overwrite the file if it already exists.

        Returns:
            pyarrow.lib.NativeFile: A NativeFile instance for the file located at self.location.

        Raises:
            FileExistsError: If the file already exists at `self.location` and `overwrite` is False.

        Note:
            This retrieves a pyarrow NativeFile by opening an output stream. If overwrite is set to False,
            a check is first performed to verify that the file does not exist. This is not thread-safe and
            a possibility does exist that the file can be created by a concurrent process after the existence
            check yet before the output stream is created. In such a case, the default pyarrow behavior will
            truncate the contents of the existing file when opening the output stream.
        """
        try:
            if not overwrite and self.exists() is True:
                raise FileExistsError(f"Cannot create file, already exists: {self.location}")
            output_file = self._filesystem.open_output_stream(self._path, buffer_size=self._buffer_size)
        except PermissionError:
            raise
        except OSError as e:
            if e.errno == 13 or "AWS Error [code 15]" in str(e):
                raise PermissionError(f"Cannot create file, access denied: {self.location}") from e
            raise  # pragma: no cover - If some other kind of OSError, raise the raw error
        return output_file

    @override
    def to_input_file(self) -> PyArrowFile:
        """Return a new PyArrowFile for the location of an existing PyArrowFile instance.

        This method is included to abide by the OutputFile abstract base class. Since this implementation uses a single
        PyArrowFile class (as opposed to separate InputFile and OutputFile implementations), this method effectively returns
        a copy of the same instance.
        """
        return self


class PyArrowFileIO(FileIO):
    """FileIO implementation using PyArrow's filesystem interface.

    Supports S3, GCS, Azure, HDFS, OSS, and local filesystems. The correct
    filesystem type is inferred from the URI scheme.
    """

    fs_by_scheme: Callable[[str, str | None], FileSystem]

    def __init__(self, properties: Properties = EMPTY_DICT):
        self.fs_by_scheme: Callable[[str, str | None], FileSystem] = lru_cache(self._initialize_fs)
        super().__init__(properties=properties)

    @staticmethod
    def parse_location(location: str, properties: Properties = EMPTY_DICT) -> tuple[str, str, str]:
        """Return (scheme, netloc, path) for the given location.

        Uses DEFAULT_SCHEME and DEFAULT_NETLOC if scheme/netloc are missing.
        """
        uri = urlparse(location)

        if not uri.scheme:
            default_scheme = properties.get("DEFAULT_SCHEME", "file")
            default_netloc = properties.get("DEFAULT_NETLOC", "")
            return default_scheme, default_netloc, os.path.abspath(location)
        elif uri.scheme in ("hdfs", "viewfs"):
            return uri.scheme, uri.netloc, uri.path
        else:
            return uri.scheme, uri.netloc, f"{uri.netloc}{uri.path}"

    def _initialize_fs(self, scheme: str, netloc: str | None = None) -> FileSystem:
        """Initialize FileSystem for different scheme."""
        if scheme in {"oss"}:
            return self._initialize_oss_fs()

        elif scheme in {"s3", "s3a", "s3n"}:
            return self._initialize_s3_fs(netloc)

        elif scheme in {"hdfs", "viewfs"}:
            return self._initialize_hdfs_fs(scheme, netloc)

        elif scheme in {"gs", "gcs"}:
            return self._initialize_gcs_fs()

        elif scheme in {"abfs", "abfss", "wasb", "wasbs"}:
            return self._initialize_azure_fs()

        elif scheme in {"file"}:
            return self._initialize_local_fs()

        else:
            raise ValueError(f"Unrecognized filesystem type in URI: {scheme}")

    def _initialize_oss_fs(self) -> FileSystem:
        from pyarrow.fs import S3FileSystem

        client_kwargs: dict[str, Any] = {
            "endpoint_override": self.properties.get(S3_ENDPOINT),
            "access_key": get_first_property_value(self.properties, S3_ACCESS_KEY_ID, AWS_ACCESS_KEY_ID),
            "secret_key": get_first_property_value(self.properties, S3_SECRET_ACCESS_KEY, AWS_SECRET_ACCESS_KEY),
            "session_token": get_first_property_value(self.properties, S3_SESSION_TOKEN, AWS_SESSION_TOKEN),
            "region": get_first_property_value(self.properties, S3_REGION, AWS_REGION),
            "force_virtual_addressing": property_as_bool(self.properties, S3_FORCE_VIRTUAL_ADDRESSING, True),
        }

        if proxy_uri := self.properties.get(S3_PROXY_URI):
            client_kwargs["proxy_options"] = proxy_uri

        if connect_timeout := self.properties.get(S3_CONNECT_TIMEOUT):
            client_kwargs["connect_timeout"] = float(connect_timeout)

        if request_timeout := self.properties.get(S3_REQUEST_TIMEOUT):
            client_kwargs["request_timeout"] = float(request_timeout)

        if role_arn := get_first_property_value(self.properties, S3_ROLE_ARN, AWS_ROLE_ARN):
            client_kwargs["role_arn"] = role_arn

        if session_name := get_first_property_value(self.properties, S3_ROLE_SESSION_NAME, AWS_ROLE_SESSION_NAME):
            client_kwargs["session_name"] = session_name

        if s3_anonymous := self.properties.get(S3_ANONYMOUS):
            client_kwargs["anonymous"] = strtobool(s3_anonymous)

        return S3FileSystem(**client_kwargs)

    def _initialize_s3_fs(self, netloc: str | None) -> FileSystem:
        from pyarrow.fs import S3FileSystem

        provided_region = get_first_property_value(self.properties, S3_REGION, AWS_REGION)

        # Do this when we don't provide the region at all, or when we explicitly enable it
        if provided_region is None or property_as_bool(self.properties, S3_RESOLVE_REGION, False) is True:
            # Resolve region from netloc(bucket), fallback to user-provided region
            # Only supported by buckets hosted by S3
            bucket_region = _cached_resolve_s3_region(bucket=netloc) or provided_region
            if provided_region is not None and bucket_region != provided_region:
                logger.warning(
                    f"PyArrow FileIO overriding S3 bucket region for bucket {netloc}: "
                    f"provided region {provided_region}, actual region {bucket_region}"
                )
        else:
            bucket_region = provided_region

        client_kwargs: dict[str, Any] = {
            "endpoint_override": self.properties.get(S3_ENDPOINT),
            "access_key": get_first_property_value(self.properties, S3_ACCESS_KEY_ID, AWS_ACCESS_KEY_ID),
            "secret_key": get_first_property_value(self.properties, S3_SECRET_ACCESS_KEY, AWS_SECRET_ACCESS_KEY),
            "session_token": get_first_property_value(self.properties, S3_SESSION_TOKEN, AWS_SESSION_TOKEN),
            "region": bucket_region,
        }

        if proxy_uri := self.properties.get(S3_PROXY_URI):
            client_kwargs["proxy_options"] = proxy_uri

        if connect_timeout := self.properties.get(S3_CONNECT_TIMEOUT):
            client_kwargs["connect_timeout"] = float(connect_timeout)

        if request_timeout := self.properties.get(S3_REQUEST_TIMEOUT):
            client_kwargs["request_timeout"] = float(request_timeout)

        if role_arn := get_first_property_value(self.properties, S3_ROLE_ARN, AWS_ROLE_ARN):
            client_kwargs["role_arn"] = role_arn

        if session_name := get_first_property_value(self.properties, S3_ROLE_SESSION_NAME, AWS_ROLE_SESSION_NAME):
            client_kwargs["session_name"] = session_name

        if self.properties.get(S3_FORCE_VIRTUAL_ADDRESSING) is not None:
            client_kwargs["force_virtual_addressing"] = property_as_bool(self.properties, S3_FORCE_VIRTUAL_ADDRESSING, False)

        if (retry_strategy_impl := self.properties.get(S3_RETRY_STRATEGY_IMPL)) and (
            retry_instance := _import_retry_strategy(retry_strategy_impl)
        ):
            client_kwargs["retry_strategy"] = retry_instance

        if s3_anonymous := self.properties.get(S3_ANONYMOUS):
            client_kwargs["anonymous"] = strtobool(s3_anonymous)

        return S3FileSystem(**client_kwargs)

    def _initialize_azure_fs(self) -> FileSystem:
        # https://arrow.apache.org/docs/python/generated/pyarrow.fs.AzureFileSystem.html
        from packaging import version

        MIN_PYARROW_VERSION_SUPPORTING_AZURE_FS = "20.0.0"
        if version.parse(pyarrow.__version__) < version.parse(MIN_PYARROW_VERSION_SUPPORTING_AZURE_FS):
            raise ImportError(
                f"pyarrow version >= {MIN_PYARROW_VERSION_SUPPORTING_AZURE_FS} required for AzureFileSystem support, "
                f"but found version {pyarrow.__version__}."
            )

        from pyarrow.fs import AzureFileSystem

        client_kwargs: dict[str, str] = {}

        if account_name := self.properties.get(ADLS_ACCOUNT_NAME):
            client_kwargs["account_name"] = account_name

        if account_key := self.properties.get(ADLS_ACCOUNT_KEY):
            client_kwargs["account_key"] = account_key

        if blob_storage_authority := self.properties.get(ADLS_BLOB_STORAGE_AUTHORITY):
            client_kwargs["blob_storage_authority"] = blob_storage_authority

        if dfs_storage_authority := self.properties.get(ADLS_DFS_STORAGE_AUTHORITY):
            client_kwargs["dfs_storage_authority"] = dfs_storage_authority

        if blob_storage_scheme := self.properties.get(ADLS_BLOB_STORAGE_SCHEME):
            client_kwargs["blob_storage_scheme"] = blob_storage_scheme

        if dfs_storage_scheme := self.properties.get(ADLS_DFS_STORAGE_SCHEME):
            client_kwargs["dfs_storage_scheme"] = dfs_storage_scheme

        if sas_token := self.properties.get(ADLS_SAS_TOKEN):
            client_kwargs["sas_token"] = sas_token

        if client_id := self.properties.get(ADLS_CLIENT_ID):
            client_kwargs["client_id"] = client_id
        if client_secret := self.properties.get(ADLS_CLIENT_SECRET):
            client_kwargs["client_secret"] = client_secret
        if tenant_id := self.properties.get(ADLS_TENANT_ID):
            client_kwargs["tenant_id"] = tenant_id

        # Validate that all three are provided together for ClientSecretCredential
        credential_keys = ["client_id", "client_secret", "tenant_id"]
        provided_keys = [key for key in credential_keys if key in client_kwargs]
        if provided_keys and len(provided_keys) != len(credential_keys):
            missing_keys = [key for key in credential_keys if key not in client_kwargs]
            raise ValueError(
                f"client_id, client_secret, and tenant_id must all be provided together "
                f"to use ClientSecretCredential for Azure authentication. "
                f"Provided: {provided_keys}, Missing: {missing_keys}"
            )

        return AzureFileSystem(**client_kwargs)

    def _initialize_hdfs_fs(self, scheme: str, netloc: str | None) -> FileSystem:
        from pyarrow.fs import HadoopFileSystem

        hdfs_kwargs: dict[str, Any] = {}
        if netloc:
            return HadoopFileSystem.from_uri(f"{scheme}://{netloc}")
        if host := self.properties.get(HDFS_HOST):
            hdfs_kwargs["host"] = host
        if port := self.properties.get(HDFS_PORT):
            # port should be an integer type
            hdfs_kwargs["port"] = int(port)
        if user := self.properties.get(HDFS_USER):
            hdfs_kwargs["user"] = user
        if kerb_ticket := self.properties.get(HDFS_KERB_TICKET):
            hdfs_kwargs["kerb_ticket"] = kerb_ticket

        return HadoopFileSystem(**hdfs_kwargs)

    def _initialize_gcs_fs(self) -> FileSystem:
        from pyarrow.fs import GcsFileSystem

        gcs_kwargs: dict[str, Any] = {}
        if access_token := self.properties.get(GCS_TOKEN):
            gcs_kwargs["access_token"] = access_token
        if expiration := self.properties.get(GCS_TOKEN_EXPIRES_AT_MS):
            gcs_kwargs["credential_token_expiration"] = millis_to_datetime(int(expiration))
        if bucket_location := self.properties.get(GCS_DEFAULT_LOCATION):
            gcs_kwargs["default_bucket_location"] = bucket_location
        if endpoint := self.properties.get(GCS_SERVICE_HOST):
            url_parts = urlparse(endpoint)
            gcs_kwargs["scheme"] = url_parts.scheme
            gcs_kwargs["endpoint_override"] = url_parts.netloc

        return GcsFileSystem(**gcs_kwargs)

    def _initialize_local_fs(self) -> FileSystem:
        return PyArrowLocalFileSystem()

    @override
    def new_input(self, location: str) -> PyArrowFile:
        """Get a PyArrowFile instance to read bytes from the file at the given location.

        Args:
            location (str): A URI or a path to a local file.

        Returns:
            PyArrowFile: A PyArrowFile instance for the given location.
        """
        scheme, netloc, path = self.parse_location(location, self.properties)
        return PyArrowFile(
            fs=self.fs_by_scheme(scheme, netloc),
            location=location,
            path=path,
            buffer_size=int(self.properties.get(BUFFER_SIZE, ONE_MEGABYTE)),
        )

    @override
    def new_output(self, location: str) -> PyArrowFile:
        """Get a PyArrowFile instance to write bytes to the file at the given location.

        Args:
            location (str): A URI or a path to a local file.

        Returns:
            PyArrowFile: A PyArrowFile instance for the given location.
        """
        scheme, netloc, path = self.parse_location(location, self.properties)
        return PyArrowFile(
            fs=self.fs_by_scheme(scheme, netloc),
            location=location,
            path=path,
            buffer_size=int(self.properties.get(BUFFER_SIZE, ONE_MEGABYTE)),
        )

    @override
    def delete(self, location: str | InputFile | OutputFile) -> None:
        """Delete the file at the given location.

        Args:
            location (Union[str, InputFile, OutputFile]): The URI to the file--if an InputFile instance or
                an OutputFile instance is provided, the location attribute for that instance is used as
                the location to delete.

        Raises:
            FileNotFoundError: When the file at the provided location does not exist.
            PermissionError: If the file at the provided location cannot be accessed due to a permission error such as
                an AWS error code 15.
        """
        str_location = location.location if isinstance(location, (InputFile, OutputFile)) else location
        scheme, netloc, path = self.parse_location(str_location, self.properties)
        fs = self.fs_by_scheme(scheme, netloc)

        try:
            fs.delete_file(path)
        except FileNotFoundError:
            raise
        except PermissionError:
            raise
        except OSError as e:
            if e.errno == 2 or "Path does not exist" in str(e):
                raise FileNotFoundError(f"Cannot delete file, does not exist: {location}") from e
            elif e.errno == 13 or "AWS Error [code 15]" in str(e):
                raise PermissionError(f"Cannot delete file, access denied: {location}") from e
            raise  # pragma: no cover - If some other kind of OSError, raise the raw error

    def __getstate__(self) -> dict[str, Any]:
        """Create a dictionary of the PyArrowFileIO fields used when pickling."""
        fileio_copy = copy(self.__dict__)
        fileio_copy["fs_by_scheme"] = None
        return fileio_copy

    def __setstate__(self, state: dict[str, Any]) -> None:
        """Deserialize the state into a PyArrowFileIO instance."""
        self.__dict__ = state
        self.fs_by_scheme = lru_cache(self._initialize_fs)
