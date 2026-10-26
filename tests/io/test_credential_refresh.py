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
"""Credential rotation through real FileIO operations and external storage boundaries."""

from __future__ import annotations

import pickle
import threading
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any
from urllib.parse import urlparse

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from fsspec.implementations.memory import MemoryFile, MemoryFileSystem
from pyarrow.fs import FSSpecHandler, PyFileSystem
from requests_mock import Mocker

from pyiceberg.catalog.rest import Capability, LoadCredentialsResponse, RestCatalog
from pyiceberg.catalog.rest.credential_provider import REFRESH_CREDENTIALS_ENABLED, CredentialsProvider
from pyiceberg.catalog.rest.scan_planning import StorageCredential
from pyiceberg.io import ARROW_FILE_IO, FSSPEC_FILE_IO, PY_IO_IMPL, FileIO
from pyiceberg.io.fsspec import FsspecFileIO
from pyiceberg.io.pyarrow import PyArrowFileIO, schema_to_pyarrow
from pyiceberg.table import Table
from pyiceberg.typedef import Properties

CATALOG_URI = "https://catalog.example/"
TABLE_URI = CATALOG_URI + "v1/namespaces/db/tables/table"


class _Clock:
    now = 1_700_000_000.0

    def __call__(self) -> float:
        return self.now


@pytest.fixture
def clock(monkeypatch: pytest.MonkeyPatch) -> _Clock:
    clock = _Clock()
    monkeypatch.setattr("pyiceberg.catalog.rest.credential_provider.time", clock)
    return clock


@dataclass
class _Storage:
    token: str | None = "original"
    prefixes: dict[str, str | None] = field(default_factory=dict)
    files: dict[str, MemoryFile] = field(default_factory=dict)
    directories: list[str] = field(default_factory=lambda: [""])
    constructions: list[Properties] = field(default_factory=list)

    def authorize(self, path: str, token: str | None) -> None:
        path = _TokenFileSystem._strip_protocol(path)
        prefix = max((p for p in self.prefixes if path.startswith(p)), key=len, default=None)
        expected = self.prefixes[prefix] if prefix is not None else self.token
        if token != expected:
            raise PermissionError("Storage rejected the supplied credential")


class _TokenFileSystem(MemoryFileSystem):
    cachable = False  # codespell:ignore cachable

    def __init__(self, storage: _Storage, token: str | None) -> None:
        super().__init__()
        self.storage = storage
        self.token = token
        self.store = storage.files
        self.pseudo_dirs = storage.directories

    @classmethod
    def _strip_protocol(cls, path: str) -> str:
        uri = urlparse(str(path))
        if uri.scheme:
            path = uri.netloc + uri.path
        return MemoryFileSystem._strip_protocol(path)

    def info(self, path: str, **kwargs: Any) -> dict[str, Any]:
        self.storage.authorize(path, self.token)
        return super().info(path, **kwargs)

    def _open(
        self,
        path: str,
        mode: str = "rb",
        block_size: int | None = None,
        autocommit: bool = True,
        cache_options: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> MemoryFile:
        self.storage.authorize(path, self.token)
        return super()._open(path, mode, block_size, autocommit, cache_options, **kwargs)

    def rm(self, path: str, recursive: bool = False, maxdepth: int | None = None) -> None:
        self.storage.authorize(path, self.token)
        super().rm(path, recursive=recursive, maxdepth=maxdepth)


@dataclass
class _Backend:
    scheme: str
    implementation: str
    storage: _Storage

    def config(self, clock: _Clock, token: str = "original", lifetime: int = 60) -> Properties:
        expiration = str(int((clock.now + lifetime) * 1000))
        if self.scheme == "s3":
            return {
                "s3.access-key-id": "access",
                "s3.secret-access-key": "secret",
                "s3.session-token": token,
                "s3.session-token-expires-at-ms": expiration,
            }
        return {"gcs.oauth2.token": token, "gcs.oauth2.token-expires-at": expiration}

    def credential(self, clock: _Clock, token: str = "original", lifetime: int = 60) -> StorageCredential:
        return StorageCredential(prefix=f"{self.scheme}://warehouse/", config=self.config(clock, token, lifetime))

    @property
    def location(self) -> str:
        return f"{self.scheme}://warehouse/data/file"

    def new_io(self, properties: Properties | None = None) -> FileIO:
        implementation = PyArrowFileIO if self.implementation == ARROW_FILE_IO else FsspecFileIO
        return implementation({"s3.region": "us-east-1", **(properties or {})})


@pytest.fixture(
    params=[
        ("s3", ARROW_FILE_IO),
        ("gs", ARROW_FILE_IO),
        ("gcs", ARROW_FILE_IO),
        ("s3", FSSPEC_FILE_IO),
        ("gs", FSSPEC_FILE_IO),
        ("gcs", FSSPEC_FILE_IO),
    ]
)
def backend(request: pytest.FixtureRequest, monkeypatch: pytest.MonkeyPatch) -> _Backend:
    scheme, implementation = request.param
    storage = _Storage()

    def filesystem(**kwargs: Any) -> _TokenFileSystem | PyFileSystem:
        storage.constructions.append(kwargs)
        if implementation == ARROW_FILE_IO:
            token = kwargs.get("session_token") if scheme == "s3" else kwargs.get("access_token")
            return PyFileSystem(FSSpecHandler(_TokenFileSystem(storage, token)))
        token = kwargs["client_kwargs"].get("aws_session_token") if scheme == "s3" else kwargs.get("token")
        if token is not None and not isinstance(token, str):
            token = token.token
        return _TokenFileSystem(storage, token)

    sdk = (
        ("pyarrow.fs.S3FileSystem" if scheme == "s3" else "pyarrow.fs.GcsFileSystem")
        if implementation == ARROW_FILE_IO
        else ("s3fs.S3FileSystem" if scheme == "s3" else "gcsfs.GCSFileSystem")
    )
    monkeypatch.setattr(sdk, filesystem)
    return _Backend(scheme, implementation, storage)


def _load_table(
    backend: _Backend,
    requests_mock: Mocker,
    metadata: dict[str, Any],
    credentials: list[StorageCredential],
    config: Properties | None = None,
    enabled: bool = True,
    auth: Properties | None = None,
) -> Table:
    requests_mock.get(
        CATALOG_URI + "v1/config",
        json={
            "defaults": {},
            "overrides": {},
            "endpoints": [
                str(Capability.V1_LOAD_TABLE),
                str(Capability.V1_CREATE_TABLE),
                str(Capability.V1_UPDATE_TABLE),
                str(Capability.V1_LOAD_CREDENTIALS),
                str(Capability.V1_SUBMIT_TABLE_SCAN_PLAN),
            ],
        },
    )
    requests_mock.get(
        TABLE_URI,
        json={
            "metadata-location": f"{backend.scheme}://warehouse/metadata/current.json",
            "metadata": {**metadata, "location": f"{backend.scheme}://warehouse/"},
            "config": config or {},
            "storage-credentials": [credential.model_dump() for credential in credentials],
        },
    )
    properties: Properties = {
        PY_IO_IMPL: backend.implementation,
        "s3.region": "us-east-1",
        REFRESH_CREDENTIALS_ENABLED: str(enabled).lower(),
        "auth": auth or {"type": "noop"},
    }
    catalog = RestCatalog("test", uri=CATALOG_URI, **properties)
    return catalog.load_table(("db", "table"))


def test_existing_handles_read_write_and_delete_across_rotation(backend: _Backend, clock: _Clock) -> None:
    requests = []

    def fetch() -> LoadCredentialsResponse:
        requests.append("refresh")
        return LoadCredentialsResponse(storage_credentials=[backend.credential(clock, "rotated")])

    io = backend.new_io()
    io.set_credentials_provider(CredentialsProvider([backend.credential(clock)], fetch))
    output = io.new_output(backend.location)
    input_file = output.to_input_file()
    with output.create() as output_stream:
        output_stream.write(b"before")
    with input_file.open() as input_stream:
        assert input_stream.read() == b"before"
    assert len(backend.storage.constructions) == 1

    clock.now += 30
    backend.storage.token = "rotated"
    with output.create(overwrite=True) as output_stream:
        output_stream.write(b"after")
    with input_file.open() as input_stream:
        assert input_stream.read() == b"after"
    assert len(input_file) == 5
    io.delete(input_file)
    assert not input_file.exists()
    assert requests == ["refresh"]
    assert len(backend.storage.constructions) == 2


def test_catalog_credentials_do_not_leak_outside_matching_prefix(
    backend: _Backend, clock: _Clock, requests_mock: Mocker, example_table_metadata_with_snapshot_v1: dict[str, Any]
) -> None:
    credential = StorageCredential(prefix=f"{backend.scheme}://warehouse/metadata/", config=backend.config(clock))
    backend.storage.token = None
    backend.storage.prefixes["/warehouse/metadata/"] = "original"
    table = _load_table(backend, requests_mock, example_table_metadata_with_snapshot_v1, [credential])
    with table.io.new_output(backend.location).create() as output_stream:
        output_stream.write(b"uses configured fallback")
    with table.io.new_input(backend.location).open() as input_stream:
        assert input_stream.read() == b"uses configured fallback"


def test_rotated_credentials_replace_optional_backend_properties(
    backend: _Backend, clock: _Clock, requests_mock: Mocker, example_table_metadata_with_snapshot_v1: dict[str, Any]
) -> None:
    initial = backend.credential(clock)
    endpoint_property = "s3.endpoint" if backend.scheme == "s3" else "gcs.service.host"
    initial.config[endpoint_property] = "http://old.example"
    table = _load_table(backend, requests_mock, example_table_metadata_with_snapshot_v1, [initial])
    with table.io.new_output(backend.location).create() as output_stream:
        output_stream.write(b"before")
    clock.now += 30
    backend.storage.token = "rotated"
    requests_mock.get(
        TABLE_URI + "/credentials",
        json={"storage-credentials": [backend.credential(clock, "rotated").model_dump()]},
    )
    with table.io.new_input(backend.location).open() as input_stream:
        assert input_stream.read() == b"before"
    kwargs = backend.storage.constructions[-1]
    if backend.implementation == ARROW_FILE_IO:
        assert kwargs.get("endpoint_override") is None
    elif backend.scheme == "s3":
        assert kwargs["client_kwargs"].get("endpoint_url") is None
    else:
        assert kwargs.get("endpoint_url") is None


def test_legacy_config_credentials_refresh_and_survive_pickle(
    backend: _Backend, clock: _Clock, requests_mock: Mocker, example_table_metadata_with_snapshot_v1: dict[str, Any]
) -> None:
    config = backend.config(clock)
    if backend.scheme == "gs":
        config["gcs.oauth2.token_expires_at"] = config.pop("gcs.oauth2.token-expires-at")
    table = _load_table(backend, requests_mock, example_table_metadata_with_snapshot_v1, [], config=config)
    output = table.io.new_output(backend.location)
    with output.create() as output_stream:
        output_stream.write(b"catalog config credentials")
    clock.now += 30
    backend.storage.token = "rotated"
    refresh = requests_mock.get(
        TABLE_URI + "/credentials",
        json={"storage-credentials": [backend.credential(clock, "rotated").model_dump()]},
    )
    requests_before = requests_mock.call_count
    restored = pickle.loads(pickle.dumps(table.io))
    assert requests_mock.call_count == requests_before
    with restored.new_input(backend.location).open() as input_stream:
        assert input_stream.read() == b"catalog config credentials"
    assert refresh.called


def test_disabled_refresh_preserves_initial_credentials(
    backend: _Backend, clock: _Clock, requests_mock: Mocker, example_table_metadata_with_snapshot_v1: dict[str, Any]
) -> None:
    table = _load_table(
        backend, requests_mock, example_table_metadata_with_snapshot_v1, [backend.credential(clock)], enabled=False
    )
    refresh = requests_mock.get(TABLE_URI + "/credentials", status_code=500)
    clock.now += 1000
    with table.io.new_output(backend.location).create() as output_stream:
        output_stream.write(b"static behavior")
    with table.io.new_input(backend.location).open() as input_stream:
        assert input_stream.read() == b"static behavior"
    assert not refresh.called


def test_gcsfs_vended_tokens_do_not_use_tokeninfo_or_oauth_refresh(requests_mock: Mocker) -> None:
    io = FsspecFileIO(
        {"gcs.project-id": "test", "gcs.oauth2.token": "short-vended-token", "gcs.oauth2.token-expires-at": "1700000060000"}
    )
    fs = io.get_fs("gs")
    headers: dict[str, str] = {}
    fs.credentials.apply(headers)
    assert headers["authorization"] == "Bearer short-vended-token"
    assert requests_mock.call_count == 0


def test_gcsfs_preserves_caller_owned_credentials_and_anonymous_mode(requests_mock: Mocker) -> None:
    from google.auth.credentials import Credentials

    class RenewableCredentials(Credentials):
        expiry: datetime | None

        def refresh(self, request: object) -> None:
            self.token = "renewed-by-owner"
            self.expiry = None

    credentials = RenewableCredentials()
    credentials.token = "expired"
    credentials.expiry = datetime(2000, 1, 1)
    for token, expected in ((credentials, {"authorization": "Bearer renewed-by-owner"}), ("anon", {})):
        io = FsspecFileIO({"gcs.project-id": "test", "gcs.oauth2.token": token, "gcs.oauth2.token-expires-at": "1700000060000"})
        headers: dict[str, str] = {}
        io.get_fs("gs").credentials.apply(headers)
        assert headers == expected
    assert requests_mock.call_count == 0


def test_scan_scoped_io_keeps_refresh_and_backend_configuration(
    backend: _Backend, clock: _Clock, requests_mock: Mocker, example_table_metadata_with_snapshot_v1: dict[str, Any]
) -> None:
    endpoint_key = "s3.endpoint" if backend.scheme == "s3" else "gcs.service.host"
    table = _load_table(
        backend,
        requests_mock,
        example_table_metadata_with_snapshot_v1,
        [backend.credential(clock)],
        config={"scan-planning-mode": "server", endpoint_key: "http://storage.example"},
    )
    requests_mock.post(
        TABLE_URI + "/plan",
        json={
            "status": "completed",
            "file-scan-tasks": [
                {
                    "data-file": {
                        "spec-id": 0,
                        "content": "data",
                        "file-path": backend.location,
                        "file-format": "parquet",
                        "file-size-in-bytes": 1,
                        "record-count": 1,
                    }
                }
            ],
            "delete-files": [],
            "plan-tasks": [],
            "storage-credentials": [backend.credential(clock, "plan").model_dump()],
        },
    )
    scan = table.scan()
    tasks = list(scan.plan_files())
    backend.storage.token = "plan"
    with scan.io.new_output(tasks[0].file.file_path).create() as output_stream:
        output_stream.write(b"scan scoped access")
    clock.now += 30
    backend.storage.token = "rotated"
    refresh = requests_mock.get(
        TABLE_URI + "/credentials",
        json={"storage-credentials": [backend.credential(clock, "rotated").model_dump()]},
    )
    with scan.io.new_input(tasks[0].file.file_path).open() as input_stream:
        assert input_stream.read() == b"scan scoped access"
    assert refresh.called
    kwargs = backend.storage.constructions[-1]
    if backend.implementation == ARROW_FILE_IO:
        expected = "http://storage.example" if backend.scheme == "s3" else "storage.example"
        assert kwargs["endpoint_override"] == expected
    elif backend.scheme == "s3":
        assert kwargs["client_kwargs"]["endpoint_url"] == "http://storage.example"
    else:
        assert kwargs["endpoint_url"] == "http://storage.example"


def test_google_authenticated_io_recreates_auth_after_pickle(
    backend: _Backend,
    clock: _Clock,
    requests_mock: Mocker,
    example_table_metadata_with_snapshot_v1: dict[str, Any],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    catalog_tokens = ["initial-catalog-token"]

    class GoogleCredentials:
        def __init__(self) -> None:
            self.lock = threading.Lock()
            self.token: str | None = None

        def refresh(self, request: object) -> None:
            self.token = catalog_tokens[0]

    monkeypatch.setattr("google.auth.default", lambda **kwargs: (GoogleCredentials(), "test"))
    table = _load_table(
        backend,
        requests_mock,
        example_table_metadata_with_snapshot_v1,
        [backend.credential(clock)],
        auth={"type": "google", "google": {"scopes": ["https://www.googleapis.com/auth/cloud-platform"]}},
    )
    with table.io.new_output(backend.location).create() as output_stream:
        output_stream.write(b"authenticated refresh")
    clock.now += 30
    backend.storage.token = "rotated"
    catalog_tokens[0] = "renewed-catalog-token"
    refresh = requests_mock.get(
        TABLE_URI + "/credentials",
        request_headers={"Authorization": "Bearer renewed-catalog-token"},
        json={"storage-credentials": [backend.credential(clock, "rotated").model_dump()]},
    )
    requests_before = requests_mock.call_count
    restored = pickle.loads(pickle.dumps(table.io))
    assert requests_mock.call_count == requests_before
    with restored.new_input(backend.location).open() as input_stream:
        assert input_stream.read() == b"authenticated refresh"
    assert refresh.called


def test_staged_table_writes_with_refreshed_credentials_without_metadata_location(
    backend: _Backend, clock: _Clock, requests_mock: Mocker, example_table_metadata_no_snapshot_v1: dict[str, Any]
) -> None:
    table = _load_table(backend, requests_mock, example_table_metadata_no_snapshot_v1, [])
    metadata = {**example_table_metadata_no_snapshot_v1, "location": f"{backend.scheme}://warehouse/"}
    requests_mock.post(
        CATALOG_URI + "v1/namespaces/db/tables",
        json={"metadata": metadata, "storage-credentials": [backend.credential(clock).model_dump()]},
    )
    staged_url = CATALOG_URI + "v1/namespaces/db/tables/staged"
    requests_mock.post(
        staged_url,
        json={"metadata": metadata, "metadata-location": f"{backend.scheme}://warehouse/metadata/committed.json"},
    )
    transaction = table.catalog.create_table_transaction(("db", "staged"), schema=table.schema())
    clock.now += 30
    backend.storage.token = "rotated"
    refresh = requests_mock.get(
        staged_url + "/credentials",
        json={"storage-credentials": [backend.credential(clock, "rotated").model_dump()]},
    )
    rows = [{"foo": "staged", "bar": 1, "baz": True}]
    transaction.append(pa.Table.from_pylist(rows, schema=schema_to_pyarrow(table.schema())))
    committed = transaction.commit_transaction()
    assert refresh.called
    data_files = [path for path in backend.storage.files if path.endswith(".parquet")]
    assert len(data_files) == 1
    with committed.io.new_input(f"{backend.scheme}://{data_files[0].lstrip('/')}").open() as input_stream:
        assert pq.read_table(input_stream).to_pylist() == rows
