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

import os
import pickle
import tempfile
import threading
import uuid
from unittest import mock

import pytest
from botocore.awsrequest import AWSRequest
from fsspec.implementations.local import LocalFileSystem
from fsspec.spec import AbstractFileSystem
from requests_mock import Mocker

from pyiceberg.catalog.rest.auth import AUTH_MANAGER
from pyiceberg.exceptions import SignError
from pyiceberg.io import fsspec
from pyiceberg.io.fsspec import FsspecFileIO, S3V4RestSigner
from pyiceberg.io.pyarrow import PyArrowFileIO
from pyiceberg.typedef import Properties
from tests.conftest import UNIFIED_AWS_SESSION_PROPERTIES


def test_fsspec_infer_local_fs_from_path(fsspec_fileio: FsspecFileIO) -> None:
    """Test path with `file` scheme and no scheme both use LocalFileSystem"""
    assert isinstance(fsspec_fileio.new_output("file://tmp/warehouse")._fs, LocalFileSystem)
    assert isinstance(fsspec_fileio.new_output("/tmp/warehouse")._fs, LocalFileSystem)


def test_fsspec_local_fs_can_create_path_without_parent_dir(fsspec_fileio: FsspecFileIO) -> None:
    """Test LocalFileSystem can create path without first creating the parent directories"""
    with tempfile.TemporaryDirectory() as tmpdirname:
        file_path = f"{tmpdirname}/foo/bar/baz.txt"
        output_file = fsspec_fileio.new_output(file_path)
        parent_path = os.path.dirname(file_path)
        assert output_file._fs.exists(parent_path) is False
        try:
            with output_file.create() as f:
                f.write(b"foo")
        except Exception:
            pytest.fail("Failed to write to file without parent directory")


def test_fsspec_get_fs_instance_per_thread_caching(fsspec_fileio: FsspecFileIO) -> None:
    """Test that filesystem instances are cached per-thread by `FsspecFileIO.get_fs`"""
    fs_instances: list[AbstractFileSystem] = []
    start_work_events: list[threading.Event] = [threading.Event() for _ in range(2)]

    def get_fs(start_work_event: threading.Event) -> None:
        # Wait to be told to actually start getting the filesystem instances
        start_work_event.wait()

        # Call twice to ensure caching within the same thread
        for _ in range(2):
            fs_instances.append(fsspec_fileio.get_fs("file"))

    threads = [threading.Thread(target=get_fs, args=[start_work_event]) for start_work_event in start_work_events]

    # Start both threads (which will immediately block on their `Event`s) as we want to ensure distinct
    # `threading.get_ident()` values that are used in the `fsspec.spec.AbstractFileSystem`s cache keys..
    for thread in threads:
        thread.start()

    # Get the filesystem instances in the first thread and wait for completion
    start_work_events[0].set()
    threads[0].join()

    # Get the filesystem instances in the second thread and wait for completion
    start_work_events[1].set()
    threads[1].join()

    # Same thread, same instance
    assert fs_instances[0] is fs_instances[1]
    assert fs_instances[2] is fs_instances[3]

    # Different threads, different instances
    assert fs_instances[0] is not fs_instances[2]


@pytest.mark.s3
def test_fsspec_new_input_file(fsspec_fileio: FsspecFileIO) -> None:
    """Test creating a new input file from a fsspec file-io"""
    filename = str(uuid.uuid4())

    input_file = fsspec_fileio.new_input(f"s3://warehouse/{filename}")

    assert isinstance(input_file, fsspec.FsspecInputFile)
    assert input_file.location == f"s3://warehouse/{filename}"


@pytest.mark.s3
def test_fsspec_new_s3_output_file(fsspec_fileio: FsspecFileIO) -> None:
    """Test creating a new output file from an fsspec file-io"""
    filename = str(uuid.uuid4())

    output_file = fsspec_fileio.new_output(f"s3://warehouse/{filename}")

    assert isinstance(output_file, fsspec.FsspecOutputFile)
    assert output_file.location == f"s3://warehouse/{filename}"


@pytest.mark.s3
def test_fsspec_write_and_read_file(fsspec_fileio: FsspecFileIO) -> None:
    """Test writing and reading a file using FsspecInputFile and FsspecOutputFile"""
    filename = str(uuid.uuid4())
    output_file = fsspec_fileio.new_output(location=f"s3://warehouse/{filename}")
    with output_file.create() as f:
        f.write(b"foo")

    input_file = fsspec_fileio.new_input(f"s3://warehouse/{filename}")
    assert input_file.open().read() == b"foo"

    fsspec_fileio.delete(input_file)


@pytest.mark.s3
def test_fsspec_getting_length_of_file(fsspec_fileio: FsspecFileIO) -> None:
    """Test getting the length of an FsspecInputFile and FsspecOutputFile"""
    filename = str(uuid.uuid4())

    output_file = fsspec_fileio.new_output(location=f"s3://warehouse/{filename}")
    with output_file.create() as f:
        f.write(b"foobar")

    assert len(output_file) == 6

    input_file = fsspec_fileio.new_input(location=f"s3://warehouse/{filename}")
    assert len(input_file) == 6

    fsspec_fileio.delete(output_file)


@pytest.mark.s3
def test_fsspec_file_tell(fsspec_fileio: FsspecFileIO) -> None:
    """Test finding cursor position for an fsspec file-io file"""

    filename = str(uuid.uuid4())

    output_file = fsspec_fileio.new_output(location=f"s3://warehouse/{filename}")
    with output_file.create() as write_file:
        write_file.write(b"foobar")

    input_file = fsspec_fileio.new_input(location=f"s3://warehouse/{filename}")
    f = input_file.open()

    f.seek(0)
    assert f.tell() == 0
    f.seek(1)
    assert f.tell() == 1
    f.seek(3)
    assert f.tell() == 3
    f.seek(0)
    assert f.tell() == 0


@pytest.mark.s3
def test_fsspec_read_specified_bytes_for_file(fsspec_fileio: FsspecFileIO) -> None:
    """Test reading a specified number of bytes from an fsspec file-io file"""

    filename = str(uuid.uuid4())
    output_file = fsspec_fileio.new_output(location=f"s3://warehouse/{filename}")
    with output_file.create() as write_file:
        write_file.write(b"foo")

    input_file = fsspec_fileio.new_input(location=f"s3://warehouse/{filename}")
    f = input_file.open()

    f.seek(0)
    assert b"f" == f.read(1)
    f.seek(0)
    assert b"fo" == f.read(2)
    f.seek(1)
    assert b"o" == f.read(1)
    f.seek(1)
    assert b"oo" == f.read(2)
    f.seek(0)
    assert b"foo" == f.read(999)  # test reading amount larger than entire content length

    fsspec_fileio.delete(input_file)


@pytest.mark.s3
def test_fsspec_raise_on_opening_file_not_found(fsspec_fileio: FsspecFileIO) -> None:
    """Test that an fsspec input file raises appropriately when the s3 file is not found"""

    filename = str(uuid.uuid4())
    input_file = fsspec_fileio.new_input(location=f"s3://warehouse/{filename}")
    with pytest.raises(FileNotFoundError) as exc_info:
        input_file.open().read()

    assert filename in str(exc_info.value)


@pytest.mark.s3
def test_checking_if_a_file_exists(fsspec_fileio: FsspecFileIO) -> None:
    """Test checking if a file exists"""

    non_existent_file = fsspec_fileio.new_input(location="s3://warehouse/does-not-exist.txt")
    assert not non_existent_file.exists()

    filename = str(uuid.uuid4())
    output_file = fsspec_fileio.new_output(location=f"s3://warehouse/{filename}")
    assert not output_file.exists()
    with output_file.create() as f:
        f.write(b"foo")

    existing_input_file = fsspec_fileio.new_input(location=f"s3://warehouse/{filename}")
    assert existing_input_file.exists()

    existing_output_file = fsspec_fileio.new_output(location=f"s3://warehouse/{filename}")
    assert existing_output_file.exists()

    fsspec_fileio.delete(existing_output_file)


@pytest.mark.s3
def test_closing_a_file(fsspec_fileio: FsspecFileIO) -> None:
    """Test closing an output file and input file"""
    filename = str(uuid.uuid4())
    output_file = fsspec_fileio.new_output(location=f"s3://warehouse/{filename}")
    with output_file.create() as write_file:
        write_file.write(b"foo")
        assert not write_file.closed  # type: ignore
    assert write_file.closed  # type: ignore

    input_file = fsspec_fileio.new_input(location=f"s3://warehouse/{filename}")
    f = input_file.open()
    assert not f.closed  # type: ignore
    f.close()
    assert f.closed  # type: ignore

    fsspec_fileio.delete(f"s3://warehouse/{filename}")


@pytest.mark.s3
def test_fsspec_converting_an_outputfile_to_an_inputfile(fsspec_fileio: FsspecFileIO) -> None:
    """Test converting an output file to an input file"""
    filename = str(uuid.uuid4())
    output_file = fsspec_fileio.new_output(location=f"s3://warehouse/{filename}")
    input_file = output_file.to_input_file()
    assert input_file.location == output_file.location


@pytest.mark.s3
def test_writing_avro_file(generated_manifest_entry_file: str, fsspec_fileio: FsspecFileIO) -> None:
    """Test that bytes match when reading a local avro file, writing it using fsspec file-io, and then reading it again"""
    filename = str(uuid.uuid4())
    with PyArrowFileIO().new_input(location=generated_manifest_entry_file).open() as f:
        b1 = f.read()
        with fsspec_fileio.new_output(location=f"s3://warehouse/{filename}").create() as out_f:
            out_f.write(b1)
        with fsspec_fileio.new_input(location=f"s3://warehouse/{filename}").open() as in_f:
            b2 = in_f.read()
            assert b1 == b2  # Check that bytes of read from local avro file match bytes written to s3

    fsspec_fileio.delete(f"s3://warehouse/{filename}")


@pytest.mark.s3
def test_fsspec_pickle_round_trip_s3(fsspec_fileio: FsspecFileIO) -> None:
    _test_fsspec_pickle_round_trip(fsspec_fileio, "s3://warehouse/foo.txt")


def test_fsspec_s3_session_properties() -> None:
    session_properties: Properties = {
        "s3.endpoint": "http://localhost:9000",
        "s3.access-key-id": "admin",
        "s3.secret-access-key": "password",
        "s3.region": "us-east-1",
        "s3.session-token": "s3.session-token",
        **UNIFIED_AWS_SESSION_PROPERTIES,
    }

    with mock.patch("s3fs.S3FileSystem") as mock_s3fs:
        s3_fileio = FsspecFileIO(properties=session_properties)
        filename = str(uuid.uuid4())

        s3_fileio.new_input(location=f"s3://warehouse/{filename}")

        mock_s3fs.assert_called_with(
            anon=False,
            client_kwargs={
                "endpoint_url": "http://localhost:9000",
                "aws_access_key_id": "admin",
                "aws_secret_access_key": "password",
                "region_name": "us-east-1",
                "aws_session_token": "s3.session-token",
            },
            config_kwargs={},
        )


def test_fsspec_s3_session_properties_force_virtual_addressing() -> None:
    session_properties: Properties = {
        "s3.force-virtual-addressing": True,
        "s3.endpoint": "http://localhost:9000",
        "s3.access-key-id": "admin",
        "s3.secret-access-key": "password",
        "s3.region": "us-east-1",
        "s3.session-token": "s3.session-token",
        **UNIFIED_AWS_SESSION_PROPERTIES,
    }

    with mock.patch("s3fs.S3FileSystem") as mock_s3fs:
        s3_fileio = FsspecFileIO(properties=session_properties)
        filename = str(uuid.uuid4())

        s3_fileio.new_input(location=f"s3://warehouse/{filename}")

        mock_s3fs.assert_called_with(
            anon=False,
            client_kwargs={
                "endpoint_url": "http://localhost:9000",
                "aws_access_key_id": "admin",
                "aws_secret_access_key": "password",
                "region_name": "us-east-1",
                "aws_session_token": "s3.session-token",
            },
            config_kwargs={"s3": {"addressing_style": "virtual"}},
        )


def test_fsspec_s3_session_properties_with_anonymous() -> None:
    session_properties: Properties = {
        "s3.anonymous": "true",
        "s3.endpoint": "http://localhost:9000",
        "s3.access-key-id": "admin",
        "s3.secret-access-key": "password",
        "s3.region": "us-east-1",
        "s3.session-token": "s3.session-token",
        **UNIFIED_AWS_SESSION_PROPERTIES,
    }

    with mock.patch("s3fs.S3FileSystem") as mock_s3fs:
        s3_fileio = FsspecFileIO(properties=session_properties)
        filename = str(uuid.uuid4())

        s3_fileio.new_input(location=f"s3://warehouse/{filename}")

        mock_s3fs.assert_called_with(
            anon=True,
            client_kwargs={
                "endpoint_url": "http://localhost:9000",
                "aws_access_key_id": "admin",
                "aws_secret_access_key": "password",
                "region_name": "us-east-1",
                "aws_session_token": "s3.session-token",
            },
            config_kwargs={},
        )


def test_fsspec_unified_session_properties() -> None:
    session_properties: Properties = {
        "s3.endpoint": "http://localhost:9000",
        **UNIFIED_AWS_SESSION_PROPERTIES,
    }

    with mock.patch("s3fs.S3FileSystem") as mock_s3fs:
        s3_fileio = FsspecFileIO(properties=session_properties)
        filename = str(uuid.uuid4())

        s3_fileio.new_input(location=f"s3://warehouse/{filename}")

        mock_s3fs.assert_called_with(
            anon=False,
            client_kwargs={
                "endpoint_url": "http://localhost:9000",
                "aws_access_key_id": "client.access-key-id",
                "aws_secret_access_key": "client.secret-access-key",
                "region_name": "client.region",
                "aws_session_token": "client.session-token",
            },
            config_kwargs={},
        )


@pytest.mark.adls
def test_fsspec_new_input_file_adls(adls_fsspec_fileio: FsspecFileIO) -> None:
    """Test creating a new input file from an fsspec file-io"""
    filename = str(uuid.uuid4())

    input_file = adls_fsspec_fileio.new_input(f"abfss://tests/{filename}")

    assert isinstance(input_file, fsspec.FsspecInputFile)
    assert input_file.location == f"abfss://tests/{filename}"


@pytest.mark.adls
def test_fsspec_new_abfss_output_file_adls(adls_fsspec_fileio: FsspecFileIO) -> None:
    """Test creating a new output file from an fsspec file-io"""
    filename = str(uuid.uuid4())

    output_file = adls_fsspec_fileio.new_output(f"abfss://tests/{filename}")

    assert isinstance(output_file, fsspec.FsspecOutputFile)
    assert output_file.location == f"abfss://tests/{filename}"


@pytest.mark.adls
def test_fsspec_write_and_read_file_adls(adls_fsspec_fileio: FsspecFileIO) -> None:
    """Test writing and reading a file using FsspecInputFile and FsspecOutputFile"""
    filename = str(uuid.uuid4())
    output_file = adls_fsspec_fileio.new_output(location=f"abfss://tests/{filename}")
    with output_file.create() as f:
        f.write(b"foo")

    input_file = adls_fsspec_fileio.new_input(f"abfss://tests/{filename}")
    assert input_file.open().read() == b"foo"

    adls_fsspec_fileio.delete(input_file)


@pytest.mark.adls
def test_fsspec_getting_length_of_file_adls(adls_fsspec_fileio: FsspecFileIO) -> None:
    """Test getting the length of an FsspecInputFile and FsspecOutputFile"""
    filename = str(uuid.uuid4())

    output_file = adls_fsspec_fileio.new_output(location=f"abfss://tests/{filename}")
    with output_file.create() as f:
        f.write(b"foobar")

    assert len(output_file) == 6

    input_file = adls_fsspec_fileio.new_input(location=f"abfss://tests/{filename}")
    assert len(input_file) == 6

    adls_fsspec_fileio.delete(output_file)


@pytest.mark.adls
def test_fsspec_file_tell_adls(adls_fsspec_fileio: FsspecFileIO) -> None:
    """Test finding cursor position for an fsspec file-io file"""

    filename = str(uuid.uuid4())

    output_file = adls_fsspec_fileio.new_output(location=f"abfss://tests/{filename}")
    with output_file.create() as write_file:
        write_file.write(b"foobar")

    input_file = adls_fsspec_fileio.new_input(location=f"abfss://tests/{filename}")
    f = input_file.open()

    f.seek(0)
    assert f.tell() == 0
    f.seek(1)
    assert f.tell() == 1
    f.seek(3)
    assert f.tell() == 3
    f.seek(0)
    assert f.tell() == 0

    adls_fsspec_fileio.delete(f"abfss://tests/{filename}")


@pytest.mark.adls
def test_fsspec_read_specified_bytes_for_file_adls(adls_fsspec_fileio: FsspecFileIO) -> None:
    """Test reading a specified number of bytes from an fsspec file-io file"""

    filename = str(uuid.uuid4())
    output_file = adls_fsspec_fileio.new_output(location=f"abfss://tests/{filename}")
    with output_file.create() as write_file:
        write_file.write(b"foo")

    input_file = adls_fsspec_fileio.new_input(location=f"abfss://tests/{filename}")
    f = input_file.open()

    f.seek(0)
    assert b"f" == f.read(1)
    f.seek(0)
    assert b"fo" == f.read(2)
    f.seek(1)
    assert b"o" == f.read(1)
    f.seek(1)
    assert b"oo" == f.read(2)
    f.seek(0)
    assert b"foo" == f.read(999)  # test reading amount larger than entire content length

    adls_fsspec_fileio.delete(input_file)


@pytest.mark.adls
def test_fsspec_raise_on_opening_file_not_found_adls(adls_fsspec_fileio: FsspecFileIO) -> None:
    """Test that an fsspec input file raises appropriately when the adls file is not found"""

    filename = str(uuid.uuid4())
    input_file = adls_fsspec_fileio.new_input(location=f"abfss://tests/{filename}")
    with pytest.raises(FileNotFoundError) as exc_info:
        input_file.open().read()

    assert filename in str(exc_info.value)


@pytest.mark.adls
def test_checking_if_a_file_exists_adls(adls_fsspec_fileio: FsspecFileIO) -> None:
    """Test checking if a file exists"""

    non_existent_file = adls_fsspec_fileio.new_input(location="abfss://tests/does-not-exist.txt")
    assert not non_existent_file.exists()

    filename = str(uuid.uuid4())
    output_file = adls_fsspec_fileio.new_output(location=f"abfss://tests/{filename}")
    assert not output_file.exists()
    with output_file.create() as f:
        f.write(b"foo")

    existing_input_file = adls_fsspec_fileio.new_input(location=f"abfss://tests/{filename}")
    assert existing_input_file.exists()

    existing_output_file = adls_fsspec_fileio.new_output(location=f"abfss://tests/{filename}")
    assert existing_output_file.exists()

    adls_fsspec_fileio.delete(existing_output_file)


@pytest.mark.adls
def test_closing_a_file_adls(adls_fsspec_fileio: FsspecFileIO) -> None:
    """Test closing an output file and input file"""
    filename = str(uuid.uuid4())
    output_file = adls_fsspec_fileio.new_output(location=f"abfss://tests/{filename}")
    with output_file.create() as write_file:
        write_file.write(b"foo")
        assert not write_file.closed  # type: ignore
    assert write_file.closed  # type: ignore

    input_file = adls_fsspec_fileio.new_input(location=f"abfss://tests/{filename}")
    f = input_file.open()
    assert not f.closed  # type: ignore
    f.close()
    assert f.closed  # type: ignore

    adls_fsspec_fileio.delete(f"abfss://tests/{filename}")


@pytest.mark.adls
def test_fsspec_converting_an_outputfile_to_an_inputfile_adls(adls_fsspec_fileio: FsspecFileIO) -> None:
    """Test converting an output file to an input file"""
    filename = str(uuid.uuid4())
    output_file = adls_fsspec_fileio.new_output(location=f"abfss://tests/{filename}")
    input_file = output_file.to_input_file()
    assert input_file.location == output_file.location


@pytest.mark.adls
def test_writing_avro_file_adls(generated_manifest_entry_file: str, adls_fsspec_fileio: FsspecFileIO) -> None:
    """Test that bytes match when reading a local avro file, writing it using fsspec file-io, and then reading it again"""
    filename = str(uuid.uuid4())
    with PyArrowFileIO().new_input(location=generated_manifest_entry_file).open() as f:
        b1 = f.read()
        with adls_fsspec_fileio.new_output(location=f"abfss://tests/{filename}").create() as out_f:
            out_f.write(b1)
        with adls_fsspec_fileio.new_input(location=f"abfss://tests/{filename}").open() as in_f:
            b2 = in_f.read()
            assert b1 == b2  # Check that bytes of read from local avro file match bytes written to adls

    adls_fsspec_fileio.delete(f"abfss://tests/{filename}")


@pytest.mark.adls
def test_fsspec_pickle_round_trip_aldfs(adls_fsspec_fileio: FsspecFileIO) -> None:
    _test_fsspec_pickle_round_trip(adls_fsspec_fileio, "abfss://tests/foo.txt")


@pytest.mark.adls
def test_adls_account_name_sas_token_extraction() -> None:
    session_properties: Properties = {
        "adls.tenant-id": "test-tenant-id",
        "adls.account-host": "testaccount.dfs.core.windows.net",
        "adls.sas-token.testaccount.dfs.core.windows.net": "test-sas-token",
        "adls.sas-token-expires-at-ms.testaccount.dfs.core.windows.net": "1757597218121",
    }

    with mock.patch("adlfs.AzureBlobFileSystem") as mock_adlfs:
        adls_fileio = FsspecFileIO(properties=session_properties)
        filename = str(uuid.uuid4())

        adls_fileio.new_input(location=f"abfss://tests/{filename}")

        mock_adlfs.assert_called_with(
            connection_string=None,
            credential=None,
            account_name="testaccount",
            account_key=None,
            sas_token="test-sas-token",
            tenant_id="test-tenant-id",
            client_id=None,
            client_secret=None,
            account_host="testaccount.dfs.core.windows.net",
            anon=None,
        )


@pytest.mark.gcs
def test_fsspec_new_input_file_gcs(fsspec_fileio_gcs: FsspecFileIO) -> None:
    """Test creating a new input file from a fsspec file-io"""
    location = f"gs://warehouse/{uuid.uuid4()}.txt"

    input_file = fsspec_fileio_gcs.new_input(location=location)

    assert isinstance(input_file, fsspec.FsspecInputFile)
    assert input_file.location == location


@pytest.mark.gcs
def test_fsspec_new_output_file_gcs(fsspec_fileio_gcs: FsspecFileIO) -> None:
    """Test creating a new output file from an fsspec file-io"""
    location = f"gs://warehouse/{uuid.uuid4()}.txt"

    output_file = fsspec_fileio_gcs.new_output(location=location)

    assert isinstance(output_file, fsspec.FsspecOutputFile)
    assert output_file.location == location


@pytest.mark.gcs
def test_fsspec_write_and_read_file_gcs(fsspec_fileio_gcs: FsspecFileIO) -> None:
    """Test writing and reading a file using FsspecInputFile and FsspecOutputFile"""
    location = f"gs://warehouse/{uuid.uuid4()}.txt"
    output_file = fsspec_fileio_gcs.new_output(location=location)
    with output_file.create() as f:
        f.write(b"foo")

    input_file = fsspec_fileio_gcs.new_input(location)
    with input_file.open() as f:
        assert f.read() == b"foo"

    fsspec_fileio_gcs.delete(input_file)


@pytest.mark.gcs
def test_fsspec_getting_length_of_file_gcs(fsspec_fileio_gcs: FsspecFileIO) -> None:
    """Test getting the length of an FsspecInputFile and FsspecOutputFile"""
    location = f"gs://warehouse/{uuid.uuid4()}.txt"

    output_file = fsspec_fileio_gcs.new_output(location=location)
    with output_file.create() as f:
        f.write(b"foobar")

    assert len(output_file) == 6

    input_file = fsspec_fileio_gcs.new_input(location=location)
    assert len(input_file) == 6

    fsspec_fileio_gcs.delete(output_file)


@pytest.mark.gcs
def test_fsspec_file_tell_gcs(fsspec_fileio_gcs: FsspecFileIO) -> None:
    """Test finding cursor position for an fsspec file-io file"""
    location = f"gs://warehouse/{uuid.uuid4()}.txt"

    output_file = fsspec_fileio_gcs.new_output(location=location)
    with output_file.create() as write_file:
        write_file.write(b"foobar")

    input_file = fsspec_fileio_gcs.new_input(location=location)
    with input_file.open() as f:
        f.seek(0)
        assert f.tell() == 0
        f.seek(1)
        assert f.tell() == 1
        f.seek(3)
        assert f.tell() == 3
        f.seek(0)
        assert f.tell() == 0


@pytest.mark.gcs
def test_fsspec_read_specified_bytes_for_file_gcs(fsspec_fileio_gcs: FsspecFileIO) -> None:
    """Test reading a specified number of bytes from a fsspec file-io file"""
    location = f"gs://warehouse/{uuid.uuid4()}.txt"

    output_file = fsspec_fileio_gcs.new_output(location=location)
    with output_file.create() as write_file:
        write_file.write(b"foo")

    input_file = fsspec_fileio_gcs.new_input(location=location)
    with input_file.open() as f:
        f.seek(0)
        assert b"f" == f.read(1)
        f.seek(0)
        assert b"fo" == f.read(2)
        f.seek(1)
        assert b"o" == f.read(1)
        f.seek(1)
        assert b"oo" == f.read(2)
        f.seek(0)
        assert b"foo" == f.read(999)  # test reading amount larger than entire content length

    fsspec_fileio_gcs.delete(input_file)


@pytest.mark.gcs
def test_fsspec_raise_on_opening_file_not_found_gcs(fsspec_fileio_gcs: FsspecFileIO) -> None:
    """Test that a fsspec input file raises appropriately when the gcs file is not found"""
    location = f"gs://warehouse/{uuid.uuid4()}.txt"
    input_file = fsspec_fileio_gcs.new_input(location=location)
    with pytest.raises(FileNotFoundError) as exc_info:
        input_file.open().read()

    assert location in str(exc_info.value)


@pytest.mark.gcs
def test_checking_if_a_file_exists_gcs(fsspec_fileio_gcs: FsspecFileIO) -> None:
    """Test checking if a file exists"""

    non_existent_file = fsspec_fileio_gcs.new_input(location="gs://warehouse/does-not-exist.txt")
    assert not non_existent_file.exists()

    location = f"gs://warehouse/{uuid.uuid4()}.txt"
    output_file = fsspec_fileio_gcs.new_output(location=location)
    assert not output_file.exists()
    with output_file.create() as f:
        f.write(b"foo")

    existing_input_file = fsspec_fileio_gcs.new_input(location=location)
    assert existing_input_file.exists()

    existing_output_file = fsspec_fileio_gcs.new_output(location=location)
    assert existing_output_file.exists()

    fsspec_fileio_gcs.delete(existing_output_file)


@pytest.mark.gcs
def test_closing_a_file_gcs(fsspec_fileio_gcs: FsspecFileIO) -> None:
    """Test closing an output file and input file"""
    location = f"gs://warehouse/{uuid.uuid4()}.txt"
    output_file = fsspec_fileio_gcs.new_output(location=location)
    with output_file.create() as write_file:
        write_file.write(b"foo")
        assert not write_file.closed  # type: ignore
    assert write_file.closed  # type: ignore

    input_file = fsspec_fileio_gcs.new_input(location=location)
    f = input_file.open()
    assert not f.closed  # type: ignore
    f.close()
    assert f.closed  # type: ignore

    fsspec_fileio_gcs.delete(location=location)


@pytest.mark.gcs
def test_fsspec_converting_an_outputfile_to_an_inputfile_gcs(fsspec_fileio_gcs: FsspecFileIO) -> None:
    """Test converting an output file to an input file"""
    filename = str(uuid.uuid4())
    output_file = fsspec_fileio_gcs.new_output(location=f"gs://warehouse/{filename}")
    input_file = output_file.to_input_file()
    assert input_file.location == output_file.location


@pytest.mark.gcs
def test_writing_avro_file_gcs(generated_manifest_entry_file: str, fsspec_fileio_gcs: FsspecFileIO) -> None:
    """Test that bytes match when reading a local avro file, writing it using fsspec file-io, and then reading it again"""
    filename = str(uuid.uuid4())
    with PyArrowFileIO().new_input(location=generated_manifest_entry_file).open() as f:
        b1 = f.read()
        with fsspec_fileio_gcs.new_output(location=f"gs://warehouse/{filename}").create() as out_f:
            out_f.write(b1)
        with fsspec_fileio_gcs.new_input(location=f"gs://warehouse/{filename}").open() as in_f:
            b2 = in_f.read()
            assert b1 == b2  # Check that bytes of read from local avro file match bytes written to s3

    fsspec_fileio_gcs.delete(f"gs://warehouse/{filename}")


@pytest.mark.gcs
def test_fsspec_pickle_roundtrip_gcs(fsspec_fileio_gcs: FsspecFileIO) -> None:
    _test_fsspec_pickle_round_trip(fsspec_fileio_gcs, "gs://warehouse/foo.txt")


def _test_fsspec_pickle_round_trip(fsspec_fileio: FsspecFileIO, location: str) -> None:
    serialized_file_io = pickle.dumps(fsspec_fileio)
    deserialized_file_io = pickle.loads(serialized_file_io)
    output_file = deserialized_file_io.new_output(location)
    with output_file.create() as f:
        f.write(b"foo")

    input_file = deserialized_file_io.new_input(location)
    with input_file.open() as f:
        data = f.read()
        assert data == b"foo"
        assert len(input_file) == 3
    deserialized_file_io.delete(location)


TEST_URI = "https://iceberg-test-signer"


def test_s3v4_rest_signer(requests_mock: Mocker) -> None:
    new_uri = "https://other-bucket/metadata/snap-8048355899640248710-1-a5c8ea2d-aa1f-48e8-89f4-1fa69db8c742.avro"
    requests_mock.post(
        f"{TEST_URI}/v1/aws/s3/sign",
        json={
            "uri": new_uri,
            "headers": {
                "Authorization": [
                    "AWS4-HMAC-SHA256 Credential=ASIAQPRZZYGHUT57DL3I/20221017/us-west-2/s3/aws4_request, "
                    "SignedHeaders=host;x-amz-content-sha256;x-amz-date;x-amz-security-token, "
                    "Signature=430582a17d61ab02c272896fa59195f277af4bdf2121c441685e589f044bbe02"
                ],
                "Host": ["bucket.s3.us-west-2.amazonaws.com"],
                "User-Agent": ["Botocore/1.27.59 Python/3.10.7 Darwin/21.5.0"],
                "x-amz-content-sha256": ["UNSIGNED-PAYLOAD"],
                "X-Amz-Date": ["20221017T102940Z"],
                "X-Amz-Security-Token": [
                    "YQoJb3JpZ2luX2VjEDoaCXVzLXdlc3QtMiJGMEQCID/fFxZP5oaEgQmcwP6XhZa0xSq9lmLSx8ffaWbySfUPAiAesa7sjd/WV4uwRTO0S03y/MWVtgpH+/NyZQ4bZgLVriqrAggTEAEaDDAzMzQwNzIyMjE1OSIMOeFOWhZIurMmAqjsKogCxMCqxX8ZjK0gacAkcDqBCyA7qTSLhdfKQIH/w7WpLBU1km+cRUWWCudan6gZsAq867DBaKEP7qI05DAWr9MChAkgUgyI8/G3Z23ET0gAedf3GsJbakB0F1kklx8jPmj4BPCht9RcTiXiJ5DxTS/cRCcalIQXmPFbaJSqpBusVG2EkWnm1v7VQrNPE2Os2b2P293vpbhwkyCEQiGRVva4Sw9D1sKvqSsK10QCRG+os6dFEOu1kARaXi6pStvR4OVmj7OYeAYjzaFchn7nz2CSae0M4IluiYQ01eQAywbfRo9DpKSmDM/DnPZWJnD/woLhaaaCrCxSSEaFsvGOHFhLd3Rknw1v0jADMILUtJoGOp4BpqKqyMz0CY3kpKL0jfR3ykTf/ge9wWVE0Alr7wRIkGCIURkhslGHqSyFRGoTqIXaxU+oPbwlw/0w/nYO7qQ6bTANOWye/wgw4h/NmJ6vU7wnZTXwREf1r6MF72++bE/fMk19LfVb8jN/qrUqAUXTc8gBAUxL5pgy8+oT/JnI2BkVrrLS4ilxEXP9Ahm+6GDUYXV4fBpqpZwdkzQ/5Gw="
                ],
                "X-Custom-Header": ["value"],
            },
            "extensions": {},
        },
        status_code=200,
    )

    request = AWSRequest(
        method="HEAD",
        url="https://bucket/metadata/snap-8048355899640248710-1-a5c8ea2d-aa1f-48e8-89f4-1fa69db8c742.avro",
        headers={"User-Agent": "Botocore/1.27.59 Python/3.10.7 Darwin/21.5.0"},
        data=b"",
        params={},
        auth_path="/metadata/snap-8048355899640248710-1-a5c8ea2d-aa1f-48e8-89f4-1fa69db8c742.avro",
    )
    request.context = {
        "client_region": "us-west-2",
        "has_streaming_input": False,
        "auth_type": None,
        "signing": {"bucket": "bucket"},
        "retries": {"attempt": 1, "invocation-id": "75d143fb-0219-439b-872c-18213d1c8d54"},
    }

    signer = S3V4RestSigner(properties={"token": "abc", "uri": TEST_URI, "header.X-Custom-Header": "value"})
    signer(request)

    assert request.url == new_uri
    assert dict(request.headers) == {
        "Authorization": (
            "AWS4-HMAC-SHA256 Credential=ASIAQPRZZYGHUT57DL3I/20221017/us-west-2/s3/aws4_request, "
            "SignedHeaders=host;x-amz-content-sha256;x-amz-date;x-amz-security-token, "
            "Signature=430582a17d61ab02c272896fa59195f277af4bdf2121c441685e589f044bbe02"
        ),
        "Host": "bucket.s3.us-west-2.amazonaws.com",
        "User-Agent": "Botocore/1.27.59 Python/3.10.7 Darwin/21.5.0",
        "X-Amz-Date": "20221017T102940Z",
        "X-Amz-Security-Token": (
            "YQoJb3JpZ2luX2VjEDoaCXVzLXdlc3QtMiJGMEQCID/fFxZP5oaEgQmcwP6XhZa0xSq9lmLSx8ffaWbySfUPAiAesa7sjd"
            "/WV4uwRTO0S03y/MWVtgpH+/NyZQ4bZgLVriqrAggTEAEaDDAzMzQwNzIyMjE1OSIMOeFOWhZIurMmAqjsKogCxMCqxX8Z"
            "jK0gacAkcDqBCyA7qTSLhdfKQIH/w7WpLBU1km+cRUWWCudan6gZsAq867DBaKEP7qI05DAWr9MChAkgUgyI8/G3Z23ET0"
            "gAedf3GsJbakB0F1kklx8jPmj4BPCht9RcTiXiJ5DxTS/cRCcalIQXmPFbaJSqpBusVG2EkWnm1v7VQrNPE2Os2b2P293v"
            "pbhwkyCEQiGRVva4Sw9D1sKvqSsK10QCRG+os6dFEOu1kARaXi6pStvR4OVmj7OYeAYjzaFchn7nz2CSae0M4IluiYQ01e"
            "QAywbfRo9DpKSmDM/DnPZWJnD/woLhaaaCrCxSSEaFsvGOHFhLd3Rknw1v0jADMILUtJoGOp4BpqKqyMz0CY3kpKL0jfR3"
            "ykTf/ge9wWVE0Alr7wRIkGCIURkhslGHqSyFRGoTqIXaxU+oPbwlw/0w/nYO7qQ6bTANOWye/wgw4h/NmJ6vU7wnZTXwRE"
            "f1r6MF72++bE/fMk19LfVb8jN/qrUqAUXTc8gBAUxL5pgy8+oT/JnI2BkVrrLS4ilxEXP9Ahm+6GDUYXV4fBpqpZwdkzQ/"
            "5Gw="
        ),
        "x-amz-content-sha256": "UNSIGNED-PAYLOAD",
        "X-Custom-Header": "value",
    }


def test_s3v4_rest_signer_endpoint(requests_mock: Mocker) -> None:
    new_uri = "https://other-bucket/metadata/snap-8048355899640248710-1-a5c8ea2d-aa1f-48e8-89f4-1fa69db8c742.avro"
    endpoint = "v1/main/s3-sign/foo.bar?e=e&b=b&k=k=k&s=s&w=w"
    requests_mock.post(
        f"{TEST_URI}/{endpoint}",
        json={
            "uri": new_uri,
            "headers": {
                "Authorization": [
                    "AWS4-HMAC-SHA256 Credential=ASIAQPRZZYGHUT57DL3I/20221017/us-west-2/s3/aws4_request, "
                    "SignedHeaders=host;x-amz-content-sha256;x-amz-date;x-amz-security-token, "
                    "Signature=430582a17d61ab02c272896fa59195f277af4bdf2121c441685e589f044bbe02"
                ],
                "Host": ["bucket.s3.us-west-2.amazonaws.com"],
                "User-Agent": ["Botocore/1.27.59 Python/3.10.7 Darwin/21.5.0"],
                "x-amz-content-sha256": ["UNSIGNED-PAYLOAD"],
                "X-Amz-Date": ["20221017T102940Z"],
                "X-Amz-Security-Token": [
                    "YQoJb3JpZ2luX2VjEDoaCXVzLXdlc3QtMiJGMEQCID/fFxZP5oaEgQmcwP6XhZa0xSq9lmLSx8ffaWbySfUPAiAesa7sjd/WV4uwRTO0S03y/MWVtgpH+/NyZQ4bZgLVriqrAggTEAEaDDAzMzQwNzIyMjE1OSIMOeFOWhZIurMmAqjsKogCxMCqxX8ZjK0gacAkcDqBCyA7qTSLhdfKQIH/w7WpLBU1km+cRUWWCudan6gZsAq867DBaKEP7qI05DAWr9MChAkgUgyI8/G3Z23ET0gAedf3GsJbakB0F1kklx8jPmj4BPCht9RcTiXiJ5DxTS/cRCcalIQXmPFbaJSqpBusVG2EkWnm1v7VQrNPE2Os2b2P293vpbhwkyCEQiGRVva4Sw9D1sKvqSsK10QCRG+os6dFEOu1kARaXi6pStvR4OVmj7OYeAYjzaFchn7nz2CSae0M4IluiYQ01eQAywbfRo9DpKSmDM/DnPZWJnD/woLhaaaCrCxSSEaFsvGOHFhLd3Rknw1v0jADMILUtJoGOp4BpqKqyMz0CY3kpKL0jfR3ykTf/ge9wWVE0Alr7wRIkGCIURkhslGHqSyFRGoTqIXaxU+oPbwlw/0w/nYO7qQ6bTANOWye/wgw4h/NmJ6vU7wnZTXwREf1r6MF72++bE/fMk19LfVb8jN/qrUqAUXTc8gBAUxL5pgy8+oT/JnI2BkVrrLS4ilxEXP9Ahm+6GDUYXV4fBpqpZwdkzQ/5Gw="
                ],
            },
            "extensions": {},
        },
        status_code=200,
    )

    request = AWSRequest(
        method="HEAD",
        url="https://bucket/metadata/snap-8048355899640248710-1-a5c8ea2d-aa1f-48e8-89f4-1fa69db8c742.avro",
        headers={"User-Agent": "Botocore/1.27.59 Python/3.10.7 Darwin/21.5.0"},
        data=b"",
        params={},
        auth_path="/metadata/snap-8048355899640248710-1-a5c8ea2d-aa1f-48e8-89f4-1fa69db8c742.avro",
    )
    request.context = {
        "client_region": "us-west-2",
        "has_streaming_input": False,
        "auth_type": None,
        "signing": {"bucket": "bucket"},
        "retries": {"attempt": 1, "invocation-id": "75d143fb-0219-439b-872c-18213d1c8d54"},
    }

    signer = S3V4RestSigner(properties={"token": "abc", "uri": TEST_URI, "s3.signer.endpoint": endpoint})
    signer(request)

    assert request.url == new_uri
    assert dict(request.headers) == {
        "Authorization": (
            "AWS4-HMAC-SHA256 Credential=ASIAQPRZZYGHUT57DL3I/20221017/us-west-2/s3/aws4_request, "
            "SignedHeaders=host;x-amz-content-sha256;x-amz-date;x-amz-security-token, "
            "Signature=430582a17d61ab02c272896fa59195f277af4bdf2121c441685e589f044bbe02"
        ),
        "Host": "bucket.s3.us-west-2.amazonaws.com",
        "User-Agent": "Botocore/1.27.59 Python/3.10.7 Darwin/21.5.0",
        "X-Amz-Date": "20221017T102940Z",
        "X-Amz-Security-Token": (
            "YQoJb3JpZ2luX2VjEDoaCXVzLXdlc3QtMiJGMEQCID/fFxZP5oaEgQmcwP6XhZa0xSq9lmLSx8ffaWbySfUPAiAesa7sjd"
            "/WV4uwRTO0S03y/MWVtgpH+/NyZQ4bZgLVriqrAggTEAEaDDAzMzQwNzIyMjE1OSIMOeFOWhZIurMmAqjsKogCxMCqxX8Z"
            "jK0gacAkcDqBCyA7qTSLhdfKQIH/w7WpLBU1km+cRUWWCudan6gZsAq867DBaKEP7qI05DAWr9MChAkgUgyI8/G3Z23ET0"
            "gAedf3GsJbakB0F1kklx8jPmj4BPCht9RcTiXiJ5DxTS/cRCcalIQXmPFbaJSqpBusVG2EkWnm1v7VQrNPE2Os2b2P293v"
            "pbhwkyCEQiGRVva4Sw9D1sKvqSsK10QCRG+os6dFEOu1kARaXi6pStvR4OVmj7OYeAYjzaFchn7nz2CSae0M4IluiYQ01e"
            "QAywbfRo9DpKSmDM/DnPZWJnD/woLhaaaCrCxSSEaFsvGOHFhLd3Rknw1v0jADMILUtJoGOp4BpqKqyMz0CY3kpKL0jfR3"
            "ykTf/ge9wWVE0Alr7wRIkGCIURkhslGHqSyFRGoTqIXaxU+oPbwlw/0w/nYO7qQ6bTANOWye/wgw4h/NmJ6vU7wnZTXwRE"
            "f1r6MF72++bE/fMk19LfVb8jN/qrUqAUXTc8gBAUxL5pgy8+oT/JnI2BkVrrLS4ilxEXP9Ahm+6GDUYXV4fBpqpZwdkzQ/"
            "5Gw="
        ),
        "x-amz-content-sha256": "UNSIGNED-PAYLOAD",
    }


def test_s3v4_rest_signer_forbidden(requests_mock: Mocker) -> None:
    requests_mock.post(
        f"{TEST_URI}/v1/aws/s3/sign",
        json={
            "method": "HEAD",
            "region": "us-west-2",
            "uri": "https://bucket/metadata/snap-8048355899640248710-1-a5c8ea2d-aa1f-48e8-89f4-1fa69db8c742.avro",
            "headers": {"User-Agent": ["Botocore/1.27.59 Python/3.10.7 Darwin/21.5.0"]},
        },
        status_code=401,
    )

    request = AWSRequest(
        method="HEAD",
        url="https://bucket/metadata/snap-8048355899640248710-1-a5c8ea2d-aa1f-48e8-89f4-1fa69db8c742.avro",
        headers={"User-Agent": "Botocore/1.27.59 Python/3.10.7 Darwin/21.5.0"},
        data=b"",
        params={},
        auth_path="/metadata/snap-8048355899640248710-1-a5c8ea2d-aa1f-48e8-89f4-1fa69db8c742.avro",
    )
    request.context = {
        "client_region": "us-west-2",
        "has_streaming_input": False,
        "auth_type": None,
        "signing": {"bucket": "bucket"},
        "retries": {"attempt": 1, "invocation-id": "75d143fb-0219-439b-872c-18213d1c8d54"},
    }

    signer = S3V4RestSigner(properties={"token": "abc", "uri": TEST_URI})
    with pytest.raises(SignError) as exc_info:
        signer(request)

    assert (
        "Failed to sign request 401: {'method': 'HEAD', 'region': 'us-west-2', "
        "'uri': 'https://bucket/metadata/snap-8048355899640248710-1-a5c8ea2d-aa1f-48e8-89f4-1fa69db8c742.avro', "
        "'headers': {'User-Agent': ['Botocore/1.27.59 Python/3.10.7 Darwin/21.5.0']}}" in str(exc_info.value)
    )


def test_s3v4_rest_signer_uses_auth_manager(requests_mock: Mocker) -> None:
    new_uri = "https://bucket/metadata/snap-signed.avro"
    requests_mock.post(
        f"{TEST_URI}/v1/aws/s3/sign",
        json={
            "uri": new_uri,
            "headers": {
                "Authorization": ["AWS4-HMAC-SHA256 Credential=ASIA.../s3/aws4_request, SignedHeaders=host, Signature=abc"],
                "Host": ["bucket.s3.us-west-2.amazonaws.com"],
            },
            "extensions": {},
        },
        status_code=200,
    )

    request = AWSRequest(
        method="HEAD",
        url="https://bucket/metadata/snap-foo.avro",
        headers={"User-Agent": "Botocore/1.27.59 Python/3.10.7 Darwin/21.5.0"},
        data=b"",
        params={},
        auth_path="/metadata/snap-foo.avro",
    )
    request.context = {
        "client_region": "us-west-2",
        "has_streaming_input": False,
        "auth_type": None,
        "signing": {"bucket": "bucket"},
        "retries": {"attempt": 1, "invocation-id": "75d143fb-0219-439b-872c-18213d1c8d54"},
    }

    class DummyAuthManager:
        def __init__(self) -> None:
            self.calls = 0

        def auth_header(self) -> str:
            self.calls += 1
            return "Bearer via-manager"

    auth_manager = DummyAuthManager()

    signer = S3V4RestSigner(properties={AUTH_MANAGER: auth_manager, "uri": TEST_URI})
    signer(request)

    assert auth_manager.calls == 1
    assert requests_mock.last_request is not None
    assert requests_mock.last_request.headers["Authorization"] == "Bearer via-manager"
    assert request.url == new_uri
