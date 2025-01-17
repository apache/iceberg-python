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
import pickle
import uuid

import pytest
from pytest_lazyfixture import lazy_fixture

from pyiceberg.io import FileIO
from pyiceberg.io.fsspec import FsspecFileIO
from pyiceberg.io.pyarrow import PyArrowFileIO


@pytest.mark.adls
@pytest.mark.parametrize(
    "fileio",
    [lazy_fixture("adls_fsspec_fileio"), lazy_fixture("adls_pyarrow_fileio")],
)
def test_new_input_file_adls(fileio: FileIO) -> None:
    """Test creating a new input file from a file-io"""
    filename = str(uuid.uuid4())

    input_file = fileio.new_input(f"abfss://tests/{filename}")
    assert input_file.location == f"abfss://tests/{filename}"


@pytest.mark.adls
@pytest.mark.parametrize(
    "fileio",
    [lazy_fixture("adls_fsspec_fileio"), lazy_fixture("adls_pyarrow_fileio")],
)
def test_new_abfss_output_file_adls(fileio: FsspecFileIO) -> None:
    """Test creating a new output file from a file-io"""
    filename = str(uuid.uuid4())

    output_file = fileio.new_output(f"abfss://tests/{filename}")
    assert output_file.location == f"abfss://tests/{filename}"


@pytest.mark.adls
@pytest.mark.parametrize(
    "fileio",
    [lazy_fixture("adls_fsspec_fileio"), lazy_fixture("adls_pyarrow_fileio")],
)
def test_write_and_read_file_adls(fileio: FileIO) -> None:
    """Test writing and reading a file using FsspecInputFile and FsspecOutputFile"""
    filename = str(uuid.uuid4())
    output_file = fileio.new_output(location=f"abfss://tests/{filename}")
    with output_file.create() as f:
        f.write(b"foo")

    input_file = fileio.new_input(f"abfss://tests/{filename}")
    assert input_file.open().read() == b"foo"

    fileio.delete(input_file)


@pytest.mark.adls
@pytest.mark.parametrize(
    "fileio",
    [lazy_fixture("adls_fsspec_fileio"), lazy_fixture("adls_pyarrow_fileio")],
)
def test_getting_length_of_file_adls(fileio: FileIO) -> None:
    """Test getting the length of aInputFile and FsspecOutputFile"""
    filename = str(uuid.uuid4())

    output_file = fileio.new_output(location=f"abfss://tests/{filename}")
    with output_file.create() as f:
        f.write(b"foobar")

    assert len(output_file) == 6

    input_file = fileio.new_input(location=f"abfss://tests/{filename}")
    assert len(input_file) == 6

    fileio.delete(output_file)


@pytest.mark.adls
@pytest.mark.parametrize(
    "fileio",
    [lazy_fixture("adls_fsspec_fileio"), lazy_fixture("adls_pyarrow_fileio")],
)
def test_file_tell_adls(fileio: FileIO) -> None:
    """Test finding cursor position for a file-io file"""

    filename = str(uuid.uuid4())

    output_file = fileio.new_output(location=f"abfss://tests/{filename}")
    with output_file.create() as write_file:
        write_file.write(b"foobar")

    input_file = fileio.new_input(location=f"abfss://tests/{filename}")
    f = input_file.open()

    f.seek(0)
    assert f.tell() == 0
    f.seek(1)
    assert f.tell() == 1
    f.seek(3)
    assert f.tell() == 3
    f.seek(0)
    assert f.tell() == 0

    fileio.delete(f"abfss://tests/{filename}")


@pytest.mark.adls
@pytest.mark.parametrize(
    "fileio",
    [lazy_fixture("adls_fsspec_fileio"), lazy_fixture("adls_pyarrow_fileio")],
)
def test_read_specified_bytes_for_file_adls(fileio: FileIO) -> None:
    """Test reading a specified number of bytes from a file-io file"""

    filename = str(uuid.uuid4())
    output_file = fileio.new_output(location=f"abfss://tests/{filename}")
    with output_file.create() as write_file:
        write_file.write(b"foo")

    input_file = fileio.new_input(location=f"abfss://tests/{filename}")
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

    fileio.delete(input_file)


@pytest.mark.adls
@pytest.mark.parametrize(
    "fileio",
    [lazy_fixture("adls_fsspec_fileio"), lazy_fixture("adls_pyarrow_fileio")],
)
def test_raise_on_opening_file_not_found_adls(fileio: FileIO) -> None:
    """Test that a input file raises appropriately when the adls file is not found"""

    filename = str(uuid.uuid4())
    input_file = fileio.new_input(location=f"abfss://tests/{filename}")
    with pytest.raises(FileNotFoundError) as exc_info:
        input_file.open().read()

    assert filename in str(exc_info.value)


@pytest.mark.adls
@pytest.mark.parametrize(
    "fileio",
    [lazy_fixture("adls_fsspec_fileio"), lazy_fixture("adls_pyarrow_fileio")],
)
def test_checking_if_a_file_exists_adls(fileio: FileIO) -> None:
    """Test checking if a file exists"""

    non_existent_file = fileio.new_input(location="abfss://tests/does-not-exist.txt")
    assert not non_existent_file.exists()

    filename = str(uuid.uuid4())
    output_file = fileio.new_output(location=f"abfss://tests/{filename}")
    assert not output_file.exists()
    with output_file.create() as f:
        f.write(b"foo")

    existing_input_file = fileio.new_input(location=f"abfss://tests/{filename}")
    assert existing_input_file.exists()

    existing_output_file = fileio.new_output(location=f"abfss://tests/{filename}")
    assert existing_output_file.exists()

    fileio.delete(existing_output_file)


@pytest.mark.adls
@pytest.mark.parametrize(
    "fileio",
    [lazy_fixture("adls_fsspec_fileio"), lazy_fixture("adls_pyarrow_fileio")],
)
def test_closing_a_file_adls(fileio: FileIO) -> None:
    """Test closing an output file and input file"""
    filename = str(uuid.uuid4())
    output_file = fileio.new_output(location=f"abfss://tests/{filename}")
    with output_file.create() as write_file:
        write_file.write(b"foo")
        assert not write_file.closed  # type: ignore
    assert write_file.closed  # type: ignore

    input_file = fileio.new_input(location=f"abfss://tests/{filename}")
    f = input_file.open()
    assert not f.closed  # type: ignore
    f.close()
    assert f.closed  # type: ignore

    fileio.delete(f"abfss://tests/{filename}")


@pytest.mark.adls
@pytest.mark.parametrize(
    "fileio",
    [lazy_fixture("adls_fsspec_fileio"), lazy_fixture("adls_pyarrow_fileio")],
)
def test_converting_an_outputfile_to_an_inputfile_adls(fileio: FileIO) -> None:
    """Test converting an output file to an input file"""
    filename = str(uuid.uuid4())
    output_file = fileio.new_output(location=f"abfss://tests/{filename}")
    input_file = output_file.to_input_file()
    assert input_file.location == output_file.location


@pytest.mark.adls
@pytest.mark.parametrize(
    "fileio",
    [lazy_fixture("adls_fsspec_fileio"), lazy_fixture("adls_pyarrow_fileio")],
)
def test_writing_avro_file_adls(fileio: FileIO, generated_manifest_entry_file: str) -> None:
    """Test that bytes match when reading a local avro file and then reading it again"""
    filename = str(uuid.uuid4())
    with PyArrowFileIO().new_input(location=generated_manifest_entry_file).open() as f:
        b1 = f.read()
        with fileio.new_output(location=f"abfss://tests/{filename}").create() as out_f:
            out_f.write(b1)
        with fileio.new_input(location=f"abfss://tests/{filename}").open() as in_f:
            b2 = in_f.read()
            assert b1 == b2  # Check that bytes of read from local avro file match bytes written to adls

    fileio.delete(f"abfss://tests/{filename}")


@pytest.mark.adls
@pytest.mark.parametrize(
    "fileio",
    [lazy_fixture("adls_fsspec_fileio"), lazy_fixture("adls_pyarrow_fileio")],
)
def test_pickle_round_trip_adls(fileio: FileIO) -> None:
    _test_pickle_round_trip(fileio, "abfss://tests/foo.txt")


def _test_pickle_round_trip(fileio: FileIO, location: str) -> None:
    serialized_file_io = pickle.dumps(fileio)
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
