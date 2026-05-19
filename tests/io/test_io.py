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

import json
import os
import pickle
import tempfile
from datetime import datetime, timedelta
from typing import Any
from unittest.mock import MagicMock

import pytest
from requests import HTTPError
from requests.models import Response

from pyiceberg.exceptions import ServerError, ValidationException
from pyiceberg.io import (
    ARROW_FILE_IO,
    CATALOG_URI,
    CREDENTIALS_ENDPOINT,
    PY_IO_IMPL,
    REFRESH_CREDENTIALS_ENABLED,
    S3_ACCESS_KEY_ID,
    S3_SECRET_ACCESS_KEY,
    S3_SESSION_TOKEN,
    S3_SESSION_TOKEN_EXPIRES_AT_MS,
    _credential_from_properties,
    _credential_refresh_endpoint,
    _get_or_refresh_credentials,
    _import_file_io,
    _infer_file_io_from_scheme,
    load_file_io,
)
from pyiceberg.io.pyarrow import PyArrowFileIO
from pyiceberg.typedef import Properties


def test_custom_local_input_file() -> None:
    """Test initializing an InputFile implementation to read a local file"""
    with tempfile.TemporaryDirectory() as tmpdirname:
        file_location = os.path.join(tmpdirname, "foo.txt")
        with open(file_location, "wb") as write_file:
            write_file.write(b"foo")

        # Confirm that the file initially exists
        assert os.path.exists(file_location)

        # Instantiate the input file
        absolute_file_location = os.path.abspath(file_location)
        input_file = PyArrowFileIO().new_input(location=f"{absolute_file_location}")

        # Test opening and reading the file
        f = input_file.open()
        data = f.read()
        assert data == b"foo"
        assert len(input_file) == 3


def test_custom_local_output_file() -> None:
    """Test initializing an OutputFile implementation to write to a local file"""
    with tempfile.TemporaryDirectory() as tmpdirname:
        file_location = os.path.join(tmpdirname, "foo.txt")

        # Instantiate the output file
        absolute_file_location = os.path.abspath(file_location)
        output_file = PyArrowFileIO().new_output(location=f"{absolute_file_location}")

        # Create the output file and write to it
        f = output_file.create()
        f.write(b"foo")

        # Confirm that bytes were written
        with open(file_location, "rb") as f:
            assert f.read() == b"foo"

        assert len(output_file) == 3


def test_pickled_pyarrow_round_trip() -> None:
    with tempfile.TemporaryDirectory() as tmpdirname:
        file_location = os.path.join(tmpdirname, "foo.txt")
        file_io = PyArrowFileIO()
        serialized_file_io = pickle.dumps(file_io)
        deserialized_file_io = pickle.loads(serialized_file_io)
        absolute_file_location = os.path.abspath(file_location)
        output_file = deserialized_file_io.new_output(location=f"{absolute_file_location}")
        with output_file.create() as f:
            f.write(b"foo")

        input_file = deserialized_file_io.new_input(location=f"{absolute_file_location}")
        f = input_file.open()
        data = f.read()
        assert data == b"foo"
        assert len(input_file) == 3
        deserialized_file_io.delete(location=f"{absolute_file_location}")


def test_custom_local_output_file_with_overwrite() -> None:
    """Test initializing an OutputFile implementation to overwrite a local file"""
    with tempfile.TemporaryDirectory() as tmpdirname:
        output_file_location = os.path.join(tmpdirname, "foo.txt")

        # Create a file in the temporary directory
        with open(output_file_location, "wb") as write_file:
            write_file.write(b"foo")

        # Instantiate an output file
        output_file = PyArrowFileIO().new_output(location=f"{output_file_location}")

        # Confirm that a FileExistsError is raised when overwrite=False
        with pytest.raises(FileExistsError):
            f = output_file.create(overwrite=False)
            f.write(b"foo")

        # Confirm that the file is overwritten with overwrite=True
        f = output_file.create(overwrite=True)
        f.write(b"bar")
        with open(output_file_location, "rb") as f:
            assert f.read() == b"bar"


def test_custom_file_exists() -> None:
    """Test that the exists property returns the proper value for existing and non-existing files"""
    with tempfile.TemporaryDirectory() as tmpdirname:
        file_location = os.path.join(tmpdirname, "foo.txt")
        with open(file_location, "wb") as f:
            f.write(b"foo")

        nonexistent_file_location = os.path.join(tmpdirname, "bar.txt")

        # Confirm that the file initially exists
        assert os.path.exists(file_location)

        # Get an absolute path for an existing file and a nonexistent file
        absolute_file_location = os.path.abspath(file_location)
        non_existent_absolute_file_location = os.path.abspath(nonexistent_file_location)

        # Create InputFile instances
        input_file = PyArrowFileIO().new_input(location=f"{absolute_file_location}")
        non_existent_input_file = PyArrowFileIO().new_input(location=f"{non_existent_absolute_file_location}")

        # Test opening and reading the file
        assert input_file.exists()
        assert not non_existent_input_file.exists()

        # Create OutputFile instances
        file = PyArrowFileIO().new_output(location=f"{absolute_file_location}")
        non_existent_file = PyArrowFileIO().new_output(location=f"{non_existent_absolute_file_location}")

        # Test opening and reading the file
        assert file.exists()
        assert not non_existent_file.exists()


def test_output_file_to_input_file() -> None:
    """Test initializing an InputFile using the `to_input_file()` method on an OutputFile instance"""
    with tempfile.TemporaryDirectory() as tmpdirname:
        output_file_location = os.path.join(tmpdirname, "foo.txt")

        # Create an output file instance
        output_file = PyArrowFileIO().new_output(location=f"{output_file_location}")

        # Create the output file and write to it
        with output_file.create() as output_stream:
            output_stream.write(b"foo")

        # Convert to an input file and confirm the contents
        input_file = output_file.to_input_file()
        with input_file.open() as f:
            assert f.read() == b"foo"


@pytest.mark.parametrize(
    "string_uri",
    [
        "foo/bar/baz.parquet",
        "file:/foo/bar/baz.parquet",
        "file:/foo/bar/baz.parquet",
    ],
)
def test_custom_file_io_locations(string_uri: str) -> None:
    """Test that the location property is maintained as the value of the location argument"""
    # Instantiate the file-io and create a new input and output file
    file_io = PyArrowFileIO()
    input_file = file_io.new_input(location=string_uri)
    assert input_file.location == string_uri

    output_file = file_io.new_output(location=string_uri)
    assert output_file.location == string_uri


def test_deleting_local_file_using_file_io() -> None:
    """Test deleting a local file using FileIO.delete(...)"""
    with tempfile.TemporaryDirectory() as tmpdirname:
        # Write to the temporary file
        output_file_location = os.path.join(tmpdirname, "foo.txt")
        with open(output_file_location, "wb") as f:
            f.write(b"foo")

        # Instantiate the file-io
        file_io = PyArrowFileIO()

        # Confirm that the file initially exists
        assert os.path.exists(output_file_location)

        # Delete the file using the file-io implementations delete method
        file_io.delete(output_file_location)

        # Confirm that the file no longer exists
        assert not os.path.exists(output_file_location)


def test_raise_file_not_found_error_for_fileio_delete() -> None:
    """Test raising a FileNotFound error when trying to delete a non-existent file"""
    with tempfile.TemporaryDirectory() as tmpdirname:
        # Write to the temporary file
        output_file_location = os.path.join(tmpdirname, "foo.txt")

        # Instantiate the file-io
        file_io = PyArrowFileIO()

        # Delete the non-existent file using the file-io implementations delete method
        with pytest.raises(FileNotFoundError) as exc_info:
            file_io.delete(output_file_location)

        assert "Cannot delete file" in str(exc_info.value)

        # Confirm that the file no longer exists
        assert not os.path.exists(output_file_location)


def test_deleting_local_file_using_file_io_input_file() -> None:
    """Test deleting a local file by passing an InputFile instance to FileIO.delete(...)"""
    with tempfile.TemporaryDirectory() as tmpdirname:
        # Write to the temporary file
        file_location = os.path.join(tmpdirname, "foo.txt")
        with open(file_location, "wb") as f:
            f.write(b"foo")

        # Instantiate the file-io
        file_io = PyArrowFileIO()

        # Confirm that the file initially exists
        assert os.path.exists(file_location)

        # Instantiate the custom InputFile
        input_file = PyArrowFileIO().new_input(location=f"{file_location}")

        # Delete the file using the file-io implementations delete method
        file_io.delete(input_file)

        # Confirm that the file no longer exists
        assert not os.path.exists(file_location)


def test_deleting_local_file_using_file_io_output_file() -> None:
    """Test deleting a local file by passing an OutputFile instance to FileIO.delete(...)"""
    with tempfile.TemporaryDirectory() as tmpdirname:
        # Write to the temporary file
        file_location = os.path.join(tmpdirname, "foo.txt")
        with open(file_location, "wb") as f:
            f.write(b"foo")

        # Instantiate the file-io
        file_io = PyArrowFileIO()

        # Confirm that the file initially exists
        assert os.path.exists(file_location)

        # Instantiate the custom OutputFile
        output_file = PyArrowFileIO().new_output(location=f"{file_location}")

        # Delete the file using the file-io implementations delete method
        file_io.delete(output_file)

        # Confirm that the file no longer exists
        assert not os.path.exists(file_location)


def test_import_file_io() -> None:
    assert isinstance(_import_file_io(ARROW_FILE_IO, {}), PyArrowFileIO)


def test_import_file_io_does_not_exist(caplog: Any) -> None:
    import logging

    caplog.set_level(logging.DEBUG)
    assert _import_file_io("pyiceberg.does.not.exist.FileIO", {}) is None
    assert "Could not initialize FileIO: pyiceberg.does.not.exist.FileIO" in caplog.text
    assert "ModuleNotFoundError: No module named 'pyiceberg.does'" in caplog.text


def test_load_file() -> None:
    assert isinstance(load_file_io({PY_IO_IMPL: ARROW_FILE_IO}), PyArrowFileIO)


def test_load_file_io_no_arguments() -> None:
    assert isinstance(load_file_io({}), PyArrowFileIO)


def test_load_file_io_does_not_exist() -> None:
    with pytest.raises(ValueError) as exc_info:
        load_file_io({PY_IO_IMPL: "pyiceberg.does.not.exist.FileIO"})

    assert "Could not initialize FileIO: pyiceberg.does.not.exist.FileIO" in str(exc_info.value)


def test_load_file_io_warehouse() -> None:
    assert isinstance(load_file_io({"warehouse": "s3://some-path/"}), PyArrowFileIO)


def test_load_file_io_location() -> None:
    assert isinstance(load_file_io({"location": "s3://some-path/"}), PyArrowFileIO)


def test_load_file_io_location_no_schema() -> None:
    assert isinstance(load_file_io({"location": "/no-schema/"}), PyArrowFileIO)


@pytest.mark.filterwarnings("ignore")
def test_mock_warehouse_location_file_io() -> None:
    # For testing the selection logic
    io = load_file_io({"warehouse": "test://some-path/"})
    assert io.properties["warehouse"] == "test://some-path/"


@pytest.mark.filterwarnings("ignore")
def test_mock_table_location_file_io() -> None:
    # For testing the selection logic
    io = load_file_io({}, "test://some-path/")
    assert io.properties == {}


def test_gibberish_table_location_file_io() -> None:
    # For testing the selection logic
    assert isinstance(load_file_io({}, "gibberish"), PyArrowFileIO)


def test_infer_file_io_from_schema_unknown() -> None:
    # When we have an unknown scheme, we would like to know
    with pytest.warns(UserWarning) as w:
        _infer_file_io_from_scheme("unknown://bucket/path/", {})

    assert str(w[0].message) == "No preferred file implementation for scheme: unknown"


def _expiry_ms(delta_seconds: int) -> int:
    return int((datetime.now() + timedelta(seconds=delta_seconds)).timestamp() * 1000)


def _full_cred_props(expiry_ms: int) -> Properties:
    return {
        S3_ACCESS_KEY_ID: "AKID",
        S3_SECRET_ACCESS_KEY: "SECRET",
        S3_SESSION_TOKEN: "TOKEN",
        S3_SESSION_TOKEN_EXPIRES_AT_MS: str(expiry_ms),
        CATALOG_URI: "https://catalog.example.com",
        CREDENTIALS_ENDPOINT: "v1/credentials",
        REFRESH_CREDENTIALS_ENABLED: "true",
    }


def _make_session(response_body: Properties | None = None, status_code: int = 200) -> MagicMock:
    session = MagicMock()
    mock_resp = MagicMock()
    mock_resp.text = json.dumps(response_body or {})
    mock_resp.raise_for_status.return_value = None
    session.get.return_value = mock_resp
    return session


def _make_http_error(status_code: int) -> HTTPError:
    response = Response()
    response.status_code = status_code
    response._content = b'{"error": {"message": "server error", "type": "ServerError", "code": 500}}'
    exc = HTTPError(response=response)
    return exc


def test_credential_from_properties_missing_access_key() -> None:
    props = {S3_SECRET_ACCESS_KEY: "SECRET", S3_SESSION_TOKEN: "TOKEN", S3_SESSION_TOKEN_EXPIRES_AT_MS: str(_expiry_ms(200))}
    assert _credential_from_properties(props) == {}


def test_credential_from_properties_missing_secret_key() -> None:
    props = {S3_ACCESS_KEY_ID: "AKID", S3_SESSION_TOKEN: "TOKEN", S3_SESSION_TOKEN_EXPIRES_AT_MS: str(_expiry_ms(200))}
    assert _credential_from_properties(props) == {}


def test_credential_from_properties_missing_session_token() -> None:
    props = {S3_ACCESS_KEY_ID: "AKID", S3_SECRET_ACCESS_KEY: "SECRET", S3_SESSION_TOKEN_EXPIRES_AT_MS: str(_expiry_ms(200))}
    assert _credential_from_properties(props) == {}


def test_credential_from_properties_missing_expiry() -> None:
    props = {S3_ACCESS_KEY_ID: "AKID", S3_SECRET_ACCESS_KEY: "SECRET", S3_SESSION_TOKEN: "TOKEN"}
    assert _credential_from_properties(props) == {}


def test_credential_from_properties_not_expiring_soon() -> None:
    props = {
        S3_ACCESS_KEY_ID: "AKID",
        S3_SECRET_ACCESS_KEY: "SECRET",
        S3_SESSION_TOKEN: "TOKEN",
        S3_SESSION_TOKEN_EXPIRES_AT_MS: str(_expiry_ms(600)),
    }
    assert _credential_from_properties(props) == {}


def test_credential_from_properties_expiring_soon() -> None:
    expiry = _expiry_ms(200)
    props = {
        S3_ACCESS_KEY_ID: "AKID",
        S3_SECRET_ACCESS_KEY: "SECRET",
        S3_SESSION_TOKEN: "TOKEN",
        S3_SESSION_TOKEN_EXPIRES_AT_MS: str(expiry),
    }
    result = _credential_from_properties(props)
    assert result[S3_ACCESS_KEY_ID] == "AKID"
    assert result[S3_SECRET_ACCESS_KEY] == "SECRET"
    assert result[S3_SESSION_TOKEN] == "TOKEN"
    assert result[S3_SESSION_TOKEN_EXPIRES_AT_MS] == expiry


def test_credential_from_properties_already_expired() -> None:
    expiry = _expiry_ms(-60)
    props = {
        S3_ACCESS_KEY_ID: "AKID",
        S3_SECRET_ACCESS_KEY: "SECRET",
        S3_SESSION_TOKEN: "TOKEN",
        S3_SESSION_TOKEN_EXPIRES_AT_MS: str(expiry),
    }
    result = _credential_from_properties(props)
    assert result[S3_ACCESS_KEY_ID] == "AKID"


def test_credential_refresh_endpoint_missing_uri() -> None:
    with pytest.raises(ValidationException, match="Invalid catalog endpoint"):
        _credential_refresh_endpoint({CREDENTIALS_ENDPOINT: "v1/creds"})


def test_credential_refresh_endpoint_missing_path() -> None:
    with pytest.raises(ValidationException, match="Invalid credentials endpoint"):
        _credential_refresh_endpoint({CATALOG_URI: "https://catalog.example.com"})


def test_credential_refresh_endpoint_trailing_slash_handling() -> None:
    props = {CATALOG_URI: "https://catalog.example.com/", CREDENTIALS_ENDPOINT: "/v1/creds"}
    assert _credential_refresh_endpoint(props) == "https://catalog.example.com/v1/creds"


def test_credential_refresh_endpoint_no_slash() -> None:
    props = {CATALOG_URI: "https://catalog.example.com", CREDENTIALS_ENDPOINT: "v1/creds"}
    assert _credential_refresh_endpoint(props) == "https://catalog.example.com/v1/creds"


def _expected_s3_creds(expiry_ms: int) -> Properties:
    return {
        S3_ACCESS_KEY_ID: "AKID",
        S3_SECRET_ACCESS_KEY: "SECRET",
        S3_SESSION_TOKEN: "TOKEN",
        S3_SESSION_TOKEN_EXPIRES_AT_MS: str(expiry_ms),
    }


def test_get_or_refresh_credentials_disabled() -> None:
    expiry = _expiry_ms(200)
    props = _full_cred_props(expiry)
    props[REFRESH_CREDENTIALS_ENABLED] = "false"
    assert _get_or_refresh_credentials(props, MagicMock()) == _expected_s3_creds(expiry)


def test_get_or_refresh_credentials_no_session() -> None:
    expiry = _expiry_ms(200)
    props = _full_cred_props(expiry)
    assert _get_or_refresh_credentials(props, None) == _expected_s3_creds(expiry)


def test_get_or_refresh_credentials_not_expiring() -> None:
    expiry = _expiry_ms(600)
    props = _full_cred_props(expiry)
    session = MagicMock()
    assert _get_or_refresh_credentials(props, session) == _expected_s3_creds(expiry)
    session.get.assert_not_called()


def test_get_or_refresh_credentials_success() -> None:
    new_expiry = _expiry_ms(3600)
    response_body = {
        "storage-credentials": [
            {
                "prefix": "s3://",
                "config": {
                    S3_ACCESS_KEY_ID: "NEW_AKID",
                    S3_SECRET_ACCESS_KEY: "NEW_SECRET",
                    S3_SESSION_TOKEN: "NEW_TOKEN",
                    S3_SESSION_TOKEN_EXPIRES_AT_MS: str(new_expiry),
                },
            }
        ]
    }
    props = _full_cred_props(_expiry_ms(200))
    session = _make_session(response_body)

    result = _get_or_refresh_credentials(props, session)

    assert result[S3_ACCESS_KEY_ID] == "NEW_AKID"
    assert result[S3_SECRET_ACCESS_KEY] == "NEW_SECRET"
    assert result[S3_SESSION_TOKEN] == "NEW_TOKEN"
    assert result[S3_SESSION_TOKEN_EXPIRES_AT_MS] == str(new_expiry)


def test_get_or_refresh_credentials_http_error() -> None:
    props = _full_cred_props(_expiry_ms(200))
    session = MagicMock()
    session.get.return_value.raise_for_status.side_effect = _make_http_error(500)

    with pytest.raises(ServerError):
        _get_or_refresh_credentials(props, session)


def test_get_or_refresh_credentials_empty_credentials() -> None:
    props = _full_cred_props(_expiry_ms(200))
    session = _make_session({"storage-credentials": []})

    with pytest.raises(ValueError, match="Invalid S3 Credentials: empty"):
        _get_or_refresh_credentials(props, session)


def test_get_or_refresh_credentials_multiple_credentials() -> None:
    credential = {
        "prefix": "s3://",
        "config": {
            S3_ACCESS_KEY_ID: "A",
            S3_SECRET_ACCESS_KEY: "B",
            S3_SESSION_TOKEN: "C",
            S3_SESSION_TOKEN_EXPIRES_AT_MS: "123",
        },
    }
    props = _full_cred_props(_expiry_ms(200))
    session = _make_session({"storage-credentials": [credential, credential]})

    with pytest.raises(ValueError, match="only one S3 credential should exist"):
        _get_or_refresh_credentials(props, session)
