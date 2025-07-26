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

import tempfile

import pytest
from fastavro import reader

import pyiceberg.avro.file as avro
from pyiceberg.io.pyarrow import PyArrowFileIO
from pyiceberg.schema import Schema
from pyiceberg.typedef import Record
from pyiceberg.types import IntegerType, NestedField, StringType
from pyiceberg.utils.schema_conversion import AvroSchemaConversion


class AvroTestRecord(Record):
    """Test record class for Avro compatibility testing."""

    @property
    def valid_field(self) -> str:
        return self._data[0]

    @property
    def invalid_field(self) -> int:
        return self._data[1]

    @property
    def field_with_dot(self) -> str:
        return self._data[2]

    @property
    def field_with_hash(self) -> int:
        return self._data[3]

    @property
    def field_starting_with_digit(self) -> str:
        return self._data[4]


@pytest.mark.integration
def test_avro_compatibility() -> None:
    """Test that Avro files with sanitized names can be read by other tools."""

    schema = Schema(
        NestedField(field_id=1, name="valid_field", field_type=StringType(), required=True),
        NestedField(field_id=2, name="invalid.field", field_type=IntegerType(), required=True),
        NestedField(field_id=3, name="field_with_dot", field_type=StringType(), required=True),
        NestedField(field_id=4, name="field_with_hash", field_type=IntegerType(), required=True),
        NestedField(field_id=5, name="9x", field_type=StringType(), required=True),
    )

    test_records = [
        AvroTestRecord("hello", 42, "world", 123, "test"),
        AvroTestRecord("goodbye", 99, "universe", 456, "example"),
    ]

    with tempfile.NamedTemporaryFile(suffix=".avro", delete=False) as tmp_file:
        tmp_avro_file = tmp_file.name

    try:
        with avro.AvroOutputFile[AvroTestRecord](
            output_file=PyArrowFileIO().new_output(tmp_avro_file),
            file_schema=schema,
            schema_name="test_schema",
            metadata={"test": "metadata"},
        ) as output_file:
            output_file.write_block(test_records)

        with open(tmp_avro_file, "rb") as fo:
            avro_reader = reader(fo)

            avro_schema = avro_reader.writer_schema
            field_names = [field["name"] for field in avro_schema["fields"]]

            # Expected sanitized names (matching Java implementation)
            expected_field_names = [
                "valid_field",
                "invalid_x2Efield",
                "field_with_dot",
                "field_with_hash",
                "_9x",
            ]

            assert field_names == expected_field_names

            for field in avro_schema["fields"]:
                if field["name"] == "invalid_x2Efield":
                    assert "iceberg-field-name" in field
                    assert field["iceberg-field-name"] == "invalid.field"
                elif field["name"] == "_9x":
                    assert "iceberg-field-name" in field
                    assert field["iceberg-field-name"] == "9x"
                else:
                    assert "iceberg-field-name" not in field

            records = list(avro_reader)

            assert len(records) == 2

            first_record = records[0]
            assert first_record["valid_field"] == "hello"
            assert first_record["invalid_x2Efield"] == 42
            assert first_record["field_with_dot"] == "world"
            assert first_record["field_with_hash"] == 123
            assert first_record["_9x"] == "test"

            second_record = records[1]
            assert second_record["valid_field"] == "goodbye"
            assert second_record["invalid_x2Efield"] == 99
            assert second_record["field_with_dot"] == "universe"
            assert second_record["field_with_hash"] == 456
            assert second_record["_9x"] == "example"

            assert avro_reader.metadata.get("test") == "metadata"

    finally:
        import os

        if os.path.exists(tmp_avro_file):
            os.unlink(tmp_avro_file)


@pytest.mark.integration
def test_avro_schema_conversion_sanitization() -> None:
    """Test that schema conversion properly sanitizes field names."""

    # Create schema with various invalid field names
    schema = Schema(
        NestedField(field_id=1, name="valid_name", field_type=StringType(), required=True),
        NestedField(field_id=2, name="invalid.name", field_type=IntegerType(), required=True),
        NestedField(field_id=3, name="name#with#hash", field_type=StringType(), required=True),
        NestedField(field_id=4, name="☃", field_type=IntegerType(), required=True),  # Unicode character
        NestedField(field_id=5, name="123number", field_type=StringType(), required=True),
    )

    avro_schema = AvroSchemaConversion().iceberg_to_avro(schema, schema_name="test_schema")

    field_names = [field["name"] for field in avro_schema["fields"]]
    expected_field_names = [
        "valid_name",  # Valid name, unchanged
        "invalid_x2Ename",  # Dot becomes _x2E
        "name_x23with_x23hash",  # Hash becomes _x23
        "_x2603",  # Unicode snowman becomes _x2603
        "_123number",  # Starts with digit, gets leading underscore
    ]

    assert field_names == expected_field_names

    for field in avro_schema["fields"]:
        if field["name"] == "invalid_x2Ename":
            assert field["iceberg-field-name"] == "invalid.name"
        elif field["name"] == "name_x23with_x23hash":
            assert field["iceberg-field-name"] == "name#with#hash"
        elif field["name"] == "_x2603":
            assert field["iceberg-field-name"] == "☃"
        elif field["name"] == "_123number":
            assert field["iceberg-field-name"] == "123number"
        else:
            assert "iceberg-field-name" not in field


@pytest.mark.integration
def test_avro_file_structure_verification() -> None:
    """Test that the Avro file structure is correct and can be parsed."""

    schema = Schema(
        NestedField(field_id=1, name="test.field", field_type=StringType(), required=True),
    )

    test_records = [AvroTestRecord("hello")]

    with tempfile.NamedTemporaryFile(suffix=".avro", delete=False) as tmp_file:
        tmp_avro_file = tmp_file.name

    try:
        with avro.AvroOutputFile[AvroTestRecord](
            output_file=PyArrowFileIO().new_output(tmp_avro_file),
            file_schema=schema,
            schema_name="simple_test",
        ) as output_file:
            output_file.write_block(test_records)

        with open(tmp_avro_file, "rb") as fo:
            # Read magic bytes (first 4 bytes should be Avro magic)
            magic = fo.read(4)
            assert magic == b"Obj\x01"  # Avro magic bytes

            import struct

            metadata_length = struct.unpack(">I", fo.read(4))[0]
            assert metadata_length > 0

            from fastavro import reader

            fo.seek(0)
            avro_reader = reader(fo)

            avro_schema = avro_reader.writer_schema

            assert len(avro_schema["fields"]) == 1
            field = avro_schema["fields"][0]
            assert field["name"] == "test_x2Efield"
            assert field["iceberg-field-name"] == "test.field"

            records = list(avro_reader)
            assert len(records) == 1
            assert records[0]["test_x2Efield"] == "hello"

    finally:
        import os

        if os.path.exists(tmp_avro_file):
            os.unlink(tmp_avro_file)


@pytest.mark.integration
def test_edge_cases_sanitization() -> None:
    """Test edge cases for field name sanitization."""

    test_cases = [
        ("123", "_123"),  # All digits
        ("_", "_"),  # Just underscore
        ("a", "a"),  # Single letter
        ("a1", "a1"),  # Letter followed by digit
        ("1a", "_1a"),  # Digit followed by letter
        ("a.b", "a_x2Eb"),  # Letter, dot, letter
        ("a#b", "a_x23b"),  # Letter, hash, letter
        ("☃", "_x2603"),  # Unicode character
        ("a☃b", "a_x2603b"),  # Letter, unicode, letter
    ]

    for original_name, expected_sanitized in test_cases:
        schema = Schema(
            NestedField(field_id=1, name=original_name, field_type=StringType(), required=True),
        )

        avro_schema = AvroSchemaConversion().iceberg_to_avro(schema, schema_name="edge_test")

        field = avro_schema["fields"][0]
        assert field["name"] == expected_sanitized

        if original_name != expected_sanitized:
            assert field["iceberg-field-name"] == original_name
        else:
            assert "iceberg-field-name" not in field


@pytest.mark.integration
def test_emoji_field_name_sanitization() -> None:
    """Test that emoji field names are properly sanitized according to Java implementation."""

    schema = Schema(
        NestedField(field_id=1, name="😎", field_type=IntegerType(), required=True),
        NestedField(field_id=2, name="valid_field", field_type=StringType(), required=True),
        NestedField(field_id=3, name="😎_with_text", field_type=StringType(), required=True),
    )

    avro_schema = AvroSchemaConversion().iceberg_to_avro(schema, schema_name="emoji_test")

    field_names = [field["name"] for field in avro_schema["fields"]]
    expected_field_names = [
        "_x1F60E",  # 😎 becomes _x1F60E (Unicode 0x1F60E)
        "valid_field",
        "_x1F60E_with_text",
    ]

    assert field_names == expected_field_names

    for field in avro_schema["fields"]:
        if field["name"] == "_x1F60E":
            assert field["iceberg-field-name"] == "😎"
        elif field["name"] == "_x1F60E_with_text":
            assert field["iceberg-field-name"] == "😎_with_text"
        else:
            assert "iceberg-field-name" not in field

    test_records = [
        AvroTestRecord(42, "hello", "world"),
    ]

    with tempfile.NamedTemporaryFile(suffix=".avro", delete=False) as tmp_file:
        tmp_avro_file = tmp_file.name

    try:
        with avro.AvroOutputFile[AvroTestRecord](
            output_file=PyArrowFileIO().new_output(tmp_avro_file),
            file_schema=schema,
            schema_name="emoji_test",
        ) as output_file:
            output_file.write_block(test_records)

        with open(tmp_avro_file, "rb") as fo:
            avro_reader = reader(fo)

            avro_schema = avro_reader.writer_schema
            field_names = [field["name"] for field in avro_schema["fields"]]

            assert field_names == expected_field_names

            for field in avro_schema["fields"]:
                if field["name"] == "_x1F60E":
                    assert field["iceberg-field-name"] == "😎"
                elif field["name"] == "_x1F60E_with_text":
                    assert field["iceberg-field-name"] == "😎_with_text"
                else:
                    assert "iceberg-field-name" not in field

            records = list(avro_reader)
            assert len(records) == 1

            first_record = records[0]
            assert first_record["_x1F60E"] == 42
            assert first_record["valid_field"] == "hello"
            assert first_record["_x1F60E_with_text"] == "world"

    finally:
        import os

        if os.path.exists(tmp_avro_file):
            os.unlink(tmp_avro_file)
