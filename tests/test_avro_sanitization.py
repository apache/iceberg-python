# type: ignore
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
from typing import Any

from fastavro import reader

import pyiceberg.avro.file as avro
from pyiceberg.io.pyarrow import PyArrowFileIO
from pyiceberg.schema import ICEBERG_FIELD_NAME_PROP, Schema
from pyiceberg.typedef import Record
from pyiceberg.types import IntegerType, NestedField, StringType
from pyiceberg.utils.schema_conversion import AvroSchemaConversion, AvroType


class AvroTestRecord(Record):
    """Test record class for Avro compatibility testing."""

    @property
    def valid_field(self) -> str:
        return self._data[0]

    @property
    def invalid_field(self) -> int:
        return self._data[1]

    @property
    def field_starting_with_digit(self) -> str:
        return self._data[2]


def test_comprehensive_field_name_sanitization() -> None:
    """Test comprehensive field name sanitization including edge cases and Java compatibility."""

    test_cases = [
        # Java compatibility test cases
        ("9x", "_9x"),
        ("x_", "x_"),
        ("a.b", "a_x2Eb"),
        ("☃", "_x2603"),
        ("a#b", "a_x23b"),
        ("123", "_123"),
        ("_", "_"),
        ("a", "a"),
        ("a1", "a1"),
        ("1a", "_1a"),
        ("a☃b", "a_x2603b"),
        ("name#with#hash", "name_x23with_x23hash"),
        ("123number", "_123number"),
        ("😎", "_x1F60E"),
        ("😎_with_text", "_x1F60E_with_text"),
    ]

    for original_name, expected_sanitized in test_cases:
        schema = Schema(NestedField(field_id=1, name=original_name, field_type=StringType(), required=True))

        avro_schema: AvroType = AvroSchemaConversion().iceberg_to_avro(schema)
        avro_dict: dict[str, Any] = avro_schema

        assert avro_dict["fields"][0]["name"] == expected_sanitized

        if original_name != expected_sanitized:
            assert avro_dict["fields"][0][ICEBERG_FIELD_NAME_PROP] == original_name
        else:
            assert ICEBERG_FIELD_NAME_PROP not in avro_dict["fields"][0]


def test_comprehensive_avro_compatibility() -> None:
    """Test comprehensive Avro compatibility including complex schemas and file structure."""

    # Create schema with various field name types
    schema = Schema(
        NestedField(field_id=1, name="valid_field", field_type=StringType(), required=True),
        NestedField(field_id=2, name="invalid.field", field_type=IntegerType(), required=True),
        NestedField(field_id=3, name="9x", field_type=StringType(), required=True),
        NestedField(field_id=4, name="name#with#hash", field_type=StringType(), required=True),
        NestedField(field_id=5, name="☃", field_type=IntegerType(), required=True),
        NestedField(field_id=6, name="😎", field_type=IntegerType(), required=True),
    )

    test_records = [
        AvroTestRecord("hello", 42, "test", "hash_value", 100, 200),
        AvroTestRecord("goodbye", 99, "example", "another_hash", 200, 300),
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
            # Test Avro file structure
            magic = fo.read(4)
            assert magic == b"Obj\x01"  # Avro magic bytes

            import struct

            metadata_length = struct.unpack(">I", fo.read(4))[0]
            assert metadata_length > 0

            fo.seek(0)
            avro_reader = reader(fo)

            avro_schema: AvroType = avro_reader.writer_schema
            avro_dict: dict[str, Any] = avro_schema
            field_names = [field["name"] for field in avro_dict["fields"]]

            # Expected sanitized names (matching Java implementation)
            expected_field_names = [
                "valid_field",
                "invalid_x2Efield",
                "_9x",
                "name_x23with_x23hash",
                "_x2603",
                "_x1F60E",
            ]

            assert field_names == expected_field_names

            # Verify iceberg-field-name properties
            for field in avro_dict["fields"]:
                field_dict: dict[str, Any] = field
                if field_dict["name"] == "invalid_x2Efield":
                    assert "iceberg-field-name" in field_dict
                    assert field_dict["iceberg-field-name"] == "invalid.field"
                elif field_dict["name"] == "_9x":
                    assert "iceberg-field-name" in field_dict
                    assert field_dict["iceberg-field-name"] == "9x"
                elif field_dict["name"] == "name_x23with_x23hash":
                    assert "iceberg-field-name" in field_dict
                    assert field_dict["iceberg-field-name"] == "name#with#hash"
                elif field_dict["name"] == "_x2603":
                    assert "iceberg-field-name" in field_dict
                    assert field_dict["iceberg-field-name"] == "☃"
                elif field_dict["name"] == "_x1F60E":
                    assert "iceberg-field-name" in field_dict
                    assert field_dict["iceberg-field-name"] == "😎"
                else:
                    assert "iceberg-field-name" not in field_dict

            records = list(avro_reader)
            assert len(records) == 2

            # Verify data integrity
            first_record = records[0]
            assert first_record["valid_field"] == "hello"
            assert first_record["invalid_x2Efield"] == 42
            assert first_record["_9x"] == "test"
            assert first_record["name_x23with_x23hash"] == "hash_value"
            assert first_record["_x2603"] == 100
            assert first_record["_x1F60E"] == 200

            second_record = records[1]
            assert second_record["valid_field"] == "goodbye"
            assert second_record["invalid_x2Efield"] == 99
            assert second_record["_9x"] == "example"
            assert second_record["name_x23with_x23hash"] == "another_hash"
            assert second_record["_x2603"] == 200
            assert second_record["_x1F60E"] == 300

            assert avro_reader.metadata.get("test") == "metadata"

    finally:
        import os

        if os.path.exists(tmp_avro_file):
            os.unlink(tmp_avro_file)


def test_emoji_field_name_sanitization() -> None:
    """Test that emoji field names are properly sanitized according to Java implementation."""

    schema = Schema(
        NestedField(field_id=1, name="😎", field_type=IntegerType(), required=True),
        NestedField(field_id=2, name="valid_field", field_type=StringType(), required=True),
        NestedField(field_id=3, name="😎_with_text", field_type=StringType(), required=True),
    )

    avro_schema: AvroType = AvroSchemaConversion().iceberg_to_avro(schema, schema_name="emoji_test")
    avro_dict: dict[str, Any] = avro_schema

    field_names = [field["name"] for field in avro_dict["fields"]]
    expected_field_names = [
        "_x1F60E",  # 😎 becomes _x1F60E (Unicode 0x1F60E)
        "valid_field",
        "_x1F60E_with_text",
    ]

    assert field_names == expected_field_names

    for field in avro_dict["fields"]:
        field_dict: dict[str, Any] = field
        if field_dict["name"] == "_x1F60E":
            assert field_dict["iceberg-field-name"] == "😎"
        elif field_dict["name"] == "_x1F60E_with_text":
            assert field_dict["iceberg-field-name"] == "😎_with_text"
        else:
            assert "iceberg-field-name" not in field_dict

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

            avro_schema_reader: AvroType = avro_reader.writer_schema
            avro_dict_reader: dict[str, Any] = avro_schema_reader
            field_names_reader = [field["name"] for field in avro_dict_reader["fields"]]

            assert field_names_reader == expected_field_names

            for field in avro_dict_reader["fields"]:
                field_dict_reader: dict[str, Any] = field
                if field_dict_reader["name"] == "_x1F60E":
                    assert field_dict_reader["iceberg-field-name"] == "😎"
                elif field_dict_reader["name"] == "_x1F60E_with_text":
                    assert field_dict_reader["iceberg-field-name"] == "😎_with_text"
                else:
                    assert "iceberg-field-name" not in field_dict_reader

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
