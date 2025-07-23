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

from pyiceberg.schema import ICEBERG_FIELD_NAME_PROP, NestedField, Schema
from pyiceberg.types import IntegerType, StringType
from pyiceberg.utils.schema_conversion import AvroSchemaConversion


def test_avro_field_name_sanitization():
    """Test that field names are sanitized according to Java implementation."""

    # Test cases from Java TestSchemaConversions.java
    test_cases = [
        ("9x", "_9x"),
        ("x_", "x_"),
        ("a.b", "a_x2Eb"),
        ("☃", "_x2603"),
        ("a#b", "a_x23b"),
    ]

    for original_name, expected_sanitized in test_cases:
        schema = Schema(NestedField(field_id=1, name=original_name, field_type=StringType(), required=True))

        avro_schema = AvroSchemaConversion().iceberg_to_avro(schema)

        assert avro_schema["fields"][0]["name"] == expected_sanitized

        if original_name != expected_sanitized:
            assert avro_schema["fields"][0][ICEBERG_FIELD_NAME_PROP] == original_name
        else:
            assert ICEBERG_FIELD_NAME_PROP not in avro_schema["fields"][0]


def test_complex_schema_sanitization():
    """Test sanitization with nested schemas."""
    schema = Schema(
        NestedField(field_id=1, name="valid_field", field_type=StringType(), required=True),
        NestedField(field_id=2, name="invalid.field", field_type=IntegerType(), required=True),
    )

    avro_schema = AvroSchemaConversion().iceberg_to_avro(schema)

    assert avro_schema["fields"][0]["name"] == "valid_field"
    assert ICEBERG_FIELD_NAME_PROP not in avro_schema["fields"][0]

    assert avro_schema["fields"][1]["name"] == "invalid_x2Efield"
    assert avro_schema["fields"][1][ICEBERG_FIELD_NAME_PROP] == "invalid.field"


def test_edge_cases():
    """Test edge cases for sanitization."""
    edge_cases = [
        ("123", "_123"),
        ("_", "_"),
        ("a", "a"),
        ("a1", "a1"),
        ("1a", "_1a"),
    ]

    for original_name, expected_sanitized in edge_cases:
        schema = Schema(NestedField(field_id=1, name=original_name, field_type=StringType(), required=True))

        avro_schema = AvroSchemaConversion().iceberg_to_avro(schema)
        assert avro_schema["fields"][0]["name"] == expected_sanitized
