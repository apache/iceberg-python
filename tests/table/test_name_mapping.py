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
import pytest

from pyiceberg.schema import Schema
from pyiceberg.table.name_mapping import (
    MappedField,
    NameMapping,
    apply_name_mapping,
    create_mapping_from_schema,
    parse_mapping_from_json,
    update_mapping,
)
from pyiceberg.types import BooleanType, FloatType, IntegerType, ListType, MapType, NestedField, StringType, StructType


@pytest.fixture(scope="session")
def table_name_mapping_nested() -> NameMapping:
    return NameMapping(
        [
            MappedField(field_id=1, names=["foo"]),
            MappedField(field_id=2, names=["bar"]),
            MappedField(field_id=3, names=["baz"]),
            MappedField(field_id=4, names=["qux"], fields=[MappedField(field_id=5, names=["element"])]),
            MappedField(
                field_id=6,
                names=["quux"],
                fields=[
                    MappedField(field_id=7, names=["key"]),
                    MappedField(
                        field_id=8,
                        names=["value"],
                        fields=[
                            MappedField(field_id=9, names=["key"]),
                            MappedField(field_id=10, names=["value"]),
                        ],
                    ),
                ],
            ),
            MappedField(
                field_id=11,
                names=["location"],
                fields=[
                    MappedField(
                        field_id=12,
                        names=["element"],
                        fields=[
                            MappedField(field_id=13, names=["latitude"]),
                            MappedField(field_id=14, names=["longitude"]),
                        ],
                    )
                ],
            ),
            MappedField(
                field_id=15,
                names=["person"],
                fields=[
                    MappedField(field_id=16, names=["name"]),
                    MappedField(field_id=17, names=["age"]),
                ],
            ),
        ]
    )


def test_json_mapped_field_deserialization() -> None:
    mapped_field = """{
        "field-id": 1,
        "names": ["id", "record_id"]
    }
    """
    assert MappedField(field_id=1, names=["id", "record_id"]) == MappedField.model_validate_json(mapped_field)

    mapped_field_with_null_fields = """{
        "field-id": 1,
        "names": ["id", "record_id"],
        "fields": null
    }
    """
    assert MappedField(field_id=1, names=["id", "record_id"]) == MappedField.model_validate_json(mapped_field_with_null_fields)


def test_json_mapped_field_no_names_deserialization() -> None:
    mapped_field = """{
        "field-id": 1,
        "names": []
    }
    """
    assert MappedField(field_id=1, names=[]) == MappedField.model_validate_json(mapped_field)

    mapped_field_with_null_fields = """{
        "field-id": 1,
        "names": [],
        "fields": null
    }
    """
    assert MappedField(field_id=1, names=[]) == MappedField.model_validate_json(mapped_field_with_null_fields)


def test_json_mapped_field_no_field_id_deserialization() -> None:
    mapped_field = """{
        "names": []
    }
    """
    assert MappedField(field_id=None, names=[]) == MappedField.model_validate_json(mapped_field)

    mapped_field_with_null_fields = """{
        "names": [],
        "fields": null
    }
    """
    assert MappedField(names=[]) == MappedField.model_validate_json(mapped_field_with_null_fields)


def test_json_name_mapping_deserialization() -> None:
    name_mapping = """
[
    {
        "field-id": 1,
        "names": [
            "id",
            "record_id"
        ]
    },
    {
        "field-id": 2,
        "names": [
            "data"
        ]
    },
    {
        "field-id": 3,
        "names": [
            "location"
        ],
        "fields": [
            {
                "field-id": 4,
                "names": [
                    "latitude",
                    "lat"
                ]
            },
            {
                "field-id": 5,
                "names": [
                    "longitude",
                    "long"
                ]
            }
        ]
    }
]
    """

    assert parse_mapping_from_json(name_mapping) == NameMapping(
        [
            MappedField(field_id=1, names=["id", "record_id"]),
            MappedField(field_id=2, names=["data"]),
            MappedField(
                names=["location"],
                field_id=3,
                fields=[
                    MappedField(field_id=4, names=["latitude", "lat"]),
                    MappedField(field_id=5, names=["longitude", "long"]),
                ],
            ),
        ]
    )


def test_json_mapped_field_no_field_id_serialization() -> None:
    table_name_mapping_nested_no_field_id = NameMapping(
        [
            MappedField(field_id=1, names=["foo"]),
            MappedField(field_id=None, names=["bar"]),
            MappedField(field_id=2, names=["qux"], fields=[MappedField(field_id=None, names=["element"])]),
        ]
    )

    assert (
        table_name_mapping_nested_no_field_id.model_dump_json()
        == """[{"names":["foo"],"field-id":1},{"names":["bar"]},{"names":["qux"],"field-id":2,"fields":[{"names":["element"]}]}]"""
    )


def test_json_serialization(table_name_mapping_nested: NameMapping) -> None:
    assert (
        table_name_mapping_nested.model_dump_json()
        == """[{"names":["foo"],"field-id":1},{"names":["bar"],"field-id":2},{"names":["baz"],"field-id":3},{"names":["qux"],"field-id":4,"fields":[{"names":["element"],"field-id":5}]},{"names":["quux"],"field-id":6,"fields":[{"names":["key"],"field-id":7},{"names":["value"],"field-id":8,"fields":[{"names":["key"],"field-id":9},{"names":["value"],"field-id":10}]}]},{"names":["location"],"field-id":11,"fields":[{"names":["element"],"field-id":12,"fields":[{"names":["latitude"],"field-id":13},{"names":["longitude"],"field-id":14}]}]},{"names":["person"],"field-id":15,"fields":[{"names":["name"],"field-id":16},{"names":["age"],"field-id":17}]}]"""
    )


def test_name_mapping_to_string() -> None:
    nm = NameMapping(
        [
            MappedField(field_id=1, names=["id", "record_id"]),
            MappedField(field_id=2, names=["data"]),
            MappedField(
                names=["location"],
                field_id=3,
                fields=[
                    MappedField(field_id=4, names=["lat", "latitude"]),
                    MappedField(field_id=5, names=["long", "longitude"]),
                ],
            ),
        ]
    )

    assert (
        str(nm)
        == """[
  ([id, record_id] -> 1)
  ([data] -> 2)
  ([location] -> 3 ([lat, latitude] -> 4), ([long, longitude] -> 5))
]"""
    )


def test_mapping_from_schema(table_schema_nested: Schema, table_name_mapping_nested: NameMapping) -> None:
    nm = create_mapping_from_schema(table_schema_nested)
    assert nm == table_name_mapping_nested


def test_mapping_by_name(table_name_mapping_nested: NameMapping) -> None:
    assert table_name_mapping_nested._field_by_name == {
        "person.age": MappedField(field_id=17, names=["age"]),
        "person.name": MappedField(field_id=16, names=["name"]),
        "person": MappedField(
            field_id=15,
            names=["person"],
            fields=[MappedField(field_id=16, names=["name"]), MappedField(field_id=17, names=["age"])],
        ),
        "location.element.longitude": MappedField(field_id=14, names=["longitude"]),
        "location.element.latitude": MappedField(field_id=13, names=["latitude"]),
        "location.element": MappedField(
            field_id=12,
            names=["element"],
            fields=[MappedField(field_id=13, names=["latitude"]), MappedField(field_id=14, names=["longitude"])],
        ),
        "location": MappedField(
            field_id=11,
            names=["location"],
            fields=[
                MappedField(
                    field_id=12,
                    names=["element"],
                    fields=[MappedField(field_id=13, names=["latitude"]), MappedField(field_id=14, names=["longitude"])],
                )
            ],
        ),
        "quux.value.value": MappedField(field_id=10, names=["value"]),
        "quux.value.key": MappedField(field_id=9, names=["key"]),
        "quux.value": MappedField(
            field_id=8,
            names=["value"],
            fields=[MappedField(field_id=9, names=["key"]), MappedField(field_id=10, names=["value"])],
        ),
        "quux.key": MappedField(field_id=7, names=["key"]),
        "quux": MappedField(
            field_id=6,
            names=["quux"],
            fields=[
                MappedField(field_id=7, names=["key"]),
                MappedField(
                    field_id=8,
                    names=["value"],
                    fields=[MappedField(field_id=9, names=["key"]), MappedField(field_id=10, names=["value"])],
                ),
            ],
        ),
        "qux.element": MappedField(field_id=5, names=["element"]),
        "qux": MappedField(field_id=4, names=["qux"], fields=[MappedField(field_id=5, names=["element"])]),
        "baz": MappedField(field_id=3, names=["baz"]),
        "bar": MappedField(field_id=2, names=["bar"]),
        "foo": MappedField(field_id=1, names=["foo"]),
    }


def test_update_mapping_no_updates_or_adds(table_name_mapping_nested: NameMapping) -> None:
    assert update_mapping(table_name_mapping_nested, {}, {}) == table_name_mapping_nested


def test_update_mapping(table_name_mapping_nested: NameMapping) -> None:
    updates = {1: NestedField(1, "foo_update", StringType(), True)}
    adds = {
        -1: [NestedField(18, "add_18", StringType(), True)],
        15: [NestedField(19, "name", StringType(), True), NestedField(20, "add_20", StringType(), True)],
    }

    expected = NameMapping(
        [
            MappedField(field_id=1, names=["foo", "foo_update"]),
            MappedField(field_id=2, names=["bar"]),
            MappedField(field_id=3, names=["baz"]),
            MappedField(field_id=4, names=["qux"], fields=[MappedField(field_id=5, names=["element"])]),
            MappedField(
                field_id=6,
                names=["quux"],
                fields=[
                    MappedField(field_id=7, names=["key"]),
                    MappedField(
                        field_id=8,
                        names=["value"],
                        fields=[
                            MappedField(field_id=9, names=["key"]),
                            MappedField(field_id=10, names=["value"]),
                        ],
                    ),
                ],
            ),
            MappedField(
                field_id=11,
                names=["location"],
                fields=[
                    MappedField(
                        field_id=12,
                        names=["element"],
                        fields=[
                            MappedField(field_id=13, names=["latitude"]),
                            MappedField(field_id=14, names=["longitude"]),
                        ],
                    )
                ],
            ),
            MappedField(
                field_id=15,
                names=["person"],
                fields=[
                    MappedField(field_id=17, names=["age"]),
                    MappedField(field_id=19, names=["name"]),
                    MappedField(field_id=20, names=["add_20"]),
                ],
            ),
            MappedField(field_id=18, names=["add_18"]),
        ]
    )
    assert update_mapping(table_name_mapping_nested, updates, adds) == expected


def test_mapping_using_by_visitor(table_schema_nested: Schema, table_name_mapping_nested: NameMapping) -> None:
    schema_without_ids = Schema(
        NestedField(field_id=0, name="foo", field_type=StringType(), required=False),
        NestedField(field_id=0, name="bar", field_type=IntegerType(), required=True),
        NestedField(field_id=0, name="baz", field_type=BooleanType(), required=False),
        NestedField(
            field_id=0,
            name="qux",
            field_type=ListType(element_id=0, element_type=StringType(), element_required=True),
            required=True,
        ),
        NestedField(
            field_id=0,
            name="quux",
            field_type=MapType(
                key_id=0,
                key_type=StringType(),
                value_id=0,
                value_type=MapType(key_id=0, key_type=StringType(), value_id=0, value_type=IntegerType(), value_required=True),
                value_required=True,
            ),
            required=True,
        ),
        NestedField(
            field_id=0,
            name="location",
            field_type=ListType(
                element_id=0,
                element_type=StructType(
                    NestedField(field_id=0, name="latitude", field_type=FloatType(), required=False),
                    NestedField(field_id=0, name="longitude", field_type=FloatType(), required=False),
                ),
                element_required=True,
            ),
            required=True,
        ),
        NestedField(
            field_id=0,
            name="person",
            field_type=StructType(
                NestedField(field_id=0, name="name", field_type=StringType(), required=False),
                NestedField(field_id=0, name="age", field_type=IntegerType(), required=True),
            ),
            required=False,
        ),
    )
    assert apply_name_mapping(schema_without_ids, table_name_mapping_nested).fields == table_schema_nested.fields
