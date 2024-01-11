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
from pyiceberg.table.name_mapping import MappedField, NameMapping, create_mapping_from_schema, parse_mapping_from_json


@pytest.fixture(scope="session")
def table_name_mapping_nested() -> NameMapping:
    return NameMapping([
        MappedField(field_id=1, names=['foo']),
        MappedField(field_id=2, names=['bar']),
        MappedField(field_id=3, names=['baz']),
        MappedField(field_id=4, names=['qux'], fields=[MappedField(field_id=5, names=['element'])]),
        MappedField(
            field_id=6,
            names=['quux'],
            fields=[
                MappedField(field_id=7, names=['key']),
                MappedField(
                    field_id=8,
                    names=['value'],
                    fields=[
                        MappedField(field_id=9, names=['key']),
                        MappedField(field_id=10, names=['value']),
                    ],
                ),
            ],
        ),
        MappedField(
            field_id=11,
            names=['location'],
            fields=[
                MappedField(
                    field_id=12,
                    names=['element'],
                    fields=[
                        MappedField(field_id=13, names=['latitude']),
                        MappedField(field_id=14, names=['longitude']),
                    ],
                )
            ],
        ),
        MappedField(
            field_id=15,
            names=['person'],
            fields=[
                MappedField(field_id=16, names=['name']),
                MappedField(field_id=17, names=['age']),
            ],
        ),
    ])


def test_json_mapped_field_deserialization() -> None:
    mapped_field = """{
        "field-id": 1,
        "names": ["id", "record_id"]
    }
    """
    assert MappedField(field_id=1, names=['id', 'record_id']) == MappedField.model_validate_json(mapped_field)

    mapped_field_with_null_fields = """{
        "field-id": 1,
        "names": ["id", "record_id"],
        "fields": null
    }
    """
    assert MappedField(field_id=1, names=['id', 'record_id']) == MappedField.model_validate_json(mapped_field_with_null_fields)


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

    assert parse_mapping_from_json(name_mapping) == NameMapping([
        MappedField(field_id=1, names=['id', 'record_id']),
        MappedField(field_id=2, names=['data']),
        MappedField(
            names=['location'],
            field_id=3,
            fields=[
                MappedField(field_id=4, names=['latitude', 'lat']),
                MappedField(field_id=5, names=['longitude', 'long']),
            ],
        ),
    ])


def test_json_serialization(table_name_mapping_nested: NameMapping) -> None:
    assert (
        table_name_mapping_nested.model_dump_json()
        == """[{"field-id":1,"names":["foo"]},{"field-id":2,"names":["bar"]},{"field-id":3,"names":["baz"]},{"field-id":4,"names":["qux"],"fields":[{"field-id":5,"names":["element"]}]},{"field-id":6,"names":["quux"],"fields":[{"field-id":7,"names":["key"]},{"field-id":8,"names":["value"],"fields":[{"field-id":9,"names":["key"]},{"field-id":10,"names":["value"]}]}]},{"field-id":11,"names":["location"],"fields":[{"field-id":12,"names":["element"],"fields":[{"field-id":13,"names":["latitude"]},{"field-id":14,"names":["longitude"]}]}]},{"field-id":15,"names":["person"],"fields":[{"field-id":16,"names":["name"]},{"field-id":17,"names":["age"]}]}]"""
    )


def test_name_mapping_to_string() -> None:
    nm = NameMapping([
        MappedField(field_id=1, names=['id', 'record_id']),
        MappedField(field_id=2, names=['data']),
        MappedField(
            names=['location'],
            field_id=3,
            fields=[
                MappedField(field_id=4, names=['lat', 'latitude']),
                MappedField(field_id=5, names=['long', 'longitude']),
            ],
        ),
    ])

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
        'person.age': MappedField(field_id=17, names=['age']),
        'person.name': MappedField(field_id=16, names=['name']),
        'person': MappedField(
            field_id=15,
            names=['person'],
            fields=[MappedField(field_id=16, names=['name']), MappedField(field_id=17, names=['age'])],
        ),
        'location.element.longitude': MappedField(field_id=14, names=['longitude']),
        'location.element.latitude': MappedField(field_id=13, names=['latitude']),
        'location.element': MappedField(
            field_id=12,
            names=['element'],
            fields=[MappedField(field_id=13, names=['latitude']), MappedField(field_id=14, names=['longitude'])],
        ),
        'location': MappedField(
            field_id=11,
            names=['location'],
            fields=[
                MappedField(
                    field_id=12,
                    names=['element'],
                    fields=[MappedField(field_id=13, names=['latitude']), MappedField(field_id=14, names=['longitude'])],
                )
            ],
        ),
        'quux.value.value': MappedField(field_id=10, names=['value']),
        'quux.value.key': MappedField(field_id=9, names=['key']),
        'quux.value': MappedField(
            field_id=8,
            names=['value'],
            fields=[MappedField(field_id=9, names=['key']), MappedField(field_id=10, names=['value'])],
        ),
        'quux.key': MappedField(field_id=7, names=['key']),
        'quux': MappedField(
            field_id=6,
            names=['quux'],
            fields=[
                MappedField(field_id=7, names=['key']),
                MappedField(
                    field_id=8,
                    names=['value'],
                    fields=[MappedField(field_id=9, names=['key']), MappedField(field_id=10, names=['value'])],
                ),
            ],
        ),
        'qux.element': MappedField(field_id=5, names=['element']),
        'qux': MappedField(field_id=4, names=['qux'], fields=[MappedField(field_id=5, names=['element'])]),
        'baz': MappedField(field_id=3, names=['baz']),
        'bar': MappedField(field_id=2, names=['bar']),
        'foo': MappedField(field_id=1, names=['foo']),
    }


def test_mapping_lookup_by_name(table_name_mapping_nested: NameMapping) -> None:
    assert table_name_mapping_nested.find("foo") == MappedField(field_id=1, names=['foo'])
    assert table_name_mapping_nested.find("location.element.latitude") == MappedField(field_id=13, names=['latitude'])
    assert table_name_mapping_nested.find("location", "element", "latitude") == MappedField(field_id=13, names=['latitude'])
    assert table_name_mapping_nested.find(*["location", "element", "latitude"]) == MappedField(field_id=13, names=['latitude'])

    with pytest.raises(ValueError, match="Could not find field with name: boom"):
        table_name_mapping_nested.find("boom")
