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
from pydantic import ValidationError

from pyiceberg.table.labels import FieldLabel, Labels


def test_labels_from_json() -> None:
    cases = [
        ('{"object-labels": null, "fields": null}', Labels()),
        (
            '{"object-labels": {"owner": "team-a", "sensitivity": "high"}}',
            Labels(object_labels={"owner": "team-a", "sensitivity": "high"}),
        ),
        (
            '{"fields": [{"field-id": 3, "labels": {"pii": "true"}}]}',
            Labels(fields=[FieldLabel(field_id=3, labels={"pii": "true"})]),
        ),
        (
            """
            {
              "object-labels": {"owner": "team-a"},
              "fields": [
                {"field-id": 1, "labels": {"classification": "pii"}},
                {"field-id": 2, "labels": {}}
              ]
            }
            """,
            Labels(
                object_labels={"owner": "team-a"},
                fields=[FieldLabel(field_id=1, labels={"classification": "pii"}), FieldLabel(field_id=2, labels={})],
            ),
        ),
    ]
    for json, expected in cases:
        assert Labels.model_validate_json(json) == expected


def test_labels_is_empty() -> None:
    assert Labels().is_empty()
    assert not Labels(object_labels={"owner": "team-a"}).is_empty()
    assert not Labels(fields=[FieldLabel(field_id=1, labels={})]).is_empty()


def test_field_label_requires_field_id_and_labels() -> None:
    cases = [
        ('{"labels": {}}', "1 validation error for FieldLabel\nfield-id\n  Field required"),
        ('{"field-id": 1}', "1 validation error for FieldLabel\nlabels\n  Field required"),
    ]
    for json, message in cases:
        with pytest.raises(ValidationError, match=message):
            FieldLabel.model_validate_json(json)
