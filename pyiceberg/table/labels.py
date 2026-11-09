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
from typing import Any

from pydantic import Field, model_validator

from pyiceberg.typedef import IcebergBaseModel


class FieldLabel(IcebergBaseModel):
    """Labels attached to a single field, identified by its field ID."""

    field_id: int = Field(alias="field-id")
    labels: dict[str, str]


class Labels(IcebergBaseModel):
    """Optional labels that a catalog returns with a table or view.

    Labels are enrichment supplied by the catalog, such as ownership or classification.
    They are not table state and are not written to the table metadata.
    """

    object_labels: dict[str, str] = Field(alias="object-labels", default_factory=dict)
    fields: list[FieldLabel] = Field(default_factory=list)

    @model_validator(mode="before")
    @classmethod
    def treat_null_as_absent(cls, data: Any) -> Any:
        # A null value parses the same as a missing key.
        if data is None:
            return {}
        if isinstance(data, dict):
            return {key: value for key, value in data.items() if value is not None}
        return data

    def is_empty(self) -> bool:
        """Return True when there are neither object-level nor field-level labels."""
        return not self.object_labels and not self.fields
