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

from typing import Any, Dict, List, Literal, Union

from pydantic import Field

from pyiceberg.transforms import Transform
from pyiceberg.typedef import IcebergBaseModel, IcebergRootModel


class Reference(IcebergRootModel[str]):
    root: str = Field(..., json_schema_extra={"example": "column-name"})


class ExpressionType(IcebergRootModel[str]):
    root: str = Field(
        ...,
        json_schema_extra={
            "example": [
                "true",
                "false",
                "eq",
                "and",
                "or",
                "not",
                "in",
                "not-in",
                "lt",
                "lt-eq",
                "gt",
                "gt-eq",
                "not-eq",
                "starts-with",
                "not-starts-with",
                "is-null",
                "not-null",
                "is-nan",
                "not-nan",
            ]
        },
    )


class TrueExpression(IcebergBaseModel):
    type: Literal["true"] = "true"


class FalseExpression(IcebergBaseModel):
    type: Literal["false"] = "false"


class TransformTerm(IcebergBaseModel):
    type: Literal["transform"] = "transform"
    transform: Transform
    term: Reference


class Term(IcebergRootModel[Union[Reference, TransformTerm]]):
    root: Union[Reference, TransformTerm]


class AndOrExpression(IcebergBaseModel):
    type: ExpressionType
    left: "Expression"
    right: "Expression"


class NotExpression(IcebergBaseModel):
    type: Literal["not"] = "not"
    child: "Expression"


class SetExpression(IcebergBaseModel):
    type: ExpressionType
    term: Term
    values: List[Dict[str, Any]]


class LiteralExpression(IcebergBaseModel):
    type: ExpressionType
    term: Term
    value: Dict[str, Any]


class UnaryExpression(IcebergBaseModel):
    type: ExpressionType
    term: Term
    value: Dict[str, Any]


class Expression(IcebergRootModel[
    Union[
        TrueExpression,
        FalseExpression,
        AndOrExpression,
        NotExpression,
        SetExpression,
        LiteralExpression,
        UnaryExpression,
    ]
]):
    root: Union[
        TrueExpression,
        FalseExpression,
        AndOrExpression,
        NotExpression,
        SetExpression,
        LiteralExpression,
        UnaryExpression,
    ]


Expression.model_rebuild()
