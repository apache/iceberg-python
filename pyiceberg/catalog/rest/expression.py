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

from typing import Any, Dict, List, Union

from pydantic import Field

from pyiceberg.expressions import Reference
from pyiceberg.transforms import Transform
from pyiceberg.typedef import IcebergBaseModel


class ExpressionType(IcebergBaseModel):
    __root__: str = Field(
        ...,
        example=[
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
        ],
    )


class TrueExpression(IcebergBaseModel):
    type: ExpressionType = Field(default_factory=lambda: ExpressionType.parse_obj("true"), const=True)


class FalseExpression(IcebergBaseModel):
    type: ExpressionType = Field(default_factory=lambda: ExpressionType.parse_obj("false"), const=True)


class TransformTerm(IcebergBaseModel):
    type: str = Field("transform", const=True)
    transform: Transform
    term: Reference


class Term(IcebergBaseModel):
    __root__: Union[Reference, TransformTerm]


class AndOrExpression(IcebergBaseModel):
    type: ExpressionType
    left: "Expression"
    right: "Expression"


class NotExpression(IcebergBaseModel):
    type: ExpressionType = Field(default_factory=lambda: ExpressionType.parse_obj("not"), const=True)
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


class Expression(IcebergBaseModel):
    __root__: Union[
        TrueExpression,
        FalseExpression,
        AndOrExpression,
        NotExpression,
        SetExpression,
        LiteralExpression,
        UnaryExpression,
    ]


Expression.model_rebuild()
