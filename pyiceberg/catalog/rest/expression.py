from typing import Any, Dict, List, Union

from pydantic import BaseModel, Field

from pyiceberg.typedef import IcebergBaseModel


class ExpressionType(BaseModel):
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


class Transform(IcebergBaseModel):
    __root__: str = Field(
        ...,
        example=[
            "identity",
            "year",
            "month",
            "day",
            "hour",
            "bucket[256]",
            "truncate[16]",
        ],
    )


class Reference(IcebergBaseModel):
    __root__: str = Field(..., example=["column-name"])


class TransformTerm(IcebergBaseModel):
    type: str = Field("transform", const=True)
    transform: Transform
    term: Reference


class Term(BaseModel):
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
