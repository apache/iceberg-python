from functools import singledispatch
from typing import (
    List,
    Tuple,
    TypeVar,
)

from pyiceberg.expressions import (
    AlwaysFalse,
    AlwaysTrue,
    And,
    BooleanExpression,
    BoundPredicate,
    Not,
    Or,
    UnboundPredicate,
)
from pyiceberg.expressions.visitors import BindVisitor, BooleanExpressionVisitor
from pyiceberg.schema import Schema
from pyiceberg.typedef import L

T = TypeVar("T")


@singledispatch
def _visit_stack(obj: BooleanExpression, stack: List[T], visitor: BooleanExpressionVisitor[T]) -> None:
    raise NotImplementedError(f"Cannot visit unsupported expression: {obj}")


@_visit_stack.register(AlwaysTrue)
def _(_: AlwaysTrue, stack: List[T], visitor: BooleanExpressionVisitor[T]) -> None:
    stack.append(visitor.visit_true())


@_visit_stack.register(AlwaysFalse)
def _(_: AlwaysFalse, stack: List[T], visitor: BooleanExpressionVisitor[T]) -> None:
    stack.append(visitor.visit_false())


@_visit_stack.register(Not)
def _(_: Not, stack: List[T], visitor: BooleanExpressionVisitor[T]) -> None:
    child_result = stack.pop()
    stack.append(visitor.visit_not(child_result))


@_visit_stack.register(And)
def _(_: And, stack: List[T], visitor: BooleanExpressionVisitor[T]) -> None:
    right_result = stack.pop()
    left_result = stack.pop()
    stack.append(visitor.visit_and(left_result, right_result))


@_visit_stack.register(UnboundPredicate)
def _(obj: UnboundPredicate[L], stack: List[T], visitor: BooleanExpressionVisitor[T]) -> None:
    stack.append(visitor.visit_unbound_predicate(predicate=obj))


@_visit_stack.register(BoundPredicate)
def _(obj: BoundPredicate[L], stack: List[T], visitor: BooleanExpressionVisitor[T]) -> None:
    stack.append(visitor.visit_bound_predicate(predicate=obj))


@_visit_stack.register(Or)
def _(_: Or, stack: List[T], visitor: BooleanExpressionVisitor[T]) -> None:
    right_result = stack.pop()
    left_result = stack.pop()
    stack.append(visitor.visit_or(left_result, right_result))


def visit_iterative(expression: BooleanExpression, visitor: BooleanExpressionVisitor[T]) -> T:
    # Store (node, visited) pairs in the stack of expressions to process
    stack: List[Tuple[BooleanExpression, bool]] = [(expression, False)]
    # Store the results of the visit in another stack
    results_stack: List[T] = []

    while stack:
        node, visited = stack.pop()
        if not visited:
            stack.append((node, True))
            # TODO: Make this nicer.
            if isinstance(node, Not):
                stack.append((node.child, False))
            elif isinstance(node, And) or isinstance(node, Or):
                stack.append((node.right, False))
                stack.append((node.left, False))
        else:
            _visit_stack(node, results_stack, visitor)

    return results_stack.pop()


def bind_iterative(schema: Schema, expression: BooleanExpression, case_sensitive: bool) -> BooleanExpression:
    """Traverse iteratively over an expression to bind the predicates to the schema.

    Args:
      schema (Schema): A schema to use when binding the expression.
      expression (BooleanExpression): An expression containing UnboundPredicates that can be bound.
      case_sensitive (bool): Whether to consider case when binding a reference to a field in a schema, defaults to True.

    Raises:
        TypeError: In the case a predicate is already bound.
    """
    return visit_iterative(expression, BindVisitor(schema, case_sensitive))
