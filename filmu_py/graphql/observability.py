"""GraphQL schema observability extensions."""

from __future__ import annotations

from collections.abc import Iterator
from time import perf_counter

from graphql.language.ast import FieldNode, OperationDefinitionNode
from prometheus_client import Counter, Histogram
from strawberry.extensions import SchemaExtension
from strawberry.types.execution import ExecutionContext

GRAPHQL_OPERATIONS_TOTAL = Counter(
    "filmu_py_graphql_operations_total",
    "GraphQL operations by operation type, root field, and outcome",
    ["operation_type", "root_field", "outcome"],
)
GRAPHQL_OPERATION_DURATION_SECONDS = Histogram(
    "filmu_py_graphql_operation_duration_seconds",
    "GraphQL operation duration in seconds",
    ["operation_type", "root_field", "outcome"],
    buckets=[0.001, 0.005, 0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0],
)


def _operation_definition(execution_context: ExecutionContext) -> OperationDefinitionNode | None:
    graphql_document = execution_context.graphql_document
    if graphql_document is None:
        return None

    operation_name = execution_context.operation_name
    for definition in graphql_document.definitions:
        if not isinstance(definition, OperationDefinitionNode):
            continue
        if operation_name is None:
            return definition
        if definition.name is not None and definition.name.value == operation_name:
            return definition
    return None


def _operation_type_label(execution_context: ExecutionContext) -> str:
    try:
        return execution_context.operation_type.value
    except Exception:
        return "unknown"


def _root_field_label(execution_context: ExecutionContext) -> str:
    definition = _operation_definition(execution_context)
    if definition is None:
        return "unknown"

    for selection in definition.selection_set.selections:
        if isinstance(selection, FieldNode):
            return selection.name.value
    return "unknown"


def _operation_outcome(execution_context: ExecutionContext) -> str:
    if execution_context.pre_execution_errors:
        return "pre_execution_error"

    result = execution_context.result
    if result is None:
        return "exception"
    if getattr(result, "errors", None):
        return "execution_error"
    return "success"


class GraphQLOperationMetricsExtension(SchemaExtension):
    """Emit Prometheus metrics for GraphQL operations using bounded labels."""

    def on_operation(self) -> Iterator[None]:
        started_at = perf_counter()
        try:
            yield
        finally:
            elapsed = perf_counter() - started_at
            execution_context = self.execution_context
            operation_type = _operation_type_label(execution_context)
            root_field = _root_field_label(execution_context)
            outcome = _operation_outcome(execution_context)
            GRAPHQL_OPERATIONS_TOTAL.labels(
                operation_type=operation_type,
                root_field=root_field,
                outcome=outcome,
            ).inc()
            GRAPHQL_OPERATION_DURATION_SECONDS.labels(
                operation_type=operation_type,
                root_field=root_field,
                outcome=outcome,
            ).observe(elapsed)
