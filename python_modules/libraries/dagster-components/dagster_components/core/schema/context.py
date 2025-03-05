import os
from collections.abc import Mapping, Sequence
from typing import Any, Optional, TypeVar, Union, overload

from dagster._core.definitions.declarative_automation.automation_condition import (
    AutomationCondition,
)
from dagster._record import copy, record
from dagster._utils.source_position import SourcePositionTree
from jinja2.exceptions import UndefinedError
from jinja2.nativetypes import NativeTemplate

from dagster_components.core.schema.base import ResolvableSchema, resolve

T = TypeVar("T")


def env_scope(key: str) -> Optional[str]:
    return os.environ.get(key)


def automation_condition_scope() -> Mapping[str, Any]:
    return {
        "eager": AutomationCondition.eager,
        "on_cron": AutomationCondition.on_cron,
    }


T = TypeVar("T")


class ResolutionException(Exception): ...


@record
class ResolutionContext:
    scope: Mapping[str, Any]
    path: list[str] = []
    source_position_tree: Optional[SourcePositionTree] = None

    def push(self, path_part: Union[str, int]):
        return copy(self, path=[*self.path, path_part])

    @staticmethod
    def default(source_position_tree: Optional[SourcePositionTree] = None) -> "ResolutionContext":
        return ResolutionContext(
            scope={"env": env_scope, "automation_condition": automation_condition_scope()},
            source_position_tree=source_position_tree,
        )

    def with_scope(self, **additional_scope) -> "ResolutionContext":
        return copy(self, scope={**self.scope, **additional_scope})

    def _invalid_scope_exc(self, exc: UndefinedError):
        msg_parts = ["Error rendering template"]

        path_str = ".".join(self.path)
        if self.source_position_tree:
            source_pos, _ = self.source_position_tree.lookup_closest_and_path(self.path, trace=None)
            msg_parts.append(f"{source_pos!s}, path {path_str}")
        else:
            msg_parts.append(f"At path {path_str}")

        msg = exc.message
        if msg:
            idx = msg.find(" is undefined")
            if idx > 0:
                missing_scope = msg[:idx]
                msg_parts.append(
                    f"{missing_scope} not found in scope, available scope is: {', '.join(self.scope.keys())}"
                )
            else:
                msg_parts.append(msg)
        else:
            msg_parts.append("UndefinedError")

        raise ResolutionException("\n".join(msg_parts))

    def _resolve_inner_value(self, val: Any) -> Any:
        """Resolves a single value, if it is a templated string."""
        if isinstance(val, ResolvableSchema):
            return resolve(val, self)
        elif isinstance(val, str):
            try:
                return NativeTemplate(val).render(**self.scope)
            except UndefinedError as undefined_error:
                raise self._invalid_scope_exc(undefined_error)

        else:
            return val

    @overload
    def resolve_value(self, val: Any, as_type: type[T]) -> T: ...

    @overload
    def resolve_value(self, val: ResolvableSchema[T]) -> T: ...

    @overload
    def resolve_value(self, val: Optional[ResolvableSchema[T]]) -> Optional[T]: ...

    @overload
    def resolve_value(self, val: Sequence[ResolvableSchema[T]]) -> Sequence[T]: ...

    @overload
    def resolve_value(self, val: Mapping) -> Mapping: ...

    @overload
    def resolve_value(self, val: tuple) -> tuple: ...

    @overload
    def resolve_value(self, val: Sequence) -> Sequence: ...

    def resolve_value(self, val: Any, as_type: Optional[type] = None) -> Any:
        """Recursively resolves templated values in a nested object."""
        if isinstance(val, dict):
            return {k: self.push(k).resolve_value(v) for k, v in val.items()}
        elif isinstance(val, tuple):
            return tuple(self.push(i).resolve_value(v) for i, v in enumerate(val))
        elif isinstance(val, list):
            return [self.push(i).resolve_value(v) for i, v in enumerate(val)]
        else:
            return self._resolve_inner_value(val)
