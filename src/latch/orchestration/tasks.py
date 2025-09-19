from __future__ import annotations

import hashlib
import time
from functools import update_wrapper
from typing import (
    Any,
    Callable,
    Generic,
    Optional,
    TypeVar,
    Union,
    overload,
    TYPE_CHECKING,
)

from typing_extensions import (
    ParamSpec,
)

from .constraints import Constraints

if TYPE_CHECKING:
    from .path import Path

T = TypeVar("T")
R = TypeVar("R")  # The return type of the user's function
P = ParamSpec("P")  # The parameters of the task


class Task(Generic[P, R]):
    def __init__(
        self,
        fn: Callable[P, R] | "classmethod[Any, P, R]" | "staticmethod[P, R]",
        name: Optional[str] = None,
        description: Optional[str] = None,
        constraints: Optional[Constraints] = None,
    ):
        self.description: str | None = description
        self.constraints = constraints
        update_wrapper(self, fn)
        self.fn = fn

        # Generate base name
        if not name:
            if hasattr(fn, "__name__"):
                base_name = fn.__name__
            else:
                base_name = type(fn).__name__
        else:
            base_name = name

        self.instance_hash = self._generate_unique_hash(base_name)

        self.name = f"{base_name}_{self.instance_hash}"
        self.base_name = base_name

        self._register_in_registry()

    def _generate_unique_hash(self, base_name: str) -> str:
        unique_data = f"{base_name}_{time.time_ns()}_{id(self)}"
        hash_digest = hashlib.sha256(unique_data.encode()).hexdigest()
        return hash_digest[:8]

    def _register_in_registry(self) -> None:
        from .registry import get_task_registry

        registry = get_task_registry()

        metadata = {
            "description": self.description,
            "has_constraints": self.constraints is not None,
            "base_name": self.base_name,
            "instance_hash": self.instance_hash,
            "unique_name": self.name,
        }

        registry.register_task(self, metadata)

    def create_path_to(self, to_task: "Task") -> "Path":
        """Create an explicit path from this task to another task."""
        from .path import Path

        return Path(from_task=self, to_task=to_task)

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> R:
        """Execute the task with constraint validation."""
        from .registry import get_task_registry

        registry = get_task_registry()

        try:
            registry.mark_task_started(self.name)
            result = self.fn(*args, **kwargs)
            registry.mark_task_completed(self.name)
            registry.print_execution_plan()

            return result

        except Exception as e:
            registry.mark_task_failed(self.name, e)
            registry.print_execution_plan()

            from .constraints import ConstraintViolationError

            if isinstance(e, ConstraintViolationError):
                raise

            # Re-raise other exceptions with context
            raise RuntimeError(f"Task '{self.name}' failed: {str(e)}") from e


@overload
def task(__fn: Callable[P, R]) -> Task[P, R]: ...


@overload
def task(
    *,
    name: Optional[str] = None,
    description: Optional[str] = None,
    constraints: Optional[Constraints] = None,
) -> Callable[[Callable[P, R]], Task[P, R]]: ...


def task(
    __fn: Optional[Callable[P, R]] = None,
    *,
    name: Optional[str] = None,
    description: Optional[str] = None,
    constraints: Optional[Constraints] = None,
) -> Union[Task[P, R], Callable[[Callable[P, R]], Task[P, R]]]:
    if __fn is None:

        def decorator(func: Callable[P, R]) -> Task[P, R]:
            return Task(
                fn=func,
                name=name,
                description=description,
                constraints=constraints,
            )

        return decorator
    else:
        # Called without arguments: @task
        return Task(
            fn=__fn,
            name=name,
            description=description,
            constraints=constraints,
        )
