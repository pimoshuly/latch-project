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
)

from typing_extensions import (
    ParamSpec,
)

from .constraints import Constraints
from .call_context import CallContext

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
            'description': self.description,
            'has_constraints': self.constraints is not None,
            'base_name': self.base_name,
            'instance_hash': self.instance_hash,
            'unique_name': self.name
        }

        registry.register_task(self, metadata)

    def _update_execution_dag(self, caller_name: str, callee_name: str, caller_task_instance: 'Task') -> None:
        from .registry import get_task_registry
        registry = get_task_registry()
        registry.add_runtime_dependency(caller_name, callee_name, caller_task_instance)

        print(f"[DEBUG] DAG Updated: {caller_name} -> {callee_name}")


    def _get_validation_context(self) -> dict:
        """Get validation context for validation."""
        call_context = CallContext.get_caller_info(skip_frames=4)  # Skip: _get_validation_context, _track_caller_callee_relation, _validate_pre_execution, __call__/_call_sync
        return call_context

    def _get_calling_task(self) -> 'Task' | None:
        from .registry import get_task_registry
        registry = get_task_registry()
        return registry.get_calling_task(self.name)

    def _track_caller_callee_relation(self) -> dict:
        call_context = self._get_validation_context()

        caller_task_instance = self._get_calling_task()

        print(f"[DEBUG] CALL CONTEXT: {call_context}")

        # Extract caller and callee information
        caller_name = call_context['caller']
        callee_name = self.name  # This task's unique hashed name

        if caller_task_instance is not None:
            # Both caller and callee are tasks - validate and track relationship
            assert isinstance(caller_task_instance, Task), f"Caller must be a Task instance"
            assert isinstance(self, Task), f"Callee {callee_name} must be a Task instance"

            # Use the caller's hashed name for proper tracking
            caller_hashed_name = caller_task_instance.name

            print(f"[DEBUG] TASK-TO-TASK: {caller_hashed_name} -> {callee_name} (updating execution DAG)")

            # Update the execution state DAG with this task dependency using hashed names
            self._update_execution_dag(caller_hashed_name, callee_name, caller_task_instance)

            call_context['caller_hashed_name'] = caller_hashed_name
            call_context['callee_hashed_name'] = callee_name
            call_context['relationship_type'] = 'task_to_task'
        else:
            print(f"[DEBUG] CALLER-CALLEE: {caller_name} -> {callee_name} (standalone)")
            call_context['caller_hashed_name'] = caller_name  # Non-task caller
            call_context['callee_hashed_name'] = callee_name
            call_context['relationship_type'] = 'standalone'

        return call_context

    def _validate_pre_execution(self) -> None:
        call_context = self._track_caller_callee_relation()

        print(f"[INFO] TASK {self.name}: Pre-execution validation starting")
        print(f"[DEBUG] Called from: {call_context['caller']} (type: {call_context['relationship_type']})")

        print(f"[DEBUG] {self.name}: Constraint validation handled by registry")

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> R:
        """Execute the task with constraint validation."""
        from .registry import get_task_registry
        registry = get_task_registry()

        try:
            self._validate_pre_execution()
            registry.mark_task_started(self.name)
            result = self.fn(*args, **kwargs)
            registry.mark_task_completed(self.name, result)
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


