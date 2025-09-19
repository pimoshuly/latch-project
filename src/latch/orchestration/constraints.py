from __future__ import annotations

from pydantic import BaseModel, Field
from typing import Optional, Set, TYPE_CHECKING

if TYPE_CHECKING:
    from .tasks import Task
    from .dag import TaskDependencyDAG


class ConstraintViolationError(Exception):
    def __init__(self, message: str, constraint_type: str, task_name: str):
        self.constraint_type = constraint_type
        self.task_name = task_name
        super().__init__(
            f"Constraint violation in task '{task_name}' ({constraint_type}): {message}"
        )


class Constraints(BaseModel):
    # from THIS node
    limit_outdegree: Optional[int] = Field(None, ge=0)
    allow_outgoing_to_names: Set[str] = set()

    # into THIS node
    limit_indegree: Optional[int] = Field(None, ge=0)
    allow_incoming_from_names: Set[str] = set()


class ConstraintValidator:
    def __init__(self, dag: "TaskDependencyDAG", tasks_registry: dict):
        self.dag = dag
        self.tasks_registry = tasks_registry

    def validate_outgoing_edge_constraints(
        self, caller_task: str, callee_task: str, caller_instance: "Task"
    ) -> None:
        if not caller_instance.constraints:
            return

        # Check current outdegree before adding new dependency
        current_dependents = self.dag.nodes.get(caller_task, None)
        current_outdegree = (
            len(current_dependents.dependents) if current_dependents else 0
        )

        # Validate outdegree limit
        if (
            caller_instance.constraints.limit_outdegree is not None
            and current_outdegree >= caller_instance.constraints.limit_outdegree
        ):
            raise ConstraintViolationError(
                f"Cannot add dependency {caller_task} -> {callee_task}. "
                f"Outdegree limit reached: {current_outdegree} >= {caller_instance.constraints.limit_outdegree}",
                "outgoing_edges",
                caller_task,
            )

        # Validate allow lists for outgoing edges using base names
        callee_instance = self.tasks_registry.get(callee_task)
        callee_base_name = (
            callee_instance.base_name
            if callee_instance and hasattr(callee_instance, "base_name")
            else callee_task
        )

        if (
            caller_instance.constraints.allow_outgoing_to_names
            and callee_base_name
            not in caller_instance.constraints.allow_outgoing_to_names
        ):
            print(f"[REGISTRY] CONSTRAINT VIOLATION: {caller_task} -> {callee_task}")
            print(
                f"[REGISTRY] Target base name '{callee_base_name}' not in allow list {caller_instance.constraints.allow_outgoing_to_names}"
            )
            raise ConstraintViolationError(
                f"Cannot add dependency {caller_task} -> {callee_task}. "
                f"Target task base name '{callee_base_name}' not in allowed outgoing task names {caller_instance.constraints.allow_outgoing_to_names}",
                "outgoing_edges",
                caller_task,
            )

    def validate_incoming_edge_constraints(
        self, caller_task: str, callee_task: str, caller_instance: "Task"
    ) -> None:
        callee_instance = self.tasks_registry.get(callee_task)
        if not callee_instance or not callee_instance.constraints:
            return

        # Check current indegree before adding new dependency
        current_dependencies = self.dag.nodes.get(callee_task, None)
        current_indegree = (
            len(current_dependencies.dependencies) if current_dependencies else 0
        )

        # Validate indegree limit
        if (
            callee_instance.constraints.limit_indegree is not None
            and current_indegree >= callee_instance.constraints.limit_indegree
        ):
            raise ConstraintViolationError(
                f"Cannot add dependency {caller_task} -> {callee_task}. "
                f"Indegree limit reached: {current_indegree} >= {callee_instance.constraints.limit_indegree}",
                "incoming_edges",
                callee_task,
            )

        # Validate allow lists for incoming edges using base names
        caller_base_name = (
            caller_instance.base_name
            if caller_instance and hasattr(caller_instance, "base_name")
            else caller_task
        )

        if (
            callee_instance.constraints.allow_incoming_from_names
            and caller_base_name
            not in callee_instance.constraints.allow_incoming_from_names
        ):
            print(f"[REGISTRY] CONSTRAINT VIOLATION: {caller_task} -> {callee_task}")
            print(
                f"[REGISTRY] Source base name '{caller_base_name}' not in allow list {callee_instance.constraints.allow_incoming_from_names}"
            )
            raise ConstraintViolationError(
                f"Cannot add dependency {caller_task} -> {callee_task}. "
                f"Source task base name '{caller_base_name}' not in allowed incoming task names {callee_instance.constraints.allow_incoming_from_names}",
                "incoming_edges",
                callee_task,
            )

    def validate_dependency(
        self, caller_task: str, callee_task: str, caller_instance: "Task"
    ) -> None:
        self.validate_outgoing_edge_constraints(
            caller_task, callee_task, caller_instance
        )
        self.validate_incoming_edge_constraints(
            caller_task, callee_task, caller_instance
        )
