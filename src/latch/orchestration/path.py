from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .tasks import Task


class Path:
    """Represents an explicit relationship between two tasks."""

    def __init__(self, from_task: "Task", to_task: "Task"):
        """
        Create a path representing a caller-callee relationship.

        Args:
            from_task: The calling task
            to_task: The called task
        """
        self.from_task = from_task
        self.to_task = to_task
        self._register_relationship()

    def _register_relationship(self) -> None:
        """Register this relationship in the task registry."""
        from .registry import get_task_registry

        registry = get_task_registry()
        registry.add_runtime_dependency(
            self.from_task.name, self.to_task.name, self.from_task
        )

    def __repr__(self) -> str:
        return f"Path(from={self.from_task.name}, to={self.to_task.name})"
