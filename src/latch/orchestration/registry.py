from __future__ import annotations

import datetime

from typing import Dict, Any, Optional, Set, List, TYPE_CHECKING
from threading import Lock

if TYPE_CHECKING:
    from .tasks import Task
    from .dag import TaskDependencyDAG
    from .constraints import ConstraintValidator


def _get_dag_classes():
    from .dag import TaskDependencyDAG, DAGNode
    return TaskDependencyDAG, DAGNode


class TaskRegistry:
    def __init__(self):
        self._lock = Lock()
        self._tasks: Dict[str, 'Task'] = {}
        self._task_metadata: Dict[str, Dict[str, Any]] = {}
        self._execution_plan: Optional['TaskDependencyDAG'] = None
        self._execution_history: List[Dict[str, Any]] = []
        self._active_tasks: Set[str] = set()
        self._constraint_validator: Optional['ConstraintValidator'] = None

        # Initialize violation handler
        from .violation import ConstraintViolationHandler
        self._violation_handler = ConstraintViolationHandler(self._get_display_name)

    @property
    def execution_plan_dag(self) -> 'TaskDependencyDAG':
        if self._execution_plan is None:
            TaskDependencyDAG, _ = _get_dag_classes()
            self._execution_plan = TaskDependencyDAG()
        return self._execution_plan

    @property
    def constraint_validator(self) -> 'ConstraintValidator':
        if self._constraint_validator is None:
            from .constraints import ConstraintValidator
            self._constraint_validator = ConstraintValidator(self.execution_plan_dag, self._tasks)
        return self._constraint_validator

    @property
    def execution_plan(self) -> List[str]:
        return self.execution_plan_dag.topological_sort()

    def register_task(self, task: 'Task', metadata: Optional[Dict[str, Any]] = None) -> None:
        with self._lock:
            self._tasks[task.name] = task
            self._task_metadata[task.name] = metadata or {}
            self.execution_plan_dag.add_task(task.name)

            print(f"[REGISTRY] Registered task: {task.name}")

    def get_task(self, name: str) -> Optional['Task']:
        return self._tasks.get(name)

    def _handle_constraint_violation(self, caller_task: str, callee_task: str, error: Exception) -> None:
        """Handle constraint validation failure using the violation handler."""
        self._violation_handler.handle_constraint_violation(caller_task, callee_task, error)

    def _get_display_name(self, task_name: str) -> str:
        """Get display name for a task, extracting base name from unique task name."""
        task = self._tasks.get(task_name)
        if task:
            metadata = self._task_metadata.get(task_name, {})
            return metadata.get('base_name', task_name)
        # Extract base name from unique task name (format: base_name_hash)
        if '_' in task_name:
            return '_'.join(task_name.split('_')[:-1])
        return task_name

    def add_runtime_dependency(self, caller_task: str, callee_task: str, caller_instance: 'Task') -> None:
        with self._lock:
            self.execution_plan_dag.add_task(caller_task)
            callee_instance = self._tasks.get(callee_task)
            if callee_instance:
                self.execution_plan_dag.add_task(callee_task)

            try:
                self.constraint_validator.validate_dependency(caller_task, callee_task, caller_instance)
            except Exception as e:
                self._handle_constraint_violation(caller_task, callee_task, e)
                raise e

            self.execution_plan_dag.add_dependency(callee_task, caller_task)

            print(f"[REGISTRY] Added runtime dependency: {caller_task} -> {callee_task}")

    def mark_task_started(self, task_name: str) -> None:
        with self._lock:
            self._active_tasks.add(task_name)
            self._execution_history.append({
                'task': task_name,
                'event': 'started',
                'timestamp': self._get_timestamp()
            })
            print(f"[REGISTRY] Task started: {task_name}")

    def mark_task_completed(self, task_name: str) -> None:
        with self._lock:
            self._active_tasks.discard(task_name)

            # Find and remove the start event for this task
            start_time = None
            start_event_index = None
            for i, event in enumerate(reversed(self._execution_history)):
                if event['task'] == task_name and event['event'] == 'started':
                    start_time = event['timestamp']
                    start_event_index = len(self._execution_history) - 1 - i
                    break

            # Remove the started event from history
            if start_event_index is not None:
                self._execution_history.pop(start_event_index)

            end_timestamp = self._get_timestamp()
            self._execution_history.append({
                'task': task_name,
                'event': 'completed',
                'start_time': start_time,
                'end_time': end_timestamp,
                'timestamp': end_timestamp  # Keep for compatibility
            })
            print(f"[REGISTRY] Task completed: {task_name}")

    def mark_task_failed(self, task_name: str, error: Exception) -> None:
        """Mark a task as failed execution."""
        with self._lock:
            self._active_tasks.discard(task_name)

            start_time = None
            start_event_index = None
            for i, event in enumerate(reversed(self._execution_history)):
                if event['task'] == task_name and event['event'] == 'started':
                    start_time = event['timestamp']
                    start_event_index = len(self._execution_history) - 1 - i
                    break

            # Remove the started event from history
            if start_event_index is not None:
                self._execution_history.pop(start_event_index)

            end_timestamp = self._get_timestamp()
            self._execution_history.append({
                'task': task_name,
                'event': 'failed',
                'error': str(error),
                'start_time': start_time,
                'end_time': end_timestamp,
                'timestamp': end_timestamp  # Keep for compatibility
            })
            print(f"[REGISTRY] Task failed: {task_name}")


    def get_calling_task(self, current_task_name: str) -> 'Task' | None:
        with self._lock:
            # Find the most recently started active task that's not the current task
            for event in reversed(self._execution_history):
                if (event['event'] == 'started' and
                    event['task'] in self._active_tasks and
                    event['task'] != current_task_name):
                    return self._tasks.get(event['task'])

            return None

    def get_active_tasks(self) -> Set[str]:
        return self._active_tasks.copy()

    def get_execution_history(self) -> List[Dict[str, Any]]:
        return self._execution_history.copy()


    def print_task_registry(self) -> None:
        print("=" * 60)
        print(f"[REGISTRY] TASK REGISTRY STATE:")
        print(f"[REGISTRY]   Registered Tasks: {len(self._tasks)}")

        if self._tasks:
            for task_name in self._tasks:
                metadata = self._task_metadata.get(task_name, {})
                base_name = metadata.get('base_name', 'unknown')
                instance_hash = metadata.get('instance_hash', 'unknown')
                has_constraints = metadata.get('has_constraints', False)

                print(f"[REGISTRY]     {base_name}[{instance_hash}] -> {task_name}")
                print(f"[REGISTRY]       constraints: {has_constraints}")
        else:
            print("[REGISTRY]     No tasks registered")

        print(f"[REGISTRY]   Active Tasks: {len(self._active_tasks)}")
        if self._active_tasks:
            for active_task in self._active_tasks:
                print(f"[REGISTRY]     Currently executing: {active_task}")
        else:
            print("[REGISTRY]     No tasks currently active")

        print(f"[REGISTRY]   Execution History: {len(self._execution_history)} events")
        if self._execution_history:
            for event in self._execution_history:
                task_name = event.get('task', 'unknown')
                event_type = event.get('event', 'unknown')
                timestamp = event.get('timestamp', 'unknown')
                print(f"[REGISTRY]     {task_name} -> {event_type} @ {timestamp[-8:]}")

        print("=" * 60)

    def _get_timestamp(self) -> str:
        return datetime.datetime.now().isoformat()

    def print_execution_plan(self) -> None:
        from .emitter import emit_dag_json

        # Print task registry state for debugging
        self.print_task_registry()

        print("=" * 60)

        # Print execution plan with topological order info
        print(f"[DAG] EXECUTION PLAN (Task Dependencies - Topologically Sorted):")
        execution_plan_list = self.execution_plan
        print(f"[DAG]   Execution Order: {' -> '.join(execution_plan_list) if execution_plan_list else 'No tasks defined'}")
        print(f"[DAG]   DAG Nodes: {len(self.execution_plan_dag.nodes)}")

        # Use the DAG's print method with display name formatting
        self.execution_plan_dag.print_dag()

        print("=" * 60)

        # Emit execution plan to visualization server
        execution_plan_dag = self.execution_plan_dag
        if execution_plan_dag.nodes:
            execution_plan_json = execution_plan_dag.to_json()
            execution_plan_json["title"] = "Execution Plan"
            execution_plan_json["metadata"]["execution_order"] = execution_plan_list
            execution_plan_json["metadata"]["execution_history"] = self.get_execution_history()
            execution_plan_json["metadata"]["skip_isolated_nodes"] = True

            self._add_metadata_and_status_to_nodes(execution_plan_json)

            emit_dag_json(execution_plan_json)

    def _add_metadata_and_status_to_nodes(self, dag_json: Dict[str, Any]) -> None:
        for node in dag_json['nodes']:
            task_name = node['id']
            task = self._tasks.get(task_name)
            metadata = self._task_metadata.get(task_name, {})

            if task:
                node.update({
                    "has_constraints": bool(getattr(task, 'constraints', None)),
                    "description": getattr(task, 'description', None)
                })

            if metadata:
                node.update(metadata)

        # task_name -> status
        task_status = {}

        # task_name -> error_message
        task_errors = {}

        # Check which tasks are currently active
        active_tasks = self.get_active_tasks()

        # Process execution history to determine final status of each task
        for event in self._execution_history:
            task_name = event['task']
            event_type = event['event']

            if event_type == 'completed':
                task_status[task_name] = 'completed'
                task_errors.pop(task_name, None)
            elif event_type == 'failed':
                task_status[task_name] = 'failed'
                task_errors[task_name] = event.get('error', 'Unknown error')
            elif event_type == 'started' and task_name in active_tasks:
                if task_name not in task_status:
                    task_status[task_name] = 'running'

        for node in dag_json['nodes']:
            task_name = node['id']
            status = task_status.get(task_name, 'pending')

            node['status'] = status
            if task_name in task_errors:
                node['error'] = task_errors[task_name]


# Global task registry instance
_task_registry: Optional[TaskRegistry] = None


def get_task_registry() -> TaskRegistry:
    global _task_registry
    if _task_registry is None:
        _task_registry = TaskRegistry()
    return _task_registry
