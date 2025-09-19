from __future__ import annotations

from typing import Dict, Any, Optional, List
from .registry import get_task_registry


class TaskScheduler:
    """Scheduler that executes tasks by following DAG dependencies."""

    def __init__(self):
        self.registry = get_task_registry()

    def execute_task_by_name(self, task_name: str, *args, **kwargs) -> Any:
        task = self.registry.get_task(task_name)
        if not task:
            raise ValueError(f"Task '{task_name}' not found in registry")

        print(f"[SCHEDULER] Executing task: {task_name}")

        return task(*args, **kwargs)


    def execute_dag(self) -> Dict[str, Any]:
        execution_plan = self.registry.execution_plan
        print(f"[SCHEDULER] Execution plan: {' -> '.join(execution_plan)}")
        print(f"[SCHEDULER] Starting DAG execution...")

        results = {}
        executed_tasks = set()

        while True:
            # Ask registry for next ready tasks
            ready_tasks = self.get_ready_tasks()

            # Filter out already executed tasks
            ready_tasks = [task for task in ready_tasks if task not in executed_tasks]

            if not ready_tasks:
                print(f"[SCHEDULER] No more ready tasks. Execution complete.")
                break

            # Execute all ready tasks (they are self-contained)
            for task_name in ready_tasks:
                print(f"[SCHEDULER] Executing ready task: {task_name}")

                try:
                    result = self.execute_task_by_name(task_name)
                    results[task_name] = result
                    executed_tasks.add(task_name)
                    print(f"[SCHEDULER] Completed task: {task_name}")
                except Exception as e:
                    from .constraints import ConstraintViolationError
                    if isinstance(e, ConstraintViolationError):
                        print(f"[SCHEDULER] CONSTRAINT VIOLATION: {e}")
                        print(f"[SCHEDULER] Stopping execution due to constraint violation")
                        return results
                    else:
                        print(f"[SCHEDULER] Failed to execute task {task_name}: {e}")
                        # Stop execution on any failure
                        return results

        print(f"[SCHEDULER] DAG execution completed. Executed {len(executed_tasks)} tasks.")
        return results

    def get_execution_plan(self) -> List[str]:
        """Get the topological execution order of all registered tasks."""
        return self.registry.execution_plan

    def get_ready_tasks(self) -> List[str]:
        """Get tasks that have no pending dependencies and can be executed."""
        dag = self.registry.execution_plan_dag
        ready_tasks = []

        for task_name, node in dag.nodes.items():
            # A task is ready if it has no dependencies or all dependencies are completed
            if not node.dependencies:
                ready_tasks.append(task_name)
            else:
                # Check if all dependencies are completed
                history = self.registry.get_execution_history()
                completed_tasks = {
                    event['task'] for event in history
                    if event['event'] == 'completed'
                }

                if all(dep in completed_tasks for dep in node.dependencies):
                    # Also check that this task hasn't been completed yet
                    if task_name not in completed_tasks:
                        ready_tasks.append(task_name)

        return ready_tasks

    def print_scheduler_status(self) -> None:
        print("=" * 60)
        print("[SCHEDULER] Current Status:")

        execution_plan = self.get_execution_plan()
        ready_tasks = self.get_ready_tasks()
        active_tasks = self.registry.get_active_tasks()

        print(f"[SCHEDULER]   Total registered tasks: {len(execution_plan)}")
        print(f"[SCHEDULER]   Ready to execute: {len(ready_tasks)}")
        print(f"[SCHEDULER]   Currently running: {len(active_tasks)}")

        if ready_tasks:
            print(f"[SCHEDULER]   Ready tasks: {', '.join(ready_tasks)}")

        if active_tasks:
            print(f"[SCHEDULER]   Running tasks: {', '.join(active_tasks)}")

        print(f"[SCHEDULER]   Execution order: {' -> '.join(execution_plan)}")
        print("=" * 60)