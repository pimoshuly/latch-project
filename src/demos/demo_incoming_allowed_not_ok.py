#!/usr/bin/env python3
import sys
import os

# Add parent directory to path so we can import latch
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import time
from latch.orchestration import task, Path, TaskScheduler
from latch.orchestration.constraints import Constraints


# ==================== CALLER TASKS ====================


@task(name="authorized_caller")
def authorized_caller():
    print("[CALLER] Authorized caller executing...")
    time.sleep(2)


@task(name="unauthorized_caller")
def unauthorized_caller():
    print("[CALLER] Unauthorized caller executing...")
    time.sleep(2)


@task(name="orchestrator_caller")
def orchestrator_caller():
    print("[CALLER] Orchestrator caller executing...")
    time.sleep(2)


# ==================== PROTECTED RESOURCE ====================


@task(
    name="protected_resource",
    constraints=Constraints(
        allow_incoming_from_names=["authorized_caller", "orchestrator_caller"]
    ),
)
def protected_resource():
    print("[PROTECTED] Processing protected resource...")
    time.sleep(2)


# ==================== EXPLICIT PATH RELATIONSHIPS ====================


def setup_task_relationships() -> str:
    authorized_caller.create_path_to(protected_resource)
    unauthorized_caller.create_path_to(protected_resource)
    orchestrator_caller.create_path_to(protected_resource)

    demo_incoming_constraints.create_path_to(authorized_caller)
    demo_incoming_constraints.create_path_to(orchestrator_caller)
    demo_incoming_constraints.create_path_to(unauthorized_caller)

    return demo_incoming_constraints.name


# ==================== DEMONSTRATION ORCHESTRATION ====================


@task(name="demo_incoming_constraints")
def demo_incoming_constraints():
    print("\n" + "=" * 60)
    print("DEMO: INCOMING CONSTRAINT VALIDATION")
    print("=" * 60)


if __name__ == "__main__":
    root_task_name = setup_task_relationships()

    # Create scheduler and execute the entire DAG
    scheduler = TaskScheduler()
    scheduler.print_scheduler_status()

    print("\n[MAIN] Executing DAG using scheduler loop...")

    results = scheduler.execute_dag()
    print(f"\n[MAIN] Execution results: {len(results)} tasks completed")
    print(f"[MAIN] Final result: {results.get(root_task_name, 'No result')}")
