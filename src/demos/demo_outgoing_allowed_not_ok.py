#!/usr/bin/env python3
import sys
import os

# Add parent directory to path so we can import latch
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import time
import datetime
import random
from latch.orchestration import task, Path, TaskScheduler
from latch.orchestration.constraints import Constraints


# ==================== ASSESSMENT ====================

@task(name="assess_data")
def assess_data():
    """Assess data and determine processing approach."""
    print("[ASSESSMENT] Assessing data...")
    time.sleep(2)

    random.seed(datetime.datetime.now().timestamp())
    data_size = random.randint(0, 20)
    assessment_result = "large" if data_size > 10 else "small"

    print(f"[ASSESSMENT] Assessment result: {assessment_result}")

    # Conditional execution based on assessment
    if assessment_result == "small":
        process_small()
    else:
        process_big()


# ==================== PROCESSING TASKS ====================

@task(name="process_small")
def process_small():
    """Process small data."""
    print("[PROCESS] Processing small data...")
    time.sleep(2)


@task(name="process_big")
def process_big():
    """Process big data."""
    print("[PROCESS] Processing big data...")
    time.sleep(2)


# ==================== ORCHESTRATOR WITH CONSTRAINTS ====================

@task(name="restricted_orchestrator", constraints=Constraints(allow_outgoing_to_names=["assess_data", "process_small"]))
def restricted_orchestrator():
    """Restricted orchestrator - only allowed to call assess_data and process_small."""
    print("\n" + "="*60)
    print("RESTRICTED ORCHESTRATOR EXECUTING")
    print("="*60)
    print("NOTE: Outgoing constraints are enforced during direct task calls.")
    print("With scheduler-driven execution, there are no direct calls to validate.")
    print("="*60)


@task(name="unrestricted_orchestrator")
def unrestricted_orchestrator():
    """Unrestricted orchestrator - can call any tasks."""
    print("\n" + "="*60)
    print("UNRESTRICTED ORCHESTRATOR EXECUTING")
    print("="*60)


# ==================== EXPLICIT PATH RELATIONSHIPS ====================

def setup_task_relationships() -> str:
    """Setup explicit caller-callee relationships using Path construct.

    Returns:
        str: The unique name of the root task in the graph
    """

    # Assessment task calls processing tasks conditionally
    restricted_orchestrator.create_path_to(process_small)
    restricted_orchestrator.create_path_to(process_big)

    unrestricted_orchestrator.create_path_to(process_small)
    unrestricted_orchestrator.create_path_to(process_big)

    # Orchestrators call assess_data
    restricted_orchestrator.create_path_to(assess_data)
    unrestricted_orchestrator.create_path_to(assess_data)

    # Main demo calls both orchestrators
    demo_outgoing_constraints.create_path_to(restricted_orchestrator)
    demo_outgoing_constraints.create_path_to(unrestricted_orchestrator)

    return demo_outgoing_constraints.name

# ==================== DEMONSTRATION ====================

@task(name="demo_outgoing_constraints")
def demo_outgoing_constraints():
    """Demonstrate outgoing constraint validation with scheduler-driven execution."""
    print("\n" + "="*80)
    print("DEMO: OUTGOING CONSTRAINT VALIDATION")
    print("="*80)
    print("NOTE: Outgoing constraints are enforced during direct task calls.")
    print("With scheduler-driven execution, there are no direct calls to validate.")
    print("This demo will complete all tasks successfully via scheduler.")
    print("="*80)


if __name__ == "__main__":
    root_task_name = setup_task_relationships()

    # Create scheduler and execute the entire DAG
    scheduler = TaskScheduler()
    scheduler.print_scheduler_status()

    print("\n[MAIN] Executing DAG using scheduler loop...")

    results = scheduler.execute_dag()
    print(f"\n[MAIN] Execution results: {len(results)} tasks completed")
    print(f"[MAIN] Final result: {results.get(root_task_name, 'No result')}")