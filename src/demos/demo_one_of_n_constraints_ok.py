#!/usr/bin/env python3
import sys
import os

# Add parent directory to path so we can import latch
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import time
from latch.orchestration import task, TaskScheduler
from latch.orchestration.constraints import Constraints


# ==================== PROCESSORS WITH SIMPLE CONSTRAINTS ====================


@task(
    name="single_router",
    constraints=Constraints(
        # Allow connections to any 
        allow_outgoing_to_names=["one", "two", "three", "four"],
    ),
)
def single_router():
    print("[ROUTER] Routing data to exactly one...")
    time.sleep(1)


@task(name="one")
def one():
    print("[PROCESSOR] One...")
    time.sleep(2)


@task(name="two")
def two():
    print("[PROCESSOR] Two...")
    time.sleep(2)


@task(name="three")
def three():
    print("[PROCESSOR] Three...")
    time.sleep(2)


@task(name="four")
def four():
    print("[PROCESSOR] Four...")
    time.sleep(2)


# ==================== SETUP FUNCTIONS ====================


def setup_relationships():
    """Demonstrate simple constraint violations."""
    print("\n[SETUP] Demonstrating simple constraint violations...")

    try:
        # This should work (first connection)
        single_router.create_path_to(one)
        single_router.create_path_to(two)
        single_router.create_path_to(three)
        single_router.create_path_to(four)

    except Exception as e:
        print(f"[SETUP] Expected outdegree violation caught: {e}")



if __name__ == "__main__":
    setup_relationships()

    # Create scheduler and execute the entire DAG
    scheduler = TaskScheduler()
    scheduler.print_scheduler_status()

    print("\n[MAIN] Executing DAG using scheduler loop...")

    results = scheduler.execute_dag()
    print(f"\n[MAIN] Execution results: {len(results)} tasks completed")

