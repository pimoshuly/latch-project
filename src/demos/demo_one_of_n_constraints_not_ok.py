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
        # But limit to only ONE outgoing connection total
        limit_outdegree=1,
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
        print("[SETUP] First connection to One succeeded")

        single_router.create_path_to(two)
        print("[SETUP] This should not print - violation should have occurred!")

    except Exception as e:
        print(f"[SETUP] Expected outdegree violation caught: {e}")



if __name__ == "__main__":
    print("\n[MAIN] Testing invalid simple constraint relationships...")
    setup_relationships()

    print("\n[MAIN] Demo completed!")
