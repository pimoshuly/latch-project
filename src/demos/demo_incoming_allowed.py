#!/usr/bin/env python3
import sys
import os

# Add parent directory to path so we can import latch
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import time
from latch.orchestration import task
from latch.orchestration.constraints import Constraints


# ==================== CALLER TASKS ====================

@task(name="authorized_caller")
def authorized_caller() -> str:
    print("[CALLER] Authorized caller executing...")
    time.sleep(1)

    # This should succeed - authorized_caller is in the allow list
    result = protected_resource("authorized_data")
    return f"authorized_result: {result}"


@task(name="unauthorized_caller")
def unauthorized_caller() -> str:
    print("[CALLER] Unauthorized caller executing...")
    time.sleep(1)

    # This should fail - unauthorized_caller is NOT in the allow list
    result = protected_resource("unauthorized_data")
    return f"unauthorized_result: {result}"


@task(name="orchestrator_caller")
def orchestrator_caller() -> str:
    print("[CALLER] Orchestrator caller executing...")
    time.sleep(1)

    # This should succeed - orchestrator_caller is in the allow list
    result = protected_resource("orchestrator_data")
    return f"orchestrator_result: {result}"


# ==================== PROTECTED RESOURCE ====================

@task(name="protected_resource", constraints=Constraints(allow_incoming_from_names=["authorized_caller", "orchestrator_caller"]))
def protected_resource(data: str) -> str:
    print(f"[PROTECTED] Processing protected data: {data}")
    time.sleep(2)
    return f"processed_{data}"


# ==================== DEMONSTRATION ORCHESTRATION ====================

@task(name="demo_incoming_constraints")
def demo_incoming_constraints() -> str:
    print("\n" + "="*60)
    print("="*60)

    authorized_caller()
    orchestrator_caller()   
    unauthorized_caller()
    
    return "demo_completed"


if __name__ == "__main__":
    demo_incoming_constraints()