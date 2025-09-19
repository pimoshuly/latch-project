#!/usr/bin/env python3
import sys
import os

# Add parent directory to path so we can import latch
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import time
import datetime
import random
from latch.orchestration import task
from latch.orchestration.constraints import Constraints


# ==================== ASSESSMENT ====================

@task(name="assess_data")
def assess_data(input_data: str) -> str:
    print(f"[ASSESSMENT] Assessing data: {input_data}")
    time.sleep(1)

    random.seed(datetime.datetime.now().timestamp())
    data_size = random.randint(0, 20)
    assessment_result = "large" if data_size > 10 else "small"

    print(f"[ASSESSMENT] Assessment result: {assessment_result}")
    return assessment_result


# ==================== PROCESSING TASKS ====================

@task(name="process_small")
def process_small(assessed_data: str) -> str:
    print(f"[PROCESS] Processing small data: {assessed_data}")
    time.sleep(2)
    return f"small_processed_{assessed_data}"


@task(name="process_big")
def process_big(assessed_data: str) -> str:
    print(f"[PROCESS] Processing big data: {assessed_data}")
    time.sleep(2)
    return f"big_processed_{assessed_data}"


# ==================== ORCHESTRATOR WITH CONSTRAINTS ====================

@task(name="restricted_orchestrator", constraints=Constraints(allow_outgoing_to_names=["assess_data", "process_small"]))
def restricted_orchestrator(input_data: str = "test_data") -> str:
    print("\n" + "="*60)
    print("="*60)

    assessment = assess_data(input_data=input_data)

    if assessment == "small":
        print("  Assessment is 'small' - calling process_small (should succeed - in allow list):")
        result = process_small(assessment)
        return result
    else:
        print("  Assessment is 'large' - calling process_big (should fail - NOT in allow list):")
        result = process_big(assessment)
        return result


@task(name="unrestricted_orchestrator")
def unrestricted_orchestrator(input_data: str = "test_data") -> str:
    print("\n" + "="*60)

    assessment = assess_data(input_data)
    if assessment == "small":
        result = process_small(assessment)
        return result
    else:
        result = process_big(assessment)
        return result


# ==================== DEMONSTRATION ====================

@task(name="demo_outgoing_constraints")
def demo_outgoing_constraints() -> str:
    """Demonstrate outgoing constraint validation."""
    print("\n" + "="*80)
    print("="*80)

    restricted_orchestrator("test_data_1")
    unrestricted_orchestrator("test_data_2")
    return "demo_completed"


if __name__ == "__main__":
    demo_outgoing_constraints()