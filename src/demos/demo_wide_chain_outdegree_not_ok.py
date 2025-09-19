#!/usr/bin/env python3
"""
Demo 1B: Wide Chain Processing Pattern

This demo showcases a wide chain where a single orchestrator function
calls multiple sequential tasks in order, but the tasks don't call each other.

Constrains: outdgree
"""

import sys
import os
import time

# Add parent directory to path so we can import latch
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from latch.orchestration import task, Path, TaskScheduler
from latch.orchestration.constraints import Constraints


# ==================== WIDE CHAIN OF TASKS ====================
# Sequential processing pipeline with 10+ steps called by orchestrator


@task(name="step1_ingest")
def step1_ingest():
    """Step 1: Data ingestion from source."""
    print(f"[CHAIN] Step 1: Ingesting data from source")
    time.sleep(2)  # Simulate processing time
    last_aggregator()  # Internal call 1


@task(name="step2_validate")
def step2_validate():
    """Step 2: Validate incoming data."""
    print(f"[CHAIN] Step 2: Validating data")
    time.sleep(2)  # Simulate processing time
    last_aggregator()  # Internal call 2


@task(name="step3_parse")
def step3_parse():
    """Step 3: Parse data structure."""
    print(f"[CHAIN] Step 3: Parsing data")
    time.sleep(2)  # Simulate processing time
    last_aggregator()  # Internal call 3


@task(name="step4_clean")
def step4_clean():
    """Step 4: Clean and normalize data."""
    print(f"[CHAIN] Step 4: Cleaning data")
    time.sleep(2)  # Simulate processing time
    last_aggregator()  # Internal call 4


@task(name="step5_enrich")
def step5_enrich():
    """Step 5: Enrich with external data."""
    print(f"[CHAIN] Step 5: Enriching data")
    time.sleep(2)  # Simulate processing time
    last_aggregator()  # Internal call 5 - This should still work (limit is 5)


@task(name="step6_transform")
def step6_transform():
    """Step 6: Apply business transformations."""
    print(f"[CHAIN] Step 6: Transforming data")
    time.sleep(4)  # Simulate processing time
    last_aggregator()  # Internal call 6 - This should FAIL due to outdegree limit


@task(name="step7_aggregate")
def step7_aggregate():
    """Step 7: Aggregate data by key dimensions."""
    print(f"[CHAIN] Step 7: Aggregating data")
    time.sleep(2)  # Simulate processing time
    last_aggregator()  # Internal call 7 - This should also fail


@task(name="step8_score")
def step8_score():
    """Step 8: Calculate quality scores."""
    print(f"[CHAIN] Step 8: Scoring data")
    time.sleep(2)  # Simulate processing time
    last_aggregator()  # Internal call 8 - This should also fail


@task(name="step9_rank")
def step9_rank():
    """Step 9: Rank and prioritize results."""
    print(f"[CHAIN] Step 9: Ranking data")
    time.sleep(2)  # Simulate processing time
    last_aggregator()  # Internal call 9 - This should also fail


@task(name="step10_finalize")
def step10_finalize():
    """Step 10: Finalize processing pipeline."""
    print(f"[CHAIN] Step 10: Finalizing data")
    time.sleep(2)  # Simulate processing time
    last_aggregator()  # Internal call 10 - This should also fail


@task(name="last_aggregator")
def last_aggregator():
    """Last: Aggregate the processing artifacts."""
    print(f"[CHAIN] Last: Aggregating...")
    time.sleep(2)  # Simulate processing time


# ==================== EXPLICIT PATH RELATIONSHIPS ====================


def setup_task_relationships() -> str:
    """Setup explicit caller-callee relationships using Path construct.

    Returns:
        str: The unique name of the root task in the graph
    """

    # Each step task calls last_aggregator
    step1_ingest.create_path_to(last_aggregator)
    step2_validate.create_path_to(last_aggregator)
    step3_parse.create_path_to(last_aggregator)
    step4_clean.create_path_to(last_aggregator)
    step5_enrich.create_path_to(last_aggregator)
    step6_transform.create_path_to(last_aggregator)
    step7_aggregate.create_path_to(last_aggregator)
    step8_score.create_path_to(last_aggregator)
    step9_rank.create_path_to(last_aggregator)
    step10_finalize.create_path_to(last_aggregator)

    # Orchestrator calls all step tasks
    demo_wide_chain.create_path_to(step1_ingest)
    demo_wide_chain.create_path_to(step2_validate)
    demo_wide_chain.create_path_to(step3_parse)
    demo_wide_chain.create_path_to(step4_clean)
    demo_wide_chain.create_path_to(step5_enrich)
    demo_wide_chain.create_path_to(step6_transform)
    demo_wide_chain.create_path_to(step7_aggregate)
    demo_wide_chain.create_path_to(step8_score)
    demo_wide_chain.create_path_to(step9_rank)
    demo_wide_chain.create_path_to(step10_finalize)

    return demo_wide_chain.name


# ==================== DEMONSTRATION ORCHESTRATION ====================


@task(name="demo_wide_chain", constraints=Constraints(limit_outdegree=5))
def demo_wide_chain():
    """Demonstrate wide chain with outdegree constraint - validates during direct calls."""
    print("\n" + "=" * 60)
    print("DEMO: WIDE CHAIN PROCESSING WITH OUTDEGREE CONSTRAINT")
    print("=" * 60)
    print("NOTE: Outdegree constraints are enforced during direct task calls.")
    print("With scheduler-driven execution, there are no direct calls to validate.")
    print("This demo will complete all tasks successfully via scheduler.")
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
