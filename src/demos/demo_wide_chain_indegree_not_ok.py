#!/usr/bin/env python3

import sys
import os
import time

# Add parent directory to path so we can import latch
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from latch.orchestration import task, Path, TaskScheduler
from latch.orchestration.constraints import Constraints


# ==================== WIDE CHAIN OF TASKS ====================


@task(name="step1_ingest")
def step1_ingest():
    print(f"[CHAIN] Step 1: Ingesting data from source")
    time.sleep(2)  # Simulate processing time
    last_aggregator()  # Internal call 1


@task(name="step2_validate")
def step2_validate():
    print(f"[CHAIN] Step 2: Validating data")
    time.sleep(2)  # Simulate processing time
    last_aggregator()  # Internal call 2


@task(name="step3_parse")
def step3_parse():
    print(f"[CHAIN] Step 3: Parsing data")
    time.sleep(2)  # Simulate processing time
    last_aggregator()  # Internal call 3


@task(name="step4_clean")
def step4_clean():
    print(f"[CHAIN] Step 4: Cleaning data")
    time.sleep(2)  # Simulate processing time
    last_aggregator()  # Internal call 4


@task(name="step5_enrich")
def step5_enrich():
    print(f"[CHAIN] Step 5: Enriching data")
    time.sleep(2)  # Simulate processing time
    last_aggregator()  # Internal call 5 - This should still work (limit is 5)


@task(name="step6_transform")
def step6_transform():
    print(f"[CHAIN] Step 6: Transforming data")
    time.sleep(4)  # Simulate processing time
    last_aggregator()  # Internal call 6 - This should FAIL due to indegree limit


@task(name="step7_aggregate")
def step7_aggregate():
    print(f"[CHAIN] Step 7: Aggregating data")
    time.sleep(2)  # Simulate processing time
    last_aggregator()  # Internal call 7 - This should also fail


@task(name="step8_score")
def step8_score():
    print(f"[CHAIN] Step 8: Scoring data")
    time.sleep(2)  # Simulate processing time
    last_aggregator()  # Internal call 8 - This should also fail


@task(name="step9_rank")
def step9_rank():
    print(f"[CHAIN] Step 9: Ranking data")
    time.sleep(2)  # Simulate processing time
    last_aggregator()  # Internal call 9 - This should also fail


@task(name="step10_finalize")
def step10_finalize():
    print(f"[CHAIN] Step 10: Finalizing data")
    time.sleep(2)  # Simulate processing time
    last_aggregator()  # Internal call 10 - This should also fail


@task(name="last_aggregator", constraints=Constraints(limit_indegree=5))
def last_aggregator():
    print(f"[CHAIN] Last: Aggregating...")
    time.sleep(2)  # Simulate processing time


# ==================== EXPLICIT PATH RELATIONSHIPS ====================


def setup_task_relationships() -> str:
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


@task(name="demo_wide_chain")
def demo_wide_chain():
    print("\n" + "=" * 60)
    print("DEMO: WIDE CHAIN PROCESSING WITH INDEGREE CONSTRAINT")
    print("=" * 60)


if __name__ == "__main__":
    root_task_name = setup_task_relationships()

    # Create scheduler and execute the entire DAG
    scheduler = TaskScheduler()
    scheduler.print_scheduler_status()

    print("\n[MAIN] Executing DAG using scheduler loop...")

    results = scheduler.execute_dag()
    print(f"\n[MAIN] Execution results: {len(results)} tasks completed")
