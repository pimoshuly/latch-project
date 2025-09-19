#!/usr/bin/env python3

import sys
import os
import time
# Add parent directory to path so we can import latch
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from latch.orchestration import task
from latch.orchestration.constraints import Constraints


# ==================== WIDE CHAIN OF TASKS ====================
# Sequential processing pipeline with 10+ steps called by orchestrator

@task(name="step1_ingest")
def step1_ingest(source_path: str) -> str:
    """Step 1: Data ingestion from source."""
    print(f"[CHAIN] Step 1: Ingesting data from {source_path}")
    time.sleep(1)  # Simulate processing time
    result = f"raw_data_from_{source_path}"
    last_aggregator(result)  # Internal call 1
    return result


@task(name="step2_validate")
def step2_validate(data: str) -> str:
    """Step 2: Validate incoming data."""
    print(f"[CHAIN] Step 2: Validating data: {data[:20]}...")
    time.sleep(1)  # Simulate processing time
    result = f"validated_{data}"
    last_aggregator(result)  # Internal call 2
    return result


@task(name="step3_parse")
def step3_parse(data: str) -> str:
    """Step 3: Parse data structure."""
    print(f"[CHAIN] Step 3: Parsing data: {data[:20]}...")
    time.sleep(2)  # Simulate processing time
    result = f"parsed_{data}"
    last_aggregator(result)  # Internal call 3
    return result


@task(name="step4_clean")
def step4_clean(data: str) -> str:
    """Step 4: Clean and normalize data."""
    print(f"[CHAIN] Step 4: Cleaning data: {data[:20]}...")
    time.sleep(1)  # Simulate processing time
    result = f"cleaned_{data}"
    last_aggregator(result)  # Internal call 4
    return result


@task(name="step5_enrich")
def step5_enrich(data: str) -> str:
    """Step 5: Enrich with external data."""
    print(f"[CHAIN] Step 5: Enriching data: {data[:20]}...")
    time.sleep(1)  # Simulate processing time
    result = f"enriched_{data}"
    last_aggregator(result)  # Internal call 5 - This should still work (limit is 5)
    return result


@task(name="step6_transform")
def step6_transform(data: str) -> str:
    """Step 6: Apply business transformations."""
    print(f"[CHAIN] Step 6: Transforming data: {data[:20]}...")
    time.sleep(4)  # Simulate processing time
    result = f"transformed_{data}"
    last_aggregator(result)  # Internal call 6 - This should FAIL due to indegree limit
    return result


@task(name="step7_aggregate")
def step7_aggregate(data: str) -> str:
    """Step 7: Aggregate data by key dimensions."""
    print(f"[CHAIN] Step 7: Aggregating data: {data[:20]}...")
    time.sleep(1)  # Simulate processing time
    result = f"aggregated_{data}"
    last_aggregator(result)  # Internal call 7 - This should also fail
    return result


@task(name="step8_score")
def step8_score(data: str) -> str:
    """Step 8: Calculate quality scores."""
    print(f"[CHAIN] Step 8: Scoring data: {data[:20]}...")
    time.sleep(1)  # Simulate processing time
    result = f"scored_{data}"
    last_aggregator(result)  # Internal call 8 - This should also fail
    return result


@task(name="step9_rank")
def step9_rank(data: str) -> str:
    """Step 9: Rank and prioritize results."""
    print(f"[CHAIN] Step 9: Ranking data: {data[:20]}...")
    time.sleep(1)  # Simulate processing time
    result = f"ranked_{data}"
    last_aggregator(result)  # Internal call 9 - This should also fail
    return result


@task(name="step10_finalize")
def step10_finalize(data: str) -> str:
    """Step 10: Finalize processing pipeline."""
    print(f"[CHAIN] Step 10: Finalizing data: {data[:20]}...")
    time.sleep(1)  # Simulate processing time
    result = f"final_{data}"
    last_aggregator(result)  # Internal call 10 - This should also fail
    return result


@task(name="last_aggregator", constraints=Constraints(limit_indegree=5))
def last_aggregator(data: str) -> str:
    """Last: Aggregate the processing artifacts."""
    print(f"[CHAIN] Last: Aggregating...")
    time.sleep(1)  # Simulate processing time
    return f"last_{data}"



# ==================== DEMONSTRATION ORCHESTRATION ====================

@task(name="demo_wide_chain")
def demo_wide_chain_indegree() -> str:
    """Demonstrate wide chain with each step internally calling last_aggregator (indegree constraint test)."""
    print("\n" + "="*60)
    print("DEMO 1: WIDE CHAIN PROCESSING WITH INDEGREE CONSTRAINT")
    print("="*60)

    # Execute the 10-step chain, each step will internally call last_aggregator
    data = step1_ingest(source_path = "/data/input.csv")
    data = step2_validate(data)
    data = step3_parse(data)
    data = step4_clean(data)
    data = step5_enrich(data)  # Internal call 5 - This should still work (limit is 5)
    data = step6_transform(data)  # Internal call 6 - This should FAIL due to indegree limit
    data = step7_aggregate(data)  # Internal call 7 - This should also fail
    data = step8_score(data)  # Internal call 8 - This should also fail
    data = step9_rank(data)  # Internal call 9 - This should also fail
    data = step10_finalize(data)  # Internal call 10 - This should also fail

    print(f"[CHAIN] Final result: {data}")
    return data


if __name__ == "__main__":
    demo_wide_chain_indegree()