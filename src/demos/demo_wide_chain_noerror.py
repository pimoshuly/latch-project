#!/usr/bin/env python3
"""
Demo 1A: Wide Chain Processing Pattern

This demo showcases a wide chain where a single orchestrator function
calls multiple sequential tasks in order, but the tasks don't call each other.
"""

import sys
import os
import time
# Add parent directory to path so we can import latch
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from latch.orchestration import task


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


@task(name="last_aggregator")
def last_aggregator(data: str) -> str:
    """Last: Aggregate the processing artifacts."""
    print(f"[CHAIN] Last: Aggregating...")
    time.sleep(1)  # Simulate processing time
    return f"last_{data}"

# ==================== DEMONSTRATION ORCHESTRATION ====================

@task(name="demo_wide_chain")
def demo_wide_chain(source_path: str = "/data/input.csv") -> str:
    """Demonstrate wide chain of sequential tasks called by orchestrator."""
    print("\n" + "="*60)
    print("DEMO 1: WIDE CHAIN PROCESSING")
    print("="*60)

    # Execute the 10-step chain
    data = step1_ingest(source_path)
    data = step2_validate(data)
    data = step3_parse(data)
    data = step4_clean(data)
    data = step5_enrich(data)
    data = step6_transform(data)
    data = step7_aggregate(data)
    data = step8_score(data)
    data = step9_rank(data)
    data = step10_finalize(data)

    print(f"[CHAIN] Final result: {data}")
    return data


if __name__ == "__main__":
    demo_wide_chain()