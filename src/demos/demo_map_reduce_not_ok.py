#!/usr/bin/env python3
import sys
import os

# Add parent directory to path so we can import latch
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import time
from latch.orchestration import task, TaskScheduler
from latch.orchestration.constraints import Constraints


# ==================== DATA SOURCE ====================

@task(name="data_source", constraints=Constraints(allow_outgoing_to_names=["mapper1", "mapper2", "mapper3", "mapper4", "mapper5"]))
def data_source():
    print("[SOURCE] Data source preparing data for mapping...")
    print("[SOURCE] Splitting data into 5 chunks for parallel processing")
    data_source.create_path_to(reducer) # failure case
    time.sleep(2)


# ==================== MAPPERS ====================

@task(name="mapper1", constraints=Constraints(
    allow_incoming_from_names=["data_source"],
    allow_outgoing_to_names=["reducer"]
))
def mapper1():
    print("[MAP1]  Mapper 1 processing data chunk...")
    print("[MAP1]  Applying transformation function to data")
    time.sleep(2)


@task(name="mapper2", constraints=Constraints(
    allow_incoming_from_names=["data_source"],
    allow_outgoing_to_names=["reducer"]
))
def mapper2():
    print("[MAP2]  Mapper 2 processing data chunk...")
    print("[MAP2] Applying transformation function to data")
    time.sleep(2)


@task(name="mapper3", constraints=Constraints(
    allow_incoming_from_names=["data_source"],
    allow_outgoing_to_names=["reducer"]
))
def mapper3():
    print("[MAP3] Mapper 3 processing data chunk...")
    print("[MAP3] Applying transformation function to data")
    time.sleep(2)


@task(name="mapper4", constraints=Constraints(
    allow_incoming_from_names=["data_source"],
    allow_outgoing_to_names=["reducer"]
))
def mapper4():
    print("[MAP4] Mapper 4 processing data chunk...")
    print("[MAP4] pplying transformation function to data")
    time.sleep(2)


@task(name="mapper5", constraints=Constraints(
    allow_incoming_from_names=["data_source"],
    allow_outgoing_to_names=["reducer"]
))
def mapper5():
    print("[MAP5] Mapper 5 processing data chunk...")
    print("[MAP5] Applying transformation function to data")
    time.sleep(2)


# ==================== REDUCER ====================

@task(name="reducer", constraints=Constraints(allow_incoming_from_names=["mapper1", "mapper2", "mapper3", "mapper4", "mapper5"]))
def reducer():
    print("[REDUCE] Reducer combining results from all 5 mappers...")
    print("[REDUCE] Applying reduction function to combine data")
    print("[REDUCE] Map-reduce operation completed successfully")
    time.sleep(2)


# ==================== EXPLICIT PATH RELATIONSHIPS ====================

def setup_task_relationships() -> str:
    print("[SETUP] Creating map-reduce constraint-compliant task relationships...")

    # Data source to mappers (1:N distribution)
    data_source.create_path_to(mapper1)
    data_source.create_path_to(mapper2)
    data_source.create_path_to(mapper3)
    data_source.create_path_to(mapper4)
    data_source.create_path_to(mapper5)
    
    # Mappers to reducer (N:1 collection)
    mapper1.create_path_to(reducer) 
    mapper2.create_path_to(reducer) 
    mapper3.create_path_to(reducer) 
    mapper4.create_path_to(reducer) 
    mapper5.create_path_to(reducer) 

    # Main demo orchestrates the data source directly
    demo_map_reduce.create_path_to(data_source)

    return demo_map_reduce.name


# ==================== DEMONSTRATION ORCHESTRATION ====================

@task(name="demo_map_reduce")
def demo_map_reduce():
    """Demonstrate successful map-reduce pattern with constraint validation."""
    print("\n" + "="*80)
    print("🎯 DEMO: MAP-REDUCE PATTERN WITH CONSTRAINTS (SUCCESS)")
    print("="*80)


if __name__ == "__main__":
    print("\n[MAIN] Setting up map-reduce task relationships...")
    root_task_name = setup_task_relationships()

    # Create scheduler and execute the entire DAG
    scheduler = TaskScheduler()
    scheduler.print_scheduler_status()

    print("\n[MAIN] Executing map-reduce DAG using scheduler loop...")

    results = scheduler.execute_dag()
    print(f"[MAIN] Execution results: {len(results)} tasks completed")
    print(f"[MAIN] Final result: {results.get(root_task_name, 'No result')}")