#!/usr/bin/env python3
import sys
import os

# Add parent directory to path so we can import latch
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import time
import random
from latch.orchestration import task, Path, TaskScheduler
from latch.orchestration.constraints import Constraints


# ==================== DATA PROCESSING SERVICES (Incoming Constraints) ====================


@task(
    name="secure_database",
    constraints=Constraints(
        allow_incoming_from_names=["data_service", "backup_service"]
    ),
)
def secure_database():
    print("[DATABASE] Secure database processing request...")
    time.sleep(2)


@task(
    name="analytics_engine",
    constraints=Constraints(
        allow_incoming_from_names=["data_service", "reporting_service"]
    ),
)
def analytics_engine():
    print("[ANALYTICS] Analytics engine processing data...")
    time.sleep(2)


@task(
    name="notification_system",
    constraints=Constraints(
        allow_incoming_from_names=["reporting_service", "alert_service"]
    ),
)
def notification_system():
    print("[NOTIFY] Notification system sending alerts...")
    print("[NOTIFY] Backup connecting to Notification...")
    backup_service.create_path_to(notification_system)

    time.sleep(5)


# ==================== SERVICE ORCHESTRATORS (Outgoing Constraints) ====================


@task(
    name="data_service",
    constraints=Constraints(
        allow_outgoing_to_names=["secure_database", "analytics_engine"]
    ),
)
def data_service():
    print("[DATA] Data service orchestrating...")
    time.sleep(2)


@task(
    name="reporting_service",
    constraints=Constraints(
        allow_outgoing_to_names=["analytics_engine", "notification_system"]
    ),
)
def reporting_service():
    print("[REPORT] Reporting service generating reports...")
    time.sleep(2)


@task(
    name="backup_service",
    constraints=Constraints(allow_outgoing_to_names=["secure_database"]),
)
def backup_service():
    print("[BACKUP] Backup service creating backups...")
    time.sleep(2)


@task(
    name="alert_service",
    constraints=Constraints(allow_outgoing_to_names=["notification_system"]),
)
def alert_service():
    print("[ALERT] Alert service monitoring...")
    time.sleep(2)


# ==================== UNRESTRICTED COORDINATOR ====================


@task(name="system_coordinator")
def system_coordinator():
    print("[COORD] System coordinator managing workflow...")
    time.sleep(2)


# ==================== EXPLICIT PATH RELATIONSHIPS ====================


def setup_task_relationships() -> str:
    print("[SETUP] Creating constraint-compliant task relationships...")

    # Data workflows (satisfies both incoming and outgoing constraints)
    data_service.create_path_to(secure_database)
    data_service.create_path_to(analytics_engine)

    # Reporting workflows
    reporting_service.create_path_to(analytics_engine)
    reporting_service.create_path_to(notification_system)

    # Backup workflows
    backup_service.create_path_to(secure_database)

    # Alert workflows
    alert_service.create_path_to(notification_system)

    # Coordinator orchestrates all services (no constraints on coordinator)
    system_coordinator.create_path_to(data_service)
    system_coordinator.create_path_to(reporting_service)
    system_coordinator.create_path_to(backup_service)
    system_coordinator.create_path_to(alert_service)

    # Main demo orchestrates the coordinator
    demo_combined_constraints.create_path_to(system_coordinator)

    return demo_combined_constraints.name


# ==================== DEMONSTRATION ORCHESTRATION ====================


@task(name="demo_combined_constraints")
def demo_combined_constraints():
    print("\n" + "=" * 80)
    print("DEMO: COMBINED INCOMING & OUTGOING CONSTRAINTS (FAIL)")
    print("=" * 80)


if __name__ == "__main__":
    print("\n[MAIN] Setting up task relationships...")
    root_task_name = setup_task_relationships()

    # Create scheduler and execute the entire DAG
    scheduler = TaskScheduler()
    scheduler.print_scheduler_status()

    print("\n[MAIN] Executing DAG using scheduler loop...")

    results = scheduler.execute_dag()
    print(f"\n[MAIN] Execution completed successfully!")
    print(f"[MAIN] Execution results: {len(results)} tasks completed")
    print(f"[MAIN] Final result: {results.get(root_task_name, 'No result')}")
