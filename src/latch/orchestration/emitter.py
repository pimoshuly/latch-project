from __future__ import annotations

from typing import Dict, Any
import requests

# Configuration
VISUALIZATION_SERVER_URL = "http://localhost:8001/api/display"


def emit_dag_json(
    dag_json: Dict[str, Any], server_url: str = VISUALIZATION_SERVER_URL
) -> bool:
    if not dag_json:
        print("Empty DAG data provided, skipping emission")
        return False

    # Add required fields for the visualization server
    enhanced_dag_json = dag_json.copy()

    # Add layout field if missing
    if "layout" not in enhanced_dag_json:
        enhanced_dag_json["layout"] = {
            "algorithm": "hierarchical",
            "direction": "TB",
            "spacing": {"node": 50, "rank": 100},
        }

    try:
        response = requests.post(
            server_url,
            json=enhanced_dag_json,
            headers={"Content-Type": "application/json"},
            timeout=30,
        )

        if response.status_code == 200:
            print(f"Successfully emitted DAG data to {server_url}")
            return True
        else:
            print(
                f"Failed to emit DAG data. Status code: {response.status_code} Server response: {response.text}"
            )
            return False
    except Exception as e:
        print(f"Error while emitting DAG data: {e}")
        return False
