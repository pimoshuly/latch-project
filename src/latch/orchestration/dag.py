from __future__ import annotations

from typing import Dict, Set, List, Any
from dataclasses import dataclass
from collections import deque

from datetime import datetime


@dataclass
class DAGNode:
    task_name: str
    dependencies: Set[str] = None
    dependents: Set[str] = None

    def __post_init__(self):
        if self.dependencies is None:
            self.dependencies = set()
        if self.dependents is None:
            self.dependents = set()


class TaskDependencyDAG:
    def __init__(self):
        self.nodes: Dict[str, DAGNode] = {}

    def add_task(self, task_name: str) -> DAGNode:
        """Add a task node to the DAG if it doesn't exist."""
        if task_name not in self.nodes:
            self.nodes[task_name] = DAGNode(task_name=task_name)
        return self.nodes[task_name]

    def add_dependency(self, dependent_task: str, dependency_task: str) -> None:
        dep_node = self.add_task(dependent_task)
        prereq_node = self.add_task(dependency_task)

        dep_node.dependencies.add(dependency_task)
        prereq_node.dependents.add(dependent_task)

    def topological_sort(self) -> List[str]:
        if not self.nodes:
            return []

        # Calculate in-degrees for all nodes
        in_degrees = {}
        for task_name in self.nodes:
            in_degrees[task_name] = len(self.nodes[task_name].dependencies)

        # Initialize queue with nodes that have no dependencies
        queue = deque([task for task, degree in in_degrees.items() if degree == 0])
        result = []

        while queue:
            current = queue.popleft()
            result.append(current)

            # Process all dependents of current node
            for dependent in self.nodes[current].dependents:
                in_degrees[dependent] -= 1
                if in_degrees[dependent] == 0:
                    queue.append(dependent)

        # Check for cycles
        if len(result) != len(self.nodes):
            remaining_nodes = [node for node in self.nodes if node not in result]
            print(
                f"[WARNING] Cycle detected in DAG. Remaining nodes: {remaining_nodes}"
            )
            # Return partial result with remaining nodes appended
            result.extend(remaining_nodes)

        return result

    def to_json(self) -> Dict[str, Any]:
        topological_order = self.topological_sort()

        nodes = []
        for position, task_name in enumerate(topological_order):
            dag_node = self.nodes[task_name]
            node_data = {
                "id": task_name,
                "label": task_name,
                "type": "task",
                "dependencies_count": len(dag_node.dependencies),
                "dependents_count": len(dag_node.dependents),
                "topological_position": position,
            }

            nodes.append(node_data)

        # Build edges array
        edges = []
        edge_id = 0
        for task_name, dag_node in self.nodes.items():
            for dependency in dag_node.dependencies:
                edge_data = {
                    "id": f"edge_{edge_id}",
                    "source": dependency,  # Source task (dependency)
                    "target": task_name,  # Target task (dependent)
                    "type": "dependency",
                    "label": "depends_on",
                }
                edges.append(edge_data)
                edge_id += 1

        # DAG metadata
        metadata = {
            "total_nodes": len(self.nodes),
            "total_edges": len(edges),
            "topological_order": topological_order,
        }

        return {
            "nodes": nodes,
            "edges": edges,
            "metadata": metadata,
            "generated_at": datetime.now().isoformat(),
        }

    def print_dag(self) -> None:
        print(f"[DAG]")
        print(f"[DAG]   Nodes: {len(self.nodes)}")

        if self.nodes:
            for node_name, node in self.nodes.items():
                if node.dependents:
                    dependent_names = []
                    for dep in node.dependents:
                        dependent_names.append(dep)
                    print(f"[DAG]   {node_name} -> {', '.join(dependent_names)}")
                else:
                    print(f"[DAG]   {node_name} (no dependencies)")
        else:
            print(f"[DAG]   No nodes in the graph")
