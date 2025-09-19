#!/usr/bin/env python3
import time
from typing import Dict, Any, Optional
import networkx as nx
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
import plotly.graph_objects as go

# Global storage for DAG data posted from client
stored_execution_state: Optional[Dict[str, Any]] = None
last_update_time: float = 0

app = FastAPI(
    title="DAG Visualizer", description="Visualize DAG structures using NetworkX"
)


class DAGData(BaseModel):
    """Model for DAG JSON data."""

    nodes: list
    edges: list
    metadata: dict
    layout: dict
    dag_type: Optional[str] = None
    title: Optional[str] = None
    generated_at: Optional[str] = None


class DAGNetworkXVisualizer:
    """Visualize DAG using NetworkX and matplotlib."""

    def __init__(self, dag_json: Dict[str, Any]):
        """Initialize visualizer with DAG JSON data."""
        self.dag_json = dag_json
        self.graph = nx.DiGraph()
        self.node_colors = {}
        self.node_sizes = {}

        # Status-based node colors
        self.status_colors = {
            "pending": "#95a5a6",  # Gray
            "running": "#f39c12",  # Orange
            "completed": "#27ae60",  # Green
            "failed": "#e74c3c",  # Red
        }
        self.default_node_color = "#3498db"  # Blue (fallback)

        self._build_graph()

    def _build_graph(self):
        """Build NetworkX graph from DAG JSON."""
        # Filter option: skip isolated nodes (no dependencies and no dependents)
        skip_isolated = self.dag_json.get("metadata", {}).get(
            "skip_isolated_nodes", False
        )

        # Add nodes with attributes
        for node_data in self.dag_json["nodes"]:
            node_id = node_data["id"]

            # Check if this is an isolated node and should be skipped
            dependencies_count = node_data.get("dependencies_count", 0)
            dependents_count = node_data.get("dependents_count", 0)
            is_isolated = dependencies_count == 0 and dependents_count == 0

            # Check if node has execution state (started, running, failed, completed)
            status = node_data.get("status", "pending")
            has_execution_state = status in ["running", "completed", "failed"]

            if skip_isolated and is_isolated and not has_execution_state:
                print(
                    f"[VISUALIZER] Skipping isolated node without execution state: {node_id} (status: {status})"
                )
                continue
            elif skip_isolated and is_isolated and has_execution_state:
                print(
                    f"[VISUALIZER] Keeping isolated node with execution state: {node_id} (status: {status})"
                )

            self.graph.add_node(node_id, **node_data)

            # Set node color based on status
            status = node_data.get("status", "pending")
            color = self.status_colors.get(status, self.default_node_color)
            self.node_colors[node_id] = color
            print(f"[VISUALIZER] Node {node_id}: status={status}, color={color}")

            # Determine node size based on connectivity and text length (larger for full page)
            total_connections = dependencies_count + dependents_count
            text_length = len(node_id)
            base_size = 2000  # Larger base size for full page display
            size_for_text = max(
                text_length * 60, 800
            )  # Minimum size based on text length
            self.node_sizes[node_id] = max(
                base_size + (total_connections * 250), size_for_text
            )

        # Add edges with attributes
        for edge_data in self.dag_json["edges"]:
            source = edge_data["source"]
            target = edge_data["target"]
            self.graph.add_edge(source, target, **edge_data)

    def get_hierarchical_layout(self) -> Dict[str, Any]:
        """Create hierarchical layout based on dependency relationships."""
        pos = {}

        # Get all nodes and sort by topological position
        nodes_with_position = []
        for node in self.graph.nodes():
            topo_pos = self.graph.nodes[node].get("topological_position", 0)
            nodes_with_position.append((node, topo_pos))

        nodes_with_position.sort(key=lambda x: x[1])  # Sort by topological position

        # If no topological positions are set, fall back to spring layout
        if not nodes_with_position or all(pos == 0 for _, pos in nodes_with_position):
            return nx.spring_layout(self.graph, k=3, iterations=50)

        # Layout parameters
        x_spacing = 8.0
        y_spacing = 3.0

        # Track which nodes have been positioned
        positioned_nodes = set()
        current_x = 0

        # Start with nodes that have no predecessors (topological position 0 or minimal)
        root_nodes = [
            node
            for node, topo_pos in nodes_with_position
            if topo_pos == min(pos for _, pos in nodes_with_position)
        ]

        # Position root nodes vertically at x=0
        if root_nodes:
            self._position_nodes_vertically(pos, root_nodes, current_x, y_spacing)
            positioned_nodes.update(root_nodes)
            current_x += x_spacing

        # Process nodes level by level based on dependencies
        while len(positioned_nodes) < len(self.graph.nodes()):
            next_level_nodes = []

            # Find nodes whose dependencies are all positioned
            for node in self.graph.nodes():
                if node in positioned_nodes:
                    continue

                # Check if all predecessors (dependencies) are positioned
                predecessors = list(self.graph.predecessors(node))
                if (
                    not predecessors
                ):  # No dependencies - should have been handled as root
                    continue

                if all(pred in positioned_nodes for pred in predecessors):
                    next_level_nodes.append(node)

            # If we found nodes to position at this level
            if next_level_nodes:
                self._position_nodes_vertically(
                    pos, next_level_nodes, current_x, y_spacing
                )
                positioned_nodes.update(next_level_nodes)
                current_x += x_spacing
            else:
                # Handle any remaining unpositioned nodes (shouldn't happen in well-formed DAG)
                remaining_nodes = [
                    node for node in self.graph.nodes() if node not in positioned_nodes
                ]
                if remaining_nodes:
                    self._position_nodes_vertically(
                        pos, remaining_nodes, current_x, y_spacing
                    )
                    positioned_nodes.update(remaining_nodes)
                break

        return pos

    def _position_nodes_vertically(
        self, pos: dict, nodes: list, x: float, y_spacing: float
    ):
        """Position a list of nodes vertically at the given x coordinate."""
        num_nodes = len(nodes)

        if num_nodes == 1:
            pos[nodes[0]] = (x, 0)
        elif num_nodes == 2:
            pos[nodes[0]] = (x, -y_spacing / 2)
            pos[nodes[1]] = (x, y_spacing / 2)
        else:
            # Distribute nodes evenly around center
            y_range = (num_nodes - 1) * y_spacing
            y_start = -y_range / 2

            for i, node in enumerate(nodes):
                y = y_start + i * y_spacing
                pos[node] = (x, y)

    def generate_plotly_html(self, layout: str = "hierarchical") -> str:
        """Generate interactive Plotly visualization and return as HTML string."""

        # Choose layout algorithm
        if layout == "hierarchical":
            pos = self.get_hierarchical_layout()
        elif layout == "spring":
            pos = nx.spring_layout(self.graph, k=3, iterations=50)
        elif layout == "circular":
            pos = nx.circular_layout(self.graph)
        else:
            pos = nx.spring_layout(self.graph)

        # Create figure
        fig = go.Figure()

        # Add edges with arrows
        edge_x, edge_y = [], []
        edge_info = []
        annotations = []

        # Node size for edge intersection calculation (matching the marker size)
        node_size = 60
        # Convert from plotly marker size to actual coordinate space
        # Plotly marker size is in "points", need to estimate coordinate space equivalent
        # Increased boundary size to ensure visible separation from node centers
        node_half_width = (
            0.2  # Approximate half-width of square nodes in coordinate space
        )
        node_half_height = (
            0.15  # Approximate half-height of square nodes in coordinate space
        )

        def calculate_edge_endpoints(x0, y0, x1, y1):
            """Calculate edge start and end points at the boundaries of square nodes."""
            import math

            # Calculate direction vector
            dx = x1 - x0
            dy = y1 - y0

            if dx == 0 and dy == 0:
                return x0, y0, x1, y1

            # Calculate intersection with source node boundary
            if abs(dx) > abs(dy):
                # Line is more horizontal, intersect with left/right edge
                if dx > 0:
                    start_x = x0 + node_half_width
                    start_y = y0 + (node_half_width * dy / dx)
                else:
                    start_x = x0 - node_half_width
                    start_y = y0 - (node_half_width * dy / dx)
            else:
                # Line is more vertical, intersect with top/bottom edge
                if dy > 0:
                    start_y = y0 + node_half_height
                    start_x = x0 + (node_half_height * dx / dy)
                else:
                    start_y = y0 - node_half_height
                    start_x = x0 - (node_half_height * dx / dy)

            # Calculate intersection with target node boundary
            if abs(dx) > abs(dy):
                # Line is more horizontal, intersect with left/right edge
                if dx > 0:
                    end_x = x1 - node_half_width
                    end_y = y1 - (node_half_width * dy / dx)
                else:
                    end_x = x1 + node_half_width
                    end_y = y1 + (node_half_width * dy / dx)
            else:
                # Line is more vertical, intersect with top/bottom edge
                if dy > 0:
                    end_y = y1 - node_half_height
                    end_x = x1 - (node_half_height * dx / dy)
                else:
                    end_y = y1 + node_half_height
                    end_x = x1 + (node_half_height * dx / dy)

            return start_x, start_y, end_x, end_y

        for edge in self.graph.edges():
            x0, y0 = pos[edge[0]]
            x1, y1 = pos[edge[1]]

            # Calculate edge endpoints at node boundaries
            start_x, start_y, end_x, end_y = calculate_edge_endpoints(x0, y0, x1, y1)

            # Add edge line from boundary to boundary (using calculated boundary points)
            edge_x.extend([start_x, end_x, None])
            edge_y.extend([start_y, end_y, None])

            # Store edge info for hover
            edge_data = self.graph[edge[0]][edge[1]]
            edge_info.append(
                f"{edge[0]} -> {edge[1]}<br>Type: {edge_data.get('type', 'dependency')}"
            )

            # Determine edge color and style based on edge type
            edge_color = (
                "#ff4444" if edge_data.get("type") == "violation" else "#7f8c8d"
            )
            edge_width = 4 if edge_data.get("type") == "violation" else 2

            # Calculate middle point of the edge for text placement (using boundary points)
            mid_x = (start_x + end_x) / 2
            mid_y = (start_y + end_y) / 2

            # Offset the text position slightly above the middle of the edge
            text_offset_y = (
                abs(end_y - start_y) * 0.1 + 0.05
            )  # Adaptive offset based on edge length
            text_y = mid_y + text_offset_y

            # Add arrow annotation for this edge (from boundary to boundary)
            annotations.append(
                dict(
                    ax=start_x,
                    ay=start_y,  # Start point at node boundary
                    x=end_x,
                    y=end_y,  # End point at node boundary
                    xref="x",
                    yref="y",
                    axref="x",
                    ayref="y",
                    arrowhead=2,  # Arrow style
                    arrowsize=1.5,
                    arrowwidth=edge_width,
                    arrowcolor=edge_color,
                    showarrow=True,
                    text="",  # Remove text from arrow itself
                )
            )

            # Add separate text annotation at middle top of edge for violation details
            if edge_data.get("type") == "violation" and edge_data.get("hover_info"):
                annotations.append(
                    dict(
                        x=mid_x,
                        y=text_y,  # Position at middle top of edge
                        xref="x",
                        yref="y",
                        text=edge_data.get("label", "❌ VIOLATION"),
                        showarrow=False,
                        font=dict(size=10, color="white"),
                        bgcolor="rgba(255, 68, 68, 0.9)",  # Red background
                        bordercolor="#ff4444",
                        borderwidth=2,
                        borderpad=4,
                        hovertext=edge_data.get("hover_info", ""),
                        hoverlabel=dict(
                            bgcolor="rgba(255, 255, 255, 0.95)",
                            bordercolor="#ff4444",
                            font=dict(size=12, color="black"),
                        ),
                    )
                )

        # Add edge traces (lighter lines behind arrows)
        fig.add_trace(
            go.Scatter(
                x=edge_x,
                y=edge_y,
                mode="lines",
                line=dict(width=1, color="rgba(127, 140, 157, 0.3)"),
                hoverinfo="none",
                showlegend=False,
                name="Edge Lines",
            )
        )

        # Add nodes as rectangles using shapes and text
        node_x = []
        node_y = []
        node_text = []
        node_colors = []
        node_info = []

        for node in self.graph.nodes():
            x, y = pos[node]
            node_x.append(x)
            node_y.append(y)

            # Format node text
            label = str(node)
            if len(label) > 15:
                words = label.split("_")
                if len(words) > 1:
                    # Smart line breaking
                    lines = []
                    current_line = []
                    current_length = 0

                    for word in words:
                        if current_length + len(word) + 1 > 15 and current_line:
                            lines.append("_".join(current_line))
                            current_line = [word]
                            current_length = len(word)
                        else:
                            current_line.append(word)
                            current_length += len(word) + (1 if current_line else 0)

                    if current_line:
                        lines.append("_".join(current_line))

                    display_text = "<br>".join(lines)
                else:
                    display_text = label
            else:
                display_text = label

            node_text.append(display_text)

            # Get node color based on status
            color = self.node_colors.get(node, self.default_node_color)
            node_colors.append(color)

            # Hover info
            node_data = self.graph.nodes[node]
            hover_text = f"<b>{node}</b><br>"

            # Add status information
            status = node_data.get("status", "pending")
            status_text = {
                "pending": "[PENDING]",
                "running": "[RUNNING]",
                "completed": "[COMPLETED]",
                "failed": "[FAILED]",
            }.get(status, "[UNKNOWN]")
            hover_text += f"Status: {status_text} {status.title()}<br>"

            # Add error information for failed tasks
            if status == "failed" and "error" in node_data:
                error_msg = node_data["error"]
                if len(error_msg) > 50:
                    error_msg = error_msg[:47] + "..."
                hover_text += f"Error: {error_msg}<br>"

            hover_text += f"Order: {node_data.get('topological_position', 0)}<br>"
            hover_text += f"Dependencies: {node_data.get('dependencies_count', 0)}<br>"
            hover_text += f"Dependents: {node_data.get('dependents_count', 0)}"

            node_info.append(hover_text)

        # Add nodes as scatter plot with rectangles (larger for full page)
        fig.add_trace(
            go.Scatter(
                x=node_x,
                y=node_y,
                mode="markers+text",
                marker=dict(
                    size=60,  # Increased size for full page display
                    color=node_colors,
                    symbol="square",
                    line=dict(width=3, color="black"),  # Thicker border
                ),
                text=node_text,
                textposition="middle center",
                textfont=dict(
                    size=12, color="black", family="Arial Black"
                ),  # Larger text
                hovertemplate="%{customdata}<extra></extra>",
                customdata=node_info,
                showlegend=False,
                name="Tasks",
            )
        )

        # Update layout for full page display
        metadata = self.dag_json.get("metadata", {})
        dag_title = self.dag_json.get("title", "DAG Visualization")
        fig.update_layout(
            title=dict(text=f"{dag_title}", x=0.5, font=dict(size=20)),
            showlegend=True,
            legend=dict(yanchor="top", y=0.98, xanchor="right", x=0.98),
            width=1600,  # Increased width for full page
            height=1000,  # Increased height for better vertical space
            margin=dict(l=50, r=50, t=100, b=50),  # Reduced right margin, increased top
            xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
            yaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
            plot_bgcolor="white",
            autosize=True,  # Allow responsive sizing
            annotations=annotations,  # Add arrow annotations
        )

        # Return as HTML string
        return fig.to_html(
            include_plotlyjs=True,
            div_id="dag-visualization",
            config={
                "displayModeBar": True,
                "displaylogo": False,
                "modeBarButtonsToRemove": ["pan2d", "lasso2d", "select2d"],
                "toImageButtonOptions": {
                    "format": "png",
                    "filename": "dag_visualization",
                    "height": 800,
                    "width": 1200,
                    "scale": 2,
                },
            },
        )


@app.post("/api/display")
async def store_dag_data(dag_data: DAGData):
    """Store DAG JSON data in memory for visualization."""
    global stored_execution_state, last_update_time

    try:
        dag_dict = dag_data.model_dump()

        # Store the DAG data
        stored_execution_state = dag_dict
        last_update_time = time.time()

        return {
            "status": "success",
            "message": f"DAG data stored successfully",
            "nodes_count": len(dag_dict["nodes"]),
            "edges_count": len(dag_dict["edges"]),
            "timestamp": last_update_time,
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error storing DAG data: {str(e)}")


@app.get("/api/status")
async def get_status():
    """Get current data status for auto-refresh checking."""
    global stored_execution_state, last_update_time

    has_state = stored_execution_state is not None

    result = {
        "has_data": has_state,
        "has_execution_state": has_state,
        "timestamp": last_update_time,
        "state_nodes": len(stored_execution_state.get("nodes", [])) if has_state else 0,
        "state_edges": len(stored_execution_state.get("edges", [])) if has_state else 0,
    }

    if stored_execution_state:
        result.update(stored_execution_state)

    return result


@app.get("/api/display", response_class=HTMLResponse)
async def display_dag():
    """Return HTML page with DAG visualization."""
    global stored_execution_state

    has_state = stored_execution_state is not None

    if not has_state:
        return HTMLResponse(
            """
        <html>
            <head><title>DAG Visualizer</title></head>
            <body>
                <h1>DAG Visualizer</h1>
                <p>No DAG data stored. Please run your tasks to generate DAG data.</p>
                <p>The orchestration system will automatically send DAG data to this server.</p>
            </body>
        </html>
        """
        )

    try:
        # Generate visualization based on available data
        visualizer = DAGNetworkXVisualizer(stored_execution_state)
        plotly_html = visualizer.generate_plotly_html()
        state_nodes = len(stored_execution_state.get("nodes", []))
        state_edges = len(stored_execution_state.get("edges", []))

        # Extract execution history and active tasks from metadata
        metadata = stored_execution_state.get("metadata", {})
        execution_history = metadata.get("execution_history", [])

        current_timestamp = last_update_time

        page_title = stored_execution_state.get("title", "DAG Visualization")
        header_title = page_title

        # Format execution history for display (show last 10 events)
        history_html = ""
        recent_history = (
            execution_history[-10:]
            if len(execution_history) > 10
            else execution_history
        )
        for event in reversed(recent_history):  # Show most recent first
            task_name = event.get("task", "Unknown")
            display_name = task_name.split("_")[0] if "_" in task_name else task_name
            event_type = event.get("event", "unknown")

            # Handle different timestamp formats
            start_time = event.get("start_time", "")
            end_time = event.get("end_time", "")
            timestamp = event.get("timestamp", "")

            time_info = ""
            if event_type in ["completed", "failed"] and start_time and end_time:
                # For completed/failed tasks, show start-end time and duration
                try:
                    from datetime import datetime

                    start_dt = datetime.fromisoformat(start_time.replace("Z", "+00:00"))
                    end_dt = datetime.fromisoformat(end_time.replace("Z", "+00:00"))

                    start_str = start_dt.strftime("%H:%M:%S")
                    end_str = end_dt.strftime("%H:%M:%S")

                    # Calculate duration
                    duration = end_dt - start_dt
                    duration_ms = duration.total_seconds() * 1000
                    if duration_ms < 1000:
                        duration_str = f"{duration_ms:.0f}ms"
                    else:
                        duration_str = f"{duration.total_seconds():.1f}s"

                    time_info = f"{start_str} -> {end_str} ({duration_str})"
                except:
                    time_info = end_time[-8:] if end_time else timestamp[-8:]
            elif timestamp:
                # For started events, just show timestamp
                try:
                    from datetime import datetime

                    dt = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
                    time_info = dt.strftime("%H:%M:%S")
                except:
                    time_info = timestamp[-8:]

            css_class = f"history-item history-{event_type}"
            history_html += f"""
                <div class="{css_class}">
                    <span>{display_name} - {event_type}</span>
                    <span class="timestamp">{time_info}</span>
                </div>
            """

        if not recent_history:
            history_html = '<div style="color: #888; font-style: italic;">No execution history</div>'

        # Return the Plotly HTML directly with auto-refresh wrapper
        html_wrapper = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>{page_title}</title>
            <style>
                body {{
                    font-family: Arial, sans-serif;
                    margin: 0;
                    padding: 10px;
                    background-color: #f5f5f5;
                    width: 100vw;
                    height: 100vh;
                    overflow-x: auto;
                }}
                .header {{
                    text-align: center;
                    margin-bottom: 20px;
                    background-color: white;
                    padding: 20px;
                    border-radius: 8px;
                    box-shadow: 0 2px 4px rgba(0,0,0,0.1);
                }}
                .stats {{
                    display: flex;
                    justify-content: space-around;
                    margin-bottom: 20px;
                    padding: 15px;
                    background-color: white;
                    border-radius: 8px;
                    box-shadow: 0 2px 4px rgba(0,0,0,0.1);
                }}
                .stat {{
                    text-align: center;
                }}
                .stat-value {{
                    font-size: 24px;
                    font-weight: bold;
                    color: #2c3e50;
                }}
                .stat-label {{
                    font-size: 14px;
                    color: #7f8c8d;
                }}
                .refresh-btn {{
                    background-color: #3498db;
                    color: white;
                    padding: 10px 20px;
                    border: none;
                    border-radius: 5px;
                    cursor: pointer;
                    font-size: 16px;
                    margin: 10px;
                }}
                .refresh-btn:hover {{
                    background-color: #2980b9;
                }}
                .status-indicator {{
                    position: fixed;
                    top: 10px;
                    right: 10px;
                    padding: 5px 10px;
                    border-radius: 5px;
                    color: white;
                    font-size: 12px;
                    z-index: 1000;
                }}
                .status-connected {{
                    background-color: #27ae60;
                }}
                .status-checking {{
                    background-color: #f39c12;
                }}
                .plotly-container {{
                    background-color: white;
                    border-radius: 8px;
                    box-shadow: 0 2px 4px rgba(0,0,0,0.1);
                    overflow: hidden;
                    width: 100%;
                    min-height: 800px;
                }}
                .dag-section {{
                    display: flex;
                    justify-content: space-around;
                    margin-bottom: 10px;
                }}
                .dag-info {{
                    text-align: center;
                    padding: 10px;
                    background-color: #ecf0f1;
                    border-radius: 5px;
                    margin: 0 5px;
                }}
                .execution-info {{
                    flex: 1
                    gap: 20px;
                    margin-bottom: 20px;
                }}
                .execution-panel {{
                    flex: 1;
                    background-color: white;
                    border-radius: 8px;
                    padding: 15px;
                    box-shadow: 0 2px 4px rgba(0,0,0,0.1);
                }}
                .execution-panel h4 {{
                    margin-top: 0;
                    margin-bottom: 10px;
                    color: #2c3e50;
                    border-bottom: 2px solid #3498db;
                    padding-bottom: 5px;
                }}
                .history-item {{
                    padding: 5px 10px;
                    margin: 2px 0;
                    border-radius: 4px;
                    font-size: 12px;
                    display: flex;
                    justify-content: space-between;
                    align-items: center;
                }}
                .history-started {{
                    background-color: #fff3cd;
                    color: #856404;
                    border-left: 3px solid #ffc107;
                }}
                .history-completed {{
                    background-color: #d4edda;
                    color: #155724;
                    border-left: 3px solid #28a745;
                }}
                .history-failed {{
                    background-color: #f8d7da;
                    color: #721c24;
                    border-left: 3px solid #dc3545;
                }}
                .timestamp {{
                    font-size: 10px;
                    opacity: 0.7;
                    min-width: 120px;
                    text-align: right;
                }}
            </style>
            <script>
                let lastTimestamp = {current_timestamp};
                let checkInterval;

                function checkForUpdates() {{
                    fetch('/api/status')
                        .then(response => response.json())
                        .then(data => {{
                            const statusEl = document.getElementById('status-indicator');

                            if (data.has_data && data.timestamp > lastTimestamp) {{
                                statusEl.textContent = 'New data detected - refreshing...';
                                statusEl.className = 'status-indicator status-checking';
                                lastTimestamp = data.timestamp;
                                setTimeout(() => {{
                                    location.reload();
                                }}, 1000);
                            }} else {{
                                statusEl.textContent = `Connected - auto-refresh active (last: ${{new Date(data.timestamp * 1000).toLocaleTimeString()}})`;
                                statusEl.className = 'status-indicator status-connected';
                            }}
                        }})
                        .catch(error => {{
                            const statusEl = document.getElementById('status-indicator');
                            statusEl.textContent = 'Connection error';
                            statusEl.className = 'status-indicator status-checking';
                        }});
                }}

                function startAutoRefresh() {{
                    checkInterval = setInterval(checkForUpdates, 2000);
                    checkForUpdates();
                }}

                function stopAutoRefresh() {{
                    if (checkInterval) {{
                        clearInterval(checkInterval);
                    }}
                }}

                document.addEventListener('DOMContentLoaded', startAutoRefresh);
                document.addEventListener('visibilitychange', function() {{
                    if (document.hidden) {{
                        stopAutoRefresh();
                    }} else {{
                        startAutoRefresh();
                    }}
                }});
            </script>
        </head>
        <body>
            <div id="status-indicator" class="status-indicator status-checking">Initializing...</div>

            <div class="execution-info">
                <h4>Recent Execution History (Last updated: {current_timestamp})</h4>
                <div>{history_html}</div>
            </div>

            <div class="plotly-container">
                {plotly_html}
            </div>
        </body>
        </html>
        """

        return HTMLResponse(content=html_wrapper)

    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Error generating visualization: {str(e)}"
        )


@app.get("/")
async def root():
    """Root endpoint with API information."""
    return {
        "title": "DAG Visualizer API",
        "version": "1.0.0",
        "endpoints": {
            "POST /api/display": "Store DAG JSON data",
            "GET /api/display": "View DAG visualization in browser",
            "GET /api/status": "Get current data status",
        },
    }


if __name__ == "__main__":
    import uvicorn

    print("Starting DAG Visualizer Server...")
    print("API Documentation: http://localhost:8001/docs")
    print("Store DAG: POST http://localhost:8001/display")
    print("View Graph: GET http://localhost:8001/display")
    uvicorn.run(app, host="0.0.0.0", port=8001)
