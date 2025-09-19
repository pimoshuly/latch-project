class ConstraintViolationHandler:
    """Handles constraint validation failures and creates Graphviz output."""

    def __init__(self, get_display_name_func):
        """
        Initialize the violation handler.

        Args:
            get_display_name_func: Function to get display names for tasks
        """
        self._get_display_name = get_display_name_func

    def handle_constraint_violation(self, caller_task: str, callee_task: str, error: Exception) -> None:
        """
        Handle constraint validation failure by outputting Graphviz format.

        Args:
            caller_task: Name of the task attempting the call
            callee_task: Name of the task being called
            error: The constraint violation exception
        """
        print(f"[VIOLATION] Constraint validation failed: {caller_task} -> {callee_task}")
        print(f"[VIOLATION] Error: {str(error)}")

        # Generate and output Graphviz format directly
        self._output_graphviz(caller_task, callee_task, error)

    def _extract_violation_reason(self, error_message: str) -> str:
        """Extract a concise violation reason from the error message."""
        error_lower = error_message.lower()

        if 'outgoing' in error_lower and 'not in allowed' in error_lower:
            return "OUTGOING NOT ALLOWED"
        elif 'incoming' in error_lower and 'not in allowed' in error_lower:
            return "INCOMING NOT ALLOWED"
        elif 'outgoing' in error_lower and 'limit' in error_lower:
            return "OUTGOING LIMIT EXCEEDED"
        elif 'incoming' in error_lower and 'limit' in error_lower:
            return "INCOMING LIMIT EXCEEDED"
        else:
            return "CONSTRAINT VIOLATION"

    def _output_graphviz(self, caller_task: str, callee_task: str, error: Exception) -> None:
        """Output Graphviz format for constraint violations."""
        try:
            graphviz_output = self._generate_graphviz(caller_task, callee_task, error)
            print(f"[VIOLATION] Graphviz DOT format:")
            print(graphviz_output)
        except Exception as graphviz_error:
            print(f"[VIOLATION] Failed to generate Graphviz output: {graphviz_error}")

    def _generate_graphviz(self, caller_task: str, callee_task: str, error: Exception) -> str:
        """Generate Graphviz DOT format for constraint violation."""
        # Extract violation details
        violation_reason = self._extract_violation_reason(str(error))

        caller_display = self._get_display_name(caller_task)
        callee_display = self._get_display_name(callee_task)

        # Determine violation type for coloring
        error_lower = str(error).lower()
        is_outgoing_violation = 'outgoing' in error_lower
        is_incoming_violation = 'incoming' in error_lower

        # Set node colors
        caller_color = "lightcoral" if is_outgoing_violation else "lightyellow"
        callee_color = "lightcoral" if is_incoming_violation else "lightyellow"

        title = f"Constraint Violation: {violation_reason}"

        # Generate DOT format
        dot_lines = [
            "digraph ConstraintViolation {",
            "    rankdir=LR;",
            "    node [shape=box, style=filled];",
            "    edge [fontsize=10];",
            f"    label=\"{title}\";",
            "    labelloc=t;",
            "",
            f"    \"{caller_task}\" [label=\"{caller_display}\", fillcolor={caller_color}];",
            f"    \"{callee_task}\" [label=\"{callee_display}\", fillcolor={callee_color}];",
            "",
            f"    \"{caller_task}\" -> \"{callee_task}\" [color=red, style=dashed, penwidth=2, label=\"X {violation_reason}\"];",
            "}"
        ]

        return "\n".join(dot_lines)