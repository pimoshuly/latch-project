from __future__ import annotations

import inspect
from typing import Dict, Any


class CallContext:
    @staticmethod
    def get_caller_info(skip_frames: int = 1) -> Dict[str, Any]:
        stack = inspect.stack()

        caller_frame = None
        for i in range(skip_frames, len(stack)):
            frame = stack[i]
            if not CallContext._is_internal_frame(frame):
                caller_frame = frame
                break

        if not caller_frame:
            return {
                "caller": "unknown",
                "filename": "unknown",
                "lineno": 0,
                "is_task": False,
                "code_context": None,
                "task_instance": None,
                "creation_origin": "unknown",
                "call_chain": [],
                "parent_task": None,
            }

        caller_function = caller_frame.function

        is_task_call, task_instance = CallContext._is_called_from_task(stack)

        creation_origin = CallContext._determine_creation_origin(stack, is_task_call)

        call_chain = CallContext._build_call_chain(stack)

        return {
            "caller": caller_function,
            "filename": caller_frame.filename,
            "lineno": caller_frame.lineno,
            "is_task": is_task_call,
            "code_context": (
                caller_frame.code_context[0].strip()
                if caller_frame.code_context
                else None
            ),
            "task_instance": task_instance,
            "creation_origin": creation_origin,
            "call_chain": call_chain,
            "parent_task": task_instance if is_task_call else None,
        }

    @staticmethod
    def _is_internal_frame(frame_info: inspect.FrameInfo) -> bool:
        filename = frame_info.filename
        internal_files = ["tasks.py", "call_context.py"]
        return any(filename.endswith(internal_file) for internal_file in internal_files)

    @staticmethod
    def _is_called_from_task(stack) -> tuple[bool, Any]:
        for frame_info in stack:
            frame_locals = frame_info.frame.f_locals

            if frame_info.function in ["__call__", "_call_sync", "_call_async"]:
                if "self" in frame_locals:
                    obj = frame_locals["self"]
                    if hasattr(obj, "__class__") and obj.__class__.__name__ == "Task":
                        return True, obj

            for var_value in frame_locals.values():
                if (
                    hasattr(var_value, "__class__")
                    and var_value.__class__.__name__ == "Task"
                ):
                    return True, var_value

        return False, None

    @staticmethod
    def _determine_creation_origin(stack, is_task_call: bool) -> str:
        if is_task_call:
            return "task"
        else:
            for frame_info in stack:
                if frame_info.function == "<module>":
                    return "module"
            return "standalone"

    @staticmethod
    def _build_call_chain(stack) -> list[str]:
        call_chain = []
        for frame_info in stack:
            if not CallContext._is_internal_frame(frame_info):
                function_name = frame_info.function

                frame_locals = frame_info.frame.f_locals
                if "self" in frame_locals:
                    obj = frame_locals["self"]
                    if hasattr(obj, "__class__"):
                        class_name = obj.__class__.__name__
                        function_name = f"{class_name}.{function_name}"

                call_chain.append(function_name)

        return call_chain
