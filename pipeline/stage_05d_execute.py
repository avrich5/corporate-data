import logging
import multiprocessing
import time
from typing import Any, Dict, Optional, Tuple

import pandas as pd
import numpy as np
import datetime
import re
import json
import collections

from RestrictedPython import compile_restricted, safe_builtins
from RestrictedPython.Eval import default_guarded_getitem, default_guarded_getiter

import config

logger = logging.getLogger(__name__)


def _guarded_getattr(obj: Any, attr: str) -> Any:
    """Safely gets attributes, allowing standard pandas usage but blocking internal magic."""
    if attr.startswith("__") and attr.endswith("__") and attr not in {
        "__add__", "__sub__", "__eq__", "__lt__", "__gt__", "__le__", "__ge__",
        "__mul__", "__truediv__", "__floordiv__", "__iter__", "__len__", "__bool__"
    }:
        raise AttributeError(f"Access to private attribute '{attr}' is prevented.")
    return getattr(obj, attr)


def _worker_execute(
    code_str: str,
    dfs: Dict[str, pd.DataFrame],
    out_queue: multiprocessing.Queue
) -> None:
    """Worker process function to run the restricted code."""
    try:
        def _safe_import(name, globals=None, locals=None, fromlist=(), level=0):
            if name in config.EXECUTOR_ALLOWED_IMPORTS:
                return __import__(name, globals, locals, fromlist, level)
            raise ImportError(f"Import of '{name}' is strictly prohibited.")

        allowed_globals = {
            "__builtins__": {
                **safe_builtins,
                "__import__": _safe_import,
                "_getattr_": _guarded_getattr,
                "_getitem_": default_guarded_getitem,
                "_getiter_": default_guarded_getiter,
                "_write_": lambda x: x,
                "list": list,
                "dict": dict,
                "set": set,
                "tuple": tuple,
                "int": int,
                "float": float,
                "str": str,
                "bool": bool,
                "len": len,
                "sum": sum,
                "max": max,
                "min": min,
                "abs": abs,
                "round": round,
                "any": any,
                "all": all,
                "enumerate": enumerate,
                "zip": zip,
                "range": range,
            },
            "pd": pd,
            "np": np,
            "datetime": datetime,
            "re": re,
            "json": json,
            "collections": collections,
            "dfs": dfs,
        }

        byte_code = compile_restricted(
            source=code_str,
            filename="<inline>",
            mode="exec"
        )
        
        exec_locals: dict[str, Any] = {}
        exec(byte_code, allowed_globals, exec_locals)
        
        res = exec_locals.get("result", None)
        
        # Enforce row limits
        if isinstance(res, pd.DataFrame) and len(res) > config.EXECUTOR_MAX_ROWS:
            res = res.head(config.EXECUTOR_MAX_ROWS)
            
        out_queue.put(("success", res))
        
    except SyntaxError as e:
        out_queue.put(("error", f"SyntaxError: {str(e)}"))
    except Exception as e:
        out_queue.put(("error", f"{type(e).__name__}: {str(e)}"))


def execute_query(code_str: str, dfs: Dict[str, pd.DataFrame]) -> Tuple[Optional[Any], Optional[str]]:
    """Executes Python pandas code in a restricted sandbox environment.
    
    Args:
        code_str: Python code to execute.
        dfs: Mapping of sheet names to pandas DataFrames.
        
    Returns:
        Tuple of (result_data, error_message).
    """
    logger.info("Executing generated query code in sandbox.")
    out_queue = multiprocessing.Queue()
    proc = multiprocessing.Process(
        target=_worker_execute,
        args=(code_str, dfs, out_queue)
    )
    
    proc.start()
    proc.join(timeout=config.EXECUTOR_TIMEOUT)
    
    if proc.is_alive():
        proc.terminate()
        proc.join()
        msg = f"TimeoutError: Execution exceeded {config.EXECUTOR_TIMEOUT} seconds."
        logger.warning(msg)
        return None, msg
        
    if not out_queue.empty():
        status, payload = out_queue.get()
        if status == "success":
            logger.info("Query executed successfully.")
            return payload, None
        else:
            logger.error(f"Execution error: {payload}")
            return None, payload
            
    return None, "Unknown execution failure (no output)."


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Stage 05d - Sandbox Executor")
    parser.add_argument("--code", type=str, required=True, help="Python code string to execute")
    args = parser.parse_args()
    
    logging.basicConfig(level=getattr(logging, config.LOG_LEVEL), format="%(levelname)s: %(message)s")
    
    # Empty DFS representing no data for sandbox baseline test
    empty_dfs: Dict[str, pd.DataFrame] = {}
    
    logger.info("Running stage 05d execute in isolation mode.")
    result, error = execute_query(code_str=args.code, dfs=empty_dfs)
    
    if error:
        logger.error(f"Execution failed: {error}")
    else:
        logger.info("Execution succeeded.")
        print("\n--- RESULT ---")
        print(result)
