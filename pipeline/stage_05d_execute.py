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


def load_excel_sheets(excel_path: str) -> Dict[str, pd.DataFrame]:
    """Loads all sheets from an Excel file, auto-detecting header rows.

    Uses the same header detection logic as stage_01: reads raw (header=None),
    searches the first HEADER_SEARCH_MAX_ROWS rows for the row with the most
    non-null string values, then re-reads with header=<detected_row>.
    Falls back to header=0 if detection fails.
    """
    import warnings
    xl = pd.ExcelFile(excel_path)
    dfs: Dict[str, pd.DataFrame] = {}

    for sheet in xl.sheet_names:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            raw = xl.parse(sheet, header=None)

        max_search = min(config.HEADER_SEARCH_MAX_ROWS, len(raw))
        best_row, best_score = 0, -1
        for i in range(max_search):
            row = raw.iloc[i]
            # Score = number of unique non-numeric string values
            # Header rows have many distinct labels; data rows have repeated values / numbers
            str_vals = [str(v).strip() for v in row
                        if pd.notna(v) and str(v).strip() not in ("", "nan")]
            try:
                unique_strings = sum(1 for v in str_vals
                                     if not pd.to_numeric(v, errors='coerce') == pd.to_numeric(v, errors='coerce'))
            except Exception:
                unique_strings = 0
            score = len(set(str_vals)) + unique_strings * 2  # weight unique text heavily
            if score > best_score:
                best_score, best_row = score, i

        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            df = xl.parse(sheet, header=best_row)

        # Drop fully-empty rows and columns
        df = df.dropna(how="all").dropna(axis=1, how="all")
        dfs[sheet] = df
        logger.debug("Sheet '%s': header_row=%d  shape=%s", sheet, best_row, df.shape)

    return dfs


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
