"""
stage_01b_navigate.py — LLM Navigation Pass
============================================
Sits between Stage 01a (raw structural scan) and Stage 01c (full column
classification). Receives only the first 15 + last 3 rows of each sheet
and asks the LLM one focused question:

  "Where is the real header? Where do data rows start?
   Are there horizontal period blocks, and if so, what are they called?"

Output: outputs/navigation_<timestamp>.json

The navigation JSON is consumed by stage_01_extract.py (and future
stage_01c) as coordinate hints — replacing the fragile heuristic
_detect_header_row() for sheets where it fails (pivots, BCG-style layouts).

Design principles:
  - One LLM call per file (all sheets in a single prompt)
  - Temperature=0, JSON mode enforced
  - Falls back to heuristic coordinates if LLM response is malformed
  - Never modifies the source Excel
"""

import asyncio
import hashlib
import json
import logging
import sys
import warnings
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd
from anthropic import AsyncAnthropic

# openpyxl emits UserWarnings for unsupported Excel extensions (x14/x15 features,
# advanced conditional formatting). These are cosmetic — data is read correctly.
warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
PREVIEW_HEAD_ROWS = 15    # rows sent to LLM per sheet (top)
PREVIEW_TAIL_ROWS = 3     # rows sent to LLM per sheet (bottom)
MAX_COLS_IN_PREVIEW = 42  # must cover widest sheet (BCG = 40 cols)
# For the prompt, wide rows are shown compactly: only non-empty cells with index
PROMPT_MAX_CELLS_PER_ROW = 30  # cap per row to keep prompt size manageable
LLM_MODEL = "claude-sonnet-4-6"
LLM_MAX_TOKENS = 2048

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Excel preview builder
# ---------------------------------------------------------------------------

def _sheet_preview(path: Path, sheet_name: str) -> dict:
    """Read raw head+tail rows from one sheet. No interpretation whatsoever."""
    df_full = pd.read_excel(path, sheet_name=sheet_name, header=None, dtype=str)
    df_trimmed = df_full.iloc[:, :MAX_COLS_IN_PREVIEW]

    def _clean_row(row) -> list[str]:
        return [
            "" if pd.isna(v) or str(v).strip() in ("nan", "None") else str(v).strip()
            for v in row
        ]

    head_rows = [
        {"row_index": int(i), "cells": _clean_row(row)}
        for i, row in df_trimmed.iloc[:PREVIEW_HEAD_ROWS].iterrows()
    ]

    tail_df = df_full.iloc[-PREVIEW_TAIL_ROWS:, :MAX_COLS_IN_PREVIEW]
    tail_rows = [
        {"row_index": int(i), "cells": _clean_row(row)}
        for i, row in tail_df.iterrows()
    ]

    return {
        "sheet_name": sheet_name,
        "total_rows": int(df_full.shape[0]),
        "total_cols": int(df_full.shape[1]),
        "head_rows": head_rows,
        "tail_rows": tail_rows,
    }


def build_previews(path: Path) -> list[dict]:
    """Build head+tail previews for every sheet in the workbook."""
    xl = pd.ExcelFile(path)
    previews = []
    for name in xl.sheet_names:
        logger.info(f"  previewing: {name}")
        previews.append(_sheet_preview(path, name))
    return previews


# ---------------------------------------------------------------------------
# Prompt
# ---------------------------------------------------------------------------

SYSTEM_PROMPT = """\
You are an expert data analyst specialising in corporate Excel files.
Your only task is to return structural coordinates for each sheet.

Rules:
- header_row: 0-based row index containing column names.
  If the very first row is the header, answer 0.
  Ignore pivot filter rows (values like "All", "Рынок Украина",
  column names like "ЦФО без иерархии") — those are metadata, not headers.
- data_start_row: 0-based index of the FIRST real data row
  (after headers, after blank separators, after filter metadata).
- layout: "standard" or "horizontal_periods".
  Use "horizontal_periods" when you see the same metric names repeating
  across columns for different time periods, e.g.:
    col 2: "Продажи за IV кв 2024"  col 6: "Продажи за II кв 2025" ...
  This is the KEY signal: if a column name contains a quarter/period label
  AND the same metric name appears again in a later column with a different
  period label — the layout IS "horizontal_periods".
- blocks: REQUIRED when layout == "horizontal_periods". List ALL blocks.
  Each block: {name: "<quarter/period label>", col_start: <int>, col_end: <int>}
  col indices are 0-based and inclusive.

  CRITICAL — block boundaries for BCG-style sheets:
  Each period block must span EXACTLY the columns that belong to that period.
  Look at the column names carefully — they contain the period label.
  Count every column whose name contains the period label and set col_end accordingly.

  Example for BCG sheet with header row 4:
    col 0: КАТЕГОРИЯ, col 1: ЛИНИЯ МОДЕЛИ (these are KEY columns, NOT in any block)
    IV кв 2024 block: col 2 (Продажи), col 3 (Доход), col 4 (Маржа), col 5 (Доля)
      → col_start=2, col_end=5
    II кв 2025 block: col 6 (Продажи), col 7 (Доход), col 8 (Маржа), col 9 (Доля),
                      col 10 (РОСТ доходах), col 11 (РОСТ доли)
      → col_start=6, col_end=11
    III кв 2025 block: col 12–17 (same 6-column structure)
      → col_start=12, col_end=17
    IV кв 2025 block: col 18 (Продажи), col 19 (Доход), col 20 (Маржа), col 21 (Доля),
                      col 22 (РОСТ доходах), col 23 (РОСТ доли),
                      col 24 (РОСТ доходов к III), col 25 (РОСТ доли к IV 2024)
      → col_start=18, col_end=25
    After col 25: col 26=empty, col 27-30=Группа по BCG (×4), col 31-34=ABC анализ (×4)
    These trailing categorical columns are NOT metric period blocks — skip them.

  General rule: the block name = the period label found in the column header.
  The first period block typically has fewer columns than later ones (no РОСТ cols).
  Later blocks add РОСТ columns for each additional metric compared to the first.
  DO NOT include trailing categorical columns (Группа по BCG, ABC анализ, PROMO, Инфо)
  in any block — they have period labels in a SEPARATE sub-header row, not the main header.

- confidence: 0.0–1.0. Use < 0.7 if the structure is genuinely ambiguous.
- notes: one sentence if confidence < 0.85 or structure is unusual.

Return ONLY valid JSON. No markdown, no text outside the JSON object.
"""

def _format_row(cells: list[str], max_cells: int = PROMPT_MAX_CELLS_PER_ROW) -> str:
    """Format a row for the prompt.

    Wide rows (>15 non-empty cells) use indexed format: '0:Val  3:Val  7:Val'
    so the LLM can see column positions and detect repeating block boundaries.
    Narrow rows use the simpler ' | ' join.
    """
    non_empty = [(i, c) for i, c in enumerate(cells) if c]
    if not non_empty:
        return "(empty)"
    if len(non_empty) <= 15:
        return " | ".join(cells) if any(cells) else "(empty)"
    # Wide: show col_index:value, capped at max_cells
    parts = [f"{i}:{c}" for i, c in non_empty[:max_cells]]
    suffix = f"  …+{len(non_empty) - max_cells} more" if len(non_empty) > max_cells else ""
    return "  ".join(parts) + suffix


def build_prompt(previews: list[dict], source_file: str) -> str:
    lines = [
        f"File: {source_file}",
        f"Total sheets: {len(previews)}",
        "",
        "Inspect each sheet and return navigation coordinates.",
        "IMPORTANT: For 'horizontal_periods' sheets, list ALL period blocks you see.",
        "",
    ]
    for prev in previews:
        lines.append(
            f"=== Sheet: {prev['sheet_name']}  "
            f"({prev['total_rows']} rows × {prev['total_cols']} cols) ==="
        )
        lines.append("HEAD rows:")
        for r in prev["head_rows"]:
            lines.append(f"  [{r['row_index']:>3}]  {_format_row(r['cells'])}")
        lines.append("TAIL rows:")
        for r in prev["tail_rows"]:
            lines.append(f"  [{r['row_index']:>3}]  {_format_row(r['cells'])}")
        lines.append("")

    lines += [
        'Return JSON with this exact schema:',
        '{',
        '  "sheets": {',
        '    "<sheet_name>": {',
        '      "header_row": <int>,',
        '      "data_start_row": <int>,',
        '      "layout": "standard" | "horizontal_periods",',
        '      "blocks": [{"name": "<period>", "col_start": <int>, "col_end": <int>}],',
        '      "confidence": <float>,',
        '      "notes": "<string>"',
        '    }',
        '  }',
        '}',
    ]
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# LLM call
# ---------------------------------------------------------------------------

async def _call_llm(prompt: str, api_key: str) -> str:
    client = AsyncAnthropic(api_key=api_key)
    resp = await client.messages.create(
        model=LLM_MODEL,
        max_tokens=LLM_MAX_TOKENS,
        temperature=0.0,
        system=SYSTEM_PROMPT,
        messages=[{"role": "user", "content": prompt}],
    )
    if resp.stop_reason != "end_turn":
        logger.warning(f"LLM stop_reason={resp.stop_reason!r} — may be truncated")
    return resp.content[0].text


def _parse_llm_response(raw: str) -> dict:
    """Strip markdown fences and parse JSON."""
    s = raw.strip()
    if "```" in s:
        for part in s.split("```"):
            candidate = part.strip().lstrip("json").strip()
            if candidate.startswith("{"):
                s = candidate
                break
    start, end = s.find("{"), s.rfind("}")
    if start != -1 and end != -1:
        s = s[start : end + 1]
    return json.loads(s)


# ---------------------------------------------------------------------------
# Heuristic fallback (used when LLM response is missing or malformed)
# ---------------------------------------------------------------------------

def _looks_numeric(s: str) -> bool:
    try:
        float(s.replace(",", ".").replace(" ", "").replace("%", ""))
        return True
    except ValueError:
        return False


def _heuristic_coords(preview: dict) -> dict:
    """Score rows by count of non-numeric string cells; pick the best."""
    best_row, best_score = 0, -1
    for r in preview["head_rows"]:
        score = sum(1 for c in r["cells"] if c and not _looks_numeric(c))
        if score > best_score:
            best_score, best_row = score, r["row_index"]
    return {
        "header_row": best_row,
        "data_start_row": best_row + 1,
        "layout": "standard",
        "blocks": [],
        "confidence": 0.4,
        "notes": "Heuristic fallback — LLM navigation unavailable for this sheet.",
    }


# ---------------------------------------------------------------------------
# Main navigate() function
# ---------------------------------------------------------------------------

async def navigate(path: Path, api_key: str) -> dict:
    """Full navigation pass. Returns the navigation JSON dict."""
    logger.info(f"Building sheet previews: {path.name}")
    previews = build_previews(path)

    prompt = build_prompt(previews, path.name)
    prompt_hash = hashlib.sha256(prompt.encode()).hexdigest()[:16]

    logger.info(f"Calling {LLM_MODEL} — prompt_hash={prompt_hash}")
    raw_response = await _call_llm(prompt, api_key)

    llm_sheets: dict = {}
    parse_error: Optional[str] = None
    try:
        parsed = _parse_llm_response(raw_response)
        llm_sheets = parsed.get("sheets", {})
        logger.info(f"LLM returned coordinates for {len(llm_sheets)} sheets")
    except (json.JSONDecodeError, KeyError) as e:
        parse_error = str(e)
        logger.warning(f"LLM parse failed: {e} — heuristic fallback for all sheets")

    # Merge LLM results with heuristic fallback for missing sheets
    final_sheets: dict = {}
    for prev in previews:
        name = prev["sheet_name"]
        if name in llm_sheets:
            nav = llm_sheets[name]
            nav.setdefault("blocks", [])
            nav.setdefault("notes", "")
        else:
            logger.warning(f"Sheet '{name}' missing from LLM response — heuristic fallback")
            nav = _heuristic_coords(prev)
        final_sheets[name] = nav

    # Warn on low-confidence sheets so operator can inspect
    for name, nav in final_sheets.items():
        if nav.get("confidence", 1.0) < 0.7:
            logger.warning(
                f"Low confidence sheet='{name}' conf={nav['confidence']:.2f} "
                f"— {nav.get('notes', '')}"
            )

    return {
        "source_file": path.name,
        "navigated_at": datetime.now().isoformat(),
        "model": LLM_MODEL,
        "prompt_hash": prompt_hash,
        "parse_error": parse_error,
        "sheets": final_sheets,
    }


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse
    import os
    from dotenv import load_dotenv

    parser = argparse.ArgumentParser(description="Stage 01b — LLM Navigation Pass")
    parser.add_argument("--input", "-i", required=True, help="Path to .xlsx file")
    args = parser.parse_args()

    load_dotenv()
    api_key = os.getenv("ANTHROPIC_API_KEY", "")
    if not api_key:
        print("ERROR: ANTHROPIC_API_KEY not set in .env", file=sys.stderr)
        sys.exit(1)

    input_path = Path(args.input)
    if not input_path.is_file():
        print(f"ERROR: File not found: {input_path}", file=sys.stderr)
        sys.exit(1)

    out_dir = Path("outputs")
    out_dir.mkdir(exist_ok=True)

    result = asyncio.run(navigate(input_path, api_key))

    ts = datetime.now().strftime("%Y-%m-%dT%H-%M-%S")
    out_path = out_dir / f"navigation_{ts}.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    # Human-readable summary table
    print(f"\nNavigation complete → {out_path}\n")
    print(f"{'Sheet':<40} {'Hdr':>4} {'Data':>5} {'Layout':<22} {'Blks':>4} {'Conf':>5}")
    print("─" * 85)
    for name, nav in result["sheets"].items():
        n_blocks = len(nav.get("blocks", []))
        layout = nav.get("layout", "?")
        conf = nav.get("confidence", 0.0)
        flag = "  ⚠" if conf < 0.7 else ""
        print(
            f"{name:<40} {nav['header_row']:>4} {nav['data_start_row']:>5} "
            f"{layout:<22} {n_blocks:>4} {conf:>5.2f}{flag}"
        )
        if nav.get("notes"):
            print(f"  {'':40}  → {nav['notes']}")
    print()
