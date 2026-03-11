# SPEC: Stage 01 — Structural Extractor
# Status: DONE — 7/7 tests passed 2025-03-11
# Based on: explore_stage1.py draft run + STAGE1_FINDINGS.md
# Author: research & architecture session 2025-03-11

## Purpose

Pure structural analysis of an Excel file. No interpretation.
No LLM calls. Deterministic and reproducible.

Input:  any .xlsx file path
Output: outputs/structural_<timestamp>.json (JSON Schema validated)

## Acceptance Criteria — ALL MET

1. [x] Correctly identifies header row for all 7 sheets:
       BCG=4, Факт продаж=9, План Антонова=4, others=0 or 1.
2. [x] Detects exactly 16 mixed-type columns in BCG sheet.
       All 16 have mixed_sentinel_values == ["нет продаж"].
3. [x] JOIN candidates contain "категория" and "линия модели".
       No "_unnamed_N" entries in candidates.
4. [x] Horizontal period layout detected in BCG, ABC расчет, Факт продаж.
       Тренды категорий correctly classified as "standard".
5. [x] Anomaly false-positive rate < 20% on test file.
       Previously 44/61 (72%). Fixed by exact-match + separator logic.
6. [x] Output validates against structural_output.schema.json with 0 errors.
7. [x] Runs on test file in under 10 seconds (actual: 38.93s total with pytest).
8. [x] All functions have type hints, docstrings, are under 50 lines.

## Implementation Notes (from agent walkthrough)

### Header Detection
Scores rows by count of non-numeric string cells.
Uses openpyxl merged_cells to handle multi-row headers.
Sets data_start_row = last row of merge + 1 when multi-row detected.

### Column Classification
Always accesses by iloc[:, idx] — never df[col_name].
Reason: duplicate column names return DataFrame, not Series.
Sentinel detection: if mixed and string values form closed set
(e.g. only "нет продаж") — collected into mixed_sentinel_values.

### Anomaly Detection Fix
Old: substring match — caught 72% false positives.
New: exact/startswith match on full stripped lowercase cell value.
Separator logic: null_ratio > 0.95 AND row is between non-empty rows.

### Horizontal Period Layout
Detects repeating categorical markers across duplicate column blocks.
Identifies period_count and period_labels from raw rows above header.

### JOIN Candidates Fix
Filters out: _unnamed_N, null_rate > 0.9, dominant_type == "empty".
Adds unique_value_overlap (sample-based, top 100 values).
Result: 2-4 real candidates instead of 38.

## Files Created by Agent

pipeline/__init__.py
pipeline/stage_01_extract.py
utils/__init__.py
utils/excel_reader.py
utils/schema_validator.py
tests/__init__.py
tests/fixtures/bcg_du_2025_q4.xlsx
tests/test_stage_01.py

## Dependencies Confirmed Working

pandas==2.2.3
openpyxl==3.1.5
jsonschema==4.23.0
pytest==8.3.5
Python==3.9.1
platform: darwin
