# ARCHITECTURE.md
# Project: corporate-data — Excel Analytics Pipeline Demo
# Purpose: Demonstrate that corporate document workflows can be
#          automated using modern LLMs and auxiliary tooling.
# Audience: Technical demo for executive stakeholders.

## Problem Statement

Organizations accumulate hundreds of Excel files built over years
by domain experts. The files encode critical business logic
(pipelines, hierarchies, thresholds) that exists only in the
file structure and in people's heads.

Goal: extract that knowledge automatically into a structured,
queryable schema — as a step toward a corporate data agent.

## Pipeline Stages

### Stage 01b — LLM Navigation Pass (pre-pass, LLM)
Responsibility: determine sheet structure before mechanical extraction.
- Sends first 15 + last 3 rows of each sheet to LLM
- Returns: header_row, data_start_row, layout type, period block col_start/col_end
- Stage 01 reads latest navigation_*.json automatically
- layout types: standard | horizontal_periods
- Confidence < 0.7 → warning logged, Stage 01 falls back to heuristics
Output: navigation_<timestamp>.json

### Stage 01 — Structural Extractor (pure script, no LLM)
Responsibility: mechanical facts only, zero interpretation.
- Detect sheet names, dimensions, header row position
- Infer column data types (numeric / categorical / mixed / text)
- Compute null rates, value ranges, unique value counts
- Detect mixed-type columns (e.g. number + "no sales" string)
- Find JOIN candidates: columns with matching names across sheets
- Detect anomaly rows (totals, medians, separators)
Output: structural_<timestamp>.json (JSON Schema validated)

### Stage 02 — Semantic Analyzer (LLM: claude-sonnet-4-6)
Responsibility: interpret structure, classify confidence, route.
Receives: structural JSON + sample rows (up to config.max_tokens)
Produces three buckets:

  resolved[]:       high confidence (>0.85), logged to audit.log
  confirm_queue[]:  medium confidence (0.5-0.85), needs human yes/no
  escalate_queue[]: unresolvable without domain knowledge

Rules:
- Uses structured JSON output (no free-text parsing)
- Each item carries: field, hypothesis, confidence, evidence[]
- Prompt template: prompts/semantic_analyzer.jinja2
- Caches responses by input hash (dev mode)
Output: semantic_<timestamp>.json

### Stage 03 — Human Review (CLI interface)
Responsibility: domain expert confirms hypotheses, answers questions.
Interface: interactive CLI (not a web form for demo simplicity)
- confirm_queue: yes/no with optional comment
- escalate_queue: free-text answer
Time budget target: < 20 minutes for a 7-sheet file
Output: human_review_<timestamp>.json

### Stage 04 — Schema Assembler (script)
Responsibility: merge all three sources into final schema + report.
- Merges resolved + confirmed + human answers
- Builds relationship graph (JOIN map between sheets)
- Generates final_schema_<timestamp>.json
- Generates schema report (.docx) via existing docx pipeline
Output: final_schema_<timestamp>.json + report_<timestamp>.docx

### Stage 05 — Query Agent (planned)
Responsibility: natural language queries against final_schema + Excel data.
Sub-stages:
  05a embed.py     — build ChromaDB vector store from final_schema fields
  05b retrieve.py  — semantic search: query → top-K relevant tables/fields
  05c generate.py  — LLM: query + schema context → Python/pandas code
  05d execute.py   — sandbox execution (RestrictedPython, timeout, whitelist)
  05e chart.py     — auto chart type selection, matplotlib/plotly output
  app/query_ui.py  — Streamlit operator UI (sales person, no code visible)
Error model: auto-retry loop → operator sees friendly message → admin escalation
Admin panel: separate password-protected Streamlit page with full debug info
Output: outputs/query_results/<session_id>/{result.csv, chart.png, audit.jsonl}

## Directory Structure

```
corporate-data/
  .cursorrules              # architectural principles (this law)
  ARCHITECTURE.md           # this file
  README.md                 # quickstart for developers
  requirements.txt          # pinned versions
  .env.example              # required env vars (no values)
  config.py                 # all thresholds, paths, defaults
  pipeline/
    __init__.py
    stage_01_extract.py     # structural extractor
    stage_02_analyze.py     # LLM semantic analyzer
    stage_03_review.py      # human review CLI
    stage_04_assemble.py    # schema assembler + report
  utils/
    __init__.py
    excel_reader.py         # Excel I/O, header detection
    schema_validator.py     # JSON Schema validation
    llm_client.py           # Anthropic API wrapper + retry + logging
    classifier.py           # confidence routing logic
    report_writer.py        # .docx generation (reuse existing)
  prompts/
    semantic_analyzer.jinja2
  outputs/                  # gitignored, timestamped results
  tests/
    fixtures/               # anonymized sample data
    test_extract.py
    test_analyze.py
    test_assemble.py
  logs/
    pipeline.log            # structured JSON
    audit.log               # model decisions log
  app/
    query_ui.py             # Stage 05: Streamlit operator UI (planned)
  outputs/
    vector_store/           # Stage 05a: ChromaDB persistent (planned)
    query_results/          # Stage 05: query session results (planned)
```

## Key Design Decisions (ADR log)

ADR-001: Stages communicate via files, not in-memory objects.
  Reason: each stage runnable in isolation, easy to debug,
  natural checkpoint if pipeline fails mid-run.

ADR-002: LLM output always in JSON mode with schema validation.
  Reason: eliminates regex parsing fragility.

ADR-003: Source Excel files are never modified.
  Reason: trust and reproducibility — analyst can verify
  that pipeline did not corrupt their data.

ADR-004: Human review is a separate stage, not inline.
  Reason: keeps LLM and human logic decoupled,
  allows async review (email form in future).

ADR-005: Confidence thresholds in config, not hardcoded.
  Reason: different domains require different sensitivity.
  Default: resolved > 0.85, confirm 0.5-0.85, escalate < 0.5

ADR-006: Text-to-Python instead of Text-to-SQL (Stage 05).
  Reason: data lives in Excel with mixed sentinels and pivot structures.
  SQL would require ETL and cannot handle 'нет продаж' without transformation.
  Python/pandas is already the pipeline stack — single toolchain.

ADR-007: Operator UI hides all code (Stage 05).
  Reason: operator is a sales analyst, not a developer.
  Error recovery: auto-retry loop is invisible; operator sees only
  'Could not answer, try rephrasing'. Admin receives failed_auto notification
  and fixes via admin panel + few-shot examples — operator unaware.
