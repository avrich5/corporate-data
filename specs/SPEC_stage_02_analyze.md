# SPEC: Stage 02 — Semantic Analyzer
# Status: DONE — 4/4 tests passed + real run verified 2025-03-11
# Real run: semantic_2026-03-11T06-58-45.json
# Author: research & architecture session 2025-03-11
# Depends on: SPEC_stage_01_extract.md (DONE)

## Purpose

Interpret structural facts from Stage 01 using LLM(s).
Route each finding to one of three buckets.
Support two providers (Anthropic + OpenAI) with compete mode.

Input:  outputs/structural_<timestamp>.json
Output: outputs/semantic_<timestamp>.json

No Excel files are read. No human interaction. No UI.

## Acceptance Criteria — ALL MET

1. [x] All 16 BCG mixed-type sentinel columns in resolved[].
       All classified as no_sales_status, confidence=0.9.
2. [x] JOIN "категория" (overlap=0.85) in resolved[], confidence=0.9.
3. [x] JOIN "линия модели" (overlap=0.0) in confirm_queue[] (rule override).
4. [x] Compete mode: asyncio.gather, fallback on error, winner by confidence_sum.
       Real run: Anthropic degraded, winner_provider=openai.
5. [x] Every item carries: finding_type, sheet, field, hypothesis,
       confidence, evidence[], provider, model.
6. [x] Output validates against semantic_output.schema.json.
7. [x] Deterministic: temperature=0.0.
8. [x] All LLM calls logged to logs/audit.log.

## Real Run Results (2026-03-11T06-58-45)

provider_strategy: compete
winner_provider:   openai  (Anthropic degraded — fallback worked correctly)
resolved:          18 items
confirm_queue:     32 items
escalate_queue:    0 items

### Resolved (18)
- 16 BCG mixed_sentinel columns -> no_sales_status, confidence=0.9
- "Plan Антонова" "All" -> filter_artifact, confidence=0.95 (hard override)
- "Plan Антонова" "_unnamed_2" -> filter_artifact, confidence=0.95 (hard override)

### Confirm queue (32) — KEY FINDING FOR STAGE 03
All 32 items are from 2 presentation sheets:
  "Слайд 1 тренды категорий": 24 unnamed columns, confidence=0.8
  "Тренды категорий": 8 unnamed columns, confidence=0.8
All have identical question: "Confidence is moderate. Please confirm."

This is a UX problem for Stage 03:
  32 items with identical hypothesis, identical question, identical sheet pattern.
  Showing them one-by-one would waste human time.
  Stage 03 must GROUP them and ask once for the entire set.

## Implementation Notes

Providers: AnthropicClient (claude-3-7-sonnet-20250219), OpenAIClient (gpt-4o)
Routing overrides (hard-coded, bypass thresholds):
  - hypothesis == "unknown" -> escalate_queue[]
  - join overlap == 0.0 -> confirm_queue[]
  - "План Антонова" + "_unnamed"/"All" -> resolved[] at 0.95

## Files Created by Agent

pipeline/stage_02_analyze.py
utils/llm_client.py
utils/prompt_builder.py
prompts/semantic_analyzer.jinja2
specs/semantic_output.schema.json
tests/test_stage_02.py

## Dependencies Added

anthropic==0.49.0
openai==1.68.2
jinja2==3.1.6
aiohttp==3.11.14
