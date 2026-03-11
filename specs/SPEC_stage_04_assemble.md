# SPEC: Stage 04 — Schema Assembler + Report Writer
# Status: DONE — verified 2025-03-11
# Generated: final_schema_2026-03-11T08-27-21.json + report (fixed)
# Author: research & architecture session 2025-03-11

## Acceptance Criteria — ALL MET

1. [x] final_schema.json merges structural + semantic + human review correctly.
2. [x] .docx matches reference structure: 15 tables, 12 H1, 6 H2.
3. [x] python-docx only. No Node.js.
4. [x] filter_artifact columns excluded from all tables.
5. [x] Tables generated dynamically from final_schema data.
6. [x] Runs without human_review file (resolved[] only), logs warning.
7. [x] Validates against final_schema.schema.json.
8. [x] Footer: dynamic provider + model from semantic JSON.

## Verified Output (report_fixed.docx)

Paragraphs: 53
Tables:     15  (T00=sheet overview, T01-T06=BCG subsections,
                 T07-T11=other sheets, T12=relations, T13=rules, T14=channels)
H1:         12
H2:          6  (BCG subsections 3.1–3.6)
Title color: RGBColor(0x1F, 0x38, 0x64) bold=True size=279400 ✓
Footer color: RGBColor(0x88, 0x88, 0x88) size=107950 ✓

## Key Fixes Applied to report_writer.py

1. Heading styles: p.add_paragraph(style="Heading 1") + _styled_run with color override.
   Previous: paragraphs had no style — H1/H2 count was 0.

2. Font properties set on runs, not styles.
   Previous: bold=None, size=None, color=None on title.

3. 4-column field tables everywhere (Поле / Колонка | Тип данных | Значения | Описание).
   Previous: 2-column ["Поле", "Категории"].

4. sentinel_values in final_schema is LLM evidence text, not values.
   Fix: use categorical_values for display; if sentinel_values present → "Число / «нет продаж»".

5. BCG column routing by field name keywords (not by data_type alone).
   _SALES_KW / _DYNAMICS_KW / _BCG_KW / _ABC_KW tuples.
   Previous: "РОСТ" substring didn't match any real column names.

6. Empty table guard: _field_table() returns silently if columns=[].
   Prevents 16th ghost table.

7. Footer paragraph added with dynamic provider/model.

## Files

pipeline/stage_04_assemble.py  — orchestrator (unchanged)
utils/report_writer.py         — REWRITTEN (final version)
specs/final_schema.schema.json
tests/test_stage_04.py

## Dependencies

python-docx==1.1.2
