# SPEC: Stage 03 — Human Review CLI
# Status: READY FOR DEVELOPMENT
# Based on: real Stage 02 output semantic_2026-03-11T06-58-45.json
# Author: research & architecture session 2025-03-11
# Depends on: SPEC_stage_02_analyze.md (DONE)

## Purpose

Present confirm_queue and escalate_queue to a human expert.
Collect answers. Write reviewed JSON for Stage 04.

Input:  outputs/semantic_<timestamp>.json
Output: outputs/human_review_<timestamp>.json

No LLM calls. No Excel files. CLI only (no web UI for demo).

## Acceptance Criteria

1. Items with identical (sheet, hypothesis, question) are grouped
   and shown as ONE question — not N separate prompts.
   From real data: 32 items collapse to 2 group questions.
2. Human answers yes/no for confirm_queue groups,
   free text for escalate_queue items.
3. All original item IDs preserved in output —
   one human answer maps back to all items in the group.
4. Session is resumable: partial answers saved after each group,
   re-run skips already-answered groups.
5. Output validates against human_review.schema.json.
6. Dry-run mode (--dry-run) shows questions without prompting.
   Used for testing without human present.
7. Total interaction time target: under 10 minutes for real data
   (32 items -> 2 grouped questions in real run).

## Key Design Decision: Grouping

From real Stage 02 output:
  32 confirm_queue items, all pattern:
    sheet in ["Слайд 1 тренды категорий", "Тренды категорий"]
    hypothesis = "filter_artifact"
    confidence = 0.8
    question = "Confidence is moderate. Please confirm."

Showing 32 identical prompts one-by-one = bad UX and wastes expert time.

Grouping algorithm:
  Group key = (finding_type, hypothesis, question_for_human)
  Items sharing the same group key -> shown as ONE question.
  Human answers once -> answer applied to all items in group.

Expected groups from real data:
  Group 1: 24 items from "Слайд 1 тренды категорий"
    -> "24 unnamed columns in sheet 'Слайд 1 тренды категорий'
        classified as filter_artifact (confidence 0.80).
        Confirm all? [y/n/edit]"

  Group 2: 8 items from "Тренды категорий"
    -> "8 unnamed columns in sheet 'Тренды категорий'
        classified as filter_artifact (confidence 0.80).
        Confirm all? [y/n/edit]"

  Total: 2 group questions instead of 32.

Edge case — "edit" option:
  If human answers "edit" on a group — expand to individual items.
  Human can then answer each separately.
  This handles cases where a group has mixed true/false items.

## CLI Interaction Flow

Run:
    python -m pipeline.stage_03_review --input outputs/semantic_<timestamp>.json

### Screen layout per group (confirm_queue)

    ════════════════════════════════════════════════
    [1/2] mixed_sentinel — confirm_queue
    ════════════════════════════════════════════════
    Sheet:      Слайд 1 тренды категорий
    Hypothesis: filter_artifact
    Confidence: 0.80
    Affects:    24 columns (_unnamed_2 ... _unnamed_24,
                Доля в доходах за январь - июнь 2025)
    Evidence:   Columns have no name and contain mixed
                string values in a presentation sheet.

    Confirm hypothesis "filter_artifact" for all 24? [y/n/edit/skip/?]:

    y     -> confirmed, all 24 resolved as filter_artifact
    n     -> rejected, all 24 marked as needs_reclassification
    edit  -> expand to individual items, answer each separately
    skip  -> defer to escalate (human not sure now)
    ?     -> show full evidence for each item

### Screen layout per item (escalate_queue)

    ════════════════════════════════════════════════
    [1/N] mixed_sentinel — escalate_queue
    ════════════════════════════════════════════════
    Sheet:     BCG
    Field:     _unnamed_26
    Evidence:  [list all evidence strings]

    Question: [question_for_human from semantic JSON]

    Your answer (free text, Enter to submit):
    >

### After each group/item: auto-save
Partial session written to outputs/human_review_partial_<timestamp>.json.
On re-run: already-answered groups skipped, resume from first unanswered.

### Summary screen (end of session)
    ════════════════════════════════════════════════
    Review complete
    ════════════════════════════════════════════════
    confirmed:               24 items  (1 group answer)
    rejected:                0 items
    needs_reclassification:  0 items
    escalate_deferred:       0 items
    free_text_answered:      0 items
    Output: outputs/human_review_2026-03-11T07-30-00.json

## Output Format

{
  "source_semantic_file": "semantic_2026-03-11T06-58-45.json",
  "reviewed_at": "2026-03-11T07-30-00",
  "schema_version": "1.0",
  "reviewer": "human",
  "stats": {
    "groups_presented": 2,
    "items_confirmed": 24,
    "items_rejected": 0,
    "items_edited_individually": 0,
    "items_deferred": 0,
    "free_text_answers": 0
  },
  "answers": [
    {
      "group_key": "mixed_sentinel::filter_artifact::Confidence is moderate. Please confirm.",
      "group_size": 24,
      "decision": "confirmed",
      "free_text": null,
      "answered_at": "2026-03-11T07-22-11",
      "item_ids": [
        "Слайд 1 тренды категорий::_unnamed_2",
        "Слайд 1 тренды категорий::_unnamed_3",
        "..."
      ]
    },
    {
      "group_key": "mixed_sentinel::filter_artifact::Confidence is moderate. Please confirm.",
      "group_size": 8,
      "decision": "confirmed",
      "free_text": null,
      "answered_at": "2026-03-11T07-22-45",
      "item_ids": [
        "Тренды категорий::_unnamed_9",
        "..."
      ]
    }
  ]
}

Note: group_key is the same for both groups above, but they are
separate groups because sheet differs. Group identity =
(finding_type, hypothesis, question_for_human, sheet).

## Module Structure

pipeline/stage_03_review.py   — main CLI, orchestrates flow
utils/review_grouper.py       — groups items by (type, hypothesis, question, sheet)
utils/review_session.py       — save/load partial session, resume logic

## Test Cases

### test_grouping
Load real semantic fixture. Assert grouper produces 2 groups.
Assert group 1: 24 items, sheet="Слайд 1 тренды категорий".
Assert group 2: 8 items, sheet="Тренды категорий".

### test_dry_run
Run with --dry-run flag. Assert no stdin prompts issued.
Assert output file written with all items marked "dry_run_skipped".

### test_resume
Simulate partial session: answer group 1 only, write partial.
Re-run. Assert group 1 skipped, only group 2 presented.

### test_edit_expansion
Simulate "edit" answer on group 1.
Assert all 24 items presented individually in sequence.

### test_output_schema
Assert output validates against human_review.schema.json.

### test_rejection_path
Simulate "n" on a group.
Assert all items in group have decision="rejected" in output.

## Files to Create

pipeline/stage_03_review.py
utils/review_grouper.py
utils/review_session.py
specs/human_review.schema.json
tests/test_stage_03.py

## Dependencies

No new dependencies. Uses only stdlib (sys, json, pathlib, datetime).
