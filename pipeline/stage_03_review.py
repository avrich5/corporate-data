from __future__ import annotations
"""
pipeline/stage_03_review.py — interactive human review CLI.

Usage:
    python -m pipeline.stage_03_review --input outputs/semantic_<ts>.json
    python -m pipeline.stage_03_review --input outputs/semantic_<ts>.json --dry-run
"""
import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

from utils.review_grouper import ReviewGroup, build_groups
from utils.review_session import answered_keys, load_partial, save_final, save_partial

_SEP = "═" * 56
_DIV = "─" * 56

# Людські назви технічних гіпотез для відображення в інтерфейсі
_HYPOTHESIS_LABELS: dict[str, str] = {
    "section_header":      "заголовок розділу (не є рядком даних)",
    "grand_total":         "підсумковий рядок (не є рядком даних)",
    "metadata":            "рядок метаданих / службовий рядок",
    "data_row":            "звичайний рядок даних",
    "no_sales_status":     "позначка відсутності продажів («нет продаж»)",
    "filter_artifact":     "артефакт фільтру Excel (не є даними)",
    "data_error":          "помилка даних",
    "unknown":             "невідомо — потрібна перевірка",
    "confirmed_join":      "підтверджений зв'язок між листами",
    "probable_join":       "імовірний зв'язок між листами",
    "false_positive":      "хибний збіг (зв'язку нема)",
    "quarter":             "квартальний період",
    "half_year":           "піврічний період",
    "month":               "місячний період",
    "spacer":              "порожня колонка-роздільник",
    "placeholder":         "порожня колонка-заповнювач",
    "artifact":            "артефакт Excel (технічна порожня колонка)",
    "primary_data":        "основна таблиця даних",
    "detail_data":         "деталізована таблиця",
    "aggregated":          "агрегований звіт",
    "source_data":         "вихідні дані",
    "plan_data":           "планові дані",
    "presentation":        "презентаційний лист (графіки/слайди)",
}


def _print_group_header(idx: int, total: int, g: ReviewGroup) -> None:
    hypothesis_label = _HYPOTHESIS_LABELS.get(g.hypothesis, g.hypothesis)
    print(f"\n{_SEP}")
    print(f"[{idx}/{total}]  {g.finding_type}  —  {g.queue_type}_queue")
    print(_SEP)
    print(f"  Лист:       {g.sheet}")
    print(f"  Висновок:   {hypothesis_label}")
    print(f"  Впевненість:{g.confidence:.0%}")
    fields_preview = ", ".join(g.field_names[:5])
    if g.size > 5:
        fields_preview += f" … (+{g.size - 5} more)"
    print(f"  Стосується: {g.size} колонок  [{fields_preview}]")
    print(f"  Факти:      {g.evidence_sample}")
    print()


def _print_full_evidence(g: ReviewGroup) -> None:
    print(f"\n{_DIV}")
    for item in g.items:
        print(f"  [{item['field']}]")
        for ev in item.get("evidence", []):
            print(f"    • {ev}")
    print(_DIV)


def _build_answer(g: ReviewGroup, decision: str,
                  free_text: str | None, item_ids: list[str]) -> dict:
    return {
        "group_key":   g.group_key,
        "group_size":  g.size,
        "decision":    decision,
        "free_text":   free_text,
        "answered_at": datetime.now().isoformat(),
        "item_ids":    item_ids,
    }


def _ask_individually(g: ReviewGroup) -> dict | None:
    print(f"\n  [edit] Expanding to {g.size} individual items...")
    individual: list[dict] = []
    for i, item in enumerate(g.items, 1):
        field = item["field"]
        print(f"\n  ({i}/{g.size})  {field}")
        for ev in item.get("evidence", []):
            print(f"    • {ev}")
        while True:
            try:
                raw = input(f'  Підтвердити "{g.hypothesis}"? [y/n/skip]: ').strip().lower()
            except (EOFError, KeyboardInterrupt):
                return None
            if raw in ("y", "n", "skip"):
                dec = {"y": "confirmed", "n": "rejected", "skip": "deferred"}[raw]
                individual.append({"id": f"{item['sheet']}::{field}", "decision": dec})
                break
            print("  Введіть y / n / skip")
    confirmed = [x["id"] for x in individual if x["decision"] == "confirmed"]
    rejected  = [x["id"] for x in individual if x["decision"] == "rejected"]
    deferred  = [x["id"] for x in individual if x["decision"] == "deferred"]
    return {
        "group_key":   g.group_key,
        "group_size":  g.size,
        "decision":    "edited_individually",
        "free_text":   None,
        "answered_at": datetime.now().isoformat(),
        "item_ids":    g.item_ids,
        "individual_decisions": {"confirmed": confirmed, "rejected": rejected, "deferred": deferred},
    }


def _ask_group(g: ReviewGroup, dry_run: bool) -> dict | None:
    hypothesis_label = _HYPOTHESIS_LABELS.get(g.hypothesis, g.hypothesis)
    prompt = (f'  Підтвердити: «{hypothesis_label}» для всіх {g.size}? '
              "[y/n/edit/skip/?]: ")
    if dry_run:
        print(f"  [dry-run] {prompt.strip()}")
        return _build_answer(g, "dry_run_skipped", None, [])
    while True:
        try:
            raw = input(prompt).strip().lower()
        except (EOFError, KeyboardInterrupt):
            print("\n[!] Interrupted — saving partial session.")
            return None
        if raw == "y":      return _build_answer(g, "confirmed", None, g.item_ids)
        elif raw == "n":    return _build_answer(g, "rejected",  None, g.item_ids)
        elif raw == "skip": return _build_answer(g, "deferred",  None, g.item_ids)
        elif raw == "?":    _print_full_evidence(g)
        elif raw == "edit": return _ask_individually(g)
        else: print("  Введіть y / n / edit / skip / ?")


def _ask_escalate(g: ReviewGroup, dry_run: bool) -> dict | None:
    item = g.items[0]
    print(f"  Питання: {g.question}\n")
    for ev in item.get("evidence", []):
        print(f"    • {ev}")
    print()
    if dry_run:
        print("  [dry-run] Would ask for free-text answer.")
        return _build_answer(g, "dry_run_skipped", None, g.item_ids)
    try:
        ans = input("  Your answer > ").strip()
    except (EOFError, KeyboardInterrupt):
        return None
    return _build_answer(g, "free_text_answered", ans, g.item_ids)


def _print_summary(answers: list[dict]) -> dict:
    confirmed = rejected = deferred = free_text = dry_run_n = 0
    for a in answers:
        d, gs = a["decision"], a["group_size"]
        if d == "confirmed":             confirmed  += gs
        elif d == "rejected":            rejected   += gs
        elif d == "deferred":            deferred   += gs
        elif d == "free_text_answered":  free_text  += gs
        elif d == "dry_run_skipped":     dry_run_n  += gs
        elif d == "edited_individually":
            ind = a.get("individual_decisions", {})
            confirmed += len(ind.get("confirmed", []))
            rejected  += len(ind.get("rejected",  []))
            deferred  += len(ind.get("deferred",  []))
    print(f"\n{_SEP}\n  Перевірку завершено\n{_SEP}")
    print(f"  confirmed:          {confirmed}")
    print(f"  rejected:           {rejected}")
    print(f"  deferred:           {deferred}")
    print(f"  free-text answered: {free_text}")
    if dry_run_n:
        print(f"  dry-run skipped:    {dry_run_n}")
    return {
        "groups_presented":  len(answers),
        "items_confirmed":   confirmed,
        "items_rejected":    rejected,
        "items_deferred":    deferred,
        "free_text_answers": free_text,
    }


def run(semantic_path: Path, dry_run: bool) -> Path:
    semantic = json.loads(semantic_path.read_text(encoding="utf-8"))
    groups   = build_groups(semantic)
    if not groups:
        print("[stage_03] Nothing to review — queues are empty.")
        sys.exit(0)
    ts      = datetime.now().strftime("%Y-%m-%dT%H-%M-%S")
    partial = load_partial(str(semantic_path))
    done    = answered_keys(partial)
    answers: list[dict] = list(partial.get("answers", [])) if partial else []
    if done:
        print(f"[stage_03] Resuming — {len(done)} group(s) already answered.")
    total      = len(groups)
    unanswered = [g for g in groups if g.group_key not in done]
    for idx, g in enumerate(unanswered, start=len(done) + 1):
        _print_group_header(idx, total, g)
        answer = _ask_group(g, dry_run) if g.queue_type == "confirm" \
            else _ask_escalate(g, dry_run)
        if answer is None:
            save_partial(answers, str(semantic_path), ts)
            print("[stage_03] Partial session saved. Re-run to resume.")
            sys.exit(0)
        answers.append(answer)
        save_partial(answers, str(semantic_path), ts)
    stats = _print_summary(answers)
    out   = save_final(answers, stats, str(semantic_path), ts)
    print(f"\n  Output: {out}")
    return out


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Stage 03 — Human Review CLI")
    parser.add_argument("--input", "-i", required=True,
                        help="Path to semantic JSON from Stage 02")
    parser.add_argument("--dry-run", action="store_true",
                        help="Show questions without prompting (for testing)")
    args = parser.parse_args()
    run(Path(args.input), dry_run=args.dry_run)
