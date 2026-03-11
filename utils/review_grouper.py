from __future__ import annotations
"""
utils/review_grouper.py — groups confirm/escalate queue items for human review.
Group key = (finding_type, hypothesis, question_for_human, sheet)
"""
from dataclasses import dataclass, field
from typing import Any


@dataclass
class ReviewGroup:
    group_key: str
    finding_type: str
    sheet: str
    hypothesis: str
    question: str
    confidence: float
    queue_type: str         # "confirm" | "escalate"
    items: list[dict] = field(default_factory=list)

    @property
    def size(self) -> int:
        return len(self.items)

    @property
    def item_ids(self) -> list[str]:
        return [f"{i['sheet']}::{i['field']}" for i in self.items]

    @property
    def field_names(self) -> list[str]:
        return [i["field"] for i in self.items]

    @property
    def evidence_sample(self) -> str:
        for item in self.items:
            evs = item.get("evidence", [])
            if evs:
                return evs[0]
        return "—"


def build_groups(semantic: dict) -> list[ReviewGroup]:
    """Group items by (finding_type, hypothesis, question_for_human, sheet).
    Confirm queue first, then escalate queue.
    """
    groups: dict[str, ReviewGroup] = {}

    def _add(item: dict[str, Any], queue_type: str) -> None:
        ft   = item.get("finding_type", "unknown")
        hyp  = item.get("hypothesis", "unknown")
        q    = item.get("question_for_human", "Перевірте це поле.")
        sh   = item.get("sheet", "")
        conf = float(item.get("confidence", 0.0))
        key  = f"{ft}::{hyp}::{q}::{sh}"
        if key not in groups:
            groups[key] = ReviewGroup(
                group_key=key, finding_type=ft, sheet=sh,
                hypothesis=hyp, question=q, confidence=conf,
                queue_type=queue_type,
            )
        groups[key].items.append(item)

    for item in semantic.get("confirm_queue", []):
        _add(item, "confirm")
    for item in semantic.get("escalate_queue", []):
        _add(item, "escalate")
    return list(groups.values())
