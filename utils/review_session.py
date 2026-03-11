from __future__ import annotations
"""
utils/review_session.py — save/load/resume partial review sessions.
"""
import json
from datetime import datetime
from pathlib import Path

_PARTIAL_PREFIX = "human_review_partial_"
_FINAL_PREFIX   = "human_review_"


def _outputs_dir() -> Path:
    return Path("outputs")


def load_partial(semantic_file: str) -> dict | None:
    """Find latest partial session for this semantic file. Returns dict or None."""
    candidates = sorted(_outputs_dir().glob(f"{_PARTIAL_PREFIX}*.json"), reverse=True)
    for p in candidates:
        try:
            d = json.loads(p.read_text(encoding="utf-8"))
            if d.get("source_semantic_file") == Path(semantic_file).name:
                return d
        except Exception:
            continue
    return None


def answered_keys(partial: dict | None) -> set[str]:
    if not partial:
        return set()
    return {a["group_key"] for a in partial.get("answers", [])}


def save_partial(answers: list[dict], semantic_file: str, ts: str) -> Path:
    path = _outputs_dir() / f"{_PARTIAL_PREFIX}{ts}.json"
    payload = {
        "source_semantic_file": Path(semantic_file).name,
        "saved_at": datetime.now().isoformat(),
        "answers": answers,
    }
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def save_final(answers: list[dict], stats: dict, semantic_file: str, ts: str) -> Path:
    path = _outputs_dir() / f"{_FINAL_PREFIX}{ts}.json"
    payload = {
        "source_semantic_file": Path(semantic_file).name,
        "reviewed_at": datetime.now().isoformat(),
        "schema_version": "1.0",
        "reviewer": "human",
        "stats": stats,
        "answers": answers,
    }
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    # clean up partial files for this semantic source
    for p in _outputs_dir().glob(f"{_PARTIAL_PREFIX}*.json"):
        try:
            d = json.loads(p.read_text(encoding="utf-8"))
            if d.get("source_semantic_file") == Path(semantic_file).name:
                p.unlink()
        except Exception:
            pass
    return path
