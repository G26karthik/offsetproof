"""Read-only evidence tools. Path escape or write attempt is refused."""

from __future__ import annotations

from pathlib import Path

ALLOWED_FILES = frozenset(
    {"metrics.csv", "consumer_logs.jsonl", "manifest.json", "runbook.md"}
)

# Anything outside the bundle dir — especially the evaluator's private
# truth — must be unreachable through these tools by construction.
FORBIDDEN_PARTS = frozenset({"private", "ground_truth", ".git"})


def read_evidence(bundle_dir: Path, name: str, max_chars: int = 20000) -> str:
    """Return the text of one allowlisted bundle file."""
    if name not in ALLOWED_FILES:
        raise ValueError(f"not an evidence file: {name}")
    target = (bundle_dir / name).resolve()
    base = bundle_dir.resolve()
    if not target.is_relative_to(base):
        raise ValueError(f"path escapes bundle: {name}")
    if any(part in FORBIDDEN_PARTS for part in target.parts):
        raise ValueError(f"forbidden path: {name}")
    text = target.read_text(encoding="utf-8", errors="replace")
    return text[:max_chars]


def search_runbook(bundle_dir: Path, keywords: list[str]) -> list[str]:
    """Return runbook lines matching any keyword (case-insensitive)."""
    try:
        text = read_evidence(bundle_dir, "runbook.md", max_chars=100000)
    except (ValueError, OSError):
        return []
    lowered = [k.lower() for k in keywords]
    return [
        line.strip()
        for line in text.splitlines()
        if line.strip() and any(k in line.lower() for k in lowered)
    ][:20]
