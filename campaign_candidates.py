#!/usr/bin/env python3
"""Build human-facing campaign lead counts and the AI review queue."""

from __future__ import annotations

import csv
import json
import sqlite3
import time
from pathlib import Path

from prospect import meets_reputation_floor


ROOT = Path(__file__).resolve().parent
QUEUE_DB = ROOT / "runs" / "local-queue" / "queue.sqlite3"
FIELDS = (
    "listing_id", "name", "category", "rating", "reviews", "phone", "maps_url",
    "source_query", "state", "niche", "website_status",
)
_CACHE: dict[tuple[str, str, str], tuple[float, dict]] = {}


def _rows_query() -> str:
    return """
        SELECT d.value, json_extract(j.spec,'$.query') source_query
        FROM jobs j, json_each(j.decisions) d
        WHERE j.state_code=?
          AND coalesce(json_extract(j.spec,'$.niche'),'auto detailing')=?
          AND j.decisions IS NOT NULL
    """


def _all_rows_query() -> str:
    return """
        SELECT d.value, json_extract(j.spec,'$.query') source_query
        FROM jobs j, json_each(j.decisions) d
        WHERE j.decisions IS NOT NULL
    """


def card_filter(row: dict) -> bool:
    return (
        bool(row.get("listing_id"))
        and row.get("website_button") == "no"
        and not row.get("website_url")
        and meets_reputation_floor(row)
    )


def basic_filter(row: dict) -> bool:
    """Compatibility alias for the pre-profile card filter."""
    return card_filter(row)


def profile_evidence_rows(path: Path) -> list[dict]:
    """Return only full-profile rows that satisfy the user's Ready-for-AI gate."""
    if not path.is_file():
        return []
    with path.open(encoding="utf-8-sig", newline="") as stream:
        rows = list(csv.DictReader(stream))
    return [row for row in rows
            if row.get("decision") == "eligible_needs_relevance_review"
            and meets_reputation_floor(row)
            and not row.get("website")
            and row.get("final_phone")
            and row.get("closure_status") not in {"temporarily_closed", "permanently_closed"}]


def campaign_rows(state: str, niche: str, queue_db: Path = QUEUE_DB) -> list[dict]:
    if not queue_db.is_file():
        return []
    best: dict[str, tuple[tuple, dict]] = {}
    db = sqlite3.connect(f"file:{queue_db}?mode=ro", uri=True, timeout=1)
    try:
        for payload, source_query in db.execute(_rows_query(), (state, niche)):
            row = json.loads(payload)
            listing_id = row.get("listing_id")
            if not listing_id:
                continue
            score = (
                int(bool(row.get("phone"))),
                int(bool(row.get("category"))),
                int(row.get("reviews") or 0),
            )
            if listing_id not in best or score > best[listing_id][0]:
                best[listing_id] = (score, {**row, "source_query": source_query})
    finally:
        db.close()
    return [item[1] for item in best.values()]


def all_campaign_rows(queue_db: Path = QUEUE_DB) -> list[dict]:
    """Return every observed business once across all campaigns and searches."""
    if not queue_db.is_file():
        return []
    best: dict[str, tuple[tuple, dict]] = {}
    db = sqlite3.connect(f"file:{queue_db}?mode=ro", uri=True, timeout=1)
    try:
        for payload, source_query in db.execute(_all_rows_query()):
            row = json.loads(payload)
            listing_id = row.get("listing_id")
            if not listing_id:
                continue
            score = (
                int(bool(row.get("phone"))),
                int(bool(row.get("category"))),
                int(row.get("reviews") or 0),
            )
            if listing_id not in best or score > best[listing_id][0]:
                best[listing_id] = (score, {**row, "source_query": source_query})
    finally:
        db.close()
    return [item[1] for item in best.values()]


def campaign_metrics(state: str, niche: str, queue_db: Path = QUEUE_DB,
                     refresh_seconds: float = 10) -> dict:
    key = (str(queue_db), state, niche)
    cached = _CACHE.get(key)
    if cached and time.monotonic() - cached[0] < refresh_seconds:
        return dict(cached[1])
    rows = campaign_rows(state, niche, queue_db)
    card_matches = [row for row in rows if card_filter(row)]
    value = {
        "businesses_found": len(rows),
        "passed_card_filters": len(card_matches),
        "ready_for_ai": 0,
        "ai_reviewed": 0,
        "niche_verified": 0,
        "mobile_verified": 0,
        "added_to_ghl": 0,
    }
    _CACHE[key] = (time.monotonic(), value)
    return dict(value)


def all_campaign_metrics(queue_db: Path = QUEUE_DB,
                         refresh_seconds: float = 10) -> dict:
    key = (str(queue_db), "*", "*")
    cached = _CACHE.get(key)
    if cached and time.monotonic() - cached[0] < refresh_seconds:
        return dict(cached[1])
    rows = all_campaign_rows(queue_db)
    value = {
        "businesses_found": len(rows),
        "passed_card_filters": sum(1 for row in rows if card_filter(row)),
        "ready_for_ai": 0,
        "ai_reviewed": 0,
        "niche_verified": 0,
        "mobile_verified": 0,
        "added_to_ghl": 0,
    }
    _CACHE[key] = (time.monotonic(), value)
    return dict(value)


def export_ai_queue(state: str, niche: str, output: Path,
                    queue_db: Path = QUEUE_DB,
                    profile_evidence: Path | None = None) -> list[dict]:
    rows = profile_evidence_rows(profile_evidence) if profile_evidence else []
    rows.sort(key=lambda row: (-int(row.get("reviews") or 0), (row.get("name") or "").casefold()))
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = output.with_suffix(".tmp")
    with temporary.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, FIELDS)
        writer.writeheader()
        for source in rows:
            row = {
                "listing_id": source.get("listing_id", ""),
                "name": source.get("name", ""),
                "category": source.get("profile_category") or source.get("card_category", ""),
                "rating": source.get("rating", ""),
                "reviews": source.get("reviews", ""),
                "phone": source.get("final_phone", ""),
                "maps_url": source.get("maps_url", ""),
                "source_query": source.get("source_query") or source.get("states", ""),
                "state": state,
                "niche": niche,
                "website_status": "No website linked on full Google Maps profile",
            }
            for field in ("name", "category", "source_query"):
                if row[field].lstrip().startswith(("=", "+", "-", "@")):
                    row[field] = "'" + row[field]
            writer.writerow(row)
    temporary.replace(output)
    return rows
