#!/usr/bin/env python3
"""Durable boundary between deterministic scraping, one AI review, and Apify.

This module never calls a model and never starts a paid Apify job. It exports a
bounded review queue, validates model decisions, records them durably, and
prepares the niche-verified phone input for the deterministic phone validator.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import sys

PROJECT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT))

import campaign_candidates  # noqa: E402
from prospect import meets_reputation_floor  # noqa: E402


DB_PATH = PROJECT / "state" / "ai_review.sqlite3"
LABELS = {"pass", "adjacent", "not_pass", "unclear"}
DECISION_FIELDS = ("listing_id", "label")


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def connect(path: Path = DB_PATH) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(path, timeout=30)
    db.row_factory = sqlite3.Row
    db.executescript("""
        PRAGMA journal_mode=WAL;
        PRAGMA synchronous=FULL;
        CREATE TABLE IF NOT EXISTS review_batches(
          campaign_slug TEXT NOT NULL,
          state TEXT NOT NULL,
          niche TEXT NOT NULL,
          queue_path TEXT NOT NULL,
          queue_sha256 TEXT NOT NULL,
          queue_count INTEGER NOT NULL,
          exported_utc TEXT NOT NULL,
          PRIMARY KEY(campaign_slug, queue_sha256)
        );
        CREATE TABLE IF NOT EXISTS decisions(
          campaign_slug TEXT NOT NULL,
          listing_id TEXT NOT NULL,
          label TEXT NOT NULL,
          model TEXT NOT NULL,
          queue_sha256 TEXT NOT NULL,
          decided_utc TEXT NOT NULL,
          imported_utc TEXT NOT NULL,
          PRIMARY KEY(campaign_slug, listing_id)
        );
    """)
    return db


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8-sig", newline="") as stream:
        return list(csv.DictReader(stream))


def export_queue(campaign_slug: str, state: str, niche: str, output: Path,
                 queue_db: Path = campaign_candidates.QUEUE_DB,
                 profile_evidence: Path | None = None,
                 db_path: Path = DB_PATH) -> dict:
    evidence = profile_evidence or output.parent / "profile-evidence" / "eligible-candidates.csv"
    rows = campaign_candidates.export_ai_queue(state, niche, output, queue_db, evidence)
    fingerprint = sha256(output)
    with connect(db_path) as db:
        db.execute("""INSERT OR REPLACE INTO review_batches
          (campaign_slug,state,niche,queue_path,queue_sha256,queue_count,exported_utc)
          VALUES(?,?,?,?,?,?,?)""",
          (campaign_slug, state, niche, str(output), fingerprint, len(rows), utc_now()))
        db.commit()
    return {"campaign": campaign_slug, "ready_for_ai": len(rows),
            "queue": str(output), "queue_sha256": fingerprint,
            "profile_evidence": str(evidence), "profile_check_required": not evidence.is_file()}


def import_decisions(campaign_slug: str, queue: Path, decisions: Path, model: str,
                     db_path: Path = DB_PATH) -> dict:
    queue_rows = read_csv(queue)
    decision_rows = read_csv(decisions)
    queue_ids = {row.get("listing_id", "").strip() for row in queue_rows}
    queue_ids.discard("")
    if len(queue_ids) != len(queue_rows):
        raise ValueError("AI queue has missing or duplicate listing_id values; nothing imported.")
    parsed: dict[str, str] = {}
    errors: list[str] = []
    for number, row in enumerate(decision_rows, 2):
        listing_id = row.get("listing_id", "").strip()
        label = row.get("label", "").strip().lower()
        if not listing_id or listing_id not in queue_ids:
            errors.append(f"row {number}: listing_id is not in this queue")
        elif label not in LABELS:
            errors.append(f"row {number}: label must be one of {', '.join(sorted(LABELS))}")
        elif listing_id in parsed:
            errors.append(f"row {number}: duplicate listing_id")
        else:
            parsed[listing_id] = label
    if errors:
        raise ValueError("Decision file rejected; " + "; ".join(errors[:20]))
    if not parsed:
        raise ValueError("Decision file contains no valid decisions.")
    fingerprint = sha256(queue)
    timestamp = utc_now()
    with connect(db_path) as db:
        known = db.execute("""SELECT 1 FROM review_batches
          WHERE campaign_slug=? AND queue_sha256=?""", (campaign_slug, fingerprint)).fetchone()
        if not known:
            raise ValueError("Queue fingerprint is not registered for this campaign. Export it first.")
        db.executemany("""INSERT INTO decisions
          (campaign_slug,listing_id,label,model,queue_sha256,decided_utc,imported_utc)
          VALUES(?,?,?,?,?,?,?)
          ON CONFLICT(campaign_slug,listing_id) DO UPDATE SET
            label=excluded.label,model=excluded.model,queue_sha256=excluded.queue_sha256,
            decided_utc=excluded.decided_utc,imported_utc=excluded.imported_utc""",
          [(campaign_slug, listing_id, label, model, fingerprint, timestamp, timestamp)
           for listing_id, label in parsed.items()])
        db.commit()
    counts = {label: sum(value == label for value in parsed.values()) for label in sorted(LABELS)}
    return {"campaign": campaign_slug, "imported": len(parsed), **counts}


def prepare_phone_input(campaign_slug: str, queue: Path, output: Path,
                        db_path: Path = DB_PATH) -> dict:
    queue_rows = read_csv(queue)
    with connect(db_path) as db:
        decisions = {row["listing_id"]: row["label"] for row in db.execute(
            "SELECT listing_id,label FROM decisions WHERE campaign_slug=?", (campaign_slug,)
        )}
    selected = []
    missing_phone = 0
    for row in queue_rows:
        if decisions.get(row.get("listing_id", "")) != "pass" or not meets_reputation_floor(row):
            continue
        phone = row.get("phone", "").strip()
        if not phone:
            missing_phone += 1
            continue
        selected.append({
            "listing_id": row.get("listing_id", ""),
            "name": row.get("name", ""),
            "final_phone": phone,
            "maps_url": row.get("maps_url", ""),
            "rating": row.get("rating", ""),
            "reviews": row.get("reviews", ""),
            "category": row.get("category", ""),
        })
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = output.with_suffix(output.suffix + ".tmp")
    fields = ("listing_id", "name", "final_phone", "maps_url", "rating", "reviews", "category")
    with temporary.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields)
        writer.writeheader()
        writer.writerows(selected)
    temporary.replace(output)
    return {"campaign": campaign_slug, "ready_for_apify": len(selected),
            "niche_verified_without_phone": missing_phone, "output": str(output)}


def status(campaign_slug: str, db_path: Path = DB_PATH) -> dict:
    with connect(db_path) as db:
        batch = db.execute("""SELECT queue_count,queue_path,queue_sha256,exported_utc
          FROM review_batches WHERE campaign_slug=? ORDER BY exported_utc DESC LIMIT 1""",
          (campaign_slug,)).fetchone()
        counts = {row["label"]: row["n"] for row in db.execute(
            "SELECT label,COUNT(*) n FROM decisions WHERE campaign_slug=? GROUP BY label",
            (campaign_slug,))}
    return {"campaign": campaign_slug, "queue_count": batch["queue_count"] if batch else 0,
            "reviewed": sum(counts.values()), "labels": counts,
            "queue": batch["queue_path"] if batch else ""}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    export = sub.add_parser("export")
    export.add_argument("--campaign", required=True); export.add_argument("--state", required=True)
    export.add_argument("--niche", required=True); export.add_argument("--output", type=Path, required=True)
    export.add_argument("--profile-evidence", type=Path)
    load = sub.add_parser("import-decisions")
    load.add_argument("--campaign", required=True); load.add_argument("--queue", type=Path, required=True)
    load.add_argument("--decisions", type=Path, required=True); load.add_argument("--model", required=True)
    prep = sub.add_parser("prepare-phone")
    prep.add_argument("--campaign", required=True); prep.add_argument("--queue", type=Path, required=True)
    prep.add_argument("--output", type=Path, required=True)
    show = sub.add_parser("status"); show.add_argument("--campaign", required=True)
    args = parser.parse_args()
    if args.command == "export":
        result = export_queue(args.campaign, args.state.upper(), args.niche, args.output,
                              profile_evidence=args.profile_evidence)
    elif args.command == "import-decisions":
        result = import_decisions(args.campaign, args.queue, args.decisions, args.model)
    elif args.command == "prepare-phone":
        result = prepare_phone_input(args.campaign, args.queue, args.output)
    else:
        result = status(args.campaign)
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
