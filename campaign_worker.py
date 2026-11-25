#!/usr/bin/env python3
"""Run one named Google Maps Scraper campaign from durable checkpoints."""

from __future__ import annotations

import argparse
import json
import sqlite3
import subprocess
import sys
import time
from pathlib import Path

import campaign_manager as campaigns
from geography.catalog import make_plan
from operator_control import PAUSE_ACTIONS, current_action


ROOT = Path(__file__).resolve().parent
QUEUE_DB = ROOT / "runs" / "local-queue" / "queue.sqlite3"


def scope_counts(state: str, niche: str, baseline_only: bool = False) -> dict[str, int]:
    if not QUEUE_DB.is_file():
        return {}
    db = sqlite3.connect(QUEUE_DB)
    try:
        baseline = "AND json_extract(spec,'$.scope_id') NOT LIKE '%:phrase:%'" if baseline_only else ""
        rows = db.execute(f"""
            SELECT stage,count(*) FROM jobs
            WHERE state_code=?
              AND coalesce(json_extract(spec,'$.niche'),'auto detailing')=?
              {baseline}
            GROUP BY stage
        """, (state, niche)).fetchall()
        return dict(rows)
    finally:
        db.close()


def planner_outcomes(state: str, niche: str) -> list[dict]:
    if not QUEUE_DB.is_file():
        return []
    db = sqlite3.connect(QUEUE_DB)
    db.row_factory = sqlite3.Row
    try:
        rows = db.execute("""
            SELECT spec,stage,capture,decisions FROM jobs
            WHERE state_code=?
              AND coalesce(json_extract(spec,'$.niche'),'auto detailing')=?
        """, (state, niche)).fetchall()
    finally:
        db.close()
    outcomes = []
    for row in rows:
        spec = json.loads(row["spec"])
        scope_id = spec.get("scope_id")
        if not scope_id or row["stage"] not in {"done", "blocked"}:
            continue
        capture = json.loads(row["capture"]) if row["capture"] else {}
        decisions = json.loads(row["decisions"]) if row["decisions"] else []
        outcomes.append({
            "scope_id": scope_id,
            "stop": capture.get("stop", "blocked" if row["stage"] == "blocked" else "maps_end_of_list"),
            "new_qualified": sum(item.get("decision") == "qualified" for item in decisions),
        })
    return outcomes


def run_campaign(campaign_id: int, jobs: int = 100) -> int:
    campaign = campaigns.get_campaign(campaign_id)
    config = Path(campaign["config_path"])
    campaigns.update_status(campaign_id, "running")
    unchanged = 0
    previous = None
    while True:
        action = current_action()
        if action in PAUSE_ACTIONS | {"stop_now"}:
            campaigns.update_status(campaign_id, "paused")
            return 0
        before = scope_counts(campaign["state"], campaign["niche"])
        blocked = before.get("blocked", 0)
        pending = sum(before.get(key, 0) for key in ("pending", "captured", "active"))
        next_scope = make_plan(
            campaign["state"], campaign["niche"], 1,
            planner_outcomes(campaign["state"], campaign["niche"]), campaign["alternate"],
        )
        if not pending and not next_scope:
            status = "attention" if blocked else "completed"
            campaigns.update_status(campaign_id, status,
                                    f"{blocked} blocked areas need review" if blocked else None)
            return 0
        command = [
            sys.executable, str(ROOT / "prospect_queue.py"),
            "--campaign-file", str(config), "--jobs", str(jobs),
            "--browsers", "1", "--tabs-per-browser", "10",
            "--independent-browser-slots", "--full-scroll", "--discovery-only",
        ]
        result = subprocess.run(command, cwd=ROOT)
        after = scope_counts(campaign["state"], campaign["niche"])
        if current_action() in PAUSE_ACTIONS | {"stop_now"}:
            campaigns.update_status(campaign_id, "paused")
            return 0
        if result.returncode:
            campaigns.update_status(campaign_id, "attention",
                                    f"Scraper exited with code {result.returncode}")
            return result.returncode
        signature = json.dumps(after, sort_keys=True)
        unchanged = unchanged + 1 if signature == previous else 0
        previous = signature
        if unchanged >= 2:
            campaigns.update_status(campaign_id, "attention", "No geography progress after two batches")
            return 1
        time.sleep(3)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--campaign-id", type=int, required=True)
    parser.add_argument("--jobs", type=int, default=100)
    args = parser.parse_args()
    try:
        return run_campaign(args.campaign_id, args.jobs)
    except Exception as error:
        campaigns.update_status(args.campaign_id, "attention", str(error))
        raise


if __name__ == "__main__":
    raise SystemExit(main())
