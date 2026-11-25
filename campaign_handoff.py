#!/usr/bin/env python3
"""Start one saved campaign only after another campaign completes successfully."""

from __future__ import annotations

import argparse
import json
import time

import campaign_manager as campaigns
from operator_control import clear


TERMINAL_TARGET_STATUSES = campaigns.ACTIVE_STATUSES | {"completed"}


def handoff_decision(source: dict, target: dict) -> str:
    if target["status"] in TERMINAL_TARGET_STATUSES:
        return "done"
    if source["status"] == "completed":
        return "launch"
    if source["status"] == "attention":
        return "source_attention"
    return "waiting"


def record(campaign_id: int, event: str, **detail) -> None:
    with campaigns.connect() as db:
        campaigns.log(db, campaign_id, event, **detail)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--after", type=int, required=True, help="Campaign that must finish first")
    parser.add_argument("--start", type=int, required=True, help="Campaign to start next")
    parser.add_argument("--poll-seconds", type=float, default=30)
    args = parser.parse_args()
    if args.after == args.start:
        parser.error("Source and target campaigns must be different.")
    if args.poll_seconds < 5:
        parser.error("Poll interval must be at least five seconds.")

    source = campaigns.get_campaign(args.after)
    target = campaigns.get_campaign(args.start)
    record(target["id"], "campaign_queued", after_campaign_id=source["id"],
           after_campaign_name=source["name"])
    print(json.dumps({"event": "queued", "after": source["name"], "start": target["name"]}), flush=True)

    reported_attention = False
    while True:
        source = campaigns.get_campaign(args.after)
        target = campaigns.get_campaign(args.start)
        decision = handoff_decision(source, target)
        if decision == "done":
            print(json.dumps({"event": "finished", "target_status": target["status"]}), flush=True)
            return 0
        if decision == "launch":
            clear()
            launched = campaigns.launch_campaign(target["id"])
            record(target["id"], "campaign_handoff_launched",
                   after_campaign_id=source["id"], after_campaign_name=source["name"])
            print(json.dumps({"event": "launched", "campaign": launched["name"]}), flush=True)
            return 0
        if decision == "source_attention" and not reported_attention:
            record(target["id"], "campaign_handoff_waiting_for_source_recovery",
                   after_campaign_id=source["id"], error=source.get("last_error"))
            print(json.dumps({"event": "waiting_for_source_recovery",
                              "source_error": source.get("last_error")}), flush=True)
            reported_attention = True
        elif decision != "source_attention":
            reported_attention = False
        time.sleep(args.poll_seconds)


if __name__ == "__main__":
    raise SystemExit(main())
