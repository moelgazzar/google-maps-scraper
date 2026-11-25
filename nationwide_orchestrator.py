#!/usr/bin/env python3
"""Continue resumable statewide campaigns until the candidate target is reached."""

import argparse
import fcntl
import json
import random
import sqlite3
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

from operator_control import PAUSE_ACTIONS, current_action


ROOT = Path(__file__).resolve().parent
RUNTIME = ROOT / ".runtime"
RUNS = ROOT / "runs"
DB = RUNS / "local-queue" / "queue.sqlite3"
STATUS = RUNS / "local-queue" / "status.json"
PROSPECTING_LOCK = RUNTIME / "prospecting.lock"
ORCHESTRATOR_LOCK = RUNTIME / "nationwide-orchestrator.lock"
LOG = RUNS / "nationwide-orchestrator.log"
STATE_ROOT = ROOT / "State Campaigns"

STATE_NAMES = {
    "FL": "Florida", "GA": "Georgia", "AL": "Alabama", "SC": "South Carolina",
    "NC": "North Carolina", "TX": "Texas", "TN": "Tennessee", "VA": "Virginia",
    "LA": "Louisiana", "MS": "Mississippi", "AR": "Arkansas", "OK": "Oklahoma",
    "KY": "Kentucky", "WV": "West Virginia", "MD": "Maryland", "DE": "Delaware",
    "DC": "District of Columbia",
    "PA": "Pennsylvania", "OH": "Ohio", "NY": "New York", "NJ": "New Jersey",
    "CT": "Connecticut", "RI": "Rhode Island", "MA": "Massachusetts",
    "VT": "Vermont", "NH": "New Hampshire", "ME": "Maine", "MI": "Michigan",
    "IN": "Indiana", "IL": "Illinois", "WI": "Wisconsin", "MN": "Minnesota",
    "IA": "Iowa", "MO": "Missouri", "KS": "Kansas", "NE": "Nebraska",
    "SD": "South Dakota", "ND": "North Dakota", "MT": "Montana", "WY": "Wyoming",
    "CO": "Colorado", "NM": "New Mexico", "AZ": "Arizona", "UT": "Utah",
    "NV": "Nevada", "CA": "California", "OR": "Oregon", "WA": "Washington",
    "ID": "Idaho", "AK": "Alaska", "HI": "Hawaii",
}

# Florida first, then nearby states and Texas as requested, followed by the rest
# of the country. Deduplication remains global in the shared SQLite ledger.
STATE_ORDER = tuple(STATE_NAMES)


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def emit(message, **detail):
    record = {"utc": utc_now(), "message": message, **detail}
    line = json.dumps(record, ensure_ascii=False)
    print(line, flush=True)
    LOG.parent.mkdir(parents=True, exist_ok=True)
    with LOG.open("a", encoding="utf-8") as stream:
        stream.write(line + "\n")


def connect():
    db = sqlite3.connect(DB)
    db.row_factory = sqlite3.Row
    return db


def qualified_count():
    if not DB.is_file():
        return 0
    with connect() as db:
        return db.execute("SELECT count(*) FROM qualified").fetchone()[0]


def latest_run_id():
    if not DB.is_file():
        return 0
    with connect() as db:
        row = db.execute("SELECT id FROM events WHERE event='run_started' ORDER BY id DESC LIMIT 1").fetchone()
        return row[0] if row else 0


def state_counts(state):
    if not DB.is_file():
        return {}
    with connect() as db:
        return {row["stage"]: row["count"] for row in db.execute(
            "SELECT stage,count(*) count FROM jobs WHERE listed=1 AND state_code=? GROUP BY stage",
            (state,),
        )}


def current_status():
    try:
        return json.loads(STATUS.read_text()).get("state", "unknown")
    except (OSError, json.JSONDecodeError):
        return "unknown"


def operator_halt_requested():
    return current_action() in PAUSE_ACTIONS | {"stop_now"}


def prospecting_active():
    PROSPECTING_LOCK.parent.mkdir(parents=True, exist_ok=True)
    with PROSPECTING_LOCK.open("a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return True
        fcntl.flock(lock, fcntl.LOCK_UN)
        return False


def ensure_campaign(state):
    folder = STATE_ROOT / STATE_NAMES[state]
    folder.mkdir(parents=True, exist_ok=True)
    campaign = folder / "campaign.json"
    expected = {
        "states": [state],
        "niche": "auto detailing",
        "alternate": "mobile detailing",
        "geography_filter": "state",
    }
    if campaign.exists():
        saved = json.loads(campaign.read_text())
        if saved.get("states") != [state] or saved.get("niche") != expected["niche"]:
            raise ValueError(f"Refusing to replace incompatible campaign: {campaign}")
    else:
        campaign.write_text(json.dumps(expected, indent=2) + "\n")
    return campaign


def adaptive_wait(streak, previous=0, minimum=30, maximum=90, cap=300,
                  minimum_step=15):
    multiplier = 1.5 ** max(0, streak - 1)
    low = min(cap, minimum * multiplier)
    high = min(cap, maximum * multiplier)
    if previous:
        low = min(cap, max(low, previous + minimum_step))
    return round(random.uniform(low, max(low, high)), 2)


def pending_recovery():
    """Return an interrupted recovery sleep recorded as the latest log event."""
    try:
        lines = LOG.read_text(encoding="utf-8").splitlines()
        record = json.loads(lines[-1]) if lines else {}
    except (OSError, json.JSONDecodeError):
        return None
    if record.get("message") != "adaptive_recovery_wait":
        return None
    try:
        started = datetime.fromisoformat(record["utc"])
        delay = float(record["wait_seconds"])
        streak = int(record["consecutive_failures"])
    except (KeyError, TypeError, ValueError):
        return None
    remaining = max(0, (started + timedelta(seconds=delay) -
                        datetime.now(timezone.utc)).total_seconds())
    return streak, delay, remaining


def run_campaign(state, jobs, retry_blocked=False):
    campaign = ensure_campaign(state)
    command = [
        sys.executable, str(ROOT / "prospect_queue.py"),
        "--campaign-file", str(campaign),
        "--jobs", str(jobs),
        "--browsers", "1",
        "--tabs-per-browser", "10",
        "--independent-browser-slots",
        "--full-scroll",
    ]
    if state == "FL":
        command.append("--florida-regions")
    if retry_blocked:
        command.append("--retry-blocked")
    emit("campaign_started", state=state, retry_blocked=retry_blocked,
         jobs=jobs, candidates=qualified_count())
    result = subprocess.run(command, cwd=ROOT)
    emit("campaign_finished", state=state, retry_blocked=retry_blocked,
         returncode=result.returncode, status=current_status(),
         counts=state_counts(state), candidates=qualified_count())
    return result.returncode


def wait_for_current_run(poll_seconds=30):
    announced = False
    while prospecting_active():
        if not announced:
            emit("waiting_for_active_florida_run", candidates=qualified_count())
            announced = True
        time.sleep(poll_seconds)
    if announced:
        emit("active_run_released_lock", status=current_status(), candidates=qualified_count())


def orchestrate(target, jobs):
    wait_for_current_run()
    if operator_halt_requested():
        emit("operator_control_observed", action=current_action(),
             boundary="before_state_queue")
        return 0
    failure_streak = 0
    last_delay = 0
    recovery = pending_recovery()
    if recovery:
        failure_streak, last_delay, remaining = recovery
        if remaining:
            emit("resuming_recovery_wait", wait_seconds=round(remaining, 2),
                 consecutive_failures=failure_streak,
                 previous_wait_seconds=last_delay)
            time.sleep(remaining)
    for state in STATE_ORDER:
        normal_empty = False
        blocked_passes = 0
        while qualified_count() < target:
            if operator_halt_requested():
                emit("operator_control_observed", action=current_action(),
                     boundary="before_campaign", state=state)
                return 0
            status = current_status()
            if status in {"paused", "error", "capacity_limited"} and failure_streak == 0:
                failure_streak += 1
                delay = adaptive_wait(failure_streak, previous=last_delay)
                last_delay = delay
                emit("adaptive_recovery_wait", state=state, status=status,
                     wait_seconds=delay, consecutive_failures=failure_streak)
                time.sleep(delay)
            before = latest_run_id()
            retry = normal_empty and blocked_passes < 2
            returncode = run_campaign(state, jobs, retry_blocked=retry)
            if operator_halt_requested():
                emit("operator_control_observed", action=current_action(),
                     boundary="after_campaign", state=state)
                return 0
            after = latest_run_id()
            status = current_status()
            if returncode or status in {"paused", "error", "capacity_limited"}:
                failure_streak += 1
                delay = adaptive_wait(failure_streak, previous=last_delay)
                last_delay = delay
                emit("adaptive_recovery_wait", state=state, status=status,
                     returncode=returncode, wait_seconds=delay,
                     consecutive_failures=failure_streak)
                time.sleep(delay)
                continue
            failure_streak = 0
            last_delay = 0
            if after == before:
                if not normal_empty:
                    normal_empty = True
                    if state_counts(state).get("blocked", 0):
                        emit("retrying_held_scopes", state=state,
                             blocked=state_counts(state)["blocked"])
                        continue
                elif retry:
                    blocked_passes += 1
                    if state_counts(state).get("blocked", 0) and blocked_passes < 2:
                        continue
                break
            if retry:
                blocked_passes += 1
                if blocked_passes >= 2:
                    break
        emit("state_complete", state=state, counts=state_counts(state),
             candidates=qualified_count())
        if qualified_count() >= target:
            break
    total = qualified_count()
    emit("target_reached" if total >= target else "all_states_exhausted",
         target=target, candidates=total)
    return 0 if total >= target else 2


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--target", type=int, default=15000)
    parser.add_argument("--jobs-per-run", type=int, default=500)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    if args.target < 1 or args.jobs_per_run < 1:
        parser.error("Target and jobs per run must be positive.")
    if args.dry_run:
        print(json.dumps({
            "target": args.target,
            "jobs_per_run": args.jobs_per_run,
            "current_candidates": qualified_count(),
            "prospecting_active": prospecting_active(),
            "state_order": STATE_ORDER,
        }, indent=2))
        return
    RUNTIME.mkdir(parents=True, exist_ok=True)
    with ORCHESTRATOR_LOCK.open("a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            parser.exit(1, "A nationwide orchestrator is already active.\n")
        raise SystemExit(orchestrate(args.target, args.jobs_per_run))


if __name__ == "__main__":
    main()
