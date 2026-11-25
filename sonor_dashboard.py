#!/usr/bin/env python3
"""Single-terminal operator dashboard for Google Maps Scraper."""

from __future__ import annotations

import argparse
import csv
import json
import os
import math
import re
import select
import shutil
import signal
import socket
import sqlite3
import subprocess
import sys
import termios
import time
import tty
from collections import Counter
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path

import campaign_manager
import campaign_candidates
from operator_control import CONTROL, clear, read_control, request


ROOT = Path(__file__).resolve().parent
QUEUE_DB = ROOT / "runs" / "local-queue" / "queue.sqlite3"
QUEUE_STATUS = ROOT / "runs" / "local-queue" / "status.json"
RECHECK_DB = ROOT / "runs" / "profile-recheck" / "recheck.sqlite3"
RECHECK_STATUS = ROOT / "runs" / "profile-recheck" / "status.json"
PHONE_DB = ROOT / "state" / "phone_verification.sqlite3"
AI_REVIEW_DB = ROOT / "state" / "ai_review.sqlite3"
GHL_MANIFEST = ROOT / "state" / "ghl_sms_outreach_contacts.csv"
APIFY_PROFILES_CSV = ROOT / "state" / "apify_profiles.csv"
ROUTE_STATE = ROOT / ".runtime" / "nord-proxy-state.json"
RUNNER_NAMES = {"campaign_worker.py", "prospect_queue.py", "profile_recheck.py", "nationwide_orchestrator.py"}
_STAGE_CACHE: dict[tuple[str, str, str], tuple[float, dict]] = {}

RESET = "\033[0m"
BOLD = "\033[1m"
DIM = "\033[2m"
GREEN = "\033[32m"
RED = "\033[31m"
CYAN = "\033[36m"
BLUE = "\033[34m"
MAGENTA = "\033[35m"
WHITE = "\033[97m"
BLACK = "\033[30m"
BG_BLUE = "\033[44m"
BG_GREEN = "\033[42m"
BG_RED = "\033[41m"


def json_file(path: Path) -> dict:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}


@contextmanager
def ro_db(path: Path):
    connection = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=0.25)
    connection.row_factory = sqlite3.Row
    try:
        yield connection
    finally:
        connection.close()


def runner_processes(root: Path = ROOT) -> list[dict]:
    found = []
    proc = Path("/proc")
    if not proc.is_dir():
        return found
    root_text = str(root)
    for entry in proc.iterdir():
        if not entry.name.isdigit():
            continue
        try:
            command = (entry / "cmdline").read_bytes().replace(b"\0", b" ").decode()
            cwd = str((entry / "cwd").resolve())
        except (OSError, UnicodeDecodeError):
            continue
        name = next((item for item in RUNNER_NAMES if item in command), "")
        if name and (root_text in command or cwd == root_text):
            found.append({"pid": int(entry.name), "name": name})
    return sorted(found, key=lambda item: item["pid"])


def route_city(host: str) -> str:
    if not host:
        return "—"
    stem = host.split(".", 1)[0].replace("-", " ").strip()
    return stem.title() if stem else "—"


def fmt_int(value) -> str:
    try:
        return f"{int(value):,}"
    except (TypeError, ValueError):
        return "—"


def fmt_duration(seconds) -> str:
    if seconds is None:
        return "—"
    seconds = max(0, int(seconds))
    hours, remainder = divmod(seconds, 3600)
    minutes, secs = divmod(remainder, 60)
    if hours:
        return f"{hours}h {minutes:02d}m"
    if minutes:
        return f"{minutes}m {secs:02d}s"
    return f"{secs}s"


def parse_utc(value: str):
    try:
        return datetime.fromisoformat(value)
    except (TypeError, ValueError):
        return None


def progress(done: int, total: int, width: int = 18) -> str:
    if total <= 0:
        return "░" * width
    filled = min(width, max(0, round(width * done / total)))
    return "█" * filled + "░" * (width - filled)


def collect_queue() -> dict:
    result = {"states": {}, "qualified": 0, "events": [], "performance": {}}
    if not QUEUE_DB.is_file():
        return result
    try:
        with ro_db(QUEUE_DB) as db:
            for row in db.execute("""
                SELECT state_code,stage,count(*) count
                FROM jobs GROUP BY state_code,stage ORDER BY state_code,stage
            """):
                result["states"].setdefault(row["state_code"], {})[row["stage"]] = row["count"]
            result["qualified"] = db.execute("SELECT count(*) FROM qualified").fetchone()[0]
            for row in db.execute("SELECT utc,event,detail FROM events ORDER BY id DESC LIMIT 8"):
                detail = json.loads(row["detail"])
                result["events"].append({"utc": row["utc"], "event": row["event"], **detail})
            start = db.execute("""
                SELECT id,utc,detail FROM events WHERE event='run_started'
                ORDER BY id DESC LIMIT 1
            """).fetchone()
            if start:
                stop = db.execute("""
                    SELECT id,utc,detail FROM events WHERE event='run_stopped' AND id>?
                    ORDER BY id LIMIT 1
                """, (start["id"],)).fetchone()
                finish_id = stop["id"] if stop else 1 << 62
                counts = {row["event"]: row["count"] for row in db.execute("""
                    SELECT event,count(*) count FROM events
                    WHERE id>? AND id<? GROUP BY event
                """, (start["id"], finish_id))}
                cards = db.execute("""
                    SELECT detail FROM events WHERE id>? AND id<? AND event='capture_saved'
                """, (start["id"], finish_id)).fetchall()
                started = parse_utc(start["utc"])
                finished = parse_utc(stop["utc"]) if stop else datetime.now(timezone.utc)
                elapsed = max(0.001, (finished - started).total_seconds()) if started and finished else 0
                result["performance"] = {
                    "active": not bool(stop),
                    "elapsed": elapsed,
                    "scopes": counts.get("scope_complete", 0),
                    "cards": sum(json.loads(item["detail"]).get("cards", 0) for item in cards),
                    "profiles": counts.get("profile_checked", 0),
                }
    except (sqlite3.Error, OSError, json.JSONDecodeError) as error:
        result["error"] = str(error)
    return result


def collect_recheck() -> dict:
    result = {"total": 0, "status": {}, "decisions": {}, "relevance": {}}
    if not RECHECK_DB.is_file():
        return result
    try:
        with ro_db(RECHECK_DB) as db:
            result["total"] = db.execute("SELECT count(*) FROM cohort").fetchone()[0]
            result["status"] = {row[0]: row[1] for row in db.execute(
                "SELECT status,count(*) FROM cohort GROUP BY status")}
            result["decisions"] = {row[0]: row[1] for row in db.execute(
                "SELECT decision,count(*) FROM cohort GROUP BY decision")}
            result["relevance"] = {row[0]: row[1] for row in db.execute(
                "SELECT label,count(*) FROM relevance_fast GROUP BY label")}
    except (sqlite3.Error, OSError) as error:
        result["error"] = str(error)
    return result


def collect_phone() -> dict:
    result = {"phones": 0, "pending": 0, "complete": 0, "error": 0,
              "mobile": 0, "landline": 0, "send": 0, "businesses": 0}
    if not PHONE_DB.is_file():
        return result
    try:
        with ro_db(PHONE_DB) as db:
            result["phones"] = db.execute("SELECT count(*) FROM phones").fetchone()[0]
            result["businesses"] = db.execute("SELECT count(*) FROM businesses").fetchone()[0]
            for status, count in db.execute("SELECT status,count(*) FROM phones GROUP BY status"):
                if status in result:
                    result[status] = count
            for outcome, action, count in db.execute(
                    "SELECT outcome,action,count(*) FROM phones GROUP BY outcome,action"):
                if outcome in result:
                    result[outcome] += count
                if action == "send":
                    result["send"] += count
    except (sqlite3.Error, OSError) as error:
        result["error"] = str(error)
    return result


def manifest_count() -> int:
    try:
        with GHL_MANIFEST.open(encoding="utf-8-sig", newline="") as handle:
            return sum(1 for _ in csv.DictReader(handle))
    except OSError:
        return 0


def apify_capacity(path: Path = APIFY_PROFILES_CSV) -> dict:
    """Return conservative phone capacity from sanitized tracker evidence."""
    result = {"configured": 0, "usable_profiles": 0, "enabled_profiles": 0,
              "phone_capacity": 0, "enabled_phone_capacity": 0,
              "credits_left": 0, "enabled_credits_left": 0,
              "dollar_room": 0.0, "last_checked": "", "health_counts": {}}
    if not path.is_file():
        return result
    with path.open(encoding="utf-8-sig", newline="") as stream:
        rows = list(csv.DictReader(stream))
    result["configured"] = len(rows)
    for row in rows:
        health = row.get("health") or "Not Checked"
        result["health_counts"][health] = result["health_counts"].get(health, 0) + 1
        if row.get("last_checked_utc", "") > result["last_checked"]:
            result["last_checked"] = row.get("last_checked_utc", "")
        if row.get("health") not in {"Active", "Low Room"} or row.get("job_status") != "Available":
            continue
        try:
            room = max(0.0, float(row.get("room_before_cap_usd") or 0))
            actor_credits = max(0, int(float(row.get("estimated_actor_credit_room_if_free_plan") or 0)))
        except (TypeError, ValueError):
            continue
        phone_capacity = max(0, min(math.floor(room / 0.02), actor_credits // 2))
        if not phone_capacity:
            continue
        result["usable_profiles"] += 1
        result["phone_capacity"] += phone_capacity
        result["credits_left"] += phone_capacity * 2
        result["dollar_room"] += room
        if str(row.get("use_for_jobs", "0")) == "1":
            result["enabled_profiles"] += 1
            result["enabled_phone_capacity"] += phone_capacity
            result["enabled_credits_left"] += phone_capacity * 2
    return result


def phone_supply_plan(phone_count: int, capacity: dict) -> dict:
    batches = math.ceil(phone_count / 240) if phone_count else 0
    shortfall = max(0, phone_count - capacity.get("enabled_phone_capacity", 0))
    return {
        "phones": phone_count,
        "credits_required": phone_count * 2,
        "estimated_cost": phone_count * 0.02,
        "safe_batches": batches,
        "profiles_required": batches,
        "hard_cap": batches * 4.85,
        "additional_profiles_needed": math.ceil(shortfall / 240) if shortfall else 0,
    }


def prepared_unique_phones(path: Path) -> int:
    if not path.is_file():
        return 0
    phones = set()
    with path.open(encoding="utf-8-sig", newline="") as stream:
        for row in csv.DictReader(stream):
            digits = re.sub(r"\D", "", row.get("final_phone", ""))
            if len(digits) == 11 and digits.startswith("1"):
                digits = digits[1:]
            if len(digits) == 10:
                phones.add(digits)
    return len(phones)


def prepared_phone_status(path: Path) -> dict:
    """Return provider status for only the phones in one prepared campaign file."""
    result = {"prepared": 0, "registered": 0, "pending": 0, "complete": 0,
              "mobile": 0, "landline": 0, "error": 0, "unregistered": 0}
    if not path.is_file():
        return result
    phones: set[str] = set()
    try:
        with path.open(encoding="utf-8-sig", newline="") as stream:
            for row in csv.DictReader(stream):
                digits = re.sub(r"\D", "", row.get("final_phone", ""))
                if len(digits) == 11 and digits.startswith("1"):
                    digits = digits[1:]
                if len(digits) == 10:
                    phones.add("+1" + digits)
    except OSError:
        return result
    result["prepared"] = len(phones)
    if not phones or not PHONE_DB.is_file():
        result["unregistered"] = len(phones)
        return result
    try:
        ordered_phones = list(phones)
        with ro_db(PHONE_DB) as db:
            for chunk_start in range(0, len(ordered_phones), 800):
                chunk = ordered_phones[chunk_start:chunk_start + 800]
                placeholders = ",".join("?" for _ in chunk)
                for row in db.execute(
                    f"SELECT phone,status,outcome FROM phones WHERE phone IN ({placeholders})",
                    chunk,
                ):
                    result["registered"] += 1
                    status = row["status"] or ""
                    outcome = row["outcome"] or ""
                    if status in {"pending", "complete", "error"}:
                        result[status] += 1
                    if outcome in {"mobile", "landline"}:
                        result[outcome] += 1
    except (sqlite3.Error, OSError):
        result["unregistered"] = len(phones)
        return result
    result["unregistered"] = max(0, result["prepared"] - result["registered"])
    return result


def campaign_stage_metrics(state: str, niche: str, campaign_slug: str = "",
                           refresh_seconds: float = 10) -> dict:
    """Return post-discovery counts belonging only to one state/niche campaign."""
    empty = {"ai_reviewed": 0, "niche_verified": 0,
             "mobile_verified": 0, "added_to_ghl": 0}
    if niche.strip().casefold() != "auto detailing":
        return empty
    key = (state, niche, campaign_slug)
    cached = _STAGE_CACHE.get(key)
    if cached and time.monotonic() - cached[0] < refresh_seconds:
        return dict(cached[1])
    state_ids: set[str] = set()
    reviewed_ids: set[str] = set()
    verified_ids: set[str] = set()
    if campaign_slug and AI_REVIEW_DB.is_file():
        try:
            with ro_db(AI_REVIEW_DB) as db:
                for row in db.execute(
                    "SELECT listing_id,label FROM decisions WHERE campaign_slug=?",
                    (campaign_slug,),
                ):
                    state_ids.add(row["listing_id"])
                    reviewed_ids.add(row["listing_id"])
                    if row["label"] == "pass":
                        verified_ids.add(row["listing_id"])
        except (sqlite3.Error, OSError):
            pass
    if not reviewed_ids and RECHECK_DB.is_file():
        try:
            with ro_db(RECHECK_DB) as db:
                for row in db.execute("""
                    SELECT c.listing_id,r.label
                    FROM cohort c
                    JOIN relevance_fast r ON r.listing_id=c.listing_id
                    JOIN json_each(c.states_json) s
                    WHERE s.value=?
                """, (state,)):
                    state_ids.add(row["listing_id"])
                    reviewed_ids.add(row["listing_id"])
                    if row["label"] == "pass":
                        verified_ids.add(row["listing_id"])
        except (sqlite3.Error, OSError):
            return empty

    mobile_phones: set[str] = set()
    if PHONE_DB.is_file() and verified_ids:
        try:
            with ro_db(PHONE_DB) as db:
                for row in db.execute("""
                    SELECT b.listing_id,p.phone
                    FROM businesses b JOIN phones p ON p.phone=b.phone
                    WHERE p.outcome='mobile'
                """):
                    if row["listing_id"] in verified_ids:
                        mobile_phones.add(row["phone"])
        except (sqlite3.Error, OSError):
            pass

    added_contacts = 0
    try:
        with GHL_MANIFEST.open(encoding="utf-8-sig", newline="") as handle:
            for row in csv.DictReader(handle):
                marker = "Listing ID: "
                notes = row.get("Notes", "")
                listing_id = notes.split(marker, 1)[1].splitlines()[0].strip() if marker in notes else ""
                if listing_id in state_ids:
                    added_contacts += 1
    except OSError:
        pass

    value = {
        "ai_reviewed": len(reviewed_ids),
        "niche_verified": len(verified_ids),
        "mobile_verified": len(mobile_phones),
        "added_to_ghl": added_contacts,
    }
    _STAGE_CACHE[key] = (time.monotonic(), value)
    return dict(value)


def collect_snapshot() -> dict:
    campaign_manager.bootstrap_existing()
    queue = collect_queue()
    recheck = collect_recheck()
    phone = collect_phone()
    queue_status = json_file(QUEUE_STATUS)
    recheck_status = json_file(RECHECK_STATUS)
    route = json_file(ROUTE_STATE)
    processes = runner_processes()
    control = read_control(CONTROL)
    status_state = queue_status.get("state", "unknown")
    if processes:
        service_state = "running"
    elif control:
        service_state = "paused"
    elif status_state in {"needs_attention", "error", "capacity_limited"}:
        service_state = "needs attention"
    else:
        service_state = "idle"
    timestamps = [parse_utc(queue_status.get("updated_utc")),
                  parse_utc(recheck_status.get("updated_utc"))]
    timestamps = [item for item in timestamps if item]
    last_checkpoint = max(timestamps) if timestamps else None
    campaigns = campaign_snapshot()
    overall_metrics = campaign_candidates.all_campaign_metrics()
    strict_ready = sum(campaign["lead_metrics"].get("ready_for_ai", 0)
                       for campaign in campaigns)
    overall_metrics.update({
        "ready_for_ai": strict_ready,
        "ai_reviewed": sum(recheck.get("relevance", {}).values()),
        "niche_verified": recheck.get("relevance", {}).get("pass", 0),
        "mobile_verified": phone.get("mobile", 0),
        "added_to_ghl": manifest_count(),
    })
    return {
        "generated": datetime.now(timezone.utc),
        "host": socket.gethostname(),
        "service_state": service_state,
        "processes": processes,
        "control": control,
        "route": route,
        "queue_status": queue_status,
        "recheck_status": recheck_status,
        "last_checkpoint": last_checkpoint,
        "queue": queue,
        "recheck": recheck,
        "phone": phone,
        "ghl_contacts": overall_metrics["added_to_ghl"],
        "campaigns": campaigns,
        "overall_metrics": overall_metrics,
    }


def campaign_snapshot() -> list[dict]:
    campaign_manager.reconcile_workers()
    ready = campaign_manager.registry_states()
    rows = campaign_manager.list_campaigns()
    counts = {}
    if QUEUE_DB.is_file():
        try:
            with ro_db(QUEUE_DB) as db:
                for row in db.execute("""
                    SELECT state_code,
                      coalesce(json_extract(spec,'$.niche'),'auto detailing') niche,
                      stage,count(*) count
                    FROM jobs
                    WHERE json_extract(spec,'$.scope_id') IS NOT NULL
                      AND json_extract(spec,'$.scope_id') NOT LIKE '%:phrase:%'
                    GROUP BY state_code,niche,stage
                """):
                    counts.setdefault((row["state_code"], row["niche"]), {})[row["stage"]] = row["count"]
        except sqlite3.Error:
            counts = {}
    result = []
    for row in rows:
        progress_counts = counts.get((row["state"], row["niche"]), {})
        lead_metrics = campaign_candidates.campaign_metrics(row["state"], row["niche"])
        evidence = Path(row["config_path"]).with_name(
            "profile-evidence") / "eligible-candidates.csv"
        lead_metrics["ready_for_ai"] = len(campaign_candidates.profile_evidence_rows(evidence))
        lead_metrics.update(campaign_stage_metrics(
            row["state"], row["niche"], row["slug"]
        ))
        result.append({
            **row,
            "state_name": campaign_manager.STATE_NAMES[row["state"]],
            "baseline_scopes": ready[row["state"]]["baseline_scopes"],
            "counts": progress_counts,
            "lead_metrics": lead_metrics,
        })
    return result


def focus_campaign(snapshot: dict) -> dict | None:
    rows = snapshot.get("campaigns", [])
    return next((row for row in rows if row["status"] in campaign_manager.ACTIVE_STATUSES),
                rows[0] if rows else None)


def color(text: str, code: str, enabled: bool) -> str:
    return f"{code}{text}{RESET}" if enabled else text


def event_text(event: dict) -> str:
    raw = event.get("event", "event")
    names = {
        "capture_progress": "Collecting businesses",
        "capture_saved": "Search saved",
        "scope_complete": "Search finished",
        "discovery_only_scope_complete": "Businesses saved",
        "browser_slot_started": "New browser location started",
        "browser_slot_finished": "Browser group finished",
        "browser_slot_waiting": "Short wait before next group",
        "state_released": "Search worker finished",
        "heartbeat": "Work is healthy",
        "scope_failed": "Search needs retry",
        "run_error": "Run error",
    }
    name = names.get(raw, raw.replace("_", " "))
    detail = ""
    for key in ("query", "state", "browser_slot", "action", "error"):
        if event.get(key):
            detail = str(event[key])
            break
    when = parse_utc(event.get("utc"))
    stamp = when.astimezone().strftime("%H:%M:%S") if when else "--:--:--"
    return f"{stamp}  {name}" + (f" · {detail}" if detail else "")


def status_presentation(state: str):
    if state == "running":
        return "WORKING", "The scraper is running. You do not need to do anything.", BG_GREEN + BLACK, "●"
    if state == "paused":
        return "PAUSED", "The scraper is safely paused. Press R to clear the pause request.", BG_BLUE + WHITE, "Ⅱ"
    if state == "needs attention":
        return "NEEDS ATTENTION", "Something needs checking. Press H to see the technical details.", BG_RED + WHITE, "!"
    return "READY", "Nothing is running right now. Your saved work is safe.", BG_BLUE + WHITE, "✓"


def render(snapshot: dict, use_color: bool = True, width: int = 120,
           message: str = "") -> str:
    """Render the simple operator view shown by default."""
    width = max(88, width)
    queue = snapshot["queue"]
    recheck = snapshot["recheck"]
    phone = snapshot["phone"]
    route = snapshot["route"]
    state = snapshot["service_state"]
    label, guidance, status_color, icon = status_presentation(state)
    checkpoint_age = None
    if snapshot["last_checkpoint"]:
        checkpoint_age = (snapshot["generated"] - snapshot["last_checkpoint"]).total_seconds()
    lines = [
        color("  Google Maps Scraper  ", BOLD + BG_BLUE + WHITE, use_color),
        "═" * min(width, 106),
        f"{icon}  {color(' '+label+' ', BOLD + status_color, use_color)}   {guidance}",
        f"🌐 VPN: {route_city(route.get('host', ''))}  ·  IP {route.get('public_ip') or 'not connected'}  ·  "
        f"Last saved {fmt_duration(checkpoint_age)} ago",
        "",
        color("  📁  CAMPAIGNS  ", BOLD + BG_BLUE + WHITE, use_color),
    ]
    campaigns = snapshot["campaigns"]
    for campaign in campaigns[:5]:
        counts = campaign["counts"]
        done = min(counts.get("done", 0), campaign["baseline_scopes"])
        total = campaign["baseline_scopes"]
        status = campaign["status"].replace("_", " ").upper()
        lines.append(
            f"   {campaign['id']:>2}. {campaign['name'][:34]:<34} "
            f"{status:<10} {done:,}/{total:,} areas"
        )
    if not campaigns:
        lines.append("   No campaigns yet. Press M to create the first one.")
    lines += [
        "",
        color("  🗺  STATES  ", BOLD + BG_BLUE + WHITE, use_color),
    ]
    states = queue["states"]
    show_states = [code for code in ("FL", "GA") if code in states]
    show_states += [code for code in sorted(states) if code not in show_states
                    and (sum(states[code].values()) >= 10 or any(k != "done" for k in states[code]))]
    for code in show_states[:8]:
        counts = states[code]
        total = sum(counts.values())
        done = counts.get("done", 0)
        blocked = counts.get("blocked", 0)
        pending = max(0, total - done - blocked)
        pct = 100 * done / total if total else 0
        okay = total > 0 and done == total and not blocked
        marker = (color(" ✓ COMPLETE ", BOLD + BG_GREEN + BLACK, use_color)
                  if okay else color(" ! IN PROGRESS ", BOLD + BG_RED + WHITE, use_color))
        lines.append(f"   {code}  {marker:<24} {progress(done,total,14)}  {done:,}/{total:,}  "
                     f"· pending {pending:,} · blocked {blocked:,} · {pct:.1f}%")
    if not show_states:
        lines.append("   No state work has been registered yet.")

    focus = focus_campaign(snapshot)
    metrics = focus["lead_metrics"] if focus else {
        "businesses_found": 0, "passed_card_filters": 0, "ready_for_ai": 0, "ai_reviewed": 0,
        "niche_verified": 0, "mobile_verified": 0, "added_to_ghl": 0,
    }
    overall = snapshot.get("overall_metrics", metrics)
    focus_name = focus["name"] if focus else "No campaign selected"
    lines += [
        "",
        color(f"  🎯  CURRENT SCRAPE · {focus_name}  ", BOLD + BG_BLUE + WHITE, use_color),
        f"   {color('UNIQUE BUSINESSES FOUND', BOLD, use_color):<30} {fmt_int(metrics['businesses_found']):>8}",
        f"   {color('NEEDS PROFILE CHECK', BOLD, use_color):<30} {fmt_int(metrics['passed_card_filters']):>8}",
        f"   {color('READY FOR AI REVIEW', BOLD, use_color):<30} {fmt_int(metrics['ready_for_ai']):>8}",
        f"   {color('NICHE VERIFIED BY AI', BOLD, use_color):<30} {fmt_int(metrics['niche_verified']):>8}",
        f"   {color('MOBILE NUMBERS VERIFIED', BOLD, use_color):<30} {fmt_int(metrics['mobile_verified']):>8}",
        f"   {color('ADDED TO GOHIGHLEVEL', BOLD, use_color):<30} {fmt_int(metrics['added_to_ghl']):>8}",
        "",
        color("  📊  ALL SCRAPES COMBINED  ", BOLD + BG_BLUE + WHITE, use_color),
        f"   {color('UNIQUE BUSINESSES FOUND', BOLD, use_color):<30} {fmt_int(overall['businesses_found']):>8}",
        f"   {color('NEEDS PROFILE CHECK', BOLD, use_color):<30} {fmt_int(overall['passed_card_filters']):>8}",
        f"   {color('READY FOR AI REVIEW', BOLD, use_color):<30} {fmt_int(overall['ready_for_ai']):>8}",
        f"   {color('NICHE VERIFIED BY AI', BOLD, use_color):<30} {fmt_int(overall['niche_verified']):>8}",
        f"   {color('MOBILE NUMBERS VERIFIED', BOLD, use_color):<30} {fmt_int(overall['mobile_verified']):>8}",
        f"   {color('ADDED TO GOHIGHLEVEL', BOLD, use_color):<30} {fmt_int(overall['added_to_ghl']):>8}",
        "",
        color("  📈  SPEED  ", BOLD + BG_BLUE + WHITE, use_color),
    ]
    perf = queue["performance"]
    elapsed = perf.get("elapsed", 0)
    scope_rate = perf.get("scopes", 0) * 3600 / elapsed if elapsed else 0
    lines.append(f"   {fmt_int(perf.get('scopes'))} searches finished  ·  {fmt_duration(elapsed)} running  ·  "
                 f"{scope_rate:,.0f} searches/hour")

    control = snapshot["control"]
    lines += [
        "",
        color("  👇  WHAT CAN I DO?  ", BOLD + BG_BLUE + WHITE, use_color),
        color("   KEYBOARD ONLY: press the letter shown in [brackets]. These are not clickable.",
              BOLD, use_color),
    ]
    if control:
        lines.append(
            f"   {color(' [C] KEEP WORKING INSTEAD ', BOLD + BG_BLUE + WHITE, use_color)}   "
            f"{color('[M] CAMPAIGNS', BOLD, use_color)}   {color('[V] VIEW AI QUEUE', BOLD, use_color)}   "
            f"{color('[F] PHONE CHECKS', BOLD, use_color)}   {color('[K] APIFY PROFILES', BOLD, use_color)}   "
            f"{color('[H] MORE INFO', BOLD, use_color)}   {color('[Q] CLOSE WINDOW', BOLD, use_color)}")
        lines.append("   C cancels the pending stop request. It does not start a stopped scraper.")
    elif snapshot["processes"]:
        lines.append(
            f"   {color(' [S] STOP AFTER SAVING ', BOLD + BG_RED + WHITE, use_color)}   "
            f"{color('[M] CAMPAIGNS', BOLD, use_color)}   {color('[V] VIEW AI QUEUE', BOLD, use_color)}   "
            f"{color('[F] PHONE CHECKS', BOLD, use_color)}   {color('[K] APIFY PROFILES', BOLD, use_color)}   "
            f"{color('[H] MORE INFO', BOLD, use_color)}   {color('[Q] CLOSE WINDOW', BOLD, use_color)}")
        lines.append("   S finishes the current small group, saves everything, and then stops.")
    else:
        lines.append(
            f"   {color('[M] CAMPAIGNS', BOLD, use_color)}   {color('[V] VIEW AI QUEUE', BOLD, use_color)}   "
            f"{color('[F] PHONE CHECKS', BOLD, use_color)}   {color('[K] APIFY PROFILES', BOLD, use_color)}   "
            f"{color('[H] MORE INFO', BOLD, use_color)}   {color('[Q] CLOSE WINDOW', BOLD, use_color)}")
        lines.append("   Nothing is running, so there is nothing to stop right now.")
    if message:
        lines.append(color("   → " + message, BOLD + BLACK, use_color))
    elif control:
        lines.append(color(f"   → Waiting to apply: {control.get('action')}", BOLD + BG_RED + WHITE, use_color))
    else:
        lines.append(color("   → Closing this screen does NOT stop the scraper.", DIM, use_color))
    lines += ["", color("Press H only if you want more information.", DIM, use_color)]
    return "\n".join(lines)


def render_advanced(snapshot: dict, use_color: bool = True, width: int = 120,
                    message: str = "") -> str:
    width = max(88, width)
    queue = snapshot["queue"]
    recheck = snapshot["recheck"]
    phone = snapshot["phone"]
    route = snapshot["route"]
    control = snapshot["control"]
    state = snapshot["service_state"]
    state_code = GREEN if state == "running" else BLUE if state == "paused" else RED if state == "needs attention" else CYAN
    state_icon = "🟢" if state == "running" else "🟡" if state == "paused" else "🔴" if state == "needs attention" else "⚪"
    pids = ", ".join(f"{item['name']}:{item['pid']}" for item in snapshot["processes"]) or "none"
    checkpoint_age = None
    if snapshot["last_checkpoint"]:
        checkpoint_age = (snapshot["generated"] - snapshot["last_checkpoint"]).total_seconds()
    lines = [
        color(" Google Maps Scraper · Technical View ", BOLD + BLUE, use_color),
        "═" * min(width, 120),
        f"{state_icon} Service {color(state.upper(), BOLD + state_code, use_color):<20}  "
        f"Host {snapshot['host']}  ·  Runner {pids}",
        f"🧭 Route  {route.get('host') or '—'}  ·  {route_city(route.get('host', ''))}  ·  "
        f"Public IP {route.get('public_ip') or '—'}",
        f"💾 Last checkpoint {fmt_duration(checkpoint_age)} ago  ·  "
        f"Control request {control.get('action', 'none')}",
        "",
        color("CAMPAIGN GEOGRAPHY · progress against the complete planned state map", BOLD, use_color),
        f"{'Campaign':<25}{'ST':<4}{'Progress':<21}{'Done':>8}{'Remaining':>11}"
        f"{'Blocked':>9}{'Planned':>10}{'Registered':>12}",
    ]
    campaigns = snapshot["campaigns"]
    for campaign in campaigns[:8]:
        counts = campaign["counts"]
        total = campaign["baseline_scopes"]
        registered = sum(counts.values())
        done = min(counts.get("done", 0), total)
        blocked = counts.get("blocked", 0)
        remaining = max(0, total - done)
        pct = (100 * done / total) if total else 0
        label = campaign["name"][:23]
        lines.append(
            f"{label:<25}{campaign['state']:<4}{progress(done,total):<21}{done:>8,}"
            f"{remaining:>11,}{blocked:>9,}{total:>10,}{registered:>12,}  {pct:>5.1f}%"
        )
    if not campaigns:
        lines.append("No campaign geography is registered.")

    focus = focus_campaign(snapshot)
    metrics = focus["lead_metrics"] if focus else {
        "businesses_found": 0, "passed_card_filters": 0, "ready_for_ai": 0, "ai_reviewed": 0,
        "niche_verified": 0, "mobile_verified": 0, "added_to_ghl": 0,
    }
    overall = snapshot.get("overall_metrics", metrics)
    focus_name = focus["name"] if focus else "No active campaign"
    lines += [
        "",
        color(f"CURRENT SCRAPE · {focus_name}", BOLD, use_color),
        f"Businesses found (unique)          {fmt_int(metrics['businesses_found'])}",
        f"Needs full-profile verification    {fmt_int(metrics['passed_card_filters'])}",
        f"Ready for AI review                {fmt_int(metrics['ready_for_ai'])}",
        f"Reviewed by AI                     {fmt_int(metrics['ai_reviewed'])}",
        f"Niche verified by AI               {fmt_int(metrics['niche_verified'])}",
        f"Mobile numbers verified            {fmt_int(metrics['mobile_verified'])}",
        f"Added to GoHighLevel                {fmt_int(metrics['added_to_ghl'])}",
        "",
        color("ALL SCRAPES COMBINED", BOLD, use_color),
        f"Businesses found (unique)          {fmt_int(overall['businesses_found'])}",
        f"Needs full-profile verification    {fmt_int(overall['passed_card_filters'])}",
        f"Ready for AI review                {fmt_int(overall['ready_for_ai'])}",
        f"Reviewed by AI                     {fmt_int(overall['ai_reviewed'])}",
        f"Niche verified by AI               {fmt_int(overall['niche_verified'])}",
        f"Mobile numbers verified            {fmt_int(overall['mobile_verified'])}",
        f"Added to GoHighLevel                {fmt_int(overall['added_to_ghl'])}",
        "Ready for AI means: full profile has no website, phone present, at least 4.0 stars,",
        "at least 10 reviews, and neither temporarily nor permanently closed.",
    ]

    perf = queue["performance"]
    elapsed = perf.get("elapsed", 0)
    scope_rate = perf.get("scopes", 0) * 3600 / elapsed if elapsed else 0
    lines += [
        "",
        color("SPEED", BOLD, use_color),
        f"Runtime {fmt_duration(elapsed)}  ·  searches completed {fmt_int(perf.get('scopes'))} "
        f"({scope_rate:,.1f} searches/hour)",
        "",
        color("RECENT EVENTS", BOLD, use_color),
    ]
    for event in queue["events"][:6]:
        lines.append("  " + event_text(event)[: max(20, width - 4)])
    if not queue["events"]:
        lines.append("  No events recorded.")
    lines += [
        "",
        color("CONTROLS", BOLD, use_color),
        "[M] Manage campaigns          [V] View businesses ready for AI",
        "[F] Phone verification queue  [K] Add or inspect Apify validation profiles",
        "[P] Pause after browser wave   [B] Pause after scope batch   [S] Safe stop at checkpoint",
        "[R] Clear pause request        [X] Stop active runner now    [H] Simple view   [Q] Close dashboard",
    ]
    if message:
        lines.append(color("› " + message, MAGENTA, use_color))
    elif control:
        lines.append(color(f"› Pending operator request: {control.get('action')}", MAGENTA, use_color))
    else:
        lines.append(color("› Dashboard is live. Closing it does not stop the scraper.", DIM, use_color))
    return "\n".join(lines)


def interrupt_runners() -> list[int]:
    pids = [item["pid"] for item in runner_processes()]
    for pid in pids:
        try:
            os.kill(pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
    return pids


def ask(prompt: str, default: str = "") -> str:
    suffix = f" [{default}]" if default else ""
    value = input(f"{prompt}{suffix}: ").strip()
    return value or default


def choose_niche(default: str = "auto detailing") -> str:
    names = campaign_manager.niche_names()
    print("\nNiches")
    for index, name in enumerate(names, 1):
        marker = " (current)" if name == default else ""
        print(f"  {index}. {name.title()}{marker}")
    while True:
        value = ask("Choose a number", str(names.index(default) + 1) if default in names else "1")
        if value.isdigit() and 1 <= int(value) <= len(names):
            return names[int(value) - 1]
        print("Enter one of the numbers shown above.")


def new_campaign_menu() -> str:
    print("\nCREATE CAMPAIGN")
    name = ask("Campaign name")
    state = ask("State code, for example TX").upper()
    niche = choose_niche()
    campaign = campaign_manager.create_campaign(name, state, niche)
    return f"Created {campaign['name']}. It is saved and has not started."


def edit_campaign_menu(campaign: dict) -> str:
    print("\nEDIT CAMPAIGN")
    name = ask("Name", campaign["name"])
    state = ask("State", campaign["state"]).upper()
    niche = choose_niche(campaign["niche"])
    changed = campaign_manager.edit_campaign(campaign["id"], name, state, niche)
    return f"Saved {changed['name']}."


def campaign_detail_menu(campaign_id: int) -> str:
    while True:
        campaign = campaign_manager.get_campaign(campaign_id)
        ready = campaign_manager.registry_states()[campaign["state"]]
        print("\033[H\033[2J", end="")
        print("Google Maps Scraper\n")
        print(campaign["name"])
        print(f"State:  {campaign_manager.STATE_NAMES[campaign['state']]} ({campaign['state']})")
        print(f"Niche:  {campaign['niche'].title()}")
        print(f"Areas:  {ready['baseline_scopes']:,}")
        print(f"Status: {campaign['status'].replace('_', ' ').title()}")
        if campaign.get("last_error"):
            print(f"Needs attention: {campaign['last_error']}")
        print("\n[S] Start or resume   [P] Pause safely   [E] Edit")
        print("[A] Archive           [D] Delete from list   [B] Back")
        choice = input("\nChoose: ").strip().lower()
        try:
            if choice == "b":
                return ""
            if choice == "s":
                if campaign["status"] == "completed":
                    return "That campaign is already complete."
                clear()
                campaign_manager.launch_campaign(campaign_id)
                return f"Started {campaign['name']} in the background."
            if choice == "p":
                if campaign["status"] not in campaign_manager.ACTIVE_STATUSES:
                    return "That campaign is not currently running."
                request("safe_stop", requested_by="campaign_manager")
                campaign_manager.update_status(campaign_id, "pausing")
                return f"{campaign['name']} will pause after saving its current work."
            if choice == "e":
                return edit_campaign_menu(campaign)
            if choice == "a":
                if ask("Type YES to archive") == "YES":
                    campaign_manager.archive_campaign(campaign_id)
                    return f"Archived {campaign['name']}."
            if choice == "d":
                print("This removes the campaign from the app list but preserves its evidence.")
                if ask("Type DELETE to continue") == "DELETE":
                    campaign_manager.soft_delete_campaign(campaign_id)
                    return f"Removed {campaign['name']} from the list."
        except (ValueError, RuntimeError) as error:
            print(f"\nCould not do that: {error}")
            input("Press Return to continue.")


def campaign_menu() -> str:
    while True:
        rows = campaign_manager.list_campaigns()
        print("\033[H\033[2J", end="")
        print("Google Maps Scraper\n")
        print("CAMPAIGNS")
        if rows:
            for campaign in rows:
                status = campaign["status"].replace("_", " ").title()
                print(f"  {campaign['id']:>2}. {campaign['name'][:38]:<38} "
                      f"{campaign['state']:<2}  {campaign['niche'].title():<20} {status}")
        else:
            print("  No campaigns yet.")
        print("\n[N] New campaign   [number] Open campaign   [B] Back")
        choice = input("\nChoose: ").strip().lower()
        try:
            if choice == "b":
                return "Campaign list closed."
            if choice == "n":
                return new_campaign_menu()
            if choice.isdigit() and any(row["id"] == int(choice) for row in rows):
                return campaign_detail_menu(int(choice))
        except (ValueError, RuntimeError, sqlite3.Error) as error:
            print(f"\nCould not do that: {error}")
            input("Press Return to continue.")


def choose_stage_campaign(title: str, rows: list[dict] | None = None,
                          preferred_id: int | None = None) -> dict | None:
    rows = rows if rows is not None else campaign_manager.list_campaigns()
    if not rows:
        return None
    default = (next((row for row in rows if row["id"] == preferred_id), None)
               if preferred_id is not None else None)
    default = default or next(
        (row for row in rows if row["status"] in campaign_manager.ACTIVE_STATUSES), rows[0]
    )
    print("\033[H\033[2J", end="")
    print(f"Google Maps Scraper\n\n{title}\n")
    for row in rows:
        marker = " (default)" if row["id"] == default["id"] else ""
        print(f"  {row['id']:>2}. {row['name']} · {row['status'].replace('_', ' ').title()}{marker}")
    choice = ask("Campaign number", str(default["id"]))
    return next((row for row in rows if str(row["id"]) == choice), None)


def ai_queue_menu() -> str:
    rows = campaign_manager.list_campaigns()
    completed_reviews = [
        (Path(row["config_path"]).with_name("ai-review-decisions.csv").stat().st_mtime, row)
        for row in rows
        if Path(row["config_path"]).with_name("ai-review-decisions.csv").is_file()
    ]
    preferred_id = max(completed_reviews, default=(0, {}), key=lambda item: item[0])[1].get("id")
    campaign = choose_stage_campaign("AI REVIEW", rows, preferred_id)
    if not campaign:
        return "Create a campaign first."
    output = Path(campaign["config_path"]).with_name("ai-review-queue.csv")
    command = [sys.executable, str(ROOT / "scripts" / "ai_review_pipeline.py"), "export",
               "--campaign", campaign["slug"], "--state", campaign["state"],
               "--niche", campaign["niche"], "--output", str(output)]
    result = subprocess.run(command, cwd=ROOT, text=True, capture_output=True, check=True)
    json.loads(result.stdout)
    candidates = []
    if output.is_file():
        with output.open(encoding="utf-8-sig", newline="") as stream:
            candidates = list(csv.DictReader(stream))
    evidence_dir = output.with_name("profile-evidence")
    evidence = evidence_dir / "eligible-candidates.csv"
    print("\033[H\033[2J", end="")
    print("Google Maps Scraper\n")
    print(f"READY FOR AI REVIEW · {campaign['name']}")
    print(f"{len(candidates):,} unique businesses currently meet the basic filters.")
    print("Full profile: no website · phone present · 4.0+ stars · 10+ reviews")
    print("Neither temporarily nor permanently closed")
    print("These still need AI relevance review and later phone verification.\n")
    print(f"{'Business':<40}{'Rating':>8}{'Reviews':>10}{'Phone':>9}")
    for row in candidates[:20]:
        print(f"{(row.get('name') or 'Unnamed')[:38]:<40}"
              f"{str(row.get('rating') or '—'):>8}{str(row.get('reviews') or '—'):>10}"
              f"{('Yes' if row.get('phone') else 'No'):>9}")
    if len(candidates) > 20:
        print(f"\nShowing 20 of {len(candidates):,}.")
    print(f"Saved full list: {output}")
    decisions = output.with_name("ai-review-decisions.csv")
    phone_input = output.with_name("phone-validation-input.csv")
    print(f"Expected AI decisions: {decisions}")
    print("\n[C] Check full profiles   [I] Import AI decisions")
    print("[P] Prepare Apify input (no spend)   [B] Back")
    choice = input("\nChoose: ").strip().lower()
    if choice == "c":
        if campaign["status"] in campaign_manager.ACTIVE_STATUSES:
            input("\nFinish or safely pause discovery before profile verification. Press Return.")
            return "Profile verification did not start while discovery is active."
        evidence_dir.mkdir(parents=True, exist_ok=True)
        log_path = evidence_dir / "worker.log"
        command = [sys.executable, str(ROOT / "profile_recheck.py"),
                   "--source", str(QUEUE_DB), "--output", str(evidence_dir),
                   "--state", campaign["state"], "--niche", campaign["niche"],
                   "--tabs", "10", "--profiles-per-browser", "100"]
        with log_path.open("ab") as log:
            subprocess.Popen(command, cwd=ROOT, stdout=log, stderr=subprocess.STDOUT,
                             start_new_session=True)
        input(f"\nProfile verification started. Log: {log_path}\nPress Return to go back.")
        return "Full-profile verification started in the background."
    if choice == "i":
        if not decisions.is_file():
            input(f"\nNo decision file found at {decisions}. Press Return to go back.")
            return "AI queue is ready; decision file has not been added yet."
        model = ask("Model used for these decisions", "claude")
        completed = subprocess.run(
            [sys.executable, str(ROOT / "scripts" / "ai_review_pipeline.py"),
             "import-decisions", "--campaign", campaign["slug"],
             "--queue", str(output), "--decisions", str(decisions), "--model", model],
            cwd=ROOT, text=True, capture_output=True,
        )
        print("\n" + (completed.stdout or completed.stderr).strip())
        input("\nPress Return to go back.")
        return ("AI decisions imported."
                if completed.returncode == 0 else "AI decision import was rejected; source data is unchanged.")
    if choice == "p":
        completed = subprocess.run(
            [sys.executable, str(ROOT / "scripts" / "ai_review_pipeline.py"),
             "prepare-phone", "--campaign", campaign["slug"],
             "--queue", str(output), "--output", str(phone_input)],
            cwd=ROOT, text=True, capture_output=True,
        )
        print("\n" + (completed.stdout or completed.stderr).strip())
        if completed.returncode == 0:
            print("\nPrepared only. No Apify job was started and no money was spent.")
        input("\nPress Return to go back.")
        return ("Apify input prepared; paid validation has not started."
                if completed.returncode == 0 else "Apify input preparation failed; source data is unchanged.")
    return f"AI review queue refreshed: {len(candidates):,} businesses."


def apify_profiles_menu() -> str:
    configure = ROOT / "scripts" / "configure_apify_tokens.py"
    tracker = ROOT / "scripts" / "apify_profile_tracker.py"
    message = ""
    while True:
        capacity = apify_capacity()
        phone = collect_phone()
        rows = campaign_manager.list_campaigns()
        prepared_count = sum(
            prepared_unique_phones(Path(row["config_path"]).with_name("phone-validation-input.csv"))
            for row in rows
        )
        demand = phone["pending"] or prepared_count
        demand_plan = phone_supply_plan(demand, capacity)
        focus = next((row for row in rows if row["status"] in campaign_manager.ACTIVE_STATUSES
                      or row["status"] == "paused"), None)
        upper_bound = 0
        upper_plan = phone_supply_plan(0, capacity)
        if focus:
            upper_bound = campaign_candidates.campaign_metrics(
                focus["state"], focus["niche"], refresh_seconds=0
            ).get("passed_card_filters", 0)
            upper_plan = phone_supply_plan(upper_bound, capacity)
        health = capacity.get("health_counts", {})
        print("\033[H\033[2J", end="")
        print("Google Maps Scraper\n")
        print("APIFY CREDIT & PROFILE DASHBOARD")
        print("Tokens stay sealed and are never shown here.")
        print("Adding profiles does not start a paid validation job.\n")
        print("CURRENT SUPPLY")
        print(f"  Profiles configured:       {capacity['configured']:,}")
        print(f"  Profiles usable now:       {capacity['usable_profiles']:,}")
        print(f"  Profiles enabled for jobs: {capacity['enabled_profiles']:,}")
        print(f"  Phone checks available:    {capacity['phone_capacity']:,}")
        print(f"  Provider credits left:     {capacity['credits_left']:,}")
        print(f"  Estimated dollar room:     ${capacity['dollar_room']:,.2f}")
        print(f"  Last credit check:         {capacity['last_checked'] or 'Not checked'}")
        print("\nPROFILE HEALTH")
        print(f"  Active: {health.get('Active', 0):,}  ·  Low room: {health.get('Low Room', 0):,}  ·  "
              f"Cap reached: {health.get('Cap Reached', 0):,}  ·  "
              f"Key rejected: {health.get('Key Rejected', 0):,}")
        print("\nCURRENT PHONE-VERIFICATION QUEUE")
        print(f"  Waiting for provider check: {phone['pending']:,}")
        print(f"  Prepared after AI review:   {prepared_count:,}")
        print(f"  Credits needed now:         {demand_plan['credits_required']:,}")
        print(f"  Safe profile batches:       {demand_plan['profiles_required']:,}")
        print(f"  Additional profiles needed: {demand_plan['additional_profiles_needed']:,}")
        if focus and upper_bound:
            print(f"\nNEXT CAMPAIGN UPPER BOUND · {focus['name']}")
            print(f"  Before profile and AI rejection: {upper_bound:,} possible phones")
            print(f"  Maximum credits needed:          {upper_plan['credits_required']:,}")
            print(f"  Maximum estimated cost:          ${upper_plan['estimated_cost']:,.2f}")
            print(f"  Maximum safe profile batches:    {upper_plan['profiles_required']:,}")
            print("  Actual demand will be lower after profile checks and AI review.")
        if message:
            print(f"\n{message}")
        print("\n[A] Add new profiles   [R] Refresh health and credit status")
        print("[E] Enable selected profiles for jobs   [D] Disable selected profiles   [B] Back")
        choice = input("\nChoose: ").strip().lower()
        if choice == "b":
            return "Apify profile manager closed."
        if choice == "a":
            completed = subprocess.run([sys.executable, str(configure), "add"], cwd=ROOT)
            if completed.returncode == 0:
                subprocess.run([sys.executable, str(tracker), "refresh", "--workers", "8"],
                               cwd=ROOT, check=False)
                input("\nProfiles appended and tracker refreshed. Press Return to continue.")
            else:
                input("\nNo profiles were added. Press Return to continue.")
        elif choice == "r":
            completed = subprocess.run(
                [sys.executable, str(tracker), "refresh", "--workers", "8"],
                cwd=ROOT, text=True, capture_output=True,
            )
            message = ("Credit status refreshed successfully."
                       if completed.returncode == 0 else
                       "Credit refresh needs attention; saved totals are still shown.")
        elif choice in {"e", "d"}:
            raw = ask("Profiles, for example 256-275 or 256,260")
            try:
                profiles = parse_profile_numbers(raw)
            except ValueError as error:
                input(f"\n{error}. Press Return to continue.")
                continue
            command = [sys.executable, str(tracker), "set-use", *map(str, profiles)]
            if choice == "e":
                command.append("--enabled")
            subprocess.run(command, cwd=ROOT, check=False)
            state = "enabled" if choice == "e" else "disabled"
            input(f"\n{len(profiles)} profile(s) {state} for jobs. "
                  "Press Return to go back to the Apify profile screen.")


def parse_profile_numbers(value: str) -> list[int]:
    numbers: set[int] = set()
    for part in value.split(","):
        part = part.strip()
        if not part:
            continue
        if "-" in part:
            start, end = (int(item.strip()) for item in part.split("-", 1))
            if start > end:
                raise ValueError("Profile range must run from low to high")
            numbers.update(range(start, end + 1))
        else:
            numbers.add(int(part))
    if not numbers or min(numbers) < 1 or max(numbers) > 1000:
        raise ValueError("Choose profile numbers from 1 through 1000")
    return sorted(numbers)


def phone_verification_menu() -> str:
    rows = campaign_manager.list_campaigns()
    prepared_campaigns = []
    for row in rows:
        path = Path(row["config_path"]).with_name("phone-validation-input.csv")
        if not path.is_file():
            continue
        with path.open(encoding="utf-8-sig", newline="") as stream:
            count = sum(1 for _ in csv.DictReader(stream))
        if count:
            prepared_campaigns.append(row)
    if not prepared_campaigns:
        phone = collect_phone()
        capacity = apify_capacity()
        focus = next((row for row in rows if row["status"] in campaign_manager.ACTIVE_STATUSES
                      or row["status"] == "paused"), rows[0] if rows else None)
        upper_bound = 0
        if focus:
            upper_bound = campaign_candidates.campaign_metrics(
                focus["state"], focus["niche"], refresh_seconds=0
            ).get("passed_card_filters", 0)
        upper_plan = phone_supply_plan(upper_bound, capacity)
        print("\033[H\033[2J", end="")
        print("Google Maps Scraper\n")
        print("PHONE VERIFICATION\n")
        print("NO PHONE LIST HAS BEEN PREPARED YET.\n")
        print("WHAT TO DO NEXT")
        print("  1. Press Return now to go back.")
        print("  2. Press V on the dashboard.")
        print("  3. Choose the campaign with completed AI review.")
        print("  4. Press P to prepare its phone list. This spends nothing.")
        print("  5. Return here with F.")
        print("\nExisting historical phone ledger:")
        print(f"  Completed: {phone['complete']:,}")
        print(f"  Mobile:    {phone['mobile']:,}")
        print(f"  Landline:  {phone['landline']:,}")
        print("\nAPIFY SUPPLY AVAILABLE NOW")
        print(f"  Configured profiles:       {capacity['configured']:,}")
        print(f"  Usable profiles:           {capacity['usable_profiles']:,}")
        print(f"  Enabled usable profiles:   {capacity['enabled_profiles']:,}")
        print(f"  Conservative checks left:  {capacity['phone_capacity']:,}")
        print(f"  Enabled checks available:  {capacity['enabled_phone_capacity']:,}")
        print(f"  Provider credits left:     {capacity['credits_left']:,}")
        if focus and upper_bound:
            print(f"\nWORST-CASE PLANNING · {focus['name']}")
            print(f"  Before profile and AI filtering: {upper_bound:,} businesses")
            print(f"  At most {upper_plan['credits_required']:,} credits · "
                  f"about ${upper_plan['estimated_cost']:,.2f}")
            print(f"  At most {upper_plan['safe_batches']:,} safe batches · "
                  f"{upper_plan['profiles_required']:,} fresh-profile equivalents")
            print("  The real requirement will be lower after profile and AI rejection.")
        input("\nPRESS RETURN NOW to go back to the dashboard.")
        return "No new businesses are ready for phone verification yet."
    campaign = (prepared_campaigns[0] if len(prepared_campaigns) == 1
                else choose_stage_campaign("CHOOSE A PREPARED PHONE QUEUE", prepared_campaigns))
    if not campaign:
        return "Phone verification menu closed."
    prepared = Path(campaign["config_path"]).with_name("phone-validation-input.csv")
    script = ROOT / "scripts" / "phone_validation.py"
    while True:
        campaign_phone = prepared_phone_status(prepared)
        capacity = apify_capacity()
        demand = campaign_phone["pending"]
        plan = phone_supply_plan(demand, capacity)
        print("\033[H\033[2J", end="")
        print("Google Maps Scraper\n")
        print(f"PHONE VERIFICATION · {campaign['name']}")
        print(f"Prepared unique phones:        {campaign_phone['prepared']:,}")
        print(f"Already verified from history: {campaign_phone['complete']:,}")
        print(f"New phones waiting for Apify:  {campaign_phone['pending']:,}")
        print(f"Not registered yet:            {campaign_phone['unregistered']:,}")
        if campaign_phone["complete"]:
            print(f"Known results: Mobile {campaign_phone['mobile']:,} · "
                  f"Landline {campaign_phone['landline']:,}")
        print("\nSUPPLY PLAN")
        print(f"Phones to verify:             {plan['phones']:,}")
        print(f"Credits required:             {plan['credits_required']:,}")
        print(f"Estimated provider cost:      ${plan['estimated_cost']:,.2f}")
        print(f"Safe 240-phone batches:       {plan['safe_batches']:,}")
        print(f"Fresh-profile equivalents:    {plan['profiles_required']:,}")
        print(f"Hard aggregate run cap:       ${plan['hard_cap']:,.2f}")
        print(f"Enabled checks available:     {capacity['enabled_phone_capacity']:,}")
        print(f"All usable checks available:  {capacity['phone_capacity']:,}")
        print(f"Additional profiles needed:   {plan['additional_profiles_needed']:,}")
        if campaign_phone["unregistered"]:
            print("\nNEXT: press I to register this list. This spends nothing.")
        elif campaign_phone["pending"] and not capacity["enabled_phone_capacity"]:
            print("\nNEXT: no Apify profiles are enabled for jobs.")
            print("Go back, press K, then enable healthy profiles before running.")
        elif campaign_phone["pending"]:
            print("\nREADY: press R to start a bounded provider run.")
        else:
            print("\nDONE: this campaign has no unverified phones waiting.")
        print("\n[I] Register list (no spend)   [R] Run Apify verification   [B] Back")
        choice = input("\nChoose: ").strip().lower()
        if choice == "b":
            return "Phone verification menu closed."
        if choice == "i":
            if not prepared.is_file():
                input("\nNo prepared input exists. Import AI decisions under V, then press P. Return.")
                continue
            subprocess.run([sys.executable, str(script), "import", "--source", str(prepared)],
                           cwd=ROOT, check=False)
            input("\nQueue registered. No provider job was started. Press Return.")
        elif choice == "r":
            campaign_status = prepared_phone_status(prepared)
            pending = campaign_status["pending"]
            if not pending:
                if campaign_status["unregistered"]:
                    input("\nThis list is not registered yet. Press Return, then press I first.")
                else:
                    input("\nThis campaign has no pending phones. Press Return.")
                continue
            capacity = apify_capacity()
            if not capacity["enabled_phone_capacity"]:
                input("\nNo healthy Apify profiles are enabled for jobs. "
                      "Press Return, go back, press K, and enable profiles first.")
                continue
            recommended_batches = max(1, math.ceil(pending / 240))
            try:
                batches = int(ask(
                    f"Maximum 240-number batches to run (recommended: {recommended_batches})",
                    str(recommended_batches),
                ))
            except ValueError:
                input("\nEnter a whole number. Press Return.")
                continue
            if batches < 1:
                input("\nBatch count must be positive. Press Return.")
                continue
            maximum = batches * 4.85
            print(f"\nThis can start up to {batches} paid Apify run(s), 240 phones each.")
            print(f"Hard provider cap across those runs: ${maximum:,.2f}.")
            print(f"To continue, type the complete phrase: RUN {batches}")
            confirmation = input("Authorization: ").strip()
            if confirmation != f"RUN {batches}":
                input("\nNothing started. Press Return.")
                continue
            completed = subprocess.run(
                [sys.executable, str(script), "run-many", "--batch-size", "240",
                 "--workers", str(min(6, batches)), "--max-batches", str(batches)],
                cwd=ROOT,
            )
            input(f"\nProvider runner exited with code {completed.returncode}. Press Return.")


def interactive(refresh: float, start_advanced: bool = False) -> int:
    if not sys.stdin.isatty() or not sys.stdout.isatty():
        renderer = render_advanced if start_advanced else render
        print(renderer(collect_snapshot(), use_color=False,
                       width=shutil.get_terminal_size((120, 40)).columns))
        return 0
    descriptor = sys.stdin.fileno()
    previous = termios.tcgetattr(descriptor)
    message = ""
    confirm_until = 0.0
    advanced = start_advanced
    try:
        tty.setcbreak(descriptor)
        sys.stdout.write("\033[?25l")
        while True:
            snapshot = collect_snapshot()
            width = shutil.get_terminal_size((120, 40)).columns
            renderer = render_advanced if advanced else render
            sys.stdout.write("\033[H\033[2J" + renderer(snapshot, width=width, message=message) + "\n")
            sys.stdout.flush()
            ready, _, _ = select.select([sys.stdin], [], [], refresh)
            if not ready:
                if confirm_until and time.monotonic() > confirm_until:
                    message, confirm_until = "Stop-now confirmation expired.", 0.0
                continue
            key = sys.stdin.read(1).lower()
            if key == "q":
                return 0
            if key == "h":
                advanced = not advanced
                message = "Technical details shown." if advanced else "Simple view restored."
                continue
            if key == "m":
                termios.tcsetattr(descriptor, termios.TCSADRAIN, previous)
                sys.stdout.write("\033[?25h\033[0m\033[H\033[2J")
                sys.stdout.flush()
                try:
                    message = campaign_menu()
                except (EOFError, KeyboardInterrupt):
                    message = "Campaign menu closed."
                finally:
                    tty.setcbreak(descriptor)
                    sys.stdout.write("\033[?25l")
                    sys.stdout.flush()
                continue
            if key == "v":
                termios.tcsetattr(descriptor, termios.TCSADRAIN, previous)
                sys.stdout.write("\033[?25h\033[0m\033[H\033[2J")
                sys.stdout.flush()
                try:
                    message = ai_queue_menu()
                except (EOFError, KeyboardInterrupt):
                    message = "AI review queue closed."
                finally:
                    tty.setcbreak(descriptor)
                    sys.stdout.write("\033[?25l")
                    sys.stdout.flush()
                continue
            if key == "k":
                termios.tcsetattr(descriptor, termios.TCSADRAIN, previous)
                sys.stdout.write("\033[?25h\033[0m\033[H\033[2J")
                sys.stdout.flush()
                try:
                    message = apify_profiles_menu()
                except (EOFError, KeyboardInterrupt):
                    message = "Apify profile manager closed."
                finally:
                    tty.setcbreak(descriptor)
                    sys.stdout.write("\033[?25l")
                    sys.stdout.flush()
                continue
            if key == "f":
                termios.tcsetattr(descriptor, termios.TCSADRAIN, previous)
                sys.stdout.write("\033[?25h\033[0m\033[H\033[2J")
                sys.stdout.flush()
                try:
                    message = phone_verification_menu()
                except (EOFError, KeyboardInterrupt):
                    message = "Phone verification menu closed."
                finally:
                    tty.setcbreak(descriptor)
                    sys.stdout.write("\033[?25l")
                    sys.stdout.flush()
                continue
            if key == "p":
                request("pause_after_wave")
                message = "Pause requested; the runner will stop after the current browser wave."
            elif key == "b":
                request("pause_after_batch")
                message = "Pause requested; the runner will stop after the current scope batch."
            elif key == "s":
                request("safe_stop")
                message = "Safe stop requested; the current checkpoint boundary will finish first."
            elif key == "r":
                clear()
                message = "Stop request cancelled. Relaunch the scraper if it is not active."
            elif key == "c":
                clear()
                message = "Stop request cancelled. Relaunch the scraper if it is not active."
            elif key == "x":
                if time.monotonic() <= confirm_until:
                    request("stop_now")
                    pids = interrupt_runners()
                    message = (f"Stop signal sent to {len(pids)} runner process(es)."
                               if pids else "Stop request saved; no active runner was found.")
                    confirm_until = 0.0
                else:
                    message = "Press X again within 5 seconds to stop the active runner."
                    confirm_until = time.monotonic() + 5
    finally:
        termios.tcsetattr(descriptor, termios.TCSADRAIN, previous)
        sys.stdout.write("\033[?25h\033[0m\n")
        sys.stdout.flush()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--once", action="store_true", help="Print one snapshot and exit.")
    parser.add_argument("--json", action="store_true", help="Print the raw snapshot as JSON.")
    parser.add_argument("--advanced", action="store_true", help="Start with technical details visible.")
    parser.add_argument("--refresh", type=float, default=2.0)
    args = parser.parse_args()
    if args.refresh < 0.25:
        parser.error("Refresh must be at least 0.25 seconds.")
    if args.once or args.json:
        snapshot = collect_snapshot()
        if args.json:
            serializable = dict(snapshot)
            for key in ("generated", "last_checkpoint"):
                value = serializable.get(key)
                serializable[key] = value.isoformat() if value else None
            print(json.dumps(serializable, indent=2, default=str))
        else:
            renderer = render_advanced if args.advanced else render
            print(renderer(snapshot, use_color=sys.stdout.isatty(),
                           width=shutil.get_terminal_size((120, 40)).columns))
        return 0
    return interactive(args.refresh, args.advanced)


if __name__ == "__main__":
    raise SystemExit(main())
