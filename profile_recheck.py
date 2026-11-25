#!/usr/bin/env python3
"""One-time, resumable profile evidence recheck for saved Maps result cards.

This intentionally does not mutate the statewide discovery ledger. It builds a
deduplicated cohort from saved cards, opens each Maps Place ID at most once per
attempt, and stores profile evidence separately from changeable qualification
rules.
"""

import argparse
import asyncio
import csv
import fcntl
import json
import random
import re
import sqlite3
import time
from contextlib import suppress
from datetime import datetime, timezone
from pathlib import Path

from playwright.async_api import Error, async_playwright

from chrome_capture import dismiss_consent, dismiss_maps_sign_in, maps_restriction, sign_in_prompt
from nord_proxy import (NordProxyError, launch_nord_browser, ordered_hosts, proxy_hosts,
                        route_state, save_route_state, service_credentials)
from prospect import MIN_RATING, MIN_REVIEWS, meets_reputation_floor, phone_key


HERE = Path(__file__).resolve().parent
SOURCE_DB = HERE / "runs" / "local-queue" / "queue.sqlite3"
OUTPUT = HERE / "runs" / "profile-recheck"
RECHECK_DB = OUTPUT / "recheck.sqlite3"
STATUS = OUTPUT / "status.json"
EVENTS = OUTPUT / "events.jsonl"
ALL_CSV = OUTPUT / "all-profile-evidence.csv"
ELIGIBLE_CSV = OUTPUT / "eligible-candidates.csv"
LOCK = OUTPUT / "profile-recheck.lock"
PROXY_STATE = HERE / ".runtime" / "nord-proxy-state.json"


def configure_output(output):
    """Point one recheck process at an isolated campaign evidence folder."""
    global OUTPUT, RECHECK_DB, STATUS, EVENTS, ALL_CSV, ELIGIBLE_CSV, LOCK
    OUTPUT = Path(output)
    RECHECK_DB = OUTPUT / "recheck.sqlite3"
    STATUS = OUTPUT / "status.json"
    EVENTS = OUTPUT / "events.jsonl"
    ALL_CSV = OUTPUT / "all-profile-evidence.csv"
    ELIGIBLE_CSV = OUTPUT / "eligible-candidates.csv"
    LOCK = OUTPUT / "profile-recheck.lock"


PROFILE_JS = r"""
main => {
  const visible = el => !!el && el.getClientRects().length > 0;
  const first = selectors => selectors.flatMap(selector => [...main.querySelectorAll(selector)]).find(visible);
  const text = el => (el?.getAttribute('aria-label') || el?.innerText || '').trim();
  const info = first(['[aria-label^="Information for "]']) || main;
  const address = first(['[data-item-id="address"]', '[aria-label^="Address:"]']);
  const phone = first(['[data-item-id^="phone:tel:"]', '[aria-label^="Phone:"]']);
  const website = first(['a[data-item-id="authority"]', 'a[aria-label^="Website:"]']);
  const category = first(['button[jsaction*="category"]', 'button.DkEaL', '.DkEaL']);
  const description = first(['[data-item-id="description"]']);
  const links = [...info.querySelectorAll('a[href]')].filter(visible);
  const bookingLinks = links.filter(el => /booking|book online|appointment|reservation/i.test(
    [el.getAttribute('aria-label'), el.getAttribute('data-item-id'), el.innerText].join(' ')
  )).map(el => el.href);
  const socialLinks = links.filter(el => /(^|\.)(facebook|instagram|tiktok|x|twitter)\.com$/i.test(
    (() => { try { return new URL(el.href).hostname; } catch { return ''; } })()
  )).map(el => el.href);
  const lines = main.innerText.split('\n').map(line => line.trim()).filter(Boolean);
  const service = lines.filter(line => /^(serves\b|service area\b)/i.test(line));
  const permanent = lines.some(line => /^permanently closed$/i.test(line));
  const temporary = lines.some(line => /^temporarily closed$/i.test(line));
  const phoneItem = phone?.getAttribute('data-item-id') || '';
  return {
    profile_name: text(first(['h1'])),
    category: text(category),
    address: text(address),
    phone: text(phone) || (phoneItem.startsWith('phone:tel:') ? phoneItem.slice(10) : ''),
    website: website?.href || '',
    booking_links: [...new Set(bookingLinks)],
    social_links: [...new Set(socialLinks)],
    service_area: service.join(' '),
    description: text(description),
    closure_status: permanent ? 'permanently_closed' : temporary ? 'temporarily_closed' : 'open_or_unknown',
    profile_text: main.innerText.slice(0, 30000)
  };
}
"""


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def number(value):
    try:
        return float(str(value).replace(",", ""))
    except (TypeError, ValueError):
        return None


def build_cohort(source=SOURCE_DB, state_filter="", niche_filter=""):
    """Return the unique no-icon cohort that meets the current reputation floor."""
    db = sqlite3.connect(source)
    grouped = {}
    try:
        for state, spec_raw, decisions_raw in db.execute(
                "SELECT state_code,spec,decisions FROM jobs WHERE decisions IS NOT NULL"):
            spec = json.loads(spec_raw)
            if state_filter and state != state_filter:
                continue
            niche = spec.get("niche", "auto detailing")
            if niche_filter and niche != niche_filter:
                continue
            query = spec.get("query", "")
            for row in json.loads(decisions_raw):
                listing_id = row.get("listing_id")
                if not listing_id:
                    continue
                item = grouped.setdefault(listing_id, {
                    "listing_id": listing_id, "website_seen": False, "observations": [],
                    "states": set(), "queries": set(),
                })
                item["website_seen"] |= (row.get("website_button") == "yes"
                                         or bool(row.get("website_url")))
                item["observations"].append(row)
                if state:
                    item["states"].add(state)
                if query:
                    item["queries"].add(query)
    finally:
        db.close()

    cohort = []
    for item in grouped.values():
        if item["website_seen"]:
            continue
        qualifying = [row for row in item["observations"] if meets_reputation_floor(row)]
        if not qualifying:
            continue
        best = max(qualifying, key=lambda row: (
            bool(phone_key(row.get("phone", ""))),
            bool(row.get("category")),
            number(row.get("reviews")) or 0,
        ))
        card_phone = next((phone_key(row.get("phone", "")) for row in qualifying
                           if phone_key(row.get("phone", ""))), "")
        cohort.append({
            "listing_id": item["listing_id"],
            "maps_url": best.get("maps_url", ""),
            "name": best.get("name", ""),
            "category": best.get("category", ""),
            "rating": number(best.get("rating")),
            "reviews": int(number(best.get("reviews")) or 0),
            "card_phone": card_phone,
            "states": sorted(item["states"]),
            "source_queries": sorted(item["queries"]),
            "observation_count": len(item["observations"]),
        })
    return sorted(cohort, key=lambda row: (-row["reviews"], row["name"].casefold(), row["listing_id"]))


class RecheckLedger:
    def __init__(self, path=None, source=SOURCE_DB, state_filter="", niche_filter=""):
        OUTPUT.mkdir(parents=True, exist_ok=True)
        path = path or RECHECK_DB
        self.db = sqlite3.connect(path)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS cohort (
                listing_id TEXT PRIMARY KEY,
                maps_url TEXT NOT NULL,
                name TEXT NOT NULL,
                card_category TEXT NOT NULL,
                rating REAL NOT NULL,
                reviews INTEGER NOT NULL,
                card_phone TEXT NOT NULL,
                states_json TEXT NOT NULL,
                source_queries_json TEXT NOT NULL,
                observation_count INTEGER NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                attempts INTEGER NOT NULL DEFAULT 0,
                claimed_by TEXT NOT NULL DEFAULT '',
                decision TEXT NOT NULL DEFAULT '',
                evidence_json TEXT,
                last_error TEXT NOT NULL DEFAULT '',
                created_utc TEXT NOT NULL,
                updated_utc TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS cohort_status_idx ON cohort(status, reviews DESC);
            CREATE TABLE IF NOT EXISTS events (
                id INTEGER PRIMARY KEY,
                utc TEXT NOT NULL,
                event TEXT NOT NULL,
                detail_json TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
        """)
        now = utc_now()
        rows = build_cohort(source, state_filter, niche_filter)
        with self.db:
            self.db.executemany("""
                INSERT OR IGNORE INTO cohort(
                    listing_id,maps_url,name,card_category,rating,reviews,card_phone,
                    states_json,source_queries_json,observation_count,created_utc,updated_utc
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
            """, [(row["listing_id"], row["maps_url"], row["name"], row["category"],
                    row["rating"], row["reviews"], row["card_phone"],
                    json.dumps(row["states"]), json.dumps(row["source_queries"]),
                    row["observation_count"], now, now) for row in rows])
            # A stopped process cannot leave records permanently claimed.
            self.db.execute("""
                UPDATE cohort SET status='retry',claimed_by='',updated_utc=?
                WHERE status='in_progress'
            """, (now,))
            self.db.execute("INSERT OR REPLACE INTO meta VALUES('source_cohort_size',?)", (str(len(rows)),))
            self.db.execute("INSERT OR REPLACE INTO meta VALUES('rules',?)", (
                json.dumps({"website_icon": "absent_in_all_saved_observations",
                            "rating": f">= {MIN_RATING}", "reviews": f">= {MIN_REVIEWS}",
                            "name_category_gate": False, "card_phone_gate": False}),))

    def close(self):
        self.db.close()

    def log(self, event, **detail):
        now = utc_now()
        payload = json.dumps(detail, ensure_ascii=False, sort_keys=True)
        with self.db:
            self.db.execute("INSERT INTO events(utc,event,detail_json) VALUES(?,?,?)",
                            (now, event, payload))
        with EVENTS.open("a") as file:
            file.write(json.dumps({"utc": now, "event": event, **detail}, ensure_ascii=False) + "\n")

    def claim(self, worker, max_attempts=3):
        self.db.execute("BEGIN IMMEDIATE")
        row = self.db.execute("""
            SELECT * FROM cohort
            WHERE status IN ('pending','retry') AND attempts < ?
              AND rating BETWEEN ? AND 5 AND reviews >= ?
            ORDER BY reviews DESC,name,listing_id LIMIT 1
        """, (max_attempts, MIN_RATING, MIN_REVIEWS)).fetchone()
        if not row:
            self.db.commit()
            return None
        now = utc_now()
        self.db.execute("""
            UPDATE cohort SET status='in_progress',attempts=attempts+1,claimed_by=?,updated_utc=?
            WHERE listing_id=?
        """, (worker, now, row["listing_id"]))
        self.db.commit()
        return dict(self.db.execute("SELECT * FROM cohort WHERE listing_id=?",
                                    (row["listing_id"],)).fetchone())

    def finish(self, row, evidence):
        profile_phone = phone_key(evidence.get("phone", ""))
        final_phone = profile_phone or row["card_phone"]
        closure = evidence.get("closure_status", "open_or_unknown")
        if evidence.get("website"):
            decision = "profile_has_website"
        elif closure == "permanently_closed":
            decision = "permanently_closed"
        elif closure == "temporarily_closed":
            decision = "temporarily_closed_hold"
        elif not final_phone:
            decision = "phone_missing"
        else:
            decision = "eligible_needs_relevance_review"
        evidence = {**evidence, "profile_phone": profile_phone, "final_phone": final_phone,
                    "checked_utc": utc_now()}
        with self.db:
            self.db.execute("""
                UPDATE cohort SET status='complete',decision=?,evidence_json=?,last_error='',
                                  claimed_by='',updated_utc=? WHERE listing_id=?
            """, (decision, json.dumps(evidence, ensure_ascii=False), utc_now(), row["listing_id"]))
        return decision, final_phone

    def fail(self, row, error, max_attempts=3):
        status = "blocked" if row["attempts"] >= max_attempts else "retry"
        with self.db:
            self.db.execute("""
                UPDATE cohort SET status=?,last_error=?,claimed_by='',updated_utc=?
                WHERE listing_id=?
            """, (status, str(error)[:500], utc_now(), row["listing_id"]))
        return status

    def counts(self):
        counts = {row[0]: row[1] for row in self.db.execute(
            "SELECT status,count(*) FROM cohort GROUP BY status")}
        decisions = {row[0]: row[1] for row in self.db.execute(
            "SELECT decision,count(*) FROM cohort WHERE status='complete' GROUP BY decision")}
        total = self.db.execute("SELECT count(*) FROM cohort").fetchone()[0]
        return total, counts, decisions

    def export(self):
        columns = ["listing_id", "name", "maps_url", "states", "rating", "reviews",
                   "card_category", "profile_category", "card_phone", "profile_phone",
                   "final_phone", "website", "address", "service_area", "closure_status",
                   "description", "booking_links", "social_links", "decision", "checked_utc"]
        all_rows = []
        for row in self.db.execute("SELECT * FROM cohort WHERE status='complete' ORDER BY name"):
            evidence = json.loads(row["evidence_json"])
            output = {
                "listing_id": row["listing_id"], "name": row["name"], "maps_url": row["maps_url"],
                "states": ",".join(json.loads(row["states_json"])), "rating": row["rating"],
                "reviews": row["reviews"], "card_category": row["card_category"],
                "profile_category": evidence.get("category", ""), "card_phone": row["card_phone"],
                "profile_phone": evidence.get("profile_phone", ""),
                "final_phone": evidence.get("final_phone", ""), "website": evidence.get("website", ""),
                "address": evidence.get("address", ""), "service_area": evidence.get("service_area", ""),
                "closure_status": evidence.get("closure_status", ""),
                "description": evidence.get("description", ""),
                "booking_links": json.dumps(evidence.get("booking_links", []), ensure_ascii=False),
                "social_links": json.dumps(evidence.get("social_links", []), ensure_ascii=False),
                "decision": row["decision"], "checked_utc": evidence.get("checked_utc", ""),
            }
            all_rows.append(output)
        for path, rows in ((ALL_CSV, all_rows),
                           (ELIGIBLE_CSV, [row for row in all_rows
                                           if row["decision"] == "eligible_needs_relevance_review"
                                           and meets_reputation_floor(row)])):
            temporary = path.with_suffix(".tmp")
            with temporary.open("w", newline="") as file:
                writer = csv.DictWriter(file, fieldnames=columns)
                writer.writeheader()
                writer.writerows(rows)
            temporary.replace(path)


def write_status(ledger, state, started, route=None, wave=0, baseline_complete=0):
    total, counts, decisions = ledger.counts()
    complete = counts.get("complete", 0)
    elapsed = max(time.monotonic() - started, 0.001)
    rate = max(complete - baseline_complete, 0) / elapsed * 3600
    remaining = total - complete - counts.get("blocked", 0)
    payload = {
        "updated_utc": utc_now(), "state": state, "cohort_total": total,
        "complete": complete, "pending": counts.get("pending", 0),
        "retry": counts.get("retry", 0), "in_progress": counts.get("in_progress", 0),
        "blocked": counts.get("blocked", 0), "decisions": decisions,
        "profiles_per_hour_this_process": round(rate, 1),
        "estimated_hours_remaining": round(remaining / rate, 2) if rate else None,
        "browser_wave": wave, "route": route or {},
    }
    temporary = STATUS.with_suffix(".tmp")
    temporary.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n")
    temporary.replace(STATUS)
    return payload


async def inspect_profile(page, row):
    started = time.monotonic()
    response = await page.goto(row["maps_url"].split("?")[0] + "?hl=en",
                               wait_until="domcontentloaded", timeout=20000)
    if restriction := maps_restriction(page, response):
        return {"error": restriction, "access_restricted": True}
    main = page.get_by_role("main").first
    await dismiss_consent(page, main)
    prompt = sign_in_prompt(page)
    await dismiss_maps_sign_in(prompt)
    await main.wait_for(state="visible", timeout=8000)
    evidence = await main.evaluate(PROFILE_JS)
    evidence["add_website"] = bool(await main.get_by_role(
        "button", name="Add website", exact=True).count())
    evidence["final_url"] = page.url
    evidence["load_seconds"] = round(time.monotonic() - started, 3)
    return evidence


async def run_wave(ledger, browser, tabs, budget, stop, started, route, wave, max_attempts,
                   baseline_complete):
    context = await browser.new_context(locale="en-US", viewport={"width": 1280, "height": 800})
    allocation_lock = asyncio.Lock()
    remaining = budget

    async def claim(worker):
        nonlocal remaining
        async with allocation_lock:
            if remaining <= 0 or stop.is_set():
                return None
            row = ledger.claim(worker, max_attempts)
            if row:
                remaining -= 1
            return row

    async def worker(number):
        page = await context.new_page()
        worker_name = f"wave-{wave}-tab-{number}"
        try:
            while row := await claim(worker_name):
                try:
                    evidence = await inspect_profile(page, row)
                    if evidence.get("error"):
                        status = ledger.fail(row, evidence["error"], max_attempts)
                        ledger.log("profile_error", listing_id=row["listing_id"], name=row["name"],
                                   worker=worker_name, status=status, error=evidence["error"])
                        if evidence.get("access_restricted"):
                            stop.set()
                    else:
                        decision, _phone = ledger.finish(row, evidence)
                        ledger.log("profile_checked", listing_id=row["listing_id"], name=row["name"],
                                   worker=worker_name, decision=decision,
                                   seconds=evidence.get("load_seconds"))
                except Error as error:
                    restriction = maps_restriction(page)
                    status = ledger.fail(row, restriction or str(error).splitlines()[0], max_attempts)
                    ledger.log("profile_error", listing_id=row["listing_id"], name=row["name"],
                               worker=worker_name, status=status,
                               error=restriction or str(error).splitlines()[0][:500])
                    if restriction:
                        stop.set()
                write_status(ledger, "paused" if stop.is_set() else "running", started, route, wave,
                             baseline_complete)
        finally:
            await page.close()

    try:
        await asyncio.gather(*(worker(number) for number in range(1, tabs + 1)))
    finally:
        await context.close()
    return budget - remaining


async def run_browser(args, ledger, started, baseline_complete):
    username, password = service_credentials()
    stop = asyncio.Event()
    total_limit = args.max_profiles
    processed_this_run = 0
    wave = 0
    route_failure_streak = 0
    async with async_playwright() as playwright:
        while not stop.is_set():
            total, counts, _decisions = ledger.counts()
            available = counts.get("pending", 0) + counts.get("retry", 0)
            if not available or (total_limit and processed_this_run >= total_limit):
                break
            wave += 1
            budget = min(args.profiles_per_browser, available)
            if total_limit:
                budget = min(budget, total_limit - processed_this_run)
            saved = route_state(PROXY_STATE)
            browser = bridge = None
            route_errors = []
            public_ip = ""
            host = ""
            for candidate in ordered_hosts(proxy_hosts(args.proxy_hosts), saved["host"]):
                try:
                    browser, bridge, public_ip = await launch_nord_browser(
                        playwright, candidate, username, password, saved["public_ip"])
                    host = candidate
                    save_route_state(PROXY_STATE, host, public_ip)
                    break
                except NordProxyError as error:
                    route_errors.append(str(error))
                    ledger.log("proxy_route_rejected", host=candidate, error=str(error))
            if not browser:
                route_failure_streak += 1
                factor = 1.5 ** (route_failure_streak - 1)
                low = min(args.route_retry_min_wait * factor, args.route_retry_wait_cap)
                high = min(args.route_retry_max_wait * factor, args.route_retry_wait_cap)
                wait_seconds = random.uniform(low, max(low, high))
                ledger.log("route_pool_unavailable", streak=route_failure_streak,
                           wait_seconds=round(wait_seconds, 1), errors=route_errors)
                write_status(ledger, "waiting_for_vpn_route", started, wave=wave,
                             baseline_complete=baseline_complete)
                print(f"All NordVPN routes are temporarily unavailable; retrying in "
                      f"{wait_seconds:.0f} seconds.", flush=True)
                await asyncio.sleep(wait_seconds)
                continue
            route_failure_streak = 0
            route = {"host": host, "public_ip": public_ip}
            ledger.log("browser_wave_started", wave=wave, budget=budget, tabs=args.tabs, **route)
            print(f"Wave {wave} | {host} | public IP {public_ip} | up to {budget} profiles", flush=True)
            write_status(ledger, "running", started, route, wave, baseline_complete)
            try:
                processed = await run_wave(ledger, browser, args.tabs, budget, stop, started,
                                           route, wave, args.max_attempts, baseline_complete)
            finally:
                await browser.close()
                await bridge.close()
            processed_this_run += processed
            ledger.export()
            payload = write_status(ledger, "paused" if stop.is_set() else "running",
                                   started, route, wave, baseline_complete)
            ledger.log("browser_wave_finished", wave=wave, processed=processed,
                       complete=payload["complete"], blocked=payload["blocked"])
            print(f"Wave {wave} complete | {payload['complete']:,}/{payload['cohort_total']:,} checked | "
                  f"{payload['profiles_per_hour_this_process']:.0f}/hour | "
                  f"{payload['decisions'].get('eligible_needs_relevance_review', 0):,} eligible so far",
                  flush=True)
            if stop.is_set() or (total_limit and processed_this_run >= total_limit):
                break
            await asyncio.sleep(random.uniform(args.rotation_min_wait, args.rotation_max_wait))
    return stop.is_set(), processed_this_run


def print_status(ledger):
    total, counts, decisions = ledger.counts()
    print(json.dumps({"cohort_total": total, "status": counts, "decisions": decisions,
                      "status_file": str(STATUS), "all_evidence_csv": str(ALL_CSV),
                      "eligible_csv": str(ELIGIBLE_CSV)}, indent=2))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--preview", action="store_true", help="Build and summarize the cohort without browsing.")
    parser.add_argument("--status", action="store_true", help="Show durable recheck progress without browsing.")
    parser.add_argument("--tabs", type=int, choices=range(1, 11), default=10)
    parser.add_argument("--profiles-per-browser", type=int, default=100,
                        help="Close and rotate Chrome after this many claimed profiles (default: 100).")
    parser.add_argument("--max-profiles", type=int,
                        help="Stop this invocation after this many claims; useful for a pilot.")
    parser.add_argument("--max-attempts", type=int, default=3)
    parser.add_argument("--rotation-min-wait", type=float, default=3)
    parser.add_argument("--rotation-max-wait", type=float, default=20)
    parser.add_argument("--proxy-hosts", help="Optional comma-separated Nord SOCKS5 hosts.")
    parser.add_argument("--route-retry-min-wait", type=float, default=30)
    parser.add_argument("--route-retry-max-wait", type=float, default=90)
    parser.add_argument("--route-retry-wait-cap", type=float, default=300)
    parser.add_argument("--source", type=Path, default=SOURCE_DB)
    parser.add_argument("--output", type=Path, default=OUTPUT)
    parser.add_argument("--state", default="", help="Optional campaign state filter.")
    parser.add_argument("--niche", default="", help="Optional exact campaign niche filter.")
    args = parser.parse_args()
    if args.profiles_per_browser < 1 or (args.max_profiles is not None and args.max_profiles < 1):
        parser.error("Profile limits must be positive.")
    if args.rotation_min_wait < 0 or args.rotation_max_wait < args.rotation_min_wait:
        parser.error("Rotation waits must satisfy 0 <= minimum <= maximum.")
    if (args.route_retry_min_wait < 0
            or args.route_retry_max_wait < args.route_retry_min_wait
            or args.route_retry_wait_cap < args.route_retry_max_wait):
        parser.error("Route retry waits must satisfy 0 <= minimum <= maximum <= cap.")

    configure_output(args.output)
    OUTPUT.mkdir(parents=True, exist_ok=True)
    with LOCK.open("a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            parser.exit(1, f"A profile recheck is already running. Status: {STATUS}\n")
        ledger = RecheckLedger(source=args.source, state_filter=args.state.upper(),
                               niche_filter=args.niche)
        started = time.monotonic()
        _total, initial_counts, _decisions = ledger.counts()
        baseline_complete = initial_counts.get("complete", 0)
        try:
            if args.preview or args.status:
                print_status(ledger)
                return
            ledger.log("run_started", tabs=args.tabs,
                       profiles_per_browser=args.profiles_per_browser,
                       max_profiles=args.max_profiles)
            paused, processed = asyncio.run(run_browser(args, ledger, started, baseline_complete))
            ledger.export()
            total, counts, _decisions = ledger.counts()
            remaining = counts.get("pending", 0) + counts.get("retry", 0)
            state = "paused_by_maps" if paused else "complete" if not remaining else "pilot_complete"
            payload = write_status(ledger, state, started, baseline_complete=baseline_complete)
            ledger.log("run_finished", processed=processed, **payload)
            print(f"Profile recheck {state}: {payload['complete']:,}/{total:,} checked; "
                  f"{payload['blocked']:,} blocked. Status: {STATUS}", flush=True)
        except KeyboardInterrupt:
            write_status(ledger, "stopped_safely", started, baseline_complete=baseline_complete)
            ledger.log("run_stopped", reason="keyboard_interrupt")
            print(f"Stopped safely. Resume with the same command. Status: {STATUS}")
        finally:
            ledger.close()


if __name__ == "__main__":
    main()
