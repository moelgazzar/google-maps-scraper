"""Run the saved geography queue with durable local checkpoints."""
import argparse
import asyncio
import csv
import fcntl
import hashlib
import json
import random
import re
import signal
import sqlite3
from collections import Counter, defaultdict, deque
from datetime import datetime, timezone
from itertools import islice
from math import ceil
from pathlib import Path
from time import perf_counter, sleep

from playwright.async_api import async_playwright

from batch_prospect import RUNTIME, dump, history, identity_key, profile_decision
from chrome_capture import capture_on_page, inspect_profile
from geography.catalog import HERE, SCREENABLE_CAPTURES, read_rows
from geography.launcher import DEFAULT_NICHE, catalog_jobs, postal_place, prepare_campaign, rotate_areas, worker_area
from nord_proxy import (NordProxyError, launch_nord_browser, ordered_hosts, proxy_hosts,
                        route_state, save_route_state, service_credentials)
from operator_control import PAUSE_ACTIONS, current_action
from prospect import ROOT, RUNS, meets_reputation_floor, phone_key, qualify

OUTPUT = RUNS / "local-queue"
DISCOVERY_ONLY = False
EXPORT_FIELDS = (
    "name", "market", "rating", "reviews", "phone", "phone_verification",
    "maps_url", "locality_status",
)
STATE_EXPORT_FIELDS = (*EXPORT_FIELDS, "searched_zip", "business_zip", "source_query", "niche")
RUN_EXPORT_FIELDS = (
    "run_id", "mode", "niche", "started_utc", "finished_utc", "wall_seconds", "workers",
    "browsers", "tabs_per_browser",
    "searches_scheduled", "searches_finished", "cards_captured", "cards_screened", "profiles_checked",
    "saved_address_matches", "new_unique", "cards_screened_per_hour", "new_unique_per_hour",
    "capture_tab_seconds", "profile_tab_seconds", "confirmed_ends", "limited_views", "card_limits",
    "scope_failures", "profile_errors", "status", "phone_checks",
)


def live_run_metrics(start, end, events):
    elapsed = (datetime.fromisoformat(end["utc"]) - datetime.fromisoformat(start["utc"])).total_seconds()
    captures = [e for e in events if e["event"] == "capture_saved"]
    complete = [e for e in events if e["event"] == "scope_complete"]
    profiles = [e for e in events if e["event"] == "profile_checked"]
    stops = Counter(e["stop"] for e in captures)
    screened = sum(e["screened"] for e in complete)
    added = sum(e["new_qualified"] for e in complete)
    return {"run_id": start["id"], "mode": "live_maps", "niche": start.get("niche", DEFAULT_NICHE),
            "started_utc": start["utc"], "finished_utc": end["utc"], "wall_seconds": round(elapsed, 2),
            "workers": start["workers"], "browsers": start.get("browsers", 1),
            "tabs_per_browser": start.get("tabs_per_browser", start["workers"]),
            "searches_scheduled": start["scheduled"], "searches_finished": len(complete),
            "cards_captured": sum(e["cards"] for e in captures), "cards_screened": screened, "profiles_checked": len(profiles),
            "new_unique": added, "cards_screened_per_hour": round(screened * 3600 / elapsed, 1) if elapsed else "",
            "new_unique_per_hour": round(added * 3600 / elapsed, 1) if elapsed else "",
            "capture_tab_seconds": round(sum(e["seconds"] for e in captures), 2),
            "profile_tab_seconds": round(sum(e["seconds"] for e in profiles), 2) if all("seconds" in e for e in profiles) else "",
            "confirmed_ends": stops["maps_end_of_list"], "limited_views": stops["maps_limited_view"],
            "card_limits": stops["requested_limit"], "scope_failures": sum(e["event"] == "scope_failed" for e in events),
            "profile_errors": sum(bool(e.get("error")) for e in profiles), "status": end["state"], "phone_checks": 0}


def now():
    return datetime.now(timezone.utc).isoformat()


def read_queue():
    with (ROOT / "geographies.csv").open(encoding="utf-8-sig", newline="") as source:
        parser = csv.DictReader(source)
        if not {"city", "state", "time_zone"} <= set(parser.fieldnames or []):
            raise ValueError("geographies.csv needs city, state and time_zone columns. zip is optional.")
        jobs = []
        keys = set()
        for pos, row in enumerate(parser, start=2):
            city = (row["city"] or "").strip()
            state = (row["state"] or "").strip().upper()
            zip_code = (row.get("zip") or "").strip()
            if not city or not re.fullmatch(r"[A-Z]{2}", state) or (zip_code and not re.fullmatch(r"\d{5}", zip_code)):
                raise ValueError(f"Check the city, state, or ZIP on geography row {pos}.")
            market = f"{city}, {state}"
            query = " ".join(part for part in ("auto detailing", city, state, zip_code) if part)
            key = hashlib.sha256(query.casefold().encode()).hexdigest()[:24]
            if key in keys:
                raise ValueError(f"Repeated search on geography row {pos}: {query}")
            keys.add(key)
            jobs.append({"key": key, "position": pos, "state": state, "city": city,
                         "market": market, "zip": zip_code, "query": query})
    return jobs


class Ledger:
    def __init__(self, root, baseline=(), state_export=None):
        self.root = root
        self.state_export = state_export
        self.baseline = list(baseline)
        self.old_ids = {row.get("listing_id") for row in self.baseline if row.get("listing_id")}
        self.old_phones = {phone_key(row.get("phone", "")) for row in self.baseline} - {""}
        self.old_names = {identity_key(row) for row in self.baseline} - {None}
        self.active = {}
        self.root.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(root / "queue.sqlite3")
        self.db.row_factory = sqlite3.Row
        self.db.executescript("""
            PRAGMA journal_mode=WAL;
            PRAGMA synchronous=FULL;
            CREATE TABLE IF NOT EXISTS jobs (
                key TEXT PRIMARY KEY,
                spec TEXT NOT NULL,
                state_code TEXT NOT NULL,
                sort_order INTEGER NOT NULL,
                listed INTEGER NOT NULL DEFAULT 1,
                stage TEXT NOT NULL,
                failures INTEGER NOT NULL DEFAULT 0,
                capture TEXT,
                decisions TEXT,
                error TEXT
            );
            CREATE TABLE IF NOT EXISTS qualified (
                listing_id TEXT PRIMARY KEY,
                phone_key TEXT NOT NULL UNIQUE,
                name_city TEXT NOT NULL UNIQUE,
                payload TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS events (
                id INTEGER PRIMARY KEY,
                utc TEXT NOT NULL,
                event TEXT NOT NULL,
                detail TEXT NOT NULL
            );
        """)
        self.event_cursor = 0
        (root / "events.jsonl").write_text("")
        self.write_log()

    def log(self, event, **detail):
        self.db.execute("INSERT INTO events(utc,event,detail) VALUES(?,?,?)",
                        (now(), event, json.dumps(detail, ensure_ascii=False)))

    def write_log(self):
        events = self.db.execute("SELECT * FROM events WHERE id>? ORDER BY id", (self.event_cursor,))
        with (self.root / "events.jsonl").open("a", encoding="utf-8") as log:
            for item in events:
                data = {"id": item["id"], "utc": item["utc"], "event": item["event"],
                        **json.loads(item["detail"])}
                log.write(json.dumps(data, ensure_ascii=False) + "\n")
                self.event_cursor = item["id"]

    def sync(self, jobs, completed, retry):
        completed = {market.casefold() for market in completed}
        with self.db:
            self.db.execute("UPDATE jobs SET listed=0")
            for job in jobs:
                already_done = (job.get("niche", DEFAULT_NICHE) == DEFAULT_NICHE and not job["zip"]
                                and job["market"].casefold() in completed)
                stage = "done" if already_done else "pending"
                self.db.execute("""
                    INSERT INTO jobs(key,spec,state_code,sort_order,stage)
                    VALUES(?,?,?,?,?)
                    ON CONFLICT(key) DO UPDATE SET listed=1, sort_order=excluded.sort_order,
                      spec=excluded.spec
                """, (job["key"], json.dumps(job), job["state"], job["position"], stage))
            if retry:
                self.db.execute("""
                    UPDATE jobs SET failures=0,error=NULL,
                      stage=CASE WHEN decisions IS NULL THEN 'pending' ELSE 'captured' END
                    WHERE listed=1 AND stage='blocked'
                """)
            self.log("queue_loaded", configured=len(jobs), historical_prospects=len(self.baseline),
                     retry_blocked=retry)

    def pending(self, limit):
        jobs = (json.loads(row["spec"]) for row in self.db.execute("""
            SELECT spec FROM jobs WHERE listed=1 AND stage IN ('pending','captured') ORDER BY sort_order
        """))
        return list(islice(rotate_areas(jobs), limit))

    def row(self, key):
        return self.db.execute("SELECT * FROM jobs WHERE key=?", (key,)).fetchone()

    def accepted(self):
        rows = (json.loads(row["payload"]) for row in self.db.execute(
            "SELECT payload FROM qualified ORDER BY rowid"))
        return [row for row in rows if meets_reputation_floor(row)]

    def duplicate_keys(self, row):
        listing, phone, name = row["listing_id"], phone_key(row["phone"]), identity_key(row)
        matches = self.db.execute("""
            SELECT listing_id,phone_key,name_city FROM qualified
            WHERE listing_id=? OR phone_key=? OR name_city=?
        """, (listing, phone, json.dumps(name))).fetchall()
        return {
            "same_maps_id": listing in self.old_ids or any(item["listing_id"] == listing for item in matches),
            "same_phone": phone in self.old_phones or any(item["phone_key"] == phone for item in matches),
            "same_name_city": name in self.old_names or any(item["name_city"] == json.dumps(name) for item in matches),
        }

    def status(self, state):
        counts = {row["stage"]: row["count"] for row in self.db.execute(
            "SELECT stage,count(*) AS count FROM jobs WHERE listed=1 GROUP BY stage")}
        workers = {}
        for slot, current in self.active.items():
            job = self.row(current["key"])
            capture = json.loads(job["capture"]) if job["capture"] else {"rows": []}
            decisions = json.loads(job["decisions"]) if job["decisions"] else []
            workers[slot] = {**current, "stage": job["stage"], "cards": len(capture["rows"]),
                             "profiles_checked": sum("profile" in row for row in decisions)}
        dump(self.root / "status.json", {
            "updated_utc": now(), "state": state, "queue": counts, "workers": workers,
            "qualified": self.db.execute("SELECT count(*) FROM qualified").fetchone()[0],
            "phone_checks": 0,
        })
        self.write_log()

    def export(self):
        rows = self.accepted()
        self.write_csv(self.root / "qualified.csv", rows, EXPORT_FIELDS)
        if self.state_export:
            state, path = self.state_export
            state_rows = []
            for row in rows:
                if row["state"] != state:
                    continue
                address = postal_place(row.get("profile", {}).get("address"))
                state_rows.append({**row, "searched_zip": row.get("zip", ""),
                                   "business_zip": row.get("business_zip") or (address[2] if address else ""),
                                   "source_query": row["query"], "niche": row.get("niche", DEFAULT_NICHE)})
            self.write_csv(path, state_rows, STATE_EXPORT_FIELDS)

    def export_runs(self, campaign, state):
        path = str(campaign)
        rows, batch = [], None
        for event in self.db.execute("""
            SELECT * FROM events WHERE event IN ('run_started','run_stopped','capture_saved',
                 'profile_checked','scope_complete','scope_failed','saved_profiles_rechecked') ORDER BY id
        """):
            e = {"id": event["id"], "utc": event["utc"], "event": event["event"], **json.loads(event["detail"])}
            if e["event"] == "run_started":
                batch = [e] if e.get("campaign") == path else None
            elif e["event"] == "run_stopped":
                if batch:
                    rows.append(live_run_metrics(batch[0], e, batch[1:]))
                batch = None
            elif e["event"] == "saved_profiles_rechecked":
                if e["state"] == state and e.get("campaign", path) == path:
                    rows.append({"run_id": e["id"], "mode": "saved_profile_recheck", "niche": e["niche"],
                                 "started_utc": e.get("started_utc", ""), "finished_utc": e["utc"],
                                 "wall_seconds": e.get("wall_seconds", ""), "saved_address_matches": e["address_matches"],
                                 "new_unique": e["new_qualified"], "status": "completed", "phone_checks": 0})
            elif batch is not None:
                batch.append(e)
        self.write_csv(campaign.with_name("runs.csv"), rows, RUN_EXPORT_FIELDS)

    @staticmethod
    def write_csv(path, rows, fields):
        temp = path.with_suffix(".tmp")
        with temp.open("w", newline="", encoding="utf-8") as file:
            writer = csv.DictWriter(file, fields, extrasaction="ignore")
            writer.writeheader()
            for row in rows:
                row = dict(row)
                for field in ("name", "market", "source_query", "niche"):
                    if re.match(r"^\s*[=+\-@]", row.get(field, "")):
                        row[field] = "'" + row[field]
                writer.writerow(row)
        temp.replace(path)

    def capture_saved(self, job, capture, decisions):
        with self.db:
            self.db.execute("UPDATE jobs SET capture=?,decisions=?,stage='captured',error=NULL WHERE key=?",
                            (json.dumps(capture), json.dumps(decisions), job["key"]))
            self.log("capture_saved", query=job["query"], cards=len(capture["rows"]), stop=capture["stop"],
                     seconds=capture["capture_seconds"])

    def capture_progress(self, job, capture):
        with self.db:
            self.db.execute("UPDATE jobs SET capture=? WHERE key=?", (json.dumps(capture), job["key"]))
            self.log("capture_progress", query=job["query"], cards=len(capture["rows"]),
                     seconds=capture["capture_seconds"])

    def checkpoint(self, job, rows, event, **detail):
        with self.db:
            self.db.execute("UPDATE jobs SET decisions=? WHERE key=?", (json.dumps(rows), job["key"]))
            self.log(event, query=job["query"], **detail)

    def fail(self, job, reason, capture=None):
        prior = self.row(job["key"])
        failures = prior["failures"] + 1
        stage = "blocked" if failures >= 2 else prior["stage"]
        with self.db:
            self.db.execute("UPDATE jobs SET failures=?,stage=?,error=?,capture=coalesce(?,capture) WHERE key=?",
                            (failures, stage, reason, json.dumps(capture) if capture else None, job["key"]))
            self.log("scope_failed", query=job["query"], reason=reason, failures=failures,
                     stage=stage, cards=len(capture["rows"]) if capture else None)
        return stage

    def save_qualified(self, row):
        """Save a qualified decision under the caller's active write transaction."""
        if not meets_reputation_floor(row):
            row.update(decision="below_threshold_or_missing", locality_status="See decision")
            return False
        key = row["listing_id"]
        matches = self.duplicate_keys(row)
        if any(matches.values()):
            row.update(decision="duplicate_at_export", locality_status="See decision")
            self.log("duplicate_at_export", query=row["query"], listing_id=key, **matches)
            return False
        self.db.execute("INSERT INTO qualified VALUES(?,?,?,?)",
                        (key, phone_key(row["phone"]), json.dumps(identity_key(row)), json.dumps(row, ensure_ascii=False)))
        return True

    def finish(self, job, rows):
        # Compare and save under the same write lock for all workers.
        with self.db:
            self.db.execute("BEGIN IMMEDIATE")
            added = sum(self.save_qualified(row) for row in rows if row["decision"] == "qualified")
            self.db.execute("UPDATE jobs SET decisions=?,stage='done',error=NULL WHERE key=?",
                            (json.dumps(rows), job["key"]))
            self.log("scope_complete", query=job["query"], screened=len(rows), new_qualified=added)
        self.export()
        return added


def screen(ledger, job, capture):
    contextual = [{**row, **job} for row in capture["rows"]]
    rows = qualify(contextual)
    for row in rows:
        if row["decision"] != "candidate":
            continue
        matches = ledger.duplicate_keys(row)
        if job.get("geography_filter") == "state":
            # Use the profile's actual city for name deduplication at export.
            matches["same_name_city"] = False
        for key, reason in (("same_maps_id", "duplicate_id"), ("same_phone", "duplicate_phone"),
                            ("same_name_city", "duplicate_name_and_city")):
            if matches[key]:
                row["decision"] = reason
                break
    return rows


def check_geography(row):
    if row["decision"] != "qualified":
        return
    address = row["profile"]["address"]
    location = postal_place(address)
    if row.get("geography_filter") == "state":
        if not location or location[1] != row["state"]:
            row.update(decision="locality_unconfirmed", locality_status="See decision")
        else:
            row.update(city=location[0], market=f"{location[0]}, {location[1]}", business_zip=location[2])
    elif row.get("scope_kind") == "zip_area":
        if not location or location[1:] != (row["state"], row["zip"]):
            row.update(decision="zip_unconfirmed", locality_status="See decision")
        else:
            row["city"], row["market"] = location[0], f"{location[0]}, {location[1]}"
    elif row["zip"] and (not location or location[1:] != (row["state"], row["zip"])):
        row.update(decision="zip_unconfirmed", locality_status="See decision")
    elif row.get("county_geo") and (not location or location[1] != row["state"]
                                    or location[2] not in row["allowed_county_zips"]):
        row.update(decision="county_unconfirmed", locality_status="See decision")


async def process_job(ledger, job, page, stop, limit):
    stored = ledger.row(job["key"])
    if stored["stage"] == "pending":
        saved_rows = json.loads(stored["capture"])["rows"] if stored["capture"] else []

        def checkpoint(partial):
            ledger.capture_progress(job, partial)
            return not stop.is_set()

        capture = await capture_on_page(page, job["query"], limit, saved_rows, checkpoint)
        if capture["stop"] == "paused":
            return
        if not capture["rows"] or capture["stop"] not in SCREENABLE_CAPTURES:
            # Keep the provider response and any partial card set in the ledger.
            # Pause allocation on access trouble without discarding other workers' checkpoints.
            ledger.fail(job, capture["error"] or capture["stop"], capture)
            if capture["stop"] in {"maps_error", "maps_access_restricted"}:
                stop.set()
            return
        rows = screen(ledger, job, capture)
        ledger.capture_saved(job, capture, rows)
    else:
        rows = json.loads(stored["decisions"])
        with ledger.db:
            ledger.log("resumed_capture", query=job["query"], screened=len(rows))
    if DISCOVERY_ONLY:
        # Production discovery must preserve every observed card without applying
        # the legacy early profile gates.  The separate profile_recheck ledger
        # globally claims each qualifying Place ID and captures complete evidence
        # exactly once before eligibility and relevance are derived.
        added = ledger.finish(job, rows)
        with ledger.db:
            ledger.log("discovery_only_scope_complete", query=job["query"],
                       observations=len(rows), legacy_qualified=added)
        print(f"{job['query']} | {len(rows)} observations saved for profile recheck", flush=True)
        return
    for row in rows:
        if stop.is_set():
            return
        if row["decision"] == "needs_qwen_review":
            row.update(decision="ambiguous_profile_hold", locality_status="See decision")
            ledger.checkpoint(job, rows, "profile_held", listing_id=row["listing_id"])
        if row["decision"] not in {"candidate", "profile_unavailable"}:
            continue
        started = perf_counter()
        evidence = await inspect_profile(page, row)
        row["profile"] = evidence
        row["geography_filter"] = job.get("geography_filter", "search")
        row["decision"] = profile_decision(row, evidence)
        row["locality_status"] = "Verified in Maps profile" if row["decision"] == "qualified" else "See decision"
        check_geography(row)
        ledger.checkpoint(job, rows, "profile_checked", listing_id=row["listing_id"],
                          decision=row["decision"], error=evidence.get("error"),
                          seconds=round(perf_counter() - started, 3))
        if evidence.get("access_restricted"):
            ledger.fail(job, evidence["error"])
            stop.set()
            return
    unavailable = sum(row["decision"] == "profile_unavailable" for row in rows)
    if unavailable:
        ledger.fail(job, f"{unavailable} profiles unavailable. Captured cards and completed reviews are saved.")
        return
    if stop.is_set():
        return
    added = ledger.finish(job, rows)
    print(f"{job['query']} | {added} new qualified", flush=True)


async def run_workers(ledger, jobs, contexts, workers, limit, stop, heartbeat=5, start_spacing=0,
                      slot_prefix=""):
    """Run durable area lanes across one or more browser contexts.

    An area normally gets one lane. When worker capacity exceeds the number of
    areas, its jobs are round-robin sharded into additional lanes. This lets the
    Florida statewide runner use ten concurrent tabs per region without losing
    the region-aware scheduling used by smaller runs.
    """
    if not isinstance(contexts, (list, tuple)):
        contexts = [contexts]
    if not contexts:
        raise ValueError("At least one browser context is required.")
    by_area = defaultdict(list)
    for job in jobs:
        by_area[worker_area(job)].append(job)
    lanes_per_area = max(1, ceil(workers / len(by_area))) if by_area else 1
    area_lanes = {}
    for area, area_jobs in by_area.items():
        shards = [[] for _ in range(min(lanes_per_area, len(area_jobs)))]
        for index, job in enumerate(area_jobs):
            shards[index % len(shards)].append(job)
        area_lanes[area] = shards
    # Interleave regions so every browser receives a geographic mix.
    lanes = []
    for lane_index in range(lanes_per_area):
        for area in by_area:
            shards = area_lanes[area]
            if lane_index < len(shards):
                lanes.append(shards[lane_index])
    area_queue = iter(lanes)
    finished = asyncio.Event()
    owned_slots = set()

    async def worker(number):
        slot = f"{slot_prefix}{number}"
        owned_slots.add(slot)
        if start_spacing:
            await asyncio.sleep((number - 1) * start_spacing)
        if stop.is_set():
            return
        page = await contexts[(number - 1) % len(contexts)].new_page()
        for area_jobs in area_queue:
            if stop.is_set():
                break
            state = area_jobs[0]["state"]
            region = area_jobs[0].get("region")
            location = {"state": state, **({"region": region} if region else {})}
            with ledger.db:
                ledger.log("region_assigned" if region else "state_assigned", worker=slot, **location)
            for job in area_jobs:
                if stop.is_set():
                    break
                ledger.active[slot] = {**location, "key": job["key"],
                                       "query": job["query"], "started_utc": now()}
                with ledger.db:
                    ledger.log("scope_started", worker=slot, query=job["query"], **location)
                ledger.status("running")
                await process_job(ledger, job, page, stop, limit)
                ledger.active.pop(slot, None)
                ledger.status("pausing" if stop.is_set() else "running")
            with ledger.db:
                ledger.log("region_released" if region else "state_released", worker=slot,
                           paused=stop.is_set(), **location)

    async def watch():
        while not finished.is_set():
            with ledger.db:
                ledger.log("heartbeat", active=ledger.active)
            ledger.status("pausing" if stop.is_set() else "running")
            try:
                await asyncio.wait_for(finished.wait(), heartbeat)
            except TimeoutError:
                continue

    monitor = asyncio.create_task(watch())
    try:
        async with asyncio.TaskGroup() as group:
            for number in range(1, min(workers, len(lanes)) + 1):
                group.create_task(worker(number))
    finally:
        finished.set()
        await monitor
        for slot in owned_slots:
            ledger.active.pop(slot, None)


async def browse(ledger, jobs, browsers, tabs_per_browser, limit, nord_route=None, previous_ip=None):
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)
    try:
        async with async_playwright() as playwright:
            launched = []
            bridge = None
            public_ip = None
            try:
                if nord_route:
                    host, username, password = nord_route
                    browser, bridge, public_ip = await launch_nord_browser(
                        playwright, host, username, password, previous_ip or ""
                    )
                    launched.append(browser)
                    launch_options = {"channel": "chrome", "headless": False,
                                      "proxy": {"server": bridge.browser_server}}
                else:
                    launch_options = {"channel": "chrome", "headless": False}
                # Keep the successfully launched processes reachable if a later
                # launch fails, so the cleanup below can still close them.
                for _ in range(browsers - len(launched)):
                    launched.append(await playwright.chromium.launch(**launch_options))
                contexts = list(await asyncio.gather(*(
                    browser.new_context(locale="en-US", viewport={"width": 1280, "height": 800})
                    for browser in launched
                )))
                if nord_route:
                    with ledger.db:
                        ledger.log("proxy_connected", host=nord_route[0], public_ip=public_ip)
                    print(f"NordVPN browser route: {nord_route[0]} | public IP {public_ip}", flush=True)
                await run_workers(ledger, jobs, contexts, browsers * tabs_per_browser,
                                  limit, stop, start_spacing=0.35)
            finally:
                await asyncio.gather(*(browser.close() for browser in launched), return_exceptions=True)
                if bridge:
                    await bridge.close()
    finally:
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.remove_signal_handler(sig)
    return stop.is_set(), public_ip


async def browse_independent_slots(ledger, jobs, browser_slots, tabs_per_browser, limit,
                                   max_rotations=None, nord_proxy_hosts=None,
                                   route_wait_seconds=60, rotation_min_wait=0,
                                   rotation_max_wait=0, route_retry_min_wait=0,
                                   route_retry_max_wait=0, route_retry_wait_cap=300):
    """Run self-rotating browser slots with one verified Nord route per browser.

    Each slot claims its own micro-batch, closes and rotates as soon as that batch
    finishes, and never waits for another slot's browser lifecycle. A Maps access
    restriction uses the shared stop event and pauses every slot.
    """
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)

    queue = deque(jobs)
    queue_lock = asyncio.Lock()
    route_lock = asyncio.Lock()
    hosts = ordered_hosts(proxy_hosts(nord_proxy_hosts), route_state(
        RUNTIME / "nord-proxy-state.json")["host"])
    credentials = service_credentials()
    proxy_state_path = RUNTIME / "nord-proxy-state.json"
    active_hosts, active_ips = set(), set()
    previous_ips = {}
    route_cursor = 0
    rotations = Counter()
    route_failure_streaks = Counter()
    capacity_errors = deque(maxlen=100)
    active_batches = 0

    async def claim_batch():
        nonlocal active_batches
        async with queue_lock:
            if current_action() in PAUSE_ACTIONS | {"stop_now"}:
                stop.set()
                return None if active_batches else []
            if not queue:
                return None if active_batches else []
            batch = [queue.popleft() for _ in range(min(tabs_per_browser, len(queue)))]
            active_batches += 1
            return batch

    async def finish_batch(batch):
        nonlocal active_batches
        retry = []
        for job in batch:
            stage = ledger.row(job["key"])["stage"]
            if stage in {"pending", "captured"}:
                retry.append(job)
        async with queue_lock:
            if retry:
                queue.extend(retry)
            active_batches -= 1

    async def acquire_route(playwright, slot):
        nonlocal route_cursor
        deadline = loop.time() + route_wait_seconds
        errors = []
        while not stop.is_set() and loop.time() < deadline:
            host = None
            async with route_lock:
                for offset in range(len(hosts)):
                    candidate = hosts[(route_cursor + offset) % len(hosts)]
                    if candidate not in active_hosts:
                        host = candidate
                        route_cursor = (route_cursor + offset + 1) % len(hosts)
                        active_hosts.add(host)
                        break
            if not host:
                await asyncio.sleep(min(1, max(0.01, deadline - loop.time())))
                continue
            try:
                browser, bridge, public_ip = await launch_nord_browser(
                    playwright, host, *credentials, previous_ips.get(slot, "")
                )
            except NordProxyError as error:
                errors.append(str(error))
                async with route_lock:
                    active_hosts.discard(host)
                with ledger.db:
                    ledger.log("proxy_route_rejected", browser_slot=slot, host=host, error=str(error))
                continue
            async with route_lock:
                if public_ip in active_ips:
                    duplicate = True
                else:
                    duplicate = False
                    active_ips.add(public_ip)
                    previous_ips[slot] = public_ip
                    save_route_state(proxy_state_path, host, public_ip)
            if duplicate:
                await browser.close()
                await bridge.close()
                async with route_lock:
                    active_hosts.discard(host)
                errors.append(f"NordVPN route {host} duplicated active public IP {public_ip}.")
                continue
            return host, public_ip, browser, bridge
        detail = " | ".join(errors[-len(hosts):]) or "No distinct NordVPN route became available."
        raise NordProxyError(f"Browser slot {slot} could not acquire a distinct route: {detail}")

    async def release_route(host, public_ip, browser, bridge):
        await browser.close()
        await bridge.close()
        async with route_lock:
            active_hosts.discard(host)
            active_ips.discard(public_ip)

    async def browser_slot(playwright, number):
        slot = f"browser-{number}"
        while not stop.is_set() and (max_rotations is None or rotations[slot] < max_rotations):
            batch = await claim_batch()
            if batch is None:
                await asyncio.sleep(0.1)
                continue
            if not batch:
                return
            try:
                host, public_ip, browser, bridge = await acquire_route(playwright, slot)
            except NordProxyError as error:
                await finish_batch(batch)
                capacity_errors.append(str(error))
                route_failure_streaks[slot] += 1
                multiplier = 1.5 ** (route_failure_streaks[slot] - 1)
                low = min(route_retry_wait_cap, route_retry_min_wait * multiplier)
                high = min(route_retry_wait_cap, route_retry_max_wait * multiplier)
                delay = round(random.uniform(low, max(low, high)), 2)
                with ledger.db:
                    ledger.log("browser_slot_waiting_for_route", browser_slot=slot,
                               reason="distinct_route_unavailable", error=str(error),
                               wait_seconds=delay,
                               consecutive_failures=route_failure_streaks[slot])
                print(f"{slot}: no distinct healthy NordVPN route; waiting {delay:.2f}s "
                      f"before retrying (failure {route_failure_streaks[slot]}).", flush=True)
                if delay:
                    try:
                        await asyncio.wait_for(stop.wait(), delay)
                    except TimeoutError:
                        pass
                continue
            route_failure_streaks[slot] = 0
            rotations[slot] += 1
            rotation = rotations[slot]
            with ledger.db:
                ledger.log("browser_slot_started", browser_slot=slot, rotation=rotation,
                           scheduled=len(batch), host=host, public_ip=public_ip)
            print(f"{slot} rotation {rotation}: {host} | public IP {public_ip} | "
                  f"{len(batch)} searches", flush=True)
            try:
                context = await browser.new_context(locale="en-US", viewport={"width": 1280, "height": 800})
                try:
                    await run_workers(ledger, batch, context, len(batch), limit, stop,
                                      start_spacing=0.35, slot_prefix=f"b{number}.")
                finally:
                    await context.close()
            finally:
                await release_route(host, public_ip, browser, bridge)
            completed = sum(ledger.row(job["key"])["stage"] == "done" for job in batch)
            await finish_batch(batch)
            with ledger.db:
                ledger.log("browser_slot_finished", browser_slot=slot, rotation=rotation,
                           scheduled=len(batch), completed=completed, paused=stop.is_set(),
                           host=host, public_ip=public_ip)
            action = current_action()
            if action in PAUSE_ACTIONS | {"stop_now"}:
                with ledger.db:
                    ledger.log("operator_control_observed", action=action,
                               boundary="browser_batch", browser_slot=slot,
                               rotation=rotation)
                stop.set()
            if (not stop.is_set() and rotation_max_wait > 0
                    and (max_rotations is None or rotations[slot] < max_rotations)):
                delay = round(random.uniform(rotation_min_wait, rotation_max_wait), 2)
                with ledger.db:
                    ledger.log("browser_slot_waiting", browser_slot=slot, rotation=rotation,
                               wait_seconds=delay)
                print(f"{slot} rotation {rotation} finished; waiting {delay:.2f}s before "
                      "claiming its next batch.", flush=True)
                try:
                    await asyncio.wait_for(stop.wait(), delay)
                except TimeoutError:
                    pass

    try:
        async with async_playwright() as playwright:
            async with asyncio.TaskGroup() as group:
                for number in range(1, browser_slots + 1):
                    group.create_task(browser_slot(playwright, number))
    finally:
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.remove_signal_handler(sig)
    return {"paused": stop.is_set(), "rotations": dict(rotations),
            "capacity_errors": list(capacity_errors), "remaining": len(queue)}


def recheck_saved(ledger, config, campaign=None):
    state = config["states"][0]
    niche = config["niche"]
    started_utc = now()
    started = perf_counter()
    if config.get("geography_filter") != "state":
        raise ValueError("Rechecking saved profiles requires a state address filter.")
    inspected = added = 0
    with ledger.db:
        ledger.db.execute("BEGIN IMMEDIATE")
        for job in ledger.db.execute("SELECT spec, decisions FROM jobs WHERE state_code=? AND stage='done'", (state,)):
            if not job["decisions"] or json.loads(job["spec"]).get("niche", DEFAULT_NICHE) != niche:
                continue
            for row in json.loads(job["decisions"]):
                if row["decision"] not in {"locality_unconfirmed", "zip_unconfirmed", "county_unconfirmed"}:
                    continue
                profile = row.get("profile", {})
                location = postal_place(profile.get("address"))
                if not location or location[1] != state:
                    continue
                if config.get("match_terms"):
                    row["match_terms"] = config["match_terms"]
                if qualify([row])[0]["decision"] != "candidate":
                    continue
                row.update(geography_filter="state", niche=niche, evidence_source="saved Maps profile")
                row["decision"] = profile_decision(row, profile)
                check_geography(row)
                if row["decision"] != "qualified":
                    continue
                inspected += 1
                row["locality_status"] = "State verified from saved Maps profile"
                if ledger.save_qualified(row):
                    added += 1
                    ledger.log("saved_profile_requalified", query=row["query"], listing_id=row["listing_id"],
                               searched_zip=row["zip"], business_zip=row["business_zip"])
        detail = {"state": state, "niche": niche, "address_matches": inspected, "new_qualified": added,
                  "started_utc": started_utc, "wall_seconds": round(perf_counter() - started, 3)}
        if campaign:
            detail["campaign"] = str(campaign)
        ledger.log("saved_profiles_rechecked", **detail)
    ledger.export()
    ledger.write_log()
    print(f"{state} | {inspected} matching saved decisions | {added} new unique prospects | no browser searches")


def run(args):
    baseline, done = history(include_queue=False)
    completed = {market.casefold() for market in done}
    campaign = args.campaign_file.resolve() if args.campaign_file else None
    config = json.loads(campaign.read_text()) if campaign else None
    if config and len(config["states"]) != 1:
        raise ValueError("A state folder campaign must select exactly one state.")
    state_export = (config["states"][0], campaign.with_name("prospects.csv")) if config else None
    ledger = Ledger(OUTPUT, baseline, state_export)
    if args.recheck_saved:
        try:
            if not config:
                raise ValueError("Choose a state folder with --campaign-file to recheck its saved profiles.")
            recheck_saved(ledger, config, campaign)
            status_path = OUTPUT / "status.json"
            previous_state = json.loads(status_path.read_text())["state"] if status_path.is_file() else "idle"
            ledger.status(previous_state)
            ledger.export_runs(campaign, config["states"][0])
        finally:
            ledger.db.close()
        return
    if args.preview:
        try:
            configured = catalog_jobs(ledger, read_queue(), args.jobs, args.retry_blocked, args.florida_regions, campaign)
            ready = []
            for job in sorted(configured, key=lambda row: row["position"]):
                stored = ledger.row(job["key"])
                if stored and (stored["stage"] == "done" or (stored["stage"] == "blocked" and not args.retry_blocked)):
                    continue
                if (not stored and job.get("niche", DEFAULT_NICHE) == DEFAULT_NICHE and not job["zip"]
                        and job["market"].casefold() in completed):
                    continue
                ready.append(job)
            print(json.dumps([{"query": row["query"], "scope": row.get("scope_id"),
                               "reason": row.get("scope_reason", "Manual queue"),
                               **({"region": row["region"]} if row.get("region") else {})}
                              for row in islice(rotate_areas(ready), args.jobs)],
                             indent=2, ensure_ascii=False))
        finally:
            ledger.db.close()
        return
    state = "error"
    try:
        configured = catalog_jobs(ledger, read_queue(), args.jobs, args.retry_blocked, args.florida_regions, campaign)
        ledger.sync(configured, done, args.retry_blocked)
        ledger.export()
        jobs = ledger.pending(args.jobs)
        if args.florida_regions:
            before = ledger.db.execute("SELECT count(*) FROM qualified").fetchone()[0]
        state = "idle"
        operator_action = current_action()
        if jobs and operator_action in PAUSE_ACTIONS | {"stop_now"}:
            state = "operator_paused"
            with ledger.db:
                ledger.log("operator_control_observed", action=operator_action,
                           boundary="before_browser_start")
        elif jobs:
            settings = RUNTIME / "batch-settings.json"
            saved_workers = json.loads(settings.read_text())["tabs"] if settings.exists() else 2
            if args.browsers or args.tabs_per_browser:
                browsers = args.browsers or 1
                tabs_per_browser = args.tabs_per_browser or args.workers or saved_workers
                workers = browsers * tabs_per_browser
            else:
                browsers = 1
                tabs_per_browser = args.workers or saved_workers
                workers = tabs_per_browser
            workers = min(workers, len(jobs))
            wave_size = min(args.wave_size or len(jobs), len(jobs))
            with ledger.db:
                ledger.log("run_started", workers=workers, scheduled=len(jobs), cards_per_scope=args.limit,
                           browsers=browsers, tabs_per_browser=tabs_per_browser,
                           wave_size=wave_size, wave_pause_seconds=args.wave_pause_seconds,
                           florida_regions=args.florida_regions, campaign=str(campaign) if campaign else None,
                           niche=config["niche"] if config else DEFAULT_NICHE)
            if args.independent_browser_slots:
                pool = asyncio.run(browse_independent_slots(
                    ledger, jobs, browsers, tabs_per_browser, args.limit,
                    args.max_browser_rotations, args.nordvpn_proxy_hosts,
                    rotation_min_wait=args.rotation_min_wait_seconds,
                    rotation_max_wait=args.rotation_max_wait_seconds,
                    route_retry_min_wait=args.route_retry_min_wait_seconds,
                    route_retry_max_wait=args.route_retry_max_wait_seconds,
                    route_retry_wait_cap=args.route_retry_wait_cap_seconds,
                ))
                if pool["paused"]:
                    state = "paused"
                elif args.max_browser_rotations:
                    state = "pilot_complete"
                elif pool["remaining"]:
                    state = "capacity_limited"
                else:
                    state = "idle"
                with ledger.db:
                    ledger.log("browser_pool_finished", browsers=browsers,
                               tabs_per_browser=tabs_per_browser, **pool)
            wave_number = 0
            proxy_state_path = RUNTIME / "nord-proxy-state.json"
            saved_proxy = route_state(proxy_state_path)
            previous_proxy_ip = saved_proxy["public_ip"] if args.nordvpn_proxy else None
            nord_hosts = ordered_hosts(proxy_hosts(args.nordvpn_proxy_hosts), saved_proxy["host"]) \
                if args.nordvpn_proxy else ()
            nord_credentials = service_credentials() if args.nordvpn_proxy else None
            while not args.independent_browser_slots:
                wave = ledger.pending(wave_size)
                if not wave:
                    state = "idle"
                    break
                wave_number += 1
                with ledger.db:
                    ledger.log("wave_started", wave=wave_number, scheduled=len(wave))
                active_workers = min(workers, len(wave))
                active_tabs_per_browser = min(tabs_per_browser, ceil(active_workers / browsers))
                if nord_hosts:
                    paused = None
                    route_errors = []
                    for offset in range(len(nord_hosts)):
                        host = nord_hosts[(wave_number - 1 + offset) % len(nord_hosts)]
                        try:
                            paused, public_ip = asyncio.run(browse(
                                ledger, wave, browsers, active_tabs_per_browser, args.limit,
                                (host, *nord_credentials), previous_proxy_ip,
                            ))
                            previous_proxy_ip = public_ip
                            save_route_state(proxy_state_path, host, public_ip)
                            break
                        except NordProxyError as error:
                            route_errors.append(str(error))
                            with ledger.db:
                                ledger.log("proxy_route_rejected", host=host, error=str(error))
                    if paused is None:
                        raise NordProxyError("No fresh NordVPN browser route was available: "
                                             + " | ".join(route_errors))
                else:
                    paused, _public_ip = asyncio.run(
                        browse(ledger, wave, browsers, active_tabs_per_browser, args.limit)
                    )
                completed_wave = sum(ledger.row(job["key"])["stage"] == "done" for job in wave)
                with ledger.db:
                    ledger.log("wave_finished", wave=wave_number, scheduled=len(wave),
                               completed=completed_wave, paused=paused)
                if paused:
                    state = "paused"
                    break
                if not ledger.pending(1):
                    state = "idle"
                    break
                state = "waiting_between_waves"
                ledger.status(state)
                if args.wave_pause_seconds:
                    print(f"Wave {wave_number} finished; waiting {args.wave_pause_seconds} seconds.", flush=True)
                    sleep(args.wave_pause_seconds)
        if ledger.db.execute("SELECT 1 FROM jobs WHERE listed=1 AND stage='blocked'").fetchone() and state == "idle":
            state = "needs_attention"
        if args.florida_regions:
            finished = sum(ledger.row(job["key"])["stage"] == "done" for job in jobs)
            after = ledger.db.execute("SELECT count(*) FROM qualified").fetchone()[0]
            searched = ledger.db.execute("""
                SELECT count(DISTINCT json_extract(spec, '$.zip')) FROM jobs
                WHERE state_code='FL' AND stage='done' AND json_extract(spec, '$.scope_kind')='zip_area'
            """).fetchone()[0]
            zip_total = sum(row["state"] == "FL" for row in read_rows("zip-areas.csv"))
    except Exception as error:
        with ledger.db:
            ledger.log("run_error", error=str(error), error_type=type(error).__name__)
        raise
    finally:
        try:
            with ledger.db:
                ledger.log("run_stopped", state=state)
            ledger.status(state)
            if config:
                ledger.export_runs(campaign, config["states"][0])
        finally:
            ledger.db.close()
    if args.florida_regions:
        print(f"Florida batch ended. Finished {finished} of {len(jobs)} searches and added {after - before} new prospects.")
        print(f"Across all batches, {searched:,} of {zip_total:,} Florida ZIP areas have a completed search.")
        print(f"Batch status: {state}. Details: {OUTPUT / 'status.json'}")
    else:
        print(f"Local queue is {state}. Status: {OUTPUT / 'status.json'}")
    print(f"Cumulative new-prospect CSV: {OUTPUT / 'qualified.csv'}")
    if state_export:
        print(f"State prospect CSV: {state_export[1]}")
        print(f"State run metrics: {campaign.with_name('runs.csv')}")


def main():
    global DISCOVERY_ONLY
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--status", action="store_true", help="Read the latest local status without opening Chrome.")
    parser.add_argument("--preview", action="store_true", help="Show the next geography searches without opening Chrome.")
    parser.add_argument("--configure", action="store_true", help="Prepare a state and niche without opening Chrome.")
    parser.add_argument("--campaign-file", type=Path, help="Use the campaign settings and CSV within a state folder.")
    parser.add_argument("--recheck-saved", action="store_true", help="Reapply a state's address filter to saved Maps evidence.")
    parser.add_argument("--discovery-only", action="store_true",
                        help="Save every result-card observation and defer all profile work to profile_recheck.py.")
    parser.add_argument("--state", help="US postal abbreviation for --configure.")
    parser.add_argument("--niche", help="Primary search phrase for --configure.")
    parser.add_argument("--alternate", help="Optional alternate phrase for deeper searches.")
    parser.add_argument("--match", action="append", default=[], help="Optional business name or category phrase. Repeat to allow more phrases.")
    parser.add_argument("--retry-blocked", action="store_true", help="Reset the retry allowance for blocked scopes.")
    parser.add_argument("--florida-regions", action="store_true", help="Allocate Florida jobs across county-based regions.")
    parser.add_argument("--workers", type=int, choices=range(1, 101), metavar="1–100",
                        help="Legacy single-browser worker count.")
    parser.add_argument("--browsers", type=int, choices=range(1, 11), metavar="1–10",
                        help="Number of separate Chrome processes.")
    parser.add_argument("--tabs-per-browser", type=int, choices=range(1, 11), metavar="1–10",
                        help="Concurrent worker tabs in each Chrome process.")
    parser.add_argument("--independent-browser-slots", action="store_true",
                        help="Let each browser claim, close, rotate and restart independently.")
    parser.add_argument("--max-browser-rotations", type=int, choices=range(1, 101), metavar="1–100",
                        help="Pilot limit per independent browser slot; omit for a continuous run.")
    parser.add_argument("--rotation-min-wait-seconds", type=float, default=3,
                        help="Minimum independent wait after a browser slot rotation (default: 3).")
    parser.add_argument("--rotation-max-wait-seconds", type=float, default=20,
                        help="Maximum independent wait after a browser slot rotation (default: 20).")
    parser.add_argument("--route-retry-min-wait-seconds", type=float, default=30,
                        help="Minimum wait after a slot exhausts the Nord route pool (default: 30).")
    parser.add_argument("--route-retry-max-wait-seconds", type=float, default=90,
                        help="Maximum wait after a slot exhausts the Nord route pool (default: 90).")
    parser.add_argument("--route-retry-wait-cap-seconds", type=float, default=300,
                        help="Adaptive ceiling for repeated route-exhaustion waits (default: 300).")
    parser.add_argument("--jobs", type=int, default=4, help="Maximum saved scopes per click.")
    parser.add_argument("--wave-size", type=int, choices=range(1, 10001), metavar="1–10000",
                        help="Process at most this many scopes before pausing for the next wave.")
    parser.add_argument("--wave-pause-seconds", type=int, choices=range(0, 3601), default=0,
                        metavar="0–3600", help="Seconds to wait between browser waves.")
    parser.add_argument("--nordvpn-proxy", action="store_true", default=True,
                        help="Route only each launched browser through a rotating NordVPN SOCKS5 proxy.")
    parser.add_argument("--nordvpn-proxy-hosts",
                        help="Optional comma-separated NordVPN SOCKS5 hosts; defaults to the official route pool.")
    depth = parser.add_mutually_exclusive_group()
    depth.add_argument("--limit", type=int, choices=range(1, 31), default=30, metavar="1–30")
    depth.add_argument("--full-scroll", dest="limit", action="store_const", const=None,
                       help="Scroll each scheduled search to the end of its available Maps results.")
    args = parser.parse_args()
    DISCOVERY_ONLY = args.discovery_only
    try:
        if args.recheck_saved and (args.preview or args.configure or args.status):
            raise ValueError("Recheck saved profiles as a separate action.")
        if args.status:
            print((OUTPUT / "status.json").read_text(), end="")
            return
        if args.jobs < 1:
            raise ValueError("Schedule at least one scope.")
        if args.max_browser_rotations and not args.independent_browser_slots:
            raise ValueError("Use --max-browser-rotations with --independent-browser-slots.")
        if (args.rotation_min_wait_seconds < 0
                or args.rotation_max_wait_seconds < args.rotation_min_wait_seconds):
            raise ValueError("Rotation wait range must satisfy 0 <= minimum <= maximum.")
        if (args.route_retry_min_wait_seconds < 0
                or args.route_retry_max_wait_seconds < args.route_retry_min_wait_seconds):
            raise ValueError("Route retry wait range must satisfy 0 <= minimum <= maximum.")
        if args.route_retry_wait_cap_seconds < args.route_retry_max_wait_seconds:
            raise ValueError("Route retry wait cap must be at least the maximum initial wait.")
        with (RUNTIME / "prospecting.lock").open("a") as lock:
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                raise RuntimeError("A prospecting run is already active. Use --status for progress.") from None
            if args.configure:
                if not args.state or not args.niche:
                    raise ValueError("Use --configure with --state and --niche.")
                prepared = prepare_campaign(args.state, args.niche, args.alternate, args.match)
                if not args.preview:
                    path = args.campaign_file or HERE / "active.json"
                    if args.campaign_file:
                        if path.exists() and json.loads(path.read_text())["states"] != prepared["campaign"]["states"]:
                            raise ValueError("Choose a separate campaign folder to change the state.")
                        prepared["campaign"]["geography_filter"] = "state"
                    dump(path, prepared["campaign"])
                print(json.dumps(prepared, indent=2, ensure_ascii=False))
            else:
                if args.state or args.niche or args.alternate or args.match:
                    raise ValueError("Use --configure to prepare a campaign.")
                run(args)
    except (OSError, RuntimeError, ValueError, sqlite3.Error) as error:
        parser.exit(1, f"Error: {error}\n")


if __name__ == "__main__":
    main()
