#!/usr/bin/env python3
"""Durable named-campaign registry for Google Maps Scraper."""

from __future__ import annotations

import json
import os
import re
import shlex
import sqlite3
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

from geography.launcher import NICHE_PRESETS, campaign_settings
from nationwide_orchestrator import STATE_NAMES


ROOT = Path(__file__).resolve().parent
DB = ROOT / ".runtime" / "campaigns.sqlite3"
CAMPAIGNS = ROOT / "Campaigns"
REGISTRY = ROOT / "geography" / "state-registry.json"
ACTIVE_STATUSES = {"starting", "running", "pausing"}
VISIBLE_STATUSES = {
    "draft", "ready", "queued", "starting", "running", "pausing", "paused",
    "completed", "attention",
}


class ManagedConnection(sqlite3.Connection):
    """Commit or roll back a context, then close it instead of leaking handles."""

    def __exit__(self, exc_type, exc_value, traceback):
        result = super().__exit__(exc_type, exc_value, traceback)
        self.close()
        return result


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def slugify(value: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", value.casefold()).strip("-")
    return slug[:60] or "campaign"


def atomic_json(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, indent=2) + "\n", encoding="utf-8")
    temporary.replace(path)


def connect(path: Path = DB) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(path, factory=ManagedConnection)
    db.row_factory = sqlite3.Row
    db.executescript("""
        PRAGMA journal_mode=WAL;
        PRAGMA synchronous=FULL;
        CREATE TABLE IF NOT EXISTS campaigns (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL COLLATE NOCASE UNIQUE,
            slug TEXT NOT NULL UNIQUE,
            state TEXT NOT NULL,
            niche TEXT NOT NULL,
            alternate TEXT,
            config_path TEXT NOT NULL,
            status TEXT NOT NULL,
            archived INTEGER NOT NULL DEFAULT 0,
            deleted INTEGER NOT NULL DEFAULT 0,
            legacy INTEGER NOT NULL DEFAULT 0,
            created_utc TEXT NOT NULL,
            updated_utc TEXT NOT NULL,
            started_utc TEXT,
            finished_utc TEXT,
            last_error TEXT
        );
        CREATE TABLE IF NOT EXISTS campaign_events (
            id INTEGER PRIMARY KEY,
            campaign_id INTEGER NOT NULL,
            utc TEXT NOT NULL,
            event TEXT NOT NULL,
            detail TEXT NOT NULL,
            FOREIGN KEY(campaign_id) REFERENCES campaigns(id)
        );
    """)
    return db


def registry_states(registry: Path = REGISTRY) -> dict[str, dict]:
    data = json.loads(registry.read_text(encoding="utf-8"))
    ready = {row["state"]: row for row in data["states"] if row.get("ready")}
    if len(ready) != 51:
        raise RuntimeError(f"Nationwide registry is incomplete: {len(ready)}/51 ready")
    return ready


def log(db: sqlite3.Connection, campaign_id: int, event: str, **detail) -> None:
    db.execute(
        "INSERT INTO campaign_events(campaign_id,utc,event,detail) VALUES(?,?,?,?)",
        (campaign_id, utc_now(), event, json.dumps(detail, ensure_ascii=False)),
    )


def unique_slug(db: sqlite3.Connection, name: str) -> str:
    base = slugify(name)
    slug = base
    number = 2
    while db.execute("SELECT 1 FROM campaigns WHERE slug=?", (slug,)).fetchone():
        slug = f"{base}-{number}"
        number += 1
    return slug


def create_campaign(name: str, state: str, niche: str,
                    db_path: Path = DB, campaigns_root: Path = CAMPAIGNS,
                    config_path: Path | None = None, legacy: bool = False,
                    status: str = "ready") -> dict:
    name = " ".join(name.split())
    if not name or len(name) > 100:
        raise ValueError("Campaign name must contain 1 to 100 characters.")
    state = state.strip().upper()
    ready = registry_states()
    if state not in ready:
        raise ValueError(f"State {state!r} is not ready in the nationwide registry.")
    settings = campaign_settings(state, niche)
    settings["geography_filter"] = "state"
    now = utc_now()
    with connect(db_path) as db:
        slug = unique_slug(db, name)
        target = config_path or campaigns_root / slug / "campaign.json"
        try:
            cursor = db.execute("""
                INSERT INTO campaigns(name,slug,state,niche,alternate,config_path,status,
                  legacy,created_utc,updated_utc)
                VALUES(?,?,?,?,?,?,?,?,?,?)
            """, (name, slug, state, settings["niche"], settings.get("alternate"),
                  str(target), status, int(legacy), now, now))
        except sqlite3.IntegrityError as error:
            raise ValueError(f"A campaign named {name!r} already exists.") from error
        campaign_id = cursor.lastrowid
        log(db, campaign_id, "campaign_created", state=state, niche=settings["niche"], legacy=legacy)
        atomic_json(target, settings)
    return get_campaign(campaign_id, db_path)


def get_campaign(campaign_id: int, db_path: Path = DB) -> dict:
    with connect(db_path) as db:
        row = db.execute("SELECT * FROM campaigns WHERE id=?", (campaign_id,)).fetchone()
    if not row:
        raise ValueError(f"Campaign {campaign_id} does not exist.")
    return dict(row)


def list_campaigns(include_archived: bool = False, db_path: Path = DB) -> list[dict]:
    where = "deleted=0" if include_archived else "deleted=0 AND archived=0"
    with connect(db_path) as db:
        return [dict(row) for row in db.execute(
            f"SELECT * FROM campaigns WHERE {where} ORDER BY archived,status IN ('completed'),id"
        )]


def update_status(campaign_id: int, status: str, error: str | None = None,
                  db_path: Path = DB) -> dict:
    if status not in VISIBLE_STATUSES:
        raise ValueError(f"Unsupported campaign status: {status}")
    now = utc_now()
    with connect(db_path) as db:
        existing = db.execute("SELECT * FROM campaigns WHERE id=?", (campaign_id,)).fetchone()
        if not existing:
            raise ValueError(f"Campaign {campaign_id} does not exist.")
        started = existing["started_utc"] or (now if status in ACTIVE_STATUSES else None)
        finished = now if status == "completed" else existing["finished_utc"]
        db.execute("""
            UPDATE campaigns SET status=?,updated_utc=?,started_utc=?,finished_utc=?,last_error=?
            WHERE id=?
        """, (status, now, started, finished, error, campaign_id))
        log(db, campaign_id, "status_changed", status=status, error=error)
    return get_campaign(campaign_id, db_path)


def rename_campaign(campaign_id: int, name: str, db_path: Path = DB) -> dict:
    name = " ".join(name.split())
    if not name or len(name) > 100:
        raise ValueError("Campaign name must contain 1 to 100 characters.")
    with connect(db_path) as db:
        try:
            changed = db.execute(
                "UPDATE campaigns SET name=?,updated_utc=? WHERE id=? AND deleted=0",
                (name, utc_now(), campaign_id),
            )
        except sqlite3.IntegrityError as error:
            raise ValueError(f"A campaign named {name!r} already exists.") from error
        if not changed.rowcount:
            raise ValueError(f"Campaign {campaign_id} does not exist.")
        log(db, campaign_id, "campaign_renamed", name=name)
    return get_campaign(campaign_id, db_path)


def edit_campaign(campaign_id: int, name: str, state: str, niche: str,
                  db_path: Path = DB) -> dict:
    campaign = get_campaign(campaign_id, db_path)
    if campaign["started_utc"]:
        if state.strip().upper() != campaign["state"] or " ".join(niche.lower().split()) != campaign["niche"]:
            raise ValueError("A started campaign can be renamed, but its state and niche are fixed.")
        return rename_campaign(campaign_id, name, db_path)
    settings = campaign_settings(state, niche)
    settings["geography_filter"] = "state"
    rename_campaign(campaign_id, name, db_path)
    atomic_json(Path(campaign["config_path"]), settings)
    with connect(db_path) as db:
        db.execute("""
            UPDATE campaigns SET state=?,niche=?,alternate=?,updated_utc=? WHERE id=?
        """, (settings["states"][0], settings["niche"], settings.get("alternate"),
              utc_now(), campaign_id))
        log(db, campaign_id, "campaign_edited", state=settings["states"][0], niche=settings["niche"])
    return get_campaign(campaign_id, db_path)


def archive_campaign(campaign_id: int, db_path: Path = DB) -> dict:
    campaign = get_campaign(campaign_id, db_path)
    if campaign["status"] in ACTIVE_STATUSES:
        raise ValueError("Pause the campaign before archiving it.")
    with connect(db_path) as db:
        db.execute("UPDATE campaigns SET archived=1,updated_utc=? WHERE id=?", (utc_now(), campaign_id))
        log(db, campaign_id, "campaign_archived")
    return get_campaign(campaign_id, db_path)


def soft_delete_campaign(campaign_id: int, db_path: Path = DB) -> None:
    campaign = get_campaign(campaign_id, db_path)
    if campaign["status"] in ACTIVE_STATUSES:
        raise ValueError("Pause the campaign before deleting it.")
    # Preserve configs, logs and discoveries. "Delete" hides only the app record.
    with connect(db_path) as db:
        db.execute("UPDATE campaigns SET deleted=1,archived=1,updated_utc=? WHERE id=?",
                   (utc_now(), campaign_id))
        log(db, campaign_id, "campaign_soft_deleted")


def active_campaign(db_path: Path = DB) -> dict | None:
    placeholders = ",".join("?" for _ in ACTIVE_STATUSES)
    with connect(db_path) as db:
        rows = db.execute(
            f"SELECT * FROM campaigns WHERE deleted=0 AND status IN ({placeholders}) ORDER BY id",
            tuple(sorted(ACTIVE_STATUSES)),
        ).fetchall()
    if len(rows) > 1:
        raise RuntimeError("More than one campaign is marked active.")
    return dict(rows[0]) if rows else None


def reconcile_workers(db_path: Path = DB) -> None:
    """Mark a campaign recoverable when its recorded worker no longer exists."""
    for campaign in list_campaigns(db_path=db_path):
        if campaign["status"] not in ACTIVE_STATUSES:
            continue
        session = f"gms-campaign-{campaign['id']}"
        result = subprocess.run(["tmux", "has-session", "-t", session], capture_output=True)
        if result.returncode:
            update_status(campaign["id"], "attention",
                          "Worker is not running; resume from the saved checkpoint.", db_path)


def launch_campaign(campaign_id: int, db_path: Path = DB, root: Path = ROOT) -> dict:
    campaign = get_campaign(campaign_id, db_path)
    active = active_campaign(db_path)
    if active and active["id"] != campaign_id:
        raise ValueError(f"{active['name']} is already running. Pause it first.")
    if campaign["archived"] or campaign["deleted"]:
        raise ValueError("Restore this campaign before starting it.")
    session = f"gms-campaign-{campaign_id}"
    if subprocess.run(["tmux", "has-session", "-t", session], capture_output=True).returncode == 0:
        raise ValueError("This campaign worker is already running.")
    update_status(campaign_id, "starting", db_path=db_path)
    display = os.environ.get("DISPLAY", ":100")
    xauthority = os.environ.get("XAUTHORITY", str(Path.home() / ".Xauthority"))
    worker_log = Path(campaign["config_path"]).with_name("worker.log")
    command = (
        f"cd {shlex.quote(str(root))} && "
        f"export DISPLAY={shlex.quote(display)} XAUTHORITY={shlex.quote(xauthority)} && "
        f"exec {shlex.quote(sys.executable)} campaign_worker.py --campaign-id {campaign_id} "
        f">>{shlex.quote(str(worker_log))} 2>&1"
    )
    result = subprocess.run(["tmux", "new-session", "-d", "-s", session, command], capture_output=True, text=True)
    if result.returncode:
        update_status(campaign_id, "attention", result.stderr.strip() or "Could not start worker", db_path)
        raise RuntimeError(result.stderr.strip() or "Could not start campaign worker.")
    return get_campaign(campaign_id, db_path)


def bootstrap_existing(db_path: Path = DB) -> None:
    existing = [
        ("Florida Auto Detailing", "FL", ROOT / "State Campaigns" / "Florida" / "campaign.json"),
        ("Georgia Auto Detailing", "GA", ROOT / "State Campaigns" / "Georgia" / "campaign.json"),
    ]
    queue_db = ROOT / "runs" / "local-queue" / "queue.sqlite3"
    ready = registry_states()
    states = {}
    if queue_db.is_file():
        source = sqlite3.connect(queue_db)
        try:
            for state, stage, count in source.execute(
                    "SELECT state_code,stage,count(*) FROM jobs GROUP BY state_code,stage"):
                states.setdefault(state, {})[stage] = count
        finally:
            source.close()
    def legacy_status(state: str) -> str:
        counts = states.get(state, {})
        if counts.get("blocked", 0):
            return "attention"
        if counts.get("done", 0) >= ready[state]["baseline_scopes"]:
            return "completed"
        return "paused"

    with connect(db_path) as db:
        known = {row["name"].casefold(): dict(row) for row in db.execute("SELECT * FROM campaigns")}
    for name, state, path in existing:
        status = legacy_status(state)
        saved = known.get(name.casefold())
        if saved:
            if (saved["legacy"] and saved["status"] not in ACTIVE_STATUSES
                    and saved["status"] != status):
                update_status(saved["id"], status, db_path=db_path)
            continue
        if not path.is_file():
            continue
        create_campaign(name, state, "auto detailing", db_path=db_path,
                        config_path=path, legacy=True, status=status)


def niche_names() -> list[str]:
    return list(NICHE_PRESETS)

