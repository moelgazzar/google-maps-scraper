#!/usr/bin/env python3
"""Durable operator requests shared by the Sonor dashboard and runners."""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parent
CONTROL = ROOT / ".runtime" / "operator-control.json"
PAUSE_ACTIONS = {"pause_after_wave", "pause_after_batch", "safe_stop"}
VALID_ACTIONS = PAUSE_ACTIONS | {"stop_now"}


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def read_control(path: Path = CONTROL) -> dict:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return data if data.get("action") in VALID_ACTIONS else {}


def current_action(path: Path = CONTROL) -> str:
    return str(read_control(path).get("action", ""))


def request(action: str, requested_by: str = "terminal_dashboard",
            path: Path = CONTROL) -> dict:
    if action not in VALID_ACTIONS:
        raise ValueError(f"Unsupported operator action: {action}")
    record = {"action": action, "requested_utc": utc_now(),
              "requested_by": requested_by, "pid": os.getpid()}
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(record, indent=2) + "\n", encoding="utf-8")
    temporary.replace(path)
    return record


def clear(path: Path = CONTROL) -> None:
    try:
        path.unlink()
    except FileNotFoundError:
        pass


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("status")
    ask = sub.add_parser("request")
    ask.add_argument("action", choices=sorted(VALID_ACTIONS))
    sub.add_parser("clear")
    args = parser.parse_args()
    if args.command == "request":
        print(json.dumps(request(args.action), indent=2))
    elif args.command == "clear":
        clear()
        print("Operator request cleared.")
    else:
        print(json.dumps(read_control(), indent=2))
