#!/usr/bin/env python3
"""One-query Google Maps capture and local CSV qualification."""
import argparse
import csv
import hashlib
import json
import re
import shutil
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent
INBOX = ROOT / "inbox"
KNOWN_PROSPECTS = INBOX / "known-prospects.csv"
RUNS = ROOT / "runs"
FIELDS = ("name", "maps_url", "category", "rating", "reviews", "address", "phone", "website_url", "website_button")
IDS_CORE = {"hfpxzc href", "xxVWCe", "MW4etd", "UY7F9", "lcr4fd href"}
PHONE = re.compile(r"\+?1?[\s(]*([2-9]\d\d)[)\s.-]*([2-9]\d\d)[\s.-]*(\d{4})")
PLACE = re.compile(r"!19s([^!&?]+)|!1s(0x[\da-f]+:0x[\da-f]+)", re.I)


def number(value):
    text = str(value or "").strip(" ()").replace(",", "")
    return float(text) if re.fullmatch(r"\d+(?:\.\d+)?", text) else None


MIN_RATING = 4.0
MIN_REVIEWS = 10


def meets_reputation_floor(row):
    """Use one inclusive qualification rule across new and historical evidence."""
    rating, reviews = number(row.get("rating")), number(row.get("reviews"))
    return (rating is not None and MIN_RATING <= rating <= 5
            and reviews is not None and reviews.is_integer() and reviews >= MIN_REVIEWS)


def phone_key(text):
    match = PHONE.search(text or "")
    return "".join(match.groups()) if match else ""


def place_id(url):
    match = PLACE.search(url)
    return next(value for value in match.groups() if value) if match else url.split("?")[0]


def read_csv(path):
    with path.open(encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        headers = set(reader.fieldnames or [])
        if IDS_CORE <= headers:
            for row in reader:
                web_url = row["lcr4fd href"] or ""
                yield {
                    "maps_url": row["hfpxzc href"] or "", "name": row["xxVWCe"] or "",
                    "rating": row["MW4etd"], "reviews": row["UY7F9"],
                    "category": row.get("W4Efsd", ""), "website_url": web_url,
                    "website_button": "yes" if web_url or "Website" in (
                        row.get("R8c4Qb"), row.get("R8c4Qb 2")) else "no",
                    "address": next((row.get(key) for key in ("W4Efsd 6", "W4Efsd 3")
                                     if re.match(r"\d", row.get(key) or "")), ""),
                    "phone": next((value for value in row.values() if isinstance(value, str)
                                   and PHONE.fullmatch(value.strip())), ""),
                }
        elif set(FIELDS) <= headers:
            yield from reader
        else:
            raise ValueError("The CSV lacks Maps listing and Website columns. In IDS, use Try another table and export the Maps results.")


def qualify(rows, existing=()):
    seen_ids = {row["listing_id"] for row in existing if row.get("listing_id")}
    seen_phones = {phone_key(row.get("phone", "")) for row in existing} - {""}
    decisions = []
    for row in rows:
        record = dict(row)
        url = record["maps_url"]
        key = place_id(url) if re.match(r"https://(?:www\.)?google\.com/maps/place/", url) else ""
        rating, reviews = number(row.get("rating")), number(row.get("reviews"))
        phone = phone_key(row.get("phone", ""))
        service = f"{row.get('name', '')} {row.get('category', '')}"
        if "match_terms" in row:
            relevant = any(re.search(rf"\b{re.escape(term)}\b", service, re.I) for term in row["match_terms"])
        else:
            relevant = bool(re.search(r"\bdetail(?:ing|ers?)?\b", service, re.I))
        if not key or not row["name"]:
            decision = "non_listing"
        elif key in seen_ids:
            decision = "duplicate_id"
        elif row["website_button"] == "yes" or row["website_url"]:
            decision = "website_button_present"
        elif row["website_button"] != "no":
            decision = "website_signal_unknown"
        elif not meets_reputation_floor(record):
            decision = "below_threshold_or_missing"
        elif not relevant:
            decision = "niche_unclear"
        elif not phone:
            decision = "public_phone_missing"
        elif phone in seen_phones:
            decision = "duplicate_phone"
        else:
            decision = "candidate"
            seen_phones.add(phone)
        if key:
            seen_ids.add(key)
        record.update(
            listing_id=key, rating=rating, reviews=reviews, decision=decision,
            phone_verification="Not verified",
            locality_status="Profile check needed" if decision == "candidate" else "",
        )
        decisions.append(record)
    return decisions


def run_folder():
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S.%fZ")
    output = RUNS / stamp
    output.mkdir(parents=True)
    return output


def write_run(source, rows, query, elapsed=None, stop=None):
    output = run_folder()
    if source:
        shutil.copy2(source, output / "source.csv")
    else:
        with (output / "source.csv").open("w", encoding="utf-8", newline="") as file:
            writer = csv.DictWriter(file, FIELDS)
            writer.writeheader()
            writer.writerows(rows)
    decisions = qualify(rows)
    candidates = [record for record in decisions if record["decision"] == "candidate"]
    ids = {record["listing_id"] for record in decisions if record["listing_id"]}
    gaps = {record["listing_id"] for record in decisions if record["listing_id"]
            and record["website_button"] == "no" and not record["website_url"]}
    summary = {
        "run_id": output.name, "query": query, "source": "IDS CSV" if source else "Terminal Chrome",
        "capture_seconds": elapsed, "capture_stop": stop, "rows": len(rows), "distinct_listings": len(ids),
        "website_button_absent": len(gaps), "candidates_pending_profile_check": len(candidates),
        "phone_checks": 0, "calls_released": 0, "decisions": dict(Counter(r["decision"] for r in decisions)),
        "source_sha256": hashlib.sha256((output / "source.csv").read_bytes()).hexdigest(),
    }
    for name, value in (("summary.json", summary), ("decisions.json", decisions), ("candidates.json", candidates)):
        (output / name).write_text(json.dumps(value, indent=2, ensure_ascii=False) + "\n")
    print(json.dumps(summary, indent=2))
    print(output)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    ingest = commands.add_parser("ingest", help="Filter an Instant Data Scraper export")
    ingest.add_argument("csv", type=Path, nargs="?", help="Defaults to the latest CSV in inbox")
    ingest.add_argument("--query", required=True, help="Exact Maps query entered when exporting")
    capture = commands.add_parser("capture", help="Open Chrome and capture a single Maps search")
    capture.add_argument("query")
    capture.add_argument("--limit", type=int, default=30, choices=range(1, 121), metavar="1–120")
    args = parser.parse_args()
    try:
        if args.command == "ingest":
            exports = (path for path in INBOX.glob("*.csv") if path != KNOWN_PROSPECTS)
            source = args.csv or max(exports, key=lambda path: path.stat().st_mtime, default=None)
            if source is None:
                raise ValueError("Export an IDS CSV to inbox or pass a CSV path.")
            rows = list(read_csv(source))
            if not any(row["maps_url"] for row in rows):
                raise ValueError("The export contains no Maps listing URLs. Select the Maps results table.")
            write_run(source, rows, args.query)
        else:
            from chrome_capture import capture_query
            rows, elapsed, stop = capture_query(args.query, args.limit)
            write_run(None, rows, args.query, elapsed, stop)
    except (ImportError, OSError, RuntimeError, ValueError) as error:
        parser.exit(1, f"Error: {error}\n")


if __name__ == "__main__":
    main()
