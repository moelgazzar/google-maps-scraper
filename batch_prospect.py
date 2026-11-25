"""Run a finite, local prospecting queue or a matched one-tab vs two-tab pilot."""
import argparse
import asyncio
import csv
import fcntl
import json
import re
import sqlite3
import time
from collections import Counter
from contextlib import closing
from pathlib import Path
from urllib.parse import unquote, urlsplit

from playwright.async_api import async_playwright

from chrome_capture import capture_window, inspect_profile
from geography.launcher import postal_place
from nord_proxy import (NordProxyError, launch_nord_browser, ordered_hosts, proxy_hosts,
                        route_state, save_route_state, service_credentials)
from prospect import KNOWN_PROSPECTS, ROOT, RUNS, place_id, qualify, run_folder

RUNTIME = ROOT / ".runtime"
FIELDS = ("city", "state", "time_zone")
STATE_NAMES = {"OH": "Ohio", "IL": "Illinois", "PA": "Pennsylvania", "WI": "Wisconsin",
               "FL": "Florida", "GA": "Georgia", "SC": "South Carolina"}
GENERIC_NAME = {"mobile", "auto", "car", "detail", "detailing", "llc", "service", "services", "the"}


def read_markets():
    with (ROOT / "geographies.csv").open(encoding="utf-8-sig", newline="") as file:
        reader = csv.DictReader(file)
        if not set(FIELDS) <= set(reader.fieldnames or []):
            raise ValueError("The city queue needs city, state and time_zone columns.")
        markets = []
        for row in reader:
            item = {key: row[key].strip() for key in FIELDS}
            if not item["city"] or not re.fullmatch(r"[A-Z]{2}", item["state"]):
                raise ValueError("Use a city and a two-letter state code for every queue row.")
            name = f"{item['city']}, {item['state']}"
            markets.append({**item, "market": name, "query": f"auto detailing {item['city']} {item['state']}"})
    if len({item["market"] for item in markets}) != len(markets):
        raise ValueError("The city queue has duplicate city and state pairs.")
    return markets


def dump(path, data):
    temp = path.with_suffix(".tmp")
    temp.write_text(json.dumps(data, indent=2, ensure_ascii=False) + "\n")
    temp.replace(path)


def identity_key(row):
    name = re.sub(r"[^a-z0-9]+", " ", row.get("name", "").casefold()).strip()
    market = re.sub(r"[^a-z0-9]+", " ", row.get("market", "").casefold()).strip()
    return (name, market) if name and market else None


def imported_rows():
    path = KNOWN_PROSPECTS
    if not path.exists():
        return []
    with path.open(encoding="utf-8-sig", newline="") as file:
        reader = csv.DictReader(file)
        headers = set(reader.fieldnames or [])
        if {"Business", "Market", "State", "Public phone", "Maps listing"} <= headers:
            return [{"maps_url": row["Maps listing"], "phone": row["Public phone"],
                     "name": row["Business"], "market": f"{row['Market']}, {row['State']}"}
                    for row in reader]
        if {"name", "market", "phone", "maps_url"} <= headers:
            return [{key: row[key] for key in ("name", "market", "phone", "maps_url")} for row in reader]
    raise ValueError("known-prospects.csv must be a prior shortlist export or this tool's qualified.csv.")


def history(exclude=None, include_queue=True):
    snapshot = RUNTIME / "prior-shortlist.json"
    seeded = json.loads(snapshot.read_text())["listings"] if snapshot.exists() else []
    existing = [
        {**row, "listing_id": place_id(row["maps_url"]) if row["maps_url"] else ""}
        for row in [*seeded, *imported_rows()]
    ]
    done = set()
    for path in sorted(RUNS.glob("*/batch.json")):
        if path.parent == exclude:
            continue
        report = json.loads(path.read_text())
        done.update(report["completed_markets"])
        existing.extend(report["qualified"])
    queue = RUNS / "local-queue" / "queue.sqlite3"
    if include_queue and queue.is_file():
        with closing(sqlite3.connect(queue)) as connection:
            existing.extend(json.loads(row[0]) for row in connection.execute("SELECT payload FROM qualified"))
    return existing, done


def make_plan(markets, benchmark, tabs):
    if not benchmark:
        return [(markets, tabs)]
    if len(markets) != 4:
        raise ValueError("The matched benchmark requires exactly four queued cities.")
    first, second = markets[:2], markets[2:]
    return [(first, 1), (first, 2), (second, 2), (second, 1)]


def pool(markets, windows):
    pooled = {}
    observed = {window["tabs"]: set() for window in windows}
    for market in markets:
        for window in windows:
            for sample in window["searches"]:
                if sample["query"] != market["query"]:
                    continue
                for row in sample["rows"]:
                    key = place_id(row["maps_url"])
                    observed[window["tabs"]].add(key)
                    if key not in pooled or row["website_button"] == "yes":
                        pooled[key] = {**row, **market}
    return list(pooled.values()), observed


def locality_matches(text, row):
    if row.get("scope_kind") == "zip_area":
        location = postal_place(text)
        return bool(location and location[1:] == (row["state"], row["zip"]))
    state = [row["state"], STATE_NAMES.get(row["state"], row["state"])]
    region = "|".join(re.escape(value) for value in dict.fromkeys(state))
    return bool(re.search(rf"\b{re.escape(row['city'])}\s*,?\s+(?:{region})\b", text, re.I))


def branded_booking(link, name):
    tokens = [part for part in re.findall(r"[a-z0-9]+", name.lower())
              if part not in GENERIC_NAME and len(part) >= 3]
    host = urlsplit(link).netloc
    if not tokens or not host or re.search(r"(^|\.)google\.com$", host, re.I):
        return False
    normalized = re.sub(r"[^a-z0-9]", "", unquote(link).lower())
    return all(token in normalized for token in tokens)


def profile_decision(row, evidence):
    if evidence.get("error"):
        return "profile_unavailable"
    if evidence["website"]:
        return "profile_has_website"
    if any(branded_booking(link, row["name"]) for link in evidence["booking_links"]):
        return "branded_booking_link"
    if not evidence["add_website"]:
        return "website_status_unclear"
    if evidence["booking_links"]:
        return "ambiguous_profile_hold"
    if row.get("geography_filter") == "state":
        location = postal_place(evidence["address"])
        if not location:
            return "locality_unconfirmed"
        return "qualified" if location[1] == row["state"] else "outside_target_state"
    if any(locality_matches(evidence[field], row) for field in ("address", "service_area")):
        return "qualified"
    if evidence["service_area"]:
        return "ambiguous_profile_hold"
    return "locality_unconfirmed"


async def browse(output, markets, benchmark, tabs, limit, existing, saved_windows=None):
    windows = saved_windows or []
    async with async_playwright() as playwright:
        username, password = service_credentials()
        proxy_state_path = RUNTIME / "nord-proxy-state.json"
        saved_proxy = route_state(proxy_state_path)
        browser = bridge = None
        errors = []
        for host in ordered_hosts(proxy_hosts(), saved_proxy["host"]):
            try:
                browser, bridge, public_ip = await launch_nord_browser(
                    playwright, host, username, password, saved_proxy["public_ip"]
                )
                save_route_state(proxy_state_path, host, public_ip)
                print(f"NordVPN browser route: {host} | public IP {public_ip}", flush=True)
                break
            except NordProxyError as error:
                errors.append(str(error))
        if not browser:
            raise NordProxyError("No fresh NordVPN browser route was available: " + " | ".join(errors))
        try:
            plan = [] if saved_windows else make_plan(markets, benchmark, tabs)
            for pair, count in plan:
                print(f"{count} tab(s) | {', '.join(item['market'] for item in pair)}", flush=True)
                window = await capture_window(browser, [item["query"] for item in pair], count, limit)
                windows.append(window)
                dump(output / "capture.json", windows)
            rows, observed = pool(markets, windows)
            decisions = qualify(rows, existing)
            old_names = {identity_key(row) for row in existing} - {None}
            for row in decisions:
                if row["decision"] == "candidate" and identity_key(row) in old_names:
                    row["decision"] = "duplicate_name_and_city"
            candidates = [row for row in decisions if row["decision"] == "candidate"]
            print(f"Checking {len(candidates)} promising Maps profiles.", flush=True)
            started = time.monotonic()
            context = await browser.new_context(locale="en-US", viewport={"width": 1280, "height": 800})
            try:
                page = await context.new_page()
                for row in candidates:
                    evidence = await inspect_profile(page, row)
                    row["profile"] = evidence
                    row["decision"] = profile_decision(row, evidence)
                    row["locality_status"] = "Verified in Maps profile" if row["decision"] == "qualified" else "See decision"
                    print(f"  {row['market']} | {row['decision']}", flush=True)
            finally:
                await context.close()
        finally:
            await browser.close()
            await bridge.close()
    return windows, decisions, observed, round(time.monotonic() - started, 2)


def comparison(windows, qualified, observed):
    results = []
    for tabs in sorted(observed):
        rounds = [window for window in windows if window["tabs"] == tabs]
        seconds = round(sum(window["wall_seconds"] for window in rounds), 2)
        samples = [item for window in rounds for item in window["searches"]]
        passes = sum(row["listing_id"] in observed[tabs] for row in qualified)
        results.append({
            "tabs": tabs, "seconds": seconds,
            "cards": sum(len(item["rows"]) for item in samples),
            "qualified_seen": passes,
            "empty_searches": sum(not item["rows"] for item in samples),
            "unfinished_searches": sum(item["stop"] not in {"requested_limit", "maps_end_of_list"} for item in samples),
            "qualified_per_capture_hour": round(passes * 3600 / seconds, 1),
        })
    return results


def choose_tabs(modes):
    if len(modes) != 2:
        return modes[0]["tabs"]
    single, double = modes
    if (not double["unfinished_searches"] and double["cards"] >= single["cards"]
            and double["qualified_seen"] >= single["qualified_seen"]
            and double["qualified_per_capture_hour"] >= single["qualified_per_capture_hour"] * 1.1
            and double["qualified_seen"]):
        return 2
    return 1


def unique_qualified(decisions):
    seen = set()
    result = []
    for row in decisions:
        if row["decision"] != "qualified":
            continue
        key = identity_key(row)
        if key in seen:
            row["decision"] = "duplicate_name_and_city"
        else:
            seen.add(key)
            result.append(row)
    return result


def write_report(output, result):
    lines = [
        "# Prospecting pilot", "",
        f"**{len(result['qualified'])} new prospects** passed the Maps sourcing checks.",
        "Each number remains unverified. The storefront or service evidence had to match the anchor city.",
        "", "| Market | Listings screened | New passed |",
        "|---|---:|---:|",
    ]
    lines.extend(f"| {r['market']} | {r['screened']} | {r['new_passed']} |" for r in result["markets"])
    lines.extend(["", "| Chrome tabs | Cards captured | Capture time | Qualified found | Unfinished searches |",
                  "|---|---:|---:|---:|---:|"])
    lines.extend(f"| {r['tabs']} | {r['cards']} | {r['seconds']:.2f}s | {r['qualified_seen']} | {r['unfinished_searches']} |"
                 for r in result["comparison"])
    timing_label = "The profile replay and report took" if result["reused_capture"] else "The full run took"
    lines.extend(["", "The comparison times cover search and card capture. Profile checks were shared across both modes.",
                  f"Profile checks took {result['profile_seconds']:.2f}s. {timing_label} {result['run_seconds']:.2f}s.",
                  f"Ambiguous profiles held for review: {result['profile_holds']}.",
                  f"Default batch setting is {result['recommended_tabs']} Chrome tab(s). Phone checks and outreach were zero.", ""])
    if result["prior_profile_timeouts"]:
        lines.insert(-1, f"Reused the stored searches after {result['prior_profile_timeouts']} initial profile timeouts.")
    (output / "report.md").write_text("\n".join(lines))
    with (output / "qualified.csv").open("w", encoding="utf-8", newline="") as file:
        fields = ["name", "market", "rating", "reviews", "phone", "phone_verification", "maps_url", "locality_status"]
        writer = csv.DictWriter(file, fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(result["qualified"])
    dump(output / "decisions.json", result.pop("decisions"))
    dump(output / "batch.json", result)


def run(args):
    output = args.resume.resolve() if args.resume else None
    existing, done = history(exclude=output)
    markets = read_markets()
    windows = None
    prior_profile_timeouts = 0
    if output:
        if output.parent != RUNS.resolve() or not (output / "capture.json").is_file():
            raise ValueError("Resume requires a saved run folder under the local runs directory.")
        windows = json.loads((output / "capture.json").read_text())
        captured_queries = {item["query"] for window in windows for item in window["searches"]}
        markets = [item for item in markets if item["query"] in captured_queries]
        if len(markets) != len(captured_queries):
            raise ValueError("Keep the saved search cities in geographies.csv while resuming a run.")
        if len({window["tabs"] for window in windows}) > 1:
            args.benchmark = True
        prior = output / "decisions.json"
        if prior.exists():
            prior_profile_timeouts = sum(row["decision"] == "profile_unavailable"
                                         for row in json.loads(prior.read_text()))
        previous_report = output / "batch.json"
        if previous_report.exists():
            prior_profile_timeouts = max(prior_profile_timeouts, json.loads(
                previous_report.read_text()).get("prior_profile_timeouts", 0))
    elif not args.benchmark:
        markets = [market for market in markets if market["market"] not in done][:4]
    make_plan(markets, args.benchmark, 1)
    if not markets:
        last = sorted(RUNS.glob("*/batch.json"))
        print("The saved city queue is complete.")
        if last:
            print("Last local report:", last[-1].with_name("report.md"))
        return
    settings = RUNTIME / "batch-settings.json"
    tabs = json.loads(settings.read_text())["tabs"] if settings.exists() else 1
    output = output or run_folder()
    print("Results will be saved to", output, flush=True)
    started = time.monotonic()
    windows, decisions, observed, profile_seconds = asyncio.run(
        browse(output, markets, args.benchmark, tabs, args.limit, existing, windows))
    qualified = unique_qualified(decisions)
    modes = comparison(windows, qualified, observed)
    recommended = choose_tabs(modes)
    completed = {item["query"] for window in windows for item in window["searches"]
                 if item["stop"] in {"requested_limit", "maps_end_of_list"} and item["rows"]}
    incomplete_profiles = {row["query"] for row in decisions if row["decision"] == "profile_unavailable"}
    screened, passes = Counter(row["market"] for row in decisions), Counter(row["market"] for row in qualified)
    result = {
        "benchmark": args.benchmark, "limit_per_query": args.limit,
        "reused_capture": bool(windows and args.resume), "prior_profile_timeouts": prior_profile_timeouts,
        "markets": [{"market": item["market"], "screened": screened[item["market"]],
                     "new_passed": passes[item["market"]]} for item in markets],
        "completed_markets": [item["market"] for item in markets
                              if item["query"] in completed and item["query"] not in incomplete_profiles],
        "comparison": modes, "qualified": qualified, "decisions": decisions,
        "profile_holds": sum(row["decision"] == "ambiguous_profile_hold" for row in decisions),
        "recommended_tabs": recommended, "profile_seconds": profile_seconds,
        "run_seconds": round(time.monotonic() - started, 2), "phone_checks": 0,
    }
    write_report(output, result)
    if args.benchmark:
        dump(settings, {"tabs": recommended})
    print(f"\n{len(qualified)} new prospects passed. Report: {output / 'report.md'}", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--benchmark", action="store_true", help="Run the four-city crossover once.")
    parser.add_argument("--resume", type=Path, help="Recheck profiles in an existing benchmark report.")
    parser.add_argument("--limit", type=int, choices=range(1, 31), default=30, metavar="1–30")
    args = parser.parse_args()
    try:
        with (RUNTIME / "prospecting.lock").open("w") as lock:
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                raise RuntimeError("A prospecting run is already in progress.") from None
            run(args)
    except (OSError, RuntimeError, ValueError) as error:
        parser.exit(1, f"Error: {error}\n")


if __name__ == "__main__":
    main()
