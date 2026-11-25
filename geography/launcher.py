"""Adapt verified geography previews for the local prospecting ledger."""
import hashlib
import json
import re
from collections import defaultdict, deque
from itertools import islice

from .catalog import HERE, STATES, make_plan, read_rows, relationship, state_catalog

DEFAULT_NICHE = "auto detailing"
NICHE_PRESETS = {
    "auto detailing": (["auto detailing", "car detailing", "mobile detailing", "detailer", "detailers"], "mobile detailing"),
    "landscaping": (["landscaping", "landscaper", "landscape contractor", "landscape designer"], "landscape contractor"),
    "pressure washing": (["pressure washing", "power washing", "soft washing"], "power washing"),
    "roofing": (["roofing", "roofer", "roof repair"], "roof repair"),
    "moving companies": (["moving company", "moving service", "movers", "moving and storage"], "local movers"),
}


def campaign_settings(state, niche, alternate=None, terms=()):
    state = state.strip().upper()
    niche = " ".join(niche.lower().split())
    if state not in STATES or not niche:
        raise ValueError("Supply a US state abbreviation and a niche.")
    defaults, second_phrase = NICHE_PRESETS.get(niche, ([niche], None))
    matches = list(dict.fromkeys(" ".join(term.lower().split()) for term in (terms or defaults) if term.strip()))
    alternate = second_phrase if alternate is None else " ".join(alternate.lower().split()) or None
    if not matches or alternate == niche:
        raise ValueError("Supply a match phrase and choose a distinct alternate search phrase.")
    return {"states": [state], "niche": niche, "alternate": alternate, "match_terms": matches}


def prepare_campaign(state, niche, alternate, terms):
    config = campaign_settings(state, niche, alternate, terms)
    state = config["states"][0]
    folder = state_catalog(state)
    inventory = json.loads((folder / "inventory.json").read_text())["states"][state]
    return {"campaign": config, "inventory": inventory,
            "sample_searches": make_plan(state, config["niche"], 4, [], config["alternate"])}

# Maps prints these fields at the end of a public US address.
POSTAL_ADDRESS = re.compile(
    r",\s*([^,]+),\s*([A-Z]{2})\s+(\d{5})(?:-\d{4})?(?:,\s*(?:United States|USA))?\s*$",
    re.I,
)


def postal_place(address):
    match = POSTAL_ADDRESS.search(address or "")
    if match:
        city, state, zip_code = match.groups()
        return city.strip(), state.upper(), zip_code
    return None


def adapt_scope(scope, position, places, county_zips, niche=DEFAULT_NICHE, match_terms=None):
    sid, state = scope["scope_id"], scope["state"]
    key_basis = scope["query"].casefold()
    if niche != DEFAULT_NICHE:
        key_basis = f"{niche}\0{key_basis}"
    job = {
        "key": hashlib.sha256(key_basis.encode()).hexdigest()[:24],
        "position": position, "state": state, "query": scope["query"],
        "scope_id": sid, "scope_reason": scope["reason"], "niche": niche,
    }
    if match_terms is not None:
        job["match_terms"] = match_terms
    if sid.startswith("place:"):
        place = places[sid]
        county = place["representative_county_geoid"]
        county_label = place["place"] != place["search_label"]
        job.update(city=place["place"], market=f"{place['place']}, {state}", zip="",
                   scope_kind="place")
        if county_label:
            job["county_geo"] = county
            job["allowed_county_zips"] = sorted(county_zips[county])
        return job

    zip_code = sid.split(":")[2]
    return {**job, "city": "", "market": f"ZIP {zip_code}, {state}", "zip": zip_code,
            "scope_kind": "zip_area"}


def worker_area(job):
    return job.get("region") or job["state"]


def rotate_areas(jobs):
    groups = defaultdict(deque)
    for job in jobs:
        groups[worker_area(job)].append(job)
    active = deque(groups.values())
    while active:
        group = active.popleft()
        yield group.popleft()
        if group:
            active.append(group)


def florida_areas(places, zips):
    regions = json.loads((HERE / "florida-regions.json").read_text())
    florida_counties = {row["county"].removesuffix(" County"): row["county_geoid"]
                        for row in read_rows("counties.csv") if row["state"] == "FL"}
    pairs = [(florida_counties[county], region) for region, rule in regions.items() for county in rule["counties"]]
    by_county = dict(pairs)
    if len(pairs) != len(by_county) or set(by_county) != set(florida_counties.values()):
        raise ValueError("Every Florida county must have exactly one region.")

    # These coastal Gazetteer points fall outside the simplified county polygons.
    coastal_counties = {"1202400": "Duval", "1218575": "Pinellas", "1248100": "Duval", "1278925": "Levy"}
    areas = {}
    anchors = set()
    for row in places.values():
        if row["state"] != "FL":
            continue
        county = row["representative_county_geoid"] or florida_counties[coastal_counties[row["place_geoid"]]]
        region = by_county[county]
        areas[row["place_scope"]] = region
        if row["search_label"] == regions[region]["anchor"]:
            anchors.add(row["place_scope"])
    if len(anchors) != len(regions):
        raise ValueError("Each Florida region needs one distinct matching anchor.")

    land_by_zip = defaultdict(lambda: defaultdict(int))
    for state, county, zcta, land in relationship("zcta-county.txt", "COUNTY"):
        if state == "FL":
            land_by_zip[zcta][by_county[county]] += land
    for row in zips:
        if row["state"] == "FL":
            land = land_by_zip[row["zip_area"]]
            areas[row["zip_scope"]] = min(land, key=lambda region: (-land[region], region))
    return areas, anchors


def catalog_jobs(ledger, manual, limit, retry=False, florida_regions=False, campaign=None):
    active = campaign or HERE / "active.json"
    if not active.is_file():
        return manual
    config = json.loads(active.read_text())
    states = config["states"]
    niche = config["niche"]
    geography_filter = config.get("geography_filter", "search")
    if geography_filter not in {"search", "state"}:
        raise ValueError("Choose a search-area or state address filter.")
    if not states or len(set(states)) != len(states) or any(state not in STATES for state in states):
        raise ValueError("Choose distinct US states for the campaign.")
    match_terms = config.get("match_terms")
    if match_terms is None and niche != DEFAULT_NICHE:
        match_terms = campaign_settings(states[0], niche)["match_terms"]
    places = {row["place_scope"]: row for state in states for row in read_rows("places.csv", state)
              if row["state"] == state}
    zips = [row for state in states for row in read_rows("zip-areas.csv", state) if row["state"] == state]
    if florida_regions and states != ["FL"]:
        raise ValueError("Florida region mode requires Florida as the only active catalog state.")
    areas, anchors = florida_areas(places, zips) if florida_regions else ({}, set())
    county_zips = defaultdict(set)
    for row in zips:
        counties = row["county_geoids"].split(" | ")
        if len(counties) == 1:
            county_zips[counties[0]].add(row["zip_area"])

    known = defaultdict(list)
    for row in ledger.db.execute("SELECT spec,stage,capture,decisions FROM jobs ORDER BY sort_order"):
        spec = json.loads(row["spec"])
        if spec.get("scope_id") and spec["state"] in states and spec.get("niche", DEFAULT_NICHE) == niche:
            known[spec["state"]].append((spec, row))

    # Manual scope rows remain visible. A catalog scope with the exact same query
    # can reuse its durable checkpoint and gain a Census identifier.
    jobs = {row["key"]: {**row, "geography_filter": geography_filter}
            for row in manual if niche == DEFAULT_NICHE and row["state"] in states}
    position = max((row["position"] for row in manual), default=1) + 1
    for state in states:
        outcomes = []
        unfinished = []
        for spec, row in known[state]:
            job = {**spec, "position": position}
            job["geography_filter"] = geography_filter
            job.pop("region", None)
            if florida_regions:
                job["region"] = areas[spec["scope_id"].split(":phrase:")[0]]
            position += 1
            jobs[job["key"]] = job
            captured = json.loads(row["capture"]) if row["capture"] else {}
            decisions = json.loads(row["decisions"]) if row["decisions"] else []
            if row["stage"] == "done":
                outcome = captured.get("stop", "maps_end_of_list")
            else:
                # Existing unfinished jobs take their saved place in the queue.
                # The planner skips them here so it doesn't issue them twice.
                outcome = "blocked"
                if row["stage"] != "blocked" or retry:
                    unfinished.append(job)
            outcomes.append({"scope_id": spec["scope_id"], "stop": outcome,
                             "new_qualified": sum(item["decision"] == "qualified" for item in decisions)})
        count = max(0, limit - len(unfinished))
        if not count and not florida_regions:
            continue
        plan_size = 2 * len(areas) if florida_regions else count
        plan = make_plan(state, niche, plan_size, outcomes, config.get("alternate"))
        if florida_regions:
            plan.sort(key=lambda scope: scope["scope_id"] not in anchors)
        fresh = [adapt_scope(scope, position + index, places, county_zips, niche, match_terms)
                 for index, scope in enumerate(plan)]
        if florida_regions:
            for job in fresh:
                job["region"] = areas[job["scope_id"].split(":phrase:")[0]]
            pending_keys = {job["key"] for job in unfinished}
            selected = (job for job in islice(rotate_areas([*unfinished, *fresh]), limit)
                        if job["key"] not in pending_keys)
        else:
            selected = fresh
        for job in selected:
            job["position"] = position
            job["geography_filter"] = geography_filter
            jobs[job["key"]] = job
            position += 1
    return list(jobs.values())
