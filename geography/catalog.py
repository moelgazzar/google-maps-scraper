"""Build the Census geography roster and preview a local search plan."""
import argparse
import csv
import hashlib
import io
import json
import re
import zipfile
from collections import Counter, defaultdict
from pathlib import Path
from tempfile import TemporaryDirectory
from xml.etree import ElementTree

HERE = Path(__file__).resolve().parent
SOURCES = HERE.parent / ".runtime" / "geography-sources"
# Preserve the order of the existing Southeast catalog.
SEED_STATES = ("FL", "GA", "SC")
STATES = dict(item.split(":") for item in (
    "FL:12 GA:13 SC:45 AL:01 AK:02 AZ:04 AR:05 CA:06 CO:08 CT:09 DE:10 DC:11 HI:15 ID:16 IL:17 IN:18 "
    "IA:19 KS:20 KY:21 LA:22 ME:23 MD:24 MA:25 MI:26 MN:27 MS:28 MO:29 MT:30 NE:31 NV:32 NH:33 "
    "NJ:34 NM:35 NY:36 NC:37 ND:38 OH:39 OK:40 OR:41 PA:42 RI:44 SD:46 TN:47 TX:48 UT:49 VT:50 "
    "VA:51 WA:53 WV:54 WI:55 WY:56"
).split())
BY_FIPS = {v: k for k, v in STATES.items()}
PRIORITY = {"FL": ("Brandon", "Riverview", "Winter Garden", "Apopka")}
GAZ = "https://www2.census.gov/geo/docs/maps-data/data/gazetteer/2026_Gazetteer/"
REL = "https://www2.census.gov/geo/docs/maps-data/data/rel2020/zcta520/"
ORIGINS = {
    "places.zip": GAZ + "2026_Gaz_place_national.zip",
    "counties.zip": GAZ + "2026_Gaz_counties_national.zip",
    "zctas.zip": GAZ + "2026_Gaz_zcta_national.zip",
    "zcta-place.txt": REL + "tab20_zcta520_place20_natl.txt",
    "zcta-county.txt": REL + "tab20_zcta520_county20_natl.txt",
    "county-shapes.zip": "https://www2.census.gov/geo/tiger/GENZ2025/kml/cb_2025_us_county_500k.zip",
}


def gazetteer(file):
    with zipfile.ZipFile(SOURCES / file) as archive:
        inside = [item for item in archive.namelist() if item.endswith(".txt")]
        if len(inside) != 1:
            raise ValueError(f"Unexpected layout in {file}.")
        with archive.open(inside[0]) as stream:
            yield from csv.DictReader(io.TextIOWrapper(stream, encoding="utf-8-sig"), delimiter="|")


def relationship(file, kind):
    with (SOURCES / file).open(encoding="utf-8-sig", newline="") as source:
        for row in csv.DictReader(source, delimiter="|"):
            place_id = row[f"GEOID_{kind}_20"]
            zcta = row["GEOID_ZCTA5_20"]
            land = int(row["AREALAND_PART"] or "0")
            state = BY_FIPS.get(place_id[:2])
            if state and zcta and land > 0:
                yield state, place_id, zcta, land


def position(state):
    return tuple(STATES).index(state)


def name_for_search(census_name):
    aliases = {"Athens-Clarke County": "Athens", "Augusta-Richmond County": "Augusta",
               "Cusseta-Chattahoochee County": "Cusseta", "Georgetown-Quitman County": "Georgetown"}
    base = re.sub(r" (?:CDP|city|town|village|borough|municipality|unified government|consolidated government)(?: \(balance\))?$",
                  "", census_name)
    return aliases.get(base, base)


def county_shapes(counties):
    ns = {"k": "http://www.opengis.net/kml/2.2"}
    shapes = defaultdict(list)
    with zipfile.ZipFile(SOURCES / "county-shapes.zip") as archive:
        inside = [name for name in archive.namelist() if name.lower().endswith(".kml")]
        if len(inside) != 1:
            raise ValueError("The Census boundary archive should contain one KML file.")
        with archive.open(inside[0]) as stream:
            for _, feature in ElementTree.iterparse(stream, events=("end",)):
                if feature.tag != f"{{{ns['k']}}}Placemark":
                    continue
                key = feature.findtext(".//k:SimpleData[@name='GEOID']", namespaces=ns)
                if key in counties:
                    polygons = []
                    for polygon in feature.findall(".//k:Polygon", ns):
                        outer = polygon.find("k:outerBoundaryIs/k:LinearRing/k:coordinates", ns)
                        inner = polygon.findall("k:innerBoundaryIs/k:LinearRing/k:coordinates", ns)
                        rings = [
                            [tuple(map(float, point.split(",")[:2])) for point in element.text.split()]
                            for element in [outer, *inner]
                        ]
                        x, y = zip(*rings[0])
                        polygons.append(((min(x), min(y), max(x), max(y)), rings))
                    if polygons:
                        shapes[BY_FIPS[key[:2]]].append((key, polygons))
                feature.clear()
    return shapes


def inside_ring(x, y, ring):
    inside = False
    previous_x, previous_y = ring[-1]
    for next_x, next_y in ring:
        if (previous_y > y) != (next_y > y):
            crossing = previous_x + (y - previous_y) * (next_x - previous_x) / (next_y - previous_y)
            if x < crossing:
                inside = not inside
        previous_x, previous_y = next_x, next_y
    return inside


def representative_county(place, shapes):
    x, y = float(place["INTPTLONG"]), float(place["INTPTLAT"])
    matches = []
    for key, polygons in shapes[place["USPS"]]:
        for (left, bottom, right, top), rings in polygons:
            if left <= x <= right and bottom <= y <= top and inside_ring(x, y, rings[0]) and not any(
                    inside_ring(x, y, ring) for ring in rings[1:]):
                matches.append(key)
                break
    return matches[0] if len(matches) == 1 else ""


def place_scope(state, geoid):
    return f"place:{state}:{geoid}"


def zip_scope(state, zcta):
    return f"zcta:{state}:{zcta}"


def write_csv(path, rows, fields):
    with path.open("w", newline="", encoding="utf-8") as output:
        writer = csv.DictWriter(output, fields)
        writer.writeheader()
        writer.writerows(rows)


def build(states=SEED_STATES, output=HERE):
    places = {r["GEOID"]: r for r in gazetteer("places.zip") if r["USPS"] in states}
    counties = {r["GEOID"]: r for r in gazetteer("counties.zip") if r["USPS"] in states}
    shapes = county_shapes(counties)
    zip_data = {r["GEOID"]: r for r in gazetteer("zctas.zip")}
    county_links = defaultdict(dict)
    for state, county, zcta, land in relationship("zcta-county.txt", "COUNTY"):
        if state in states and zcta in zip_data:
            county_links[state, zcta][county] = land

    # A ZIP-area can overlap several places and counties. Preserve each land intersection.
    linked = set()
    has_2020_place = set()
    legacy_only = Counter()
    unavailable_zip_links = Counter()
    links = []
    for state, place, zcta, land in relationship("zcta-place.txt", "PLACE"):
        if state not in states:
            continue
        if (state, zcta) not in county_links:
            unavailable_zip_links[state] += 1
            continue
        has_2020_place.add((state, zcta))
        if place not in places:
            legacy_only[state] += 1
            continue
        linked.add(place)
        total_land = sum(county_links[state, zcta].values())
        links.append({"state": state, "place_scope": place_scope(state, place),
                      "zip_scope": zip_scope(state, zcta), "place": name_for_search(places[place]["NAME"]),
                      "zip_area": zcta, "overlap_land_sq_m": land,
                      "share_of_2020_in_state_zip_land": round(land / total_land, 6)})

    labels = Counter((row["USPS"], name_for_search(row["NAME"]).casefold()) for row in places.values())
    place_rows = []
    for key, r in places.items():
        label = name_for_search(r["NAME"])
        county = representative_county(r, shapes)
        county_name = counties[county]["NAME"] if county else ""
        repeated_label = labels[r["USPS"], label.casefold()] > 1
        search_label = label if not repeated_label else f"{label}, {county_name}" if county_name else ""
        place_rows.append({
            "state": r["USPS"], "place_geoid": key, "place_scope": place_scope(r["USPS"], key),
            "census_name": r["NAME"], "place": label, "search_label": search_label,
            "representative_county_geoid": county, "representative_county": county_name,
            "type": "Census-designated place" if r["LSAD"] == "57" else "Incorporated place",
            "lat": r["INTPTLAT"], "lon": r["INTPTLONG"], "has_2020_zip_overlap": int(key in linked),
        })
    qualified_labels = Counter((row["state"], row["search_label"].casefold()) for row in place_rows)
    for row in place_rows:
        if qualified_labels[row["state"], row["search_label"].casefold()] > 1:
            # A few jurisdictions contain a city/town/village/CDP with the same
            # base name in the same county. Retain the Census legal type rather
            # than dropping the place-level search and relying only on ZIPs.
            row["search_label"] = row["census_name"]
    place_rows.sort(key=lambda r: (position(r["state"]), r["place"].casefold(), r["place_geoid"]))
    county_rows = [
        {"state": r["USPS"], "county_geoid": key, "county": r["NAME"], "lat": r["INTPTLAT"], "lon": r["INTPTLONG"]}
        for key, r in counties.items()
    ]
    county_rows.sort(key=lambda r: (position(r["state"]), r["county"]))
    zip_rows = []
    for (state, zcta), overlap in sorted(county_links.items(), key=lambda x: (position(x[0][0]), x[0][1])):
        county_ids = sorted(overlap)
        zip_rows.append({"state": state, "zip_area": zcta, "zip_scope": zip_scope(state, zcta),
                         "counties": " | ".join(counties[c]["NAME"] if c in counties else f"2020 county {c}"
                                                for c in county_ids),
                         "county_geoids": " | ".join(county_ids),
                         "lat": zip_data[zcta]["INTPTLAT"], "lon": zip_data[zcta]["INTPTLONG"],
                         "has_2020_place_overlap": int((state, zcta) in has_2020_place)})
    links.sort(key=lambda r: (position(r["state"]), r["place_scope"], -r["overlap_land_sq_m"], r["zip_area"]))
    for file, rows in (("places.csv", place_rows), ("counties.csv", county_rows),
                       ("zip-areas.csv", zip_rows), ("place-zip-links.csv", links)):
        if not rows:
            raise ValueError(f"The Census sources contain no rows for {file} and {', '.join(states)}.")
        write_csv(output / file, rows, list(rows[0]))

    summary = {}
    for state in states:
        p = [row for row in place_rows if row["state"] == state]
        z = [row for row in zip_rows if row["state"] == state]
        boundary_ids = {key for key, _ in shapes[state]}
        summary[state] = {
            "counties": sum(row["state"] == state for row in county_rows),
            "places": len(p), "census_designated_places": sum(row["type"] == "Census-designated place" for row in p),
            "zip_areas": len(z), "zip_areas_without_2020_place_overlap": sum(not row["has_2020_place_overlap"] for row in z),
            "places_without_matched_2020_zip_overlap": sum(not row["has_2020_zip_overlap"] for row in p),
            "places_with_county_qualified_search_label": sum(bool(row["search_label"]) and row["search_label"] != row["place"] for row in p),
            "places_requiring_manual_search_label": sum(not row["search_label"] for row in p),
            "place_zip_links": sum(row["state"] == state for row in links),
            "legacy_only_place_links": legacy_only[state],
            "place_links_without_a_matched_zip_area": unavailable_zip_links[state],
            "county_ids_without_matching_boundary": sum(
                row["state"] == state and row["county_geoid"] not in boundary_ids
                for row in county_rows),
        }
    provenance = {file: {"url": url, "bytes": (SOURCES / file).stat().st_size,
                         "sha256": hashlib.sha256((SOURCES / file).read_bytes()).hexdigest()}
                  for file, url in ORIGINS.items()}
    (output / "inventory.json").write_text(json.dumps({"states": summary, "sources": provenance}, indent=2) + "\n")
    return summary


def state_catalog(state):
    if state not in STATES:
        raise ValueError(f"Unsupported US state code: {state}")
    if state in SEED_STATES:
        return HERE
    target = HERE / "states" / state
    if not target.is_dir():
        target.parent.mkdir(exist_ok=True)
        with TemporaryDirectory(dir=target.parent) as scratch:
            staged = Path(scratch)
            build((state,), staged)
            staged.rename(target)
    return target


def read_rows(file, state=None):
    folder = state_catalog(state) if state else HERE
    with (folder / file).open(encoding="utf-8", newline="") as source:
        return list(csv.DictReader(source))


SCREENABLE_CAPTURES = {"requested_limit", "maps_end_of_list", "maps_limited_view", "direct_profile"}


def make_plan(state, niche, limit, results, alternate=None):
    places = [row for row in read_rows("places.csv", state) if row["state"] == state]
    zips = [row for row in read_rows("zip-areas.csv", state) if row["state"] == state]
    finished = {r["scope_id"] for r in results if r["stop"] in SCREENABLE_CAPTURES}
    deepen = {r["scope_id"] for r in results if r["stop"] in SCREENABLE_CAPTURES
              and (r["stop"] != "maps_end_of_list" or r.get("new_qualified", 0) > 0)}
    priority = {name: index for index, name in enumerate(PRIORITY.get(state, ()))}
    places.sort(key=lambda row: (priority.get(row["place"], len(priority)), float(row["lat"]), row["place_scope"]))
    depth = defaultdict(float)
    for link in read_rows("place-zip-links.csv", state):
        if link["place_scope"] in deepen:
            key = link["zip_scope"]
            depth[key] = max(depth[key], float(link["share_of_2020_in_state_zip_land"]))

    promoted = sorted((row for row in zips if row["zip_scope"] in depth),
                      key=lambda row: (-depth[row["zip_scope"]], row["zip_area"]))
    outside_places = [row for row in zips if row["has_2020_place_overlap"] == "0"]
    result = []
    seen = finished | {row["scope_id"] for row in results if row["stop"] == "blocked"}

    def add_zip(row, reason):
        key = row["zip_scope"]
        if key not in seen:
            result.append({"scope_id": key, "state": state, "level": "ZIP area",
                           "query": f"{niche} {row['zip_area']} {state}", "reason": reason})
            seen.add(key)

    for row in promoted:
        add_zip(row, "Follow up a productive or capped place search")
    if alternate:
        phrase_key = hashlib.sha256(alternate.casefold().encode()).hexdigest()[:12]
        for row in zips:
            scope = row["zip_scope"]
            lower_yield_sample = (scope in finished and scope not in deepen
                                  and hashlib.sha256(scope.encode()).digest()[0] < 26)
            if scope not in deepen and not lower_yield_sample:
                continue
            key = f"{scope}:phrase:{phrase_key}"
            if key not in seen:
                reason = ("Audit a sampled ZIP-area search that ended without a new prospect" if lower_yield_sample
                          else "Follow up a productive or capped ZIP-area search")
                result.append({"scope_id": key, "state": state, "level": "ZIP area alternate phrase",
                               "query": f"{alternate} {row['zip_area']} {state}",
                               "reason": reason})
    # Queue distinct search labels once. Ambiguous names remain in the geographic catalog.
    seen_labels = {row["search_label"].casefold() for row in places if row["place_scope"] in seen}
    has_outcomes = any(scope.startswith((f"place:{state}:", f"zcta:{state}:")) for scope in finished)
    gaps = iter(outside_places if has_outcomes else [])
    for row in places:
        label = row["search_label"].casefold()
        if row["place_scope"] not in seen and label and label not in seen_labels:
            result.append({"scope_id": row["place_scope"], "state": state, "level": "place",
                           "query": f"{niche} {row['search_label']} {state}", "reason": "First pass for a Census place"})
            seen_labels.add(label)
            gap = next(gaps, None)
            if gap:
                add_zip(gap, "Coverage gap outside 2020 Census places")
    for row in outside_places + zips:
        reason = ("ZIP area without a 2020 Census place overlap" if row["has_2020_place_overlap"] == "0"
                  else "Remaining ZIP area coverage")
        add_zip(row, reason)
    return result[:limit]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="mode", required=True)
    commands.add_parser("build")
    plan = commands.add_parser("plan", help="Preview search scopes. Does not open Chrome.")
    plan.add_argument("--state", choices=STATES, default="FL")
    plan.add_argument("--niche", default="auto detailing")
    plan.add_argument("--limit", type=int, default=4)
    plan.add_argument("--results", type=Path, help="Prior per-scope search outcomes for this niche in JSON.")
    plan.add_argument("--alternate", help="Optional second search phrase for productive or capped ZIP areas.")
    args = parser.parse_args()
    if args.mode == "build":
        result = build()
    else:
        if not args.niche.strip() or args.limit < 1:
            parser.error("Enter a niche and a positive scope limit.")
        niche = args.niche.strip()
        alternate = args.alternate.strip() if args.alternate else None
        if alternate and alternate.casefold() == niche.casefold():
            parser.error("The alternate phrase needs to differ from the primary phrase.")
        evidence = []
        if args.results:
            run = json.loads(args.results.read_text())
            if run["niche"].strip().casefold() != niche.casefold():
                parser.error("The outcomes file is for a different niche.")
            evidence = run["outcomes"]
        result = make_plan(args.state, niche, args.limit, evidence, alternate)
    print(json.dumps(result, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
