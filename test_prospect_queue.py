"""Offline recovery and duplicate checks for the local state workers."""
import asyncio
import csv
import json
import subprocess
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
from io import StringIO
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import prospect_queue as queue
from chrome_capture import PlaywrightTimeout, capture_on_page
from geography import launcher
from prospect import qualify


def market(key, city, state, position):
    return {"key": key, "position": position, "city": city, "state": state,
            "market": f"{city}, {state}", "zip": "", "query": f"auto detailing {city} {state}"}


def card(code, phone):
    return {"name": f"{code} Auto Detailing", "maps_url": f"https://www.google.com/maps/place/{code}/data=!19s{code}",
            "category": "Car detailing service", "rating": 4.9, "reviews": 12,
            "address": "", "phone": phone, "website_url": "", "website_button": "no"}


def capture(rows, stop="requested_limit"):
    return {"rows": rows, "stop": stop, "capture_seconds": 0.01, "error": ""}


def profile(row):
    return {"address": f"Address: 1 Test St, {row['city']}, {row['state']} 12345",
            "website": "", "booking_links": [], "service_area": "", "add_website": True}


class BrowserFixture:
    async def new_context(self, **kwargs):
        return self

    async def new_page(self):
        return object()

    async def close(self):
        return None


class InterruptedRun(RuntimeError):
    pass


class WorkersTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory(dir=queue.RUNTIME)
        self.addCleanup(directory.cleanup)
        root = Path(directory.name)
        (root / "active.json").write_text(json.dumps(launcher.campaign_settings("FL", "auto detailing")))
        (root / "florida-regions.json").write_bytes((launcher.HERE / "florida-regions.json").read_bytes())
        self.enterContext(patch.object(launcher, "HERE", root))

    async def test_operator_pause_prevents_new_browser_batch(self):
        with tempfile.TemporaryDirectory(dir=queue.RUNTIME) as folder:
            ledger = queue.Ledger(Path(folder))
            job = market("operator-pause", "Ocala", "FL", 1)
            ledger.sync([job], set(), False)
            try:
                with patch.object(queue, "current_action", return_value="safe_stop"), \
                     patch.object(queue, "service_credentials", return_value=("user", "password")), \
                     patch.object(queue, "proxy_hosts", return_value=("proxy",)), \
                     patch.object(queue, "ordered_hosts", return_value=("proxy",)):
                    result = await queue.browse_independent_slots(
                        ledger, [job], 1, 10, None, max_rotations=1)
                self.assertTrue(result["paused"])
                self.assertEqual(result["remaining"], 1)
                self.assertEqual(result["rotations"], {})
            finally:
                ledger.db.close()

    def test_saved_state_address_passes_and_exports_both_zips_once(self):
        with tempfile.TemporaryDirectory(dir=queue.RUNTIME) as folder:
            root = Path(folder)
            path = root / "prospects.csv"
            ledger = queue.Ledger(root, state_export=("FL", path))
            try:
                job = {**market("florida", "Brandon", "FL", 1), "zip": "33510", "scope_kind": "zip_area"}
                source = card("FloridaFixture", "813-555-0199")
                candidate = qualify([{**source, **job}])[0]
                evidence = {**profile(candidate), "address": "Address: 1 Test St, Tampa, FL 33602"}
                decision = queue.profile_decision(candidate, evidence)
                self.assertEqual(decision, "locality_unconfirmed")
                candidate.update(decision=decision, profile=evidence)
                ledger.sync([job], set(), False)
                ledger.capture_saved(job, capture([source]), [candidate])
                ledger.finish(job, [candidate])

                state_mode = {**candidate, "geography_filter": "state"}
                self.assertEqual(queue.profile_decision(state_mode, {**evidence, "address": "1 Test St, Macon, GA 31201"}),
                                 "outside_target_state")
                self.assertEqual(queue.profile_decision(state_mode, {**evidence, "address": ""}), "locality_unconfirmed")
                config = {"states": ["FL"], "niche": "auto detailing", "geography_filter": "state"}
                with redirect_stdout(StringIO()):
                    queue.recheck_saved(ledger, config)
                    queue.recheck_saved(ledger, config)

                with path.open() as file:
                    saved = list(csv.DictReader(file))
                self.assertEqual(len(saved), 1)
                self.assertEqual((saved[0]["market"], saved[0]["searched_zip"], saved[0]["business_zip"]),
                                 ("Tampa, FL", "33510", "33602"))
            finally:
                ledger.db.close()

    async def test_discovery_preserves_checkpoint_on_rate_limit_or_timeout(self):
        prior = [card("SavedFixture", "305-555-0101")]

        class Page:
            url = "https://www.google.com/maps/search/example"

            async def goto(self, *args, **kwargs):
                if self.timed_out:
                    raise PlaywrightTimeout("Navigation timed out")
                return SimpleNamespace(status=429)

        for timed_out, expected in [(False, "maps_access_restricted"), (True, "maps_timeout")]:
            page = Page()
            page.timed_out = timed_out
            result = await capture_on_page(page, "auto detailing Ocala FL", None, prior)
            self.assertEqual(result["stop"], expected)
            self.assertEqual(result["rows"], prior)

    async def test_discovery_only_saves_all_cards_without_opening_profiles(self):
        with tempfile.TemporaryDirectory(dir=queue.RUNTIME) as folder:
            ledger = queue.Ledger(Path(folder))
            job = market("discovery-only", "Atlanta", "GA", 1)
            rows = [card("NoPhone", ""), card("WithPhone", "404-555-0101")]
            ledger.sync([job], set(), False)
            try:
                with patch.object(queue, "DISCOVERY_ONLY", True), \
                        patch.object(queue, "capture_on_page", AsyncMock(return_value=capture(rows))), \
                        patch.object(queue, "inspect_profile", AsyncMock(side_effect=AssertionError("profile opened"))):
                    await queue.process_job(ledger, job, object(), asyncio.Event(), None)
                stored = ledger.row(job["key"])
                self.assertEqual(stored["stage"], "done")
                decisions = json.loads(stored["decisions"])
                self.assertEqual(len(decisions), 2)
                self.assertTrue(all("profile" not in row for row in decisions))
                self.assertTrue(ledger.db.execute(
                    "SELECT 1 FROM events WHERE event='discovery_only_scope_complete'"
                ).fetchone())
            finally:
                ledger.db.close()

    def test_niche_switch_uses_its_own_search_history_and_filters(self):
        with tempfile.TemporaryDirectory(dir=queue.RUNTIME) as folder:
            root = Path(folder)
            ledger = queue.Ledger(root)
            try:
                jobs = queue.catalog_jobs(ledger, [], 1)
                ledger.sync(jobs, set(), False)
                ledger.capture_saved(jobs[0], capture([card("First", "813-555-0101")]), [])
                ledger.finish(jobs[0], [])

                config = launcher.campaign_settings("FL", "landscaping")
                (root / "active.json").write_text(json.dumps(config))
                with patch.object(launcher, "HERE", root):
                    new_jobs = queue.catalog_jobs(ledger, [], 1)
                self.assertEqual(new_jobs[0]["scope_id"], jobs[0]["scope_id"])
                self.assertNotEqual(new_jobs[0]["key"], jobs[0]["key"])
                ledger.sync(new_jobs, {jobs[0]["market"]}, False)
                self.assertEqual(ledger.pending(1), new_jobs)

                sample = {**card("LandscapeFixture", "813-555-0102"), **new_jobs[0],
                          "category": "Landscaper", "rating": 4.0, "reviews": 10}
                self.assertEqual(qualify([sample])[0]["decision"], "candidate")
                self.assertEqual(qualify([{**sample, "rating": 3}])[0]["decision"], "below_threshold_or_missing")
                self.assertEqual(qualify([{**sample, "category": "Car detailing service"}])[0]["decision"], "niche_unclear")
            finally:
                ledger.db.close()

    async def test_ten_florida_regions_run_at_the_same_time(self):
        with tempfile.TemporaryDirectory(dir=queue.RUNTIME) as folder:
            ledger = queue.Ledger(Path(folder))
            try:
                jobs = queue.catalog_jobs(ledger, [], 10, florida_regions=True)
                self.assertEqual(len({job["region"] for job in jobs}), 10)
                self.assertEqual({job["state"] for job in jobs}, {"FL"})
                ledger.sync(jobs, set(), False)
                cards = {job["query"]: card(f"Florida{index}", f"305-555-{index:04d}")
                         for index, job in enumerate(jobs, start=100)}
                all_started = asyncio.Event()
                arrivals = set()

                async def fake_capture(page, query, limit, saved_rows, checkpoint):
                    arrivals.add(query)
                    if len(arrivals) == 10:
                        all_started.set()
                    await asyncio.wait_for(all_started.wait(), 1)
                    return capture([cards[query]])

                async def fake_profile(page, row):
                    return profile(row)

                with patch.object(queue, "capture_on_page", side_effect=fake_capture), \
                     patch.object(queue, "inspect_profile", side_effect=fake_profile):
                    await queue.run_workers(ledger, ledger.pending(10), BrowserFixture(), 10, 10, asyncio.Event())
                self.assertEqual(len(ledger.accepted()), 10)
                self.assertEqual(ledger.pending(10), [])
            finally:
                ledger.db.close()

    async def test_statewide_topology_uses_ten_tabs_in_one_context(self):
        class CountingContext:
            def __init__(self):
                self.pages = 0

            async def new_page(self):
                self.pages += 1
                return object()

        with tempfile.TemporaryDirectory(dir=queue.RUNTIME) as folder:
            ledger = queue.Ledger(Path(folder))
            try:
                jobs = []
                for index in range(10):
                    job = market(f"statewide-{index}", f"Fixture {index}", "FL", index + 1)
                    job["region"] = f"Region {index % 10}"
                    jobs.append(job)
                ledger.sync(jobs, set(), False)
                contexts = [CountingContext()]
                all_started = asyncio.Event()
                arrivals = set()

                async def fake_process(ledger_arg, job, page, stop, limit):
                    arrivals.add(job["key"])
                    if len(arrivals) == 10:
                        all_started.set()
                    await asyncio.wait_for(all_started.wait(), 2)

                with patch.object(queue, "process_job", side_effect=fake_process):
                    await queue.run_workers(ledger, jobs, contexts, 10, None,
                                            asyncio.Event(), heartbeat=0.005)

                self.assertEqual(len(arrivals), 10)
                self.assertEqual([context.pages for context in contexts], [10])
            finally:
                ledger.db.close()

    async def test_independent_browsers_use_distinct_routes_and_finish_without_waiting(self):
        class FakePlaywrightContext:
            async def __aenter__(self):
                return object()

            async def __aexit__(self, *args):
                return None

        class FakeBridge:
            async def close(self):
                return None

        with tempfile.TemporaryDirectory(dir=queue.RUNTIME) as folder:
            ledger = queue.Ledger(Path(folder))
            try:
                jobs = [market(f"slot-{index}", f"Fixture {index}", "FL", index)
                        for index in range(1, 5)]
                ledger.sync(jobs, set(), False)
                launches = []

                async def fake_launch(playwright, host, username, password, previous_ip):
                    launches.append(host)
                    return BrowserFixture(), FakeBridge(), f"203.0.113.{len(launches)}"

                async def fake_process(ledger_arg, job, page, stop, limit):
                    if job["key"] in {"slot-3", "slot-4"}:
                        await asyncio.sleep(0.03)
                    ledger_arg.finish(job, [])

                with patch.object(queue, "async_playwright", return_value=FakePlaywrightContext()), \
                     patch.object(queue, "service_credentials", return_value=("user", "password")), \
                     patch.object(queue, "proxy_hosts", return_value=("proxy-a", "proxy-b")), \
                     patch.object(queue, "route_state", return_value={"host": "", "public_ip": ""}), \
                     patch.object(queue, "save_route_state"), \
                     patch.object(queue, "launch_nord_browser", side_effect=fake_launch), \
                     patch.object(queue, "process_job", side_effect=fake_process):
                    result = await queue.browse_independent_slots(
                        ledger, jobs, 2, 2, 30, max_rotations=1)

                self.assertEqual(set(launches), {"proxy-a", "proxy-b"})
                self.assertEqual(result["rotations"], {"browser-1": 1, "browser-2": 1})
                self.assertEqual(result["capacity_errors"], [])
                self.assertTrue(all(ledger.row(job["key"])["stage"] == "done" for job in jobs))
            finally:
                ledger.db.close()

    async def test_capacity_limited_slot_retires_without_stopping_healthy_slot(self):
        class FakePlaywrightContext:
            async def __aenter__(self):
                return object()

            async def __aexit__(self, *args):
                return None

        class FakeBridge:
            async def close(self):
                return None

        with tempfile.TemporaryDirectory(dir=queue.RUNTIME) as folder:
            ledger = queue.Ledger(Path(folder))
            try:
                jobs = [market(f"capacity-{index}", f"Fixture {index}", "FL", index)
                        for index in range(1, 3)]
                ledger.sync(jobs, set(), False)
                launches = 0

                async def fake_launch(playwright, host, username, password, previous_ip):
                    nonlocal launches
                    launches += 1
                    return BrowserFixture(), FakeBridge(), f"203.0.113.{launches}"

                async def fake_process(ledger_arg, job, page, stop, limit):
                    await asyncio.sleep(0.06)
                    ledger_arg.finish(job, [])

                with patch.object(queue, "async_playwright", return_value=FakePlaywrightContext()), \
                     patch.object(queue, "service_credentials", return_value=("user", "password")), \
                     patch.object(queue, "proxy_hosts", return_value=("only-proxy",)), \
                     patch.object(queue, "route_state", return_value={"host": "", "public_ip": ""}), \
                     patch.object(queue, "save_route_state"), \
                     patch.object(queue, "launch_nord_browser", side_effect=fake_launch), \
                     patch.object(queue, "process_job", side_effect=fake_process):
                    result = await queue.browse_independent_slots(
                        ledger, jobs, 2, 1, 30, route_wait_seconds=0.02,
                        route_retry_min_wait=0.01, route_retry_max_wait=0.01)

                self.assertFalse(result["paused"])
                self.assertGreaterEqual(len(result["capacity_errors"]), 1)
                self.assertTrue(all(ledger.row(job["key"])["stage"] == "done" for job in jobs))
            finally:
                ledger.db.close()

    def test_limited_city_result_promotes_zip_and_keeps_unfinished_searches(self):
        with tempfile.TemporaryDirectory(dir=queue.RUNTIME) as folder:
            ledger = queue.Ledger(Path(folder))
            try:
                jobs = queue.catalog_jobs(ledger, [], 4)
                self.assertEqual(jobs[0]["city"], "Brandon")
                ledger.sync(jobs, set(), False)
                ledger.capture_saved(jobs[0], capture([card("BrandonFixture", "813-555-0111")], "maps_limited_view"), [])
                ledger.finish(jobs[0], [])

                ledger.sync(queue.catalog_jobs(ledger, [], 4), set(), False)
                pending = ledger.pending(4)
                self.assertEqual([job["key"] for job in pending[:3]], [job["key"] for job in jobs[1:]])
                self.assertEqual(pending[3]["scope_kind"], "zip_area")
                self.assertEqual(pending[3]["state"], "FL")
            finally:
                ledger.db.close()

    def test_zip_scope_sets_observed_city_and_holds_unconfirmed_zip(self):
        job = {"scope_kind": "zip_area", "state": "FL", "zip": "33511", "city": "",
               "market": "ZIP 33511, FL", "name": "Fixture Auto Detailing"}
        evidence = {**profile({"city": "Brandon", "state": "FL"}),
                    "address": "Address: 1 Test St, Brandon, FL 33511-1234, United States"}
        result = {**job, "decision": queue.profile_decision(job, evidence), "profile": evidence}
        queue.check_geography(result)
        self.assertEqual((result["decision"], result["market"]), ("qualified", "Brandon, FL"))

        only_service_area = {**evidence, "address": "", "service_area": "Serves Brandon, FL 33511"}
        result = {**job, "decision": "qualified", "profile": only_service_area}
        queue.check_geography(result)
        self.assertEqual(result["decision"], "zip_unconfirmed")

    def test_commit_survives_forced_exit(self):
        with tempfile.TemporaryDirectory(dir=queue.RUNTIME) as folder:
            spec = market("extensionless", "Akron", "OH", 1)
            saved = capture([card("SavedFixture", "330-555-0111")], "capturing")
            script = """
import json, os, sys
from pathlib import Path
from prospect_queue import Ledger
ledger = Ledger(Path(sys.argv[1]))
job, saved = json.loads(sys.argv[2])
ledger.sync([job], set(), False)
ledger.capture_progress(job, saved)
os._exit(17)
"""
            child = subprocess.run(
                [sys.executable, "-c", script, folder, json.dumps([spec, saved])], check=False)
            self.assertEqual(child.returncode, 17)
            ledger = queue.Ledger(Path(folder))
            try:
                self.assertEqual(json.loads(ledger.row(spec["key"])["capture"]), saved)
                self.assertIn('"event": "capture_progress"', (Path(folder) / "events.jsonl").read_text())
                self.assertEqual(ledger.pending(1), [spec])
            finally:
                ledger.db.close()

    async def test_workers_handoff_and_conflicting_prospect(self):
        with tempfile.TemporaryDirectory(dir=queue.RUNTIME) as folder:
            ledger = queue.Ledger(Path(folder))
            try:
                jobs = [
                    market("a", "Akron", "OH", 1), market("b", "Peoria", "IL", 2),
                    market("c", "Canton", "OH", 3), market("d", "Green Bay", "WI", 4),
                ]
                phones = ["330-555-0111", "(330) 555-0111", "330-555-0112", "920-555-0113"]
                saved = {job["query"]: card(job["key"] + "Fixture", phone) for job, phone in zip(jobs, phones)}
                ledger.sync(jobs, set(), False)
                barrier = asyncio.Event()
                arrivals = 0

                async def fake_capture(page, query, limit, saved_rows, checkpoint):
                    assert not saved_rows
                    result = capture([saved[query]])
                    checkpoint(result)
                    return result

                async def fake_profile(page, row):
                    nonlocal arrivals
                    arrivals += 1
                    if arrivals == 2:
                        barrier.set()
                    if arrivals <= 2:
                        await asyncio.wait_for(barrier.wait(), 1)
                    await asyncio.sleep(0.02)
                    return profile(row)

                with patch.object(queue, "capture_on_page", side_effect=fake_capture), \
                     patch.object(queue, "inspect_profile", side_effect=fake_profile):
                    await queue.run_workers(ledger, jobs, BrowserFixture(), 2, 30, asyncio.Event(), heartbeat=0.005)
                ledger.status("idle")
                events = [json.loads(line) for line in (Path(folder) / "events.jsonl").read_text().splitlines()]
                assignments = [event for event in events if event["event"] == "state_assigned"]
                self.assertEqual({event["state"] for event in assignments[:2]}, {"OH", "IL"})
                self.assertEqual(assignments[2]["state"], "WI")
                self.assertEqual([event["state"] for event in assignments].count("OH"), 1)
                duplicate = [event for event in events if event["event"] == "duplicate_at_export"]
                self.assertEqual(len(duplicate), 1)
                self.assertTrue(duplicate[0]["same_phone"])
                self.assertGreaterEqual(sum(event["event"] == "heartbeat" for event in events), 2)
                self.assertFalse(ledger.pending(4))
                with (Path(folder) / "qualified.csv").open(newline="") as file:
                    self.assertEqual(len(list(csv.DictReader(file))), 3)
                self.assertEqual(json.loads((Path(folder) / "status.json").read_text())["workers"], {})
            finally:
                ledger.db.close()

    def test_grouped_city_file_still_starts_both_workers(self):
        with tempfile.TemporaryDirectory(dir=queue.RUNTIME) as folder:
            ledger = queue.Ledger(Path(folder))
            try:
                jobs = [market(str(i), f"City {i}", "OH" if i < 5 else "IL", i)
                        for i in range(1, 9)]
                ledger.sync(jobs, set(), False)
                self.assertEqual([job["state"] for job in ledger.pending(4)], ["OH", "IL", "OH", "IL"])
                self.assertEqual([job["position"] for job in ledger.pending(4)], [1, 5, 2, 6])
            finally:
                ledger.db.close()

    async def test_resume_captured_cards_and_completed_profile(self):
        with tempfile.TemporaryDirectory(dir=queue.RUNTIME) as folder:
            root = Path(folder)
            job = market("resume", "Akron", "OH", 1)
            first, second = card("FirstFixture", "330-555-0111"), card("SecondFixture", "330-555-0112")

            async def interrupt_capture(page, query, limit, saved_rows, checkpoint):
                checkpoint(capture([first], "capturing"))
                raise InterruptedRun("Interrupted between result batches.")

            async def finish_capture(page, query, limit, saved_rows, checkpoint):
                self.assertEqual(saved_rows, [first])
                return capture([first, second])

            async def interrupt_profile(page, row):
                if row["listing_id"] == "SecondFixture":
                    raise InterruptedRun("Interrupted after checking the first profile.")
                return profile(row)

            async def resume_profile(page, row):
                self.assertEqual(row["listing_id"], "SecondFixture")
                return profile(row)

            for fixture_capture, fixture_profile, interrupts in (
                (interrupt_capture, None, True),
                (finish_capture, interrupt_profile, True),
                (None, resume_profile, False),
            ):
                ledger = queue.Ledger(root)
                try:
                    ledger.sync([job], set(), False)
                    with patch.object(queue, "capture_on_page", side_effect=fixture_capture or AssertionError("Captured already")), \
                         patch.object(queue, "inspect_profile", side_effect=fixture_profile or AssertionError("No profiles yet")):
                        task = queue.run_workers(
                            ledger, ledger.pending(4), BrowserFixture(), 2, 30, asyncio.Event())
                        if interrupts:
                            with self.assertRaises(ExceptionGroup) as error:
                                await task
                            self.assertEqual(len(error.exception.exceptions), 1)
                            self.assertIsInstance(error.exception.exceptions[0], InterruptedRun)
                        else:
                            await task
                finally:
                    ledger.db.close()

            ledger = queue.Ledger(root)
            try:
                ledger.export()
                self.assertEqual(ledger.row(job["key"])["stage"], "done")
                self.assertEqual(len(ledger.accepted()), 2)
                self.assertEqual(ledger.row(job["key"])["failures"], 0)
                events = (root / "events.jsonl").read_text().splitlines()
                self.assertEqual(sum(json.loads(line)["event"] == "scope_complete" for line in events), 1)
            finally:
                ledger.db.close()


if __name__ == "__main__":
    unittest.main()
