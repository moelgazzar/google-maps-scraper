"""Offline tests for the one-time saved-profile recheck."""

import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

import profile_recheck as recheck


def card(listing_id, **changes):
    row = {"listing_id": listing_id, "maps_url": f"https://google.com/maps/place/{listing_id}",
           "name": listing_id, "category": "Anything", "rating": 4.5, "reviews": 10,
           "phone": "", "website_url": "", "website_button": "no",
           "decision": "niche_unclear"}
    row.update(changes)
    return row


class RecheckTest(unittest.TestCase):
    def source(self, root, jobs):
        path = root / "source.sqlite3"
        db = sqlite3.connect(path)
        db.execute("CREATE TABLE jobs(state_code TEXT,spec TEXT,decisions TEXT)")
        db.executemany("INSERT INTO jobs VALUES(?,?,?)", [
            (state, json.dumps({"query": query}), json.dumps(rows))
            for state, query, rows in jobs
        ])
        db.commit()
        db.close()
        return path

    def test_cohort_ignores_niche_and_phone_but_is_conservative_about_website(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            source = self.source(root, [
                ("FL", "one", [card("keep"), card("low", rating=3.0),
                                card("few", reviews=4), card("mixed")]),
                ("GA", "two", [card("keep", phone="305-555-0101"),
                                card("mixed", website_button="yes")]),
            ])
            cohort = recheck.build_cohort(source)
            self.assertEqual([row["listing_id"] for row in cohort], ["keep"])
            self.assertEqual(cohort[0]["card_phone"], "3055550101")
            self.assertEqual(cohort[0]["states"], ["FL", "GA"])

    def test_campaign_filters_isolate_state_and_niche(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            path = root / "source.sqlite3"
            db = sqlite3.connect(path)
            db.execute("CREATE TABLE jobs(state_code TEXT,spec TEXT,decisions TEXT)")
            db.executemany("INSERT INTO jobs VALUES(?,?,?)", [
                ("TX", json.dumps({"query": "one", "niche": "auto detailing"}),
                 json.dumps([card("texas-detail")])),
                ("TX", json.dumps({"query": "two", "niche": "car wash"}),
                 json.dumps([card("texas-wash")])),
                ("GA", json.dumps({"query": "three", "niche": "auto detailing"}),
                 json.dumps([card("georgia-detail")])),
            ])
            db.commit(); db.close()
            cohort = recheck.build_cohort(path, "TX", "auto detailing")
            self.assertEqual([row["listing_id"] for row in cohort], ["texas-detail"])

    def test_address_is_not_a_gate_and_profile_phone_can_qualify(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            source = self.source(root, [("FL", "one", [card("keep")])])
            ledger = recheck.RecheckLedger(root / "recheck.sqlite3", source)
            try:
                row = ledger.claim("test")
                decision, phone = ledger.finish(row, {
                    "phone": "305-555-0199", "website": "", "address": "",
                    "closure_status": "open_or_unknown",
                })
                self.assertEqual(decision, "eligible_needs_relevance_review")
                self.assertEqual(phone, "3055550199")
                self.assertIsNone(ledger.claim("second"))
            finally:
                ledger.close()

    def test_closed_and_website_profiles_are_preserved_but_not_eligible(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            source = self.source(root, [("FL", "one", [card("closed", phone="3055550101"),
                                                         card("site", phone="3055550102")])])
            ledger = recheck.RecheckLedger(root / "recheck.sqlite3", source)
            try:
                first = ledger.claim("one")
                if first["listing_id"] == "closed":
                    closed, site = first, ledger.claim("two")
                else:
                    site, closed = first, ledger.claim("two")
                self.assertEqual(ledger.finish(closed, {"phone": "", "website": "",
                    "closure_status": "permanently_closed"})[0], "permanently_closed")
                self.assertEqual(ledger.finish(site, {"phone": "", "website": "https://example.com",
                    "closure_status": "open_or_unknown"})[0], "profile_has_website")
            finally:
                ledger.close()


if __name__ == "__main__":
    unittest.main()
