import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

import campaign_candidates as candidates


class CampaignCandidateTests(unittest.TestCase):
    def test_unique_business_and_basic_filter_counts(self):
        with tempfile.TemporaryDirectory() as folder:
            db_path = Path(folder) / "queue.sqlite3"
            db = sqlite3.connect(db_path)
            db.execute("CREATE TABLE jobs(spec TEXT,state_code TEXT,decisions TEXT)")
            good = {"listing_id": "one", "name": "One", "rating": 4.0, "reviews": 10,
                    "website_button": "no", "website_url": "", "phone": "555"}
            low = {"listing_id": "two", "name": "Two", "rating": 3.0, "reviews": 9,
                   "website_button": "no", "website_url": ""}
            spec = json.dumps({"niche": "auto detailing", "query": "auto detailing Austin TX"})
            db.execute("INSERT INTO jobs VALUES(?,?,?)", (spec, "TX", json.dumps([good, low, good])))
            db.commit(); db.close()
            metrics = candidates.campaign_metrics("TX", "auto detailing", db_path, 0)
            self.assertEqual(metrics["businesses_found"], 2)
            self.assertEqual(metrics["passed_card_filters"], 1)
            self.assertEqual(metrics["ready_for_ai"], 0)
            overall = candidates.all_campaign_metrics(db_path, 0)
            self.assertEqual(overall["businesses_found"], 2)
            self.assertEqual(overall["passed_card_filters"], 1)
            self.assertEqual(overall["ready_for_ai"], 0)
            output = Path(folder) / "ai-review-queue.csv"
            evidence = Path(folder) / "eligible-candidates.csv"
            evidence.write_text(
                "listing_id,name,rating,reviews,final_phone,maps_url,card_category,profile_category,"
                "website,closure_status,decision,states\n"
                "one,One,4.0,10,555,https://maps/one,Detailer,Car detailing service,,open_or_unknown,"
                "eligible_needs_relevance_review,TX\n", encoding="utf-8")
            rows = candidates.export_ai_queue("TX", "auto detailing", output, db_path, evidence)
            self.assertEqual(len(rows), 1)
            self.assertTrue(output.is_file())
            with output.open(encoding="utf-8") as stream:
                header = stream.readline()
            self.assertIn("listing_id", header)

    def test_card_filter_does_not_require_phone_before_profile_check(self):
        row = {"listing_id": "one", "rating": 4.0, "reviews": 10,
               "website_button": "no", "website_url": "", "phone": ""}
        self.assertTrue(candidates.card_filter(row))
        self.assertFalse(candidates.card_filter({**row, "rating": 3.9}))
        self.assertFalse(candidates.card_filter({**row, "reviews": 9}))

    def test_ready_for_ai_requires_profile_evidence(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "eligible.csv"
            path.write_text(
                "listing_id,final_phone,website,closure_status,decision,rating,reviews\n"
                "one,555,,open_or_unknown,eligible_needs_relevance_review,4.0,10\n"
                "two,555,https://example.com,open_or_unknown,profile_has_website,4.5,10\n"
                "three,555,,temporarily_closed,temporarily_closed_hold,4.5,10\n"
                "four,555,,open_or_unknown,eligible_needs_relevance_review,4.9,9\n",
                encoding="utf-8")
            self.assertEqual([row["listing_id"] for row in candidates.profile_evidence_rows(path)],
                             ["one"])


if __name__ == "__main__":
    unittest.main()
