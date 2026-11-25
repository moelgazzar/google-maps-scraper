import csv
import importlib.util
import json
import sqlite3
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location(
    "ai_review_pipeline", ROOT / "scripts" / "ai_review_pipeline.py")
pipeline = importlib.util.module_from_spec(spec); spec.loader.exec_module(pipeline)


class AiReviewPipelineTests(unittest.TestCase):
    def seed_queue_db(self, path: Path):
        db = sqlite3.connect(path)
        db.execute("CREATE TABLE jobs(spec TEXT,state_code TEXT,decisions TEXT)")
        rows = [
            {"listing_id": "one", "name": "One Detail", "rating": 4.8, "reviews": 9,
             "website_button": "no", "website_url": "", "phone": "5551112222"},
            {"listing_id": "two", "name": "Two Repair", "rating": 4.5, "reviews": 8,
             "website_button": "no", "website_url": "", "phone": "5553334444"},
        ]
        spec = json.dumps({"niche": "auto detailing", "query": "auto detailing Austin TX"})
        db.execute("INSERT INTO jobs VALUES(?,?,?)", (spec, "TX", json.dumps(rows)))
        db.commit(); db.close()

    def test_export_import_and_prepare_phone(self):
        with tempfile.TemporaryDirectory() as folder:
            folder = Path(folder)
            queue_db, ledger = folder / "queue.sqlite3", folder / "ai.sqlite3"
            queue, decisions, phone = folder / "queue.csv", folder / "decisions.csv", folder / "phone.csv"
            evidence = folder / "eligible.csv"
            self.seed_queue_db(queue_db)
            evidence.write_text(
                "listing_id,name,rating,reviews,final_phone,maps_url,card_category,profile_category,"
                "website,closure_status,decision,states\n"
                "one,One Detail,4.0,10,5551112222,https://maps/one,Detailer,Detailer,,open_or_unknown,"
                "eligible_needs_relevance_review,TX\n"
                "two,Two Repair,4.5,12,5553334444,https://maps/two,Repair,Repair,,open_or_unknown,"
                "eligible_needs_relevance_review,TX\n", encoding="utf-8")
            report = pipeline.export_queue("texas-auto-detailing", "TX", "auto detailing",
                                           queue, queue_db, evidence, ledger)
            self.assertEqual(report["ready_for_ai"], 2)
            with decisions.open("w", encoding="utf-8", newline="") as stream:
                writer = csv.DictWriter(stream, fieldnames=("listing_id", "label"))
                writer.writeheader(); writer.writerows([
                    {"listing_id": "one", "label": "pass"},
                    {"listing_id": "two", "label": "not_pass"},
                ])
            loaded = pipeline.import_decisions("texas-auto-detailing", queue, decisions,
                                               "codex-test", ledger)
            self.assertEqual(loaded["imported"], 2)
            prepared = pipeline.prepare_phone_input("texas-auto-detailing", queue, phone, ledger)
            self.assertEqual(prepared["ready_for_apify"], 1)
            with phone.open(encoding="utf-8") as stream:
                rows = list(csv.DictReader(stream))
            self.assertEqual(rows[0]["listing_id"], "one")

    def test_rejects_unknown_listing(self):
        with tempfile.TemporaryDirectory() as folder:
            folder = Path(folder)
            queue_db, ledger = folder / "queue.sqlite3", folder / "ai.sqlite3"
            queue, decisions, evidence = folder / "queue.csv", folder / "decisions.csv", folder / "eligible.csv"
            self.seed_queue_db(queue_db)
            evidence.write_text(
                "listing_id,name,rating,reviews,final_phone,maps_url,card_category,profile_category,"
                "website,closure_status,decision,states\n"
                "one,One,4.8,9,5551112222,https://maps/one,Detailer,Detailer,,open_or_unknown,"
                "eligible_needs_relevance_review,TX\n", encoding="utf-8")
            pipeline.export_queue("campaign", "TX", "auto detailing", queue, queue_db, evidence, ledger)
            decisions.write_text("listing_id,label\nunknown,pass\n", encoding="utf-8")
            with self.assertRaises(ValueError):
                pipeline.import_decisions("campaign", queue, decisions, "test", ledger)


if __name__ == "__main__":
    unittest.main()
