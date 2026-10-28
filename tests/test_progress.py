from pathlib import Path
from tempfile import TemporaryDirectory
import os
import unittest
from unittest.mock import patch

from speedrun_etl.entity_ods import EntityPackage
from speedrun_etl.jsonutil import atomic_write, file_lock
from speedrun_etl.progress import progress, render_collection, render_batch, driver_running


class ProgressTests(unittest.TestCase):
    def test_repair_progress_counts_only_selected_sections_and_shows_date_leaf(self):
        with TemporaryDirectory() as temp, patch.dict(os.environ,{"ETL_STATE":"unused"}):
            os.environ["ETL_STATE"]=temp
            root=Path(temp)/'snapshots/repair-runs'
            atomic_write(root/'config.json',{'repair_from_snapshot_id':'base-runs',
                         'repair_partitions':[{'category_id':'c','status_code':1},{'category_id':'c','status_code':2}]})
            atomic_write(root/'date-parts/c-verified/2020-01-01_2020-12-31/checkpoints/c-verified.json',
                         {'pages':{'1':{}},'pages_planned':5,'complete':False})
            report=progress('repair');item=report['snapshots']['runs']
            self.assertEqual(2,item['expected_sections'])
            self.assertEqual('base-runs',item['retained_base_snapshot_id'])
            self.assertIn('date range 2020-01-01_2020-12-31: pages 1/5',render_collection(report))

    def test_ready_shared_catalog_is_not_rendered_as_an_active_collection(self):
        catalog = {"collection": "b-catalog", "budget": {"used": 264, "limit": 650},
                   "snapshots": {"games_metadata": {"phase": "ready", "records": {"games": 52207}}}}
        report = {"batch": "b", "driver": {}, "driver_running": False, "games_complete": 0,
                  "games_total": 10, "selection_ready": True, "games": [],
                  "collections": {"b-catalog": catalog}}
        output = render_batch(report)
        self.assertIn("Directory: ready, shared snapshot reused; game versions=52207", output)
        self.assertNotIn("HTTP 264/650", output)
        catalog["snapshots"]["games_metadata"].update(phase="collecting", checkpoints=[])
        self.assertIn("HTTP 264/650", render_batch(report))

    def test_audit_uses_selected_checkpoint_pass_not_union_of_prior_history(self):
        from speedrun_etl.audit_entity_ods import checkpoint_run_ids
        responses = {"old": {"r1", "removed"}, "new": {"r1", "r2"}, "boundary": {"extra"}}
        checkpoint = {"complete": True, "pages": {"1": {"response_id": "new"}},
                      "coverage": {"coverage": "verified_public_partitions", "unique_runs": 2}}
        self.assertEqual({"r1", "r2"}, checkpoint_run_ids(checkpoint, responses))
        checkpoint["coverage"]["unique_runs"] = 3
        with self.assertRaisesRegex(ValueError, "count differs"):
            checkpoint_run_ids(checkpoint, responses)
        self.assertIsNone(checkpoint_run_ids({"complete": True, "pages": {},
            "coverage": {"coverage": "partial", "diagnostic": "pagination_unstable_after_one_retry"}}, responses))

    def test_wr_audit_respects_e2e_scope_and_rejects_wrong_category(self):
        from speedrun_etl.audit_entity_ods import expected_wr_categories
        raw = {"categories": [{"id": "one", "isPerLevel": False},
                               {"id": "two", "isPerLevel": False}, {"id": "il", "isPerLevel": True}]}
        manifest = {"input_config": {"scope": "e2e"}, "source_result": {"category_id": "one"}}
        self.assertEqual({"one"}, expected_wr_categories(raw, manifest))
        self.assertEqual({"one", "two"}, expected_wr_categories(raw, {"input_config": {"scope": "game"}}))
        manifest["source_result"]["category_id"] = "il"
        with self.assertRaises(ValueError):
            expected_wr_categories(raw, manifest)

    def test_live_pages_and_expected_sections_without_fake_zero_records(self):
        with TemporaryDirectory() as temp, patch.dict(os.environ, {"ETL_STATE": "unused"}):
            os.environ["ETL_STATE"] = temp
            metadata = EntityPackage(Path(temp) / "snapshots" / "test-games_metadata" / "entity-package")
            metadata.put("manifests", {"key": "m", "source_result": {"category_count": 2}})
            metadata.db.commit()
            metadata.db.close()
            root = Path(temp) / "snapshots" / "test-runs"
            atomic_write(root / "checkpoints" / "c-verified.json", {
                "pages": {"1": {}, "2": {}}, "source_pages": 8, "pages_planned": 8,
                "source_count": 745, "unique_runs": 200, "complete": False, "pass": 0})
            report = progress("test")
            runs = report["snapshots"]["runs"]
            self.assertEqual({}, runs["records"])
            self.assertEqual("collecting", runs["phase"])
            self.assertEqual(6, runs["expected_sections"])
            self.assertIn("pages 2/8", render_collection(report))
            self.assertIn("sections 0/6", render_collection(report))

    def test_finished_package_has_distinct_version_counts_and_ready_phase(self):
        with TemporaryDirectory() as temp, patch.dict(os.environ, {"ETL_STATE": "unused"}):
            os.environ["ETL_STATE"] = temp
            root = Path(temp) / "snapshots" / "test-runs"
            package = EntityPackage(root / "entity-package")
            for dataset, row in (("runs", {"key": "r", "source_id": "run"}),
                                 ("players", {"key": "p", "source_id": "player"}),
                                 ("checks", {"key": "cr", "dataset": "runs"}),
                                 ("checks", {"key": "cp", "dataset": "players"})):
                package.put(dataset, row)
            package.db.commit()
            package.db.close()
            atomic_write(root / "extracted.json", {"role": "runs"})
            self.assertEqual("loading", progress("test")["snapshots"]["runs"]["phase"])
            atomic_write(root / "verified.json", {"state": "ready"})
            item = progress("test")["snapshots"]["runs"]
            self.assertEqual("ready", item["phase"])
            self.assertEqual(1, item["records"]["runs"])
            self.assertEqual(1, item["records"]["checks"])
            self.assertNotIn("players", item["records"])

    def test_driver_lock_detects_live_process_and_stopped_process(self):
        with TemporaryDirectory() as temp:
            path = Path(temp) / "driver.lock"
            self.assertFalse(driver_running(path))
            self.assertFalse(path.exists())
            with file_lock(path):
                self.assertTrue(driver_running(path))
            self.assertFalse(driver_running(path))
