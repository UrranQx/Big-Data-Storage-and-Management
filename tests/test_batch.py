from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch
from urllib.error import HTTPError
import os
import sys
from uuid import uuid4

from speedrun_etl.batch import start,validate_selection
from speedrun_etl.collect import validate_conf
from speedrun_etl.entity_ods import EntityPackage


class BatchTests(unittest.TestCase):
    @unittest.skipUnless(os.environ.get("ETL_INTEGRATION") == "1", "Real control DB opt-in")
    def test_queue_persists_and_resume_skips_completed_games(self):
        from speedrun_etl.batch import main
        from speedrun_etl.storage import pg_connect
        batch_id="testbatch"+uuid4().hex[:10]
        games=[{"id":"g1","url":"one","name":"One"},{"id":"g2","url":"two","name":"Two"}]
        started=[]
        with TemporaryDirectory() as temp:
            def fake_start(collection,conf):
                started.append(collection)
                package=EntityPackage(Path(temp)/"snapshots"/(collection+"-runs")/"entity-package")
                package.put("manifests",{"key":"m","source_result":{"coverage":"verified_public_partitions"}})
                package.db.commit();package.db.close()
            argv=["batch","run","--collection",batch_id,"--top","2"]
            with patch.dict(os.environ,{"ETL_STATE":temp}),patch.object(sys,"argv",argv),patch(
                "speedrun_etl.batch.start",side_effect=fake_start),patch("speedrun_etl.batch.wait"),patch(
                "speedrun_etl.batch.selection",return_value=games):
                main()
                first=sum("-g-" in x for x in started)
                main()
            self.assertEqual(2,first)
            self.assertEqual(2,sum("-g-" in x for x in started))
        with pg_connect(True) as conn:
            rows=conn.execute("SELECT state FROM entity_batch_queue WHERE batch_id=%s",[batch_id]).fetchall()
        self.assertEqual(["complete","complete"],sorted(r[0] for r in rows))

    def test_general_game_scope_has_no_old_machine_deadline(self):
        conf=validate_conf({"collection_id":"team-g-123","scope":"game","game":"example",
            "game_id":"game123","selection_snapshot_id":"team-ranking-games_metadata"})
        self.assertNotIn("deadline",conf)
        self.assertEqual(5000,conf["request_budget"])
        with self.assertRaises(ValueError):
            validate_conf({**conf,"game":"../bad"})

    def test_game_must_match_selected_id_and_url(self):
        with TemporaryDirectory() as temp:
            package=EntityPackage(Path(temp)/"parent")
            package.put("manifests",{"key":"manifest","source_result":{"selected":[{"id":"g","url":"game"}]}})
            package.db.commit()
            def parent(*args):return EntityPackage(package.root)
            with patch("speedrun_etl.batch.parent_package",side_effect=parent):
                validate_selection(temp,{"scope":"game","game_id":"g","game":"game","selection_snapshot_id":"parent"})
                with self.assertRaises(ValueError):
                    validate_selection(temp,{"scope":"game","game_id":"g","game":"wrong","selection_snapshot_id":"parent"})
            package.db.close()

    def test_existing_dag_run_is_reused_only_for_same_scope(self):
        conf={"collection_id":"team","scope":"catalog","request_budget":650}
        def call(method,path,body=None,token=None):
            if method=="POST":raise HTTPError(path,409,"exists",{},None)
            return {"conf":conf}
        with patch("speedrun_etl.batch.token",return_value="fake-test-token"),patch("speedrun_etl.batch.api",side_effect=call):
            start("team",{"scope":"catalog","request_budget":650})
            with self.assertRaises(ValueError):start("team",{"scope":"catalog","request_budget":700})
