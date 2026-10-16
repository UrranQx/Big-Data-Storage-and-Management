import os
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from uuid import uuid4

from speedrun_etl.entity_ods import build, EntityPackage, DATASETS
from speedrun_etl.entity_storage import init_entities, load, ready, read_entity
from speedrun_etl.jsonutil import atomic_write, digest
from speedrun_etl.storage import pg_connect, Mongo, S3
from test_ods import response


@unittest.skipUnless(os.environ.get("ETL_INTEGRATION") == "1", "Opt in inside Compose")
class EntityStorageTests(unittest.TestCase):
    def test_partial_load_resume_replay_and_entity_history(self):
        namespace = "test_entity_" + uuid4().hex[:10]
        with TemporaryDirectory() as directory:
            root = Path(directory)
            for i, name in enumerate(("A", "B", "A")):
                body = {"runList": [{"id": "r", "gameId": "g", "categoryId": "c", "verified": 2,
                    "orphaned": True, "playerIds": ["p"]}], "playerList": [{"id": "p", "name": name}]}
                atomic_write(root / "snapshot/requests" / str(i) / "response.json",
                             response(body, f"resp{i}", f"2026-10-16T0{i}:00:00+00:00"))
            build(root / "snapshot", root / "package")
            package = EntityPackage(root / "package")
            summary = package.summary()
            init_entities(namespace)
            for name in ("postgres", "mongo", "garage"):
                with self.assertRaisesRegex(RuntimeError, "Injected"):
                    load(name, package, "snapshot", namespace, fail_after=1)
                first = load(name, package, "snapshot", namespace)
                second = load(name, package, "snapshot", namespace)
                self.assertEqual(first["digest"], second["digest"])
                self.assertEqual(digest(summary), first["digest"])
            self.assertEqual(digest(summary), ready("snapshot", namespace)["digest"])
            for dataset, entity in (("runs", "r"), ("players", "p"), ("checks", "p")):
                results = [read_entity(name, dataset, entity, "snapshot", summary, namespace) for name in ("postgres", "mongo", "garage")]
                self.assertEqual(1, len({digest(r) for r in results}))
                if dataset == "checks":
                    states = results[0][0]["states"]
                    self.assertEqual(3, len(states))
                    self.assertEqual(states[0]["version_key"], states[-1]["version_key"])
            from psycopg import sql
            with pg_connect() as conn, Mongo() as mongo:
                for dataset in DATASETS:
                    count = summary[dataset]["count"]
                    self.assertEqual(count, conn.execute(sql.SQL("SELECT count(*) FROM {}").format(sql.Identifier(namespace, dataset))).fetchone()[0])
                    self.assertEqual(count, mongo.client[namespace][dataset].count_documents({}))
            with S3() as store:
                keys = [obj["Key"] for page in store.client.get_paginator("list_objects_v2").paginate(
                    Bucket=store.bucket, Prefix=namespace + "/") for obj in page.get("Contents", [])]
                load("garage", package, "snapshot", namespace)
                after = [obj["Key"] for page in store.client.get_paginator("list_objects_v2").paginate(
                    Bucket=store.bucket, Prefix=namespace + "/") for obj in page.get("Contents", [])]
                self.assertEqual(keys, after)
            package.db.close()
