"""Выбранный ODS, безопасная очистка, точный replay и raw после удаления staging."""

import os
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch
from uuid import uuid4
from types import SimpleNamespace

from speedrun_etl.collect import validate_conf
from speedrun_etl.entity_ods import EntityPackage, build, DATASETS
from speedrun_etl.entity_storage import init_entities, load, ready
from speedrun_etl.jsonutil import atomic_write, digest, read
from speedrun_etl.model import kinds, role
from speedrun_etl.settings import stores
from speedrun_etl.staging import cleanup_collection, open_package, manifest, within
from test_ods import response


class PostgresModeTests(unittest.TestCase):
    def test_postgres_is_default_and_lab1_is_explicit(self):
        with patch.dict(os.environ, {}, clear=True):
            self.assertEqual(("postgres",), stores())
        with patch.dict(os.environ, ETL_STORES="postgres,mongo,garage"):
            self.assertEqual(("postgres", "mongo", "garage"), stores())
        with patch.dict(os.environ, ETL_STORES="mongo"):
            with self.assertRaises(ValueError):
                stores()

    def test_default_dag_does_not_load_other_databases_and_cleans_only_at_end(self):
        from speedrun_etl.entity_workflow import make_dag

        with patch.dict(os.environ, ETL_STORES="postgres"):
            runs = make_dag("runs", "players")
            players = make_dag("players")
            self.assertIn("load_postgres", runs.task_ids)
            self.assertNotIn("load_mongo", runs.task_ids)
            self.assertNotIn("load_garage", runs.task_ids)
            self.assertNotIn("cleanup", runs.task_ids)
            self.assertEqual(
                {"finalize"}, players.get_task("cleanup").upstream_task_ids
            )
            self.assertIn(
                "load_postgres", players.get_task("finalize").upstream_task_ids
            )

    def test_cleanup_rejects_path_traversal_and_keeps_unfinished_game(self):
        with TemporaryDirectory() as directory:
            state = Path(directory)
            with self.assertRaises(ValueError):
                within(state.parent, state)
            with self.assertRaises(ValueError):
                cleanup_collection(state, "../unsafe", apply=True)
            root = state / "snapshots" / "game-games_metadata"
            atomic_write(root / "config.json", {"scope": "single_game"})
            atomic_write(
                root / "requests" / "r" / "response.json", {"body": {"keep": True}}
            )
            self.assertEqual(
                "unfinished_chain", cleanup_collection(state, "game", True)["skipped"]
            )
            self.assertTrue((root / "requests" / "r" / "response.json").exists())

    def test_symlink_cannot_redirect_cleanup_to_checkpoints(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            checkpoints = root / "checkpoints"
            checkpoints.mkdir()
            redirected = root / "requests"
            redirected.symlink_to(checkpoints, target_is_directory=True)
            with self.assertRaises(ValueError):
                within(redirected, root)
            self.assertTrue(checkpoints.exists())


@unittest.skipUnless(
    os.environ.get("ETL_PG_INTEGRATION") == "1", "Opt in for isolated PostgreSQL"
)
class PostgresCleanupIntegrationTests(unittest.TestCase):
    def setUp(self):
        self.environment = patch.dict(os.environ, ETL_STORES="postgres")
        self.environment.start()
        self.addCleanup(self.environment.stop)
        self.temporary = TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.state = Path(self.temporary.name)
        self.collection = "test-cleanup-" + uuid4().hex[:12]
        self.ns = "test_cleanup_" + uuid4().hex[:12]
        init_entities(self.ns)
        self.conf = validate_conf(
            {
                "collection_id": self.collection,
                "scope": "single_game",
                "game_id": "g",
                "data_model": "entity-v3",
            }
        )
        self.references, self.rows = {}, {}
        self.body = {
            "runList": [
                {"id": "r", "gameId": "g", "playerIds": ["p"], "unknown": [None, 3]}
            ],
            "playerList": [{"id": "p", "name": "A"}],
        }
        for kind in kinds(self.conf):
            sid = self.collection + "-" + kind
            root = self.state / "snapshots" / sid
            atomic_write(root / "config.json", self.conf)
            atomic_write(
                root / "requests" / "r" / "response.json", response(self.body, sid)
            )
            build(
                root,
                root / "entity-package",
                format_version="entity-v3",
                source_result={
                    "coverage": "verified_public_partitions",
                    "partitions": [{"coverage": "verified_public_partitions"}],
                },
            )
            package = EntityPackage(root / "entity-package", role=role(kind))
            summary = package.summary()
            reference = {
                "snapshot_id": sid,
                "package_uri": str(package.root),
                "format_version": "entity-v3",
                "namespace": self.ns,
                "role": role(kind),
                "digest": digest(summary),
            }
            load("postgres", package, sid, self.ns, role(kind))
            self.rows[kind] = {
                dataset: list(package.records(dataset)) for dataset in DATASETS
            }
            package.db.close()
            atomic_write(root / "extracted.json", reference)
            atomic_write(
                root / "verified.json",
                ready(sid, self.ns, role(kind), reference["digest"]),
            )
            self.references[kind] = reference

    def test_cleanup_rehydrates_exact_packages_and_replay_without_source(self):
        before = cleanup_collection(self.state, self.collection)
        self.assertGreater(before["reclaimable_bytes"], 0)
        self.assertTrue(Path(self.references["runs"]["package_uri"]).exists())
        result = cleanup_collection(self.state, self.collection, apply=True)
        self.assertTrue(result["applied"])
        for kind, ref in self.references.items():
            root = Path(ref["package_uri"]).parent
            self.assertFalse(Path(ref["package_uri"]).exists())
            self.assertFalse((root / "requests").exists())
            self.assertTrue((root / "config.json").exists())
            with self.assertRaises(RuntimeError):
                EntityPackage(ref["package_uri"])
            with open_package(ref) as package:
                self.assertEqual(
                    self.rows[kind],
                    {dataset: list(package.records(dataset)) for dataset in DATASETS},
                )
                self.assertEqual(
                    self.body,
                    package.decode(next(package.records("responses"))["body"]),
                )
                self.assertEqual(
                    ref["digest"],
                    load("postgres", package, ref["snapshot_id"], self.ns, ref["role"])[
                        "digest"
                    ],
                )
            self.assertFalse(Path(ref["package_uri"]).exists())
            self.assertEqual(self.rows[kind]["manifests"][0], manifest(root))
        self.assertEqual(
            0,
            cleanup_collection(self.state, self.collection, apply=True)[
                "reclaimable_bytes"
            ],
        )

    def test_corrupt_postgres_prevents_deletion_of_the_whole_chain(self):
        from psycopg import sql
        from speedrun_etl.connections import pg_connect

        with pg_connect() as conn:
            conn.execute(
                sql.SQL(
                    "UPDATE {} SET document=jsonb_set(document,'{{corruption}}','true')"
                ).format(sql.Identifier(self.ns, "runs"))
            )
        with self.assertRaisesRegex(RuntimeError, "differs"):
            cleanup_collection(self.state, self.collection, apply=True)
        for ref in self.references.values():
            self.assertTrue((Path(ref["package_uri"]).parent / "requests").exists())

    def test_wrong_receipt_or_unfinished_chain_cannot_be_pruned(self):
        root = Path(self.references["runs"]["package_uri"]).parent
        ref = read(root / "extracted.json")
        atomic_write(root / "extracted.json", {**ref, "digest": "wrong"})
        with self.assertRaises(RuntimeError):
            cleanup_collection(self.state, self.collection, apply=True)
        self.assertTrue((root / "requests").exists())

    def test_partial_runs_and_shared_catalog_stay_on_disk(self):
        root = Path(self.references["runs"]["package_uri"]).parent
        atomic_write(
            root / "retired.json",
            {"manifest": {"source_result": {"coverage": "partial"}}},
        )
        self.assertEqual(
            "partial_runs",
            cleanup_collection(self.state, self.collection, True)["skipped"],
        )
        meta = Path(self.references["games_metadata"]["package_uri"]).parent
        atomic_write(meta / "config.json", {**self.conf, "scope": "catalog"})
        self.assertEqual(
            "shared_selection_input",
            cleanup_collection(self.state, self.collection, True)["skipped"],
        )
        self.assertTrue((root / "requests").exists())


@unittest.skipUnless(
    os.environ.get("ETL_PG_INTEGRATION") == "1", "Opt in for isolated PostgreSQL"
)
class PostgresFreshDagTests(unittest.TestCase):
    def test_production_task_callables_extract_load_finalize_cleanup_and_replay(self):
        from speedrun_etl.entity_workflow import make_dag
        from speedrun_etl.source import Client
        from test_single_game import FixtureSource

        source = FixtureSource()

        def client(state, collection, snapshot, **kwargs):
            return Client(
                state,
                collection,
                snapshot,
                **{**kwargs, "adaptive_rate": False},
                initial_pause=0,
                interval=0,
                transport=source,
            )

        with (
            TemporaryDirectory() as directory,
            patch.dict(
                os.environ,
                ETL_STATE=directory,
                ETL_STORES="postgres",
                ETL_STAGING_CLEANUP="1",
            ),
            patch("speedrun_etl.entity_runtime.Client", side_effect=client),
            patch("airflow.sdk.get_current_context") as context,
        ):
            collection = "test-dag-" + uuid4().hex[:12]
            initial = {
                "collection_id": collection,
                "scope": "single_game",
                "game_id": "g",
                "data_model": "entity-v3",
            }
            for replay in (False, True):
                forwarded = initial
                before = len(source.calls)
                for kind in kinds(initial):
                    context.return_value = {"dag_run": SimpleNamespace(conf=forwarded)}
                    child = {
                        "games_metadata": "runs",
                        "runs": "players",
                        "wr_history": "players",
                    }.get(kind)
                    dag = make_dag(kind, child)
                    conf = dag.get_task("initialize").python_callable()
                    ref = dag.get_task("collect").python_callable(conf)
                    loaded = dag.get_task("load_postgres").python_callable(
                        ref, "postgres"
                    )
                    self.assertEqual(ref["digest"], loaded["digest"])
                    forwarded = dag.get_task("finalize").python_callable(conf)
                    if "cleanup" in dag.task_ids:
                        dag.get_task("cleanup").python_callable(forwarded)
                self.assertFalse(
                    (
                        Path(directory)
                        / "snapshots"
                        / (collection + "-runs")
                        / "requests"
                    ).exists()
                )
                if replay:
                    self.assertEqual(before, len(source.calls))
            self.assertNotIn("GetGameList", [endpoint for endpoint, _ in source.calls])
