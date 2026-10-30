"""Синтетические workload-фикстуры и операции в изолированных ODS.

Проверяет JSON-массивы, JOIN/окна/CRUD/DDL и replay; реальные сервисы требуют opt-in."""

import os
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from uuid import uuid4

from speedrun_etl.entity_ods import EntityPackage
from speedrun_etl.jsonutil import digest, dumps
from speedrun_etl.storage_workloads import (
    OPS,
    DATA,
    duration_ns,
    project_run,
    scratch,
    parameters,
    local_query,
    connect,
    setup,
    load_native,
    all_rows,
    indexes,
    analyze,
    query,
    clone_runs,
    rename_clone,
    verify_clone,
    cache_counts,
    cached_counts,
    backfill,
    verify_backfill,
    append_rows,
    delete_appended,
    capabilities,
    table,
    plans,
    concurrent_reads,
    pg_copy,
    alias_snapshot,
    s3_get,
)


def fixture(root):
    p = EntityPackage(root)
    for g in ("g1", "g2"):
        p.put("games", {"key": g, "name": g, "raw_json": dumps({"id": g})})
        for i in range(7):
            key = g + ":p" + str(i)
            p.put(
                "players",
                {
                    "key": key,
                    "game_id": g,
                    "profile_id": "p" + str(i),
                    "name": "Игрок " + str(i),
                    "area_id": None if i % 2 else "area",
                    "raw_json": "{}",
                },
            )
    for i in range(120):
        gid = "g1" if i % 2 else "g2"
        pid = "p" + str(i % 7)
        raw = {
            "id": str(i),
            "gameId": gid,
            "categoryId": "c" + str(i % 2),
            "verified": i % 3,
            "playerIds": [pid, pid] if i == 1 else [pid],
            "performedAt": None
            if i % 13 == 0
            else f"2020-{i % 12 + 1:02}-01T00:00:00Z",
            "time": f"{i}.123456789s",
            "platformId": "rare" if i == 1 else "common",
            "emulator": False,
            "obsolete": bool(i % 2),
            "valueIds": ["v"],
            "unknown": {"keep": [None, 1, 1]},
        }
        entity = {
            "key": "version" + str(i),
            "source_id": str(i),
            "game_id": gid,
            "category_id": raw["categoryId"],
            "status": i % 3,
            "projection": "leaderboard_run",
            "player_ids": raw["playerIds"],
        }
        p.put(
            "runs",
            project_run(entity, raw, "2026-10-28T00:00:00+00:00", "fixture-runs"),
        )
    p.db.commit()
    return p


class WorkloadTests(unittest.TestCase):
    def test_scope_guard_blocks_production_and_unqualified_names(self):
        for name in (
            "entity_v2",
            "speedrun_ods",
            "test_bench_a",
            "test_wl_a;drop schema entity_v2",
        ):
            with self.assertRaises(ValueError):
                scratch(name)
        self.assertEqual("test_wl_safe", scratch("test_wl_safe"))

    def test_duration_projection_does_not_round_or_invent_missing_times(self):
        self.assertEqual(1382760000000, duration_ns("1382.760s"))
        self.assertEqual(1000000001, duration_ns("1.000000001s"))
        for v in (
            None,
            1.5,
            "1.0000000001s",
            "1.0000000000000000000000000000000000000001s",
            "invalid",
            "999999999999999999999s",
        ):
            self.assertIsNone(duration_ns(v))

    def test_pagination_matches_and_join_preserves_context_without_duplicate_profiles(
        self,
    ):
        with TemporaryDirectory() as root:
            p = fixture(root)
            runs = list(p.records("runs"))
            profiles = {r["key"]: r for r in p.records("players")}
            params = parameters(runs)
            self.assertEqual(
                local_query(runs, profiles, "offset_page", params),
                local_query(runs, profiles, "keyset_page", params),
            )
            joined = local_query(runs, profiles, "join_1000", params)
            self.assertEqual(120, len(joined))
            self.assertEqual(1, max(len(r[1]) for r in joined))
            months = local_query(runs, profiles, "monthly_counts", params)
            self.assertEqual(120, sum(r[3] for r in months))
            self.assertTrue(any(r[2] == "" for r in months))
            p.db.close()

    @unittest.skipUnless(
        os.environ.get("ETL_INTEGRATION") == "1", "Real three-store opt-in"
    )
    def test_copy_and_staging_replay_preserve_json_arrays_and_all_rows(self):
        ns = "test_wl_copyfixture_" + uuid4().hex[:10]
        with TemporaryDirectory() as root:
            p = fixture(root)
            expected = {d: digest(list(p.records(d))) for d in DATA}
            with connect("postgres", ns) as h:
                setup(h)
                for merge in (False, True):
                    pg_copy(h, p, merge=merge)
                    for d in DATA:
                        self.assertEqual(expected[d], digest(all_rows(h, d)))
            p.db.close()

    @unittest.skipUnless(
        os.environ.get("ETL_INTEGRATION") == "1", "Real three-store opt-in"
    )
    def test_queries_ddl_mutations_and_failure_semantics_in_isolated_stores(self):
        ns = "test_wl_fixture_" + uuid4().hex[:10]
        with TemporaryDirectory() as root:
            p = fixture(root)
            runs = list(p.records("runs"))
            profiles = {r["key"]: r for r in p.records("players")}
            params = parameters(runs)
            expected = {
                op: digest(local_query(runs, profiles, op, params)) for op in OPS
            }
            original = {d: digest(list(p.records(d))) for d in DATA}
            for name in ("postgres", "mongo", "garage"):
                with self.subTest(store=name), connect(name, ns) as h:
                    setup(h)
                    load_native(h, p)
                    for d in DATA:
                        self.assertEqual(original[d], digest(all_rows(h, d)))
                    analyze(h)
                    for phase in ("pk_only", "indexed"):
                        if phase == "indexed":
                            indexes(h)
                            analyze(h)
                        for op in OPS:
                            self.assertEqual(
                                expected[op],
                                digest(query(h, op, params)),
                                (name, phase, op),
                            )
                    if name != "garage":
                        self.assertEqual(4, len(plans(h, params)))
                    concurrent = concurrent_reads(
                        name, ns, params, expected["batch_100"]
                    )
                    self.assertEqual(20, len(concurrent["latency_seconds"]))
                    if name == "garage":
                        from speedrun_etl.jsonutil import loads

                        alias = alias_snapshot(h)
                        self.assertEqual(h["index"], loads(s3_get(h, alias)))
                    clone_runs(h)
                    rename_clone(h)
                    verify_clone(h, original["runs"])
                    cache_counts(h, params)
                    self.assertEqual(
                        expected["monthly_counts"], digest(cached_counts(h))
                    )
                    if name == "postgres":
                        from psycopg import sql

                        h["conn"].execute(
                            sql.SQL(
                                "ALTER TABLE {} ADD COLUMN scratch_tag text"
                            ).format(table(h, "runs"))
                        )
                        h["conn"].commit()
                    backfill(h, params["update_ids"])
                    verify_backfill(h, params["update_ids"], original["runs"])
                    added = [
                        {**r, "key": "benchmark-new:" + r["key"]} for r in runs[:3]
                    ]
                    append_rows(h, added)
                    self.assertEqual(123, len(all_rows(h, "runs")))
                    delete_appended(h, [r["key"] for r in added])
                    verify_backfill(h, params["update_ids"], original["runs"])
                    result = capabilities(h, "g1")
                    if name == "postgres":
                        self.assertEqual(
                            [["a", 0], ["b", 0]], result["multi_record_rollback"]
                        )
                        self.assertTrue(result["foreign_key"]["rejected"])
                    elif name == "mongo":
                        self.assertFalse(
                            result["multi_record_transaction"]["supported"]
                        )
                        self.assertEqual("accepted", result["foreign_key"])
                        self.assertEqual(
                            [["a", 1], ["b", 0]],
                            result["nontransactional_partial_write"],
                        )
                    else:
                        self.assertEqual(
                            [{"counter": 1}, {"counter": 0}],
                            result["multi_object_partial_write"],
                        )
                        self.assertIn("stale_etag_write", result)
            p.db.close()
