"""Проверяет входы и границы нового сравнения без сети и реальных ODS.

Четыре входа v3, корректный S3-префикс, сохранение общего контекста и защита
замороженной копии нужны, чтобы измерения не потеряли WR или обычные раны.
"""

import hashlib
import io
import os
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import MagicMock, patch

from speedrun_etl.benchmark import prepare_input, reader
from speedrun_etl.entity_ods import EntityPackage
from speedrun_etl.experiment_inputs import isolated, verify_ref
from speedrun_etl.jsonutil import atomic_write, digest, dumps
from speedrun_etl.model import V3
from speedrun_etl.storage_workloads import prepare, project_run
from speedrun_etl.storage_workloads import OPS, local_query, parameters, s3_query


def make_inputs(state):
    refs = {}
    for kind in ("games_metadata", "runs", "wr_history", "players"):
        sid = "sample-" + kind
        root = state / "snapshots" / sid
        role = kind if kind in ("runs", "wr_history") else "all"
        package = EntityPackage(root / "entity-package", role=role)
        package.put("manifests", {"key": sid, "snapshot_id": sid, "format_version": V3})
        if kind == "games_metadata":
            package.put(
                "games",
                {
                    "key": "game",
                    "source_id": "g",
                    "projection": "game_detail",
                    "raw": {
                        "encoding": "native-json-v1",
                        "value": {
                            "game": {"id": "g", "name": "Game", "rules": "Keep rules"}
                        },
                    },
                },
            )
        if kind == "runs":
            for i in range(120):
                key = f"run-{i}"
                package.put(
                    "runs",
                    {
                        "key": key,
                        "source_id": str(i),
                        "game_id": "g",
                        "category_id": "c",
                        "status": 1,
                        "projection": "run_core",
                        "player_ids": ["p"],
                        "raw": {
                            "encoding": "native-json-v1",
                            "value": {"id": str(i), "time": "1s", "platformId": "pc"},
                        },
                    },
                )
                package.put(
                    "checks",
                    {
                        "key": "check-" + key,
                        "dataset": "runs",
                        "last_seen": {
                            "version_key": key,
                            "observed_at": "2026-10-30T00:00:00Z",
                        },
                        "unfiltered_leaderboard": {
                            "last_seen": {
                                "values": {"obsolete": True},
                                "response_id": "r",
                            }
                        },
                    },
                )
        if kind == "wr_history":
            package.put(
                "wr_history", {"key": "event", "source_id": "event", "game_id": "g"}
            )
        if kind == "players":
            package.put(
                "players",
                {
                    "key": "player",
                    "source_id": "p",
                    "projection": "player:test",
                    "raw": {
                        "encoding": "native-json-v1",
                        "value": {"id": "p", "name": "Player", "areaId": "US"},
                    },
                },
            )
            package.put(
                "checks",
                {
                    "key": "check-player",
                    "dataset": "players",
                    "last_seen": {
                        "version_key": "player",
                        "observed_at": "2026-10-30T00:00:00Z",
                    },
                },
            )
        package.db.commit()
        ref = {
            "snapshot_id": sid,
            "package_uri": str(package.root),
            "namespace": "entity_v3",
            "role": role,
            "format_version": V3,
            "digest": digest(package.summary()),
        }
        package.db.close()
        atomic_write(root / "extracted.json", ref)
        atomic_write(root / "config.json", {"data_model": V3})
        refs[sid] = ref
    return refs


class ExperimentComparisonTests(unittest.TestCase):
    def test_streaming_s3_matches_all_queries_without_materializing_corpus(self):
        from test_storage_workloads import fixture

        with TemporaryDirectory() as temp:
            package = fixture(Path(temp))
            try:
                runs = list(package.records("runs"))
                profiles = {row["key"]: row for row in package.records("players")}
                params = parameters(runs)
                for operation in OPS:

                    def rows(handle, dataset):
                        yield from (runs if dataset == "runs" else profiles.values())

                    with patch(
                        "speedrun_etl.storage_workloads.iter_s3_rows", side_effect=rows
                    ) as fetch:
                        self.assertEqual(
                            local_query(runs, profiles, operation, params),
                            s3_query({}, operation, params),
                        )
                        self.assertEqual(
                            2 if operation == "join_1000" else 1, fetch.call_count
                        )
            finally:
                package.db.close()

    def test_merge_includes_all_four_v3_inputs(self):
        with TemporaryDirectory() as temp:
            state = Path(temp)
            make_inputs(state)
            with patch("speedrun_etl.benchmark.ready") as ready:
                package, sources = prepare_input(state, "sample", state / "merged")
            try:
                self.assertEqual(4, len(sources))
                self.assertEqual(4, ready.call_count)
                self.assertEqual(120, len(list(package.records("runs"))))
                self.assertEqual(1, len(list(package.records("wr_history"))))
                self.assertEqual(1, len(list(package.records("players"))))
                self.assertTrue(
                    all(
                        call.kwargs["namespace"] == "entity_v3"
                        for call in ready.call_args_list
                    )
                )
            finally:
                package.db.close()

    def test_workload_accepts_v3_core_and_preserves_context(self):
        with TemporaryDirectory() as temp:
            state = Path(temp)
            make_inputs(state)
            output = state / "workloads"
            output.mkdir()
            with patch("speedrun_etl.storage_workloads.verify_ref"):
                roots = prepare(state, ["sample"], output, [100])
            package = EntityPackage(roots[0])
            try:
                rows = list(package.records("runs"))
                self.assertEqual(100, len(rows))
                self.assertTrue(all(r["properties"]["obsolete"] is True for r in rows))
                self.assertTrue(all("obsolete" not in r["raw_json"] for r in rows))
                self.assertEqual(1, len(list(package.records("players"))))
            finally:
                package.db.close()

    def test_s3_reader_uses_actual_format(self):
        store = MagicMock()
        store.bucket = "speedrun-ods"
        store.client.get_object.return_value = {
            "Body": io.BytesIO(dumps({"chunks": []}).encode())
        }
        with patch("speedrun_etl.benchmark.S3") as factory:
            factory.return_value.__enter__.return_value = store
            with reader("garage", "test_comparison", "sample-runs", {}, V3) as fetch:
                self.assertEqual([], fetch("wr_history"))
        self.assertEqual(
            "test_comparison/entity-v3/snapshots/sample-runs/all/"
            + digest({})
            + ".json",
            store.client.get_object.call_args.kwargs["Key"],
        )

    def test_isolated_marker_requires_matching_explicit_id(self):
        with TemporaryDirectory() as temp, patch.dict(os.environ, {}, clear=True):
            state = Path(temp)
            self.assertFalse(isolated(state))
            atomic_write(
                state / "isolated-experiment.json",
                {"experiment_id": "test", "production_access": False},
            )
            with self.assertRaises(ValueError):
                isolated(state)
            os.environ["ETL_EXPERIMENT_ID"] = "test"
            self.assertTrue(isolated(state))

    def test_frozen_receipts_and_physical_copy_must_match(self):
        with (
            TemporaryDirectory() as temp,
            patch.dict(os.environ, {"ETL_EXPERIMENT_ID": "test"}),
        ):
            state = Path(temp)
            refs = make_inputs(state)
            ref = refs["sample-runs"]
            package = EntityPackage(ref["package_uri"], role=ref["role"])
            summary = package.summary()
            package.db.close()
            database = Path(ref["package_uri"]) / "entities.sqlite3"
            atomic_write(
                state / "isolated-experiment.json",
                {"experiment_id": "test", "production_access": False},
            )
            atomic_write(
                state / "frozen-inputs.json",
                {
                    "snapshots": {
                        ref["snapshot_id"]: {
                            "digest": ref["digest"],
                            "sqlite_sha256": hashlib.sha256(
                                database.read_bytes()
                            ).hexdigest(),
                            "verified": {
                                "state": "ready",
                                "digest": ref["digest"],
                                "stores": {
                                    name: summary
                                    for name in ("postgres", "mongo", "garage")
                                },
                            },
                        }
                    }
                },
            )
            self.assertEqual(ref["digest"], verify_ref(state, ref)["digest"])
            with database.open("ab") as handle:
                handle.write(b"changed")
            with self.assertRaisesRegex(ValueError, "SQLite copy changed"):
                verify_ref(state, ref)


if __name__ == "__main__":
    unittest.main()
