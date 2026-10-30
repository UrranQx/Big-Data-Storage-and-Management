"""Подготовка подтверждённых входов и изолированное сравнение ODS.

Проверяет роли и общие checks; реальные хранилища доступны только по opt-in."""

import os
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch
from uuid import uuid4

from speedrun_etl.benchmark import prepare_input, reader, read_operation, store_size
from speedrun_etl.entity_ods import EntityPackage, build
from speedrun_etl.entity_storage import init_entities, load
from speedrun_etl.jsonutil import atomic_write, digest
from test_ods import response


class BenchmarkTests(unittest.TestCase):
    def test_merge_uses_verified_roles_and_does_not_duplicate_shared_checks(self):
        with TemporaryDirectory() as temp:
            state = Path(temp)
            for kind in ("games_metadata", "runs", "players"):
                root = state / "snapshots" / ("c-" + kind)
                p = EntityPackage(root / "entity-package")
                p.put("manifests", {"key": kind, "snapshot_id": "c-" + kind})
                if kind in ("runs", "players"):
                    p.put("players", {"key": "p", "source_id": "p"})
                    p.put(
                        "checks",
                        {
                            "key": "pcheck",
                            "dataset": "players",
                            "source_id": "p",
                            "snapshot_id": "c-runs",
                        },
                    )
                p.db.commit()
                p.db.close()
                atomic_write(
                    root / "extracted.json",
                    {
                        "package_uri": str(root / "entity-package"),
                        "role": "runs" if kind == "runs" else "all",
                        "digest": kind,
                    },
                )
            with patch("speedrun_etl.benchmark.ready") as ready:
                result, inputs = prepare_input(state, "c", state / "merged")
            self.assertEqual(3, len(inputs))
            self.assertEqual(3, ready.call_count)
            self.assertEqual(1, len(list(result.records("players"))))
            self.assertEqual(1, len(list(result.records("checks"))))
            result.db.close()

    @unittest.skipUnless(
        os.environ.get("ETL_INTEGRATION") == "1", "Real three-store opt-in"
    )
    def test_measured_reads_and_sizes_match_in_three_isolated_stores(self):
        namespace = "test_benchread_" + uuid4().hex[:10]
        init_entities(namespace)
        with TemporaryDirectory() as temp:
            root = Path(temp)
            source = root / "sample-runs"
            for i, name in enumerate(("A", "B", "A")):
                saved = response(
                    {
                        "runList": [
                            {
                                "id": "r",
                                "gameId": "g",
                                "categoryId": "c",
                                "verified": 1,
                                "playerIds": ["p"],
                                "comment": name,
                            }
                        ],
                        "playerList": [{"id": "p", "name": name}],
                    },
                    str(i),
                    f"2026-10-28T00:00:{i:02}+00:00",
                )
                atomic_write(source / "requests" / str(i) / "response.json", saved)
            build(source, root / "package")
            p = EntityPackage(root / "package")
            summary = p.summary()
            expected = {}
            for name in ("postgres", "mongo", "garage"):
                load(name, p, "sample-runs", namespace)
                with reader(name, namespace, "sample-runs", summary) as fetch:
                    for operation in ("history_100", "latest_100", "filter"):
                        rows = read_operation(
                            fetch, operation, ["r"], "sample-runs", ("g", "c", 1)
                        )
                        checksum = digest(rows)
                        self.assertEqual(
                            expected.setdefault(operation, checksum), checksum
                        )
                        if operation == "latest_100":
                            self.assertEqual("2", rows[0][0]["seen"]["response_id"])
                size = store_size(name, namespace)
                self.assertGreater(size["reported_bytes"], 0)
                if name != "garage":
                    self.assertEqual(
                        sum(v["count"] for v in summary.values()), size["count"]
                    )
            p.db.close()
