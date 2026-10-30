"""Проверяет глобальный ID, равные места, пропуски и обновление рейтинга.

Малый ручной набор выявляет двойной учёт отправок и ошибку локального ранга;
реальные три сервиса включаются только флагом ETL_INTEGRATION.
"""

import os
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch
from uuid import uuid4

from speedrun_etl.analytics_input import database, put, rows, select_time
from speedrun_etl.analytics_benchmark import (
    BASE,
    PIPELINE,
    oracle,
    client_query,
    canonical,
    connect,
    initial,
    write,
    add_indexes,
    materialize,
    output_digest,
    patch_plan,
    apply_patch_to_store,
    incremental,
    publish,
    top,
    scan,
)


def fixture(path):
    db = database(path)
    data = {
        "games": [{"key": "g1", "name": "One"}, {"key": "g2", "name": "Two"}],
        "categories": [
            {"key": "c1", "game_id": "g1", "source_id": "c1", "name": "Any"},
            {"key": "c2", "game_id": "g2", "source_id": "c2", "name": "Any"},
        ],
        "boards": [
            {"key": "b1", "category_key": "c1", "scope_json": "[]"},
            {"key": "b2", "category_key": "c2", "scope_json": "[]"},
        ],
        "players": [
            {"key": "p1", "name": "Первый", "area": "us", "resolved": True},
            {"key": "p2", "name": "Second", "area": None, "resolved": True},
            {"key": "p3", "name": "Third", "area": "us", "resolved": True},
            {"key": "guest", "name": None, "area": None, "resolved": False},
        ],
        "runs": [],
        "run_players": [],
    }
    specs = [
        ("r1", "b1", 100, True, ["p1"]),
        ("r2", "b1", 120, True, ["p1"]),
        ("r3", "b1", 100, True, ["p2"]),
        ("r4", "b1", 150, True, ["p3"]),
        ("r5", "b2", 200, True, ["p1", "p2"]),
        ("r6", "b2", 1, False, ["p3"]),
        ("r7", "b2", 1, True, ["guest"]),
        ("r8", "b2", None, False, []),
    ]
    for rid, board, seconds, eligible, players in specs:
        data["runs"].append(
            {
                "key": rid,
                "board_id": board,
                "status": 1 if eligible else 2,
                "month": "2020-01" if rid != "r8" else None,
                "time_ns": seconds * 10**9 if seconds is not None else None,
                "eligible": eligible,
            }
        )
        for i, pid in enumerate(players):
            data["run_players"].append(
                {"key": rid + ":" + str(i), "run_id": rid, "player_id": pid}
            )
    for dataset, values in data.items():
        for value in values:
            put(db, dataset, value["key"], value)
    db.commit()
    return db, data


class AnalyticsTests(unittest.TestCase):
    def test_timer_field_is_explicit_and_missing_is_not_zero(self):
        self.assertEqual(
            ("igt", 5000000000000, False),
            select_time({"igt": "5000s"}, {"defaultTimer": 2}),
        )
        self.assertEqual(
            ("timeWithLoads", 5000000000000, True),
            select_time({"timeWithLoads": "5000s"}, {"defaultTimer": 0}),
        )
        self.assertEqual(("igt", None, False), select_time({}, {"defaultTimer": 2}))

    def test_reference_and_streaming_client_match_with_ties_and_unknowns(self):
        with TemporaryDirectory() as temp:
            db, data = fixture(Path(temp) / "fixture.sqlite3")
            expected = oracle(db)
            with patch(
                "speedrun_etl.analytics_benchmark.scan",
                side_effect=lambda h, d: iter(data[d]),
            ):
                for op in PIPELINE:
                    data[op] = client_query({}, op)
                    import hashlib
                    from speedrun_etl.jsonutil import dumps

                    digest = hashlib.sha256()
                    for row in sorted(data[op], key=lambda r: r["key"]):
                        digest.update(dumps(canonical(op, row)).encode() + b"\n")
                    self.assertEqual(
                        expected[op],
                        {"count": len(data[op]), "digest": digest.hexdigest()},
                        op,
                    )
            b1 = {
                r["player_id"]: r["place"]
                for r in data["ranked"]
                if r["board_id"] == "b1"
            }
            self.assertEqual({"p1": 1, "p2": 1, "p3": 3}, b1)
            self.assertEqual(
                2, next(r["boards"] for r in data["scores"] if r["key"] == "p1")
            )
            self.assertNotIn("guest", {r["key"] for r in data["scores"]})
            self.assertEqual(8, sum(r["n_runs"] for r in data["activity"]))
            db.close()

    @unittest.skipUnless(
        os.environ.get("ETL_INTEGRATION") == "1", "Opt in for three isolated stores"
    )
    def test_real_stores_full_pipeline_and_incremental_equal_full_refresh(self):
        with TemporaryDirectory() as temp:
            db, data = fixture(Path(temp) / "fixture.sqlite3")
            expected = oracle(db)
            delta = patch_plan(db)
            namespace = "test_wl_an_it_" + uuid4().hex[:12]
            for name in ("postgres", "mongo", "garage"):
                with self.subTest(store=name), connect(name, namespace) as h:
                    initial(h)
                    for dataset in BASE:
                        write(h, dataset, rows(db, dataset))
                    add_indexes(h)
                    for op in PIPELINE:
                        materialize(h, op)
                        self.assertEqual(expected[op], output_digest(h, op), op)
                    publish(h)
                    expected_top = [
                        [r["key"], r["score"], r["place"]]
                        for r in sorted(
                            scan(h, "rating"), key=lambda r: (r["place"], r["key"])
                        )[:20]
                    ]
                    self.assertEqual(expected_top, top(h))
                    apply_patch_to_store(h, delta)
                    for op in ("best", "ranked", "features", "scores", "rating"):
                        materialize(h, op, "_full")
                    correct = output_digest(h, "rating_full")
                    incremental(h, delta)
                    self.assertEqual(correct, output_digest(h, "rating"))
            db.close()
