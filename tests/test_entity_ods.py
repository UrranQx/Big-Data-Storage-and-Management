from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

from speedrun_etl.entity_ods import EntityPackage, build
from speedrun_etl.jsonutil import atomic_write, loads
from test_ods import response


class EntityODSTests(unittest.TestCase):
    def test_empty_wr_filters_and_profile_projections_are_separate(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "snapshot"
            cases = [
                ("GetGameRecordHistory", {}, {"params": {"gameId": "g", "categoryId": "c", "values": []}, "page": 1}),
                ("GetGameRecordHistory", {"recordEventList": []}, {"params": {"gameId": "g", "categoryId": "c", "values": ["subcategory"]}, "page": 1}),
                ("GetGameLeaderboard2", {"playerList": [{"id": "p", "name": "P"}]}, {}),
                ("GetGameLeaderboard2", {"playerList": [{"id": "p", "name": "P", "country": None}]}, {}),
                ("GetGameData", {"game": {"id": "g", "name": "G"}, "categories": [{"id": "c"}], "unknown": [1, None]}, {}),
            ]
            for index, (endpoint, body, params) in enumerate(cases):
                saved = response(body, str(index), f"2026-10-16T0{index}:00:00+00:00")
                saved.update(endpoint=endpoint, params=params)
                atomic_write(source / "requests" / str(index) / "response.json", saved)
            build(source, root / "new")
            package = EntityPackage(root / "new")
            histories = list(package.records("wr_history"))
            self.assertEqual(2, len(histories))
            self.assertEqual(2, len({r["source_id"] for r in histories}))
            self.assertEqual(["not_checked", "unavailable"], sorted(r["coverage"] for r in histories))
            profiles = list(package.records("players"))
            self.assertEqual(2, len(profiles))
            self.assertEqual(2, len({r["projection"] for r in profiles}))
            game = next(package.records("games"))
            self.assertEqual(cases[-1][1], package.decode(game["raw"]))
            package.db.close()

    def test_entities_history_wr_and_full_source_roundtrip(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "snapshot"
            bodies = []
            for index, name in enumerate(("A", "A", "B", "A")):
                body = {"runList": [{"id": "run", "gameId": "game", "categoryId": "cat",
                    "playerIds": ["p", "q"], "orphaned": True, "unknown": [None, 2, 2]}],
                    "playerList": [{"id": "p", "name": name}]}
                bodies.append(body)
                raw = response(body, f"r{index}", f"2026-10-16T0{index}:00:00+00:00")
                # Reverse filenames to check timestamps, rather than file order.
                atomic_write(source / "requests" / str(9-index) / "response.json", raw)
            wr = {"recordEventList": [{"performedAt": "2020-07-05T00:00:00Z", "runList": [
                {"id": "record", "gameId": "game", "categoryId": "cat", "playerIds": ["p"], "time": "10.123s"}]}]}
            raw = response(wr, "wr", "2026-10-16T04:00:00+00:00")
            raw.update(endpoint="GetGameRecordHistory", params={"params": {"gameId": "game", "categoryId": "cat", "values": []}, "page": 1})
            atomic_write(source / "requests/wr/response.json", raw)
            result = build(source, root / "new")
            package = EntityPackage(root / "new")
            self.assertEqual(2, result["records"]["runs"]["count"])
            self.assertEqual(2, result["records"]["players"]["count"])
            check = next(r for r in package.records("checks") if r["dataset"] == "players")
            self.assertEqual(["r0", "r2", "r3"], [s["response_id"] for s in check["states"]])
            self.assertEqual("r3", check["last_seen"]["response_id"])
            self.assertEqual(check["states"][0]["version_key"], check["states"][-1]["version_key"])
            for saved in package.records("responses"):
                self.assertIn(package.decode(saved["body"]), bodies + [wr])
            history = next(package.records("wr_history"))
            self.assertEqual(wr, package.decode(history["raw"]))
            self.assertEqual("not_checked", history["coverage"])
            package.db.close()
            from speedrun_etl.audit_entity_ods import audit
            evidence = audit(source, root / "new")
            self.assertEqual(1, evidence["leaderboard_unique_run_ids"])
            self.assertEqual(1, evidence["embedded_profile_ids"])
            self.assertEqual(["cat"], evidence["wr_categories"])
            with self.assertRaises(ValueError):
                build(source, root / "new")
            self.assertEqual(result["digest"], build(source, root / "new", resume=True)["digest"])
            from speedrun_etl.entity_runtime import profiles
            runs_view = EntityPackage(root / "new", role="runs")
            self.assertEqual([], list(runs_view.records("players")))
            self.assertFalse(any(c["dataset"] == "players" for c in runs_view.records("checks")))
            runs_view.db.close()
            source_package = EntityPackage(root / "new")
            target = EntityPackage(root / "profiles")
            profile_result = profiles(source_package, target, "players-snapshot", "runs-snapshot")
            self.assertEqual(1, profile_result["available_profiles"])
            self.assertEqual(["q"], profile_result["unavailable_or_unresolved"])
            self.assertEqual(2, len(list(target.records("players"))))
            self.assertEqual([], list(target.records("runs")))
            self.assertEqual([], list(target.records("responses")))
            self.assertEqual(["r0", "r2", "r3"], [s["response_id"] for s in next(target.records("checks"))["states"]])
            source_package.db.close()
            target.db.close()

    def test_blocks_exact_numbers_unknowns_and_corruption(self):
        with TemporaryDirectory() as directory:
            package = EntityPackage(directory)
            from speedrun_etl.jsonutil import dumps
            value = loads('{"number":0.1234567890123456789012345,"x":[null,2,2],"nul":"\\u0000"}')
            for compressed in (False, True):
                with patch("speedrun_etl.entity_ods.CHUNK_BYTES", 16):
                    encoded = package.encoded(dumps(value).encode(), compressed)
                self.assertEqual(value, package.decode(encoded))
            key = encoded["blocks"][0]
            package.db.execute("UPDATE records SET document=? WHERE dataset='blocks' AND key=?", (dumps({"key": key, "data": "YQ=="}), key))
            with self.assertRaises(ValueError):
                package.decode(encoded)
            package.db.close()
