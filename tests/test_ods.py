"""Обратимость прежнего ODS, точные числа и история состояний.

Повторы и возврат A→B→A не должны терять происхождение или создавать ложную версию."""

from decimal import Decimal
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

from speedrun_etl.collect import profiles
from speedrun_etl.jsonutil import digest, dumps, loads
from speedrun_etl.ods import Package, pack_response, resolve_payload, restore


def response(body, rid="r1", time="2026-10-15T00:00:00+00:00"):
    return {
        "body": body,
        "response_id": rid,
        "endpoint": "GetGameLeaderboard2",
        "params": {},
        "requested_at": time,
        "fetched_at": time,
        "http_status": 200,
        "attempt": 1,
        "network_bytes": 123,
    }


class ODSTests(unittest.TestCase):
    def test_streamed_membership_array_matches_normal_payload(self):
        for block_size in (1000, 16, 9):
            with (
                self.subTest(block_size=block_size),
                patch("speedrun_etl.ods.BLOCK_BYTES", block_size),
            ):
                values = ["a", "b", "c"]
                package = Package(Path(self.temp.name) / str(block_size), compact=True)
                key = package.payload_array(iter(values))
                self.assertEqual(key, package.payload(values))
                self.assertEqual(values, resolve_payload(package.get, key))
                package.flush()
                package.db.close()

    def test_compact_package_has_identical_records_and_reopens(self):
        body = {
            "runList": [{"id": "r", "playerIds": ["p"]}],
            "playerList": [{"id": "p", "name": "P"}],
        }
        expected = self.pack(body)
        compact = Package(Path(self.temp.name) / "compact", compact=True)
        pack_response(compact, response(body), "snapshot", {"game_id": "game"})
        self.assertEqual(self.package.summary(), compact.summary())
        compact.db.close()
        reopened = Package(compact.root)
        self.assertEqual(body, restore(reopened.get, expected))
        pack_response(reopened, response(body), "snapshot", {"game_id": "game"})
        self.assertEqual(self.package.summary(), reopened.summary())
        reopened.db.close()

    def setUp(self):
        self.temp = TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.package = Package(Path(self.temp.name) / "package")

    def pack(self, body, rid="r1", time="2026-10-15T00:00:00+00:00"):
        return pack_response(
            self.package, response(body, rid, time), "snapshot", {"game_id": "game"}
        )

    def test_retry_fresh_and_reversion_history(self):
        a = {"runList": [{"id": "run", "comment": "A", "orphaned": True}]}
        b = {"runList": [{"id": "run", "comment": "B", "orphaned": True}]}
        self.pack(a)
        before = self.package.summary()
        self.pack(a)
        self.assertEqual(before, self.package.summary())
        self.pack(a, "r2", "2026-10-15T01:00:00+00:00")
        self.pack(b, "r3", "2026-10-15T02:00:00+00:00")
        self.pack(a, "r4", "2026-10-15T03:00:00+00:00")
        history = sorted(
            self.package.records("observations"), key=lambda o: o["observed_at"]
        )
        self.assertEqual(4, len(history))
        self.assertEqual(2, len({o["payload"] for o in history}))
        self.assertEqual(history[0]["payload"], history[-1]["payload"])

    def test_exact_roundtrip_unknown_fields_null_duplicates_and_numbers(self):
        body = loads(
            '{"runList":[{"id":"r","time":0.12345678901234567890123456789,"null":null},'
            '{"id":"r","time":0.12345678901234567890123456789,"null":null}],'
            '"unknown":{"x":[1,null,[2,2]],"$ref":"not a reference","huge":1e100000},"nul":"\\u0000"}'
        )
        saved = self.pack(body)
        restored = restore(self.package.get, saved)
        self.assertEqual(body, restored)
        self.assertEqual(2, len(list(self.package.records("observations"))))
        self.assertNotEqual(digest({}), digest({"x": None}))
        self.assertEqual(digest({"n": 100}), digest({"n": Decimal("100.00")}))

    def test_reordering_keeps_entity_payloads(self):
        items = [{"id": "p1", "name": "A"}, {"id": "p2", "name": "B"}]
        self.pack({"playerList": items})
        first = {
            o["source_id"]: o["payload"] for o in self.package.records("observations")
        }
        self.pack({"playerList": list(reversed(items))}, "r2")
        self.assertEqual(
            first,
            {
                o["source_id"]: o["payload"]
                for o in self.package.records("observations")
            },
        )

    def test_chunked_roundtrip_and_corruption(self):
        value = {"unknown": "Я" * 1000, "decimal": Decimal("0.0000000000000000001")}
        with patch("speedrun_etl.ods.BLOCK_BYTES", 100):
            key = self.package.payload(value)
        self.assertEqual(
            "json-v1-blocks", self.package.get("payloads", key)["encoding"]
        )
        self.assertEqual(value, resolve_payload(self.package.get, key))
        first = self.package.get("payloads", key)["blocks"][0]
        self.package.path("payloads", first).write_text(
            '{"encoding":"base64-block","data":"YQ=="}'
        )
        with self.assertRaises(ValueError):
            resolve_payload(self.package.get, key)

    def test_conflicting_immutable_key(self):
        self.pack({"game": {"id": "g", "name": "old"}})
        with self.assertRaises(ValueError):
            self.pack({"game": {"id": "g", "name": "new"}})

    def test_profiles_all_statuses_roles_versions_and_missing(self):
        self.pack(
            {
                "runList": [
                    {
                        "id": "r",
                        "verified": 2,
                        "playerIds": ["p1", "p2", "missing", None],
                        "submittedById": "author",
                        "verifiedById": "moderator",
                    }
                ],
                "playerList": [
                    {"id": "p1", "name": "First"},
                    {"id": "p2", "name": "Second", "areaId": "ru"},
                    {"id": "author", "name": "Author"},
                ],
            }
        )
        self.pack(
            {"playerList": [{"id": "p1", "name": "Changed"}]},
            "r2",
            "2026-10-15T02:00:00+00:00",
        )
        target = Package(Path(self.temp.name) / "profiles")
        result = profiles(self.package, target)
        self.assertEqual(2, result["available_profiles"])
        self.assertEqual(3, result["profile_observations"])
        self.assertEqual(["missing"], result["unavailable_or_unresolved"])
        self.assertEqual(1, len(result["guests_without_id"]))
        self.assertFalse(list(target.records("responses")))
        self.assertEqual(
            {"p1", "p2"}, {o["source_id"] for o in target.records("observations")}
        )
        self.assertEqual(
            {"2026-10-15T00:00:00+00:00", "2026-10-15T02:00:00+00:00"},
            {o["observed_at"] for o in target.records("observations")},
        )

    def test_404_does_not_remove_history(self):
        self.pack({"runList": [{"id": "r", "playerIds": [], "orphaned": True}]})
        failed = response({"error": "not found"}, "404")
        failed["http_status"] = 404
        pack_response(self.package, failed, "snapshot", {})
        self.assertEqual(1, len(list(self.package.records("observations"))))
        self.assertEqual(2, len(list(self.package.records("responses"))))

    def test_readback_rejects_observation_link_even_when_stores_match(self):
        from speedrun_etl.storage import verify_store
        from speedrun_etl.jsonutil import atomic_write

        self.pack({"runList": [{"id": "r"}]})
        observation = next(self.package.records("observations"))
        observation["payload"] = self.package.payload({"id": "different"})
        atomic_write(self.package.path("observations", observation["key"]), observation)
        package = self.package

        class Store:
            def get(self, kind, key, reference=None):
                return package.get(kind, key)

        with self.assertRaisesRegex(ValueError, "original response fragment"):
            verify_store(Store(), self.package)


if __name__ == "__main__":
    unittest.main()
