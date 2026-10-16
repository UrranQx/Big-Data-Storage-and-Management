"""Opt-in real PG/Mongo/S3 readback and partial-write recovery, no source requests."""
import os
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from uuid import uuid4
from unittest.mock import patch

from speedrun_etl.jsonutil import digest
from speedrun_etl.ods import Package, pack_response
from test_ods import response


@unittest.skipUnless(os.environ.get("ETL_INTEGRATION") == "1", "Opt in inside Compose: ETL_INTEGRATION=1")
class StorageIntegrationTests(unittest.TestCase):
    def test_partial_write_replay_and_identical_history(self):
        from speedrun_etl.storage import init_stores, load_package, STORES
        namespace = "test_" + uuid4().hex[:12]
        with TemporaryDirectory() as temp, patch.dict(os.environ, {"ETL_NAMESPACE": namespace}):
            init_stores()
            package = Package(Path(temp) / "package", compact=True)
            for index, comment in enumerate(("A", "B", "A")):
                body = {"runList": [{"id": "synthetic_run", "gameId": "synthetic_game", "comment": comment,
                    "orphaned": True, "playerIds": ["synthetic_player"]}],
                    "playerList": [{"id": "synthetic_player", "name": "Synthetic"}]}
                pack_response(package, response(body, f"{namespace}-response{index}", f"2026-10-15T0{index}:00:00+00:00"),
                              namespace, {"game_id": "synthetic_game", "category_id": "synthetic_category", "status": "rejected"})
            package.payload({"decimal_text": "fallback test", "null_byte": "\x00", "huge_integer": 10**100})
            package.put("manifests", {"key": namespace + ":1", "records": package.summary(("payloads", "responses", "observations"))})
            summaries, histories = [], []
            for name, cls in STORES.items():
                with self.assertRaisesRegex(RuntimeError, "Injected"):
                    load_package(name, package, namespace, fail_after=3)
                first = load_package(name, package, namespace)
                second = load_package(name, package, namespace)
                self.assertEqual(first, second)
                summaries.append(first["digest"])
                with cls() as store:
                    history = store.history("run", "synthetic_run")
                    self.assertEqual(3, len(history))
                    self.assertEqual(2, len({item["payload"] for item in history}))
                    histories.append(digest(history))
            self.assertEqual(1, len(set(summaries)))
            self.assertEqual(1, len(set(histories)))
            print(f"Integration namespace retained for inspection: {namespace}")


if __name__ == "__main__":
    unittest.main()
