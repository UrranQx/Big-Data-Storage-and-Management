from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from speedrun_etl.jsonutil import dumps
from speedrun_etl.ods import Package, pack_response, restore
from speedrun_etl.repack import repack, expand
from test_ods import response


class RepackTests(unittest.TestCase):
    def test_repeated_checks_reversion_reordering_and_exact_recovery(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            source = Package(root / "original", compact=True)
            bodies = []
            for index, name in enumerate(("A", "A", "B", "A")):
                profiles = [{"id": "p", "name": name}, {"id": "q", "name": "Q"}]
                if index % 2:
                    profiles.reverse()
                body = {"playerList": profiles, "unknown": [None, 2, 2]}
                bodies.append(body)
                pack_response(source, response(body, f"r{index}", f"2026-10-16T0{index}:00:00+00:00"), "snapshot", {})
            source.flush()
            before = source.summary()
            result = repack(source.root, root / "grouped")
            target = Package(root / "grouped")
            self.assertEqual(8, result["original_observations"])
            self.assertEqual(3, result["grouped_observations"])
            original = sorted(dumps(row) for row in source.records("observations"))
            recovered = sorted(dumps(row) for group in target.records("observations") for row in expand(group))
            self.assertEqual(original, recovered)
            self.assertEqual(before, source.summary())
            for saved in target.records("responses"):
                self.assertIn(restore(target.get, saved), bodies)
            with self.assertRaises(ValueError):
                repack(source.root, target.root)
            source.db.close()
            target.db.close()
