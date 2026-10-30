"""Анализ встроенных профилей по сохранённым страницам.

Проверяет покрытие поздних ранов общим списком и видимость изменения профиля."""

import tempfile
import unittest
from pathlib import Path

from speedrun_etl.inspect_player_lists import inspect
from speedrun_etl.jsonutil import atomic_write


class PlayerListInspectionTests(unittest.TestCase):
    def test_global_list_covers_later_page_and_profile_change_is_visible(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for filename, stamp, run, profiles in (
                (
                    "z-first",
                    "2026-10-27T10:00:00+00:00",
                    {"id": "r1", "playerIds": ["p1"]},
                    [{"id": "p1", "name": "A"}, {"id": "p2", "name": "B"}],
                ),
                (
                    "a-last",
                    "2026-10-27T10:01:00+00:00",
                    {"id": "r2", "playerIds": ["p2"]},
                    [{"id": "p2", "name": "Changed"}, {"id": "p1", "name": "A"}],
                ),
            ):
                atomic_write(
                    root / "requests" / filename / "response.json",
                    {
                        "endpoint": "GetGameLeaderboard2",
                        "http_status": 200,
                        "response_id": filename,
                        "fetched_at": stamp,
                        "params": {
                            "params": {"gameId": "g", "categoryId": "c", "verified": 1},
                            "page": 1 if filename == "z-first" else 2,
                        },
                        "body": {
                            "runList": [run],
                            "playerList": profiles,
                            "pagination": {"count": 2},
                        },
                    },
                )
            atomic_write(
                root / "requests" / "error" / "response.json",
                {"endpoint": "GetGameLeaderboard2", "http_status": 500, "body": None},
            )
            report = inspect(root)
            self.assertEqual(report["responses_scanned"], 2)
            row = report["sections"][0]
            self.assertEqual(row["observed_unique_run_ids"], 2)
            self.assertEqual(row["observed_unique_participant_ids"], 2)
            self.assertEqual(row["first_profiles_missing_observed_participants"], 0)
            self.assertEqual(row["first_profiles_not_in_observed_runs"], 0)
            self.assertEqual(row["distinct_profile_id_sets"], 1)
            self.assertTrue(row["first_last_profile_ids_equal"])
            self.assertFalse(row["first_last_full_profile_states_equal"])
            self.assertEqual(row["first_response_id"], "z-first")


if __name__ == "__main__":
    unittest.main()
