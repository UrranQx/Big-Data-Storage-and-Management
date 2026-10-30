"""Выбор игр по full-game статистике со сверенной границей каталога.

Проверяет исключение extensions и замену кандидатов без подмены рейтинга catalog count."""

import unittest
from speedrun_etl.ranking import rank_top5, rank_games, game_classification
from speedrun_etl.source import SourceError


class RankingTests(unittest.TestCase):
    def test_extensions_are_excluded_by_type_id_not_name_or_base_game(self):
        self.assertEqual(
            "category_extensions",
            game_classification(
                {"type": "game", "name": "Unrelated title", "gameTypeIds": [8]}
            ),
        )
        self.assertEqual(
            "in_scope",
            game_classification(
                {
                    "type": "game",
                    "name": "Category Extensions",
                    "gameTypeIds": [2, 13],
                    "baseGameId": "parent",
                }
            ),
        )
        for game in (
            {"type": "other"},
            {"type": "game", "gameTypeIds": [999]},
            {"type": "game", "gameTypeIds": "8"},
            {"type": "game", "gameTypeIds": {}},
        ):
            with self.assertRaises(SourceError):
                game_classification(game)

    def test_top_replaces_extension_with_next_in_scope_game(self):
        class Client:
            def request(self, endpoint, params, key, context):
                if endpoint == "GetGameList":
                    return {
                        "body": {
                            "pagination": {
                                "page": 1,
                                "pages": 1,
                                "count": 4,
                                "per": 200,
                            },
                            "gameList": [
                                {
                                    "id": str(i),
                                    "url": "game" + str(i),
                                    "name": str(i),
                                    "type": "game",
                                    "gameTypeIds": [8] if i == 0 else [],
                                    "runCount": 100 - i * 10,
                                }
                                for i in range(4)
                            ],
                        }
                    }
                i = int(params["gameId"])
                if i == 0:
                    raise AssertionError(
                        "Excluded game should not need a stats request"
                    )
                return {
                    "body": {
                        "stats": {
                            "gameId": str(i),
                            "totalRunsFG": 100 - i * 10,
                            "totalRunsIL": 0,
                            "totalRunsChallenge": 0,
                            "totalRuns": 100 - i * 10,
                        }
                    }
                }

        result = rank_games(Client(), 2)
        self.assertEqual(["1", "2"], [g["id"] for g in result["selected"]])
        self.assertEqual("0", result["excluded"][0]["id"])

    def test_top_ten_and_game_stats_list_shape(self):
        class Client:
            def request(self, endpoint, params, key, context):
                if endpoint == "GetGameList":
                    return {
                        "body": {
                            "pagination": {
                                "page": 1,
                                "pages": 1,
                                "count": 11,
                                "per": 200,
                            },
                            "gameList": [
                                {
                                    "id": str(n),
                                    "url": "game" + str(n),
                                    "name": str(n),
                                    "type": "game",
                                    "runCount": 1100 - n * 100,
                                }
                                for n in range(11)
                            ],
                        }
                    }
                n = int(params["gameId"])
                return {
                    "body": {
                        "gameStats": [
                            {
                                "gameId": str(n),
                                "totalRunsFG": 1100 - n * 100,
                                "totalRunsIL": 0,
                                "totalRunsChallenge": 0,
                                "totalRuns": 1100 - n * 100,
                            }
                        ]
                    }
                }

        result = rank_games(Client(), 10)
        self.assertEqual(10, len(result["selected"]))
        self.assertEqual(
            [str(n) for n in range(10)], [r["id"] for r in result["selected"]]
        )

    def test_full_game_stats_not_catalog_total_and_checked_bound(self):
        class Client:
            def __init__(self):
                self.queried = []

            def request(self, endpoint, params, key, context):
                if endpoint == "GetGameList":
                    return {
                        "body": {
                            "pagination": {
                                "page": 1,
                                "pages": 1,
                                "count": 7,
                                "per": 200,
                            },
                            "gameList": [
                                {
                                    "id": str(n),
                                    "url": f"g{n}",
                                    "name": f"G{n}",
                                    "type": "game",
                                    "runCount": total,
                                }
                                for n, total in enumerate(
                                    [1000, 900, 800, 700, 600, 500, 50]
                                )
                            ],
                        }
                    }
                n = int(params["gameId"])
                self.queried.append(n)
                fg = [1, 900, 800, 700, 600, 500, 50][n]
                return {
                    "body": {
                        "stats": {
                            "gameId": str(n),
                            "totalRunsFG": fg,
                            "totalRunsIL": 0,
                            "totalRunsChallenge": 0,
                            "totalRuns": fg,
                        }
                    }
                }

        client = Client()
        result = rank_top5(client)
        self.assertEqual(["1", "2", "3", "4", "5"], [g["id"] for g in result["top5"]])
        self.assertNotIn(6, client.queried)
        self.assertEqual(50, result["boundary"]["catalog_run_count"])
