import unittest
from speedrun_etl.ranking import rank_top5, rank_games


class RankingTests(unittest.TestCase):
    def test_top_ten_and_game_stats_list_shape(self):
        class Client:
            def request(self, endpoint, params, key, context):
                if endpoint == "GetGameList":
                    return {"body":{"pagination":{"page":1,"pages":1,"count":11,"per":200},
                        "gameList":[{"id":str(n),"url":"game"+str(n),"name":str(n),"type":"game","runCount":1100-n*100} for n in range(11)]}}
                n=int(params["gameId"])
                return {"body":{"gameStats":[{"gameId":str(n),"totalRunsFG":1100-n*100,
                    "totalRunsIL":0,"totalRunsChallenge":0,"totalRuns":1100-n*100}]}}
        result=rank_games(Client(),10)
        self.assertEqual(10,len(result["selected"]))
        self.assertEqual([str(n) for n in range(10)],[r["id"] for r in result["selected"]])

    def test_full_game_stats_not_catalog_total_and_checked_bound(self):
        class Client:
            def __init__(self):
                self.queried = []
            def request(self, endpoint, params, key, context):
                if endpoint == "GetGameList":
                    return {"body": {"pagination": {"page": 1, "pages": 1, "count": 7, "per": 200},
                        "gameList": [{"id": str(n), "url": f"g{n}", "name": f"G{n}", "type": "game", "runCount": total}
                                     for n,total in enumerate([1000, 900, 800, 700, 600, 500, 50])]}}
                n = int(params["gameId"])
                self.queried.append(n)
                fg = [1, 900, 800, 700, 600, 500, 50][n]
                return {"body": {"stats": {"gameId": str(n), "totalRunsFG": fg,
                                            "totalRunsIL": 0, "totalRunsChallenge": 0, "totalRuns": fg}}}
        client = Client()
        result = rank_top5(client)
        self.assertEqual(["1", "2", "3", "4", "5"], [g["id"] for g in result["top5"]])
        self.assertNotIn(6, client.queried)
        self.assertEqual(50, result["boundary"]["catalog_run_count"])
