"""Execute the actual ranking SQL on isolated PostgreSQL temporary fixtures."""

import os
import unittest
from decimal import Decimal
from pathlib import Path

from psycopg.rows import dict_row

from speedrun_etl.connections import pg_connect
from speedrun_etl.runner_tenure import build_tenure

QUERY = (Path(__file__).resolve().parents[1] / "infra/runner_ranking.sql").read_text(
    encoding="utf-8"
)


@unittest.skipUnless(
    os.environ.get("ETL_LAB3_INTEGRATION") == "1", "Requires PostgreSQL"
)
class RunnerRankingTests(unittest.TestCase):
    def setUp(self):
        self.conn = pg_connect()
        self.addCleanup(self.conn.close)
        self.conn.execute("""
            CREATE TEMP TABLE context(build_id text,as_of timestamptz);
            INSERT INTO context VALUES ('fixture','2026-10-30');
            CREATE TEMP TABLE player(player_id text PRIMARY KEY,player_name text);
            CREATE TEMP TABLE scope(scope_id text PRIMARY KEY,game_id text,game_name text,
                scope_name text,direction integer,timer text,field_platform text);
            CREATE TEMP TABLE pb(holder_id text,scope_id text,run_id text,seconds numeric,
                score numeric,participant_ids text[]);
            CREATE TEMP TABLE run(holder_id text,scope_id text,run_id text,score numeric,
                participant_ids text[],verified_at timestamptz);
        """)

    def field(self, name, times, *, game=None, direction=1, platform="all"):
        self.conn.execute(
            "INSERT INTO scope VALUES (%s,%s,%s,%s,%s,'time',%s)",
            (name, game or name, game or name, name, direction, platform),
        )
        for person, seconds in times:
            self.conn.execute(
                "INSERT INTO player VALUES (%s,%s) ON CONFLICT DO NOTHING",
                (person, person),
            )
            self.conn.execute(
                "INSERT INTO pb VALUES (%s,%s,%s,%s,%s,%s)",
                (
                    person,
                    name,
                    name + person,
                    seconds,
                    int(Decimal(str(seconds)) * 10**9) * direction,
                    [person],
                ),
            )
            self.conn.execute(
                "INSERT INTO run VALUES (%s,%s,%s,%s,%s,'2026-10-30')",
                (
                    person,
                    name,
                    name + person,
                    int(Decimal(str(seconds)) * 10**9) * direction,
                    [person],
                ),
            )

    def rankings(self, minimum=2):
        self.conn.execute(
            "DROP TABLE IF EXISTS pg_temp.runner_result,pg_temp.runner_tenure,pg_temp.runner_field,pg_temp._ranking_pb"
        )
        root = Path(__file__).resolve().parents[1]
        first = (root / "sql/marts/06_runner_ranking.sql").read_text(encoding="utf-8")
        self.conn.execute(first.replace("CREATE TABLE ", "CREATE TEMP TABLE "))
        build_tenure(self.conn)
        last = (root / "sql/marts/07_runner_results.sql").read_text(encoding="utf-8")
        self.conn.execute(last.replace("CREATE TABLE ", "CREATE TEMP TABLE "))
        query = QUERY.replace("speedrun_marts.", "").replace(
            "VALUES (10)", f"VALUES ({minimum})"
        )
        with self.conn.cursor(row_factory=dict_row) as cur:
            cur.execute(query)
            return {r["holder_id"]: r for r in cur.fetchall()}

    def test_ties_reverse_direction_and_duration_scale(self):
        self.field("short", [("a", 10), ("b", 10), ("c", 20)])
        self.field("long", [("d", 18000), ("e", 18000), ("f", 36000)])
        self.field("survive", [("g", 20), ("h", 20), ("i", 10)], direction=-1)
        data = self.rankings()
        for first, second, last in (("a", "b", "c"), ("d", "e", "f"), ("g", "h", "i")):
            self.assertEqual(
                (
                    data[first]["solo_place"],
                    data[second]["solo_place"],
                    data[last]["solo_place"],
                ),
                (1, 1, 3),
            )
            self.assertEqual(
                data[first]["result_points"], data[second]["result_points"]
            )
            self.assertEqual(data[first]["percentile"], 75)
            self.assertEqual(data[last]["result_points"], 0)
        self.assertEqual(data["a"]["result_points"], data["d"]["result_points"])
        self.assertEqual(data["a"]["result_points"], data["g"]["result_points"])
        self.assertIsNone(data["g"]["completion_median_seconds"])

    def test_large_field_outweighs_tiny_podium_without_inventing_solo_scores(self):
        self.field("tiny", [("tiny1", 1), ("tiny2", 2)])
        self.field("alone", [("only", 18000)])
        self.conn.execute("INSERT INTO player VALUES ('no-run','no-run')")
        self.field("crowded", [(f"runner{i}", i) for i in range(1, 1001)])
        data = self.rankings()
        self.assertGreater(
            data["runner100"]["result_points"], data["tiny1"]["result_points"]
        )
        self.assertGreater(
            data["runner100"]["result_points"], data["tiny2"]["result_points"]
        )
        self.assertIsNone(data["only"]["best_rank"])
        self.assertIsNone(data["no-run"]["best_rank"])
        stricter = self.rankings(10)
        self.assertIsNone(stricter["tiny1"]["best_rank"])

    def test_category_and_platform_copies_do_not_inflate_portfolio(self):
        self.field("base", [("a", 1), ("b", 2)])
        before = self.rankings()["a"]
        self.field("subcategory", [("a", 1), ("b", 2)], game="base")
        self.field("platform-copy", [("a", 1), ("b", 2)], game="base", platform="pc")
        self.conn.execute(
            "INSERT INTO pb VALUES ('a+b','base','team',0.5,0.5,ARRAY['a','b'])"
        )
        after = self.rankings()["a"]
        self.assertEqual(after["portfolio_points"], before["portfolio_points"])
        self.assertEqual(after["solo_field_size"], 2)
        self.assertEqual(after["eligible_games"], 1)
        for i in range(12):
            self.field(f"other{i}", [("a", 1), ("b", 2)])
        after = self.rankings()["a"]
        self.assertAlmostEqual(
            float(after["portfolio_points"]),
            sum(0.9**i for i in range(13)) * float(before["result_points"]),
        )
        self.assertEqual(after["eligible_games"], 13)

    def test_density_rewards_tight_top_without_infinite_tie_bonus(self):
        self.field("tight", [(f"t{i}", 100 + i / 100) for i in range(25)])
        self.field("wide", [(f"w{i}", 100 + i) for i in range(25)])
        self.field("ties", [(f"e{i}", 100) for i in range(25)])
        result = self.rankings(10)
        self.assertGreater(
            result["t0"]["density_points"], result["w0"]["density_points"]
        )
        self.assertEqual(result["e0"]["density_points"], 0)
        self.assertEqual(result["e0"]["result_points"], result["e24"]["result_points"])
        self.assertLessEqual(
            result["t0"]["density_points"],
            result["t0"]["quality_points"] * Decimal(".25"),
        )

    def test_past_podium_retained_and_unknown_history_is_null(self):
        self.field("history", [(f"p{i}", 100 + i * 10) for i in range(10)])
        self.conn.execute("UPDATE run SET verified_at='2026-10-01'")
        for i, seconds in [(3, 90), (4, 80), (5, 70)]:
            self.conn.execute(
                "INSERT INTO run VALUES (%s,'history',%s,%s,%s,'2026-10-11')",
                (f"p{i}", f"new{i}", seconds * 10**9, [f"p{i}"]),
            )
            self.conn.execute(
                "UPDATE pb SET score=%s,seconds=%s WHERE holder_id=%s",
                (seconds * 10**9, seconds, f"p{i}"),
            )
        result = self.rankings(10)
        self.assertEqual(result["p0"]["solo_place"], 4)
        self.assertEqual(result["p0"]["days_first"], 10)
        self.assertGreater(result["p0"]["tenure_points"], 0)
        self.conn.execute("UPDATE run SET verified_at=NULL WHERE run_id='historyp9'")
        result = self.rankings(10)
        self.assertIsNone(result["p0"]["tenure_points"])
        self.assertIn("неизвестна", result["p0"]["history_note"])

    def test_game_contributions_sum_to_global_portfolio(self):
        for game in range(12):
            self.field(str(game), [("a", 10), ("b", 20)])
        ranking = self.rankings(2)
        query = (
            Path(__file__).resolve().parents[1] / "infra/runner_contributions.sql"
        ).read_text(encoding="utf-8")
        query = query.replace("speedrun_marts.", "").replace(
            "VALUES (10)", "VALUES (2)"
        )
        total = self.conn.execute(
            "SELECT sum(weighted_points) FROM (" + query + ") x WHERE holder_id='a'"
        ).fetchone()[0]
        self.assertAlmostEqual(
            float(total), float(ranking["a"]["portfolio_points"]), places=7
        )


if __name__ == "__main__":
    unittest.main()
