"""Read the published marts and print ranking evidence without changing them."""

import json
import time
from datetime import UTC, datetime
from pathlib import Path

from psycopg.rows import dict_row

from speedrun_etl.connections import pg_connect


def main():
    started = time.monotonic()
    query = Path(__file__).with_name("runner_ranking.sql").read_text(encoding="utf-8")
    with pg_connect() as conn:
        conn.execute("CREATE TEMP TABLE ranking_result ON COMMIT DROP AS " + query)
        with conn.cursor(row_factory=dict_row) as cur:

            def read(sql):
                cur.execute(sql)
                return cur.fetchall()

            result = {
                "checked_at": datetime.now(UTC).isoformat(),
                "policy": {"minimum_field_size": 10, "shrinkage": 20, "max_games": 10},
                "summary": read("""SELECT build_id,as_of,count(*) archive_people,
                    count(*) FILTER(WHERE portfolio_rank IS NOT NULL) ranked_people,
                    count(*) FILTER(WHERE solo_fields=0) without_solo,
                    count(*) FILTER(WHERE ranking_status='Поле меньше порога') below_minimum
                    FROM ranking_result GROUP BY build_id,as_of"""),
                "top_universal": read("""SELECT portfolio_rank,player_name,holder_id,portfolio_points,
                    eligible_games,portfolio_games,best_rank,result_points FROM ranking_result
                    WHERE portfolio_rank<=20 ORDER BY portfolio_rank,holder_id"""),
                "top_specialists": read("""SELECT best_rank,player_name,result_points,game_name,
                    solo_place,solo_field_size,wr_gap_pct,rank_k5,rank_k50 FROM ranking_result
                    WHERE best_rank<=10 ORDER BY best_rank,holder_id"""),
                "validation": read("""SELECT count(*) FILTER(WHERE result_points NOT BETWEEN 0 AND 100
                    OR portfolio_points NOT BETWEEN 0 AND 1000) invalid_scores,
                    count(*) FILTER(WHERE solo_place>solo_field_size) invalid_places,
                    count(*) FILTER(WHERE portfolio_rank IS NOT NULL AND eligible_games<1) invalid_coverage,
                    count(*)-count(DISTINCT holder_id) duplicate_people FROM ranking_result"""),
            }
    if any(result["validation"][0].values()):
        raise ValueError("Ranking invariants failed")
    result["query_seconds"] = round(time.monotonic() - started, 3)
    print(json.dumps(result, default=str, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
