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
                "policy": {
                    "minimum_field_size": 10,
                    "shrinkage": 20,
                    "max_games": None,
                    "game_decay": 0.9,
                    "density_maximum": 0.25,
                    "tenure_maximum_points": 25,
                    "score_precision": 9,
                    "maximum_portfolio_points": 1500,
                },
                "summary": read("""SELECT build_id,as_of,count(*) archive_people,
                    count(*) FILTER(WHERE portfolio_rank IS NOT NULL) ranked_people,
                    count(*) FILTER(WHERE solo_fields=0) without_solo,
                    count(*) FILTER(WHERE ranking_status='Поле меньше порога') below_minimum,
                    count(*) FILTER(WHERE unknown_history_games>0) incomplete_history_people
                    FROM ranking_result GROUP BY build_id,as_of"""),
                "top_universal": read("""SELECT portfolio_rank,player_name,holder_id,portfolio_points,
                    eligible_games,portfolio_games,best_rank,result_points,
                    portfolio_quality,portfolio_density,portfolio_tenure,unknown_history_games FROM ranking_result
                    WHERE portfolio_rank<=20 ORDER BY portfolio_rank,holder_id"""),
                "top_specialists": read("""SELECT best_rank,player_name,result_points,game_name,
                    solo_place,solo_field_size,wr_gap_pct,quality_points,density_points,tenure_points,history_note FROM ranking_result
                    WHERE best_rank<=10 ORDER BY best_rank,holder_id"""),
                "history_coverage": read("""SELECT history_status,count(*) fields,sum(solo_field_size) pb_people
                    FROM speedrun_marts.runner_field WHERE solo_field_size>=10 GROUP BY history_status ORDER BY history_status"""),
                "retained_example": read("""SELECT r.*,s.scope_name FROM speedrun_marts.runner_result r
                    JOIN speedrun_marts.scope s USING(scope_id)
                    WHERE r.holder_id='qjne76wj' AND s.game_name='Red Ball' ORDER BY r.result_points DESC LIMIT 1"""),
                "validation": read("""SELECT count(*) FILTER(WHERE result_points NOT BETWEEN 0 AND 150
                    OR portfolio_points NOT BETWEEN 0 AND 1500) invalid_scores,
                    count(*) FILTER(WHERE abs(portfolio_points-portfolio_quality-portfolio_density-portfolio_tenure)>0.00000001) invalid_components,
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
