"""Print a small, non-secret verification summary of the published archive."""

import json
from datetime import UTC, datetime

from speedrun_etl.connections import pg_connect


def rows(conn, query):
    cursor = conn.execute(query)
    names = [column.name for column in cursor.description]
    return [dict(zip(names, row, strict=True)) for row in cursor]


def main():
    with pg_connect() as conn:
        report = {
            "checked_at": datetime.now(UTC).isoformat(),
            "ods_versions": rows(
                conn,
                """SELECT
                (SELECT count(*) FROM entity_v3.runs) AS runs,
                (SELECT count(*) FROM entity_v3.players) AS players,
                (SELECT count(*) FROM entity_v3.manifests) AS manifests""",
            ),
            "context": rows(conn, "SELECT * FROM speedrun_marts.context"),
            "quality": rows(conn, "SELECT * FROM speedrun_marts.quality_summary"),
            "validation": rows(
                conn, "SELECT check_name,failures FROM speedrun_marts.validation"
            ),
            "counts": rows(
                conn,
                """SELECT
                (SELECT count(*) FROM speedrun_3nf.analytics_batch) AS batches,
                (SELECT count(*) FROM speedrun_marts.run) AS field_run_rows,
                (SELECT count(DISTINCT run_id) FROM speedrun_marts.run) AS analytical_runs,
                (SELECT count(*) FROM speedrun_marts.pb) AS personal_bests,
                (SELECT count(*) FROM speedrun_marts.scope) AS fields,
                (SELECT count(DISTINCT game_id) FROM speedrun_marts.scope) AS games,
                (SELECT count(*) FROM speedrun_marts.record_event) AS reconstructed_events,
                (SELECT count(*) FROM speedrun_marts.source_wr) AS source_wr_events,
                (SELECT count(*) FROM speedrun_marts.cohort_survival) AS survival_points""",
            ),
            "medals_top10": rows(
                conn,
                "SELECT country_name,gold,silver,bronze,medal_score,overall_rank FROM speedrun_marts.country_medals ORDER BY overall_rank,country_name LIMIT 10",
            ),
            "recommendation_evaluation": rows(
                conn, "SELECT * FROM speedrun_marts.recommendation_evaluation"
            ),
            "demo_selection": rows(conn, "SELECT * FROM speedrun_marts.demo_selection"),
            "unknown_medals": rows(
                conn,
                "SELECT count(*) AS contributions FROM speedrun_marts.country_contribution WHERE country_code='UNKNOWN'",
            ),
            "examples": rows(
                conn,
                """SELECT game_name,scope_name,field_size,wr_seconds,top10_gap_pct
                FROM speedrun_marts.field WHERE field_platform='all'
                AND game_name IN ('Celeste','Super Mario 64','Break Dirt')
                ORDER BY field_size DESC LIMIT 20""",
            ),
        }
    with pg_connect(True) as conn:
        report["queue"] = rows(
            conn,
            "SELECT state,count(*) FROM entity_batch_queue WHERE batch_id='lab1-popular500-20261030' GROUP BY state ORDER BY state",
        )
        report["receipts"] = rows(
            conn,
            "SELECT namespace,store,count(*) FROM entity_receipts GROUP BY namespace,store ORDER BY namespace,store",
        )
    print(json.dumps(report, ensure_ascii=False, indent=2, default=str))


if __name__ == "__main__":
    main()
